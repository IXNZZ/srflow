//! V21-06 起的共享测试支持：poll／gate 驱动、业务事件与 child Scope 观测、Root 输入登记。
//!
//! 只在本 crate 的 `#[cfg(test)]` 构建下存在；不提供生产调用、业务 Data 注入或长期借用
//! 存储。V21-05 与 V21-06 的样本共用这里的实现，避免复制第二份 gate／pending 机制：
//! - [`drive`]／[`release_gate`]：手动 poll（`Waker::noop`），每次 Pending 先释放挂起点；
//! - [`take_events`]／[`take_shared_events`]：业务见证与 V21-04 既有
//!   `context::creation_counts` 事件合入同一序列；
//!
//! 每个样本在起点重置自己使用的 gate／事件观测，保持线程局部隔离；业务值、
//! Node／Orchestrator 夹具与业务调用计数留在各自测试模块。

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll, Waker};

use super::context::{BodyError, ExecutionContext};
use super::identity::ScopeId;
use super::ref_id::RefId;

// ---- 业务事件（与 Context 事件同序列） ----

thread_local! {
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// 取走 Context 侧共享日志（含 guard 清理与 frame 退出事件）。
pub(crate) fn take_shared_events() -> Vec<String> {
    super::context::creation_counts::take_events()
}

/// 取走当前线程的业务事件序列。
pub(crate) fn take_events() -> Vec<String> {
    EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

// ---- Consume prepare 拒绝、内部清理之前的窄只读快照 ----

/// Consume 收口前后的完整只读观察（不吞观察错误、不修改状态）。
///
/// 记录两侧完整 target-aware refs／owned（含 Scope／DataId 身份）、collector 状态与
/// 被选输出的责任方／存活；`Before` 与 `AfterReject` 成对比较，`None` 表示无该项观察。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsumePreCleanupSnapshot {
    /// 观察阶段：`prepare` 之前 / prepare 拒绝之后（内部 cleanup 之前）。
    pub(crate) phase: ConsumeSnapshotPhase,
    /// Item 侧完整 refs（target-aware，含位置与目标身份）。
    pub(crate) item_refs: Option<
        Vec<(
            crate::core::ref_id::RefId,
            crate::core::scope::TargetSnapshot,
        )>,
    >,
    /// Item 侧 owned 集合。
    pub(crate) item_owned: Option<Vec<crate::core::identity::DataId>>,
    /// 直接 parent（collector owner）侧完整 refs。
    pub(crate) parent_refs: Option<
        Vec<(
            crate::core::ref_id::RefId,
            crate::core::scope::TargetSnapshot,
        )>,
    >,
    /// 直接 parent 侧 owned 集合。
    pub(crate) parent_owned: Option<Vec<crate::core::identity::DataId>>,
    /// 被选输出当时的责任方（目标为完整 Data 时）。
    pub(crate) selected_data: Option<crate::core::identity::DataId>,
    /// 被选输出当时的责任方。
    pub(crate) selected_owner: Option<crate::core::identity::ScopeId>,
    /// 被选输出当时是否仍存活。
    pub(crate) selected_alive: bool,
    /// collector 元素类型名。
    pub(crate) collector_element: Option<&'static str>,
    /// collector 已移动的元素个数。
    pub(crate) collector_moves: Option<usize>,
    /// collector 的责任 Scope。
    pub(crate) collector_owner: Option<crate::core::identity::ScopeId>,
    /// 观察失败时的显式说明（不静默降级为默认值）。
    pub(crate) observation_error: Option<String>,
}

/// 观察阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumeSnapshotPhase {
    /// `prepare_consume` 之前。
    Before,
    /// prepare 拒绝之后、内部 cleanup 之前。
    AfterReject,
}

thread_local! {
    static PRE_CLEANUP: RefCell<Vec<ConsumePreCleanupSnapshot>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次 Consume 收口观察。
pub(crate) fn record_consume_pre_cleanup(snapshot: ConsumePreCleanupSnapshot) {
    PRE_CLEANUP.with(|slots| slots.borrow_mut().push(snapshot));
}

// ---- Round 收口（Promote／discard）的完整前后观察（独立 typed 通道） ----

/// Round 收口的观察操作类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoundCollectOperation {
    /// 保留到父控制器状态。
    Promote,
}

/// Round 收口的观察阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoundCollectSnapshotPhase {
    /// 冻结之后、`prepare` 之前。
    Before,
    /// `prepare` 拒绝之后、内部 cleanup 之前。
    AfterReject,
}

/// Round 收口的完整前后快照（只读；`None` 表示该项观察失败，见 `observation_error`）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RoundCollectPreCleanupSnapshot {
    /// 观察阶段。
    pub(crate) phase: RoundCollectSnapshotPhase,
    /// 本次操作类型。
    pub(crate) operation: RoundCollectOperation,
    /// 来源 Scope（Round）。
    pub(crate) source: Option<crate::core::identity::ScopeId>,
    /// 来源 Scope 的可见状态。
    pub(crate) source_state: Option<crate::core::scope::ScopeState>,
    /// 来源侧完整 refs（target-aware）。
    pub(crate) source_refs: Option<
        Vec<(
            crate::core::ref_id::RefId,
            crate::core::scope::TargetSnapshot,
        )>,
    >,
    /// 来源侧 owned 集合。
    pub(crate) source_owned: Option<Vec<crate::core::identity::DataId>>,
    /// 控制器（状态 / Round 的直接 parent）Scope。
    pub(crate) controller: Option<crate::core::identity::ScopeId>,
    /// 控制器侧完整 refs。
    pub(crate) controller_refs: Option<
        Vec<(
            crate::core::ref_id::RefId,
            crate::core::scope::TargetSnapshot,
        )>,
    >,
    /// 控制器侧 owned 集合。
    pub(crate) controller_owned: Option<Vec<crate::core::identity::DataId>>,
    /// 被选输出当时的完整 Data 身份（item 目标为空）。
    pub(crate) selected_data: Option<crate::core::identity::DataId>,
    /// 被选输出当时的完整目标（target-aware；item 别名不为空）。
    pub(crate) selected_target: Option<crate::core::scope::TargetSnapshot>,
    /// 被选输出若为 item：其来源集合的实际 owner。
    pub(crate) selected_collection_owner: Option<crate::core::identity::ScopeId>,
    /// 被选输出当时的责任方。
    pub(crate) selected_owner: Option<crate::core::identity::ScopeId>,
    /// 被选输出当时是否仍存活。
    pub(crate) selected_alive: bool,
    /// 控制状态当前 target（target-aware）。
    pub(crate) state_target: Option<crate::core::scope::TargetSnapshot>,
    /// 控制状态当前 target 的责任方（完整 Data 时；item 目标为空）。
    pub(crate) state_target_owner: Option<crate::core::identity::ScopeId>,
    /// 控制状态当前 target 是否仍存活（item 目标不适用时为 false）。
    pub(crate) state_target_alive: bool,
    /// 控制状态待回收旧值。
    pub(crate) state_pending: Option<Vec<crate::core::identity::DataId>>,
    /// 观察时的下一个 `DataId` 序号。
    pub(crate) next_data_id: Option<u64>,
    /// 观察失败时的显式说明（不静默降级为默认值）。
    pub(crate) observation_error: Option<String>,
}

thread_local! {
    static ROUND_COLLECT_PRE_CLEANUP: RefCell<Vec<RoundCollectPreCleanupSnapshot>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次 Round 收口观察（独立通道；不进入共享事件序列）。
pub(crate) fn record_round_collect_pre_cleanup(snapshot: RoundCollectPreCleanupSnapshot) {
    ROUND_COLLECT_PRE_CLEANUP.with(|slots| slots.borrow_mut().push(snapshot));
}

/// 取走已记录的 Round 收口观察。
pub(crate) fn take_round_collect_pre_cleanup() -> Vec<RoundCollectPreCleanupSnapshot> {
    ROUND_COLLECT_PRE_CLEANUP.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
}

// ---- Round 成组收口（成组 Promote）的完整前后观察（独立 typed 通道） ----

/// 成组收口中一个声明槽的只读快照。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupSlotSnapshot {
    /// 声明输出位置。
    pub(crate) position: RefId,
    /// 该位置的 target（未绑定为 `None`）。
    pub(crate) target: Option<super::scope::TargetSnapshot>,
    /// 完整 Data 的责任方（item 目标为空）。
    pub(crate) owner: Option<super::identity::ScopeId>,
    /// 完整 Data 是否仍存活（item／未绑定恒为 false）。
    pub(crate) alive: bool,
}

/// 成组收口中一个目的控制状态的只读快照。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupStateSnapshot {
    /// 状态归属控制器。
    pub(crate) owner: super::identity::ScopeId,
    /// 控制器内的状态序号。
    pub(crate) seq: u64,
    /// 当前 target（target-aware；未初始化为 `None`）。
    pub(crate) target: Option<super::scope::TargetSnapshot>,
    /// 当前完整 Data target 是否仍存活（item／未初始化恒为 false）。
    pub(crate) alive: bool,
    /// 待回收旧值。
    pub(crate) pending: Vec<super::identity::DataId>,
}

/// 成组收口的完整前后快照（只读；失败项以 `observation_error` 显式记录，不静默降级）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RoundCollectGroupSnapshot {
    /// 观察阶段：冻结之后逐槽 `prepare` 之前 / 某槽 `prepare` 拒绝之后（cleanup 之前）。
    pub(crate) phase: RoundCollectSnapshotPhase,
    /// 来源 Scope（Round）。
    pub(crate) source: super::identity::ScopeId,
    /// 声明槽的逐槽快照。
    pub(crate) selected: Vec<GroupSlotSnapshot>,
    /// 目的控制状态的逐槽快照。
    pub(crate) states: Vec<GroupStateSnapshot>,
    /// 来源侧完整 refs（target-aware）。
    pub(crate) source_refs: Vec<(RefId, super::scope::TargetSnapshot)>,
    /// 来源侧 owned 集合。
    pub(crate) source_owned: Vec<super::identity::DataId>,
    /// 控制器侧完整 refs（target-aware）。
    pub(crate) controller_refs: Vec<(RefId, super::scope::TargetSnapshot)>,
    /// 控制器侧 owned 集合。
    pub(crate) controller_owned: Vec<super::identity::DataId>,
    /// 观察时的下一个 `DataId` 序号。
    pub(crate) next_data_id: Option<u64>,
    /// 观察失败时的显式说明。
    pub(crate) observation_error: Option<String>,
}

thread_local! {
    static ROUND_COLLECT_GROUP: RefCell<Vec<RoundCollectGroupSnapshot>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次成组 Round 收口观察（独立通道；不进入共享事件序列）。
pub(crate) fn record_round_collect_group(snapshot: RoundCollectGroupSnapshot) {
    ROUND_COLLECT_GROUP.with(|slots| slots.borrow_mut().push(snapshot));
}

/// 取走已记录的成组 Round 收口观察。
pub(crate) fn take_round_collect_group() -> Vec<RoundCollectGroupSnapshot> {
    ROUND_COLLECT_GROUP.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
}

/// 复位成组 Round 收口观察。
pub(crate) fn round_collect_group_reset() {
    ROUND_COLLECT_GROUP.with(|slots| slots.borrow_mut().clear());
}

// ---- 成组 Consume 的整组前后观察与窄故障钩子（独立 typed 通道） ----

/// 成组 Consume 的观察阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumeGroupSnapshotPhase {
    /// 冻结之后、逐槽 `prepare_consume` 之前。
    Before,
    /// 某槽 `prepare_consume` 拒绝之后、内部 cleanup 之前。
    AfterReject,
    /// 全部槽物理移动与撤销 owner 之后、`close_scope` 之前。
    AfterCommit,
}

/// 成组 Consume 中一个声明槽的只读快照。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ConsumeGroupSlotSnapshot {
    /// 声明输出位置。
    pub(crate) position: RefId,
    /// 该位置的 target（未绑定为 `None`）。
    pub(crate) target: Option<super::scope::TargetSnapshot>,
    /// 该槽解析到的完整 `DataId`（item 目标／未绑定为 `None`）。
    pub(crate) data_id: Option<super::identity::DataId>,
    /// 完整 Data 的责任方（item 目标为空）。
    pub(crate) owner: Option<super::identity::ScopeId>,
    /// 完整 Data 是否仍存活。
    pub(crate) alive: bool,
    /// 目的 collector 句柄。
    pub(crate) collector: Option<super::identity::CollectorId>,
    /// collector 登记的元素类型名。
    pub(crate) collector_element: Option<&'static str>,
    /// collector 的责任 Scope。
    pub(crate) collector_owner: Option<super::identity::ScopeId>,
    /// collector 已移动进建构区的元素个数。
    pub(crate) collector_moves: Option<usize>,
}

/// 成组 Consume 的整组前后快照（只读；失败项以 `observation_error` 显式记录，不静默降级）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ConsumeGroupSnapshot {
    /// 观察阶段。
    pub(crate) phase: ConsumeGroupSnapshotPhase,
    /// 当前 Item Scope。
    pub(crate) item: super::identity::ScopeId,
    /// Item 的可见状态。
    pub(crate) item_state: Option<super::scope::ScopeState>,
    /// Item 侧完整 refs（target-aware）。
    pub(crate) item_refs: Vec<(RefId, super::scope::TargetSnapshot)>,
    /// Item 侧 owned 集合。
    pub(crate) item_owned: Vec<super::identity::DataId>,
    /// Item 的直接 parent（collector 组 owner）。
    pub(crate) each: Option<super::identity::ScopeId>,
    /// Each 侧完整 refs（target-aware）。
    pub(crate) each_refs: Vec<(RefId, super::scope::TargetSnapshot)>,
    /// Each 侧 owned 集合。
    pub(crate) each_owned: Vec<super::identity::DataId>,
    /// 声明槽的逐槽快照。
    pub(crate) slots: Vec<ConsumeGroupSlotSnapshot>,
    /// 观察时的下一个 `DataId` 序号。
    pub(crate) next_data_id: Option<u64>,
    /// 观察失败时的显式说明。
    pub(crate) observation_error: Option<String>,
}

thread_local! {
    static CONSUME_GROUP: RefCell<Vec<ConsumeGroupSnapshot>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次成组 Consume 观察（独立通道；不进入共享事件序列）。
pub(crate) fn record_consume_group(snapshot: ConsumeGroupSnapshot) {
    CONSUME_GROUP.with(|slots| slots.borrow_mut().push(snapshot));
}

/// 取走已记录的成组 Consume 观察。
pub(crate) fn take_consume_group() -> Vec<ConsumeGroupSnapshot> {
    CONSUME_GROUP.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
}

/// 成组 Consume 的窄故障钩子（cfg(test)；只影响下一次命中的整组收口，不改变判断路径）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumeGroupFault {
    /// 令第 `index` 个槽在预检阶段失败；该槽之前没有任何物理移动。
    RejectPreflightAt(usize),
    /// 令第 `index` 个槽的提交在物理移动前失败（不可恢复的不变量破坏）。
    FailCommitAt(usize),
    /// 在预检之前销毁 Item 的一个待清理 owned 存储 entry，使清理前提真实失败。
    FailCleanup,
}

thread_local! {
    static CONSUME_GROUP_FAULT: RefCell<Option<ConsumeGroupFault>> = const { RefCell::new(None) };
}

/// 安装一个成组 Consume 故障（在下一次匹配阶段消费）。
pub(crate) fn install_consume_group_fault(fault: ConsumeGroupFault) {
    CONSUME_GROUP_FAULT.with(|slot| *slot.borrow_mut() = Some(fault));
}

/// 取出满足条件（一次性）的成组 Consume 故障；不匹配时保留。
pub(crate) fn take_consume_group_fault_if(
    predicate: impl Fn(&ConsumeGroupFault) -> bool,
) -> Option<ConsumeGroupFault> {
    CONSUME_GROUP_FAULT.with(|slot| {
        let matches = slot.borrow().as_ref().is_some_and(&predicate);
        if matches {
            slot.borrow_mut().take()
        } else {
            None
        }
    })
}

/// 复位成组 Consume 故障。
pub(crate) fn consume_group_fault_reset() {
    CONSUME_GROUP_FAULT.with(|slot| *slot.borrow_mut() = None);
}

// ---- Each 整组 finish 中途失败现场（WE-T06 §5.1／X17 的 Engine 级只读证据） ----

/// Each `finish_collectors` 中途失败的真实现场（cfg(test)）。
///
/// 只在真实 `invoke_each` 收口失败分支登记一次；逐槽保留 collector 是否仍在未完成建构区、
/// 已收项次数，以及 Each／caller 的本地绑定与 owned，附真实拒绝诊断（variant 与精确
/// scope／position）。全部为只读观察，不改变判断路径，也不提供任何生产旁路。
#[derive(Debug, Clone)]
pub(crate) struct EachFinishRejectScene {
    /// 失败时正在收口的 Each Scope。
    pub(crate) each: ScopeId,
    /// 调用方（Each 的直接 parent）。
    pub(crate) caller: ScopeId,
    /// 逐槽 Each 本地最终输出位置。
    pub(crate) slot_locals: Vec<RefId>,
    /// 逐槽 caller 声明输出位置。
    pub(crate) slot_outputs: Vec<RefId>,
    /// 逐槽 collector 身份（与 slot 一一对应）。
    pub(crate) collectors: Vec<super::identity::CollectorId>,
    /// 逐槽 collector 是否仍在未完成建构区（已 finish 的槽为 `false`）。
    pub(crate) collector_present: Vec<bool>,
    /// 逐槽 collector 已收项次数（不在建构区为 `None`）。
    pub(crate) collector_moves: Vec<Option<usize>>,
    /// Each 本地绑定（位置 → target）。
    pub(crate) each_refs: Vec<(RefId, super::scope::TargetSnapshot)>,
    /// Each owned 集合。
    pub(crate) each_owned: Vec<super::identity::DataId>,
    /// caller 本地绑定。
    pub(crate) caller_refs: Vec<(RefId, super::scope::TargetSnapshot)>,
    /// caller owned 集合。
    pub(crate) caller_owned: Vec<super::identity::DataId>,
    /// 真实拒绝诊断（保留 variant 与精确 scope／position）。
    pub(crate) rejected: super::internal_error::ScopeError,
}

thread_local! {
    static EACH_FINISH_REJECT: RefCell<Vec<EachFinishRejectScene>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次 Each 整组 finish 中途失败现场。
pub(crate) fn record_each_finish_reject(scene: EachFinishRejectScene) {
    EACH_FINISH_REJECT.with(|slots| slots.borrow_mut().push(scene));
}

/// 取走已记录的 Each finish 失败现场。
pub(crate) fn take_each_finish_reject() -> Vec<EachFinishRejectScene> {
    EACH_FINISH_REJECT.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
}

// ---- X19：后续不同清理诊断不得覆盖首诊断（test-only 前提破坏） ----

thread_local! {
    static LATER_CLEANUP_DIAGNOSTIC: RefCell<Option<super::internal_error::ScopeError>> =
        const { RefCell::new(None) };
}

/// cfg(test)：安装一次“后续不同清理诊断”。
///
/// 来源说明：这是 T04 R2 audit 路径的正式化——内部 test-only 前提破坏，**自然业务不会
/// 产生**该诊断；只用于核对“首次清理诊断不被后续不同诊断覆盖”。一次性消费。
#[allow(dead_code)] // 窄故障钩子：由 W06 见证正式化样本安装（消费者随后接入）
pub(crate) fn install_later_cleanup_diagnostic(error: super::internal_error::ScopeError) {
    LATER_CLEANUP_DIAGNOSTIC.with(|slot| *slot.borrow_mut() = Some(error));
}

/// cfg(test)：取出（一次性）已安装的后续清理诊断。
pub(crate) fn take_later_cleanup_diagnostic() -> Option<super::internal_error::ScopeError> {
    LATER_CLEANUP_DIAGNOSTIC.with(|slot| slot.borrow_mut().take())
}

// ---- Orchestrator Export 收口的完整前后观察（独立 typed 通道） ----

/// Export 收口的观察阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExportSnapshotPhase {
    /// 冻结之后、prepare_export 之前。
    Before,
    /// prepare_export 拒绝之后、来源清理之前。
    AfterReject,
}

/// Export 收口的完整前后快照（只读；`None` 表示观察失败）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExportPreCleanupSnapshot {
    /// 观察阶段。
    pub(crate) phase: ExportSnapshotPhase,
    /// 来源（child）Scope。
    pub(crate) child: Option<crate::core::identity::ScopeId>,
    /// caller Scope。
    pub(crate) caller: Option<crate::core::identity::ScopeId>,
    /// 来源侧完整 refs（target-aware）。
    pub(crate) child_refs: Option<
        Vec<(
            crate::core::ref_id::RefId,
            crate::core::scope::TargetSnapshot,
        )>,
    >,
    /// 来源侧 owned 集合。
    pub(crate) child_owned: Option<Vec<crate::core::identity::DataId>>,
    /// caller 侧完整 refs。
    pub(crate) caller_refs: Option<
        Vec<(
            crate::core::ref_id::RefId,
            crate::core::scope::TargetSnapshot,
        )>,
    >,
    /// caller 侧 owned 集合。
    pub(crate) caller_owned: Option<Vec<crate::core::identity::DataId>>,
    /// 本次 slot 的 child／caller 位置与声明类型名。
    pub(crate) slots: Vec<(
        crate::core::ref_id::RefId,
        crate::core::ref_id::RefId,
        &'static str,
    )>,
    /// caller 冲突位置当前的目标（target-aware）。
    pub(crate) conflict_target: Option<crate::core::scope::TargetSnapshot>,
    /// caller 冲突位置当前目标的 owner。
    pub(crate) conflict_owner: Option<crate::core::identity::ScopeId>,
    /// caller 冲突位置当前目标是否存活。
    pub(crate) conflict_alive: bool,
    /// caller 侧状态。
    pub(crate) caller_state: Option<crate::core::scope::ScopeState>,
    /// 观察时的下一个 `DataId` 序号。
    pub(crate) next_data_id: Option<u64>,
    /// 观察失败时的显式说明。
    pub(crate) observation_error: Option<String>,
}

thread_local! {
    static EXPORT_PRE_CLEANUP: RefCell<Vec<ExportPreCleanupSnapshot>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次 Export 收口观察（独立通道；不进入共享事件序列）。
pub(crate) fn record_export_pre_cleanup(snapshot: ExportPreCleanupSnapshot) {
    EXPORT_PRE_CLEANUP.with(|slots| slots.borrow_mut().push(snapshot));
}

/// 取走已记录的 Export 收口观察。
pub(crate) fn take_export_pre_cleanup() -> Vec<ExportPreCleanupSnapshot> {
    EXPORT_PRE_CLEANUP.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
}

// ---- 首次终止内容的只读记录（独立通道，不进入共享事件序列） ----

/// 一次 `terminate` 实际保存的首次终止内容（类型化只读副本）。
#[derive(Debug, Clone)]
pub(crate) struct SavedTermination {
    /// 终止类别。
    pub(crate) kind: super::context::TerminationKind,
    /// 定位 Scope。
    pub(crate) scope: Option<super::identity::ScopeId>,
    /// 说明文本。
    pub(crate) note: &'static str,
}

thread_local! {
    static SAVED_TERMINATIONS: RefCell<Vec<SavedTermination>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次实际保存的首次终止（只读；不进入共享事件序列，避免影响其它样本的次序断言）。
pub(crate) fn record_termination_saved(
    kind: super::context::TerminationKind,
    scope: Option<super::identity::ScopeId>,
    note: &'static str,
) {
    SAVED_TERMINATIONS.with(|slots| {
        slots
            .borrow_mut()
            .push(SavedTermination { kind, scope, note })
    });
}

/// 取走已记录的首次终止。
pub(crate) fn take_termination_saved() -> Vec<SavedTermination> {
    SAVED_TERMINATIONS.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
}

// ---- WE-T04：首清理诊断、失败责任与控制器错误前后快照（cfg(test) 只读观察） ----

/// 一次清理失败登记：被保存的首个诊断与后续不覆盖的尝试都记录。
#[derive(Debug, Clone)]
pub(crate) struct CleanupFailureRecord {
    /// 发生清理失败的真实 Scope。
    pub(crate) scope: super::identity::ScopeId,
    /// 真实清理失败诊断（不是通用字符串）。
    pub(crate) error: super::internal_error::ScopeError,
    /// 是否为被保存的首个清理失败（后续诊断不得覆盖它）。
    pub(crate) stored_first: bool,
}

thread_local! {
    static CLEANUP_FAILURES: RefCell<Vec<CleanupFailureRecord>> = const { RefCell::new(Vec::new()) };
}

/// 记录一次清理失败尝试（只读；不进入共享事件序列，避免影响其它样本的次序断言）。
pub(crate) fn record_cleanup_failure_probe(
    scope: super::identity::ScopeId,
    error: super::internal_error::ScopeError,
    stored_first: bool,
) {
    CLEANUP_FAILURES.with(|records| {
        records.borrow_mut().push(CleanupFailureRecord {
            scope,
            error,
            stored_first,
        })
    });
}

/// 取走全部清理失败登记。
pub(crate) fn take_cleanup_failures() -> Vec<CleanupFailureRecord> {
    CLEANUP_FAILURES.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// guard 标记失败退出时的责任快照（在本 guard 的 Drop 清理之前）。
#[derive(Debug, Clone)]
pub(crate) struct GuardFailureRecord {
    /// 本次调用 frame 的真实 Scope。
    pub(crate) frame_scope: Option<super::identity::ScopeId>,
    /// 仍由本 guard 负责退出的 Scope（未被解除的责任）。
    pub(crate) responsible: Vec<super::identity::ScopeId>,
}

thread_local! {
    static GUARD_FAILURES: RefCell<Vec<GuardFailureRecord>> = const { RefCell::new(Vec::new()) };
}

/// 记录一次 guard 失败退出的责任快照。
pub(crate) fn record_guard_failure(
    frame_scope: Option<super::identity::ScopeId>,
    responsible: Vec<super::identity::ScopeId>,
) {
    GUARD_FAILURES.with(|records| {
        records.borrow_mut().push(GuardFailureRecord {
            frame_scope,
            responsible,
        })
    });
}

/// 取走全部 guard 失败责任快照。
pub(crate) fn take_guard_failures() -> Vec<GuardFailureRecord> {
    GUARD_FAILURES.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// guard Drop 中的一次 abort 尝试（本层／父层接管的可观察证据）。
#[derive(Debug, Clone)]
pub(crate) struct GuardCleanupRecord {
    /// 发起清理的 guard frame 的真实 Scope。
    pub(crate) frame_scope: Option<super::identity::ScopeId>,
    /// 被清理的真实 Scope。
    pub(crate) scope: super::identity::ScopeId,
    /// 该次 abort 是否成功。
    pub(crate) ok: bool,
}

thread_local! {
    static GUARD_CLEANUPS: RefCell<Vec<GuardCleanupRecord>> = const { RefCell::new(Vec::new()) };
}

/// 记录一次 guard Drop 的 abort 尝试。
pub(crate) fn record_guard_cleanup(
    frame_scope: Option<super::identity::ScopeId>,
    scope: super::identity::ScopeId,
    ok: bool,
) {
    GUARD_CLEANUPS.with(|records| {
        records.borrow_mut().push(GuardCleanupRecord {
            frame_scope,
            scope,
            ok,
        })
    });
}

/// 取走全部 guard Drop abort 记录。
pub(crate) fn take_guard_cleanups() -> Vec<GuardCleanupRecord> {
    GUARD_CLEANUPS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 控制器／Round Scope 在错误前后的只读快照。
#[derive(Debug, Clone)]
pub(crate) struct ControllerScopeSnapshot {
    /// 观察点标签（`before`／`failed`…）。
    pub(crate) phase: &'static str,
    /// 角色标签（`round`／`controller`）。
    pub(crate) role: &'static str,
    /// 被观察的真实 Scope。
    pub(crate) scope: super::identity::ScopeId,
    /// 本地绑定（位置 → target）。
    pub(crate) refs: Vec<(RefId, super::scope::TargetSnapshot)>,
    /// owned 集合。
    pub(crate) owned: Vec<super::identity::DataId>,
    /// owned 中每个实例的存活与 owner。
    pub(crate) owned_alive: Vec<(
        super::identity::DataId,
        bool,
        Option<super::identity::ScopeId>,
    )>,
    /// 当时的真实状态。
    pub(crate) state: Option<super::scope::ScopeState>,
    /// 控制器登记待回收旧值（仅 controller 角色）。
    pub(crate) pending: Option<Vec<super::identity::DataId>>,
}

thread_local! {
    static CONTROLLER_SNAPSHOTS: RefCell<Vec<ControllerScopeSnapshot>> =
        const { RefCell::new(Vec::new()) };
}

/// 采集一个控制器／Round Scope 的只读快照（不改变任何状态）。
pub(crate) fn capture_controller_scope_snapshot(
    ctx: &ExecutionContext,
    phase: &'static str,
    role: &'static str,
    scope: &super::identity::ScopeId,
    controller: Option<&super::identity::ScopeId>,
) -> ControllerScopeSnapshot {
    let (refs, owned) = ctx.snapshot_targets_probe(scope).unwrap_or_default();
    let owned_alive = owned
        .iter()
        .map(|id| (id.clone(), ctx.alive_probe(id), ctx.owner_probe(id).ok()))
        .collect();
    let state = ctx.state(scope).ok();
    let pending = match (role, controller) {
        ("controller", Some(controller)) => ctx.pending_probe(controller),
        _ => None,
    };
    ControllerScopeSnapshot {
        phase,
        role,
        scope: scope.clone(),
        refs,
        owned,
        owned_alive,
        state,
        pending,
    }
}

/// 记录一个控制器／Round 快照。
pub(crate) fn record_controller_scope_snapshot(snapshot: ControllerScopeSnapshot) {
    CONTROLLER_SNAPSHOTS.with(|records| records.borrow_mut().push(snapshot));
}

/// 取走全部控制器／Round 快照。
pub(crate) fn take_controller_snapshots() -> Vec<ControllerScopeSnapshot> {
    CONTROLLER_SNAPSHOTS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 每个样本在起点重置 gate／事件／观测记录。
pub(crate) fn reset_observations() {
    closed_scope_reset();
    root_snapshots_reset();
    take_events();
    take_shared_events();
    boundary_child_scope_reset();
    boundary_address_reset();
    boundary_creation_reset();
    match_failure_scope_snapshot();
    export_attempt_snapshot();
    take_export_conflict();
    take_round_collect_pre_cleanup();
    take_consume_group();
    consume_group_fault_reset();
    take_each_finish_reject();
    take_later_cleanup_diagnostic();
    take_export_pre_cleanup();
    take_termination_saved();
    take_cleanup_failures();
    take_guard_failures();
    take_guard_cleanups();
    take_controller_snapshots();
    take_post_terminate_probe();
    release_gate();
}

// ---- 线程局部挂起点 ----

thread_local! {
    static PENDING_GATE: RefCell<Option<Rc<Cell<bool>>>> = const { RefCell::new(None) };
}

/// 释放已安装的挂起点。
pub(crate) fn release_gate() {
    PENDING_GATE.with(|gate| {
        if let Some(open) = gate.borrow().as_ref() {
            open.set(true);
        }
    });
}

// ---- V21-10：Root 提取观察、Scope 关闭记录与故障注入 ----

thread_local! {
    static CLOSED_SCOPES: RefCell<Vec<super::identity::ScopeId>> = const { RefCell::new(Vec::new()) };
}

/// 记录一个刚刚关闭的 Scope（只读观测，不改变关闭语义）。
pub(crate) fn record_scope_closed(scope: super::identity::ScopeId) {
    CLOSED_SCOPES.with(|closed| closed.borrow_mut().push(scope));
}

/// 已关闭 Scope 的顺序快照（逐层 Closed 证据）。
pub(crate) fn closed_scope_snapshot() -> Vec<super::identity::ScopeId> {
    CLOSED_SCOPES.with(|closed| closed.borrow().clone())
}

/// 复位已关闭 Scope 记录。
pub(crate) fn closed_scope_reset() {
    CLOSED_SCOPES.with(|closed| closed.borrow_mut().clear());
}

/// 成组 Promote 提交现场记录：在全部槽 `apply_promote_effects` 之后、来源 `close_scope`
/// 之前，逐槽记录控制状态现场与去重后的责任转移集合（元数据，不含业务值）。
#[derive(Debug, Clone)]
pub(crate) struct PromoteGroupStateRecord {
    /// 控制器（Loop）Scope。
    pub(crate) controller: super::identity::ScopeId,
    /// 来源 Round Scope（close 之前仍可读）。
    pub(crate) source: super::identity::ScopeId,
    /// 逐槽控制状态：(归属控制器, 序号, 提交后 target, 提交后 pending)。
    pub(crate) states: Vec<(
        super::identity::ScopeId,
        u64,
        Option<super::scope::TargetSnapshot>,
        Vec<super::identity::DataId>,
    )>,
    /// 去重后实际转移责任的 `DataId`（同一实例被多个槽共享时只出现一次）。
    pub(crate) transferred: Vec<super::identity::DataId>,
    /// 提交后控制器的 owned 集合。
    pub(crate) controller_owned: Vec<super::identity::DataId>,
}

thread_local! {
    static PROMOTE_GROUP_STATES: RefCell<Vec<PromoteGroupStateRecord>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次真实成组 Promote 提交现场（cfg(test) 只读元数据）。
pub(crate) fn record_promote_group_state(record: PromoteGroupStateRecord) {
    PROMOTE_GROUP_STATES.with(|records| records.borrow_mut().push(record));
}

/// 取出全部成组 Promote 提交现场记录。
pub(crate) fn take_promote_group_states() -> Vec<PromoteGroupStateRecord> {
    PROMOTE_GROUP_STATES.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 复位成组 Promote 提交现场记录。
pub(crate) fn promote_group_states_reset() {
    PROMOTE_GROUP_STATES.with(|records| records.borrow_mut().clear());
}

/// Root 提取观察点。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootSnapshotPhase {
    /// 同步提交完成、关闭开始之前。
    AfterCommit,
    /// 关闭尾段完成后（Root 已 Closed）。
    AfterClose,
    /// 提取预检拒绝后、失败清理之前。
    PreflightRejected,
    /// 失败清理（guard 退出）完成后。
    AfterFailureCleanup,
}

/// Root 提取的只读快照：只含元数据与物理身份，不携带业务值。
#[derive(Debug, Clone)]
pub(crate) struct RootSnapshot {
    /// 观察点。
    pub(crate) phase: RootSnapshotPhase,
    /// 预检计划的提取数。
    pub(crate) planned_takes: usize,
    /// 计划中的实例是否仍在 Container 中存活。
    pub(crate) taken_alive: Vec<bool>,
    /// 计划中的实例当前的责任方（`None` = 已不属于任何 Scope）。
    pub(crate) taken_owned_by: Vec<Option<super::identity::ScopeId>>,
    /// Root 的本地引用（位置，target）。
    pub(crate) root_refs: Vec<(RefId, super::scope::TargetSnapshot)>,
    /// Root 的 owned 集合。
    pub(crate) root_owned: Vec<super::identity::DataId>,
    /// Root 当前状态。
    pub(crate) root_state: super::scope::ScopeState,
    /// 下一个将被分配的 DataId 序号。
    pub(crate) next_data_id: Option<u64>,
    /// 该观察点上的首次终止类别（`None` = 本次执行未终止）。
    pub(crate) terminated: Option<super::context::TerminationKind>,
}

thread_local! {
    static ROOT_SNAPSHOTS: RefCell<Vec<RootSnapshot>> = const { RefCell::new(Vec::new()) };
}

/// 记录一个 Root 提取观察点。
pub(crate) fn record_root_snapshot(snapshot: RootSnapshot) {
    ROOT_SNAPSHOTS.with(|snapshots| snapshots.borrow_mut().push(snapshot));
}

/// 取出全部 Root 提取观察点。
pub(crate) fn take_root_snapshots() -> Vec<RootSnapshot> {
    ROOT_SNAPSHOTS.with(|snapshots| std::mem::take(&mut *snapshots.borrow_mut()))
}

/// 复位 Root 提取观察点。
pub(crate) fn root_snapshots_reset() {
    ROOT_SNAPSHOTS.with(|snapshots| snapshots.borrow_mut().clear());
}

/// 读取真实状态构造一个观察点（只读，不改变任何状态）。
///
/// V21-10 的历史口径（观察失败回退默认值）；V21-11 起真实路径改用
/// [`strict_root_snapshot`]／[`record_strict_root_snapshot`]，本函数只保留给前序样本。
#[allow(dead_code)]
pub(crate) fn root_snapshot(
    ctx: &super::context::ExecutionContext,
    root: &super::identity::ScopeId,
    phase: RootSnapshotPhase,
    planned_takes: usize,
    taken: &[super::identity::DataId],
) -> RootSnapshot {
    let root_refs = ctx
        .snapshot_targets_probe(root)
        .map(|(refs, _)| refs)
        .unwrap_or_default();
    let root_owned = ctx
        .snapshot_targets_probe(root)
        .map(|(_, owned)| owned)
        .unwrap_or_default();
    RootSnapshot {
        phase,
        planned_takes,
        taken_alive: taken.iter().map(|id| ctx.alive_probe(id)).collect(),
        taken_owned_by: taken.iter().map(|id| ctx.owner_probe(id).ok()).collect(),
        root_refs,
        root_owned,
        root_state: ctx.state(root).unwrap_or(super::scope::ScopeState::Closed),
        next_data_id: ctx.next_data_id_probe(),
        terminated: ctx.termination().map(|termination| termination.kind()),
    }
}

// ---- V21-11：终止后入口探针（cfg(test) 窄开关，只由 L19 安装） ----

thread_local! {
    static POST_TERMINATE_PROBE: Cell<bool> = const { Cell::new(false) };
}

/// 取出（一次性）终止后入口探针开关。
pub(crate) fn take_post_terminate_probe() -> bool {
    POST_TERMINATE_PROBE.with(|probe| {
        let armed = probe.get();
        probe.set(false);
        armed
    })
}

// ---- V21-11：严格观察（take 计数、无默认值快照） ----

thread_local! {
    static TAKEN_IDS: RefCell<Vec<super::identity::DataId>> = const { RefCell::new(Vec::new()) };
}

/// 记录一次真实 `take_verified`（只含完整 `DataId`，按调用顺序）。
pub(crate) fn record_take(id: &super::identity::DataId) {
    TAKEN_IDS.with(|taken| taken.borrow_mut().push(id.clone()));
}

/// 本次样本实际发生的 `take_verified` 完整序列。
#[allow(dead_code)] // 各 V21-11 样本按需引用
pub(crate) fn take_log_snapshot() -> Vec<super::identity::DataId> {
    TAKEN_IDS.with(|taken| taken.borrow().clone())
}

/// 复位 take 记录（每个样本开始一次）。
pub(crate) fn take_log_reset() {
    TAKEN_IDS.with(|taken| taken.borrow_mut().clear());
}

/// 观察失败：不返回默认值，携带实际失败项与底层诊断。
#[derive(Debug, Clone)]
#[allow(dead_code)] // 字段供断言与诊断引用；随严格快照被 V21-11 样本消费
pub(crate) struct ObservationError {
    /// 失败项说明。
    pub(crate) note: &'static str,
    /// 相关 Scope（如有）。
    pub(crate) scope: Option<super::identity::ScopeId>,
    /// 相关位置（如有）。
    pub(crate) position: Option<RefId>,
    /// 底层 Scope 诊断（如有）。
    pub(crate) source: Option<super::internal_error::ScopeError>,
}

/// 严格 Root 快照：任何观察项失败都返回 `ObservationError`，不使用默认值。
#[allow(dead_code)] // 各 V21-11 样本按需引用
pub(crate) fn strict_root_snapshot(
    ctx: &super::context::ExecutionContext,
    root: &super::identity::ScopeId,
    phase: RootSnapshotPhase,
    planned_takes: usize,
    taken: &[super::identity::DataId],
) -> Result<RootSnapshot, ObservationError> {
    let (root_refs, root_owned) =
        ctx.snapshot_targets_probe(root)
            .map_err(|source| ObservationError {
                note: "root refs/owned snapshot failed",
                scope: Some(root.clone()),
                position: None,
                source: Some(source),
            })?;
    let mut taken_alive = Vec::with_capacity(taken.len());
    let mut taken_owned_by = Vec::with_capacity(taken.len());
    for id in taken {
        taken_alive.push(ctx.alive_probe(id));
        // 明确区分"没有 owner"与观察失败：只有 `NoOwner` 映射成 `None`。
        match ctx.owner_probe(id) {
            Ok(owner) => taken_owned_by.push(Some(owner)),
            Err(super::internal_error::ScopeError::NoOwner { .. }) => taken_owned_by.push(None),
            Err(source) => {
                return Err(ObservationError {
                    note: "root take ownership observation failed",
                    scope: Some(root.clone()),
                    position: None,
                    source: Some(source),
                });
            }
        }
    }
    let root_state = ctx.state(root).map_err(|source| ObservationError {
        note: "root state observation failed",
        scope: Some(root.clone()),
        position: None,
        source: Some(source),
    })?;
    Ok(RootSnapshot {
        phase,
        planned_takes,
        taken_alive,
        taken_owned_by,
        root_refs,
        root_owned,
        root_state,
        next_data_id: ctx.next_data_id_probe(),
        terminated: ctx.termination().map(|termination| termination.kind()),
    })
}

/// 严格记录一个 Root 观察点：失败时登记到错误通道并**不**落快照，测试必须显式证明无失败。
#[allow(dead_code)] // 各 V21-11 样本按需引用
pub(crate) fn record_strict_root_snapshot(
    ctx: &super::context::ExecutionContext,
    root: &super::identity::ScopeId,
    phase: RootSnapshotPhase,
    planned_takes: usize,
    taken: &[super::identity::DataId],
) {
    match strict_root_snapshot(ctx, root, phase, planned_takes, taken) {
        Ok(snapshot) => record_root_snapshot(snapshot),
        Err(error) => record_root_observation_error(error),
    }
}

thread_local! {
    static ROOT_OBSERVATION_ERRORS: RefCell<Vec<ObservationError>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次严格 Root 观察的失败（生产路径只记录元数据，不改变执行）。
pub(crate) fn record_root_observation_error(error: ObservationError) {
    ROOT_OBSERVATION_ERRORS.with(|errors| errors.borrow_mut().push(error));
}

/// 本次样本的全部严格观察失败。
pub(crate) fn take_root_observation_errors() -> Vec<ObservationError> {
    ROOT_OBSERVATION_ERRORS.with(|errors| std::mem::take(&mut *errors.borrow_mut()))
}

/// 复位严格观察错误。
pub(crate) fn root_observation_errors_reset() {
    ROOT_OBSERVATION_ERRORS.with(|errors| errors.borrow_mut().clear());
}

/// 严格版本的 `reset_observations`：额外复位 take 记录。
#[allow(dead_code)] // 各 V21-11 样本按需引用
pub(crate) fn strict_reset_observations() {
    reset_observations();
    take_log_reset();
    root_observation_errors_reset();
}

/// 断言本次样本的全部严格观察都成功（V21-11 样本在比较快照前调用）。
pub(crate) fn assert_no_observation_errors() {
    let errors = take_root_observation_errors();
    assert!(
        errors.is_empty(),
        "严格观察失败（不允许默认值回退）: {errors:?}"
    );
}

// ---- Future 驱动 ----

/// 推进到 Ready；每次 Pending 先释放挂起点。
pub(crate) fn drive<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let mut pending = 0usize;
    loop {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 64, "future is not making progress");
                release_gate();
            }
        }
    }
}

// ---- 真实调用边界的 child Scope 观测（cfg(test) 只读元数据） ----

thread_local! {
    static BOUNDARY_CHILD_SCOPES: RefCell<Vec<ScopeId>> = const { RefCell::new(Vec::new()) };
    static BOUNDARY_ADDRESSES: RefCell<Vec<(ScopeId, *const (), *const (), *const ())>> =
        const { RefCell::new(Vec::new()) };
    #[allow(clippy::type_complexity)]
    static EXPORT_ATTEMPTS: RefCell<Vec<(ScopeId, Vec<(RefId, crate::core::identity::DataId)>, Vec<crate::core::identity::DataId>)>> =
        const { RefCell::new(Vec::new()) };
}

/// 清空由真实调用边界自动记录的 child Scope。
pub(crate) fn boundary_child_scope_reset() {
    BOUNDARY_CHILD_SCOPES.with(|scopes| scopes.borrow_mut().clear());
}

/// 真实 `OrchSite` 调用边界建立 child 后记录其身份（仅元数据）。
pub(crate) fn boundary_child_scope_record(child: ScopeId) {
    BOUNDARY_CHILD_SCOPES.with(|scopes| scopes.borrow_mut().push(child));
}

/// 记录一次真实调用边界的执行域地址（身份／Coordinator／Container）。
pub(crate) fn boundary_address_record(
    child: ScopeId,
    identity: *const (),
    coordinator: *const (),
    container: *const (),
) {
    BOUNDARY_ADDRESSES.with(|entries| {
        entries
            .borrow_mut()
            .push((child.clone(), identity, coordinator, container))
    });
    boundary_child_scope_record(child);
}

/// 由真实调用边界记录的执行域地址快照。
#[allow(clippy::type_complexity)]
pub(crate) fn boundary_address_snapshot() -> Vec<(ScopeId, *const (), *const (), *const ())> {
    BOUNDARY_ADDRESSES.with(|entries| entries.borrow().clone())
}

/// 清空真实调用边界记录的执行域地址。
pub(crate) fn boundary_address_reset() {
    BOUNDARY_ADDRESSES.with(|entries| entries.borrow_mut().clear());
    EXPORT_ATTEMPTS.with(|entries| entries.borrow_mut().clear());
}

/// 取走 Export 提交前记录的快照（失败时用于比较）。
#[allow(clippy::type_complexity)]
pub(crate) fn export_attempt_snapshot() -> Vec<(
    ScopeId,
    Vec<(RefId, crate::core::identity::DataId)>,
    Vec<crate::core::identity::DataId>,
)> {
    EXPORT_ATTEMPTS.with(|entries| std::mem::take(&mut *entries.borrow_mut()))
}

// ---- 真实创建点的 Scope 角色／parent 记录（cfg(test) 只读元数据） ----

thread_local! {
    static BOUNDARY_CREATIONS: RefCell<Vec<(ScopeId, ScopeId, super::orchestrator::ScopeRole)>> =
        const { RefCell::new(Vec::new()) };
    #[allow(clippy::type_complexity)]
    static MATCH_FAILURE_SCOPES: RefCell<Vec<(ScopeId, Vec<(RefId, crate::core::identity::DataId)>, Vec<crate::core::identity::DataId>)>> =
        const { RefCell::new(Vec::new()) };
}

/// 真实 `OrchSite` 调用边界建立 child 后记录完整创建元数据：ScopeId、parent 与调用角色。
///
/// 记录点在真实 `create_child` 成功之后、Import 与 body 之前，因此它是"某次调用未建立
/// Scope"的证据来源（未选 branch 没有对应记录）；同时保留既有执行域地址序列。
pub(crate) fn boundary_creation_record(
    child: ScopeId,
    parent: ScopeId,
    role: super::orchestrator::ScopeRole,
    identity: *const (),
    coordinator: *const (),
    container: *const (),
) {
    boundary_address_record(child.clone(), identity, coordinator, container);
    BOUNDARY_CREATIONS.with(|entries| entries.borrow_mut().push((child, parent, role)));
}

/// 清空真实创建点的记录。
pub(crate) fn boundary_creation_reset() {
    BOUNDARY_CREATIONS.with(|entries| entries.borrow_mut().clear());
}

/// 真实创建点记录的 `(child, parent, role)` 快照（按建立顺序）。
pub(crate) fn boundary_creation_snapshot() -> Vec<(ScopeId, ScopeId, super::orchestrator::ScopeRole)>
{
    BOUNDARY_CREATIONS.with(|entries| entries.borrow().clone())
}

/// 取走 Match 路由失败时刻记录的自身 Scope 快照。
#[allow(clippy::type_complexity)]
pub(crate) fn match_failure_scope_snapshot() -> Vec<(
    ScopeId,
    Vec<(RefId, crate::core::identity::DataId)>,
    Vec<crate::core::identity::DataId>,
)> {
    MATCH_FAILURE_SCOPES.with(|entries| std::mem::take(&mut *entries.borrow_mut()))
}

// ---- Match 预占端口开关（cfg(test) 窄注入，M17） ----

thread_local! {
    static EXPORT_CONFLICT: Cell<Option<usize>> = const { Cell::new(None) };
}

/// 取出（一次性）预占开关。
pub(crate) fn take_export_conflict() -> Option<usize> {
    EXPORT_CONFLICT.with(|slot| slot.take())
}

// ---- Root 观察视图与只读驱动（不解释 Definition／不做 Output 移交） ----

/// Root 关闭前的只读观察视图（含受控登记，供权限与 Scope 状态检查使用）。
///
/// 由 [`observe_root`] 建立：guard 与 Root Scope 由该驱动提供；字段对本 crate 的样本
/// 可见，不提供生产入口。
pub(crate) struct RootView<'a, 'ctx> {
    /// 当前 Root frame 的调用 guard。
    pub(crate) guard: &'a mut super::context::InvocationGuard<'ctx>,
    /// 本次 Root Scope。
    pub(crate) root: ScopeId,
}

impl RootView<'_, '_> {
    /// 本次 Root Scope。
    pub(crate) fn root(&self) -> &ScopeId {
        &self.root
    }

    /// 某个 Scope 的当前状态。
    pub(crate) fn state(&self, scope: &ScopeId) -> Result<super::scope::ScopeState, BodyError> {
        Ok(self.guard.state(scope)?)
    }
}

/// 在真实 Root frame 中运行一个只读观察体：建立唯一 `RootExecution` 与 Root frame，
/// 把 [`RootView`] 交给 `observe`，随后按观察结果正常收口或受控失败退出。
///
/// 驱动不登记业务输入、不解释 Definition、不做 Output 移交：它只提供合法的 Root frame
/// 与 guard，供权限／终止等 Core 不变量样本调用真实 Core 入口（`prepare_root_extraction`、
/// `close_root`、`terminate`、`enter` 等）。
pub(crate) fn observe_root<F>(observe: F) -> Result<(), BodyError>
where
    F: for<'a, 'ctx> FnOnce(&mut RootView<'a, 'ctx>) -> Result<(), BodyError>,
{
    drive(async move {
        let mut execution = super::runtime::RootExecution::start();
        let root = execution.context().root_scope().clone();
        let mut guard = execution
            .context_mut()
            .enter(super::context::InvocationKind::Root, &root, true)
            .expect("fresh execution accepts a root frame");
        let result = {
            let mut view = RootView {
                guard: &mut guard,
                root: root.clone(),
            };
            observe(&mut view)
        };
        match result {
            Ok(()) => {
                guard
                    .finalize(&root, &[], &mut Vec::new())
                    .map_err(BodyError::from)?;
                guard.complete();
                Ok(())
            }
            Err(error) => {
                guard.failed_with(&error);
                Err(error)
            }
        }
    })
}
