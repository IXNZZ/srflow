//! V21-06 起的共享测试支持：poll／gate 驱动、业务事件与 child Scope 观测、Root 输入登记。
//!
//! 只在本 crate 的 `#[cfg(test)]` 构建下存在；不提供生产调用、业务 Data 注入或长期借用
//! 存储。V21-05 与 V21-06 的样本共用这里的实现，避免复制第二份 gate／pending 机制：
//! - [`drive`]／[`drive_pinned`]／[`advance_to_pending`]：手动 poll（`Waker::noop`），
//!   每次 Pending 先释放挂起点；
//! - [`install_gate`]／[`release_gate`]／[`gate_wait`]：线程局部挂起点；
//! - [`record`]／[`take_events`]／[`take_shared_events`]：业务见证与 V21-04 既有
//!   `context::creation_counts` 事件合入同一序列；
//! - child Scope 观测：窄函数读写线程局部列表，不公开可任意修改的集合；
//! - [`RootInput`]／[`root_input`]：只经真实 `register_owned` 把应用层夹具值登记到
//!   RootScope 的声明输入位置。
//!
//! 每个样本在起点重置自己使用的 gate／事件／child 观测，保持线程局部隔离；业务值、
//! Node／Orchestrator 夹具与业务调用计数留在各自测试模块。

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll, Waker};

use super::context::{BodyError, ExecutionContext};
use super::identity::ScopeId;
use super::ref_id::RefId;

// ---- 业务事件（与 Context 事件同序列） ----

thread_local! {
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// 记录一个可观察事件（顺序即证据）。
///
/// 同时写入 V21-04 既有的 `context::creation_counts` 事件日志：业务 Drop 见证因此与
/// guard 清理／frame 退出事件处在同一条可比较序列上（frame 次序证据）。
pub(crate) fn record(event: &str) {
    EVENTS.with(|events| events.borrow_mut().push(event.to_string()));
    super::context::creation_counts::record_event(event);
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

/// 取走已记录的观察。
pub(crate) fn take_consume_pre_cleanup() -> Vec<ConsumePreCleanupSnapshot> {
    PRE_CLEANUP.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
}

// ---- Round 收口（Promote／discard）的完整前后观察（独立 typed 通道） ----

/// Round 收口的观察操作类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoundCollectOperation {
    /// 保留到父控制器状态。
    Promote,
    /// 丢弃本轮结果并关闭来源。
    Discard,
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

/// 通用 promote 探针的结构化结果（独立通道）。
#[derive(Debug, Clone)]
pub(crate) struct GenericPromoteProbeResult {
    /// 通用 `Context::promote` 的返回值。
    pub(crate) outcome: Option<crate::core::internal_error::ScopeError>,
}

thread_local! {
    static GENERIC_PROMOTE_PROBE: RefCell<Vec<GenericPromoteProbeResult>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次结构化通用 promote 探针结果。
pub(crate) fn record_generic_promote_probe(
    outcome: Option<crate::core::internal_error::ScopeError>,
) {
    GENERIC_PROMOTE_PROBE.with(|slots| {
        slots
            .borrow_mut()
            .push(GenericPromoteProbeResult { outcome })
    });
}

/// 取走结构化通用 promote 探针结果。
pub(crate) fn take_generic_promote_probe() -> Vec<GenericPromoteProbeResult> {
    GENERIC_PROMOTE_PROBE.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
}

// ---- 最终绑定（bind_state_output）的完整前后观察（独立 typed 通道） ----

/// 最终绑定的观察阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalBindSnapshotPhase {
    /// 故障已注入、目标操作之前。
    Before,
    /// 拒绝之后、guard cleanup 之前。
    AfterReject,
}

/// 最终绑定的完整前后快照（只读；`None` 表示该项观察失败，见 `observation_error`）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FinalBindPreCleanupSnapshot {
    /// 观察阶段。
    pub(crate) phase: FinalBindSnapshotPhase,
    /// 控制器（Loop）Scope。
    pub(crate) controller: Option<crate::core::identity::ScopeId>,
    /// 控制器状态。
    pub(crate) controller_state: Option<crate::core::scope::ScopeState>,
    /// 控制器侧完整 refs（target-aware）。
    pub(crate) controller_refs: Option<
        Vec<(
            crate::core::ref_id::RefId,
            crate::core::scope::TargetSnapshot,
        )>,
    >,
    /// 控制器侧 owned 集合。
    pub(crate) controller_owned: Option<Vec<crate::core::identity::DataId>>,
    /// 控制状态 target（target-aware）。
    pub(crate) state_target: Option<crate::core::scope::TargetSnapshot>,
    /// 控制状态待回收旧值。
    pub(crate) state_pending: Option<Vec<crate::core::identity::DataId>>,
    /// 最终声明输出位置当前的完整 Data 身份（未绑定为空）。
    pub(crate) position_data: Option<crate::core::identity::DataId>,
    /// 最终声明输出位置当前的目标（target-aware；item 别名不为空）。
    pub(crate) position_target: Option<crate::core::scope::TargetSnapshot>,
    /// 该位置当前目标的 owner。
    pub(crate) position_owner: Option<crate::core::identity::ScopeId>,
    /// 该位置当前目标是否存活。
    pub(crate) position_alive: bool,
    /// 观察时的下一个 `DataId` 序号。
    pub(crate) next_data_id: Option<u64>,
    /// 观察失败时的显式说明。
    pub(crate) observation_error: Option<String>,
}

thread_local! {
    static FINAL_BIND_PRE_CLEANUP: RefCell<Vec<FinalBindPreCleanupSnapshot>> =
        const { RefCell::new(Vec::new()) };
}

/// 记录一次最终绑定观察（独立通道；不进入共享事件序列）。
pub(crate) fn record_final_bind_pre_cleanup(snapshot: FinalBindPreCleanupSnapshot) {
    FINAL_BIND_PRE_CLEANUP.with(|slots| slots.borrow_mut().push(snapshot));
}

/// 取走已记录的最终绑定观察。
pub(crate) fn take_final_bind_pre_cleanup() -> Vec<FinalBindPreCleanupSnapshot> {
    FINAL_BIND_PRE_CLEANUP.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
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
    /// 原始 Scope 诊断。
    pub(crate) scope_error: Option<super::internal_error::ScopeError>,
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
    scope_error: Option<super::internal_error::ScopeError>,
) {
    SAVED_TERMINATIONS.with(|slots| {
        slots.borrow_mut().push(SavedTermination {
            kind,
            scope,
            note,
            scope_error,
        })
    });
}

/// 取走已记录的首次终止。
pub(crate) fn take_termination_saved() -> Vec<SavedTermination> {
    SAVED_TERMINATIONS.with(|slots| std::mem::take(&mut *slots.borrow_mut()))
}

// ---- 共享事件断言辅助（V21-07 的私有辅助提升，语义保持不变） ----

/// 业务事件是否出现。
pub(crate) fn saw(events: &[String], event: &str) -> bool {
    events.iter().any(|candidate| candidate == event)
}

/// 事件序列中是否出现带该前缀的事件（用于 `x:` 之类的事件族；`saw` 只做精确匹配）。
pub(crate) fn saw_prefix(events: &[String], prefix: &str) -> bool {
    events.iter().any(|event| event.starts_with(prefix))
}

/// 事件序列中第 `n` 次出现的位置；缺失即失败。
pub(crate) fn nth(events: &[String], needle: &str, n: usize) -> usize {
    events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.contains(needle))
        .map(|(index, _)| index)
        .nth(n)
        .unwrap_or_else(|| panic!("event `{needle}` #{n} missing: {events:?}"))
}

/// 事件序列中的位置；缺失即失败（不允许用 `None` 比较次序）。
pub(crate) fn at(events: &[String], needle: &str) -> usize {
    events
        .iter()
        .position(|event| event.contains(needle))
        .unwrap_or_else(|| panic!("event `{needle}` missing: {events:?}"))
}

/// 事件出现次数（精确匹配）。
pub(crate) fn count(events: &[String], event: &str) -> usize {
    events
        .iter()
        .filter(|candidate| *candidate == event)
        .count()
}

/// 每个样本在起点重置 gate／事件／观测记录。
pub(crate) fn reset_observations() {
    closed_scope_reset();
    root_snapshots_reset();
    take_events();
    take_shared_events();
    child_scope_reset();
    boundary_child_scope_reset();
    boundary_address_reset();
    boundary_creation_reset();
    match_failure_scope_snapshot();
    export_attempt_snapshot();
    take_export_conflict();
    take_round_collect_pre_cleanup();
    take_final_bind_pre_cleanup();
    take_export_pre_cleanup();
    take_generic_promote_probe();
    take_termination_saved();
    release_gate();
}

// ---- 线程局部挂起点 ----

thread_local! {
    static PENDING_GATE: RefCell<Option<Rc<Cell<bool>>>> = const { RefCell::new(None) };
}

/// 安装一个挂起点；异步业务体在 await 前调用 [`gate_wait`]。
pub(crate) fn install_gate() {
    PENDING_GATE.with(|gate| *gate.borrow_mut() = Some(Rc::new(Cell::new(false))));
}

/// 释放已安装的挂起点。
pub(crate) fn release_gate() {
    PENDING_GATE.with(|gate| {
        if let Some(open) = gate.borrow().as_ref() {
            open.set(true);
        }
    });
}

/// 在挂起点上等待：安装时先 Pending，释放后下一次 poll 返回。
pub(crate) async fn gate_wait() {
    let open = PENDING_GATE.with(|gate| gate.borrow().clone());
    if let Some(open) = open {
        std::future::poll_fn(|_| {
            if open.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
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

/// Root 阶段的 test-only 故障：只改变前置元数据，不改变生产判断路径。
#[derive(Debug)]
pub(crate) enum RootFault {
    /// 用给定列表替换本次 Root 的声明输出端口（可表达数量／类型／重复／未绑定位置）。
    ReplacePorts(Vec<super::signature::DeclaredPort>),
    /// 在真实声明输出端口列表的第 `index` 项之后复制一份（同 RefId 重复）。
    DuplicatePort(usize),
    /// 在真实声明输出端口列表末尾追加一个端口（数量不符）。
    AppendPort(super::signature::DeclaredPort),
    /// 登记输入前，在第 `index` 个声明输入位置上预占一个值（后项装配失败）。
    PrebindInput(usize),
    /// body 结束后、预检前，销毁 RootScope 第 `index` 个 owned entry（清理前提损坏）。
    DestroyRootOwned(usize),
    /// body 结束后、预检前，把 RootScope 第 `index` 个 owned 的责任移到一个已关闭 Scope。
    RelocateOwnedToClosedScope(usize),
    /// body 结束后、预检前，把第 `index` 个声明输出位置的目标改成指定 target。
    InjectOutputTarget {
        /// 声明输出端口下标。
        index: usize,
        /// 注入的 target。
        target: super::scope::RefTarget,
    },
    /// body 结束后、预检前，把第 `index` 个声明输出位置的目标改成第 `input` 个声明输入的
    /// 目标（构造"声明类型与目标实际类型不符"的反例）。
    RetargetOutputFromInput {
        /// 声明输出端口下标。
        index: usize,
        /// 声明输入下标。
        input: usize,
    },
}

thread_local! {
    static ROOT_FAULT: RefCell<Option<RootFault>> = const { RefCell::new(None) };
}

/// 安装一个 Root 阶段故障（在下一次匹配阶段消费）。
pub(crate) fn install_root_fault(fault: RootFault) {
    ROOT_FAULT.with(|slot| *slot.borrow_mut() = Some(fault));
}

fn take_root_fault_if(predicate: impl Fn(&RootFault) -> bool) -> Option<RootFault> {
    ROOT_FAULT.with(|slot| {
        if slot.borrow().as_ref().is_some_and(&predicate) {
            slot.borrow_mut().take()
        } else {
            None
        }
    })
}

/// 取出口径故障（端口列表类）。
pub(crate) fn take_root_ports_fault() -> Option<RootFault> {
    take_root_fault_if(|fault| {
        matches!(
            fault,
            RootFault::ReplacePorts(_) | RootFault::DuplicatePort(_) | RootFault::AppendPort(_)
        )
    })
}

/// 取出指定下标的输入装配故障（其它下标不消费）。
pub(crate) fn take_root_input_fault_at(index: usize) -> Option<RootFault> {
    take_root_fault_if(|fault| matches!(fault, RootFault::PrebindInput(at) if *at == index))
}

/// 取出 body 之后、预检之前的故障。
pub(crate) fn take_root_post_body_fault() -> Option<RootFault> {
    take_root_fault_if(|fault| {
        matches!(
            fault,
            RootFault::DestroyRootOwned(_)
                | RootFault::RelocateOwnedToClosedScope(_)
                | RootFault::InjectOutputTarget { .. }
                | RootFault::RetargetOutputFromInput { .. }
        )
    })
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

/// 推进一个已 boxed 的 Future 到 Ready；每次 Pending 先释放挂起点。
pub(crate) fn drive_pinned<F: Future + ?Sized>(mut boxed: Pin<Box<F>>) -> F::Output {
    let mut pending = 0usize;
    loop {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match boxed.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 64, "future is not making progress");
                release_gate();
            }
        }
    }
}

/// 推进到第 `stops` 次 Pending 后停下，返回仍持有 Future 本体的 Box。
///
/// 丢弃这个 Box 才是"丢弃 Future 本体"；只丢一个 `Pin<&mut F>` 或引用不算。
pub(crate) fn advance_to_pending<F: Future>(future: F, stops: usize) -> Pin<Box<F>> {
    let mut boxed = Box::pin(future);
    for stop in 1..=stops {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match boxed.as_mut().poll(&mut cx) {
            Poll::Pending => {}
            Poll::Ready(_) => panic!("future completed before pending stop {stop}"),
        }
    }
    boxed
}

// ---- child Scope 观测（窄接口） ----

thread_local! {
    static CHILD_SCOPES: RefCell<Vec<ScopeId>> = const { RefCell::new(Vec::new()) };
}

/// 清空本次样本记录的 child Scope。
pub(crate) fn child_scope_reset() {
    CHILD_SCOPES.with(|scopes| scopes.borrow_mut().clear());
}

/// 记录一个被观察到的直接 child Scope（由样本中的编排体调用）。
pub(crate) fn child_scope_record(child: ScopeId) {
    CHILD_SCOPES.with(|scopes| scopes.borrow_mut().push(child));
}

/// 当前记录的 child Scope 快照（按记录顺序）。
pub(crate) fn child_scope_snapshot() -> Vec<ScopeId> {
    CHILD_SCOPES.with(|scopes| scopes.borrow().clone())
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

/// 由真实调用边界记录的 child Scope 快照（按建立顺序）。
pub(crate) fn boundary_child_scope_snapshot() -> Vec<ScopeId> {
    BOUNDARY_CHILD_SCOPES.with(|scopes| scopes.borrow().clone())
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

/// 真实调用边界在整组 Export 预检失败时记录 child 的本地绑定与责任集合（只读元数据）。
pub(crate) fn export_attempt_record(
    child: ScopeId,
    refs: Vec<(RefId, crate::core::identity::DataId)>,
    owned: Vec<crate::core::identity::DataId>,
) {
    EXPORT_ATTEMPTS.with(|entries| entries.borrow_mut().push((child, refs, owned)));
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

/// 真实 Match body 在判定"无匹配且无 default"、尚未返回执行错误时记录自身 Scope 快照。
///
/// 取点必须在受控清理之前：`Closed` 之后的空快照不能证明"运行中没有自行绑定输出"。
pub(crate) fn match_failure_scope_record(
    child: ScopeId,
    refs: Vec<(RefId, crate::core::identity::DataId)>,
    owned: Vec<crate::core::identity::DataId>,
) {
    MATCH_FAILURE_SCOPES.with(|entries| entries.borrow_mut().push((child, refs, owned)));
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

/// 让下一次真实 Match 调用在运行 branch 之前预占其共同端口 `index`（构造后项冲突）。
pub(crate) fn install_export_conflict(index: usize) {
    EXPORT_CONFLICT.with(|slot| slot.set(Some(index)));
}

/// 取出（一次性）预占开关。
pub(crate) fn take_export_conflict() -> Option<usize> {
    EXPORT_CONFLICT.with(|slot| slot.take())
}

// ---- Match body 阶段快照（cfg(test) 只读；M17／M18 的两侧原子性证据） ----

thread_local! {
    #[allow(clippy::type_complexity)]
    static MATCH_STAGES: RefCell<Vec<(&'static str, ScopeId, Vec<(RefId, crate::core::identity::DataId)>, Vec<crate::core::identity::DataId>)>> =
        const { RefCell::new(Vec::new()) };
}

/// 在真实 Match body 内、受控清理之前记录自身 Scope 的完整绑定与责任快照。
///
/// 阶段名由 Match body 提供（提交前／被选 branch 失败后／body 成功）：记录点都在真实调用
/// 路径内，不复制 Export 算法，也不改变清理顺序。
pub(crate) fn match_stage_record(stage: &'static str, child: ScopeId, ctx: &ExecutionContext) {
    if let Ok((refs, owned)) = ctx.snapshot_probe(&child) {
        MATCH_STAGES.with(|entries| entries.borrow_mut().push((stage, child, refs, owned)));
    }
}

/// 取走 Match body 阶段快照（按记录顺序）。
#[allow(clippy::type_complexity)]
pub(crate) fn match_stage_snapshot() -> Vec<(
    &'static str,
    ScopeId,
    Vec<(RefId, crate::core::identity::DataId)>,
    Vec<crate::core::identity::DataId>,
)> {
    MATCH_STAGES.with(|entries| std::mem::take(&mut *entries.borrow_mut()))
}

// ---- Root 驱动（按 &Definition 参数化，Flow 侧只做薄委托） ----

/// Root 关闭前的只读观察视图（含受控登记，供 DataId 序号与责任检查使用）。
///
/// 与 Definition 一起参数化：V21-06 的 Flow 夹具与 V21-07 的 Match 主场景共用同一驱动，
/// 不按 Flow 类型分叉，也不另建第二份 Root 观察设施。
pub(crate) struct RootView<'a, 'ctx> {
    /// guard 与 Root Scope 由创建它的驱动提供；字段对本 crate 的样本可见，不提供生产入口。
    pub(crate) guard: &'a mut super::context::InvocationGuard<'ctx>,
    pub(crate) root: ScopeId,
}

impl RootView<'_, '_> {
    /// 本次 Root Scope。
    pub(crate) fn root(&self) -> &ScopeId {
        &self.root
    }

    /// 只读 Context 视图：快照、存活／owner 诊断与 frame 状态。
    pub(crate) fn probe(&self) -> &ExecutionContext {
        self.guard
    }

    /// 从 Root Scope 的本地位置解析只读借用。
    pub(crate) fn resolve<T: 'static>(&self, position: &RefId) -> Result<&T, BodyError> {
        Ok(self.guard.resolve::<T>(&self.root, position)?)
    }

    /// 把一个业务值登记到 Root Scope 的指定位置（测试夹具，不代表生产注入入口）。
    pub(crate) fn register<T: 'static>(
        &mut self,
        position: &RefId,
        value: T,
    ) -> Result<super::identity::DataId, BodyError> {
        Ok(self.guard.register_owned(&self.root, position, value)?)
    }

    /// 某个 Scope 的当前状态。
    pub(crate) fn state(&self, scope: &ScopeId) -> Result<super::scope::ScopeState, BodyError> {
        Ok(self.guard.state(scope)?)
    }

    /// Root Scope 是否仍可接受新业务操作。
    pub(crate) fn root_is_active(&self) -> bool {
        matches!(self.state(&self.root), Ok(super::scope::ScopeState::Active))
    }

    /// Root Scope 的 `(位置, DataId)` 只读快照。
    pub(crate) fn snapshot(&self) -> Result<Vec<(RefId, super::identity::DataId)>, BodyError> {
        let (refs, _) = self.guard.snapshot_probe(&self.root)?;
        Ok(refs)
    }

    /// Root Scope 的完整只读快照：本地引用绑定与责任集合。
    #[allow(clippy::type_complexity)]
    pub(crate) fn snapshot_full(
        &self,
    ) -> Result<
        (
            Vec<(RefId, super::identity::DataId)>,
            Vec<super::identity::DataId>,
        ),
        BodyError,
    > {
        Ok(self.guard.snapshot_probe(&self.root)?)
    }

    /// 快照里某个位置当前解析到的 `DataId`。
    pub(crate) fn data_id_of(
        &self,
        position: &RefId,
    ) -> Result<super::identity::DataId, BodyError> {
        self.snapshot()?
            .into_iter()
            .find(|(candidate, _)| candidate == position)
            .map(|(_, id)| id)
            .ok_or_else(|| BodyError::new("position is not bound"))
    }
}

/// 在 Root 中执行一个 Definition 的异步主体：登记输入 → 预备钩子 → 进入 Root frame →
/// 顺序主体 → 关闭前观察 → **固定空声明输出／空 ExportSlot 收口**。
///
/// 收口不代表 Root Output 移交：不 take、不校验声明输出的提取资格（类型／可移交 owner／
/// 重复 DataId）、不向 Application 转交 Data、也不为移交预先解除 Root owned 责任。
pub(crate) async fn definition_in_root<P, F>(
    definition: &super::builder::Definition,
    inputs: Vec<RootInput>,
    prepare: P,
    observe: Option<F>,
) -> Result<(), BodyError>
where
    P: FnOnce(&mut ExecutionContext, &ScopeId),
    F: FnOnce(&mut RootView<'_, '_>) -> Result<(), BodyError>,
{
    let mut execution = super::runtime::RootExecution::start();
    let root = execution.context().root_scope();
    for (position, register) in inputs {
        register(execution.context_mut(), &position);
    }
    prepare(execution.context_mut(), &root);
    let observe = RefCell::new(observe);
    let mut guard = execution
        .context_mut()
        .enter(super::context::InvocationKind::Root, &root, true)
        .expect("fresh execution accepts a root frame");
    match super::builder::run_definition(&mut guard, definition).await {
        Ok(()) => {
            let observed = match observe.borrow_mut().take() {
                Some(observer) => {
                    let mut view = RootView {
                        guard: &mut guard,
                        root: root.clone(),
                    };
                    observer(&mut view)
                }
                None => Ok(()),
            };
            match observed {
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
        }
        Err(error) => {
            guard.failed_with(&error);
            Err(error)
        }
    }
}

/// 在 Root 中执行一个 Definition，可携带预备钩子（例如预占 caller 输出位置）。
pub(crate) fn run_definition_prepared<P>(
    definition: &super::builder::Definition,
    inputs: Vec<RootInput>,
    prepare: P,
) -> Result<(), BodyError>
where
    P: FnOnce(&mut ExecutionContext, &ScopeId),
{
    drive(definition_in_root::<
        P,
        fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
    >(definition, inputs, prepare, None))
}

/// 在 Root 中执行一个 Definition，关闭前观察。
pub(crate) fn run_definition_in_root<F>(
    definition: &super::builder::Definition,
    inputs: Vec<RootInput>,
    observe: F,
) -> Result<(), BodyError>
where
    F: FnOnce(&mut RootView<'_, '_>) -> Result<(), BodyError>,
{
    drive(definition_in_root(
        definition,
        inputs,
        |_, _| {},
        Some(observe),
    ))
}

/// 在 Root 中执行一个 Definition，不做额外观察。
pub(crate) fn run_definition_plain(
    definition: &super::builder::Definition,
    inputs: Vec<RootInput>,
) -> Result<(), BodyError> {
    run_definition_prepared(definition, inputs, |_, _| {})
}

// ---- Root 输入登记 ----

/// 一个 Root 输入：声明位置 + 把值登记到该位置。
pub(crate) type RootInput = (RefId, Box<dyn FnOnce(&mut ExecutionContext, &RefId)>);

/// 把 `value` 登记到声明输入位置的便捷构造。
///
/// 只代表测试驱动中的 Application 交接：经真实 `register_owned` 进入 RootScope 的
/// 声明输入位置，不提供生产 Data 注入入口。
pub(crate) fn root_input<T: 'static>(
    position: &super::data_ref::DataRef<T>,
    value: T,
) -> RootInput {
    let position = position.position().clone();
    (
        position,
        Box::new(move |ctx: &mut ExecutionContext, position: &RefId| {
            ctx.register_owned(&ctx.root_scope(), position, value)
                .expect("root input");
        }),
    )
}
