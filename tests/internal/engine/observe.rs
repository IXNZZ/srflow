//! WE-T02／WE-T03 执行链的 cfg(test) 定向观察通道。
//!
//! 只记录元数据与物理身份（Scope／RefId／DataId／状态），不携带业务值，也不改变任何
//! 执行路径；生产构建不包含本模块。既有 Core 观察通道（`test_support::boundary_creation_record`、
//! `closed_scope_snapshot`、`root_snapshot`、`take_log_snapshot` 等）继续复用，这里只补
//! 新引擎特有的 Import 批次、Export 快照、frame 进出与 Scope 状态观察点。
#![cfg(test)]

use crate::core::identity::{DataId, ScopeId};
use crate::core::internal_error::ScopeError;
use crate::core::ref_id::RefId;
use crate::core::scope::{ScopeState, TargetSnapshot};
use std::cell::RefCell;

/// 一次 Chain 的整组 Import 记录。
#[derive(Debug, Clone)]
pub(crate) struct ChainImportRecord {
    /// 建立的真实 child Scope。
    pub(crate) child: ScopeId,
    /// 调用方（caller）Scope。
    pub(crate) caller: ScopeId,
    /// 每个槽位的（来源 RefId, 目的 RefId）；同静态 RefId 策略下两者相同。
    pub(crate) pairs: Vec<(RefId, RefId)>,
}

/// Export 前后的 caller 侧快照（只读）。
#[derive(Debug, Clone)]
pub(crate) struct ExportSnapshotRecord {
    /// 观察点标签（`before`／`rejected`／`committed`）。
    pub(crate) phase: &'static str,
    /// 被观察的 Scope（caller）。
    pub(crate) scope: ScopeId,
    /// 本地绑定（位置 → target）。
    pub(crate) refs: Vec<(RefId, TargetSnapshot)>,
    /// owned 集合。
    pub(crate) owned: Vec<DataId>,
    /// owned 中每个实例的存活与 owner。
    pub(crate) owned_alive: Vec<(DataId, bool, Option<ScopeId>)>,
    /// 下一个将分配的 DataId 序号（分配是否推进的观察点）。
    pub(crate) next_data_id: Option<u64>,
}

/// Scope 状态观察点。
#[derive(Debug, Clone)]
pub(crate) struct ScopeStateRecord {
    /// 观察标签。
    pub(crate) at: &'static str,
    /// 被观察的 Scope。
    pub(crate) scope: ScopeId,
    /// 当时的真实状态。
    pub(crate) state: ScopeState,
}

/// frame 进入／退出观察点。
#[derive(Debug, Clone)]
pub(crate) struct FrameRecord {
    /// frame 类别（`root`／`boundary`／`leaf`）。
    pub(crate) kind: &'static str,
    /// 观察阶段（`enter`／`exit`）。
    pub(crate) phase: &'static str,
    /// 该 frame 使用的 Scope。
    pub(crate) scope: ScopeId,
}

/// 已关闭 Scope 拒绝再次进入的观察记录。
#[derive(Debug, Clone)]
pub(crate) struct ScopeRejectionRecord {
    /// 被拒绝的 Scope。
    pub(crate) scope: ScopeId,
    /// 被尝试的操作。
    pub(crate) operation: &'static str,
    /// 真实拒绝诊断。
    pub(crate) error: ScopeError,
}

/// Chain 创建观察点：同时记录创建时已关闭的 Scope 集合（顺序链的“先关闭再创建”证据）。
#[derive(Debug, Clone)]
pub(crate) struct ChainCreationRecord {
    /// 新建的 child。
    pub(crate) child: ScopeId,
    /// 其 caller。
    pub(crate) caller: ScopeId,
    /// 创建时已经关闭的 Scope（含本次执行中更早的 child）。
    pub(crate) closed_before: Vec<ScopeId>,
}

/// Boundary Control 退出前的责任解除观察点。
#[derive(Debug, Clone)]
pub(crate) struct ControlReleaseRecord {
    /// 退出的 child。
    pub(crate) child: ScopeId,
    /// 完成 frame 前责任集合是否已清空。
    pub(crate) responsible_empty: bool,
    /// 本层解除的责任数。
    pub(crate) released: usize,
}

/// Boundary Control 完成后的 Context 状态观察点。
#[derive(Debug, Clone)]
pub(crate) struct ControlCompletionRecord {
    /// 已退出的 child。
    pub(crate) child: ScopeId,
    /// 完成时 Context 是否已被标记不可恢复终止（合法 Control 传播应为 `false`）。
    pub(crate) terminated: bool,
    /// 首次终止的定位 Scope（如有）。
    pub(crate) termination_scope: Option<ScopeId>,
    /// 完成后的 child 状态。
    pub(crate) child_state: Option<ScopeState>,
}

/// 跨执行身份访问观察点（第二域 Context 中访问第一域的身份）。
#[derive(Debug, Clone)]
pub(crate) struct ForeignAccessRecord {
    /// 被尝试的操作。
    pub(crate) operation: &'static str,
    /// 是否被拒绝。
    pub(crate) rejected: bool,
    /// 原框架拒绝诊断。
    pub(crate) detail: String,
    /// 访问前后本域绑定数与责任数（不变即无副作用）。
    pub(crate) local_refs_before: usize,
    pub(crate) local_refs_after: usize,
    pub(crate) local_owned_before: usize,
    pub(crate) local_owned_after: usize,
}

/// Choose 选择结果与分支边界观察点。
#[derive(Debug, Clone)]
pub(crate) struct ChooseSelectionRecord {
    /// 执行选择的 caller Scope。
    pub(crate) caller: ScopeId,
    /// 命中的 case 下标；`None` 表示未命中任何 case。
    pub(crate) selected: Option<u16>,
    /// 是否改由 otherwise 分支执行。
    pub(crate) otherwise_used: bool,
    /// 选中的分支边界；无匹配且无 otherwise 时为 `None`（未建立任何 running Scope）。
    pub(crate) child: Option<ScopeId>,
}

thread_local! {
    static CHAIN_IMPORTS: RefCell<Vec<ChainImportRecord>> = const { RefCell::new(Vec::new()) };
    static EXPORT_SNAPSHOTS: RefCell<Vec<ExportSnapshotRecord>> = const { RefCell::new(Vec::new()) };
    static SCOPE_STATES: RefCell<Vec<ScopeStateRecord>> = const { RefCell::new(Vec::new()) };
    static FRAMES: RefCell<Vec<FrameRecord>> = const { RefCell::new(Vec::new()) };
    static SCOPE_REJECTIONS: RefCell<Vec<ScopeRejectionRecord>> = const { RefCell::new(Vec::new()) };
    static CHAIN_CREATIONS: RefCell<Vec<ChainCreationRecord>> = const { RefCell::new(Vec::new()) };
    static CONTROL_RELEASES: RefCell<Vec<ControlReleaseRecord>> = const { RefCell::new(Vec::new()) };
    static CONTROL_COMPLETIONS: RefCell<Vec<ControlCompletionRecord>> = const { RefCell::new(Vec::new()) };
    static FOREIGN_ACCESSES: RefCell<Vec<ForeignAccessRecord>> = const { RefCell::new(Vec::new()) };
    static CHOOSE_SELECTIONS: RefCell<Vec<ChooseSelectionRecord>> = const { RefCell::new(Vec::new()) };
}

/// 记录一次 Choose 选择结果。
pub(crate) fn record_choose_selection(record: ChooseSelectionRecord) {
    CHOOSE_SELECTIONS.with(|records| records.borrow_mut().push(record));
}

/// 取出全部 Choose 选择记录。
pub(crate) fn take_choose_selections() -> Vec<ChooseSelectionRecord> {
    CHOOSE_SELECTIONS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一次 Chain 创建。
pub(crate) fn record_chain_creation(record: ChainCreationRecord) {
    CHAIN_CREATIONS.with(|records| records.borrow_mut().push(record));
}

/// 取出全部 Chain 创建记录。
pub(crate) fn take_chain_creations() -> Vec<ChainCreationRecord> {
    CHAIN_CREATIONS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一次 Control 退出的责任解除。
pub(crate) fn record_control_release(record: ControlReleaseRecord) {
    CONTROL_RELEASES.with(|records| records.borrow_mut().push(record));
}

/// 取出全部 Control 责任解除记录。
pub(crate) fn take_control_releases() -> Vec<ControlReleaseRecord> {
    CONTROL_RELEASES.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一次 Control 完成后的 Context 状态。
pub(crate) fn record_control_completion(record: ControlCompletionRecord) {
    CONTROL_COMPLETIONS.with(|records| records.borrow_mut().push(record));
}

/// 取出全部 Control 完成记录。
pub(crate) fn take_control_completions() -> Vec<ControlCompletionRecord> {
    CONTROL_COMPLETIONS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一次跨执行身份访问。
pub(crate) fn record_foreign_access(record: ForeignAccessRecord) {
    FOREIGN_ACCESSES.with(|records| records.borrow_mut().push(record));
}

/// 取出全部跨执行身份访问记录。
pub(crate) fn take_foreign_accesses() -> Vec<ForeignAccessRecord> {
    FOREIGN_ACCESSES.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一次 Chain Import 批次。
pub(crate) fn record_chain_import(record: ChainImportRecord) {
    CHAIN_IMPORTS.with(|records| records.borrow_mut().push(record));
}

/// 取出全部 Chain Import 记录。
pub(crate) fn take_chain_imports() -> Vec<ChainImportRecord> {
    CHAIN_IMPORTS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一个 Export 快照。
pub(crate) fn record_export_snapshot(record: ExportSnapshotRecord) {
    EXPORT_SNAPSHOTS.with(|records| records.borrow_mut().push(record));
}

/// 取出全部 Export 快照。
pub(crate) fn take_export_snapshots() -> Vec<ExportSnapshotRecord> {
    EXPORT_SNAPSHOTS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一个 Scope 状态观察点。
pub(crate) fn record_scope_state(record: ScopeStateRecord) {
    SCOPE_STATES.with(|records| records.borrow_mut().push(record));
}

/// 取出全部 Scope 状态观察点。
pub(crate) fn take_scope_states() -> Vec<ScopeStateRecord> {
    SCOPE_STATES.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一个 frame 观察点。
pub(crate) fn record_frame(record: FrameRecord) {
    FRAMES.with(|records| records.borrow_mut().push(record));
}

/// 取出全部 frame 观察点。
pub(crate) fn take_frames() -> Vec<FrameRecord> {
    FRAMES.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一个已关闭 Scope 的拒绝。
pub(crate) fn record_scope_rejection(record: ScopeRejectionRecord) {
    SCOPE_REJECTIONS.with(|records| records.borrow_mut().push(record));
}

/// 取出全部已关闭 Scope 拒绝记录。
pub(crate) fn take_scope_rejections() -> Vec<ScopeRejectionRecord> {
    SCOPE_REJECTIONS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// Retry／Iter 控制器与 Round 的定向观察点（WE-T04）。
#[derive(Debug, Clone)]
pub(crate) struct ControllerRoundRecord {
    /// 控制器类别：`retry`／`iter`。
    pub(crate) kind: &'static str,
    /// 观察标签（`controller-created`／`round-created`／`round-normal`／`round-caught`／
    /// `round-propagated`／`round-failed`／`round-promoted`／`budget-exhausted`／`delivered`…）。
    pub(crate) at: &'static str,
    /// 调用方 Scope。
    pub(crate) caller: ScopeId,
    /// 控制器 Scope。
    pub(crate) controller: ScopeId,
    /// 对应的 Round Scope（若有）。
    pub(crate) round: Option<ScopeId>,
    /// 第几次 attempt／round（从 1 起）。
    pub(crate) index: usize,
}

thread_local! {
    static CONTROLLER_ROUNDS: std::cell::RefCell<Vec<ControllerRoundRecord>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static CONTROLLER_DELIVERIES: std::cell::RefCell<Vec<ControllerDeliveryRecord>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// 一次控制器两跳交付的事实：来源 Scope → 目的 Scope 的（来源 RefId, 目的 RefId）。
#[derive(Debug, Clone)]
pub(crate) struct ControllerDeliveryRecord {
    /// 控制器类别：`retry`／`iter`。
    pub(crate) kind: &'static str,
    /// 跳数标签（`round-to-controller`／`controller-to-caller`）。
    pub(crate) hop: &'static str,
    /// 来源 Scope。
    pub(crate) from: ScopeId,
    /// 目的 Scope。
    pub(crate) to: ScopeId,
    /// 逐槽（来源位置, 目的位置）。
    pub(crate) pairs: Vec<(RefId, RefId)>,
}

/// 记录一个控制器／Round 观察点。
pub(crate) fn record_controller_round(record: ControllerRoundRecord) {
    CONTROLLER_ROUNDS.with(|records| records.borrow_mut().push(record));
}

/// 取出全部控制器／Round 观察点。
pub(crate) fn take_controller_rounds() -> Vec<ControllerRoundRecord> {
    CONTROLLER_ROUNDS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 记录一次控制器两跳交付。
pub(crate) fn record_controller_delivery(record: ControllerDeliveryRecord) {
    CONTROLLER_DELIVERIES.with(|records| records.borrow_mut().push(record));
}

/// 取出全部控制器交付记录。
pub(crate) fn take_controller_deliveries() -> Vec<ControllerDeliveryRecord> {
    CONTROLLER_DELIVERIES.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

/// 复位全部本模块观察点。
pub(crate) fn reset() {
    let _ = take_controller_rounds();
    let _ = take_controller_deliveries();
    let _ = take_choose_selections();
    let _ = take_chain_imports();
    let _ = take_export_snapshots();
    let _ = take_scope_states();
    let _ = take_frames();
    let _ = take_scope_rejections();
    let _ = take_chain_creations();
    let _ = take_control_releases();
    let _ = take_control_completions();
    let _ = take_foreign_accesses();
}
