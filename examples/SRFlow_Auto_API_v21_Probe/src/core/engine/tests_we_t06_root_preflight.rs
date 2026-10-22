//! WE-T06 复审 R01：Root 预检判据的正式回归样本（模块已注册，普通 `cargo test` 驱动）。
//!
//! 目标（对应 `docs/workflow_engine_tasks/WE_T06_REVIEW_v0.1.md` §2.3 必需修订）：把被删除的
//! 两条旧样本的真实断言以当前 Core／新 Runtime 正式入口重新落到正式套件，替换 ledger 中不等价
//! 的 Consume／Definition／普通 Failure 承接。R02 追加承接 R1 复审的 F01（Root 失败终止类别）
//! 与 F02 §3.1（Root 第二输入登记失败）。
//!
//! 覆盖：
//! - 样本 A（`r01_root_item_target_preflight_rejects_in_both_cap_states`）：Root owned 输出预检
//!   对 **CollectionItem target** 的完整 Data 拒绝在两态（cap 存活／cap 已关闭）都精确返回
//!   `NonCompleteTarget{position}`，不依赖 cap 状态；仍冻结、原根数据存活、take 零。附正向对照：
//!   同一位置绑定完整 Data 时预检接受（拒绝确实只针对 CollectionItem）。
//! - 样本 B（`r01_root_late_duplicate_keeps_primary_and_cleanup_distinct`）：正式 `Runtime::execute`
//!   的 Root **后项重复实际 DataId** 预检首错叠加真实剩余 owned 清理故障——`Error::source` 保留
//!   实际 `DuplicateRootDataId`（两个声明位置不同、实例仍由 Root 负责），`cleanup_source` 保留
//!   Root 首条 owned 清理诊断，stored-first 为真、零 take、两通道内容不同，互不覆盖。
//!   附对照（`r01_root_late_duplicate_without_cleanup_fault_keeps_primary`）：无清理故障时首错
//!   仍是实际 `DuplicateRootDataId` 且 `cleanup_source` 为 `None`；并补真实 Root 终止记录：
//!   唯一首终止、具体 Root Scope、`TerminationKind::BodyError`（F01；旧 k20 的 Root 出口承接，
//!   `guard.failed(...)` 的单行退化会被该断言捕获）。
//! - Root 输入登记失败（`r02_root_second_input_registration_failure_runs_no_body_and_no_take`）：
//!   正式 `Runtime::execute` 的 `register_owned_erased` 失败提前返回分支——第二输入声明位置冲突
//!   → 精确 `RefAlreadyBound`；只有第一输入被接管、业务零调用、两份输入各 Drop 一次、尚未进入
//!   Root frame／提取（零 take）（F02 §3.1；旧 k05 的正式入口承接，前提由 test-only
//!   `runtime::install_root_input_prebind_fault` 构造）。
//! - Root 预检拒绝族（`r01_root_preflight_*`）：在正式 `ScopeCoordinator::prepare_root_extraction`
//!   上承接旧 k13／k14／k16 的精确判据——`TypeMismatch`、`RefNotBound`、`ForeignExecution`
//!   （`Storage`）、`Invariant`（清理前提）、`IllegalOwner`（非 Root 所有者）；每条都断言精确
//!   变体与位置、仍冻结、原根数据存活／实例归属不变、take 零。内部前提由既有 cfg(test) 钩子或
//!   最小 `relocate_owned_probe` 构造，并在样本注释中显式声明「自然业务不会产生」。
//! - Export 入口（`r01_chain_export_group_rejects_alias_plus_new_owned_without_partial_binding`）：
//!   全程 Chain 整组 Export 含「祖先 alias 首项 ＋ 新 owned 后项」，caller 后项位置被预占 → 整组
//!   在任何提交前拒绝、无部分绑定／责任转移、零 take（旧 f20 的正式承接；`m18` 型由
//!   `tests_runtime.rs::c19_export_preflight_rejects_the_whole_choose_group` 同入口承接）。
//!
//! 归属：本文件由 WE-T06 R01 修订的 src 写者独占；不依赖审查目录或临时追加模块。复用既有
//! `#[cfg(test)]` 钩子（`runtime::install_root_cleanup_faults`、`boundary::install_export_occupy_fault`、
//! `test_support` 只读观察、`scope::ScopeCoordinator` 的正式预检与只读／前提构造 probe），并新增
//! 一个最小 `#[cfg(test)]` 前提构造 `scope::ScopeCoordinator::relocate_owned_probe`（镜像已删除的
//! `RelocateOwnedToClosedScope`）；不新增公开面、不新增判定路径。

use crate::core::context::TerminationKind;
use crate::core::engine::boundary::install_export_occupy_fault;
use crate::core::engine::data::Data;
use crate::core::engine::error::{BodyError, FrameworkDiagnostic, RunError};
use crate::core::engine::observe;
use crate::core::engine::runtime::{
    Runtime, install_root_cleanup_faults, install_root_input_prebind_fault,
};
use crate::core::engine::shape::Query;
use crate::core::identity::{DataId, DataIdAllocator, ExecutionIdentity, ScopeId};
use crate::core::internal_error::{InternalError, ScopeError};
use crate::core::ref_id::{RefIdAllocator, RefIdSource};
use crate::core::scope::{CollectionRoot, ItemAccess, RefTarget, ScopeCoordinator, ScopeState};
use crate::core::signature::DeclaredPort;
use crate::core::test_support as ts;
use futures::executor::block_on;
use std::any::{TypeId, type_name};
use std::cell::Cell;
use std::error::Error;
use std::rc::Rc;

/// 真实 Root owned 清理故障的诊断文本（`destroy_probe` 破坏一个 owned entry 的自然结果）。
const NATURAL_OWNED_CLEANUP: &str = "owned entry must exist until its scope closes";

// ================================================================ 样本 A：Root 预检对 CollectionItem target 的两态拒绝

/// 在真实 `ScopeCoordinator` 上建立「Root owned 输出位置是 CollectionItem target」的前提，并调用
/// 正式 `prepare_root_extraction`。`closed_cap` 决定 lifetime cap 是否已被关闭。
///
/// 两态都必须返回精确 `NonCompleteTarget{position}`：完整 Data 判据优先，cap 状态不改变拒绝类别。
fn root_item_target_rejected(closed_cap: bool) {
    let label = if closed_cap { "closed cap" } else { "live cap" };
    ts::reset_observations();
    ts::take_log_reset();

    let mut coordinator = ScopeCoordinator::new(ExecutionIdentity::new());
    let root = coordinator.root();

    // 本域真实 Vec Data 的根数据登记在 Root；输出位置注入指向其元素 0 的受限 item 借用。
    let refs = RefIdAllocator::new(RefIdSource::new());
    let collection_position = refs.allocate().unwrap();
    let output_position = refs.allocate().unwrap();
    let data = coordinator
        .register_owned(&root, &collection_position, vec![7u32])
        .unwrap();

    let cap = if closed_cap {
        // 已关闭的 child Scope 作为 lifetime cap：cap 失效，但完整 Data 判据仍应先拒绝。
        let cap = coordinator.create_child(&root).unwrap();
        coordinator.abort(&cap).unwrap();
        cap
    } else {
        root.clone()
    };
    assert_eq!(
        coordinator.state(&cap).unwrap(),
        if closed_cap {
            ScopeState::Closed
        } else {
            ScopeState::Active
        },
        "{label}"
    );

    coordinator.inject_target_probe(
        &root,
        &output_position,
        RefTarget::CollectionItem {
            collection: CollectionRoot::Data(data.clone()),
            index: 0,
            lifetime_cap: cap,
            access: ItemAccess::for_collection::<u32>(),
        },
    );
    coordinator.freeze_root(&root).unwrap();
    assert_eq!(
        coordinator.state(&root).unwrap(),
        ScopeState::Finalizing,
        "{label}"
    );

    let port = DeclaredPort::with_type(
        output_position.clone(),
        TypeId::of::<u32>(),
        type_name::<u32>(),
    );
    let error = coordinator
        .prepare_root_extraction(&root, &[port])
        .expect_err("a CollectionItem target is never a complete Root owned Data");
    assert!(
        matches!(&error, ScopeError::NonCompleteTarget { position } if position == &output_position),
        "{label}: expected NonCompleteTarget at the declared position, got {error:?}"
    );
    // 拒绝只读、不改状态：仍冻结、原根数据存活、没有任何 take。
    assert_eq!(
        coordinator.state(&root).unwrap(),
        ScopeState::Finalizing,
        "{label}"
    );
    assert!(
        coordinator.alive_probe(&data),
        "{label}: the original root data must survive"
    );
    assert!(
        ts::take_log_snapshot().is_empty(),
        "{label}: a preflight rejection takes nothing: {:?}",
        ts::take_log_snapshot()
    );

    coordinator.abort(&root).unwrap();
}

/// 正向对照：同一位置绑定**完整 Data** 时，同一正式预检入口接受（证明上面的拒绝确实针对
/// CollectionItem，而不是任意输入都会失败）。
fn complete_data_target_is_accepted() {
    ts::reset_observations();
    ts::take_log_reset();

    let mut coordinator = ScopeCoordinator::new(ExecutionIdentity::new());
    let root = coordinator.root();
    let refs = RefIdAllocator::new(RefIdSource::new());
    let collection_position = refs.allocate().unwrap();
    let output_position = refs.allocate().unwrap();
    let data = coordinator
        .register_owned(&root, &collection_position, vec![7u32])
        .unwrap();
    coordinator.inject_target_probe(&root, &output_position, RefTarget::Data(data.clone()));
    coordinator.freeze_root(&root).unwrap();

    let port = DeclaredPort::with_type(
        output_position.clone(),
        TypeId::of::<Vec<u32>>(),
        type_name::<Vec<u32>>(),
    );
    let plan = coordinator
        .prepare_root_extraction(&root, &[port])
        .expect("a complete Data target is accepted by the same preflight entry");
    assert_eq!(
        plan.take_count(),
        1,
        "the complete Data target is planned for extraction"
    );
    assert_eq!(coordinator.state(&root).unwrap(), ScopeState::Finalizing);
    coordinator.abort(&root).unwrap();
}

/// 样本 A：Root 预检对 CollectionItem target 的完整 Data 拒绝在 cap 存活与 cap 已关闭两态都精确成立。
#[test]
fn r01_root_item_target_preflight_rejects_in_both_cap_states() {
    root_item_target_rejected(false);
    root_item_target_rejected(true);
    complete_data_target_is_accepted();
    ts::reset_observations();
}

// ================================================================ 样本 B：Root 后项重复 DataId ＋ 真实剩余 owned 清理故障

/// 业务 Node：读一个 Root 输入并产生一个新的 owned 值（Root 输出 alias 的真实实例来源）。
async fn increment(q: Query<&u32>) -> Result<u32, BodyError> {
    Ok(*q.get() + 1)
}

/// 正式 `Runtime::execute`：Chain 的两个声明出口是不同逻辑位置、合法地指向同一实际 DataId；
/// 第一个未使用的 Root 输入被既有 cleanup fault 销毁，形成真实剩余 owned 清理故障。
fn run_root_late_duplicate() -> RunError {
    block_on(Runtime::new().execute(
        |flow, (_unused, live)| {
            flow.chain(|sub| {
                let produced = sub.then(increment, live);
                (produced, produced)
            })
        },
        (42u32, 3u32),
    ))
    .expect_err("two Root outputs resolving to one DataId must be rejected at extraction")
}

/// 从稳定的 `ScopeError` Display 文本取回重复身份三元组。
///
/// 格式为 `DuplicateRootDataId` 的正式 Display：`root outputs {first} and {duplicate} resolve to the same {data}`。
fn parse_duplicate(text: &str) -> (String, String, String) {
    let rest = text
        .strip_prefix("root outputs ")
        .unwrap_or_else(|| panic!("unexpected DuplicateRootDataId display: {text}"));
    let (first, rest) = rest
        .split_once(" and ")
        .unwrap_or_else(|| panic!("missing first/duplicate separator: {text}"));
    let (duplicate, data) = rest
        .split_once(" resolve to the same ")
        .unwrap_or_else(|| panic!("missing resolve clause: {text}"));
    (first.to_string(), duplicate.to_string(), data.to_string())
}

/// 取回正式 `Runtime::execute` 的预检首错通道（`Error::source`），并证明它是实际 `DuplicateRootDataId`。
fn primary_duplicate(error: &RunError) -> (String, String, String) {
    let RunError::Runtime(runtime_error) = error else {
        panic!("a Root preflight rejection is Runtime: {error}");
    };
    let source = Error::source(runtime_error).expect("the framework primary survives in source");
    let primary = source
        .downcast_ref::<FrameworkDiagnostic>()
        .expect("the primary channel is the framework diagnostic");
    assert!(
        format!("{primary:?}")
            .starts_with("FrameworkDiagnostic { scope_error: DuplicateRootDataId {"),
        "the primary is the framework DuplicateRootDataId: {primary:?}"
    );
    let (first_ref, duplicate_ref, data_id) = parse_duplicate(&primary.to_string());
    assert_ne!(
        first_ref, duplicate_ref,
        "the two declared Root positions are different logical positions"
    );
    let rejected = ts::take_root_snapshots()
        .into_iter()
        .find(|snapshot| snapshot.phase == ts::RootSnapshotPhase::PreflightRejected)
        .expect("the preflight rejection is observed");
    assert_eq!(
        rejected.root_state,
        ScopeState::Finalizing,
        "the rejected Root stays frozen"
    );
    assert!(
        rejected
            .root_owned
            .iter()
            .any(|id| format!("{id}") == data_id),
        "the duplicated instance is still owned by Root: {data_id} not in {:?}",
        rejected.root_owned
    );
    (first_ref, duplicate_ref, data_id)
}

/// 样本 B：Root 后项重复实际 DataId 的预检首错与真实剩余 owned 清理故障分别保留，互不覆盖。
#[test]
fn r01_root_late_duplicate_keeps_primary_and_cleanup_distinct() {
    ts::reset_observations();
    ts::take_log_reset();
    observe::reset();
    // 真实故障：Root 退出清理破坏第一个（未被业务读取的）owned 输入。
    install_root_cleanup_faults(1);

    let error = run_root_late_duplicate();
    let RunError::Runtime(runtime_error) = &error else {
        panic!("a Root preflight rejection is Runtime: {error}");
    };
    assert_eq!(
        runtime_error.note(),
        "root extraction preflight rejected",
        "the frozen framework class is preserved"
    );

    let (_first_ref, duplicate_ref, data_id) = primary_duplicate(&error);
    let source = Error::source(runtime_error).expect("the framework primary survives in source");
    assert!(
        ts::take_log_snapshot().is_empty(),
        "zero take on the rejected extraction"
    );

    // 首清理诊断独立保存：真实 Root Scope、stored-first、内容与首错不同。
    let records = ts::take_cleanup_failures();
    assert!(
        !records.is_empty(),
        "the real owned cleanup failure is recorded"
    );
    assert!(
        records[0].stored_first,
        "the first cleanup diagnostic is stored: {records:?}"
    );
    let root_scope = observe::take_frames()
        .iter()
        .find(|frame| frame.kind == "root" && frame.phase == "enter")
        .map(|frame| frame.scope.clone())
        .expect("the Root frame is observed");
    assert_eq!(
        records[0].scope, root_scope,
        "the cleanup diagnosis is positioned on the real Root scope"
    );
    assert!(
        matches!(&records[0].error, ScopeError::Invariant { violated } if *violated == NATURAL_OWNED_CLEANUP),
        "the natural owned-cleanup diagnosis: {:?}",
        records[0].error
    );

    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    assert_eq!(
        cleanup.to_string(),
        records[0].error.to_string(),
        "cleanup_source is exactly the first owned-cleanup diagnostic"
    );
    assert_ne!(
        source.to_string(),
        cleanup.to_string(),
        "the primary and cleanup channels carry different diagnostics"
    );
    assert!(
        source.to_string().contains(&data_id),
        "the primary channel still names the duplicated instance"
    );
    assert!(
        !cleanup.to_string().contains(&duplicate_ref),
        "the cleanup channel does not overwrite the primary positions"
    );
    ts::reset_observations();
}

/// 对照：无清理故障时，同一 Root 后项重复仍以实际 `DuplicateRootDataId` 作为唯一首错。
#[test]
fn r01_root_late_duplicate_without_cleanup_fault_keeps_primary() {
    ts::reset_observations();
    ts::take_log_reset();
    observe::reset();

    let error = run_root_late_duplicate();
    let RunError::Runtime(runtime_error) = &error else {
        panic!("a Root preflight rejection is Runtime: {error}");
    };
    assert_eq!(runtime_error.note(), "root extraction preflight rejected");
    let (_first_ref, _duplicate_ref, _data_id) = primary_duplicate(&error);
    assert!(
        runtime_error.cleanup_source().is_none(),
        "no cleanup fault means no cleanup channel"
    );
    // F01（旧 k20 的 Root 出口承接）：真实 Root 预检失败必须记录**唯一的首次终止**，
    // 定位于真实 Root Scope，类别为 `BodyError`（执行失败），而不是默认的取消收口。
    // 该类别由 `guard.failed("root output extraction rejected")` 显式写入；把该行退化成
    // `drop(guard)` 会被本断言捕获（记录变为 `Cancelled`）。
    let root_scope = observe::take_frames()
        .into_iter()
        .find(|frame| frame.kind == "root" && frame.phase == "enter")
        .map(|frame| frame.scope.clone())
        .expect("the Root frame is observed");
    let terminations = ts::take_termination_saved();
    assert_eq!(
        terminations.len(),
        1,
        "a rejected Root extraction records exactly one first termination: {terminations:?}"
    );
    assert_eq!(
        terminations[0].scope.as_ref(),
        Some(&root_scope),
        "the first termination is positioned on the real Root scope"
    );
    assert_eq!(
        terminations[0].kind,
        TerminationKind::BodyError,
        "a real Root preflight failure terminates the execution as BodyError, not cancellation"
    );
    assert!(
        ts::take_log_snapshot().is_empty(),
        "zero take on the rejected extraction"
    );
    ts::reset_observations();
}

// ================================================================ Root 预检拒绝族（k13／k14／k16 的 Root 入口承接）

/// 统一核对：preflight 拒绝后 Root 仍冻结、原根实例存活且归属不变、零 take。
fn assert_root_unchanged_and_no_take(
    coordinator: &ScopeCoordinator,
    root: &ScopeId,
    data: &DataId,
    label: &str,
) {
    assert_eq!(
        coordinator.state(root).unwrap(),
        ScopeState::Finalizing,
        "{label}: the rejected Root stays frozen"
    );
    assert!(
        coordinator.alive_probe(data),
        "{label}: the original root data survives"
    );
    assert_eq!(
        coordinator.owner_probe(data).unwrap(),
        *root,
        "{label}: ownership is unchanged"
    );
    assert!(
        ts::take_log_snapshot().is_empty(),
        "{label}: a preflight rejection takes nothing: {:?}",
        ts::take_log_snapshot()
    );
}

/// k13 型：Root 预检 **TypeMismatch**（声明类型与实际不符）。
#[test]
fn r01_root_preflight_declared_type_mismatch_is_rejected() {
    ts::reset_observations();
    ts::take_log_reset();
    let mut coordinator = ScopeCoordinator::new(ExecutionIdentity::new());
    let root = coordinator.root();
    let refs = RefIdAllocator::new(RefIdSource::new());
    let position = refs.allocate().unwrap();
    let data = coordinator.register_owned(&root, &position, 41u64).unwrap();
    coordinator.freeze_root(&root).unwrap();

    let port = DeclaredPort::with_type(position.clone(), TypeId::of::<u32>(), type_name::<u32>());
    let error = coordinator
        .prepare_root_extraction(&root, &[port])
        .unwrap_err();
    assert!(
        matches!(&error, ScopeError::TypeMismatch { position: at, expected, actual }
            if at == &position && *expected == "u32" && *actual == "u64"),
        "expected a u32/u64 mismatch at the declared position, got {error:?}"
    );
    assert_root_unchanged_and_no_take(&coordinator, &root, &data, "declared type mismatch");
    coordinator.abort(&root).unwrap();
}

/// k13 型：Root 预检 **RefNotBound**（后项声明位置未绑定，首项在拒绝后仍保留）。
#[test]
fn r01_root_preflight_unbound_later_declaration_is_rejected() {
    ts::reset_observations();
    ts::take_log_reset();
    let mut coordinator = ScopeCoordinator::new(ExecutionIdentity::new());
    let root = coordinator.root();
    let refs = RefIdAllocator::new(RefIdSource::new());
    let bound = refs.allocate().unwrap();
    let unbound = refs.allocate().unwrap();
    let data = coordinator.register_owned(&root, &bound, 7u32).unwrap();
    coordinator.freeze_root(&root).unwrap();

    let ports = [
        DeclaredPort::with_type(bound.clone(), TypeId::of::<u32>(), type_name::<u32>()),
        DeclaredPort::with_type(unbound.clone(), TypeId::of::<u32>(), type_name::<u32>()),
    ];
    let error = coordinator
        .prepare_root_extraction(&root, &ports)
        .unwrap_err();
    assert!(
        matches!(&error, ScopeError::RefNotBound { scope, position }
            if scope == &root && position == &unbound),
        "expected RefNotBound at the unbound declared position, got {error:?}"
    );
    assert_root_unchanged_and_no_take(&coordinator, &root, &data, "unbound later declaration");
    coordinator.abort(&root).unwrap();
}

/// k14 型：Root 预检 **ForeignExecution**（目标属于另一次 Execution）。
///
/// 合成内部前提：`inject_target_probe` 把一个**另一次 Execution** 的 `DataId` 写入 Root 输出
/// 位置；自然业务不会产生该状态。
#[test]
fn r01_root_preflight_foreign_execution_target_is_rejected() {
    ts::reset_observations();
    ts::take_log_reset();
    let mut coordinator = ScopeCoordinator::new(ExecutionIdentity::new());
    let root = coordinator.root();
    let refs = RefIdAllocator::new(RefIdSource::new());
    let owned_position = refs.allocate().unwrap();
    let output_position = refs.allocate().unwrap();
    let own = coordinator
        .register_owned(&root, &owned_position, 5u32)
        .unwrap();
    let foreign = DataIdAllocator::new(ExecutionIdentity::new())
        .allocate()
        .unwrap();
    coordinator.inject_target_probe(&root, &output_position, RefTarget::Data(foreign.clone()));
    coordinator.freeze_root(&root).unwrap();

    let port = DeclaredPort::with_type(
        output_position.clone(),
        TypeId::of::<u32>(),
        type_name::<u32>(),
    );
    let error = coordinator
        .prepare_root_extraction(&root, &[port])
        .unwrap_err();
    assert!(
        matches!(&error, ScopeError::Storage { source: InternalError::ForeignExecution { requested, .. } }
            if requested == &foreign),
        "expected the foreign-execution storage rejection at the injected target, got {error:?}"
    );
    assert_root_unchanged_and_no_take(&coordinator, &root, &own, "foreign execution target");
    coordinator.abort(&root).unwrap();
}

/// k16 型：Root 预检 **Invariant**（清理前提被破坏）。
///
/// 合成内部前提：`destroy_probe` 破坏一个**未被选择**的 owned entry；自然业务不会产生该状态。
#[test]
fn r01_root_preflight_cleanup_precondition_damage_is_rejected() {
    ts::reset_observations();
    ts::take_log_reset();
    let mut coordinator = ScopeCoordinator::new(ExecutionIdentity::new());
    let root = coordinator.root();
    let refs = RefIdAllocator::new(RefIdSource::new());
    let selected_position = refs.allocate().unwrap();
    let damaged_position = refs.allocate().unwrap();
    let selected = coordinator
        .register_owned(&root, &selected_position, 7u32)
        .unwrap();
    let damaged = coordinator
        .register_owned(&root, &damaged_position, 8u32)
        .unwrap();
    coordinator.destroy_probe(&damaged);
    coordinator.freeze_root(&root).unwrap();

    let port = DeclaredPort::with_type(
        selected_position.clone(),
        TypeId::of::<u32>(),
        type_name::<u32>(),
    );
    let error = coordinator
        .prepare_root_extraction(&root, &[port])
        .unwrap_err();
    assert!(
        matches!(&error, ScopeError::Invariant { violated } if *violated == NATURAL_OWNED_CLEANUP),
        "expected the owned-cleanup precondition invariant, got {error:?}"
    );
    assert_root_unchanged_and_no_take(
        &coordinator,
        &root,
        &selected,
        "cleanup precondition damage",
    );
    // `damaged` 是故意破坏清理前提的未选实例：前提已坏，故不再 abort Root。
}

/// k14 型：Root 预检 **IllegalOwner**（非 Root 所有者，责任不迁移）。
///
/// 合成内部前提：`relocate_owned_probe` 把一个 owned 记录移到**已关闭**的 child Scope；自然
/// 业务不会产生该状态——正式预检先拒绝仍有存活 descendant 的 Root，而关闭 Scope 会清空 owned
/// 并销毁实例。判定仍由**未修改**的正式 `prepare_root_extraction` 完成。
#[test]
fn r01_root_preflight_non_root_owner_is_rejected() {
    ts::reset_observations();
    ts::take_log_reset();
    let mut coordinator = ScopeCoordinator::new(ExecutionIdentity::new());
    let root = coordinator.root();
    let refs = RefIdAllocator::new(RefIdSource::new());
    let retained_position = refs.allocate().unwrap();
    let moved_position = refs.allocate().unwrap();
    let retained = coordinator
        .register_owned(&root, &retained_position, 5u32)
        .unwrap();
    let moved = coordinator
        .register_owned(&root, &moved_position, 9u32)
        .unwrap();
    let closed = coordinator.create_child(&root).unwrap();
    coordinator.abort(&closed).unwrap();
    assert_eq!(coordinator.state(&closed).unwrap(), ScopeState::Closed);
    coordinator.relocate_owned_probe(&moved, &closed);
    assert!(
        coordinator.alive_probe(&moved),
        "the relocated instance stays alive"
    );
    coordinator.freeze_root(&root).unwrap();

    let port = DeclaredPort::with_type(
        moved_position.clone(),
        TypeId::of::<u32>(),
        type_name::<u32>(),
    );
    let error = coordinator
        .prepare_root_extraction(&root, &[port])
        .unwrap_err();
    assert!(
        matches!(&error, ScopeError::IllegalOwner { id, owner, boundary }
            if id == &moved && owner == &closed && boundary == &root),
        "expected IllegalOwner with the closed scope as owner and Root as boundary, got {error:?}"
    );
    assert_eq!(
        coordinator.state(&root).unwrap(),
        ScopeState::Finalizing,
        "the rejected Root stays frozen"
    );
    assert!(
        coordinator.alive_probe(&moved),
        "the instance survives the rejection"
    );
    assert_eq!(
        coordinator.owner_probe(&moved).unwrap(),
        closed,
        "the rejection does not relocate responsibility"
    );
    assert!(coordinator.alive_probe(&retained));
    assert_eq!(coordinator.owner_probe(&retained).unwrap(), root);
    assert!(ts::take_log_snapshot().is_empty(), "take 零");
}

// ================================================================ Export 入口（f20 正式承接）

/// 业务 Node：读 Root 输入并产生一个新的 owned 值。
async fn bump(q: Query<&u32>) -> Result<u32, BodyError> {
    Ok(*q.get() + 1)
}

/// f20 型：Chain 整组 Export 同时含「祖先 alias 首项 ＋ 新 owned 后项」，caller 后项位置被预占
/// → 整组在任何提交之前拒绝：alias 没有被部分绑定、无责任转移、零 take。
///
/// 合成内部前提：`install_export_occupy_fault` 预占一个 caller 输出位置；自然业务不会产生。
#[test]
fn r01_chain_export_group_rejects_alias_plus_new_owned_without_partial_binding() {
    ts::reset_observations();
    ts::take_log_reset();
    observe::reset();
    install_export_occupy_fault(1);

    let error = block_on(Runtime::new().execute(
        |flow, seed| {
            flow.chain(|sub| {
                let produced = sub.then(bump, seed);
                (seed, produced)
            })
        },
        3u32,
    ))
    .expect_err("an occupied caller slot rejects the whole chain export");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("a rejected chain export is Runtime: {error}");
    };
    assert_eq!(runtime_error.note(), "chain export rejected");

    let snapshots = observe::take_export_snapshots();
    assert!(
        !snapshots.iter().any(|record| record.phase == "committed"),
        "no export group was committed"
    );
    let before = snapshots
        .iter()
        .find(|record| record.phase == "before")
        .expect("the caller snapshot before the export group");
    let rejected = snapshots
        .iter()
        .find(|record| record.phase == "rejected")
        .expect("the caller snapshot after the rejected group");
    assert_eq!(
        rejected.refs.len(),
        before.refs.len(),
        "the alias slot is not partially bound: {:?} vs {:?}",
        before.refs,
        rejected.refs
    );
    assert_eq!(
        rejected.owned, before.owned,
        "no responsibility is transferred"
    );
    assert_eq!(
        rejected.next_data_id, before.next_data_id,
        "the rejected group allocates nothing"
    );
    assert!(ts::take_log_snapshot().is_empty(), "no take");
    ts::reset_observations();
}

// ================================================================ F02 §3.1：Root 第二输入登记失败（旧 k05 正式入口承接）

thread_local! {
    /// 本样本的业务调用计数：登记失败时业务体必须零调用。
    static SECOND_INPUT_BUSINESS_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// 带逐实例析构计数的 Root 输入（两份输入各 Drop 一次的证据）。
struct CountedInput {
    value: u32,
    drops: Rc<Cell<usize>>,
}

impl Data for CountedInput {}

impl Drop for CountedInput {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

/// 业务 Node：读取第一份 Root 输入；只有登记成功、进入 Root frame 后才会被调用。
async fn read_first(q: Query<&CountedInput>) -> Result<u32, BodyError> {
    SECOND_INPUT_BUSINESS_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(q.get().value)
}

/// 旧 `k05` 的正式入口承接：第二输入声明位置已被占用 → `Runtime::execute` 的
/// `register_owned_erased` 提前返回分支以精确 `RefAlreadyBound` 拒绝：只有第一输入被接管、
/// 业务零调用、两份输入各 Drop 一次、尚未进入 Root frame／提取（零 take）。
///
/// 合成内部前提：`install_root_input_prebind_fault(1)` 把第二输入位置预先绑定到已接管的第一
/// 输入；**test-only**，自然业务不会产生该状态（静态输入形状的每个声明位置互不相同且初始
/// 未绑定，登记入口的占用检查只防守重复登记）。判定仍由**未修改**的正式 `Runtime::execute`
/// 输入登记循环与 `ScopeCoordinator::register_owned_erased` 完成。
#[test]
fn r02_root_second_input_registration_failure_runs_no_body_and_no_take() {
    ts::reset_observations();
    ts::take_log_reset();
    observe::reset();
    SECOND_INPUT_BUSINESS_CALLS.with(|calls| calls.set(0));

    let left_drops = Rc::new(Cell::new(0));
    let right_drops = Rc::new(Cell::new(0));
    install_root_input_prebind_fault(1);

    let error = block_on(Runtime::new().execute(
        |flow, (left, _right)| flow.then(read_first, left),
        (
            CountedInput {
                value: 6,
                drops: Rc::clone(&left_drops),
            },
            CountedInput {
                value: 7,
                drops: Rc::clone(&right_drops),
            },
        ),
    ))
    .expect_err("the second Root input position conflict must reject registration");

    let RunError::Runtime(runtime_error) = &error else {
        panic!("a Root input registration failure is Runtime: {error}");
    };
    assert_eq!(runtime_error.note(), "root input registration failed");

    // 精确拒绝变体与位置：冲突位置就是第二输入的声明位置（已接管数量为 1 证明第一已成功）。
    let failures = ts::take_root_input_registration_failures();
    assert_eq!(failures.len(), 1, "one registration failure: {failures:?}");
    assert_eq!(
        failures[0].registered, 1,
        "only the first input was taken over: {failures:?}"
    );
    let source = Error::source(runtime_error).expect("the registration diagnostic survives");
    assert!(
        format!("{source:?}").starts_with("FrameworkDiagnostic { scope_error: RefAlreadyBound {"),
        "the primary channel is RefAlreadyBound: {source:?}"
    );
    assert_eq!(
        source.to_string(),
        failures[0].diagnostic,
        "the public diagnostic is exactly the recorded conflict"
    );
    assert!(
        source
            .to_string()
            .starts_with(&failures[0].position.to_string()),
        "the conflict is reported at the failing declaration position: {source}"
    );

    // 业务零调用、尚未进入 Root frame。
    assert_eq!(
        SECOND_INPUT_BUSINESS_CALLS.with(|calls| calls.get()),
        0,
        "the business body never runs before registration completes"
    );
    assert!(
        observe::take_frames()
            .iter()
            .all(|frame| frame.kind != "root"),
        "no Root frame is entered before registration completes"
    );

    // 两份输入各 Drop 一次：已接管的第一输入随 Execution 清理，未接管的第二输入由登记入口丢弃。
    assert_eq!(
        left_drops.get(),
        1,
        "the taken-over first input drops exactly once"
    );
    assert_eq!(
        right_drops.get(),
        1,
        "the rejected second input drops exactly once"
    );

    // 尚未提取：零 take。
    assert!(
        ts::take_log_snapshot().is_empty(),
        "no take before the Root frame: {:?}",
        ts::take_log_snapshot()
    );
    ts::reset_observations();
}
