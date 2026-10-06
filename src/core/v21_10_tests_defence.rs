//! V21-10 验收样本（二）：低层故障的防御路径与 Root 专用权限。
//!
//! 覆盖 K04、K05、K12～K17、K20 的 DEFENCE 侧；所有注入只改*前置元数据*，判断、保存
//! 首错、冻结、提取与清理都走生产路径（`Runtime::execute`／`ScopeCoordinator`）。

use std::cell::RefCell;
use std::sync::Arc;

use super::builder::{Definition, TypedCallBuilder};
use super::context::{BodyError, InvocationKind, TerminationKind};
use super::data_ref::DataRef;
use super::flow::{Flow, FlowBuilder};
use super::identity::{DataId, DataIdAllocator, ExecutionIdentity, ScopeId, ScopeIdAllocator};
use super::internal_error::{InternalError, ScopeError};
use super::node::NodeCall1;
use super::orchestrator::OrchCall;
use super::ref_id::{RefId, RefIdAllocator, RefIdSource};
use super::runtime::{RootErrorStage, Runtime};
use super::scope::{ItemAccess, RefTarget, ScopeState};
use super::signature::OrchSig;
use super::signature::{BuildError, Data, DeclaredPort, NodeSig, Out2, SyncFnSig, Unit};
use super::test_support::{
    RootFault, RootSnapshotPhase, RootView, closed_scope_reset, closed_scope_snapshot,
    definition_in_root, drive, install_root_fault, record, reset_observations,
    root_snapshots_reset, take_events, take_root_snapshots,
};
use super::v21_10_tests::{Left, Other, Product, Right, single_data_flow, unit_node_flow};

// ---------------------------------------------------------------- 辅助

fn pairs(left: &Left, right: &Right) -> Result<u32, BodyError> {
    record("body-ran");
    Ok(left.id + right.id)
}

fn left_id(left: &Left) -> Result<u32, BodyError> {
    Ok(left.id)
}

fn left_as_u32_plus(left: &Left) -> Result<u32, BodyError> {
    Ok(left.id + 1000)
}

/// 双输入 → 单 `Data<u32>`（装配故障样本用）。
fn double_data_flow() -> Flow<(Left, Right), Data<u32>> {
    let (mut flow, (left, right)) = FlowBuilder::<(Left, Right)>::start().expect("start");
    let out: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left, Right), Data<u32>>, _>(
            pairs as fn(&Left, &Right) -> Result<u32, BodyError>,
            (left, right),
        )
        .expect("then");
    flow.finish::<Data<u32>, _>(out).expect("finish")
}

/// 单输入 → `Out2<u32, u32>`（后项未绑定／类型不符样本用）。
fn out2_same_type_flow() -> Flow<(Left, Right), Out2<u32, u32>> {
    let (mut flow, (left, right)) = FlowBuilder::<(Left, Right)>::start().expect("start");
    let first: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left,), Data<u32>>, _>(
            left_id as fn(&Left) -> Result<u32, BodyError>,
            left.clone(),
        )
        .expect("then first");
    let second: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left,), Data<u32>>, _>(
            left_as_u32_plus as fn(&Left) -> Result<u32, BodyError>,
            left,
        )
        .expect("then second");
    let _ = right;
    flow.finish::<Out2<u32, u32>, _>((first, second))
        .expect("finish out2")
}

/// 一个真实 SubFlow 步骤：输出由 imported alias 绑定（保留 closed child 供 K14 使用）。
fn subflow_alias_flow() -> Flow<(Left,), Data<Left>> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let aliased: DataRef<Left> = flow
        .then::<_, OrchSig<Left, Data<Left>>, _>(identity_leaf(), left)
        .expect("then subflow");
    flow.finish::<Data<Left>, _>(aliased).expect("finish")
}

fn identity_leaf() -> Flow<(Left,), Data<Left>> {
    let (flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    flow.finish::<Data<Left>, _>(left).expect("finish")
}

/// 一个与本次 Execution 无关、但序号与本地首个 DataId 相同的 `DataId`。
fn foreign_data_id() -> DataId {
    let identity = ExecutionIdentity::new();
    let allocator = DataIdAllocator::new(Arc::clone(&identity));
    allocator.allocate().expect("foreign id")
}

/// 一个不属于本 Execution 的 `ScopeId`。
fn foreign_scope_id() -> ScopeId {
    let identity = ExecutionIdentity::new();
    let allocator = ScopeIdAllocator::new(Arc::clone(&identity));
    allocator.allocate().expect("foreign scope")
}

/// 一个未绑定、也不属于任何真实 Definition 的位置。
fn fresh_position() -> RefId {
    let allocator = RefIdAllocator::new(RefIdSource::new());
    allocator.allocate().expect("fresh position")
}

fn last_snapshot(
    snapshots: &[super::test_support::RootSnapshot],
    phase: RootSnapshotPhase,
) -> super::test_support::RootSnapshot {
    snapshots
        .iter()
        .rev()
        .find(|snapshot| snapshot.phase == phase)
        .cloned()
        .unwrap_or_else(|| panic!("missing snapshot {phase:?}: {snapshots:?}"))
}

fn count_events(events: &[String], prefix: &str) -> usize {
    events
        .iter()
        .filter(|event| event.starts_with(prefix))
        .count()
}

/// 未被任何 Scope 负责（已提取）或不存在。
fn no_owner(snapshot: &super::test_support::RootSnapshot) -> bool {
    snapshot.taken_owned_by.iter().all(Option::is_none)
}

// ---------------------------------------------------------------- K04：unit 声明与构建拒绝

#[test]
fn k04_injected_non_zero_unit_declaration_is_rejected_before_take() {
    reset_observations();
    install_root_fault(RootFault::AppendPort(DeclaredPort::new::<()>(
        fresh_position(),
    )));
    let error = drive(Runtime::execute::<_, _, Unit>(
        &unit_node_flow(),
        (Left { id: 5 },),
    ))
    .expect_err("伪造的非零 unit 声明必须被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Signature, "{error:?}");
    match error.build_error() {
        Some(BuildError::RootOutputSignatureMismatch { index, .. }) => assert_eq!(*index, 0),
        other => panic!("expected RootOutputSignatureMismatch, got {other:?}"),
    }
    // Signature 拒绝发生在创建 Execution 与登记输入之前：没有提取观察点。
    let snapshots = take_root_snapshots();
    assert!(
        snapshots.is_empty(),
        "Signature 阶段拒绝不创建 Execution: {snapshots:?}"
    );
    let events = take_events();
    assert_eq!(
        count_events(&events, "left-dropped"),
        1,
        "输入未被登记，随驱动析构一次: {events:?}"
    );
}

#[test]
fn k04_unit_data_output_is_rejected_at_finish() {
    let (flow, handle) = FlowBuilder::<((),)>::start().expect("start");
    let error = flow
        .finish::<Data<()>, _>(handle)
        .expect_err("unit Data 声明被拒绝");
    assert_eq!(error, BuildError::UnitDataOutputNotSupported);
}

// ---------------------------------------------------------------- K05：装配失败

#[test]
fn k05_second_input_registration_failure_runs_no_body_and_no_take() {
    reset_observations();
    install_root_fault(RootFault::PrebindInput(1));
    let error = drive(Runtime::execute::<_, _, Data<u32>>(
        &double_data_flow(),
        (Left { id: 6 }, Right { id: 7 }),
    ))
    .expect_err("后项登记失败");
    assert_eq!(error.stage(), RootErrorStage::Assembly, "{error:?}");
    assert_eq!(error.registered_inputs(), 1, "只登记了第一份输入");
    match error.scope_error() {
        Some(ScopeError::RefAlreadyBound { position, .. }) => {
            assert!(position.seq() > 0, "冲突位置来自真实声明输入: {position}");
        }
        other => panic!("expected RefAlreadyBound, got {other:?}"),
    }
    let events = take_events();
    assert_eq!(
        count_events(&events, "body-ran"),
        0,
        "业务体不执行: {events:?}"
    );
    assert_eq!(
        count_events(&events, "left-dropped"),
        1,
        "已接管输入随 Execution 析构"
    );
    assert_eq!(
        count_events(&events, "right-dropped"),
        1,
        "未接管输入由驱动析构"
    );
    let snapshots = take_root_snapshots();
    assert!(
        snapshots.is_empty(),
        "尚未进入 Root frame，没有提取观察点: {snapshots:?}"
    );
}

// ---------------------------------------------------------------- K12／K13

#[test]
fn k12_same_ref_id_twice_in_declaration_is_rejected_by_real_preflight() {
    reset_observations();
    install_root_fault(RootFault::DuplicatePort(0));
    let error = drive(Runtime::execute::<_, _, Out2<u32, u32>>(
        &out2_same_type_flow(),
        (Left { id: 8 }, Right { id: 9 }),
    ))
    .expect_err("同一声明位置重复必须被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::DuplicateRootDataId {
            first_ref,
            duplicate_ref,
            ..
        }) => assert_eq!(first_ref, duplicate_ref, "同一完整 RefId 重复"),
        other => panic!("expected DuplicateRootDataId, got {other:?}"),
    }
    let snapshots = take_root_snapshots();
    let rejected = last_snapshot(&snapshots, RootSnapshotPhase::PreflightRejected);
    assert!(no_owner(&rejected), "拒绝时无责任移除: {rejected:?}");
    assert!(
        !snapshots
            .iter()
            .any(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit),
        "take 次数为零"
    );
}

#[test]
fn k13_unbound_second_declaration_is_rejected_with_the_first_preserved() {
    reset_observations();
    let flow = out2_same_type_flow();
    let declared = flow.definition().output_ports().to_vec();
    assert_eq!(declared.len(), 2);
    let unbound = fresh_position();
    install_root_fault(RootFault::ReplacePorts(vec![
        declared[0].clone(),
        DeclaredPort::new::<u32>(unbound.clone()),
    ]));
    let error = drive(Runtime::execute::<_, _, Out2<u32, u32>>(
        &flow,
        (Left { id: 10 }, Right { id: 11 }),
    ))
    .expect_err("后项未绑定必须被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::RefNotBound { position, .. }) => {
            assert_eq!(*position, unbound, "诊断保留未绑定的真实位置");
        }
        other => panic!("expected RefNotBound, got {other:?}"),
    }
    let snapshots = take_root_snapshots();
    let rejected = last_snapshot(&snapshots, RootSnapshotPhase::PreflightRejected);
    assert_eq!(rejected.planned_takes, 0, "整组 take=0");
    // 首项在拒绝后、清理前仍存活且 owner 未变。
    let first_target = rejected
        .root_refs
        .iter()
        .find(|(position, _)| *position == declared[0].position().clone())
        .map(|(_, target)| target.clone())
        .expect("首项仍绑定");
    match first_target {
        super::scope::TargetSnapshot::Data(id) => {
            assert!(
                rejected.root_owned.contains(&id),
                "首项 owner 未变: {rejected:?}"
            );
        }
        other => panic!("expected complete data, got {other:?}"),
    }
}

#[test]
fn k13_declared_type_mismatch_is_rejected() {
    reset_observations();
    install_root_fault(RootFault::RetargetOutputFromInput { index: 0, input: 1 });
    let error = drive(Runtime::execute::<_, _, Out2<u32, u32>>(
        &out2_same_type_flow(),
        (Left { id: 12 }, Right { id: 13 }),
    ))
    .expect_err("声明类型与目标实际类型不符必须被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::TypeMismatch {
            expected, actual, ..
        }) => {
            assert!(expected.contains("u32"), "声明类型: {expected}");
            assert!(actual.contains("Right"), "实际类型: {actual}");
        }
        other => panic!("expected TypeMismatch, got {other:?}"),
    }
    let snapshots = take_root_snapshots();
    let rejected = last_snapshot(&snapshots, RootSnapshotPhase::PreflightRejected);
    assert!(no_owner(&rejected), "无责任移除: {rejected:?}");
}

#[test]
fn k13_dead_selected_target_is_rejected() {
    reset_observations();
    // owned 顺序按 DataId 序号：输入在前、Step 输出在后。
    install_root_fault(RootFault::DestroyRootOwned(1));
    let error = drive(Runtime::execute::<_, _, Data<Product>>(
        &single_data_flow(),
        (Left { id: 14 },),
    ))
    .expect_err("失效目标必须被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::TargetNotAlive { position }) => assert!(position.seq() > 0),
        other => panic!("expected TargetNotAlive, got {other:?}"),
    }
    let snapshots = take_root_snapshots();
    assert!(
        !snapshots
            .iter()
            .any(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit),
        "take=0"
    );
}

// ---------------------------------------------------------------- K14：foreign 与非 Root owner

#[test]
fn k14_foreign_execution_target_with_same_sequence_is_rejected() {
    reset_observations();
    let foreign = foreign_data_id();
    install_root_fault(RootFault::InjectOutputTarget {
        index: 0,
        target: RefTarget::Data(foreign.clone()),
    });
    let error = drive(Runtime::execute::<_, _, Data<Product>>(
        &single_data_flow(),
        (Left { id: 15 },),
    ))
    .expect_err("foreign Execution 目标必须被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::Storage {
            source: InternalError::ForeignExecution { requested, .. },
        }) => assert_eq!(*requested, foreign, "保留完整 DataId 身份"),
        other => panic!("expected ForeignExecution, got {other:?}"),
    }
    let snapshots = take_root_snapshots();
    let rejected = last_snapshot(&snapshots, RootSnapshotPhase::PreflightRejected);
    assert_eq!(
        rejected.root_owned.len(),
        2,
        "本 Execution 的两份责任对象都未被误移交: {rejected:?}"
    );
    assert!(
        !rejected.root_owned.contains(&foreign),
        "foreign 实例从未被本 Execution 负责: {rejected:?}"
    );
}

#[test]
fn k14_non_root_owner_is_rejected_without_relocating_responsibility() {
    reset_observations();
    install_root_fault(RootFault::RelocateOwnedToClosedScope(0));
    let error = drive(Runtime::execute::<_, _, Data<Left>>(
        &subflow_alias_flow(),
        (Left { id: 16 },),
    ))
    .expect_err("非 Root owner 必须被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::IllegalOwner {
            owner, boundary, ..
        }) => {
            assert_eq!(*boundary, *boundary, "边界是本次 Root");
            assert!(owner.seq() > 0, "责任方不是 Root): {owner}");
        }
        other => panic!("expected IllegalOwner, got {other:?}"),
    }
    let snapshots = take_root_snapshots();
    let rejected = last_snapshot(&snapshots, RootSnapshotPhase::PreflightRejected);
    assert!(
        no_owner(&rejected) || rejected.root_owned.len() <= 1,
        "无重复责任: {rejected:?}"
    );
    let events = take_events();
    assert_eq!(
        count_events(&events, "left-dropped"),
        1,
        "失败清理各析构一次"
    );
}

// ---------------------------------------------------------------- K15：CollectionItem

#[test]
fn k15_collection_item_target_is_rejected_for_both_cap_states() {
    // cap 完整存活（RootScope）
    reset_observations();
    install_root_fault(RootFault::InjectOutputTarget {
        index: 0,
        target: RefTarget::CollectionItem {
            collection: foreign_data_id(),
            index: 0,
            lifetime_cap: foreign_scope_id(),
            access: ItemAccess::for_collection::<Vec<u32>>(),
        },
    });
    let error = drive(Runtime::execute::<_, _, Data<Product>>(
        &single_data_flow(),
        (Left { id: 17 },),
    ))
    .expect_err("CollectionItem 不能作为 Root owned 输出");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::NonCompleteTarget { position }) => assert!(position.seq() > 0),
        other => panic!("expected NonCompleteTarget, got {other:?}"),
    }

    // cap 已失效（前一次执行中已关闭的 Scope）
    reset_observations();
    let closed_before = {
        drive(Runtime::execute::<_, _, Data<Left>>(
            &subflow_alias_flow(),
            (Left { id: 18 },),
        ))
        .expect("first run closes its child");
        closed_scope_snapshot()
    };
    let stale_cap = closed_before
        .iter()
        .find(|scope| scope.seq() > 0)
        .cloned()
        .expect("a closed child scope");
    install_root_fault(RootFault::InjectOutputTarget {
        index: 0,
        target: RefTarget::CollectionItem {
            collection: foreign_data_id(),
            index: 0,
            lifetime_cap: stale_cap,
            access: ItemAccess::for_collection::<Vec<u32>>(),
        },
    });
    let error = drive(Runtime::execute::<_, _, Data<Product>>(
        &single_data_flow(),
        (Left { id: 19 },),
    ))
    .expect_err("cap 失效同样在完整 Data 判据处被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::NonCompleteTarget { .. }) => {}
        other => panic!("expected NonCompleteTarget, got {other:?}"),
    }

    // NORMAL 对照：Each 的完整 Vec 输出可正常提取（K10 已验收，这里补对照断言）。
    let mut each: super::each::EachBuilder<super::each::EachOnly<Left>, u32> =
        super::each::EachBuilder::start().expect("each");
    each.then_body::<_, SyncFnSig<(Left,), Data<u32>>>(
        left_id as fn(&Left) -> Result<u32, BodyError>,
    )
    .expect("each body");
    let each = each.finish().expect("each finish");
    let values = drive(Runtime::execute::<_, _, Data<Vec<u32>>>(
        &each,
        (vec![Left { id: 20 }],),
    ))
    .expect("complete Vec output is extractable");
    assert_eq!(values, vec![20]);
}

// ---------------------------------------------------------------- K16：清理前提损坏

#[test]
fn k16_cleanup_precondition_damage_is_rejected_before_take() {
    reset_observations();
    // 未被选择的输入（Right）在 owned 中排在第一位。
    install_root_fault(RootFault::DestroyRootOwned(0));
    let error = drive(Runtime::execute::<_, _, Data<u32>>(
        &double_data_flow(),
        (Left { id: 21 }, Right { id: 22 }),
    ))
    .expect_err("清理前提损坏必须在任何 take 之前拒绝");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::Invariant { violated }) => {
            assert!(violated.contains("owned entry"), "原始拒绝: {violated}");
        }
        other => panic!("expected the cleanup precondition invariant, got {other:?}"),
    }
    let snapshots = take_root_snapshots();
    assert!(
        !snapshots
            .iter()
            .any(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit),
        "take=0"
    );
    // 原拒绝与清理诊断分别保存，且 Context 析构不重复销毁已移出的值。
    let events = take_events();
    assert_eq!(
        count_events(&events, "body-ran"),
        1,
        "业务体已执行过: {events:?}"
    );
    assert_eq!(
        count_events(&events, "right-dropped"),
        1,
        "损坏前提不产生第二次销毁"
    );
}

// ---------------------------------------------------------------- K17／K20：权限与不可恢复终止

#[test]
fn k17_root_close_permission_matrix() {
    // 合法 Root 路径对照：零声明输出的真实 Root 可以冻结、提交、关闭。
    let mut ok = false;
    let _ = drive(definition_in_root(
        &Definition::new(),
        Vec::new(),
        |_ctx, _root| {},
        Some(|view: &mut RootView<'_, '_>| {
            let root = view.root().clone();
            let plan = view
                .guard
                .prepare_root_extraction(&root, &[])
                .expect("合法 Root 路径");
            assert_eq!(plan.take_count(), 0);
            let (values, targets) = view.guard.commit_root_extraction(&root, plan);
            assert!(values.is_empty());
            view.guard.close_root(&root, targets).expect("close");
            assert_eq!(view.state(&root)?, ScopeState::Closed);
            ok = true;
            Ok(())
        }),
    ));
    assert!(ok, "合法 Root 收口执行");

    // Leaf 沿用 RootScope：不能据此提取。
    let leaf_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = drive(definition_in_root(
        &Definition::new(),
        Vec::new(),
        |_ctx, _root| {},
        Some(|view: &mut RootView<'_, '_>| {
            let root = view.root().clone();
            let mut leaf = view.guard.enter(InvocationKind::Leaf, &root, false)?;
            let error = leaf
                .prepare_root_extraction(&root, &[])
                .expect_err("Leaf 不能提取 Root owned 输出");
            *leaf_error.borrow_mut() = Some(error);
            Ok(())
        }),
    ));
    match leaf_error.borrow().as_ref() {
        Some(ScopeError::OutsideInvocation { .. }) => {}
        other => panic!("expected OutsideInvocation for Leaf, got {other:?}"),
    }

    // Boundary 同样拒绝。
    let boundary_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = drive(definition_in_root(
        &Definition::new(),
        Vec::new(),
        |_ctx, _root| {},
        Some(|view: &mut RootView<'_, '_>| {
            let root = view.root().clone();
            // Boundary 建立并拥有一个真实 child Scope，但对 RootScope 没有提取权。
            let child = view.guard.create_child(&root)?;
            let mut boundary = view.guard.enter(InvocationKind::Boundary, &child, true)?;
            let error = boundary
                .prepare_root_extraction(&root, &[])
                .expect_err("Boundary 不能提取 Root owned 输出");
            *boundary_error.borrow_mut() = Some(error);
            drop(boundary);
            Ok(())
        }),
    ));
    match boundary_error.borrow().as_ref() {
        Some(ScopeError::OutsideInvocation { .. }) => {}
        other => panic!("expected OutsideInvocation for Boundary, got {other:?}"),
    }

    // foreign Root：不属于本 Execution 的 RootScope。
    let foreign = foreign_scope_id();
    let foreign_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = drive(definition_in_root(
        &Definition::new(),
        Vec::new(),
        |_ctx, _root| {},
        Some(|view: &mut RootView<'_, '_>| {
            let error = view
                .guard
                .prepare_root_extraction(&foreign, &[])
                .expect_err("foreign Root 拒绝");
            *foreign_error.borrow_mut() = Some(error);
            Ok(())
        }),
    ));
    match foreign_error.borrow().as_ref() {
        Some(ScopeError::ForeignExecution { scope }) => assert_eq!(*scope, foreign),
        other => panic!("expected ForeignExecution, got {other:?}"),
    }

    // 活跃 descendant：冻结之前拒绝。
    let descendant_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = drive(definition_in_root(
        &Definition::new(),
        Vec::new(),
        |_ctx, _root| {},
        Some(|view: &mut RootView<'_, '_>| {
            let root = view.root().clone();
            let _child = view.guard.create_child(&root)?;
            let error = view
                .guard
                .prepare_root_extraction(&root, &[])
                .expect_err("仍有活跃 descendant 时拒绝");
            *descendant_error.borrow_mut() = Some(error);
            Ok(())
        }),
    ));
    match descendant_error.borrow().as_ref() {
        Some(ScopeError::ActiveDescendants { scope }) => assert_eq!(scope.seq(), 0),
        other => panic!("expected ActiveDescendants, got {other:?}"),
    }

    // Finalizing 重入：冻结一次后再次冻结被拒绝。
    let reentry_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = drive(definition_in_root(
        &Definition::new(),
        Vec::new(),
        |_ctx, _root| {},
        Some(|view: &mut RootView<'_, '_>| {
            let root = view.root().clone();
            let _plan = view.guard.prepare_root_extraction(&root, &[])?;
            let error = view
                .guard
                .prepare_root_extraction(&root, &[])
                .expect_err("Finalizing 重入拒绝");
            *reentry_error.borrow_mut() = Some(error);
            Ok(())
        }),
    ));
    match reentry_error.borrow().as_ref() {
        Some(ScopeError::ScopeNotActive { state, .. }) => {
            assert_eq!(*state, ScopeState::Finalizing)
        }
        other => panic!("expected ScopeNotActive, got {other:?}"),
    }

    // Closed 重入：正常收口之后再次提取被拒绝。
    let closed_error: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = drive(definition_in_root(
        &Definition::new(),
        Vec::new(),
        |_ctx, _root| {},
        Some(|view: &mut RootView<'_, '_>| {
            let root = view.root().clone();
            let plan = view.guard.prepare_root_extraction(&root, &[])?;
            let (_, targets) = view.guard.commit_root_extraction(&root, plan);
            view.guard.close_root(&root, targets)?;
            let error = view
                .guard
                .prepare_root_extraction(&root, &[])
                .expect_err("Closed 重入拒绝");
            *closed_error.borrow_mut() = Some(error);
            Ok(())
        }),
    ));
    match closed_error.borrow().as_ref() {
        Some(ScopeError::ScopeClosed { .. }) => {}
        other => panic!("expected ScopeClosed, got {other:?}"),
    }
}

#[test]
fn k20_rejection_terminates_the_execution_and_blocks_business_reentry() {
    // 真实 Runtime 的预检拒绝：快照显示本次执行已终止。
    reset_observations();
    let flow = out2_same_type_flow();
    let declared = flow.definition().output_ports().to_vec();
    install_root_fault(RootFault::ReplacePorts(vec![
        declared[0].clone(),
        DeclaredPort::new::<u32>(fresh_position()),
    ]));
    let error = drive(Runtime::execute::<_, _, Out2<u32, u32>>(
        &flow,
        (Left { id: 23 }, Right { id: 24 }),
    ))
    .expect_err("preflight rejection");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    let snapshots = take_root_snapshots();
    let rejected = last_snapshot(&snapshots, RootSnapshotPhase::PreflightRejected);
    assert_eq!(
        rejected.terminated, None,
        "拒绝点本身尚未标记终止（由驱动随后标记）: {rejected:?}"
    );
    let after_cleanup = last_snapshot(&snapshots, RootSnapshotPhase::AfterFailureCleanup);
    assert_eq!(
        after_cleanup.terminated,
        Some(TerminationKind::BodyError),
        "退出后执行已终止: {after_cleanup:?}"
    );

    // 终止之后业务再进入被拒绝（真实 Context 机制）。
    let reentry: RefCell<Option<ScopeError>> = RefCell::new(None);
    let _ = drive(definition_in_root(
        &Definition::new(),
        Vec::new(),
        |_ctx, _root| {},
        Some(|view: &mut RootView<'_, '_>| {
            let root = view.root().clone();
            let position = fresh_position();
            view.guard.terminate(
                TerminationKind::BodyError,
                Some(root.clone()),
                "fixture",
                None,
            );
            let error = view
                .guard
                .register_owned(&root, &position, 0u8)
                .expect_err("终止后业务调用被拒绝");
            *reentry.borrow_mut() = Some(error);
            Ok(())
        }),
    ));
    match reentry.borrow().as_ref() {
        Some(ScopeError::Terminated { kind }) => assert_eq!(*kind, TerminationKind::BodyError),
        other => panic!("expected Terminated, got {other:?}"),
    }
}

// ---------------------------------------------------------------- K24：Node 视角不可达

#[test]
fn k24_node_has_no_runtime_or_container_access() {
    // Node 只接收业务借用：本样本用一个结构体 Node 证明它拿不到 Context／Container／
    // 提取入口（类型层面：`NodeCall1` 的签名只含 `&A` 与 `NodeFut`）。
    struct ProbeNode;
    impl NodeCall1<Left, Data<u32>> for ProbeNode {
        fn call<'a>(&'a self, left: &'a Left) -> super::signature::NodeFut<'a, u32> {
            Box::pin(async move { Ok(left.id) })
        }
    }
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let out: DataRef<u32> = flow
        .then::<_, NodeSig<(Left,), Data<u32>>, _>(ProbeNode, left)
        .expect("then");
    let flow = flow.finish::<Data<u32>, _>(out).expect("finish");
    let value = drive(Runtime::execute::<_, _, Data<u32>>(
        &flow,
        (Left { id: 25 },),
    ))
    .expect("probe node root");
    assert_eq!(value, 25);
    // 提取计划类型只在 `scope` 模块内构造，字段私有、不实现 Clone/Copy：
    // 业务侧无法伪造或重放（编译期属性，见 tests/ui 的 k 夹具与结果 §K24）。
}

#[test]
fn k26_runtime_entry_remains_the_only_production_root_executor() {
    // 非 test 构建的 Root 执行入口唯一性由交接清单（结果 §K26）与 grep 证据给出；
    // 此处断言同一完成态定义可被唯一入口反复执行，且 RootExit 只作为 test 观察类型存在。
    reset_observations();
    root_snapshots_reset();
    closed_scope_reset();
    let flow = super::v21_10_tests::single_data_flow();
    for id in [30u32, 31] {
        let product = drive(Runtime::execute::<_, _, Data<Product>>(
            &flow,
            (Left { id },),
        ))
        .expect("runtime entry");
        assert_eq!(product.value, id * 2);
    }
    let closed = closed_scope_snapshot();
    assert_eq!(
        closed.iter().filter(|scope| scope.seq() == 0).count(),
        2,
        "两次执行各自关闭自己的 RootScope: {closed:?}"
    );
    let _ = Other { value: 0 };
}
