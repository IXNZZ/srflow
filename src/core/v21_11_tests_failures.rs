//! V21-11 验收样本（三）：L10～L19 的真实负例、故障传播与双诊断。

use std::sync::Arc;

use super::builder::TypedCallBuilder;
use super::context::BodyError;
use super::data_ref::DataRef;
use super::each::{
    Each, EachBuilder, EachOnly, EachShared, FinishFault, ItemInputFault, install_finish_fault,
    install_item_input_fault,
};
use super::flow::{Flow, FlowBuilder};
use super::identity::{DataId, DataIdAllocator, ExecutionIdentity};
use super::internal_error::{InternalError, ScopeError};
use super::loop_orchestrator::{LoopFault, install_loop_fault};
use super::match_orchestrator::MatchBuilder;
use super::node::NodeCall1;
use super::orchestrator::{OrchCall, ScopeRole};
use super::runtime::{RootErrorStage, Runtime};
use super::scope::{RefTarget, TargetSnapshot};
use super::signature::OrchSig;
use super::signature::{Data, Out2, SyncFnSig};
use super::test_support::{
    ConsumeSnapshotPhase, ExportFaultHit, ExportSnapshotPhase, RootFault, RootSnapshotPhase,
    RoundCollectSnapshotPhase, advance_to_pending, boundary_creation_snapshot,
    closed_scope_snapshot, drive, drive_pinned, install_gate, install_root_fault,
    item_target_snapshot, record, release_gate, strict_reset_observations,
    take_consume_pre_cleanup, take_events, take_export_fault_hits, take_export_pre_cleanup,
    take_log_snapshot, take_round_collect_pre_cleanup,
};
use super::v21_11_tests::{ItemResult, Report, Route, Rules, State, item_flow_and_subflow_helpers};

// ---------------------------------------------------------------- L10 业务类型

/// SubFlow 的输入类型（非 Clone，无 Eq 需求）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Payload {
    pub(crate) id: u32,
}

impl Drop for Payload {
    fn drop(&mut self) {
        record(&format!("payload-dropped:{}", self.id));
    }
}

/// 把输入原样重新暴露为输出的 identity Flow。
fn identity_payload() -> Flow<(Payload,), Data<Payload>> {
    let (flow, payload) = FlowBuilder::<(Payload,)>::start().expect("identity start");
    flow.finish::<Data<Payload>, _>(payload)
        .expect("identity finish")
}

/// 两个输出位置都 alias 同一导入 Data 的 SubFlow。
fn alias_subflow() -> Flow<(Payload,), Out2<Payload, Payload>> {
    let (mut flow, payload) = FlowBuilder::<(Payload,)>::start().expect("sub start");
    let first: DataRef<Payload> = flow
        .then::<_, OrchSig<Payload, Data<Payload>>, _>(identity_payload(), payload.clone())
        .expect("first alias step");
    let second: DataRef<Payload> = flow
        .then::<_, OrchSig<Payload, Data<Payload>>, _>(identity_payload(), payload)
        .expect("second alias step");
    flow.finish::<Out2<Payload, Payload>, _>((first, second))
        .expect("sub finish")
}

/// Root：SubFlow 的两个 alias 位置直接作为 Root 的两个声明输出。
fn alias_root() -> Flow<(Payload,), Out2<Payload, Payload>> {
    let (mut flow, payload) = FlowBuilder::<(Payload,)>::start().expect("root start");
    let (first, second) = flow
        .then::<_, OrchSig<Payload, Out2<Payload, Payload>>, _>(alias_subflow(), payload)
        .expect("alias subflow step");
    flow.finish::<Out2<Payload, Payload>, _>((first, second))
        .expect("root finish")
}

// ---------------------------------------------------------------- 辅助

fn count(events: &[String], prefix: &str) -> usize {
    events
        .iter()
        .filter(|event| event.starts_with(prefix))
        .count()
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

fn new_foreign_data_id() -> DataId {
    let identity = ExecutionIdentity::new();
    let allocator = DataIdAllocator::new(Arc::clone(&identity));
    allocator.allocate().expect("foreign data id")
}

fn state_with(progress: u32, target: u32) -> State {
    State {
        item: 0,
        initial: progress,
        progress,
        step: 1,
        target,
        skip: false,
    }
}

fn creation_role(
    creations: &[(
        super::identity::ScopeId,
        super::identity::ScopeId,
        ScopeRole,
    )],
    scope: &super::identity::ScopeId,
) -> Option<ScopeRole> {
    creations
        .iter()
        .find(|(child, _, _)| child == scope)
        .map(|(_, _, role)| *role)
}

fn creation_parent(
    creations: &[(
        super::identity::ScopeId,
        super::identity::ScopeId,
        ScopeRole,
    )],
    scope: &super::identity::ScopeId,
) -> Option<super::identity::ScopeId> {
    creations
        .iter()
        .find(|(child, _, _)| child == scope)
        .map(|(_, parent, _)| parent.clone())
}

// ---------------------------------------------------------------- L10

#[test]
fn l10_root_rejects_alias_of_one_data_id_exported_by_subflow() {
    strict_reset_observations();
    let error = drive(Runtime::execute::<_, _, Out2<Payload, Payload>>(
        &alias_root(),
        (Payload { id: 3 },),
    ))
    .expect_err("物理重复必须在任何 take 前整体拒绝");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    let (first_ref, duplicate_ref, data_id) = match error.scope_error() {
        Some(ScopeError::DuplicateRootDataId {
            first_ref,
            duplicate_ref,
            data_id,
        }) => (first_ref.clone(), duplicate_ref.clone(), data_id.clone()),
        other => panic!("expected DuplicateRootDataId, got {other:?}"),
    };
    assert_ne!(first_ref, duplicate_ref, "两个不同声明位置");
    // 真实 take 调用数 0 先于变体匹配断言：提前 take 注入必须在这里被捕获，而不是靠变体改变。
    assert_eq!(
        take_log_snapshot(),
        Vec::<super::identity::DataId>::new(),
        "零 take（真实 take 入口记录）"
    );
    let snapshots = super::v21_11_tests::take_snapshots_checked();
    let rejected = last_snapshot(&snapshots, RootSnapshotPhase::PreflightRejected);
    for position in [&first_ref, &duplicate_ref] {
        let target = rejected
            .root_refs
            .iter()
            .find(|(candidate, _)| candidate == position)
            .map(|(_, target)| target.clone())
            .expect("内部 Export 已成功绑定两个位置");
        assert_eq!(
            target,
            TargetSnapshot::Data(data_id.clone()),
            "同一物理实例"
        );
    }
    assert_eq!(
        rejected.root_owned,
        vec![data_id.clone()],
        "owner 未重复转移"
    );
    assert!(
        rejected.taken_alive.is_empty(),
        "拒绝时没有任何实例被移出: {rejected:?}"
    );
    let events = take_events();
    assert_eq!(count(&events, "payload-dropped"), 1, "失败清理销毁输入一次");
}

// ---------------------------------------------------------------- L11

#[test]
fn l11_each_rejects_imported_shared_and_item_alias_as_outputs() {
    // (a) 把 imported shared 的完整 Data 当作 item 输出。
    strict_reset_observations();
    let error = drive(Runtime::execute::<_, _, Data<Vec<Rules>>>(
        &each_exporting_shared(),
        (
            vec![State {
                item: 0,
                initial: 0,
                progress: 0,
                step: 1,
                target: 1,
                skip: false,
            }],
            Rules { step: 1, target: 1 },
        ),
    ))
    .expect_err("imported shared 不能成为 item 输出");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    let shared_variant = error.scope_error().cloned();
    assert!(
        matches!(
            shared_variant,
            Some(ScopeError::IllegalOwner { .. }) | Some(ScopeError::NonCompleteTarget { .. })
        ),
        "imported 完整 Data 的拒绝类别: {shared_variant:?}"
    );
    let events = take_events();
    assert_eq!(
        count(&events, "collector-after:"),
        0,
        "拒绝发生在任何移动之前: {events:?}"
    );

    // (b) 把 item 的 CollectionItem 当作 item 输出。
    strict_reset_observations();
    let error = drive(Runtime::execute::<_, _, Data<Vec<State>>>(
        &each_exporting_item(),
        (vec![state_with(0, 1)],),
    ))
    .expect_err("CollectionItem 不能成为 item 输出");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::NonCompleteTarget { .. }) => {}
        other => panic!("expected NonCompleteTarget, got {other:?}"),
    }
    let events = take_events();
    assert_eq!(count(&events, "collector-after:"), 0, "无移动: {events:?}");

    // (c) 对照：Node 显式产生新 owned 值可以正常 Consume。
    strict_reset_observations();
    let ok = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &item_flow_and_subflow_helpers::subflow_over_states(),
        (vec![state_with(0, 1)],),
    ))
    .expect("新 owned 输出可 Consume");
    assert_eq!(ok.len(), 1);
}

/// Each 的 body 直接把 imported shared（Rules）声明为输出。
fn each_exporting_shared() -> Each<EachShared<State, Rules>, Rules> {
    let (mut body, (state, rules)) = FlowBuilder::<(State, Rules)>::start().expect("body start");
    let _state_used: DataRef<State> = body
        .then::<_, SyncFnSig<(State,), Data<State>>, _>(
            (|state: &State| Ok(state_with(state.progress, state.target)))
                as fn(&State) -> Result<State, BodyError>,
            state,
        )
        .expect("state step");
    let body = body.finish::<Data<Rules>, _>(rules).expect("body finish");
    let mut each: EachBuilder<EachShared<State, Rules>, Rules> =
        EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<(State, Rules), Data<Rules>>>(body)
        .expect("each body");
    each.finish().expect("each finish")
}

/// Each 的 body 直接把 item（CollectionItem）声明为输出。
fn each_exporting_item() -> Each<EachOnly<State>, State> {
    let (body, state) = FlowBuilder::<(State,)>::start().expect("body start");
    let body = body.finish::<Data<State>, _>(state).expect("body finish");
    let mut each: EachBuilder<EachOnly<State>, State> = EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<State, Data<State>>>(body)
        .expect("each body");
    each.finish().expect("each finish")
}

// ---------------------------------------------------------------- L12

/// 真实借用成功后记录 witness 的 item 结果 Node（拒绝路径上不会运行）。
fn borrow_witness_item_flow() -> Flow<(State,), Data<ItemResult>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body start");
    let result: DataRef<ItemResult> = flow
        .then::<_, SyncFnSig<(State,), Data<ItemResult>>, _>(
            (|state: &State| {
                record(&format!("borrow-observed:{}", state.item));
                Ok(ItemResult {
                    item: state.item,
                    progress: state.progress,
                })
            }) as fn(&State) -> Result<ItemResult, BodyError>,
            state,
        )
        .expect("witness step");
    flow.finish::<Data<ItemResult>, _>(result)
        .expect("body finish")
}

fn borrow_witness_subflow() -> Flow<(Vec<State>,), Data<Vec<ItemResult>>> {
    let mut each: EachBuilder<EachOnly<State>, ItemResult> = EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<State, Data<ItemResult>>>(borrow_witness_item_flow())
        .expect("each body");
    let each = each.finish().expect("each finish");
    let (mut flow, states) = FlowBuilder::<(Vec<State>,)>::start().expect("sub start");
    let collected: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(each, states)
        .expect("each step");
    flow.finish::<Data<Vec<ItemResult>>, _>(collected)
        .expect("sub finish")
}

#[test]
fn l12_stale_item_reuse_and_foreign_identity_are_rejected_without_borrow() {
    // (a) 下一合法 Item 复用上一真实 Item 的 CollectionItem（旧 cap 已关闭）→ 真实拒绝。
    strict_reset_observations();
    install_item_input_fault(ItemInputFault::ReusePreviousItemTarget { index: 1 });
    let error = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &borrow_witness_subflow(),
        (vec![
            State {
                item: 0,
                ..state_with(0, 0)
            },
            State {
                item: 1,
                ..state_with(0, 0)
            },
        ],),
    ))
    .expect_err("陈旧 item 目标必须被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    // 上一真实 Item 的完整 metadata：来源集合／下标／cap（第二 Item 记录复用同一目标）。
    let targets = item_target_snapshot();
    assert_eq!(
        targets.len(),
        2,
        "两个 Item 的绑定目标都被记录: {targets:?}"
    );
    let previous = &targets[0];
    let reused = &targets[1];
    assert_eq!(
        reused.target, previous.target,
        "第二 Item 复用上一 Item 的同一目标（来源集合／下标／cap）"
    );
    let (index, cap) = match &previous.target {
        TargetSnapshot::CollectionItem {
            index,
            lifetime_cap,
            ..
        } => (*index, lifetime_cap.clone()),
        other => panic!("上一 Item 目标必须是 CollectionItem: {other:?}"),
    };
    assert_eq!(index, 0, "上一 Item 的真实下标");
    let creations = boundary_creation_snapshot();
    assert_eq!(
        creation_role(&creations, &cap),
        Some(ScopeRole::Item),
        "旧 cap 是上一真实 ItemScope: {creations:?}"
    );
    assert_eq!(previous.item, cap, "cap 就是上一 Item");
    assert_ne!(reused.item, cap, "请求方是下一 Item");
    assert!(
        closed_scope_snapshot().contains(&cap),
        "旧 cap 已关闭（tombstone）"
    );
    match error.scope_error() {
        Some(ScopeError::ScopeClosed { scope }) => {
            assert_eq!(*scope, cap, "诊断定位上一真实 Item 的已关闭 cap");
        }
        other => panic!("expected ScopeClosed on the stale cap, got {other:?}"),
    }
    // 借用 witness：第一项真实借用，第二项在拒绝路径上没有业务借用。
    let events = take_events();
    assert_eq!(
        count(&events, "borrow-observed:0"),
        1,
        "第一项合法借用一次: {events:?}"
    );
    assert_eq!(
        count(&events, "borrow-observed:1"),
        0,
        "被拒绝的第二项不产生业务借用: {events:?}"
    );
    // 对照：同一夹具不注入时，两项各由真实业务借用产生 witness。
    strict_reset_observations();
    let ok = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &borrow_witness_subflow(),
        (vec![
            State {
                item: 0,
                ..state_with(0, 0)
            },
            State {
                item: 1,
                ..state_with(0, 0)
            },
        ],),
    ))
    .expect("合法 resolve 对照");
    assert_eq!(ok.len(), 2, "两项都完成");
    let events = take_events();
    assert_eq!(
        count(&events, "borrow-observed:0"),
        1,
        "第一项真实借用: {events:?}"
    );
    assert_eq!(
        count(&events, "borrow-observed:1"),
        1,
        "第二项真实借用: {events:?}"
    );

    // (b) foreign Execution（同裸序号）目标单独拒绝，不被类型判据遮蔽。
    strict_reset_observations();
    let foreign = new_foreign_data_id();
    install_item_input_fault(ItemInputFault::ReplaceItemTarget {
        index: 1,
        target: RefTarget::Data(foreign.clone()),
    });
    let error = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &borrow_witness_subflow(),
        (vec![
            State {
                item: 0,
                ..state_with(0, 0)
            },
            State {
                item: 1,
                ..state_with(0, 0)
            },
        ],),
    ))
    .expect_err("foreign 目标拒绝");
    match error.scope_error() {
        Some(ScopeError::Storage {
            source: InternalError::ForeignExecution { requested, .. },
        }) => assert_eq!(*requested, foreign, "保留完整身份"),
        other => panic!("expected ForeignExecution, got {other:?}"),
    }
    let events = take_events();
    assert_eq!(count(&events, "borrow-observed:0"), 1, "第一项合法借用");
    assert_eq!(
        count(&events, "borrow-observed:1"),
        0,
        "foreign 目标不产生业务借用: {events:?}"
    );
}

// ---------------------------------------------------------------- L13

/// 带挂起点的 item 推进 Node（第一次调用等待闸门），供中程安装完整 ScopeId 故障。
pub(crate) struct GatedProgressNode {
    calls: std::cell::Cell<u32>,
}

impl GatedProgressNode {
    fn new() -> Self {
        Self {
            calls: std::cell::Cell::new(0),
        }
    }
}

impl NodeCall1<State, Data<State>> for GatedProgressNode {
    fn call<'a>(&'a self, state: &'a State) -> super::signature::NodeFut<'a, State> {
        let call = self.calls.get();
        self.calls.set(call + 1);
        Box::pin(async move {
            if call == 0 {
                super::test_support::gate_wait().await;
            }
            Ok(State {
                item: state.item,
                initial: state.initial,
                progress: (state.progress + state.step).min(state.target),
                step: state.step,
                target: state.target,
                skip: state.skip,
            })
        })
    }
}

fn gated_item_state_flow() -> Flow<(State,), Data<ItemResult>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body start");
    let progressed: DataRef<State> = flow
        .then::<_, super::signature::NodeSig<(State,), Data<State>>, _>(
            GatedProgressNode::new(),
            state,
        )
        .expect("gated step");
    let result: DataRef<ItemResult> = flow
        .then::<_, SyncFnSig<(State,), Data<ItemResult>>, _>(
            (|state: &State| {
                Ok(ItemResult {
                    item: state.item,
                    progress: state.progress,
                })
            }) as fn(&State) -> Result<ItemResult, BodyError>,
            progressed,
        )
        .expect("result step");
    flow.finish::<Data<ItemResult>, _>(result)
        .expect("body finish")
}

fn gated_subflow_over_states() -> Flow<(Vec<State>,), Data<Vec<ItemResult>>> {
    let mut each: EachBuilder<EachOnly<State>, ItemResult> = EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<State, Data<ItemResult>>>(gated_item_state_flow())
        .expect("each body");
    let each = each.finish().expect("each finish");
    let (mut flow, states) = FlowBuilder::<(Vec<State>,)>::start().expect("sub start");
    let collected: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(each, states)
        .expect("each step");
    flow.finish::<Data<Vec<ItemResult>>, _>(collected)
        .expect("sub finish")
}

/// L13 载体：双输出 SubFlow 的第一个输出内可挂起（第二个输出为新 owned）。
fn two_output_subflow_armed() -> Flow<(Vec<State>,), DoubleResults> {
    let (mut flow, states) = FlowBuilder::<(Vec<State>,)>::start().expect("sub start");
    let first: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(
            gated_subflow_over_states(),
            states.clone(),
        )
        .expect("first output step");
    let second: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(
            item_flow_and_subflow_helpers::subflow_over_states(),
            states,
        )
        .expect("second output step");
    flow.finish::<DoubleResults, _>((first, second))
        .expect("sub finish")
}

fn two_output_subflow_armed_root() -> Flow<(Vec<State>,), DoubleResults> {
    let (mut flow, states) = FlowBuilder::<(Vec<State>,)>::start().expect("root start");
    let (first, second) = flow
        .then::<_, OrchSig<Vec<State>, DoubleResults>, _>(two_output_subflow_armed(), states)
        .expect("double subflow step");
    flow.finish::<DoubleResults, _>((first, second))
        .expect("root finish")
}

/// 双 Payload 输出形状（L13 imported-alias 对照）。
pub(crate) type DoublePayloads = Out2<Payload, Payload>;

/// 后项新 owned Payload 的挂起 Node：第一次调用等待闸门。
pub(crate) struct GatedOwnedPayloadNode;

impl NodeCall1<Payload, Data<Payload>> for GatedOwnedPayloadNode {
    fn call<'a>(&'a self, payload: &'a Payload) -> super::signature::NodeFut<'a, Payload> {
        Box::pin(async move {
            super::test_support::gate_wait().await;
            Ok(Payload {
                id: payload.id + 100,
            })
        })
    }
}

/// 首项 imported alias、后项新 owned 的双输出 SubFlow。
fn alias_first_subflow() -> Flow<(Payload,), DoublePayloads> {
    let (mut flow, payload) = FlowBuilder::<(Payload,)>::start().expect("sub start");
    let first: DataRef<Payload> = flow
        .then::<_, OrchSig<Payload, Data<Payload>>, _>(identity_payload(), payload.clone())
        .expect("alias step");
    let second: DataRef<Payload> = flow
        .then::<_, super::signature::NodeSig<(Payload,), Data<Payload>>, _>(
            GatedOwnedPayloadNode,
            payload,
        )
        .expect("owned step");
    flow.finish::<DoublePayloads, _>((first, second))
        .expect("sub finish")
}

fn alias_first_step_root() -> Flow<(Payload,), DoublePayloads> {
    let (mut flow, payload) = FlowBuilder::<(Payload,)>::start().expect("root start");
    let (first, second) = flow
        .then::<_, OrchSig<Payload, DoublePayloads>, _>(alias_first_subflow(), payload)
        .expect("alias-first subflow step");
    flow.finish::<DoublePayloads, _>((first, second))
        .expect("root finish")
}

#[test]
fn l13_late_export_failure_keeps_both_sides_unchanged() {
    strict_reset_observations();
    install_gate();
    let root = two_output_subflow_armed_root();
    let declared: Vec<super::ref_id::RefId> = root
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    assert_eq!(declared.len(), 2, "两个声明输出");
    let future = Runtime::execute::<_, _, DoubleResults>(&root, (vec![state_with(0, 0)],));
    let boxed = advance_to_pending(future, 1);
    // 完整 ScopeId：双输出 SubFlow 是 pending 现场唯一挂在 RootScope 下的 Flow。
    let creations = boundary_creation_snapshot();
    let subflow_scope = creations
        .iter()
        .find(|(_, parent, role)| *role == ScopeRole::Flow && parent.seq() == 0)
        .map(|(scope, _, _)| scope.clone())
        .expect("双输出 SubFlow 已创建");
    let root_scope = creation_parent(&creations, &subflow_scope).expect("subflow caller");
    assert_eq!(root_scope.seq(), 0, "caller 是 RootScope");
    super::orchestrator::install_export_fault(super::orchestrator::ExportFault::OccupyCallerSlot {
        child: subflow_scope.clone(),
        index: 1,
    });
    release_gate();
    let error = drive_pinned(boxed).expect_err("后项 caller 位置冲突");
    match error.scope_error() {
        Some(ScopeError::RefAlreadyBound { scope, position }) => {
            assert_eq!(*scope, root_scope, "冲突发生在 caller（RootScope）");
            assert_eq!(*position, declared[1], "冲突位置是第二声明输出");
        }
        other => panic!("expected RefAlreadyBound, got {other:?}"),
    }
    assert_eq!(
        take_export_fault_hits(),
        vec![ExportFaultHit {
            kind: "occupy-caller-slot",
            child: subflow_scope.clone(),
            caller: root_scope.clone(),
            index: Some(1),
        }],
        "Export 故障命中完整身份"
    );
    // 拒绝前后整组双侧元数据完全不变（读取真实 export 快照；唯一差异是观察阶段）。
    let exports = take_export_pre_cleanup();
    let after = exports
        .iter()
        .rev()
        .find(|snapshot| snapshot.phase == ExportSnapshotPhase::AfterReject)
        .expect("export after-reject snapshot")
        .clone();
    let before = exports
        .iter()
        .rev()
        .find(|snapshot| {
            snapshot.phase == ExportSnapshotPhase::Before && snapshot.child == after.child
        })
        .expect("export before snapshot for the same child")
        .clone();
    assert_eq!(after.child, Some(subflow_scope.clone()), "命中的 child");
    let mut normalized = before.clone();
    normalized.phase = after.phase;
    assert_eq!(normalized, after, "整组拒绝前后完整双侧元数据相等");
    // 随后各自清理：两个已提交结果各析构一次、Root 输入清理一次、零 take、无成功提交。
    let events = take_events();
    assert_eq!(
        count(&events, "item-result-dropped"),
        2,
        "两个已提交结果各清理一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped"),
        3,
        "Root 输入一项＋两个内层中间值各清理一次: {events:?}"
    );
    assert!(take_log_snapshot().is_empty(), "Root 零 take");
    let snapshots = super::v21_11_tests::take_snapshots_checked();
    assert!(
        !snapshots
            .iter()
            .any(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit),
        "无成功提交"
    );
}

/// L13 对照：首项为 imported alias、后项为新 owned 的 caller 冲突。
#[test]
fn l13_imported_alias_first_output_failure_keeps_alias_and_owned_unchanged() {
    strict_reset_observations();
    install_gate();
    let root = alias_first_step_root();
    let declared: Vec<super::ref_id::RefId> = root
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let future = Runtime::execute::<_, _, DoublePayloads>(&root, (Payload { id: 3 },));
    let boxed = advance_to_pending(future, 1);
    let creations = boundary_creation_snapshot();
    let subflow_scope = creations
        .iter()
        .find(|(_, parent, role)| *role == ScopeRole::Flow && parent.seq() == 0)
        .map(|(scope, _, _)| scope.clone())
        .expect("SubFlow 已创建");
    let root_scope = creation_parent(&creations, &subflow_scope).expect("subflow caller");
    super::orchestrator::install_export_fault(super::orchestrator::ExportFault::OccupyCallerSlot {
        child: subflow_scope.clone(),
        index: 1,
    });
    release_gate();
    let error = drive_pinned(boxed).expect_err("后项 caller 位置冲突");
    match error.scope_error() {
        Some(ScopeError::RefAlreadyBound { scope, position }) => {
            assert_eq!(*scope, root_scope, "冲突发生在 caller（RootScope）");
            assert_eq!(*position, declared[1], "冲突位置是第二声明输出");
        }
        other => panic!("expected RefAlreadyBound, got {other:?}"),
    }
    assert_eq!(
        take_export_fault_hits(),
        vec![ExportFaultHit {
            kind: "occupy-caller-slot",
            child: subflow_scope.clone(),
            caller: root_scope.clone(),
            index: Some(1),
        }],
        "Export 故障命中完整身份"
    );
    let exports = take_export_pre_cleanup();
    let after = exports
        .iter()
        .rev()
        .find(|snapshot| snapshot.phase == ExportSnapshotPhase::AfterReject)
        .expect("export after-reject snapshot")
        .clone();
    let before = exports
        .iter()
        .rev()
        .find(|snapshot| {
            snapshot.phase == ExportSnapshotPhase::Before && snapshot.child == after.child
        })
        .expect("export before snapshot for the same child")
        .clone();
    // 首项 imported alias：child 绑定指向 Root-owned 的输入实例，责任仍在 caller；
    // 后项新 owned 由 child 负责（另含故障注入的 caller 位置锚点）。
    let caller_owned = after.caller_owned.as_ref().expect("caller owned recorded");
    let child_owned = after.child_owned.as_ref().expect("child owned recorded");
    assert_eq!(
        caller_owned.len(),
        1,
        "caller 只负责 imported 输入: {after:?}"
    );
    let alias_id = caller_owned[0].clone();
    let child_refs = after.child_refs.as_ref().expect("child refs recorded");
    let target_of = |position: &super::ref_id::RefId| {
        child_refs
            .iter()
            .find(|(candidate, _)| candidate == position)
            .map(|(_, target)| target.clone())
            .unwrap_or_else(|| panic!("slot {position:?} bound in {child_refs:?}"))
    };
    assert_eq!(
        target_of(&after.slots[0].0),
        TargetSnapshot::Data(alias_id.clone()),
        "首项声明位置导出 imported alias"
    );
    let new_id = match target_of(&after.slots[1].0) {
        TargetSnapshot::Data(id) => id,
        other => panic!("后项应为完整 Data: {other:?}"),
    };
    assert_ne!(alias_id, new_id, "imported alias 与后项新 owned 是不同实例");
    assert!(
        child_owned.contains(&new_id),
        "后项新 owned 由 child 负责: {after:?}"
    );
    assert!(
        !child_owned.contains(&alias_id),
        "child 不持有 imported alias 的责任: {after:?}"
    );
    let anchor = match after.conflict_target.clone() {
        Some(TargetSnapshot::Data(id)) => id,
        other => panic!("冲突位置应绑定注入锚点: {other:?}"),
    };
    let mut expected_owned = vec![anchor.clone(), new_id.clone()];
    expected_owned.sort_by_key(super::identity::DataId::seq);
    let mut observed_owned = child_owned.clone();
    observed_owned.sort_by_key(super::identity::DataId::seq);
    assert_eq!(
        observed_owned, expected_owned,
        "child 恰好负责新 owned 与注入锚点: {after:?}"
    );
    assert_eq!(
        after.conflict_owner,
        Some(subflow_scope.clone()),
        "锚点由 child 负责: {after:?}"
    );
    let mut normalized = before.clone();
    normalized.phase = after.phase;
    assert_eq!(normalized, after, "拒绝前后完整双侧元数据相等");
    // 清理次序：新 owned 由 child 清理一次；imported alias 由 Root 在退出时清理一次。
    let events = take_events();
    assert_eq!(
        count(&events, "payload-dropped:103"),
        1,
        "child 只清理自身新 owned: {events:?}"
    );
    assert_eq!(
        count(&events, "payload-dropped:3"),
        1,
        "输入 alias 恰清理一次"
    );
    assert!(
        super::test_support::at(&events, "payload-dropped:103")
            < super::test_support::at(&events, "payload-dropped:3"),
        "child 清理先于 Root 退出清理: {events:?}"
    );
    assert!(take_log_snapshot().is_empty(), "Root 零 take");
}

// ---------------------------------------------------------------- L14

/// 一次 Round 收口的 Before／AfterReject 完整相等（唯一允许差异是观察阶段）。
fn assert_round_reject_unchanged(
    snapshots: &[super::test_support::RoundCollectPreCleanupSnapshot],
    expected_primary: &str,
) {
    let before = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RoundCollectSnapshotPhase::Before)
        .expect("promote before");
    let after = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RoundCollectSnapshotPhase::AfterReject)
        .expect("promote after-reject");
    assert!(before.observation_error.is_none() && after.observation_error.is_none());
    assert_eq!(
        format!("{:?}", after.source_state),
        "Some(Finalizing)",
        "拒绝时来源停在冻结状态"
    );
    let mut normalized = before.clone();
    normalized.phase = after.phase;
    assert_eq!(
        normalized, *after,
        "拒绝前后完整元数据相等: {expected_primary}"
    );
}

#[test]
fn l14_round_promote_rejection_leaves_state_and_responsibility_untouched() {
    // (a) 类型前提：控制状态声明类型被损坏为 u8。
    strict_reset_observations();
    install_loop_fault(LoopFault::PromoteStateTypeMismatch);
    let error = drive(Runtime::execute::<_, _, Data<State>>(
        &iter_state_root(),
        (state_with(0, 2),),
    ))
    .expect_err("Promote 类型拒绝");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::TypeMismatch {
            expected, actual, ..
        }) => {
            assert_eq!(*expected, "u8", "类型前提被损坏的声明类型");
            assert!(actual.contains("State"), "实际存储类型: {actual}");
        }
        other => panic!("expected the type-precondition rejection, got {other:?}"),
    }
    let all_round = take_round_collect_pre_cleanup();
    assert_round_reject_unchanged(&all_round, "state type precondition");
    let events = take_events();
    assert_eq!(count(&events, "loop-round:2"), 0, "后轮为 0: {events:?}");
    assert_eq!(
        count(&events, "state-dropped:0:0"),
        1,
        "imported 初态由 Root 关闭销毁一次"
    );

    // (b) 目标前提：被选输出在 Promote 预检前被真实销毁（存活判据拒绝）。
    strict_reset_observations();
    install_loop_fault(LoopFault::PromoteSelectedDestroyed);
    let error = drive(Runtime::execute::<_, _, Data<State>>(
        &iter_state_root(),
        (state_with(0, 2),),
    ))
    .expect_err("Promote 目标存活拒绝");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::TargetNotAlive { position }) => {
            assert!(position.seq() > 0, "被选输出位置: {position}");
        }
        other => panic!("expected the target-precondition rejection, got {other:?}"),
    }
    let all_round = take_round_collect_pre_cleanup();
    assert_round_reject_unchanged(&all_round, "selected target precondition");
    let events = take_events();
    assert_eq!(count(&events, "loop-round:2"), 0, "后轮为 0: {events:?}");

    // (c) 错误状态 owner：许可里的状态换成登记在本次 Round 上的状态（真实入口拒绝，无部分更新）。
    strict_reset_observations();
    install_loop_fault(LoopFault::PermitProbeWrongStateOwner);
    let outcome = drive(Runtime::execute::<_, _, Data<State>>(
        &iter_state_root(),
        (state_with(0, 2),),
    ))
    .expect("许可拒绝后同一轮仍正常推进");
    assert_eq!(outcome.progress, 2, "错误 owner 的尝试未被接受");
    let events = take_events();
    assert!(
        events.iter().any(|event| event
            == "permit-probe:Rejected { primary: Invariant { violated: \"round permit state is not owned by the registered loop scope\" }, cleanup_failure: None }"),
        "错误 state owner 的真实拒绝: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event == "permit-probe-round-state:Some(Active)"),
        "拒绝不改变 Round 状态: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event == "permit-probe-round-refs:2"),
        "拒绝不改变 Round 绑定: {events:?}"
    );
}

// ---------------------------------------------------------------- L15

/// L15 业务元素：每实例可区分（逐实例 Drop 见证）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Marker {
    pub(crate) id: u32,
}

impl Drop for Marker {
    fn drop(&mut self) {
        record(&format!("marker-dropped:{}", self.id));
    }
}

/// L15 路由 Node：记录第 `n` 项进入，第 0 项返回新 owned 分支、之后返回 imported 分支。
pub(crate) struct L15RouteNode {
    calls: std::cell::Cell<u32>,
}

impl L15RouteNode {
    fn new() -> Self {
        Self {
            calls: std::cell::Cell::new(0),
        }
    }
}

impl NodeCall1<State, Data<Route>> for L15RouteNode {
    fn call<'a>(&'a self, _state: &'a State) -> super::signature::NodeFut<'a, Route> {
        let call = self.calls.get();
        self.calls.set(call + 1);
        record(&format!("l15-item:{call}"));
        Box::pin(async move { Ok(Route(u32::from(call > 0))) })
    }
}

fn new_marker(marker: &Marker) -> Result<Marker, BodyError> {
    Ok(Marker {
        id: 100 + marker.id,
    })
}

fn identity_marker_flow() -> Flow<(Marker,), Data<Marker>> {
    let (flow, marker) = FlowBuilder::<(Marker,)>::start().expect("identity start");
    flow.finish::<Data<Marker>, _>(marker)
        .expect("identity finish")
}

fn l15_item_body() -> Flow<(State, Marker), Data<Marker>> {
    let (mut flow, (state, marker)) = FlowBuilder::<(State, Marker)>::start().expect("body start");
    let route: DataRef<Route> = flow
        .then::<_, super::signature::NodeSig<(State,), Data<Route>>, _>(L15RouteNode::new(), state)
        .expect("route step");
    let mut matched: MatchBuilder<Route, Marker, Data<Marker>> =
        MatchBuilder::start().expect("match");
    matched
        .branch::<_, SyncFnSig<(Marker,), Data<Marker>>>(
            Route(0),
            new_marker as fn(&Marker) -> Result<Marker, BodyError>,
        )
        .expect("new owned branch");
    matched
        .branch::<_, OrchSig<Marker, Data<Marker>>>(Route(1), identity_marker_flow())
        .expect("imported alias branch");
    let matched = matched.finish().expect("match finish");
    let produced: DataRef<Marker> = flow
        .then::<_, OrchSig<(Route, Marker), Data<Marker>>, _>(matched, (route, marker))
        .expect("match step");
    flow.finish::<Data<Marker>, _>(produced)
        .expect("body finish")
}

fn l15_root() -> Flow<(Vec<State>, Marker), Data<Vec<Marker>>> {
    let mut each: EachBuilder<EachShared<State, Marker>, Marker> =
        EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<(State, Marker), Data<Marker>>>(l15_item_body())
        .expect("each body");
    let each = each.finish().expect("each finish");
    let (mut flow, (states, marker)) = FlowBuilder::<(Vec<State>, Marker)>::start().expect("root");
    let collected: DataRef<Vec<Marker>> = flow
        .then::<_, OrchSig<(Vec<State>, Marker), Data<Vec<Marker>>>, _>(each, (states, marker))
        .expect("each step");
    flow.finish::<Data<Vec<Marker>>, _>(collected)
        .expect("root finish")
}

#[test]
fn l15_second_item_imported_owner_and_type_rejections_keep_partial_collector() {
    // (a) 第二 item 输出 imported shared（owner 反例）：第一项已 Consume，第二项在移动前拒绝。
    strict_reset_observations();
    let error = drive(Runtime::execute::<_, _, Data<Vec<Marker>>>(
        &l15_root(),
        (
            vec![state_with(0, 1), state_with(0, 1), state_with(0, 1)],
            Marker { id: 7 },
        ),
    ))
    .expect_err("imported shared 不能成为 item 输出");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    let creations = boundary_creation_snapshot();
    let item_scopes: Vec<super::identity::ScopeId> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Item)
        .map(|(scope, _, _)| scope.clone())
        .collect();
    assert_eq!(
        item_scopes.len(),
        2,
        "前两项已建立 ItemScope: {creations:?}"
    );
    let root_scope = closed_scope_snapshot()
        .into_iter()
        .find(|scope| scope.seq() == 0)
        .expect("root closed once");
    match error.scope_error() {
        Some(ScopeError::IllegalOwner {
            id,
            owner,
            boundary,
        }) => {
            assert_eq!(*owner, root_scope, "拒绝 owner 是 Root");
            assert_eq!(*boundary, item_scopes[1], "拒绝边界是第二 Item（完整身份）");
            let observed: Vec<super::identity::DataId> = take_consume_pre_cleanup()
                .iter()
                .filter(|snapshot| snapshot.phase == ConsumeSnapshotPhase::Before)
                .filter_map(|snapshot| snapshot.item_refs.as_ref())
                .flatten()
                .filter_map(|(_, target)| match target {
                    TargetSnapshot::Data(observed) => Some(observed.clone()),
                    _ => None,
                })
                .collect();
            assert!(
                observed.contains(id),
                "拒绝目标与 Consume 观察中的 imported Data 一致: {id}"
            );
        }
        other => panic!("expected IllegalOwner, got {other:?}"),
    }
    let events = take_events();
    assert_eq!(
        count(&events, "collector-after:1"),
        1,
        "第一项已 Consume: {events:?}"
    );
    assert_eq!(
        count(&events, "collector-after:2"),
        0,
        "第二项结果未移动: {events:?}"
    );
    assert_eq!(count(&events, "l15-item:1"), 1, "第二项已进入: {events:?}");
    assert_eq!(count(&events, "l15-item:2"), 0, "第三项未执行: {events:?}");
    assert_eq!(
        count(&events, "marker-dropped:107"),
        1,
        "部分 collector 的第一项结果清理一次: {events:?}"
    );
    assert_eq!(
        count(&events, "marker-dropped:7"),
        1,
        "imported shared 由 Root 退出清理一次: {events:?}"
    );
    assert!(
        super::test_support::at(&events, "marker-dropped:107")
            < super::test_support::at(&events, "marker-dropped:7"),
        "collector 清理先于 Root 退出: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event.starts_with("finish")),
        "不返回部分 Vec: {events:?}"
    );

    // (b) 第二 item 输入类型故障（只损坏元素声明类型）：第一项已 Consume，第二项 body 前拒绝。
    strict_reset_observations();
    install_item_input_fault(ItemInputFault::CorruptItemInputType { index: 1 });
    let error = drive(Runtime::execute::<_, _, Data<Vec<Marker>>>(
        &l15_root(),
        (
            vec![state_with(0, 1), state_with(0, 1), state_with(0, 1)],
            Marker { id: 7 },
        ),
    ))
    .expect_err("第二项的输入类型故障拒绝");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    match error.scope_error() {
        Some(ScopeError::TypeMismatch { .. }) | Some(ScopeError::Storage { .. }) => {}
        other => panic!("expected a type rejection, got {other:?}"),
    }
    let events = take_events();
    assert_eq!(
        count(&events, "collector-after:1"),
        1,
        "第一项已 Consume: {events:?}"
    );
    assert_eq!(
        count(&events, "collector-after:2"),
        0,
        "第二项结果未移动: {events:?}"
    );
    assert_eq!(count(&events, "l15-item:0"), 1, "第一项进入: {events:?}");
    assert_eq!(
        count(&events, "l15-item:1"),
        0,
        "第二项 body 前拒绝: {events:?}"
    );
    assert_eq!(count(&events, "l15-item:2"), 0, "第三项未执行: {events:?}");
    assert_eq!(
        count(&events, "marker-dropped:107"),
        1,
        "部分 collector 清理一次: {events:?}"
    );
    assert_eq!(count(&events, "marker-dropped:7"), 1, "shared 各清理一次");
    assert!(
        !events.iter().any(|event| event.starts_with("finish")),
        "不返回部分 Vec: {events:?}"
    );
}

// ---------------------------------------------------------------- L16

#[test]
fn l16_final_bind_and_caller_export_conflicts_are_atomic() {
    // (a) collector final bind 冲突：三项都已完成并移入 collector 后，最终位置已被占用。
    strict_reset_observations();
    install_finish_fault(FinishFault::AlreadyBound);
    let error = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &item_flow_and_subflow_helpers::subflow_over_states(),
        (vec![state_with(0, 1), state_with(0, 1), state_with(0, 1)],),
    ))
    .expect_err("final bind 冲突");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    let creations = boundary_creation_snapshot();
    let each_scope = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Each)
        .map(|(scope, _, _)| scope.clone())
        .expect("EachScope created");
    match error.scope_error() {
        Some(ScopeError::RefAlreadyBound { scope, position }) => {
            assert_eq!(*scope, each_scope, "冲突报告在 EachScope");
            assert!(position.seq() > 0);
        }
        other => panic!("expected RefAlreadyBound, got {other:?}"),
    }
    let events = take_events();
    assert_eq!(
        count(&events, "collector-after:3"),
        1,
        "三项均已完成并进入 collector: {events:?}"
    );
    // 故障注入后基线 == 拒绝后（完整状态字符串逐字相等），且 collector 移动数保持 3。
    let before_state = events
        .iter()
        .find(|event| event.starts_with("finish-before-state:"))
        .expect("finish 基线")
        .clone();
    let reject_state = events
        .iter()
        .find(|event| event.starts_with("finish-reject-state:"))
        .expect("finish 拒绝快照")
        .clone();
    assert_eq!(
        before_state.replacen("finish-before-state:", "", 1),
        reject_state.replacen("finish-reject-state:", "", 1),
        "拒绝前后状态逐字相等"
    );
    let before_next = events
        .iter()
        .find(|event| event.starts_with("finish-before-next-id:"))
        .expect("finish 序号基线")
        .clone();
    let reject_next = events
        .iter()
        .find(|event| event.starts_with("finish-reject-next-id:"))
        .expect("finish 拒绝序号")
        .clone();
    assert_eq!(
        before_next.replacen("finish-before-next-id:", "", 1),
        reject_next.replacen("finish-reject-next-id:", "", 1),
        "无额外 DataId 消耗"
    );
    assert_eq!(
        count(&events, "finish-reject-moves:3"),
        1,
        "拒绝后 collector 仍持有三项: {events:?}"
    );
    assert_eq!(
        count(&events, "item-result-dropped:0:1"),
        3,
        "三项结果各清理一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:0"),
        3,
        "输入三项各清理一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:1"),
        3,
        "三个中间 State 各清理一次: {events:?}"
    );
    assert!(take_log_snapshot().is_empty(), "Root 零 take");

    // (b) Each 自身 caller Export 冲突：多项完成后整组预检拒绝，完整双侧元数据不变。
    strict_reset_observations();
    install_gate();
    let root = gated_subflow_over_states();
    let declared: Vec<super::ref_id::RefId> = root
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let future = Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &root,
        (vec![state_with(0, 1), state_with(0, 1), state_with(0, 1)],),
    );
    let boxed = advance_to_pending(future, 1);
    let creations = boundary_creation_snapshot();
    let each_scope = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Each)
        .map(|(scope, _, _)| scope.clone())
        .expect("EachScope created");
    let root_scope = creation_parent(&creations, &each_scope).expect("each caller");
    assert_eq!(root_scope.seq(), 0, "caller 是 RootScope");
    super::orchestrator::install_export_fault(super::orchestrator::ExportFault::OccupyCallerSlot {
        child: each_scope.clone(),
        index: 0,
    });
    release_gate();
    let error = drive_pinned(boxed).expect_err("caller Export 冲突");
    match error.scope_error() {
        Some(ScopeError::RefAlreadyBound { scope, position }) => {
            assert_eq!(*scope, root_scope, "冲突发生在 caller（RootScope）");
            assert_eq!(*position, declared[0], "冲突位置是声明输出");
        }
        other => panic!("expected RefAlreadyBound, got {other:?}"),
    }
    assert_eq!(
        take_export_fault_hits(),
        vec![ExportFaultHit {
            kind: "occupy-caller-slot",
            child: each_scope.clone(),
            caller: root_scope.clone(),
            index: Some(0),
        }],
        "Export 故障命中完整身份"
    );
    let exports = take_export_pre_cleanup();
    let after = exports
        .iter()
        .rev()
        .find(|snapshot| snapshot.phase == ExportSnapshotPhase::AfterReject)
        .expect("caller Export 冲突必有 AfterReject 观察")
        .clone();
    let before = exports
        .iter()
        .rev()
        .find(|snapshot| {
            snapshot.phase == ExportSnapshotPhase::Before && snapshot.child == after.child
        })
        .expect("同一 child 的 Before 观察")
        .clone();
    assert_eq!(
        after.child,
        Some(each_scope.clone()),
        "命中的 child 是 Each"
    );
    let mut normalized = before.clone();
    normalized.phase = after.phase;
    assert_eq!(normalized, after, "拒绝前后完整双侧元数据相等");
    let events = take_events();
    assert_eq!(
        count(&events, "collector-after:3"),
        1,
        "三项均已完成: {events:?}"
    );
    assert_eq!(
        count(&events, "item-result-dropped:0:1"),
        3,
        "三项结果各清理一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:0"),
        3,
        "输入三项各清理一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:1"),
        3,
        "三个中间 State 各清理一次: {events:?}"
    );
    assert!(take_log_snapshot().is_empty(), "Root 零 take");
}

// ---------------------------------------------------------------- L17

#[test]
fn l17_deep_execution_error_propagates_with_exact_location() {
    strict_reset_observations();
    let error = drive(Runtime::execute::<_, _, Out2<Vec<ItemResult>, Report>>(
        &l17_root(),
        (vec![state_with(0, 2), state_with(0, 2)],),
    ))
    .expect_err("Round 深层错误经 Loop／Match／Each／SubFlow 传播到 Root");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    assert_eq!(error.note(), "round node failed", "局部返回说明保留");
    assert_eq!(
        error.termination_note(),
        Some("round node failed"),
        "**实际保存**的首错说明一致: {error:?}"
    );
    assert!(
        error.scope_error().is_none(),
        "业务 Node 失败保留可选 ScopeError 为空: {error:?}"
    );
    // 精确最深定位：Round 包装 Flow（Round 的直接 child），不是三选一。
    let deepest = error.termination_scope().cloned().expect("终止定位");
    let creations = boundary_creation_snapshot();
    let round_scope = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Round)
        .map(|(scope, _, _)| scope.clone())
        .expect("Round created");
    let wrapper_flow = creations
        .iter()
        .find(|(_, parent, role)| *role == ScopeRole::Flow && parent == &round_scope)
        .map(|(scope, _, _)| scope.clone())
        .expect("round wrapper Flow created");
    assert_eq!(deepest, wrapper_flow, "最深 frame 是 Round 包装 Flow");
    // 完整父链：Round→Loop→Branch→Match→item Flow→Item→Each→SubFlow→Root。
    assert_eq!(
        creation_parent(&creations, &deepest),
        Some(round_scope.clone())
    );
    let loop_scope = creation_parent(&creations, &round_scope).expect("loop");
    assert_eq!(
        creation_role(&creations, &loop_scope),
        Some(ScopeRole::Loop)
    );
    let branch = creation_parent(&creations, &loop_scope).expect("branch");
    assert_eq!(creation_role(&creations, &branch), Some(ScopeRole::Branch));
    let match_scope = creation_parent(&creations, &branch).expect("match");
    assert_eq!(
        creation_role(&creations, &match_scope),
        Some(ScopeRole::Match)
    );
    let item_body = creation_parent(&creations, &match_scope).expect("item body flow");
    assert_eq!(creation_role(&creations, &item_body), Some(ScopeRole::Flow));
    let item_scope = creation_parent(&creations, &item_body).expect("item");
    assert_eq!(
        creation_role(&creations, &item_scope),
        Some(ScopeRole::Item)
    );
    let each_scope = creation_parent(&creations, &item_scope).expect("each");
    assert_eq!(
        creation_role(&creations, &each_scope),
        Some(ScopeRole::Each)
    );
    let subflow_scope = creation_parent(&creations, &each_scope).expect("subflow");
    assert_eq!(
        creation_role(&creations, &subflow_scope),
        Some(ScopeRole::Flow)
    );
    let root_scope = creation_parent(&creations, &subflow_scope).expect("root");
    assert_eq!(root_scope.seq(), 0, "SubFlow 直接挂在 RootScope 下");
    // 真实事件生产者：失败轮一次、后轮／后项／父后步／其它 branch／default 全为 0。
    let events = take_events();
    assert_eq!(count(&events, "item-state:0"), 1, "第一项进入: {events:?}");
    assert_eq!(count(&events, "item-state:1"), 0, "后项不执行: {events:?}");
    assert_eq!(
        count(&events, "round-attempt:0:0"),
        1,
        "失败轮恰好一次: {events:?}"
    );
    assert_eq!(
        count(&events, "round-tail"),
        0,
        "同轮后步不执行: {events:?}"
    );
    assert_eq!(count(&events, "loop-round:2"), 0, "后轮不执行: {events:?}");
    assert_eq!(
        count(&events, "other-branch"),
        0,
        "其它 branch 不运行: {events:?}"
    );
    assert_eq!(
        count(&events, "default-branch"),
        0,
        "default 不运行: {events:?}"
    );
    assert_eq!(
        count(&events, "l17-result"),
        0,
        "item 结果未产生: {events:?}"
    );
    assert_eq!(count(&events, "later-step"), 0, "父后步不执行: {events:?}");
    let snapshots = super::v21_11_tests::take_snapshots_checked();
    assert!(
        !snapshots
            .iter()
            .any(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit),
        "无成功提交"
    );
}

// ---------------------------------------------------------------- L18

#[test]
fn l18_parent_failure_after_child_export_cleans_only_once() {
    strict_reset_observations();
    let error = drive(Runtime::execute::<_, _, Out2<Vec<ItemResult>, Report>>(
        &failing_after_subflow_root(),
        (vec![state_with(0, 0)],),
    ))
    .expect_err("父级在 child 提交后失败");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    let events = take_events();
    assert_eq!(
        count(&events, "item-result-dropped"),
        1,
        "parent 清理已接管值一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped"),
        2,
        "实际清理恰两份（元素与中间值）: {events:?}"
    );
    assert_eq!(
        count(&events, "item-result-dropped:0:0"),
        1,
        "parent 清理已接管值一次、child 不重清理: {events:?}"
    );
    assert!(
        take_log_snapshot().is_empty(),
        "Root 不 take: {:?}",
        take_log_snapshot()
    );
    let snapshots = super::v21_11_tests::take_snapshots_checked();
    assert!(
        !snapshots
            .iter()
            .any(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit),
        "无正常结果移交"
    );
}

// ---------------------------------------------------------------- L19

/// L19：Root 两个声明位置 alias 同一 DataId（预检首错）＋一个未选临时值（清理前提损坏目标）。
fn l19_dual_error_root() -> Flow<(Payload,), Out2<Payload, Payload>> {
    let (mut flow, payload) = FlowBuilder::<(Payload,)>::start().expect("root start");
    let (first, second) = flow
        .then::<_, OrchSig<Payload, Out2<Payload, Payload>>, _>(alias_subflow(), payload)
        .expect("alias subflow step");
    let temp: DataRef<Payload> = flow
        .then::<_, SyncFnSig<(Payload,), Data<Payload>>, _>(
            (|payload: &Payload| {
                Ok(Payload {
                    id: payload.id + 50,
                })
            }) as fn(&Payload) -> Result<Payload, BodyError>,
            first.clone(),
        )
        .expect("unselected temp step");
    let _ = temp; // 未声明输出：保留为 Root owned 的未选临时值
    flow.finish::<Out2<Payload, Payload>, _>((first, second))
        .expect("root finish")
}

#[test]
fn l19_preflight_first_error_and_cleanup_failure_are_both_saved() {
    strict_reset_observations();
    // Root owned 顺序：输入（下标 0）、未选临时值（下标 1）；随后预检首错是重复 DataId。
    install_root_fault(RootFault::DestroyRootOwned(1));
    super::test_support::install_post_terminate_probe();
    let error = drive(Runtime::execute::<_, _, Out2<Payload, Payload>>(
        &l19_dual_error_root(),
        (Payload { id: 3 },),
    ))
    .expect_err("预检首错＋清理前提损坏");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    // 首错：完整 DuplicateRootDataId 身份。
    let (first_ref, duplicate_ref, data_id) = match error.scope_error() {
        Some(ScopeError::DuplicateRootDataId {
            first_ref,
            duplicate_ref,
            data_id,
        }) => (first_ref.clone(), duplicate_ref.clone(), data_id.clone()),
        other => panic!("expected DuplicateRootDataId, got {other:?}"),
    };
    assert_ne!(first_ref, duplicate_ref, "两个不同声明位置");
    // 从真实保存点回读首次终止：说明／定位／原始 ScopeError 逐字段一致。
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "恰一次首次终止保存: {saved:?}");
    let saved = &saved[0];
    assert_eq!(saved.kind, super::context::TerminationKind::BodyError);
    assert_eq!(saved.note, "scope operation failed", "保存的说明");
    match &saved.scope_error {
        Some(ScopeError::DuplicateRootDataId {
            first_ref: saved_first,
            duplicate_ref: saved_duplicate,
            data_id: saved_data,
        }) => {
            assert_eq!(*saved_first, first_ref, "保存的首错第一个位置");
            assert_eq!(*saved_duplicate, duplicate_ref, "保存的首错重复位置");
            assert_eq!(*saved_data, data_id, "保存的首错实例身份");
        }
        other => panic!("expected the saved DuplicateRootDataId, got {other:?}"),
    }
    assert_eq!(
        error.termination_note(),
        Some(saved.note),
        "报告说明回读自保存字段"
    );
    assert_eq!(error.termination_scope(), saved.scope.as_ref(), "定位一致");
    assert_eq!(
        format!("{:?}", error.scope_error()),
        format!("{:?}", saved.scope_error),
        "报告首错回读自保存字段"
    );
    // 清理诊断独立保存：内容与首错不同、定位为实际清理 Scope（Root），互不覆盖。
    let cleanup = error
        .cleanup_failure()
        .cloned()
        .expect("独立保存的清理诊断");
    match cleanup.error() {
        ScopeError::Invariant { violated } => assert!(
            violated.contains("owned entry must exist until its scope closes"),
            "清理前提损坏诊断: {violated}"
        ),
        other => panic!("expected the cleanup invariant, got {other:?}"),
    }
    assert_eq!(
        Some(cleanup.scope().clone()),
        saved.scope.clone(),
        "清理定位实际 Scope"
    );
    assert!(
        !format!("{:?}", cleanup.error()).contains("DuplicateRootDataId"),
        "清理诊断不被首错覆盖: {cleanup:?}"
    );
    assert!(take_log_snapshot().is_empty(), "零 take");
    // 退出后仍分别保留：终止记录 = BodyError、清理失败不标记 Closed。
    let snapshots = super::v21_11_tests::take_snapshots_checked();
    let after = last_snapshot(&snapshots, RootSnapshotPhase::AfterFailureCleanup);
    assert_eq!(
        after.terminated,
        Some(super::context::TerminationKind::BodyError),
        "退出后仍记录首次终止: {after:?}"
    );
    assert_eq!(
        after.root_state,
        super::scope::ScopeState::Finalizing,
        "清理失败不标记 Closed"
    );
    // 终止后两类入口拒绝、受控清理可执行（探针在预检终止之后、guard 退出之前运行）。
    let events = take_events();
    let business = events
        .iter()
        .find(|event| event.starts_with("post-fail-business:"))
        .expect("业务入口探针");
    let commit = events
        .iter()
        .find(|event| event.starts_with("post-fail-commit:"))
        .expect("普通 commit 探针");
    let controlled = events
        .iter()
        .find(|event| event.starts_with("post-fail-cleanup:"))
        .expect("受控清理探针");
    assert!(business.contains("Terminated"), "业务入口拒绝: {business}");
    assert!(commit.contains("Terminated"), "普通 commit 拒绝: {commit}");
    assert!(
        controlled.contains("owned entry must exist until its scope closes"),
        "受控清理可执行并返回真实清理诊断: {controlled}"
    );
}

// ---------------------------------------------------------------- 需要的组合与 Node

fn iter_state_root() -> super::loop_orchestrator::Loop<super::loop_orchestrator::Iter1<State>> {
    use super::loop_orchestrator::{Iter1, LoopBuilder};
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(node_round_body(Arc::new(ProgressLike)))
        .expect("iter body");
    builder.finish().expect("iter finish")
}

/// Round body：一个 Node 推进状态（供 L14／L17 使用）。
fn node_round_body(node: Arc<ProgressLike>) -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("round start");
    let produced: DataRef<State> = flow
        .then::<_, super::signature::ArcNodeSig<(State,), Data<State>>, _>(node, state)
        .expect("progress step");
    flow.finish::<Data<State>, _>(produced)
        .expect("round finish")
}

pub(crate) struct ProgressLike;

impl NodeCall1<State, Data<State>> for ProgressLike {
    fn call<'a>(&'a self, state: &'a State) -> super::signature::NodeFut<'a, State> {
        Box::pin(async move {
            Ok(State {
                item: state.item,
                initial: state.initial,
                progress: (state.progress + state.step).min(state.target),
                step: state.step,
                target: state.target,
                skip: state.skip,
            })
        })
    }
}

/// L17 item 路由 Node：记录项进入，固定走 iterate 分支。
pub(crate) struct ItemRouteNode;

impl NodeCall1<State, Data<Route>> for ItemRouteNode {
    fn call<'a>(&'a self, state: &'a State) -> super::signature::NodeFut<'a, Route> {
        Box::pin(async move {
            record(&format!("item-state:{}", state.item));
            Ok(Route(0))
        })
    }
}

/// L17 深层失败 Node：失败轮恰好一次。
pub(crate) struct FailingRoundNode;

impl NodeCall1<State, Data<State>> for FailingRoundNode {
    fn call<'a>(&'a self, state: &'a State) -> super::signature::NodeFut<'a, State> {
        Box::pin(async move {
            record(&format!("round-attempt:{}:{}", state.item, state.progress));
            Err(BodyError::new("round node failed"))
        })
    }
}

fn round_tail(state: &State) -> Result<State, BodyError> {
    record("round-tail");
    Ok(State {
        item: state.item,
        initial: state.initial,
        progress: state.progress,
        step: state.step,
        target: state.target,
        skip: state.skip,
    })
}

fn other_branch(state: &State) -> Result<State, BodyError> {
    record("other-branch");
    Ok(State {
        item: state.item,
        initial: state.initial,
        progress: state.progress,
        step: state.step,
        target: state.target,
        skip: state.skip,
    })
}

fn default_branch(state: &State) -> Result<State, BodyError> {
    record("default-branch");
    Ok(State {
        item: state.item,
        initial: state.initial,
        progress: state.progress,
        step: state.step,
        target: state.target,
        skip: state.skip,
    })
}

fn l17_result_node(state: &State) -> Result<ItemResult, BodyError> {
    record("l17-result");
    Ok(ItemResult {
        item: state.item,
        progress: state.progress,
    })
}

fn l17_later_step(items: &Vec<ItemResult>) -> Result<Report, BodyError> {
    let _ = items;
    record("later-step");
    Ok(Report { count: 0, total: 0 })
}

/// L17 Round 包装 body：失败 Node 之后还有后步（失败必须停止后步）。
fn l17_round_body() -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("round start");
    let failed: DataRef<State> = flow
        .then::<_, super::signature::NodeSig<(State,), Data<State>>, _>(
            FailingRoundNode,
            state.clone(),
        )
        .expect("failing step");
    let tail: DataRef<State> = flow
        .then::<_, SyncFnSig<(State,), Data<State>>, _>(
            round_tail as fn(&State) -> Result<State, BodyError>,
            failed,
        )
        .expect("tail step");
    flow.finish::<Data<State>, _>(tail).expect("round finish")
}

fn l17_iter_loop() -> super::loop_orchestrator::Loop<super::loop_orchestrator::Iter1<State>> {
    use super::loop_orchestrator::{Iter1, LoopBuilder};
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(l17_round_body())
        .expect("iter body");
    builder.finish().expect("iter finish")
}

/// L17 item body：路由 → Match（iterate＝Loop；其它 branch／default 有真实生产者）。
fn l17_item_body() -> Flow<(State,), Data<ItemResult>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body start");
    let route: DataRef<Route> = flow
        .then::<_, super::signature::NodeSig<(State,), Data<Route>>, _>(
            ItemRouteNode,
            state.clone(),
        )
        .expect("route step");
    let mut matched: MatchBuilder<Route, State, Data<State>> =
        MatchBuilder::start().expect("match");
    matched
        .branch::<_, OrchSig<State, Data<State>>>(Route(0), l17_iter_loop())
        .expect("iterate branch");
    matched
        .branch::<_, SyncFnSig<(State,), Data<State>>>(
            Route(1),
            other_branch as fn(&State) -> Result<State, BodyError>,
        )
        .expect("other branch");
    matched
        .default::<_, SyncFnSig<(State,), Data<State>>>(
            default_branch as fn(&State) -> Result<State, BodyError>,
        )
        .expect("default branch");
    let matched = matched.finish().expect("match finish");
    let produced: DataRef<State> = flow
        .then::<_, OrchSig<(Route, State), Data<State>>, _>(matched, (route, state))
        .expect("match step");
    let result: DataRef<ItemResult> = flow
        .then::<_, SyncFnSig<(State,), Data<ItemResult>>, _>(
            l17_result_node as fn(&State) -> Result<ItemResult, BodyError>,
            produced,
        )
        .expect("result step");
    flow.finish::<Data<ItemResult>, _>(result)
        .expect("body finish")
}

fn l17_subflow() -> Flow<(Vec<State>,), Data<Vec<ItemResult>>> {
    let mut each: EachBuilder<EachOnly<State>, ItemResult> = EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<State, Data<ItemResult>>>(l17_item_body())
        .expect("each body");
    let each = each.finish().expect("each finish");
    let (mut flow, states) = FlowBuilder::<(Vec<State>,)>::start().expect("sub start");
    let collected: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(each, states)
        .expect("each step");
    flow.finish::<Data<Vec<ItemResult>>, _>(collected)
        .expect("sub finish")
}

/// L17 完整链：Root→SubFlow→Each→item Flow→Match→Loop→Round(body Flow)，父后步在 Root。
fn l17_root() -> Flow<(Vec<State>,), Out2<Vec<ItemResult>, Report>> {
    let (mut flow, states) = FlowBuilder::<(Vec<State>,)>::start().expect("root start");
    let results: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(l17_subflow(), states)
        .expect("subflow step");
    let report: DataRef<Report> = flow
        .then::<_, SyncFnSig<(Vec<ItemResult>,), Data<Report>>, _>(
            l17_later_step as fn(&Vec<ItemResult>) -> Result<Report, BodyError>,
            results.clone(),
        )
        .expect("later step");
    flow.finish::<Out2<Vec<ItemResult>, Report>, _>((results, report))
        .expect("root finish")
}

fn failing_after_subflow_root() -> Flow<(Vec<State>,), Out2<Vec<ItemResult>, Report>> {
    let (mut flow, states) = FlowBuilder::<(Vec<State>,)>::start().expect("start");
    let results: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(
            item_flow_and_subflow_helpers::subflow_over_states(),
            states,
        )
        .expect("subflow step");
    let report: DataRef<Report> = flow
        .then::<_, SyncFnSig<(Vec<ItemResult>,), Data<Report>>, _>(
            (|_items: &Vec<ItemResult>| Err(BodyError::new("parent step failed")))
                as fn(&Vec<ItemResult>) -> Result<Report, BodyError>,
            results.clone(),
        )
        .expect("failing parent step");
    flow.finish::<Out2<Vec<ItemResult>, Report>, _>((results, report))
        .expect("finish")
}

/// 双输出形状的类型别名（供 L13 的两个夹具复用）。
pub(crate) type DoubleResults = Out2<Vec<ItemResult>, Vec<ItemResult>>;

// ---------------------------------------------------------------- L23

#[test]
fn l23_build_rejections_do_not_consume_sequence() {
    strict_reset_observations();
    // 跨 Definition／未声明 Ref：接线期拒绝且不消耗 Ref 序号。
    let (mut flow, state) = FlowBuilder::<(State, Rules)>::start().expect("flow start");
    let before = flow.allocated_probe();
    let foreign = super::ref_id::RefIdAllocator::new(super::ref_id::RefIdSource::new())
        .allocate()
        .expect("foreign position");
    let undeclared = super::data_ref::DataRef::<State>::from_position(foreign.clone());
    let rejected = flow.then::<_, SyncFnSig<(State,), Data<State>>, _>(
        (|state: &State| Ok(state_with(state.progress, state.target)))
            as fn(&State) -> Result<State, BodyError>,
        undeclared,
    );
    assert!(rejected.is_err(), "外来位置在接线期拒绝");
    assert_eq!(flow.allocated_probe(), before, "拒绝不消耗 Ref 序号");
    let _ = state;
}
