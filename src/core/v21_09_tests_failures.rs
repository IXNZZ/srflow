//! V21-09 验收样本（二）：登记授权、构建原子性、收口拒绝与双诊断、不可恢复终止与取消。
//!
//! 覆盖 J08、J09、J16～J25、J27 的主要拒绝／诊断路径；主路径见 `v21_09_tests.rs`。

use std::cell::{Cell, RefCell};

use super::builder::TypedCallBuilder;
use super::context::BodyError;
use super::each::{Each, EachBuilder, EachOnly};
use super::flow::{Flow, FlowBuilder};
use super::identity::DataId;
use super::internal_error::ScopeError;
use super::loop_orchestrator::{
    Iter1, Loop, LoopBuilder, LoopDecision, LoopFault, Retry1, install_loop_fault,
};
use super::node::NodeCall1;
use super::orchestrator::{OrchCall, OrchScope, ScopeRole};
use super::signature::{AsyncFnSig, Data, NodeFut, NodeSig, OrchSig, SyncFnSig};
use super::test_support::{
    RootView, advance_to_pending, at, boundary_creation_reset, count, definition_in_root, drive,
    install_gate, release_gate, saw, saw_prefix, take_shared_events,
};

use super::v21_09_tests::{Draft, Item, RetryBody, State};

// ---------------------------------------------------------------- 本文件专用业务与 Node

// body 错误注入开关（只影响本文件的样本）。
thread_local! {
    static BODY_FAILS: Cell<bool> = const { Cell::new(false) };
}

fn body_should_fail() -> bool {
    BODY_FAILS.with(|slot| slot.get())
}

fn install_body_failure(fail: bool) {
    BODY_FAILS.with(|slot| slot.set(fail));
}

/// 单输入 Node：按开关返回执行错误或 Finish。
pub(crate) struct FaultyItemBody;

impl NodeCall1<Item, Data<Draft>> for FaultyItemBody {
    fn call<'a>(&'a self, input: &'a Item) -> NodeFut<'a, Draft> {
        let id = input.id;
        super::test_support::record("faulty-body:called");
        Box::pin(async move {
            if body_should_fail() {
                super::test_support::record("faulty-body:failed");
                return Err(BodyError::from(ScopeError::Invariant {
                    violated: "loop body failure probe",
                }));
            }
            Ok(Draft {
                round: id,
                finish: true,
            })
        })
    }
}

/// Iter 单输入 Node：首轮即 Finish（用于收口／绑定拒绝样本）。
fn iter_finish_now(state: &State) -> Result<State, BodyError> {
    Ok(State {
        value: state.value + 1,
        finish: true,
    })
}

/// Iter Node：继续到值 >= 3（用于回收故障样本）。
fn iter_step(state: &State) -> Result<State, BodyError> {
    let value = state.value + 1;
    Ok(State {
        value,
        finish: value >= 3,
    })
}

/// 读取最终值。
fn read_state(state: &State) -> Result<u32, BodyError> {
    Ok(state.value)
}

fn read_draft(draft: &Draft) -> Result<u32, BodyError> {
    Ok(draft.round)
}

/// 记录"父后步是否执行"的 Node（用于"停止后续轮次／父后步"断言）。
fn marker_node(value: &u32) -> Result<u32, BodyError> {
    super::test_support::record("after-loop-step");
    Ok(*value)
}

// ---------------------------------------------------------------- 夹具

/// Retry 主夹具：父 Flow → Retry（单输入 Node body）→ 父后步。
fn retry_fixture() -> (
    Flow<(Item,), Data<u32>>,
    super::ref_id::RefId,
    super::ref_id::RefId,
) {
    let (mut parent, input) = FlowBuilder::<(Item,)>::start().expect("parent");
    let position = input.position().clone();
    let mut retry: LoopBuilder<Retry1<Item, Draft>> = LoopBuilder::start().expect("retry");
    retry
        .then_body::<_, NodeSig<(Item,), Data<Draft>>>(FaultyItemBody)
        .expect("retry body");
    let orchestrator = retry.finish().expect("retry finish");
    let produced: super::data_ref::DataRef<Draft> = parent
        .then::<_, OrchSig<Item, Data<Draft>>, _>(orchestrator, input)
        .expect("then retry");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(Draft,), Data<u32>>, _>(
            read_draft as fn(&Draft) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let read_position = read.position().clone();
    let afterwards: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(
            marker_node as fn(&u32) -> Result<u32, BodyError>,
            read,
        )
        .expect("afterwards step");
    (
        parent
            .finish::<Data<u32>, _>(afterwards)
            .expect("parent finish"),
        position,
        read_position,
    )
}

/// Retry 三夹具：结构体 Node 给出 Continue／Continue／Finish（用于丢弃路径样本）。
fn retry_continue_fixture() -> (Flow<(Item,), Data<u32>>, super::ref_id::RefId) {
    let (mut parent, input) = FlowBuilder::<(Item,)>::start().expect("parent");
    let position = input.position().clone();
    let mut retry: LoopBuilder<Retry1<Item, Draft>> = LoopBuilder::start().expect("retry");
    retry
        .then_body::<_, NodeSig<(Item,), Data<Draft>>>(RetryBody::new())
        .expect("retry body");
    let orchestrator = retry.finish().expect("retry finish");
    let produced: super::data_ref::DataRef<Draft> = parent
        .then::<_, OrchSig<Item, Data<Draft>>, _>(orchestrator, input)
        .expect("then retry");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(Draft,), Data<u32>>, _>(
            read_draft as fn(&Draft) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let afterwards: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(
            marker_node as fn(&u32) -> Result<u32, BodyError>,
            read,
        )
        .expect("afterwards step");
    (
        parent
            .finish::<Data<u32>, _>(afterwards)
            .expect("parent finish"),
        position,
    )
}

/// Iter 主夹具：父 Flow → Iter（单输入 Node body）→ 父后步。
fn iter_fixture() -> (Flow<(State,), Data<u32>>, super::ref_id::RefId) {
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, SyncFnSig<(State,), Data<State>>>(
        iter_step as fn(&State) -> Result<State, BodyError>,
    )
    .expect("iter body");
    let orchestrator = iter.finish().expect("iter finish");
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator, input)
        .expect("then iter");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    (
        parent.finish::<Data<u32>, _>(read).expect("parent finish"),
        position,
    )
}

// ---------------------------------------------------------------- J09：构建原子性

#[test]
fn j09_build_atomicity_and_completion_state() {
    // 第二个 body：在追加 Step／分配 Ref 之前拒绝，且保留第一登记。
    let mut builder: LoopBuilder<Retry1<Item, Draft>> = LoopBuilder::start().expect("loop");
    let before = builder.allocated_probe();
    builder
        .then_body::<_, NodeSig<(Item,), Data<Draft>>>(FaultyItemBody)
        .expect("first body");
    let steps_after_first = builder.wrapper_step_count_probe();
    let error = builder
        .then_body::<_, NodeSig<(Item,), Data<Draft>>>(FaultyItemBody)
        .expect_err("second body rejected");
    assert_eq!(error, super::signature::BuildError::SecondLoopBody);
    assert_eq!(builder.allocated_probe(), before, "拒绝不消耗 Ref 序号");
    assert_eq!(
        builder.wrapper_step_count_probe(),
        steps_after_first,
        "拒绝不追加 Step"
    );
    // 第一登记仍有效：可以正常 finish。
    let orchestrator = builder.finish().expect("finish after rejection");
    assert_eq!(orchestrator.wrapper_definition().steps().len(), 1);

    // 缺少 body：完成被拒绝且不产生 Loop。
    let empty: LoopBuilder<Retry1<Item, Draft>> = LoopBuilder::start().expect("loop");
    let error = empty.finish().expect_err("missing body rejected");
    assert_eq!(error, super::signature::BuildError::LoopBodyMissing);
}

// ---------------------------------------------------------------- J08：登记授权

#[test]
fn j08_foreign_definition_delegation_is_rejected_before_state_or_round() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    // 真实登记一份 Iter；另一个同类型 Loop 作为"外来"对象由普通 Orchestrator 委派。
    let foreign: Loop<Iter1<State>> = {
        let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("foreign loop");
        builder
            .then_body::<_, SyncFnSig<(State,), Data<State>>>(
                iter_finish_now as fn(&State) -> Result<State, BodyError>,
            )
            .expect("foreign body");
        builder.finish().expect("foreign finish")
    };
    // 委派者是一个真实 Flow：它的单 Step 调用委派 Orchestrator。
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let delegate = DelegateToForeignLoop {
        foreign,
        definition: {
            let mut definition = super::builder::Definition::new();
            definition
                .declare_input::<State>("delegate input")
                .expect("delegate input");
            definition
                .declare_output_port::<State>("delegate output")
                .expect("delegate output");
            #[allow(clippy::arc_with_non_send_sync)] // 单线程、非 Send 执行模型
            std::sync::Arc::new(definition)
        },
    };
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(delegate, input)
        .expect("parent then delegate");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    let error = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                State {
                    value: 0,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("foreign delegation must be rejected");
    assert_eq!(
        error.note(),
        "loop session requires the running definition to be the loop orchestrator",
        "{error:?}"
    );
    // 拒绝发生在 state／Round／body 之前。
    let events = take_shared_events();
    assert!(!saw(&events, "loop-round:1"), "没有建立 Round: {events:?}");
    assert_eq!(
        boundary_creation_snapshot_roles(&ScopeRole::Round),
        0,
        "没有创建 RoundScope"
    );
    assert_eq!(
        boundary_creation_snapshot_roles(&ScopeRole::Loop),
        0,
        "没有创建 LoopScope"
    );
    assert!(
        !saw(&events, "faulty-body:called"),
        "未执行任何 Loop body: {events:?}"
    );
}

fn boundary_creation_snapshot_roles(role: &ScopeRole) -> usize {
    super::test_support::boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, candidate)| candidate == role)
        .count()
}

/// 普通 Orchestrator：持有另一个同类型 Loop，并把本次 Scope 交给它。
struct DelegateToForeignLoop {
    foreign: Loop<Iter1<State>>,
    definition: std::sync::Arc<super::builder::Definition>,
}

impl DelegateToForeignLoop {
    fn delegated(&self) -> &Loop<Iter1<State>> {
        &self.foreign
    }
}

impl OrchCall<(State,), Data<State>> for DelegateToForeignLoop {
    type Pack = super::orchestrator::Targets1<State>;

    fn definition(&self) -> &super::builder::Definition {
        &self.definition
    }

    fn run<'a>(&'a self, scope: OrchScope<'a, Self::Pack, Data<State>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            // 委派给 foreign Loop：其会话必须以"正在执行的 Definition"为准。
            let _session = super::orchestrator::LoopScopeTransfer::begin_loop_session(
                scope,
                self.delegated(),
            )?;
            Ok(())
        })
    }
}

#[test]
fn j08_tampered_registration_metadata_is_rejected() {
    // 篡改包装端口／Step 身份：`verify_registration` 在任何 Round 之前拒绝。
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("loop");
    builder
        .then_body::<_, SyncFnSig<(State,), Data<State>>>(
            iter_finish_now as fn(&State) -> Result<State, BodyError>,
        )
        .expect("body");
    let foreign_position = super::ref_id::RefIdAllocator::new(super::ref_id::RefIdSource::new())
        .allocate()
        .expect("foreign position");
    let foreign_port = super::signature::DeclaredPort::new::<u32>(foreign_position);
    builder.tamper_wrapper_outputs_probe(vec![foreign_port.clone()]);
    let orchestrator = builder.finish().expect("finish with tampered outputs");
    let error = orchestrator
        .verify_registration()
        .expect_err("tampered outputs rejected");
    assert!(
        error.note().contains("outputs do not match"),
        "输出端口不符被拒绝: {error:?}"
    );

    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("loop");
    builder
        .then_body::<_, SyncFnSig<(State,), Data<State>>>(
            iter_finish_now as fn(&State) -> Result<State, BodyError>,
        )
        .expect("body");
    builder.tamper_wrapper_inputs_probe(vec![foreign_port]);
    let orchestrator = builder.finish().expect("finish with tampered inputs");
    let error = orchestrator
        .verify_registration()
        .expect_err("tampered inputs rejected");
    assert!(
        error.note().contains("inputs do not match"),
        "输入端口不符被拒绝: {error:?}"
    );

    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("loop");
    builder
        .then_body::<_, SyncFnSig<(State,), Data<State>>>(
            iter_finish_now as fn(&State) -> Result<State, BodyError>,
        )
        .expect("body");
    builder.tamper_wrapper_step_probe(0xdead_beefusize as *const ());
    let orchestrator = builder.finish().expect("finish with tampered step");
    let error = orchestrator
        .verify_registration()
        .expect_err("tampered step rejected");
    assert!(
        error
            .note()
            .contains("step is not the registered wrapper step"),
        "Step 身份不符被拒绝: {error:?}"
    );
}

// ---------------------------------------------------------------- J17／J18：收口拒绝与双诊断

#[test]
fn j17_retry_discard_rejection_keeps_input_and_does_not_continue() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    // 三轮：第 1 轮 Continue 时注入"剩余 owned 清理前提"故障（discard 拒绝）。
    install_loop_fault(LoopFault::CollectCleanupPrecondition);
    let (parent, position) = retry_continue_fixture();
    let drops: RefCell<Vec<DataId>> = RefCell::new(Vec::new());
    let error = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            let id = ctx
                .register_owned(&ctx.root_scope(), &position, Item { id: 1 })
                .expect("input");
            drops.borrow_mut().push(id);
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("discard rejection must fail the loop");
    assert!(error.note().contains("scope"), "{error:?}");

    let events = take_shared_events();
    assert_eq!(count(&events, "loop-round:1"), 1, "{events:?}");
    assert!(
        !saw(&events, "loop-round:2"),
        "拒绝后不继续下一轮: {events:?}"
    );
    assert!(
        !saw(&events, "loop-collect:discarded"),
        "未成功 discard: {events:?}"
    );
    assert!(!saw(&events, "after-loop-step"), "父后步不执行: {events:?}");
    assert!(
        saw(&events, "round-guard:open"),
        "来源未 Closed: {events:?}"
    );
    assert!(
        saw(&events, "round-cleanup-failure"),
        "双诊断独立记录: {events:?}"
    );
    assert!(saw_prefix(&events, "round-primary:"), "{events:?}");
    // 原输入未被销毁：只在 Root 收口时各销毁一次（不早于 Round 清理）。
    assert_eq!(
        count(&events, "item-dropped"),
        1,
        "原输入由 Root 负责: {events:?}"
    );
    assert!(
        at(&events, "item-dropped") > at(&events, "round-guard:open"),
        "Loop 不销毁 ancestor-owned 输入: {events:?}"
    );
    // 收口观察：Before／AfterReject 成对，且拒绝发生在 cleanup 之前（两侧状态可比较）。
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let before = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .expect("before snapshot");
    let after = snapshots
        .iter()
        .find(|snapshot| {
            snapshot.phase == super::test_support::RoundCollectSnapshotPhase::AfterReject
        })
        .expect("after-reject snapshot");
    assert_eq!(
        before.operation,
        super::test_support::RoundCollectOperation::Discard
    );
    assert_eq!(
        after.operation,
        super::test_support::RoundCollectOperation::Discard
    );
    assert!(before.observation_error.is_none(), "{before:?}");
    assert!(after.observation_error.is_none(), "{after:?}");
    assert_eq!(
        before.source_owned, after.source_owned,
        "cleanup 前状态未变"
    );
    assert_eq!(before.controller_owned, after.controller_owned);
}

#[test]
fn j18_generic_promote_from_round_frame_is_rejected_at_runtime() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    install_loop_fault(LoopFault::GenericPromoteProbe);
    let (parent, position) = iter_fixture();
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                State {
                    value: 3,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("probe run still completes through the permitted path");

    let events = take_shared_events();
    let generic = events
        .iter()
        .find(|event| event.starts_with("generic-promote:"))
        .expect("probe recorded");
    assert!(
        generic.contains("OutsideInvocation"),
        "通用 promote 在 Round frame 请求 parent state 必须被拒绝: {generic}"
    );
    // 两端状态不变、来源尚未冻结。
    assert!(saw(&events, "generic-promote-stable:true"), "{events:?}");
    assert!(saw(&events, "generic-promote-unfrozen:true"), "{events:?}");
    // 合法窄许可仍成功：同一轮随后完成 Promote 与最终绑定。
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "{events:?}");
    assert!(saw(&events, "loop-finished"), "{events:?}");
}

#[test]
fn j18_promote_rejection_reports_primary_and_cleanup_separately() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    install_loop_fault(LoopFault::CollectCleanupPrecondition);
    let (parent, position) = iter_fixture();
    let _ = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                State {
                    value: 3,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("promote rejection fails the loop");

    let events = take_shared_events();
    assert!(
        saw(&events, "round-cleanup-failure"),
        "清理失败独立报告: {events:?}"
    );
    assert!(
        !saw(&events, "loop-finished"),
        "拒绝不产生最终输出: {events:?}"
    );
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let before = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .expect("before");
    let after = snapshots
        .iter()
        .find(|snapshot| {
            snapshot.phase == super::test_support::RoundCollectSnapshotPhase::AfterReject
        })
        .expect("after");
    assert_eq!(
        before.operation,
        super::test_support::RoundCollectOperation::Promote
    );
    assert_eq!(
        before.state_target, after.state_target,
        "状态 target 在 cleanup 前未变"
    );
    assert_eq!(before.state_pending, after.state_pending, "pending 未变");
    assert_eq!(before.source_refs, after.source_refs, "双侧 refs 未变");
    assert_eq!(before.controller_refs, after.controller_refs);
    assert_eq!(
        before.next_data_id, after.next_data_id,
        "拒绝不消耗 DataId 序号"
    );
    // 保存的诊断：拒绝原因与清理原因都进入 Context 的实际保存内容。
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "{saved:?}");
    assert!(
        saved[0].scope_error.is_some(),
        "保留原始 ScopeError: {saved:?}"
    );
    assert!(saved[0].scope.is_some(), "定位到实际失败 Scope: {saved:?}");
    let round = super::test_support::boundary_creation_snapshot()
        .into_iter()
        .find(|(_, _, role)| *role == ScopeRole::Round)
        .expect("round creation")
        .0;
    let expected_cleanup = format!(
        "round-cleanup-report:CleanupDiagnostic {{ scope: {round:?}, error: Invariant {{ violated: \"owned entry must exist until its scope closes\" }} }}"
    );
    assert!(
        saw(&events, &expected_cleanup),
        "保存的清理诊断: {events:?}"
    );
}

// ---------------------------------------------------------------- J19：post-Promote 回收失败

#[test]
fn j19_parent_recycle_failure_stops_and_cleans_once() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    install_loop_fault(LoopFault::RecycleNotActive);
    let (parent, position) = iter_fixture();
    let error = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                State {
                    value: 0,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("recycle failure propagates");

    let events = take_shared_events();
    // 第 1 轮 Promote 成功、其后回收前置失败：错误不再被当作"成功 Round"继续，也不建立下一轮。
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "{events:?}");
    assert_eq!(count(&events, "loop-round:1"), 1, "{events:?}");
    assert!(
        !saw(&events, "loop-round:2"),
        "回收失败后不再建立下一轮: {events:?}"
    );
    assert!(!saw(&events, "loop-finished"), "{events:?}");
    // 当前状态由 Loop 负责并清理一次（不重复销毁、不误删 Root 输入）。
    assert_eq!(
        state_drop_values(&events),
        vec![0, 1],
        "Root 输入与 Loop-owned current state 各清理一次: {events:?}"
    );
    assert!(
        saw_prefix(&events, "cleanup-start:1"),
        "错误定位在 Loop 调用范围: {events:?}"
    );
    assert!(
        error.note().contains("scope"),
        "错误来自 Loop 调用: {error:?}"
    );
}

fn state_drop_values(events: &[String]) -> Vec<u32> {
    let mut drops: Vec<u32> = events
        .iter()
        .filter_map(|event| event.strip_prefix("state-dropped:"))
        .filter_map(|value| value.parse::<u32>().ok())
        .collect();
    drops.sort_unstable();
    drops
}

// ---------------------------------------------------------------- J20：最终绑定／导出拒绝

#[test]
fn j20_final_binding_rejection_does_not_change_state() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    install_loop_fault(LoopFault::FinalPositionOccupied);
    let (parent, position) = iter_fixture();
    let error = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                State {
                    value: 3,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("binding rejection fails the loop");

    let events = take_shared_events();
    assert!(
        !saw(&events, "loop-finished"),
        "拒绝不产生最终输出: {events:?}"
    );
    assert!(!saw(&events, "after-loop-step"), "父后步不执行: {events:?}");
    assert!(
        error.note().contains("scope"),
        "原错是位置已绑定的结构化诊断: {error:?}"
    );
}

#[test]
fn j20_caller_output_conflict_is_rejected_without_partial_output() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    let (parent, position, read_position) = retry_fixture();
    // 预先占用父 Flow 的 step 输出位置：Loop 的 Export 在提交前拒绝。
    let error = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &position, Item { id: 4 })
                .expect("input");
            ctx.register_owned(&ctx.root_scope(), &read_position, 9u32)
                .expect("occupy caller output");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("caller conflict must be rejected");
    assert!(error.note().contains("scope"), "Export 预检拒绝: {error:?}");
    let events = take_shared_events();
    assert!(!saw(&events, "after-loop-step"), "父后步不执行: {events:?}");
}

// ---------------------------------------------------------------- J21／J22：体错误与不可恢复终止

#[test]
fn j21_body_error_stops_rounds_and_preserves_original_cause() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(true);
    let (parent, position, _) = retry_fixture();
    let error = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &position, Item { id: 3 })
                .expect("input");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("body error stops the loop");
    assert!(
        matches!(error.scope_error(), Some(ScopeError::Invariant { .. })),
        "保留原始 ScopeError: {error:?}"
    );

    let events = take_shared_events();
    assert!(saw(&events, "faulty-body:failed"), "{events:?}");
    assert!(!saw(&events, "loop-round:2"), "不自动技术重试: {events:?}");
    assert!(
        !saw(&events, "loop-collect:promoted"),
        "不返回上一轮结果: {events:?}"
    );
    assert!(!saw(&events, "after-loop-step"), "父后步不执行: {events:?}");
    assert_eq!(
        count(&events, "item-dropped"),
        1,
        "原输入仍由 Root 负责: {events:?}"
    );
    // 保存的首错就是 body 错误本身。
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "{saved:?}");
    assert!(matches!(
        saved[0].scope_error,
        Some(ScopeError::Invariant { .. })
    ));
    assert!(saved[0].scope.is_some(), "定位到实际失败 Scope: {saved:?}");

    install_body_failure(false);
}

#[test]
fn j22_termination_is_final_for_business_but_allows_controlled_cleanup() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(true);
    install_loop_fault(LoopFault::ProbeAfterBodyError);
    let (parent, position, _) = retry_fixture();
    let _ = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &position, Item { id: 3 })
                .expect("input");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("body error stops the loop");

    let events = take_shared_events();
    let business = events
        .iter()
        .find(|event| event.starts_with("post-fail-business:"))
        .expect("business probe recorded");
    assert!(
        business.contains("Terminated"),
        "终止后普通提交被拒绝: {business}"
    );
    let cleanup = events
        .iter()
        .find(|event| event.starts_with("post-fail-cleanup:"))
        .expect("cleanup probe recorded");
    assert!(cleanup.ends_with("Ok(())"), "受控清理仍允许: {cleanup}");
    // 完整退出后首错不被覆盖。
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "首错只保存一次: {saved:?}");
    assert!(matches!(
        saved[0].scope_error,
        Some(ScopeError::Invariant { .. })
    ));
    install_body_failure(false);
}

// ---------------------------------------------------------------- J16：cap 与状态组合

#[test]
fn j16_item_backed_state_promotes_within_cap() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    let (flow, collection_position) = each_item_state_fixture();
    drive(definition_in_root(
        flow.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &collection_position,
                vec![State {
                    value: 1,
                    finish: true,
                }],
            )
            .expect("collection");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("item backed state scenario");

    let events = take_shared_events();
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "{events:?}");
    assert!(
        boundary_creation_snapshot_roles(&ScopeRole::Loop) == 1,
        "Loop 在 Each item 内真实执行"
    );
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let promote = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .expect("promote snapshot");
    assert!(
        matches!(
            promote.state_target,
            Some(super::scope::TargetSnapshot::CollectionItem { .. })
        ),
        "初始 current-state 是 cap 内 item 目标: {promote:?}"
    );
    // 原集合身份不变、不产生 item-owned。
    assert!(
        promote
            .source_owned
            .as_ref()
            .is_some_and(|owned| owned.len() == 1),
        "Round 只 owns body 新产生的 State: {promote:?}"
    );
}

#[test]
fn j16_corrupted_item_state_is_rejected_before_commit() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    // Loop 的初始状态来自 Each 的 item（cap 内合法目标）：在登记前损坏该 item 元数据，
    // 真实新入口 `register_state` 必须在任何 Round 之前拒绝。
    install_loop_fault(LoopFault::CorruptStateInputType);
    let (flow, collection_position) = each_item_state_fixture();
    let error = drive(definition_in_root(
        flow.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &collection_position,
                vec![State {
                    value: 1,
                    finish: true,
                }],
            )
            .expect("collection");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("corrupted item state must be rejected");

    assert!(error.note().contains("scope"), "结构化拒绝: {error:?}");
    let events = take_shared_events();
    assert!(
        !saw(&events, "loop-collect:promoted"),
        "拒绝发生在提交前: {events:?}"
    );
    assert_eq!(
        boundary_creation_snapshot_roles(&ScopeRole::Round),
        0,
        "拒绝发生在建立 Round 之前"
    );
}

/// Each item 作为 Loop 初始状态的夹具：Flow → Each → Loop（body 产生新 State）。
fn each_item_state_fixture() -> (Flow<(Vec<State>,), Data<usize>>, super::ref_id::RefId) {
    let (mut flow, collection) = FlowBuilder::<(Vec<State>,)>::start().expect("flow");
    let collection_position = collection.position().clone();
    let mut each: EachBuilder<EachOnly<State>, State> = EachBuilder::start().expect("each");
    let mut loop_builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("loop");
    loop_builder
        .then_body::<_, SyncFnSig<(State,), Data<State>>>(
            iter_finish_now as fn(&State) -> Result<State, BodyError>,
        )
        .expect("loop body");
    let orchestrator = loop_builder.finish().expect("loop finish");
    each.then_body::<_, OrchSig<State, Data<State>>>(orchestrator)
        .expect("each body is the loop");
    let each: Each<EachOnly<State>, State> = each.finish().expect("each finish");
    let produced: super::data_ref::DataRef<Vec<State>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<State>>>, _>(each, collection)
        .expect("flow then each");
    let read: super::data_ref::DataRef<usize> = flow
        .then::<_, SyncFnSig<(Vec<State>,), Data<usize>>, _>(
            |values: &Vec<State>| Ok(values.len()),
            produced,
        )
        .expect("read length");
    (
        flow.finish::<Data<usize>, _>(read).expect("flow finish"),
        collection_position,
    )
}

// ---------------------------------------------------------------- J23／J24：Future 本体取消

/// 带 gate 的异步 body：真实停在 Pending，供取消样本丢弃 Future 本体。
async fn gated_body(state: &State) -> Result<State, BodyError> {
    super::test_support::record("gated-body:start");
    super::test_support::gate_wait().await;
    let value = state.value + 1;
    Ok(State {
        value,
        finish: value >= 3,
    })
}

#[test]
fn j23_erased_loop_future_cancellation_cleans_layer_by_layer() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_gate();
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, AsyncFnSig<(State,), Data<State>>>(gated_body)
        .expect("gated body");
    let orchestrator = iter.finish().expect("finish");
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator, input)
        .expect("then iter");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    // 未 poll 对照：同一入口的 Future 从不推进，直接丢弃。
    {
        let mut execution = super::runtime::RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(
                &root,
                &position,
                State {
                    value: 0,
                    finish: false,
                },
            )
            .expect("state");
        let mut guard = execution
            .context_mut()
            .enter(super::context::InvocationKind::Root, &root, true)
            .expect("root frame");
        let site = parent.definition().steps()[0].site();
        let unpolled = match site {
            super::builder::CallSite::Orchestrator(site) => {
                Box::pin(site.invoke(&mut guard, &root))
            }
            super::builder::CallSite::Node(_) => {
                panic!("loop step must be an orchestrator call site")
            }
        };
        drop(unpolled);
        assert!(take_shared_events().is_empty(), "未 poll 不进入任何边界");
    }

    // Pending 后丢弃真实 Loop Future 本体；ancestor Root guard 仍存活。
    super::test_support::reset_observations();
    install_gate();
    let mut execution = super::runtime::RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(
            &root,
            &position,
            State {
                value: 0,
                finish: false,
            },
        )
        .expect("state");
    let mut guard = execution
        .context_mut()
        .enter(super::context::InvocationKind::Root, &root, true)
        .expect("root frame");
    let site = parent.definition().steps()[0].site();
    let boxed = match site {
        super::builder::CallSite::Orchestrator(site) => {
            advance_to_pending(site.invoke(&mut guard, &root), 1)
        }
        super::builder::CallSite::Node(_) => panic!("loop step must be an orchestrator call site"),
    };
    let creations = super::test_support::boundary_creation_snapshot();
    let roles: Vec<ScopeRole> = creations.iter().map(|(_, _, role)| *role).collect();
    assert_eq!(
        roles,
        vec![ScopeRole::Loop, ScopeRole::Round],
        "{creations:?}"
    );
    let loop_seq = creations[0].0.seq();
    let round_seq = creations[1].0.seq();
    let shared_before = take_shared_events();
    assert!(
        saw(&shared_before, "gated-body:start"),
        "body 真实停在 Pending: {shared_before:?}"
    );
    drop(boxed);

    // 逐层清理：最深处先退出，Round 与 Loop 各有 frame-exit 与清理区间（共享通道）。
    let events = take_shared_events();
    let leaf_exit = events
        .iter()
        .position(|event| event.starts_with("frame-exit:leaf"))
        .expect("leaf exits");
    let round_exit = events
        .iter()
        .position(|event| event == &format!("frame-exit:boundary:{round_seq}"))
        .unwrap_or_else(|| panic!("round frame exits: {events:?}"));
    let loop_exit = events
        .iter()
        .position(|event| event == &format!("frame-exit:boundary:{loop_seq}"))
        .unwrap_or_else(|| panic!("loop frame exits: {events:?}"));
    assert!(
        leaf_exit < round_exit && round_exit < loop_exit,
        "内层先退出: {events:?}"
    );
    assert!(saw_prefix_any(&events, "cleanup-start"), "{events:?}");
    assert!(saw_prefix_any(&events, "cleanup-end"), "{events:?}");
    // Root imported 保留：原输入仍由 Root 负责，未被销毁。
    assert!(
        !saw(&events, "item-dropped"),
        "Root imported 保留: {events:?}"
    );
    let drops = state_drop_values(&events);
    let unique: std::collections::HashSet<&u32> = drops.iter().collect();
    assert_eq!(drops.len(), unique.len(), "owned 各 Drop 一次: {drops:?}");
    drop(guard);
    drop(execution);
    release_gate();
}

#[test]
fn j23_ready_contrast_completes_normally() {
    // Ready 对照：同一 gated 夹具在释放挂起点后正常完成，与取消样本共用调用链。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_gate();
    release_gate();
    let (parent, position) = iter_fixture_gated();
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                State {
                    value: 0,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("ready contrast completes");
    let events = take_shared_events();
    assert_eq!(count(&events, "loop-collect:promoted"), 3, "{events:?}");
    assert!(saw(&events, "loop-finished"), "{events:?}");
}

#[test]
fn j24_owning_root_future_cancellation_orders_inner_exit_before_context() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_gate();
    let (parent, position) = iter_fixture_gated();

    // 未 poll 对照：拥有 Context 的 Root Future 从不推进。
    {
        let unpolled = Box::pin(definition_in_root::<
            fn(&mut super::context::ExecutionContext, &super::identity::ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            parent.definition(),
            vec![root_input_state(&position, 0)],
            |_, _| {},
            None,
        ));
        drop(unpolled);
        let untouched = take_shared_events();
        assert!(
            !untouched
                .iter()
                .any(|event| event.starts_with("frame-exit") || event.starts_with("cleanup")),
            "未 poll 的执行不创建 Context／Scope: {untouched:?}"
        );
    }

    // Pending 后直接丢弃真正拥有 Context 的 Root Future。
    super::test_support::reset_observations();
    install_gate();
    let boxed = advance_to_pending(
        definition_in_root::<
            fn(&mut super::context::ExecutionContext, &super::identity::ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            parent.definition(),
            vec![root_input_state(&position, 0)],
            |_, _| {},
            None,
        ),
        1,
    );
    let creations = super::test_support::boundary_creation_snapshot();
    let roles: Vec<ScopeRole> = creations.iter().map(|(_, _, role)| *role).collect();
    assert_eq!(
        roles,
        vec![ScopeRole::Loop, ScopeRole::Round],
        "{creations:?}"
    );
    let shared_before = take_shared_events();
    assert!(saw(&shared_before, "gated-body:start"), "{shared_before:?}");
    drop(boxed);

    let shared = take_shared_events();
    let events = &shared;
    // 内层（Leaf／Round／Loop）退出先于 Context／Container 析构。
    let inner_exit = events
        .iter()
        .position(|event| event.starts_with("frame-exit"))
        .expect("inner frame exits");
    let context_drop = events
        .iter()
        .position(|event| event == "context-drop")
        .unwrap_or(events.len());
    assert!(
        inner_exit < context_drop,
        "内层清理先于 Context 析构: {events:?}"
    );
    let drops = state_drop_values(&shared);
    let unique: std::collections::HashSet<&u32> = drops.iter().collect();
    assert_eq!(drops.len(), unique.len(), "owned 各 Drop 一次: {drops:?}");
    release_gate();
}

fn root_input_state(position: &super::ref_id::RefId, value: u32) -> super::test_support::RootInput {
    let position = position.clone();
    (
        position,
        Box::new(
            move |ctx: &mut super::context::ExecutionContext, position: &super::ref_id::RefId| {
                ctx.register_owned(
                    &ctx.root_scope(),
                    position,
                    State {
                        value,
                        finish: false,
                    },
                )
                .expect("state input");
            },
        ),
    )
}

fn saw_prefix_any(events: &[String], prefix: &str) -> bool {
    events.iter().any(|event| event.starts_with(prefix))
}

fn iter_fixture_gated() -> (Flow<(State,), Data<u32>>, super::ref_id::RefId) {
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, AsyncFnSig<(State,), Data<State>>>(gated_body)
        .expect("gated body");
    let orchestrator = iter.finish().expect("finish");
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator, input)
        .expect("then iter");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    (
        parent.finish::<Data<u32>, _>(read).expect("parent finish"),
        position,
    )
}

// ---------------------------------------------------------------- J25：复用

#[test]
fn j25_loop_reuse_across_callsites_and_executions() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    // 同一个完成态 Loop 加到父 Flow 的两个位置，各自独立执行。
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, SyncFnSig<(State,), Data<State>>>(
        iter_finish_now as fn(&State) -> Result<State, BodyError>,
    )
    .expect("iter body");
    let orchestrator: Loop<Iter1<State>> = iter.finish().expect("finish");
    let (mut parent, handles) = FlowBuilder::<(State, State)>::start().expect("parent");
    let (first, second) = handles;
    let first_position = first.position().clone();
    let second_position = second.position().clone();
    let first_out: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator.clone(), first)
        .expect("first call site");
    let second_out: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator, second)
        .expect("second call site");
    let read_first: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            first_out,
        )
        .expect("read first");
    let read_second: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            second_out,
        )
        .expect("read second");
    let _ = &read_second;
    let parent = parent
        .finish::<Data<u32>, _>(read_first)
        .expect("parent finish");

    for run in 0..2 {
        drive(definition_in_root(
            parent.definition(),
            Vec::new(),
            |ctx, _root| {
                ctx.register_owned(
                    &ctx.root_scope(),
                    &first_position,
                    State {
                        value: run,
                        finish: false,
                    },
                )
                .expect("first");
                ctx.register_owned(
                    &ctx.root_scope(),
                    &second_position,
                    State {
                        value: run + 1,
                        finish: false,
                    },
                )
                .expect("second");
            },
            None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
        ))
        .expect("reuse run");
    }
    let events = take_shared_events();
    assert_eq!(
        count(&events, "loop-collect:promoted"),
        4,
        "两处调用点 × 两次 Execution 各自独立推进: {events:?}"
    );
    let creations = super::test_support::boundary_creation_snapshot();
    let loop_scopes: Vec<_> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Loop)
        .collect();
    assert_eq!(
        loop_scopes.len(),
        4,
        "每次调用都建立新的 LoopScope: {creations:?}"
    );
    // 身份不混：按 Scope 身份去重（比较文本身份，避免把可变类型放进 HashSet）。
    let mut rendered: Vec<String> = loop_scopes
        .iter()
        .map(|(scope, _, _)| scope.to_string())
        .collect();
    rendered.sort();
    rendered.dedup();
    assert_eq!(
        rendered.len(),
        4,
        "ControlState／Scope 身份不混: {loop_scopes:?}"
    );
}

// ---------------------------------------------------------------- J27：权限与借用

#[test]
fn j27_borrow_blocks_controlled_mutation_until_it_ends() {
    // 真实 &State 借用持有期间，受控 mutation 必须被借用检查拒绝（编译期），
    // 借用结束后正例可运行；本样本以 UI 夹具 `j27_borrow_blocks_state_promotion.rs`
    // 提供 E0502 证据，这里只做运行期正例：body 借用结束后 Promote 成功。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    let (parent, position) = iter_fixture();
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                State {
                    value: 3,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("borrow ends before mutation");
    let events = take_shared_events();
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "{events:?}");
}

#[test]
fn j28_prior_root_driver_still_works() {
    // 前序驱动未被本次改动影响：普通 Flow 经空声明收口仍可运行，且父后步真实执行。
    let (mut flow, input) = FlowBuilder::<(u32,)>::start().expect("flow");
    let position = input.position().clone();
    let read: super::data_ref::DataRef<u32> = flow
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(
            marker_node as fn(&u32) -> Result<u32, BodyError>,
            input,
        )
        .expect("step");
    let flow = flow.finish::<Data<u32>, _>(read).expect("finish");
    drive(definition_in_root(
        flow.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &position, 5u32)
                .expect("root input");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("plain flow still runs");
    assert!(
        saw(&take_shared_events(), "after-loop-step"),
        "父后步真实执行"
    );
}

#[test]
fn j25_definition_is_immutable_across_executions() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    let (parent, position) = iter_fixture();
    let before = parent.definition().allocated_probe();
    for run in 0..2 {
        drive(definition_in_root(
            parent.definition(),
            Vec::new(),
            |ctx, _root| {
                ctx.register_owned(
                    &ctx.root_scope(),
                    &position,
                    State {
                        value: 2 + run,
                        finish: false,
                    },
                )
                .expect("state");
            },
            None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
        ))
        .expect("execution");
    }
    assert_eq!(
        parent.definition().allocated_probe(),
        before,
        "完成态不缓存运行状态、不新增 Ref"
    );
}

// ---------------------------------------------------------------- 未使用导入的显式引用

#[test]
fn j28_shared_observation_channels_are_isolated() {
    // 新观察通道与前序共享事件序列分离：Looop 样本不写入 `take_events` 的清理次序。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_body_failure(false);
    let (parent, position) = iter_fixture();
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                State {
                    value: 3,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("run");
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    assert!(!snapshots.is_empty(), "Round 收口观察走独立通道");
}

/// 保留 `LoopDecision` 的二值见证（形状与 reader 的返回类型绑定）。
#[allow(dead_code)]
fn decisions() -> [LoopDecision; 2] {
    [LoopDecision::Continue, LoopDecision::Finish]
}
