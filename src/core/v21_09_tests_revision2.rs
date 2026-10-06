//! V21-09 验收样本（四）：第二次复审 R11～R16 剩余条件的补齐证据。
//!
//! 对照[任务书 §16](../docs/tasks/V21_09_Loop_Round_And_State_Promotion.md)的剩余关闭条件：
//! 同一 Future Pending→Ready、真实 descendant Flow 错误、两类有限 Continue 场景、Export／cap
//! 完整原子性、tuple／unit 正式样本、typed 权限断言与保存诊断。

use std::cell::{Cell, RefCell};

use super::builder::TypedCallBuilder;
use super::context::{BodyError, ExecutionContext};
use super::each::{Each, EachBuilder, EachOnly};
use super::flow::{Flow, FlowBuilder};
use super::identity::{DataId, ScopeId};
use super::internal_error::ScopeError;
use super::loop_orchestrator::{
    Iter1, Loop, LoopBuilder, LoopControl, LoopDecision, LoopFault, Retry1, install_loop_fault,
};
use super::match_orchestrator::MatchBuilder;
use super::node::NodeCall1;
use super::orchestrator::{OrchCall, ScopeRole};
use super::ref_id::RefId;
use super::scope::TargetSnapshot;
use super::signature::{AsyncFnSig, Data, NodeFut, NodeSig, OrchSig, SyncFnSig};
use super::test_support::{
    ExportSnapshotPhase, RootView, definition_in_root, drive, gate_wait, install_gate,
    release_gate, saw, saw_prefix, take_export_pre_cleanup, take_generic_promote_probe,
    take_shared_events,
};
use super::v21_09_tests::{Item, Rules, State};

// ---------------------------------------------------------------- 业务与 body

thread_local! {
    /// 一次性挂起标志：只安装一次 gate。
    static GATE_ARMED: Cell<bool> = const { Cell::new(false) };
}

/// 每轮路由计数（由普通结构体 Node 的配置产出 Data）。
pub(crate) struct RouteNode(pub(crate) Cell<u32>);

impl NodeCall1<State, Data<u32>> for RouteNode {
    fn call<'a>(&'a self, _state: &'a State) -> NodeFut<'a, u32> {
        let round = self.0.get() + 1;
        self.0.set(round);
        Box::pin(async move { Ok(round) })
    }
}

/// 新产生的 Continue 值（owned，finish=false）。
fn new_continue(state: &State) -> Result<State, BodyError> {
    Ok(State {
        value: state.value + 1,
        finish: false,
    })
}

/// 新产生的 Finish 值（owned，finish=true）。
fn new_finish(state: &State) -> Result<State, BodyError> {
    Ok(State {
        value: state.value + 10,
        finish: true,
    })
}

/// identity 完成态 Flow：把导入状态原样声明为输出（重新暴露同一 DataId）。
fn identity_state() -> Flow<(State,), Data<State>> {
    let (flow, handle) = FlowBuilder::<(State,)>::start().expect("identity flow");
    flow.finish::<Data<State>, _>(handle)
        .expect("identity finish")
}

/// 有限 Continue 序列的 body：由既有 Match 依据普通 Node 产出的路由键选择 branch。
fn routed_body(iter: bool) -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body flow");
    let route: super::data_ref::DataRef<u32> = flow
        .then::<_, NodeSig<(State,), Data<u32>>, _>(RouteNode(Cell::new(0)), state.clone())
        .expect("route step");
    let mut matched = MatchBuilder::<u32, State, Data<State>>::start().expect("match");
    if iter {
        // 第 1 轮：新 owned Continue；第 2 轮：重新暴露同一 Loop-owned DataId（仍 Continue）。
        matched
            .branch::<_, SyncFnSig<(State,), Data<State>>>(
                1,
                new_continue as fn(&State) -> Result<State, BodyError>,
            )
            .expect("continue branch");
        matched
            .branch::<_, OrchSig<State, Data<State>>>(2, identity_state())
            .expect("identity branch");
    } else {
        // 第 1 轮：重新暴露 imported alias（Continue）；第 2 轮起：新 owned Finish。
        matched
            .branch::<_, OrchSig<State, Data<State>>>(1, identity_state())
            .expect("identity branch");
    }
    matched
        .default::<_, SyncFnSig<(State,), Data<State>>>(
            new_finish as fn(&State) -> Result<State, BodyError>,
        )
        .expect("default finish");
    let out: super::data_ref::DataRef<State> = flow
        .then::<_, OrchSig<(u32, State), Data<State>>, _>(
            matched.finish().expect("match finish"),
            (route, state),
        )
        .expect("match step");
    flow.finish::<Data<State>, _>(out).expect("body finish")
}

/// 真实 descendant Flow：内部 Step 在其自己的 child Scope 中执行。
fn descendant_flow() -> Flow<(State,), Data<State>> {
    let (mut inner, state) = FlowBuilder::<(State,)>::start().expect("descendant flow");
    let out: super::data_ref::DataRef<State> = inner
        .then::<_, SyncFnSig<(State,), Data<State>>, _>(
            (|state: &State| {
                super::test_support::record("descendant-step");
                Ok(State {
                    value: state.value + 1,
                    finish: true,
                })
            }) as fn(&State) -> Result<State, BodyError>,
            state,
        )
        .expect("descendant step");
    inner
        .finish::<Data<State>, _>(out)
        .expect("descendant finish")
}

/// 带真实 descendant 的 body：单 Step = descendant Flow（child Scope）。
fn descendant_body() -> Flow<(State,), Data<State>> {
    let (mut wrapper, state) = FlowBuilder::<(State,)>::start().expect("wrapper flow");
    let out: super::data_ref::DataRef<State> = wrapper
        .then::<_, OrchSig<State, Data<State>>, _>(descendant_flow(), state)
        .expect("descendant call");
    wrapper
        .finish::<Data<State>, _>(out)
        .expect("wrapper finish")
}

/// descendant 内的深层真实失败（第 2 轮）。
fn deep_descendant_step(state: &State) -> Result<State, BodyError> {
    super::test_support::record(&format!("descendant-step:{}", state.value));
    if state.value >= 1 {
        return Err(BodyError::from(ScopeError::Invariant {
            violated: "nested descendant failure probe",
        }));
    }
    Ok(State {
        value: state.value + 1,
        finish: false,
    })
}

/// 深层失败版本：body 的单 Step 是 descendant Flow，descendant 内部第 2 轮失败。
fn deep_descendant_body() -> Flow<(State,), Data<State>> {
    let (mut inner, state) = FlowBuilder::<(State,)>::start().expect("descendant flow");
    let out: super::data_ref::DataRef<State> = inner
        .then::<_, SyncFnSig<(State,), Data<State>>, _>(
            deep_descendant_step as fn(&State) -> Result<State, BodyError>,
            state,
        )
        .expect("descendant step");
    let inner = inner
        .finish::<Data<State>, _>(out)
        .expect("descendant finish");
    let (mut wrapper, state) = FlowBuilder::<(State,)>::start().expect("wrapper flow");
    let out: super::data_ref::DataRef<State> = wrapper
        .then::<_, OrchSig<State, Data<State>>, _>(inner, state)
        .expect("descendant call");
    wrapper
        .finish::<Data<State>, _>(out)
        .expect("wrapper finish")
}

/// Root 输入：把 `State` 登记到给定位置（只代表 Application 交接）。
pub(crate) fn root_input_state(position: &RefId, value: u32) -> super::test_support::RootInput {
    let position = position.clone();
    (
        position,
        Box::new(move |ctx: &mut ExecutionContext, position: &RefId| {
            ctx.register_owned(
                &ctx.root_scope(),
                position,
                State {
                    value,
                    finish: false,
                },
            )
            .expect("state input");
        }),
    )
}

/// 读取见证。
fn read_value(state: &State) -> Result<u32, BodyError> {
    super::test_support::record("read-node-called");
    Ok(state.value)
}

/// 挂起 body：第 `arm_at` 轮起在 gate 上 Pending。
async fn gated_round(arm_at: u32, state: &State) -> Result<State, BodyError> {
    let round = state.value;
    super::test_support::record(&format!("gated-round:{round}"));
    if round + 1 >= arm_at {
        // 只安装一次：释放后不再重新挂起（同一 Future 可继续推进到 Ready）。
        if !GATE_ARMED.with(|slot| slot.replace(true)) {
            install_gate();
        }
        // 借用结束见证：本局部在 Future 被丢弃时析构，早于 Scope 清理。
        let _borrow_witness = BorrowWitness;
        gate_wait().await;
    }
    Ok(State {
        value: state.value + 1,
        finish: state.value + 1 >= 9,
    })
}

struct BorrowWitness;

impl Drop for BorrowWitness {
    fn drop(&mut self) {
        super::test_support::record("borrow-witness-dropped");
    }
}

async fn gated_round_2(state: &State) -> Result<State, BodyError> {
    gated_round(2, state).await
}

async fn gated_round_3(state: &State) -> Result<State, BodyError> {
    gated_round(3, state).await
}

// ---------------------------------------------------------------- 夹具

/// Iter 夹具（Loop 的 body 由调用方给出），返回父 Flow 与输入位置。
fn iter_parent(body: Loop<Iter1<State>>) -> (Flow<(State,), Data<u32>>, RefId) {
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(body, input)
        .expect("then loop");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_value as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    (
        parent.finish::<Data<u32>, _>(read).expect("parent finish"),
        position,
    )
}

fn iter_loop(body: Flow<(State,), Data<State>>) -> Loop<Iter1<State>> {
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(body)
        .expect("body");
    builder.finish().expect("finish")
}

// ---------------------------------------------------------------- 有限 Continue 序列（§16.3）

#[test]
fn s17_iter_owned_same_id_continue_then_owned_finish() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    let (parent, position) = iter_parent(iter_loop(routed_body(true)));
    let state_id: RefCell<Option<DataId>> = RefCell::new(None);
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            let id = ctx
                .register_owned(
                    &ctx.root_scope(),
                    &position,
                    State {
                        value: 0,
                        finish: false,
                    },
                )
                .expect("state");
            *state_id.borrow_mut() = Some(id);
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("finite Iter sequence");

    let events = take_shared_events();
    assert_eq!(
        super::test_support::boundary_creation_snapshot()
            .into_iter()
            .filter(|(_, _, role)| *role == ScopeRole::Round)
            .count(),
        3,
        "三轮真实 Round: {events:?}"
    );
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let before: Vec<_> = snapshots
        .iter()
        .filter(|s| s.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .collect();
    assert_eq!(before.len(), 3, "三轮 Before: {snapshots:?}");
    // 第 2 轮：重新暴露同一 Loop-owned DataId（Continue），不增 owner／pending。
    assert_eq!(
        before[1].selected_data, before[0].selected_data,
        "第 2 轮仍是同一 DataId: {before:?}"
    );
    assert_eq!(
        before[1].selected_owner.as_ref(),
        before[1].controller.as_ref(),
        "同一值由 Loop 负责: {before:?}"
    );
    assert_eq!(
        before[1].state_target,
        Some(TargetSnapshot::Data(
            before[0].selected_data.clone().expect("S1")
        )),
        "current-state 就是该 Loop-owned 值"
    );
    assert_eq!(
        before[1].state_pending.as_ref().map(Vec::len),
        Some(0),
        "不重复 pending"
    );
    // 第 3 轮：新 owned Finish。
    assert_ne!(
        before[2].selected_data, before[1].selected_data,
        "第 3 轮产生新值"
    );
    // S0 全程 Root-owned 且运行中未被销毁。
    assert!(
        events
            .iter()
            .position(|event| event == "state-dropped:0")
            .is_some_and(|at| at
                > events
                    .iter()
                    .position(|e| e == "loop-finished")
                    .expect("finished")),
        "Root 输入只在收口后销毁: {events:?}"
    );
}

#[test]
fn s17_retry_imported_alias_continue_then_owned_finish() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    let mut builder: LoopBuilder<Retry1<State, State>> = LoopBuilder::start().expect("retry");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(routed_body(false))
        .expect("body");
    let loop_ = builder.finish().expect("finish");
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(loop_, input)
        .expect("then retry");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_value as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let read_position = read.position().clone();
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");
    let job_id: RefCell<Option<DataId>> = RefCell::new(None);
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            let id = ctx
                .register_owned(
                    &ctx.root_scope(),
                    &position,
                    State {
                        value: 0,
                        finish: false,
                    },
                )
                .expect("input");
            *job_id.borrow_mut() = Some(id);
        },
        Some(move |view: &mut RootView<'_, '_>| {
            // 最终把新 owned 结果交给父后步（原 alias 只在前一轮被重新暴露）。
            assert_eq!(
                *view.resolve::<u32>(&read_position)?,
                10,
                "最终新 owned 结果"
            );
            Ok(())
        }),
    ))
    .expect("finite Retry sequence");

    let events = take_shared_events();
    assert_eq!(
        super::test_support::count(&events, "loop-collect:discarded"),
        1,
        "{events:?}"
    );
    assert_eq!(
        super::test_support::count(&events, "loop-collect:promoted"),
        1,
        "{events:?}"
    );
    assert_eq!(
        super::test_support::count(&events, "read-node-called"),
        1,
        "父后步真实执行一次: {events:?}"
    );
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let first = snapshots
        .iter()
        .find(|s| s.operation == super::test_support::RoundCollectOperation::Discard)
        .expect("discard snapshot");
    // Continue 轮：被选目标就是 imported alias（Root-owned），只关闭本轮 refs。
    assert_eq!(first.selected_data, job_id.borrow().clone(), "{first:?}");
    assert_eq!(
        first.selected_owner.as_ref().map(ScopeId::seq),
        Some(0),
        "原 owner 是 Root"
    );
    assert_eq!(
        first.state_pending.as_ref().map(Vec::len),
        Some(0),
        "{first:?}"
    );
    assert!(
        !super::test_support::take_shared_events()
            .iter()
            .any(|event| event == "state-dropped:0"),
        "原 alias 不被销毁"
    );
}

// ---------------------------------------------------------------- 同一 Future：Pending→Ready

#[test]
fn s17_same_future_pending_then_ready_with_counts_and_owner_evidence() {
    super::test_support::reset_observations();
    GATE_ARMED.with(|slot| slot.set(false));
    super::test_support::boundary_creation_reset();
    let (parent, position) = iter_parent(iter_loop_async());
    let definition = parent.definition();
    // 未 poll 对照：同一入口不推进，不创建 Context／Scope。
    {
        let unpolled = Box::pin(definition_in_root::<
            fn(&mut ExecutionContext, &ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            definition,
            vec![root_input_state(&position, 0)],
            |_, _| {},
            None,
        ));
        drop(unpolled);
        let events = take_shared_events();
        assert!(
            !events
                .iter()
                .any(|event| event.starts_with("frame-exit") || event.starts_with("cleanup")),
            "未 poll 不创建 Scope: {events:?}"
        );
    }

    // Pending：同一 Future 停在真实借用；断言下一轮／父后步为 0、创建计数各 +1、Root 三地址。
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    install_gate();
    let before_contexts = super::context::creation_counts::contexts();
    let before_coordinators = super::context::creation_counts::coordinators();
    let before_containers = super::context::creation_counts::containers();
    let mut boxed = Box::pin(definition_in_root::<
        fn(&mut ExecutionContext, &ScopeId),
        fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
    >(
        definition,
        vec![root_input_state(&position, 0)],
        |_, _| {},
        None,
    ));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..64 {
        if boxed.as_mut().poll(&mut cx).is_pending() {
            break;
        }
    }
    let pending_events = take_shared_events();
    assert!(
        saw(&pending_events, "gated-round:0"),
        "首轮进入真实借用: {pending_events:?}"
    );
    assert!(
        saw(&pending_events, "gated-round:1"),
        "Pending 发生在第 2 轮: {pending_events:?}"
    );
    assert!(
        !saw(&pending_events, "gated-round:2"),
        "Pending 时不建立下一轮"
    );
    assert!(
        !saw(&pending_events, "read-node-called"),
        "Pending 时父后步为 0"
    );
    assert_eq!(
        super::context::creation_counts::contexts(),
        before_contexts + 1,
        "Context 创建计数 +1"
    );
    assert_eq!(
        super::context::creation_counts::coordinators(),
        before_coordinators + 1,
        "Coordinator 创建计数 +1"
    );
    assert_eq!(
        super::context::creation_counts::containers(),
        before_containers + 1,
        "Container 创建计数 +1"
    );
    let addresses = super::test_support::boundary_address_snapshot();
    let (_, identity, coordinator, container) = &addresses[0];
    assert!(
        addresses
            .iter()
            .all(|(_, i, c, t)| i == identity && c == coordinator && t == container),
        "同一执行域地址: {addresses:?}"
    );

    // Ready：释放 gate 后继续 poll **同一个** Future 到完成。
    release_gate();
    for _ in 0..256 {
        if boxed.as_mut().poll(&mut cx).is_ready() {
            break;
        }
    }
    let mut events = pending_events;
    events.extend(take_shared_events());
    assert!(
        saw(&events, "gated-round:2"),
        "同一 Future 继续推进: {events:?}"
    );
    assert_eq!(
        super::test_support::count(&events, "read-node-called"),
        1,
        "Ready 后父后步执行一次: {events:?}"
    );
    assert!(saw(&events, "loop-finished"), "{events:?}");
    // 正常关闭：Root 输入直到 Loop 完成前保留，随后由 Root 自身收口销毁。
    let finished = events
        .iter()
        .position(|event| event == "loop-finished")
        .expect("loop finished");
    assert!(
        events
            .iter()
            .position(|event| event == "state-dropped:0")
            .is_some_and(|at| at > finished),
        "Root 输入在 Loop 运行期间保留: {events:?}"
    );
    // S0／shared 的运行中 owner 与存活：由收口快照的 controller_refs 指向 Root-owned 目标证明。
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let first = snapshots
        .iter()
        .find(|s| s.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .expect("before");
    // 第 1 轮 Before：current-state 是 Root-owned 的初始值，被选值是本轮新产生的结果。
    assert!(
        matches!(first.state_target, Some(TargetSnapshot::Data(_))),
        "初始 current-state 来自 Root 输入: {first:?}"
    );
    assert_ne!(
        first.selected_data,
        first.state_target.as_ref().and_then(|t| match t {
            TargetSnapshot::Data(id) => Some(id.clone()),
            _ => None,
        })
    );
    assert_eq!(
        first.selected_owner.as_ref(),
        first.source.as_ref(),
        "新产生结果由 Round 负责"
    );
}

fn iter_loop_async() -> Loop<Iter1<State>> {
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    builder
        .then_body::<_, AsyncFnSig<(State,), Data<State>>>(gated_round_2)
        .expect("body");
    builder.finish().expect("finish")
}

// ---------------------------------------------------------------- 真实 descendant Flow 错误

#[test]
fn s17_real_descendant_flow_error_preserves_scope_and_cleans_current() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    // 正例：descendant body 真实建立 child Flow Scope。
    let (parent_ok, position_ok) = iter_parent(iter_loop(descendant_body()));
    drive(definition_in_root(
        parent_ok.definition(),
        Vec::new(),
        move |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position_ok,
                State {
                    value: 0,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("descendant positive");
    let creations = super::test_support::boundary_creation_snapshot();
    assert!(
        creations
            .iter()
            .filter(|(_, _, role)| *role == ScopeRole::Flow)
            .count()
            >= 1,
        "descendant 建立真实 child Flow Scope: {creations:?}"
    );

    // 深层失败：descendant 内 Step 在其 child Scope 失败，保留该 Scope 诊断。
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    let (parent_deep, position_deep) = iter_parent(iter_loop(deep_descendant_body()));
    let error = drive(definition_in_root(
        parent_deep.definition(),
        Vec::new(),
        move |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position_deep,
                State {
                    value: 0,
                    finish: false,
                },
            )
            .expect("state");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("descendant failure");
    assert!(
        matches!(
            error.scope_error(),
            Some(ScopeError::Invariant { violated }) if *violated == "nested descendant failure probe"
        ),
        "保留真实深层诊断: {error:?}"
    );
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "{saved:?}");
    // 定位是真实最深的 descendant Flow Scope（比 Round 更深），而不是外层。
    let deepest_flow_seq = super::test_support::boundary_creation_snapshot()
        .into_iter()
        .rfind(|(_, _, role)| *role == ScopeRole::Flow)
        .expect("descendant flow scope")
        .0
        .seq();
    let round_seqs: Vec<u64> = super::test_support::boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, role)| *role == ScopeRole::Round)
        .map(|(scope, _, _)| scope.seq())
        .collect();
    assert_eq!(
        saved[0].scope.as_ref().map(ScopeId::seq),
        Some(deepest_flow_seq)
    );
    assert!(!round_seqs.contains(&deepest_flow_seq), "不是 Round 定位");
}

// ---------------------------------------------------------------- Export 原子性与目的 cap

#[test]
fn s17_export_caller_conflict_full_atomicity() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    let (mut parent, input) = FlowBuilder::<(Item,)>::start().expect("parent");
    let position = input.position().clone();
    let mut builder: LoopBuilder<Retry1<Item, State>> = LoopBuilder::start().expect("retry");
    builder
        .then_body::<_, OrchSig<Item, Data<State>>>(finish_item_flow())
        .expect("body");
    let loop_ = builder.finish().expect("finish");
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<Item, Data<State>>, _>(loop_, input)
        .expect("then loop");
    let loop_caller = produced.position().clone();
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_value as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");
    let occupied: RefCell<Option<DataId>> = RefCell::new(None);
    let error = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &position, Item { id: 3 })
                .expect("input");
            let id = ctx
                .register_owned(&ctx.root_scope(), &loop_caller, 42u32)
                .expect("occupy loop caller");
            *occupied.borrow_mut() = Some(id);
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("caller conflict");
    match error.scope_error() {
        Some(ScopeError::RefAlreadyBound { scope, position }) => {
            assert_eq!(scope.seq(), 0, "冲突 Scope 是父 Root");
            assert_eq!(*position, loop_caller, "冲突位置是 Loop caller");
        }
        other => panic!("expected RefAlreadyBound, got {other:?}"),
    }
    let events = take_shared_events();
    assert_eq!(
        super::test_support::count(&events, "read-node-called"),
        0,
        "父 read Node 调用数=0: {events:?}"
    );
    let snapshots = take_export_pre_cleanup();
    let is_loop_export = |s: &&super::test_support::ExportPreCleanupSnapshot| {
        s.slots.iter().any(|(_, caller, _)| *caller == loop_caller)
    };
    let before = snapshots
        .iter()
        .find(|s| s.phase == ExportSnapshotPhase::Before && is_loop_export(s))
        .expect("loop export before");
    let after = snapshots
        .iter()
        .find(|s| s.phase == ExportSnapshotPhase::AfterReject && is_loop_export(s))
        .expect("loop export after-reject");
    assert!(before.observation_error.is_none() && after.observation_error.is_none());
    assert_eq!(before.child, after.child, "来源 Scope 未变");
    assert_eq!(before.caller, after.caller);
    assert_eq!(before.child_refs, after.child_refs, "来源 refs 未变");
    assert_eq!(before.child_owned, after.child_owned, "来源 owned 未变");
    assert_eq!(before.caller_refs, after.caller_refs, "caller refs 未变");
    assert_eq!(before.caller_owned, after.caller_owned, "caller owned 未变");
    assert_eq!(before.slots, after.slots, "slot 身份未变");
    assert_eq!(
        before.conflict_target, after.conflict_target,
        "冲突目标未变"
    );
    assert_eq!(before.conflict_owner, after.conflict_owner);
    assert_eq!(before.conflict_alive, after.conflict_alive);
    assert_eq!(before.next_data_id, after.next_data_id, "拒绝不取号");
    assert!(before.conflict_alive, "预占值仍存活");
    assert_eq!(
        before.conflict_target,
        Some(TargetSnapshot::Data(
            occupied.borrow().clone().expect("occupied id")
        )),
        "旧 target 未被覆盖"
    );
}

fn finish_item_flow() -> Flow<(Item,), Data<State>> {
    let (mut flow, item) = FlowBuilder::<(Item,)>::start().expect("body flow");
    let out: super::data_ref::DataRef<State> = flow
        .then::<_, SyncFnSig<(Item,), Data<State>>, _>(
            (|item: &Item| {
                Ok(State {
                    value: item.id,
                    finish: true,
                })
            }) as fn(&Item) -> Result<State, BodyError>,
            item,
        )
        .expect("finish step");
    flow.finish::<Data<State>, _>(out).expect("body finish")
}

#[test]
fn s17_promote_destination_cap_outside_is_rejected_with_baselines() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    install_loop_fault(LoopFault::PromoteDestCapOutside);
    let (flow, collection_position) = item_alias_fixture_for_revision2();
    let error = drive(definition_in_root(
        flow.definition(),
        Vec::new(),
        move |ctx, _root| {
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
    .expect_err("destination cap outside rejected");
    match error.scope_error() {
        Some(ScopeError::ItemOutsideCap {
            cap,
            requester,
            position,
        }) => {
            assert!(
                cap.seq() > 0 && requester.seq() > 0 && position.seq() > 0,
                "{error:?}"
            );
        }
        other => panic!("expected ItemOutsideCap, got {other:?}"),
    }
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let before = snapshots
        .iter()
        .find(|s| s.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .expect("before");
    let after = snapshots
        .iter()
        .find(|s| s.phase == super::test_support::RoundCollectSnapshotPhase::AfterReject)
        .expect("after");
    assert!(before.observation_error.is_none() && after.observation_error.is_none());
    assert_eq!(before.state_target, after.state_target, "state target 未变");
    assert_eq!(before.state_pending, after.state_pending, "pending 未变");
    assert_eq!(before.source_refs, after.source_refs, "来源 refs 未变");
    assert_eq!(before.source_owned, after.source_owned, "来源 owned 未变");
    assert_eq!(
        before.controller_refs, after.controller_refs,
        "控制器 refs 未变"
    );
    assert_eq!(
        before.controller_owned, after.controller_owned,
        "控制器 owned 未变"
    );
    assert_eq!(
        before.selected_target, after.selected_target,
        "被选目标未变"
    );
    assert_eq!(before.selected_owner, after.selected_owner);
    assert_eq!(before.selected_alive, after.selected_alive);
    assert_eq!(before.next_data_id, after.next_data_id, "不取号");
    // 实际目的身份：cap 与 requester 都是真实 Scope（Loop 与 Round）。
    assert_eq!(before.controller.as_ref().map(ScopeId::seq), Some(4));
}

#[test]
fn s17_export_destination_cap_outside_is_rejected_with_baselines() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    install_loop_fault(LoopFault::ExportDestCapOutside);
    let (flow, collection_position) = item_alias_fixture_for_revision2();
    let error = drive(definition_in_root(
        flow.definition(),
        Vec::new(),
        move |ctx, _root| {
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
    .expect_err("export destination cap outside rejected");
    // 目的端诊断的精确变体与字段：cap 是来源实际 Scope，destination 是 caller。
    match error.scope_error() {
        Some(ScopeError::ItemCapEscape {
            position,
            cap,
            destination,
        }) => {
            assert!(position.seq() > 0, "位置字段: {error:?}");
            assert_eq!(cap.seq(), 4, "cap 是实际 LoopScope: {error:?}");
            assert_ne!(
                cap, destination,
                "destination 是 caller（cap 之外）: {error:?}"
            );
        }
        other => panic!("expected ItemCapEscape, got {other:?}"),
    }
    let snapshots = take_export_pre_cleanup();
    let before = snapshots
        .iter()
        .rfind(|s| s.phase == ExportSnapshotPhase::Before)
        .expect("export before");
    let after = snapshots
        .iter()
        .rfind(|s| s.phase == ExportSnapshotPhase::AfterReject)
        .expect("export after");
    assert!(before.observation_error.is_none() && after.observation_error.is_none());
    assert_eq!(before.child, after.child, "来源 Scope 未变");
    assert_eq!(before.caller, after.caller, "caller 未变");
    assert_eq!(before.child_refs, after.child_refs, "来源 refs 未变");
    assert_eq!(before.child_owned, after.child_owned, "来源 owned 未变");
    assert_eq!(before.caller_refs, after.caller_refs, "caller refs 未变");
    assert_eq!(before.caller_owned, after.caller_owned, "caller owned 未变");
    assert_eq!(before.slots, after.slots, "slot 未变");
    assert_eq!(before.conflict_target, after.conflict_target);
    assert_eq!(before.conflict_owner, after.conflict_owner);
    assert_eq!(before.conflict_alive, after.conflict_alive);
    assert_eq!(before.caller_state, after.caller_state);
    assert_eq!(before.next_data_id, after.next_data_id, "不取号");
}

/// Each item 作为 Loop 初始状态、body 由 identity Flow 重新暴露 item 的夹具。
fn item_alias_fixture_for_revision2() -> (Flow<(Vec<State>,), Data<usize>>, RefId) {
    let (mut flow, collection) = FlowBuilder::<(Vec<State>,)>::start().expect("flow");
    let collection_position = collection.position().clone();
    let mut each: EachBuilder<EachOnly<State>, State> = EachBuilder::start().expect("each");
    let mut inner_builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("loop");
    inner_builder
        .then_body::<_, OrchSig<State, Data<State>>>(identity_state())
        .expect("identity body");
    let orchestrator = inner_builder.finish().expect("loop finish");
    let (mut wrapper, item) = FlowBuilder::<(State,)>::start().expect("wrapper");
    let alias: super::data_ref::DataRef<State> = wrapper
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator, item)
        .expect("wrapper then loop");
    let out: super::data_ref::DataRef<State> = wrapper
        .then::<_, SyncFnSig<(State,), Data<State>>, _>(
            (|state: &State| {
                Ok(State {
                    value: state.value + 100,
                    finish: true,
                })
            }) as fn(&State) -> Result<State, BodyError>,
            alias,
        )
        .expect("derive step");
    let wrapper = wrapper
        .finish::<Data<State>, _>(out)
        .expect("wrapper finish");
    each.then_body::<_, OrchSig<State, Data<State>>>(wrapper)
        .expect("each body");
    let each: Each<EachOnly<State>, State> = each.finish().expect("each finish");
    let produced: super::data_ref::DataRef<Vec<State>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<State>>>, _>(each, collection)
        .expect("flow then each");
    let read: super::data_ref::DataRef<usize> = flow
        .then::<_, SyncFnSig<(Vec<State>,), Data<usize>>, _>(
            (|values: &Vec<State>| Ok(values.len())) as fn(&Vec<State>) -> Result<usize, BodyError>,
            produced,
        )
        .expect("read length");
    (
        flow.finish::<Data<usize>, _>(read).expect("flow finish"),
        collection_position,
    )
}

// ---------------------------------------------------------------- typed 权限与保存诊断

#[test]
fn s17_generic_promote_scope_identity_is_typed() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    install_loop_fault(LoopFault::GenericPromoteFullProbe);
    let (parent, position) = iter_parent(iter_loop(finish_immediately_body()));
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        move |ctx, _root| {
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
    .expect("legal path completes");
    let probes = take_generic_promote_probe();
    assert_eq!(probes.len(), 1, "{probes:?}");
    let creations = super::test_support::boundary_creation_snapshot();
    let loop_scope = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Loop)
        .expect("loop scope")
        .0
        .clone();
    let round_scope = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Round)
        .expect("round scope")
        .0
        .clone();
    match probes[0].outcome.as_ref() {
        Some(ScopeError::OutsideInvocation { scope, current }) => {
            // typed 精确匹配：字段互换或换 Scope 都会失败。
            assert_eq!(*scope, loop_scope, "第一端是实际 LoopScope");
            assert_eq!(
                current.as_ref(),
                Some(&round_scope),
                "current 是实际 RoundScope"
            );
        }
        other => panic!("expected OutsideInvocation, got {other:?}"),
    }
}

#[test]
fn s17_permit_wrong_state_owner_is_rejected_before_commit() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    install_loop_fault(LoopFault::PermitProbeWrongStateOwner);
    let (parent, position) = iter_parent(iter_loop(finish_immediately_body()));
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        move |ctx, _root| {
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
    .expect("legal path completes");
    let probes = take_generic_promote_probe();
    assert_eq!(probes.len(), 1, "{probes:?}");
    match probes[0].outcome.as_ref() {
        Some(ScopeError::Invariant { violated }) => {
            assert_eq!(
                *violated,
                "round permit state is not owned by the registered loop scope"
            )
        }
        other => panic!("expected state-owner rejection, got {other:?}"),
    }
    let events = take_shared_events();
    let probe = events
        .iter()
        .find(|event| event.starts_with("permit-probe:"))
        .expect("probe recorded");
    assert!(probe.contains("Rejected"), "{probe}");
    assert!(
        saw_prefix(&events, "permit-probe-round-state:Some(Active)"),
        "来源未冻结: {events:?}"
    );
    assert_eq!(
        super::test_support::count(&events, "loop-collect:promoted"),
        1,
        "随后合法路径成功: {events:?}"
    );
}

fn finish_immediately_body() -> Flow<(State,), Data<State>> {
    finish_body_with_output()
}

/// 完成态 body（Loop 的包装 Step child）。
fn finish_body_with_output() -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body");
    let out: super::data_ref::DataRef<State> = flow
        .then::<_, SyncFnSig<(State,), Data<State>>, _>(
            (|state: &State| {
                Ok(State {
                    value: state.value + 1,
                    finish: true,
                })
            }) as fn(&State) -> Result<State, BodyError>,
            state,
        )
        .expect("step");
    flow.finish::<Data<State>, _>(out).expect("finish")
}

#[test]
fn s17_promote_rejection_saved_first_cause_is_read_back() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    install_loop_fault(LoopFault::PromoteSelectedDestroyed);
    let body = finish_immediately_body();
    let loop_ = iter_loop(body);
    // 被选位置来自**登记包装**的唯一声明输出（Loop 登记的 RefId），不是 body 自身的端口。
    let selected_position = loop_.wrapper_output_position().clone();
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(loop_, input)
        .expect("then loop");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_value as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");
    let _ = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        move |ctx, _root| {
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
    .expect_err("promote rejection");
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "{saved:?}");
    // 首错是 Promote 拒绝这条路径保存的实参：TargetNotAlive + **本次实际 selected 的完整 RefId**。
    match saved[0].scope_error.as_ref() {
        Some(ScopeError::TargetNotAlive { position }) => {
            assert_eq!(*position, selected_position, "保存的位置是本次实际被选位置");
        }
        other => panic!("expected TargetNotAlive, got {other:?}"),
    }
    assert_eq!(saved[0].kind, super::context::TerminationKind::BodyError);
    assert_eq!(saved[0].note, "scope operation failed");
    let round_seq = super::test_support::boundary_creation_snapshot()
        .into_iter()
        .find(|(_, _, role)| *role == ScopeRole::Round)
        .expect("round")
        .0
        .seq();
    assert_eq!(saved[0].scope.as_ref().map(ScopeId::seq), Some(round_seq));
    // 清理诊断与 Scope 精确匹配。
    let events = take_shared_events();
    let expected = format!(
        "round-cleanup-report:CleanupDiagnostic {{ scope: {:?}, error: Invariant {{ violated: \"owned entry must exist until its scope closes\" }} }}",
        saved[0].scope.as_ref().expect("scope")
    );
    assert!(
        events.iter().any(|event| event == &expected),
        "保存的 cleanup 内容与 Scope: {events:?}"
    );
}

#[test]
fn s17_termination_rejects_two_distinct_entries_and_allows_cleanup() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    install_loop_fault(LoopFault::ProbeAfterBodyError);
    let (parent, position) = iter_parent(iter_loop(deep_descendant_body()));
    let _ = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        move |ctx, _root| {
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
    .expect_err("body error");
    let events = take_shared_events();
    let business = events
        .iter()
        .find(|event| event.starts_with("post-fail-business:"))
        .expect("business probe");
    let commit = events
        .iter()
        .find(|event| event.starts_with("post-fail-commit:"))
        .expect("commit probe");
    assert!(business.contains("Terminated"), "业务提交被拒: {business}");
    assert!(commit.contains("Terminated"), "普通 commit 被拒: {commit}");
    let cleanup = events
        .iter()
        .find(|event| event.starts_with("post-fail-cleanup:"))
        .expect("cleanup probe");
    assert!(cleanup.ends_with("Ok(())"), "受控清理允许: {cleanup}");
}

// ---------------------------------------------------------------- tuple／unit 正式样本

fn pair_read(pair: &(u32, u32)) -> Result<u32, BodyError> {
    Ok(pair.0 + pair.1)
}

#[test]
fn s17_tuple_outputs_are_single_data_positions() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    // Retry 的 O 是**一份** tuple Data（不是两个独立位置）。
    let mut builder: LoopBuilder<Retry1<u32, (u32, u32)>> = LoopBuilder::start().expect("retry");
    builder
        // Retry：输入是单个 `u32`，输出是**一份** tuple Data。
        .then_body::<_, SyncFnSig<(u32,), Data<(u32, u32)>>>(
            (|input: &u32| Ok((*input, *input))) as fn(&u32) -> Result<(u32, u32), BodyError>,
        )
        .expect("tuple body");
    let loop_ = builder.finish().expect("finish");
    let (mut parent, input) = FlowBuilder::<(u32,)>::start().expect("parent");
    let position = input.position().clone();
    let produced: super::data_ref::DataRef<(u32, u32)> = parent
        .then::<_, OrchSig<u32, Data<(u32, u32)>>, _>(loop_, input)
        .expect("then retry");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<((u32, u32),), Data<u32>>, _>(pair_read, produced)
        .expect("read");
    let read_position = read.position().clone();
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        move |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &position, 7u32)
                .expect("input");
        },
        Some(move |view: &mut RootView<'_, '_>| {
            assert_eq!(
                *view.resolve::<u32>(&read_position)?,
                14,
                "tuple 作为一份 Data 导出"
            );
            Ok(())
        }),
    ))
    .expect("tuple retry");

    // Iter 的 S 是**一份** tuple Data：接到真实父 Flow 运行并核对导出值。
    let mut iter: LoopBuilder<Iter1<(u32, u32)>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, SyncFnSig<((u32, u32),), Data<(u32, u32)>>>(
        (|state: &(u32, u32)| Ok((state.0 + 1, state.1 + 2)))
            as fn(&(u32, u32)) -> Result<(u32, u32), BodyError>,
    )
    .expect("tuple iter body");
    let iter = iter.finish().expect("finish");
    let (mut iter_parent_flow, iter_input) =
        FlowBuilder::<((u32, u32),)>::start().expect("iter parent");
    let iter_input_position = iter_input.position().clone();
    let produced: super::data_ref::DataRef<(u32, u32)> = iter_parent_flow
        .then::<_, OrchSig<(u32, u32), Data<(u32, u32)>>, _>(iter, iter_input)
        .expect("then iter");
    let read: super::data_ref::DataRef<u32> = iter_parent_flow
        .then::<_, SyncFnSig<((u32, u32),), Data<u32>>, _>(pair_read, produced)
        .expect("read");
    let read_position = read.position().clone();
    let iter_parent_flow = iter_parent_flow
        .finish::<Data<u32>, _>(read)
        .expect("iter parent finish");
    drive(definition_in_root(
        iter_parent_flow.definition(),
        Vec::new(),
        move |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &iter_input_position, (10u32, 20u32))
                .expect("tuple input");
        },
        Some(move |view: &mut RootView<'_, '_>| {
            assert_eq!(
                *view.resolve::<u32>(&read_position)?,
                33,
                "Iter tuple 导出核值"
            );
            Ok(())
        }),
    ))
    .expect("tuple iter run");
}

impl LoopControl for (u32, u32) {
    fn loop_decision(&self) -> LoopDecision {
        LoopDecision::Finish
    }
}

/// 允许 `Value = ()`：`LoopControl` 未封闭，`()` 可以实现它。
impl LoopControl for () {
    fn loop_decision(&self) -> LoopDecision {
        LoopDecision::Finish
    }
}

pub(crate) struct UnitNode;

impl super::node::NodeCall1<u32, Data<()>> for UnitNode {
    fn call<'a>(&'a self, _input: &'a u32) -> NodeFut<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

#[test]
fn s17_unit_value_build_rejection_and_state_preservation() {
    // `Value = ()` 且实现 `LoopControl`：类型关系成立，但既有 R7/R8 路径在构建期拒绝。
    let mut builder: LoopBuilder<Retry1<u32, ()>> = LoopBuilder::start().expect("loop");
    let before_allocated = builder.allocated_probe();
    let error = builder
        .then_body::<_, NodeSig<(u32,), Data<()>>>(UnitNode)
        .expect_err("unit value body rejected");
    assert_eq!(
        error,
        super::signature::BuildError::UnitDataOutputNotSupported,
        "既有构建错误: {error:?}"
    );
    // 合法构建态保留：拒绝不消耗序号，也不留下半份 body（finish 仍给出"缺 body"而不是 panic）。
    assert_eq!(
        builder.allocated_probe(),
        before_allocated,
        "拒绝不消耗序号"
    );
    let finish = builder.finish().expect_err("no body registered");
    assert_eq!(finish, super::signature::BuildError::LoopBodyMissing);

    let mut arc_builder: LoopBuilder<Retry1<u32, ()>> = LoopBuilder::start().expect("arc loop");
    let arc_before = arc_builder.allocated_probe();
    assert_eq!(
        arc_builder.then_body::<_, super::signature::ArcNodeSig<(u32,), Data<()>>>(
            std::sync::Arc::new(UnitNode)
        ),
        Err(super::signature::BuildError::UnitDataOutputNotSupported)
    );
    assert_eq!(arc_before, arc_builder.allocated_probe());
    assert!(matches!(
        arc_builder.finish(),
        Err(super::signature::BuildError::LoopBodyMissing)
    ));

    fn unit_function(_: &u32) -> Result<(), BodyError> {
        Ok(())
    }
    let mut fn_builder: LoopBuilder<Retry1<u32, ()>> = LoopBuilder::start().expect("function loop");
    let fn_before = fn_builder.allocated_probe();
    assert_eq!(
        fn_builder.then_body::<_, SyncFnSig<(u32,), Data<()>>>(
            unit_function as fn(&u32) -> Result<(), BodyError>
        ),
        Err(super::signature::BuildError::UnsupportedFunctionUnitOutput)
    );
    assert_eq!(fn_before, fn_builder.allocated_probe());
    assert!(matches!(
        fn_builder.finish(),
        Err(super::signature::BuildError::LoopBodyMissing)
    ));

    let (unit_flow, input) = FlowBuilder::<((),)>::start().expect("unit data declaration");
    assert!(matches!(
        unit_flow.finish::<Data<()>, _>(input),
        Err(super::signature::BuildError::UnitDataOutputNotSupported)
    ));
}

// ---------------------------------------------------------------- 取消：current+pending 与三态

#[test]
fn s17_cancel_reports_and_orders_layers_with_borrow_witness() {
    super::test_support::reset_observations();
    GATE_ARMED.with(|slot| slot.set(false));
    super::test_support::boundary_creation_reset();
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    builder
        .then_body::<_, AsyncFnSig<(State,), Data<State>>>(gated_round_2)
        .expect("body");
    let loop_ = builder.finish().expect("finish");
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(loop_, input)
        .expect("then loop");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_value as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    let definition = parent.definition();
    let mut execution = super::runtime::RootExecution::start();
    let root = execution.context().root_scope();
    {
        let ctx = execution.context_mut();
        ctx.register_owned(
            &ctx.root_scope(),
            &position,
            State {
                value: 0,
                finish: false,
            },
        )
        .expect("state");
    }
    let mut guard = execution
        .context_mut()
        .enter(super::context::InvocationKind::Root, &root, true)
        .expect("root frame");
    let site = definition.steps()[0].site();
    let boxed = match site {
        super::builder::CallSite::Orchestrator(site) => {
            super::test_support::advance_to_pending(site.invoke(&mut guard, &root), 1)
        }
        super::builder::CallSite::Node(_) => panic!("loop step is an orchestrator site"),
    };
    drop(boxed);

    // 取消报告必须存在，且直接回读内容与实际 Scope（缺失即失败）。
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "取消必须保存终止: {saved:?}");
    assert_eq!(saved[0].kind, super::context::TerminationKind::Cancelled);
    assert_eq!(saved[0].note, "pending future dropped");
    let creations = super::test_support::boundary_creation_snapshot();
    let loop_seq = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Loop)
        .expect("loop")
        .0
        .seq();
    let round_seq = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Round)
        .nth(1)
        .expect("second round")
        .0
        .seq();
    assert_eq!(
        saved[0].scope.as_ref().map(ScopeId::seq),
        Some(round_seq),
        "最深取消定位是第 2 轮: {saved:?}"
    );
    let events = take_shared_events();
    // 借用结束见证早于该 Scope 的清理。
    let witness = events
        .iter()
        .position(|event| event == "borrow-witness-dropped")
        .expect("借用在 Future 丢弃时结束: {events:?}");
    let cleanup_start = events
        .iter()
        .position(|event| event == &format!("cleanup-start:{round_seq}"))
        .expect("本轮 cleanup-start");
    let cleanup_end = events
        .iter()
        .position(|event| event == &format!("cleanup-end:{round_seq}"))
        .expect("本轮 cleanup-end");
    let frame_exit = events
        .iter()
        .position(|event| event == &format!("frame-exit:boundary:{round_seq}"))
        .expect("本轮 frame-exit");
    assert!(
        witness < cleanup_start && cleanup_start < cleanup_end && cleanup_end < frame_exit,
        "borrow-end → cleanup-start → cleanup-end → frame-exit: {events:?}"
    );
    let loop_cleanup = events
        .iter()
        .position(|event| event == &format!("cleanup-start:{loop_seq}"))
        .expect("Loop cleanup");
    assert!(
        frame_exit < loop_cleanup,
        "内层退出先于外层清理: {events:?}"
    );
    // 已绑定真实位置在 Closed 后拒绝解析：用第 1 轮曾真实绑定的输入位置。
    let bound_position = super::test_support::take_round_collect_pre_cleanup()
        .into_iter()
        .find(|s| s.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .and_then(|s| s.source_refs)
        .and_then(|refs| refs.first().map(|(position, _)| position.clone()))
        .expect("a real bound position existed");
    assert!(
        guard
            .resolve::<State>(&creations[1].0, &bound_position)
            .is_err(),
        "曾绑定的位置在 Closed 后必须拒绝"
    );
    assert!(
        guard
            .resolve::<State>(&creations[2].0, &bound_position)
            .is_err()
    );
    drop(guard);
    drop(execution);
}

#[test]
fn s17_ready_contrast_on_the_same_fixture() {
    super::test_support::reset_observations();
    GATE_ARMED.with(|slot| slot.set(false));
    super::test_support::boundary_creation_reset();
    let (parent, position) = iter_parent(iter_loop_async());
    // 同一 fixture：先 install 再立即 release，走完整正常完成。
    install_gate();
    release_gate();
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        move |ctx, _root| {
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
    .expect("ready contrast");
    let events = take_shared_events();
    assert!(saw(&events, "loop-finished"), "{events:?}");
    assert_eq!(
        super::test_support::count(&events, "read-node-called"),
        1,
        "父后步执行一次: {events:?}"
    );
    assert!(
        saw(&events, "borrow-witness-dropped"),
        "借用正常结束见证: {events:?}"
    );
}

#[test]
fn s17_owning_cancel_orders_context_and_drops_each_instance() {
    super::test_support::reset_observations();
    GATE_ARMED.with(|slot| slot.set(false));
    super::test_support::boundary_creation_reset();
    let (parent, position) = iter_parent(iter_loop_async());
    let definition = parent.definition();
    let boxed = super::test_support::advance_to_pending(
        definition_in_root::<
            fn(&mut ExecutionContext, &ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            definition,
            vec![root_input_state(&position, 0)],
            |_, _| {},
            None,
        ),
        1,
    );
    let creations = super::test_support::boundary_creation_snapshot();
    let loop_seq = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Loop)
        .expect("loop")
        .0
        .seq();
    let round_seq = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Round)
        .nth(1)
        .expect("round2")
        .0
        .seq();
    drop(boxed);
    let events = take_shared_events();
    // 完整次序：Round frame-exit < Loop frame-exit < Root frame-exit < Context 析构 < Container 析构。
    let round_exit = events
        .iter()
        .position(|event| event == &format!("frame-exit:boundary:{round_seq}"))
        .expect("round exit");
    let loop_exit = events
        .iter()
        .position(|event| event == &format!("frame-exit:boundary:{loop_seq}"))
        .expect("loop exit");
    let root_exit = events
        .iter()
        .position(|event| event.starts_with("frame-exit:root"))
        .expect("root exit");
    let context_drop = events
        .iter()
        .position(|event| event == "context-drop")
        .expect("context-drop 缺失必须失败");
    let container_drop = events
        .iter()
        .position(|event| event == "container-drop")
        .expect("container-drop 缺失必须失败");
    assert!(
        round_exit < loop_exit
            && loop_exit < root_exit
            && root_exit < context_drop
            && context_drop <= container_drop,
        "逐层退出与析构次序: {events:?}"
    );
    // 各实例 Drop 一次：Root 输入（S0）与第 1 轮 Promote 出的 S1。
    let mut drops: Vec<u32> = events
        .iter()
        .filter_map(|event| event.strip_prefix("state-dropped:"))
        .filter_map(|value| value.parse::<u32>().ok())
        .collect();
    drops.sort_unstable();
    assert_eq!(drops, vec![0, 1], "S0 与 S1 各一次: {events:?}");
}

#[test]
fn s17_cancel_with_owned_current_and_pending() {
    super::test_support::reset_observations();
    GATE_ARMED.with(|slot| slot.set(false));
    super::test_support::boundary_creation_reset();
    // 第 1／2 轮完成 Promote 并留下待回收旧值（alias 保留）；第 3 轮 Pending 后取消。
    install_loop_fault(LoopFault::DelayedRecycleAlias);
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    builder
        .then_body::<_, AsyncFnSig<(State,), Data<State>>>(gated_round_3)
        .expect("body");
    let loop_ = builder.finish().expect("finish");
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(loop_, input)
        .expect("then loop");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_value as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");
    let definition = parent.definition();
    let boxed = super::test_support::advance_to_pending(
        definition_in_root::<
            fn(&mut ExecutionContext, &ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            definition,
            vec![root_input_state(&position, 0)],
            |_, _| {},
            None,
        ),
        1,
    );
    let events = take_shared_events();
    assert_eq!(
        super::test_support::count(&events, "loop-collect:promoted"),
        2,
        "取消发生前已有两轮 Promote: {events:?}"
    );
    assert!(
        saw_prefix(&events, "delayed-alias:"),
        "pending 被 alias 保留: {events:?}"
    );
    drop(boxed);
    let events = take_shared_events();
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "{saved:?}");
    assert_eq!(saved[0].kind, super::context::TerminationKind::Cancelled);
    // current 与 pending 各 Drop 一次：S1（pending）、S2（current）以及 Root 的 S0。
    let mut drops: Vec<u32> = events
        .iter()
        .filter_map(|event| event.strip_prefix("state-dropped:"))
        .filter_map(|value| value.parse::<u32>().ok())
        .collect();
    drops.sort_unstable();
    let unique: std::collections::HashSet<&u32> = drops.iter().collect();
    assert_eq!(drops.len(), unique.len(), "不重复销毁: {drops:?}");
    assert_eq!(
        drops,
        vec![0, 1, 2],
        "S0／pending S1／current S2 各一次: {events:?}"
    );
}

#[test]
fn s17_pending_contrast_without_cancel_completes() {
    // 三态对照：同一 gated fixture 在「未 poll」下不创建 Scope；这里补 Ready 之外的
    // Pending→release→完成已在 s17_same_future_pending_then_ready_* 覆盖。
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    let (parent, position) = iter_parent(iter_loop_async());
    let boxed = Box::pin(definition_in_root::<
        fn(&mut ExecutionContext, &ScopeId),
        fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
    >(
        parent.definition(),
        vec![root_input_state(&position, 0)],
        |_, _| {},
        None,
    ));
    drop(boxed);
    assert!(
        take_shared_events().is_empty(),
        "未 poll 的 Future 不进入任何边界"
    );
}

#[test]
fn s17_item_alias_output_target_and_collection_owner() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    let (flow, collection_position) = item_alias_fixture_for_revision2();
    let collection_id: std::rc::Rc<RefCell<Option<DataId>>> = std::rc::Rc::new(RefCell::new(None));
    let captured = std::rc::Rc::clone(&collection_id);
    drive(definition_in_root(
        flow.definition(),
        Vec::new(),
        move |ctx, _root| {
            let id = ctx
                .register_owned(
                    &ctx.root_scope(),
                    &collection_position,
                    vec![State {
                        value: 1,
                        finish: true,
                    }],
                )
                .expect("collection");
            *captured.borrow_mut() = Some(id);
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("item alias chain");
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let promote = snapshots
        .iter()
        .find(|s| s.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .expect("promote snapshot");
    // 实际输出 target 仍是该 item（不是扁平化集合 DataId），cap 是实际 ItemScope。
    match promote.selected_target.as_ref() {
        Some(TargetSnapshot::CollectionItem {
            collection,
            index,
            lifetime_cap,
            ..
        }) => {
            assert_eq!(
                Some(collection.clone()),
                collection_id.borrow().clone(),
                "集合身份"
            );
            assert_eq!(*index, 0, "item 下标");
            assert_ne!(
                *lifetime_cap,
                promote.source.clone().expect("round"),
                "cap 是 ItemScope"
            );
        }
        other => panic!("expected item selected target, got {other:?}"),
    }
    // collection 的原 owner 与被选输出的集合身份（收口通道）。
    let loop_scope = super::test_support::boundary_creation_snapshot()
        .into_iter()
        .find(|(_, _, role)| *role == ScopeRole::Loop)
        .expect("loop scope")
        .0;
    assert_eq!(
        promote.selected_collection_owner.as_ref().map(ScopeId::seq),
        Some(0),
        "集合仍由 Root 负责: {promote:?}"
    );
    // 集合 owner 不变且无新增 item DataId：**Loop 自身**的 Export 仍指向同一 item。
    let exports = take_export_pre_cleanup();
    let export = exports
        .iter()
        .find(|s| {
            s.phase == ExportSnapshotPhase::Before
                && s.child.as_ref() == Some(&loop_scope)
                && !s.slots.is_empty()
        })
        .expect("the loop export");
    let child_slot = export.slots[0].0.clone();
    let bound_target = export
        .child_refs
        .as_ref()
        .and_then(|refs| refs.iter().find(|(position, _)| *position == child_slot))
        .map(|(_, target)| target.clone())
        .expect("child output bound");
    match &bound_target {
        TargetSnapshot::CollectionItem {
            collection,
            index,
            lifetime_cap,
            ..
        } => {
            assert_eq!(
                Some(collection.clone()),
                collection_id.borrow().clone(),
                "同一集合"
            );
            assert_eq!(*index, 0, "同一下标");
            // cap 是实际 ItemScope（Each 的 item 边界），不是 Loop/Round。
            assert_ne!(*lifetime_cap, loop_scope, "cap 不是 LoopScope");
            let item_scope = super::test_support::boundary_creation_snapshot()
                .into_iter()
                .find(|(_, _, role)| *role == ScopeRole::Item)
                .map(|(scope, _, _)| scope);
            assert_eq!(
                *lifetime_cap,
                item_scope.expect("actual ItemScope must be recorded"),
                "cap 是实际 ItemScope"
            );
        }
        other => panic!("expected the same item alias, got {other:?}"),
    }
}

// ---------------------------------------------------------------- R14：共享主场景的 Root／shared 观察

#[test]
fn s18_shared_main_scenario_root_ownership_and_local_ref_invalidation() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    // 原共享多步 Flow 主场景（v21_09_tests 的 Iter 主夹具）。
    let (parent, state_position, rules_position) = super::v21_09_tests::iter_main_parent();
    let state_id: RefCell<Option<DataId>> = RefCell::new(None);
    let rules_id: RefCell<Option<DataId>> = RefCell::new(None);
    let root_refs_owned: RefCell<Option<(usize, usize)>> = RefCell::new(None);
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, root| {
            let state = ctx
                .register_owned(
                    &ctx.root_scope(),
                    &state_position,
                    State {
                        value: 0,
                        finish: false,
                    },
                )
                .expect("state");
            *state_id.borrow_mut() = Some(state);
            let rules = ctx
                .register_owned(&ctx.root_scope(), &rules_position, Rules { step: 1 })
                .expect("rules");
            *rules_id.borrow_mut() = Some(rules);
            let (refs, owned) = ctx.snapshot_probe(root).expect("root snapshot");
            *root_refs_owned.borrow_mut() = Some((refs.len(), owned.len()));
        },
        Some(|view: &mut RootView<'_, '_>| {
            // Root 的责任集合仍包含两份声明输入（Loop 未触碰 Root 的责任；父 Flow
            // 自身新增的 Step 输出绑定不计入本断言）。
            let observation_before = view.snapshot_full()?;
            let (_, owned) = &observation_before;
            let expected = root_refs_owned.borrow().expect("initial root snapshot");
            assert!(expected.0 >= 2, "初始 Root 至少绑定两份输入");
            let state_id = state_id.borrow().clone().expect("state id");
            let rules_id = rules_id.borrow().clone().expect("rules id");
            for id in [&state_id, &rules_id] {
                assert!(owned.contains(id), "Root 仍负责该输入: {owned:?}");
            }
            // 声明输入位置的绑定未被改写。
            assert_eq!(
                view.data_id_of(&state_position).expect("state bound"),
                state_id
            );
            assert_eq!(
                view.data_id_of(&rules_position).expect("rules bound"),
                rules_id
            );
            // S0 与 shared 仍由 Root 负责且存活。
            for id in [&state_id, &rules_id] {
                assert!(view.probe().alive_probe(id), "Root 输入存活");
                assert_eq!(
                    view.probe().owner_probe(id).expect("owner").seq(),
                    0,
                    "owner 仍是 Root"
                );
            }
            // 正常退出后本地 refs 失效：Loop／Round 已 Closed 且引用集合已清空。
            for (scope, _, role) in super::test_support::boundary_creation_snapshot() {
                if !matches!(role, ScopeRole::Loop | ScopeRole::Round) {
                    continue;
                }
                assert_eq!(
                    view.state(&scope)?,
                    super::scope::ScopeState::Closed,
                    "Loop／Round 已关闭"
                );
                let (refs, owned) = view.probe().snapshot_probe(&scope)?;
                assert!(
                    refs.is_empty() && owned.is_empty(),
                    "Closed 后无残留本地引用: {scope}"
                );
            }
            assert_eq!(
                observation_before,
                view.snapshot_full()?,
                "Root observation must preserve complete refs and ownership"
            );
            Ok(())
        }),
    ))
    .expect("shared main scenario");
    let events = take_shared_events();
    assert!(saw(&events, "loop-finished"), "{events:?}");
    assert_eq!(
        super::test_support::count(&events, "loop-collect:promoted"),
        3,
        "{events:?}"
    );
}

#[test]
fn s18_retry_flow_body_round_input_data_id_matches_root_input() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    super::v21_09_tests_revision::rounds_reset();
    let (parent, input_position, _loop_caller) =
        super::v21_09_tests_revision::retry_flow_body_fixture();
    let input_id: RefCell<Option<DataId>> = RefCell::new(None);
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            let id = ctx
                .register_owned(&ctx.root_scope(), &input_position, Item { id: 6 })
                .expect("input");
            *input_id.borrow_mut() = Some(id);
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("retry flow body");
    let input_id = input_id.borrow().clone().expect("input id");
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let rounds: Vec<_> = snapshots
        .iter()
        .filter(|s| s.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .collect();
    assert_eq!(rounds.len(), 2, "两轮: {snapshots:?}");
    for snapshot in rounds {
        // 每轮 Round 的本地输入目标就是 Root 登记的那一份 DataId（真实 RefTarget 身份）。
        let targets: Vec<DataId> = snapshot
            .source_refs
            .as_ref()
            .expect("source refs")
            .iter()
            .filter_map(|(_, target)| match target {
                TargetSnapshot::Data(id) => Some(id.clone()),
                _ => None,
            })
            .collect();
        assert!(
            targets.contains(&input_id),
            "轮次输入 DataId 与 Root 输入一致: {snapshot:?}"
        );
    }
}

#[test]
fn s18_loop_refs_content_and_single_final_binding() {
    super::test_support::reset_observations();
    super::test_support::boundary_creation_reset();
    let (parent, position) = iter_parent(iter_loop(finish_immediately_body()));
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        move |ctx, _root| {
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
    .expect("single round");
    // final 声明 Ref 实际只绑定一次：本地 final-bind 观察恰好一对 Before／AfterReject 之外的
    // 一次 Before（成功路径只记录 Before）。
    let bind_snapshots = super::test_support::take_final_bind_pre_cleanup();
    assert_eq!(
        bind_snapshots.len(),
        1,
        "最终绑定只发生一次: {bind_snapshots:?}"
    );
    // Loop 的本地 refs 内容：Export 时恰好是"声明输入 + 最终声明输出"两个位置，且目标等于
    // 输入 DataId 与最终选中值。
    let loop_scope = super::test_support::boundary_creation_snapshot()
        .into_iter()
        .find(|(_, _, role)| *role == ScopeRole::Loop)
        .expect("loop scope")
        .0;
    let exports = take_export_pre_cleanup();
    let export = exports
        .iter()
        .find(|s| s.phase == ExportSnapshotPhase::Before && s.child.as_ref() == Some(&loop_scope))
        .expect("loop export");
    let refs = export.child_refs.as_ref().expect("loop refs");
    assert_eq!(refs.len(), 2, "声明输入＋最终输出: {refs:?}");
    let bound = bind_snapshots[0]
        .state_target
        .clone()
        .expect("final state target");
    assert!(
        refs.iter().any(|(_, target)| *target == bound),
        "最终声明输出指向被 Promote 的值: {refs:?} vs {bound:?}"
    );
    assert!(
        export
            .child_owned
            .as_ref()
            .is_some_and(|owned| !owned.is_empty()),
        "已 Promote 的值由 Loop 负责: {export:?}"
    );
}
