//! V21-11 验收样本（四）：L20～L22 真实取消（按复审 R11-09 强化）。
//!
//! 取消点是"已 Consume 一个 item、当前 Loop 已 Promote owned current、下一 Round 已有临时值
//! 并持真实借用 Pending"；逐层退出用真实创建记录＋共享单序列比较，不靠总数或 seq 大小。

use std::cell::Cell;
use std::sync::Arc;

use super::builder::TypedCallBuilder;
use super::context::{BodyError, TerminationKind};
use super::data_ref::DataRef;
use super::each::{EachBuilder, EachShared};
use super::flow::{Flow, FlowBuilder};
use super::identity::ScopeId;
use super::loop_orchestrator::{Iter1, Loop, LoopBuilder};
use super::match_orchestrator::MatchBuilder;
use super::node::NodeCall1;
use super::orchestrator::ScopeRole;
use super::runtime::Runtime;
use super::scope::TargetSnapshot;
use super::signature::{Data, NodeFut, NodeSig, OrchSig, SyncFnSig};
use super::test_support::{
    PromoteStateRecord, RootSnapshotPhase, advance_to_pending, boundary_creation_reset,
    boundary_creation_snapshot, closed_scope_reset, closed_scope_snapshot, drive, install_gate,
    record, release_gate, strict_reset_observations, take_events, take_log_snapshot,
    take_promote_states,
};
use super::v21_11_tests::{ItemResult, Route, Rules, State};

// ---------------------------------------------------------------- 夹具

/// 每次调用都在真实借用处挂起，并记录 borrow witness。
pub(crate) struct GatedBorrowNode;

impl NodeCall1<State, Data<State>> for GatedBorrowNode {
    fn call<'a>(&'a self, state: &'a State) -> NodeFut<'a, State> {
        Box::pin(async move {
            record(&format!("borrow-enter:{}:{}", state.item, state.progress));
            super::test_support::gate_wait().await;
            record(&format!("borrow-exit:{}:{}", state.item, state.progress));
            Ok(advance_state(state))
        })
    }
}

/// 只在第 `nth` 次调用挂起（L21：第二个 item 的输入借用处）。
pub(crate) struct GateOnCallNode {
    nth: u32,
    calls: Cell<u32>,
}

impl GateOnCallNode {
    pub(crate) fn new(nth: u32) -> Self {
        Self {
            nth,
            calls: Cell::new(0),
        }
    }
}

impl NodeCall1<State, Data<State>> for GateOnCallNode {
    fn call<'a>(&'a self, state: &'a State) -> NodeFut<'a, State> {
        let call = self.calls.get();
        self.calls.set(call + 1);
        let gate = call == self.nth;
        Box::pin(async move {
            record(&format!("borrow-enter:{}:{}", state.item, state.progress));
            if gate {
                super::test_support::gate_wait().await;
            }
            record(&format!("borrow-exit:{}:{}", state.item, state.progress));
            Ok(advance_state(state))
        })
    }
}

fn advance_state(state: &State) -> State {
    State {
        item: state.item,
        initial: state.initial,
        progress: (state.progress + state.step).min(state.target),
        step: state.step,
        target: state.target,
        skip: state.skip,
    }
}

fn state(item: u32, progress: u32, target: u32) -> State {
    State {
        item,
        initial: progress,
        progress,
        step: 1,
        target,
        skip: false,
    }
}

/// Round 内的临时值（证明"下一 Round 已有临时值"）。
#[derive(Debug)]
pub(crate) struct RoundTemp {
    pub(crate) item: u32,
    pub(crate) round: u32,
}

impl Drop for RoundTemp {
    fn drop(&mut self) {
        record(&format!("temp-dropped:{}:{}", self.item, self.round));
    }
}

fn temp_of_state(state: &State) -> Result<RoundTemp, BodyError> {
    let round = (state.progress.saturating_sub(state.initial)) / state.step.max(1) + 1;
    Ok(RoundTemp {
        item: state.item,
        round,
    })
}

/// 最深 Round body：临时值 → 真实借用处挂起的 Arc Node。
fn gated_round_body() -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("round start");
    let _temp: DataRef<RoundTemp> = flow
        .then::<_, SyncFnSig<(State,), Data<RoundTemp>>, _>(
            temp_of_state as fn(&State) -> Result<RoundTemp, BodyError>,
            state.clone(),
        )
        .expect("temp step");
    let produced: DataRef<State> = flow
        .then::<_, super::signature::ArcNodeSig<(State,), Data<State>>, _>(
            Arc::new(GatedBorrowNode),
            state,
        )
        .expect("gated step");
    flow.finish::<Data<State>, _>(produced)
        .expect("round finish")
}

/// item body：Match 的 iterate 分支调用该 Loop（含 Round／Loop／Branch／Match 层）。
fn cancel_item_body() -> Flow<(State,), Data<ItemResult>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("item start");
    let route: DataRef<Route> = flow
        .then::<_, SyncFnSig<(State,), Data<Route>>, _>(
            (|state: &State| Ok(Route(u32::from(state.skip))))
                as fn(&State) -> Result<Route, BodyError>,
            state.clone(),
        )
        .expect("route step");
    let mut matched: MatchBuilder<Route, State, Data<State>> =
        MatchBuilder::start().expect("match");
    matched
        .branch::<_, OrchSig<State, Data<State>>>(Route(0), iter_loop())
        .expect("iterate branch");
    matched
        .default::<_, SyncFnSig<(State,), Data<State>>>(
            (|state: &State| Ok(advance_state(state))) as fn(&State) -> Result<State, BodyError>,
        )
        .expect("skip branch");
    let matched = matched.finish().expect("match finish");
    let progressed: DataRef<State> = flow
        .then::<_, OrchSig<(Route, State), Data<State>>, _>(matched, (route, state))
        .expect("match step");
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
        .expect("item finish")
}

fn iter_loop() -> Loop<Iter1<State>> {
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(gated_round_body())
        .expect("iter body");
    builder.finish().expect("iter finish")
}

/// L20／L22 的 Root：`(Vec<State>,) -> Data<Vec<ItemResult>>`。
fn cancel_root() -> Flow<(Vec<State>,), Data<Vec<ItemResult>>> {
    let (mut flow, states) = FlowBuilder::<(Vec<State>,)>::start().expect("start");
    let mut each: EachBuilder<super::each::EachOnly<State>, ItemResult> =
        EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<State, Data<ItemResult>>>(cancel_item_body())
        .expect("each body");
    let each = each.finish().expect("each finish");
    let collected: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(each, states)
        .expect("each step");
    flow.finish::<Data<Vec<ItemResult>>, _>(collected)
        .expect("finish")
}

/// L21 的 Root：带 shared 的 Each（集合 + Rules）。
fn shared_cancel_root() -> Flow<(Vec<State>, Rules), Data<Vec<ItemResult>>> {
    let (mut flow, (states, rules)) = FlowBuilder::<(Vec<State>, Rules)>::start().expect("start");
    let mut each: EachBuilder<EachShared<State, Rules>, ItemResult> =
        EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<(State, Rules), Data<ItemResult>>>(gated_item_flow())
        .expect("each body");
    let each = each.finish().expect("each finish");
    let collected: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<(Vec<State>, Rules), Data<Vec<ItemResult>>>, _>(each, (states, rules))
        .expect("each step");
    flow.finish::<Data<Vec<ItemResult>>, _>(collected)
        .expect("finish")
}

/// L21 的 item body：第 2 次调用才挂起；shared 只被借用（不在 item 内析构）。
fn gated_item_flow() -> Flow<(State, Rules), Data<ItemResult>> {
    let (mut flow, (state, rules)) = FlowBuilder::<(State, Rules)>::start().expect("item start");
    let progressed: DataRef<State> = flow
        .then::<_, NodeSig<(State,), Data<State>>, _>(GateOnCallNode::new(1), state)
        .expect("gated step");
    let result: DataRef<ItemResult> = flow
        .then::<_, SyncFnSig<(State, Rules), Data<ItemResult>>, _>(
            (|state: &State, rules: &Rules| {
                let _ = rules;
                Ok(ItemResult {
                    item: state.item,
                    progress: state.progress,
                })
            }) as fn(&State, &Rules) -> Result<ItemResult, BodyError>,
            (progressed, rules),
        )
        .expect("result step");
    flow.finish::<Data<ItemResult>, _>(result).expect("finish")
}

// ---------------------------------------------------------------- 辅助

fn shared_events() -> Vec<String> {
    super::test_support::take_shared_events()
}

fn count(events: &[String], prefix: &str) -> usize {
    events
        .iter()
        .filter(|event| event.starts_with(prefix))
        .count()
}

/// 再推进一次到新的 Pending（闸已在调用前重新安装）。
fn poll_once_to_pending<F: Future>(mut boxed: std::pin::Pin<Box<F>>) -> std::pin::Pin<Box<F>> {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    match boxed.as_mut().poll(&mut cx) {
        std::task::Poll::Pending => boxed,
        std::task::Poll::Ready(_) => panic!("future completed instead of suspending again"),
    }
}

fn index_of(events: &[String], needle: &str) -> usize {
    events
        .iter()
        .position(|event| event == needle)
        .unwrap_or_else(|| panic!("missing event {needle} in {events:?}"))
}

fn creation_role(
    creations: &[(ScopeId, ScopeId, ScopeRole)],
    scope: &ScopeId,
) -> Option<ScopeRole> {
    creations
        .iter()
        .find(|(child, _, _)| child == scope)
        .map(|(_, _, role)| *role)
}

fn creation_parent(
    creations: &[(ScopeId, ScopeId, ScopeRole)],
    scope: &ScopeId,
) -> Option<ScopeId> {
    creations
        .iter()
        .find(|(child, _, _)| child == scope)
        .map(|(_, parent, _)| parent.clone())
}

// ---------------------------------------------------------------- L20

#[test]
fn l20_cancel_requires_promoted_current_in_the_pending_loop() {
    strict_reset_observations();
    boundary_creation_reset();
    closed_scope_reset();
    install_gate();
    // item 0：一轮即 Finish 并被 Consume；item 1：需要两轮。
    let root = cancel_root();
    let future = Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &root,
        (vec![state(0, 2, 2), state(1, 0, 2)],),
    );
    let mut boxed = advance_to_pending(future, 1);
    // 放行 item 0 的那一轮，并重新装闸，停到 item 1 第一轮（等待其 Promote）。
    release_gate();
    install_gate();
    boxed = poll_once_to_pending(boxed);
    // 放行 item 1 第一轮（Promote owned current），再装闸，停到 item 1 第二轮。
    release_gate();
    install_gate();
    let boxed = poll_once_to_pending(boxed);

    // ---- drop 前：直接核对当前 Loop 的状态／owner 与本轮身份。
    let creations = boundary_creation_snapshot();
    let pending_round = creations
        .iter()
        .rev()
        .find(|(_, _, role)| *role == ScopeRole::Round)
        .map(|(scope, _, _)| scope.clone())
        .expect("pending round created");
    let pending_loop =
        creation_parent(&creations, &pending_round).expect("round has a Loop parent");
    assert_eq!(
        creation_role(&creations, &pending_loop),
        Some(ScopeRole::Loop),
        "pending Round 直接归 Loop"
    );
    let promotes: Vec<PromoteStateRecord> = take_promote_states();
    let current: Vec<&PromoteStateRecord> = promotes
        .iter()
        .filter(|record| record.controller == pending_loop)
        .collect();
    assert_eq!(current.len(), 1, "本 Loop 恰好一次 Promote: {promotes:?}");
    let record = current[0];
    assert!(record.transferred, "owned current 已转给控制器: {record:?}");
    let previous_round = creations
        .iter()
        .filter(|(_, parent, role)| *role == ScopeRole::Round && *parent == pending_loop)
        .map(|(scope, _, _)| scope.clone())
        .rfind(|scope| *scope != pending_round)
        .expect("pending Round 之前该 Loop 已有一个 Round");
    assert_eq!(
        record.source, previous_round,
        "Promote 来源是同 Loop 的前一轮 Round"
    );
    assert_ne!(record.source, pending_round, "pending Round 尚未 Promote");
    assert!(
        matches!(record.state_target, Some(TargetSnapshot::Data(_))),
        "控制状态指向完整 Data: {record:?}"
    );
    assert!(
        !record.controller_owned.is_empty(),
        "控制器负责 promoted 值: {record:?}"
    );
    assert!(
        record.pending.is_empty(),
        "首轮 Promote 无待回收旧值（imported 初态不随替换回收）: {record:?}"
    );
    // 部分 collector：item 0 已完成并 Consume 进 collector，item 1 仍在轮内。
    let item_scopes: Vec<ScopeId> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Item)
        .map(|(scope, _, _)| scope.clone())
        .collect();
    assert_eq!(item_scopes.len(), 2, "两个 Item 已创建: {creations:?}");
    let closed_before_drop = closed_scope_snapshot();
    assert!(
        closed_before_drop.contains(&item_scopes[0]),
        "item 0 已 Consume 并关闭: {closed_before_drop:?}"
    );
    assert!(
        !closed_before_drop.contains(&item_scopes[1]),
        "item 1 仍在执行: {closed_before_drop:?}"
    );
    let events_before_drop = take_events();
    assert_eq!(
        count(&events_before_drop, "collector-after:1"),
        1,
        "item 0 恰好一次移动进部分 collector: {events_before_drop:?}"
    );
    assert_eq!(
        count(&events_before_drop, "collector-after:2"),
        0,
        "item 1 尚未移动: {events_before_drop:?}"
    );
    assert_eq!(
        count(&events_before_drop, "item-result-dropped:0:2"),
        0,
        "item 0 的结果仍由部分 collector 持有: {events_before_drop:?}"
    );
    assert_eq!(
        count(&events_before_drop, "temp-dropped:1:2"),
        0,
        "pending Round 的临时值尚未析构: {events_before_drop:?}"
    );
    assert_eq!(
        count(&events_before_drop, "borrow-enter:1:1"),
        1,
        "pending Round 持有真实借用: {events_before_drop:?}"
    );
    assert_eq!(
        count(&events_before_drop, "borrow-exit:1:1"),
        0,
        "取消前该借用尚未结束（witness 缺失即证据）"
    );

    // ---- 丢弃 owning Runtime Future。
    drop(boxed);

    // 借用 witness：最后一个 borrow-enter 没有对应的 borrow-exit。
    let events = take_events();
    assert_eq!(count(&events, "borrow-exit:1:1"), 0, "取消发生在借用内部");
    assert_eq!(
        count(&events, "temp-dropped:1:2"),
        1,
        "pending Round 临时值清理一次"
    );
    assert_eq!(
        count(&events, "item-result-dropped:0:2"),
        1,
        "已 Consume 的部分 collector 清理一次"
    );
    assert_eq!(
        count(&events, "state-dropped:1:1"),
        1,
        "promoted owned current 在退出时清理一次: {events:?}"
    );
    assert!(take_log_snapshot().is_empty(), "取消零 take");

    // 逐层关闭：每个 (child → parent) 对都满足 child 先关闭，且每个创建点恰好关闭一次。
    let closed = closed_scope_snapshot();
    for (child, parent, _) in &creations {
        let child_at = closed
            .iter()
            .position(|scope| scope == child)
            .unwrap_or_else(|| panic!("{child} 未关闭: {closed:?}"));
        if parent.seq() == 0 {
            continue;
        }
        let parent_at = closed
            .iter()
            .position(|scope| scope == parent)
            .unwrap_or_else(|| panic!("{parent} 未关闭: {closed:?}"));
        assert!(
            child_at < parent_at,
            "child 先于 parent 关闭: {child} vs {parent}"
        );
    }
    for (child, _, _) in &creations {
        assert_eq!(
            closed.iter().filter(|scope| *scope == child).count(),
            1,
            "已关闭的层不重复清理: {closed:?}"
        );
    }

    // 共享单序列：逐相邻层 frame-exit（child 先于 parent），Root → Context → Container 收尾。
    let shared = shared_events();
    let frame_exit_at = |scope: &ScopeId| -> usize {
        let event = if scope.seq() == 0 {
            String::from("frame-exit:root:0")
        } else {
            format!("frame-exit:boundary:{}", scope.seq())
        };
        assert_eq!(
            shared
                .iter()
                .filter(|candidate| **candidate == event)
                .count(),
            1,
            "每个创建点恰好退出一次: {event} in {shared:?}"
        );
        index_of(&shared, &event)
    };
    for (child, parent, _) in &creations {
        assert!(
            frame_exit_at(child) < frame_exit_at(parent),
            "共享单序列中 child 先于 parent 退出: {child} -> {parent}\n{shared:?}"
        );
    }
    // 取消时仍活跃的层：guard 触发 abort 清理，同层顺序为 start → end → frame-exit
    // （正常完成的层没有 abort 清理，只出现 frame-exit，因此按实际存在的事件断言）。
    let mut aborted_layers = 0usize;
    for (child, _, _) in &creations {
        let Some(start) = shared
            .iter()
            .position(|event| event == &format!("cleanup-start:{}", child.seq()))
        else {
            continue;
        };
        aborted_layers += 1;
        let end = index_of(&shared, &format!("cleanup-end:{}", child.seq()));
        let exit = frame_exit_at(child);
        assert!(
            start < end && end < exit,
            "同层顺序 start<end<exit: {child}"
        );
    }
    assert!(
        aborted_layers >= 3,
        "取消时至少有三层活跃（Round 包装／Round／Loop）: {aborted_layers} in {shared:?}"
    );
    let root_exit = index_of(&shared, "frame-exit:root:0");
    let context_drop = index_of(&shared, "context-drop");
    let container_drop = index_of(&shared, "container-drop");
    assert!(
        root_exit < context_drop && context_drop < container_drop,
        "Root → Context → Container 完整析构次序: {shared:?}"
    );
    for event in &events {
        if event.starts_with("state-dropped") || event.starts_with("temp-dropped") {
            let at = index_of(&shared, event);
            assert!(
                at < context_drop,
                "Container 内的值在 Context 析构前清理: {event}"
            );
        }
    }
    // 最深取消定位 = pending Round（最深 frame 沿用其 Scope）。
    let snapshots = super::v21_11_tests::take_snapshots_checked();
    let after = snapshots
        .iter()
        .rev()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterFailureCleanup)
        .expect("取消后的观察点");
    assert_eq!(
        after.terminated,
        Some(TerminationKind::Cancelled),
        "记录为取消: {after:?}"
    );
    let deepest = after.termination_scope.clone().expect("Cancelled 记录定位");
    assert_eq!(
        creation_parent(&creations, &deepest),
        Some(pending_round.clone()),
        "Cancelled 定位是 pending Round 的直接 child（本轮包装，实际最深 frame）: {after:?}"
    );
    assert_eq!(
        creation_role(&creations, &deepest),
        Some(ScopeRole::Flow),
        "最深 frame 是 round 包装 Flow"
    );
    assert_eq!(
        after.root_state,
        super::scope::ScopeState::Closed,
        "Root 已关闭"
    );
    release_gate();
}

// ---------------------------------------------------------------- L21

#[test]
fn l21_cancel_at_next_item_borrow_keeps_inputs_until_root_exit() {
    strict_reset_observations();
    boundary_creation_reset();
    closed_scope_reset();
    install_gate();
    let root = shared_cancel_root();
    let future = Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &root,
        (
            vec![state(0, 0, 1), state(1, 0, 1)],
            Rules { step: 1, target: 1 },
        ),
    );
    let boxed = advance_to_pending(future, 1);
    let pre_drop = take_events();
    assert_eq!(
        count(&pre_drop, "borrow-enter:1:0"),
        1,
        "第二个 item 已进入借用: {pre_drop:?}"
    );
    assert_eq!(count(&pre_drop, "borrow-exit:1:0"), 0, "借用未结束");
    assert_eq!(
        count(&pre_drop, "borrow-enter:0:0"),
        1,
        "第一个 item 正常完成过: {pre_drop:?}"
    );
    assert_eq!(count(&pre_drop, "borrow-exit:0:0"), 1);
    drop(boxed);

    let post_drop = take_events();
    assert_eq!(
        count(&post_drop, "borrow-exit:1:0"),
        0,
        "取消发生在借用内部"
    );
    assert!(take_log_snapshot().is_empty(), "零 take");

    // 输入（集合元素与 shared）在 child 清理点之后、Root 退出时才各清理一次。
    let shared = shared_events();
    let last_child_exit = shared
        .iter()
        .rev()
        .find(|event| event.starts_with("frame-exit:boundary:"))
        .cloned()
        .expect("child frame-exit recorded");
    assert!(shared.iter().any(|event| event == &last_child_exit));
    let collection_drop = index_of(&shared, "state-dropped:0:0");
    let rules_drop = index_of(&shared, "rules-dropped");
    let child_exit_at = index_of(&shared, &last_child_exit);
    assert!(
        child_exit_at < collection_drop && child_exit_at < rules_drop,
        "祖先 imported 输入在 child 清理点之后才析构: {shared:?}"
    );
    assert_eq!(count(&shared, "rules-dropped"), 1, "shared 恰好清理一次");
    // 两个集合元素 + 第二个 item 内产生的中间 State，各清理一次。
    assert_eq!(
        count(&shared, "state-dropped"),
        3,
        "实例各清理一次: {shared:?}"
    );
    let context_drop = index_of(&shared, "context-drop");
    let container_drop = index_of(&shared, "container-drop");
    assert!(
        collection_drop < context_drop && context_drop < container_drop,
        "Root → Context → Container 完整析构次序: {shared:?}"
    );
    release_gate();
}

// ---------------------------------------------------------------- L22

#[test]
fn l22_unpolled_and_ready_drops_differ_from_cancellation() {
    // (a) 未 poll：没有创建任何 Execution 设施，输入析构一次。
    strict_reset_observations();
    let contexts_before = super::context::creation_counts::contexts();
    let coordinators_before = super::context::creation_counts::coordinators();
    let containers_before = super::context::creation_counts::containers();
    let root = cancel_root();
    let future = Runtime::execute::<_, _, Data<Vec<ItemResult>>>(&root, (vec![state(0, 0, 1)],));
    drop(future);
    assert_eq!(
        super::context::creation_counts::contexts(),
        contexts_before,
        "未 poll 不创建 Context"
    );
    assert_eq!(
        super::context::creation_counts::coordinators(),
        coordinators_before,
        "未 poll 不创建 Coordinator"
    );
    assert_eq!(
        super::context::creation_counts::containers(),
        containers_before,
        "未 poll 不创建 Container"
    );
    let events = take_events();
    assert_eq!(
        count(&events, "state-dropped"),
        1,
        "输入析构一次: {events:?}"
    );
    assert_eq!(count(&events, "cleanup-start"), 0, "未 poll 无取消清理");

    // (b) Ready 后丢弃：无额外取消清理，输出在 Context 析构后仍可用。
    strict_reset_observations();
    let root = cancel_root();
    let results = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &root,
        (vec![state(0, 1, 1)],),
    ))
    .expect("ready execution");
    let shared = shared_events();
    assert_eq!(
        count(&shared, "cleanup-start"),
        0,
        "成功路径没有 abort 清理: {shared:?}"
    );
    assert_eq!(count(&shared, "context-drop"), 1, "Context 已析构");
    assert_eq!(results.len(), 1, "输出在 Context 析构后可用");
    let events = take_events();
    assert_eq!(
        count(&events, "item-result-dropped"),
        0,
        "Ready 输出未被取消清理"
    );
    drop(results);
    let events = take_events();
    assert_eq!(
        count(&events, "item-result-dropped:0:1"),
        1,
        "应用 drop 才析构"
    );

    // (c) 另一 Execution 不受影响。
    strict_reset_observations();
    let root = cancel_root();
    let other = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &root,
        (vec![state(0, 1, 1)],),
    ))
    .expect("independent execution");
    assert_eq!(other.len(), 1);
}
