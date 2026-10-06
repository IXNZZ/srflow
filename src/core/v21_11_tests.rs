//! V21-11 验收样本（一）：§4.1 完整主场景与 L01～L03、L06、L07。
//!
//! 全部经真实 `Runtime::execute`；观察使用严格快照与 take 记录（无默认值）。

use std::sync::Arc;

use super::builder::TypedCallBuilder;
use super::context::BodyError;
use super::data_ref::DataRef;
use super::each::{Each, EachBuilder, EachOnly, EachShared};
use super::flow::{Flow, FlowBuilder};
use super::identity::ScopeId;
use super::loop_orchestrator::{Iter1, Loop, LoopBuilder, LoopControl, LoopDecision};
use super::match_orchestrator::{Match, MatchBuilder};
use super::node::NodeCall1;
use super::orchestrator::{OrchCall, ScopeRole};
use super::runtime::{RootErrorStage, Runtime};
use super::scope::ScopeState;
use super::signature::{ArcNodeSig, Data, NodeFut, NodeSig, OrchSig, Out2, SyncFnSig, Unit};
use super::test_support::{
    RootSnapshotPhase, advance_to_pending, boundary_address_reset, boundary_address_snapshot,
    boundary_creation_reset, boundary_creation_snapshot, closed_scope_reset, closed_scope_snapshot,
    drive, drive_pinned, install_gate, record, release_gate, strict_reset_observations,
    take_events, take_log_snapshot, take_root_snapshots,
};

// ---------------------------------------------------------------- 业务类型（非 Clone，逐实例 Drop 见证）

/// 待处理作业：`id` 定位实例、`progress` 初始进度、`skip` 标记走 skip branch。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Job {
    pub(crate) id: u32,
    pub(crate) progress: u32,
    pub(crate) skip: bool,
}

impl Drop for Job {
    fn drop(&mut self) {
        record(&format!("job-dropped:{}", self.id));
    }
}

/// 推进规则（Root 第二份输入，非 Clone）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Rules {
    pub(crate) step: u32,
    pub(crate) target: u32,
}

impl Drop for Rules {
    fn drop(&mut self) {
        record("rules-dropped");
    }
}

/// 每 item 由业务 Node 显式产生的推进状态；自身表达 Continue／Finish。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct State {
    pub(crate) item: u32,
    pub(crate) initial: u32,
    pub(crate) progress: u32,
    pub(crate) step: u32,
    pub(crate) target: u32,
    pub(crate) skip: bool,
}

impl Drop for State {
    fn drop(&mut self) {
        record(&format!("state-dropped:{}:{}", self.item, self.progress));
    }
}

impl LoopControl for State {
    fn loop_decision(&self) -> LoopDecision {
        if self.progress >= self.target {
            LoopDecision::Finish
        } else {
            LoopDecision::Continue
        }
    }
}

/// Match 的路由键（普通 Node 产生的 Data）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Route(pub(crate) u32);

/// Each 的元素结果。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ItemResult {
    pub(crate) item: u32,
    pub(crate) progress: u32,
}

impl Drop for ItemResult {
    fn drop(&mut self) {
        record(&format!(
            "item-result-dropped:{}:{}",
            self.item, self.progress
        ));
    }
}

/// SubFlow 之后的聚合报告。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Report {
    pub(crate) count: u32,
    pub(crate) total: u32,
}

impl Drop for Report {
    fn drop(&mut self) {
        record(&format!("report-dropped:{}:{}", self.count, self.total));
    }
}

/// Round 内的临时值（未选中的 owned），按 item／round 记录实例。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Temp {
    pub(crate) item: u32,
    pub(crate) round: u32,
}

impl Drop for Temp {
    fn drop(&mut self) {
        record(&format!("temp-dropped:{}:{}", self.item, self.round));
    }
}

// ---------------------------------------------------------------- 业务 Node

fn state_from_job(job: &Job, rules: &Rules) -> Result<State, BodyError> {
    record(&format!("item-state:{}", job.id));
    Ok(State {
        item: job.id,
        initial: job.progress,
        progress: job.progress,
        step: rules.step,
        target: rules.target,
        skip: job.skip,
    })
}

fn route_from_state(state: &State) -> Result<Route, BodyError> {
    Ok(Route(u32::from(state.skip)))
}

fn skip_state(state: &State) -> Result<State, BodyError> {
    Ok(State {
        item: state.item,
        initial: state.initial,
        progress: state.progress,
        step: state.step,
        target: state.target,
        skip: state.skip,
    })
}

fn temp_of_state(state: &State) -> Result<Temp, BodyError> {
    // 轮次序号由业务字段推导：`(progress - initial) / step + 1`。
    let round = (state.progress.saturating_sub(state.initial)) / state.step.max(1) + 1;
    Ok(Temp {
        item: state.item,
        round,
    })
}

/// 最深异步 Arc 具体 Node：真实 `&State` 借用下推进一轮。
pub(crate) struct ProgressNode;

impl NodeCall1<State, Data<State>> for ProgressNode {
    fn call<'a>(&'a self, state: &'a State) -> NodeFut<'a, State> {
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

/// 同形但先在真实借用处挂起的 Arc Node（L02／L20 用）。
pub(crate) struct GatedProgressNode;

impl NodeCall1<State, Data<State>> for GatedProgressNode {
    fn call<'a>(&'a self, state: &'a State) -> NodeFut<'a, State> {
        Box::pin(async move {
            super::test_support::gate_wait().await;
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

fn item_result(state: &State) -> Result<ItemResult, BodyError> {
    Ok(ItemResult {
        item: state.item,
        progress: state.progress,
    })
}

#[allow(clippy::ptr_arg)] // 业务 Data 就是 `Vec<ItemResult>`，Node 只接收其只读借用。
async fn report_from_results(results: &Vec<ItemResult>) -> Result<Report, BodyError> {
    Ok(Report {
        count: results.len() as u32,
        total: results.iter().map(|item| item.progress).sum(),
    })
}

/// 结构体 unit Node：成功只表示完成动作，不产生 unit Data。
struct UnitReportNode;

impl NodeCall1<Report, Unit> for UnitReportNode {
    fn call<'a>(&'a self, report: &'a Report) -> NodeFut<'a, ()> {
        Box::pin(async move {
            let _ = (report.count, report.total);
            Ok(())
        })
    }
}

// ---------------------------------------------------------------- §4.1 组合

/// Round body：`Node -> Temp` ＋ 异步 Arc Node：`&State -> State`。
fn round_body(gated: bool) -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("round start");
    let _temp: DataRef<Temp> = flow
        .then::<_, SyncFnSig<(State,), Data<Temp>>, _>(
            temp_of_state as fn(&State) -> Result<Temp, BodyError>,
            state.clone(),
        )
        .expect("temp step");
    let produced: DataRef<State> = if gated {
        flow.then::<_, ArcNodeSig<(State,), Data<State>>, _>(Arc::new(GatedProgressNode), state)
            .expect("gated progress step")
    } else {
        flow.then::<_, ArcNodeSig<(State,), Data<State>>, _>(Arc::new(ProgressNode), state)
            .expect("progress step")
    };
    flow.finish::<Data<State>, _>(produced)
        .expect("round finish")
}

/// iterate branch：`Loop<Iter1<State>>`，body 是完成态 Round Flow。
fn iterate_branch(gated: bool) -> Loop<Iter1<State>> {
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter loop");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(round_body(gated))
        .expect("iter body");
    builder.finish().expect("iter finish")
}

/// 带 Orchestrator branch 的 Match（iterate = Loop，skip = Node）。
fn item_match_with_loop(gated: bool) -> Match<Route, State, Data<State>> {
    let mut matched: MatchBuilder<Route, State, Data<State>> =
        MatchBuilder::start().expect("match");
    matched
        .branch::<_, OrchSig<State, Data<State>>>(Route(0), iterate_branch(gated))
        .expect("iterate branch");
    matched
        .branch::<_, SyncFnSig<(State,), Data<State>>>(
            Route(1),
            skip_state as fn(&State) -> Result<State, BodyError>,
        )
        .expect("skip branch");
    matched.finish().expect("match finish")
}

/// item Flow：`Job + Rules -> ItemResult`（Node／Match／Loop 异构 Step）。
fn item_flow(gated: bool) -> Flow<(Job, Rules), Data<ItemResult>> {
    let (mut flow, (job, rules)) = FlowBuilder::<(Job, Rules)>::start().expect("item start");
    let state: DataRef<State> = flow
        .then::<_, SyncFnSig<(Job, Rules), Data<State>>, _>(
            state_from_job as fn(&Job, &Rules) -> Result<State, BodyError>,
            (job, rules),
        )
        .expect("state step");
    let route: DataRef<Route> = flow
        .then::<_, SyncFnSig<(State,), Data<Route>>, _>(
            route_from_state as fn(&State) -> Result<Route, BodyError>,
            state.clone(),
        )
        .expect("route step");
    let routed: DataRef<State> = flow
        .then::<_, OrchSig<(Route, State), Data<State>>, _>(
            item_match_with_loop(gated),
            (route, state),
        )
        .expect("match step");
    let result: DataRef<ItemResult> = flow
        .then::<_, SyncFnSig<(State,), Data<ItemResult>>, _>(
            item_result as fn(&State) -> Result<ItemResult, BodyError>,
            routed,
        )
        .expect("result step");
    flow.finish::<Data<ItemResult>, _>(result)
        .expect("item finish")
}

/// SubFlow：`Vec<Job> + Rules -> Vec<ItemResult>`（Each 为唯一 Step）。
pub(crate) fn subflow(gated: bool) -> Flow<(Vec<Job>, Rules), Data<Vec<ItemResult>>> {
    let (mut flow, (jobs, rules)) = FlowBuilder::<(Vec<Job>, Rules)>::start().expect("sub start");
    let mut each: EachBuilder<EachShared<Job, Rules>, ItemResult> =
        EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<(Job, Rules), Data<ItemResult>>>(item_flow(gated))
        .expect("each body");
    let each: Each<EachShared<Job, Rules>, ItemResult> = each.finish().expect("each finish");
    let collected: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<(Vec<Job>, Rules), Data<Vec<ItemResult>>>, _>(each, (jobs, rules))
        .expect("each step");
    flow.finish::<Data<Vec<ItemResult>>, _>(collected)
        .expect("sub finish")
}

/// §4.1 的子编排：**同一 SubFlow** 的两个声明输出（Each 的 Vec 与 async 函数产生的 Report）。
///
/// 按复审 R11-13：两个异构结果由同一个 SubFlow Export 给出，Unit 结构体 Node 也在其中。
pub(crate) fn subflow_with_outputs(
    gated: bool,
) -> Flow<(Vec<Job>, Rules), Out2<Vec<ItemResult>, Report>> {
    let (mut flow, (jobs, rules)) = FlowBuilder::<(Vec<Job>, Rules)>::start().expect("sub start");
    let results: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<(Vec<Job>, Rules), Data<Vec<ItemResult>>>, _>(
            subflow(gated),
            (jobs, rules),
        )
        .expect("each step");
    // 普通 async 函数 Step（不是 `Arc` 异步方法）。
    let report: DataRef<Report> = flow
        .then::<_, super::signature::AsyncFnSig<(Vec<ItemResult>,), Data<Report>>, _>(
            report_from_results,
            results.clone(),
        )
        .expect("async report step");
    let _unit: () = flow
        .then::<_, NodeSig<(Report,), Unit>, _>(UnitReportNode, report.clone())
        .expect("unit step");
    flow.finish::<Out2<Vec<ItemResult>, Report>, _>((results, report))
        .expect("sub finish")
}

/// §4.1 完整主场景：Root 的单 Step 就是上面的双输出 SubFlow。
pub(crate) fn main_chain(gated: bool) -> Flow<(Vec<Job>, Rules), Out2<Vec<ItemResult>, Report>> {
    let (mut flow, (jobs, rules)) = FlowBuilder::<(Vec<Job>, Rules)>::start().expect("root start");
    let (results, report) = flow
        .then::<_, OrchSig<(Vec<Job>, Rules), Out2<Vec<ItemResult>, Report>>, _>(
            subflow_with_outputs(gated),
            (jobs, rules),
        )
        .expect("double-output subflow step");
    flow.finish::<Out2<Vec<ItemResult>, Report>, _>((results, report))
        .expect("root finish")
}

/// 主场景的固定输入：skip（id 0）、iterate 初值 0（id 1）、iterate 初值 1（id 2）。
pub(crate) fn main_input() -> (Vec<Job>, Rules) {
    (
        vec![
            Job {
                id: 0,
                progress: 0,
                skip: true,
            },
            Job {
                id: 1,
                progress: 0,
                skip: false,
            },
            Job {
                id: 2,
                progress: 1,
                skip: false,
            },
        ],
        Rules { step: 1, target: 2 },
    )
}

/// Root Unit 变体：产生 Results 与 Report 后同时丢弃（显式 Unit 收口）。
fn unit_chain() -> Flow<(Vec<Job>, Rules), Unit> {
    let (mut flow, (jobs, rules)) = FlowBuilder::<(Vec<Job>, Rules)>::start().expect("root start");
    let results: DataRef<Vec<ItemResult>> = flow
        .then::<_, OrchSig<(Vec<Job>, Rules), Data<Vec<ItemResult>>>, _>(
            subflow(false),
            (jobs, rules),
        )
        .expect("subflow step");
    let report: DataRef<Report> = flow
        .then::<_, super::signature::AsyncFnSig<(Vec<ItemResult>,), Data<Report>>, _>(
            report_from_results,
            results,
        )
        .expect("report step");
    let _unit: () = flow
        .then::<_, NodeSig<(Report,), Unit>, _>(UnitReportNode, report)
        .expect("unit step");
    flow.finish::<Unit, _>(()).expect("finish unit")
}

// ---------------------------------------------------------------- 观察辅助

fn count(events: &[String], prefix: &str) -> usize {
    events
        .iter()
        .filter(|event| event.starts_with(prefix))
        .count()
}

/// 断言实际 take 的 DataId 序列与声明输出端口（Definition 声明顺序）逐项一致。
pub(crate) fn assert_take_order_by_declaration(
    snapshots: &[super::test_support::RootSnapshot],
    declared: &[super::ref_id::RefId],
    taken: &[super::identity::DataId],
) {
    let after_commit = last_snapshot(snapshots, RootSnapshotPhase::AfterCommit);
    let expected: Vec<super::identity::DataId> = declared
        .iter()
        .map(|position| {
            after_commit
                .root_refs
                .iter()
                .find(|(candidate, _)| candidate == position)
                .map(|(_, target)| match target {
                    super::scope::TargetSnapshot::Data(id) => id.clone(),
                    other => panic!("declared output must be complete Data: {other:?}"),
                })
                .unwrap_or_else(|| panic!("declared port {position} not bound: {after_commit:?}"))
        })
        .collect();
    assert_eq!(taken.to_vec(), expected, "take 顺序 = 声明顺序");
}

/// 读取本次样本的 Root 观察点：先证明严格观察全部成功，再返回快照。
pub(crate) fn take_snapshots_checked() -> Vec<super::test_support::RootSnapshot> {
    super::test_support::assert_no_observation_errors();
    take_root_snapshots()
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

/// 成功收口后：Root refs／owned 空、Root 关闭、本次样本的 take 序列与声明顺序一致。
fn assert_settled_root(
    taken: &[super::identity::DataId],
    expected_takes: usize,
) -> Vec<super::test_support::RootSnapshot> {
    let snapshots = take_snapshots_checked();
    let after_commit = last_snapshot(&snapshots, RootSnapshotPhase::AfterCommit);
    assert_eq!(
        after_commit.taken_alive,
        vec![false; expected_takes],
        "提交段已移出实例: {after_commit:?}"
    );
    assert_eq!(
        after_commit.taken_owned_by,
        vec![None; expected_takes],
        "提交段已解除 Root 责任: {after_commit:?}"
    );
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(after_close.root_refs.len(), 0, "Root 本地引用失效");
    assert_eq!(after_close.root_owned.len(), 0, "Root 无剩余责任");
    assert_eq!(after_close.root_state, ScopeState::Closed, "Root 已关闭");
    assert_eq!(
        after_close.taken_alive,
        vec![false; expected_takes],
        "声明输出已移出 Container: {after_close:?}"
    );
    assert_eq!(
        after_close.taken_owned_by,
        vec![None; expected_takes],
        "责任已移交: {after_close:?}"
    );
    assert_eq!(
        taken.len(),
        expected_takes,
        "真实 take 调用次数等于声明输出数: {taken:?}"
    );
    snapshots
}

/// 角色与 parent 链：item 直接归 Each、round 直接归 Loop。
fn assert_role_parent_chain() {
    let creations = boundary_creation_snapshot();
    let role_of = |scope: &ScopeId| {
        creations
            .iter()
            .find(|(child, _, _)| child == scope)
            .map(|(_, _, role)| *role)
    };
    for (scope, parent, role) in &creations {
        match role {
            ScopeRole::Item => {
                assert_eq!(
                    role_of(parent),
                    Some(ScopeRole::Each),
                    "Item 的 parent 是 Each: {scope}"
                );
            }
            ScopeRole::Round => {
                assert_eq!(
                    role_of(parent),
                    Some(ScopeRole::Loop),
                    "Round 的 parent 是 Loop: {scope}"
                );
            }
            ScopeRole::Loop => {
                // item Flow 中 Loop 是 Match 的 branch 包装内的唯一 Step。
                assert_eq!(
                    role_of(parent),
                    Some(ScopeRole::Branch),
                    "Loop 的 parent 是 branch 包装: {scope}"
                );
            }
            ScopeRole::Branch => {
                assert_eq!(
                    role_of(parent),
                    Some(ScopeRole::Match),
                    "branch 包装的 parent 是 Match: {scope}"
                );
            }
            ScopeRole::Match => {
                assert_eq!(
                    role_of(parent),
                    Some(ScopeRole::Flow),
                    "Match 的 parent 是 item 包装 Flow: {scope}"
                );
            }
            ScopeRole::Each => {
                assert_eq!(
                    role_of(parent),
                    Some(ScopeRole::Flow),
                    "Each 的 parent 是 SubFlow: {scope}"
                );
            }
            ScopeRole::Flow => {
                // 包装 Flow 的 parent 是 Root（SubFlow）、Item（item 包装）或 Round（round 包装）。
                let parent_role = role_of(parent);
                assert!(
                    parent.seq() == 0
                        || matches!(
                            parent_role,
                            Some(ScopeRole::Flow) | Some(ScopeRole::Item) | Some(ScopeRole::Round)
                        ),
                    "Flow 包装的实际 parent: {scope} -> {parent} ({parent_role:?})"
                );
            }
        }
    }
    assert!(
        creations
            .iter()
            .any(|(_, _, role)| *role == ScopeRole::Item),
        "至少一个 Item Scope: {creations:?}"
    );
    assert!(
        creations
            .iter()
            .any(|(_, _, role)| *role == ScopeRole::Round),
        "至少一个 Round Scope: {creations:?}"
    );
}

// ---------------------------------------------------------------- L01～L03

#[test]
fn l01_main_chain_returns_two_heterogeneous_owned_outputs() {
    strict_reset_observations();
    boundary_creation_reset();
    closed_scope_reset();
    let (jobs, rules) = main_input();
    let root = main_chain(false);
    let declared: Vec<super::ref_id::RefId> = root
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let (results, report) = drive(Runtime::execute::<_, _, Out2<Vec<ItemResult>, Report>>(
        &root,
        (jobs, rules),
    ))
    .expect("main chain succeeds");

    // 逐 item 数值与顺序：skip=0，两个 iterate 都到 2。
    let observed: Vec<(u32, u32)> = results
        .iter()
        .map(|item| (item.item, item.progress))
        .collect();
    assert_eq!(observed, vec![(0, 0), (1, 2), (2, 2)], "逐 item 结果与顺序");
    assert_eq!((report.count, report.total), (3, 4), "Report 聚合值");

    // 每个实例的 Drop 见证（临时值按 item／round 记录，不用同名总数掩盖）。
    let events = take_events();
    assert_eq!(count(&events, "temp-dropped:1:1"), 1, "{events:?}");
    assert_eq!(count(&events, "temp-dropped:1:2"), 1, "{events:?}");
    assert_eq!(count(&events, "temp-dropped:2:1"), 1, "{events:?}");
    assert_eq!(count(&events, "temp-dropped"), 3, "恰好三轮：{events:?}");
    assert_eq!(
        count(&events, "item-result-dropped"),
        0,
        "结果仍由 Application 持有"
    );
    assert_eq!(count(&events, "report-dropped"), 0);
    assert_eq!(count(&events, "job-dropped"), 3, "输入各析构一次");
    assert_eq!(count(&events, "rules-dropped"), 1);
    assert_eq!(
        count(&events, "state-dropped:0:0"),
        2,
        "skip item 两个 State 实例: {events:?}"
    );
    assert_eq!(count(&events, "state-dropped:1:0"), 1);
    assert_eq!(count(&events, "state-dropped:1:1"), 1);
    assert_eq!(count(&events, "state-dropped:1:2"), 1);
    assert_eq!(count(&events, "state-dropped:2:1"), 1);
    assert_eq!(count(&events, "state-dropped:2:2"), 1);
    assert_eq!(
        count(&events, "state-dropped"),
        7,
        "全部 State 实例各回收一次"
    );

    // Application drop 后逐实例析构一次。
    drop(results);
    drop(report);
    let events = take_events();
    assert_eq!(count(&events, "item-result-dropped:0:0"), 1);
    assert_eq!(count(&events, "item-result-dropped:1:2"), 1);
    assert_eq!(count(&events, "item-result-dropped:2:2"), 1);
    assert_eq!(count(&events, "report-dropped:3:4"), 1);

    let taken = take_log_snapshot();
    let snapshots = assert_settled_root(&taken, 2);
    assert_take_order_by_declaration(&snapshots, &declared, &taken);
    assert_role_parent_chain();
    let closed = closed_scope_snapshot();
    assert!(
        closed.iter().filter(|scope| scope.seq() == 0).count() == 1,
        "唯一 Root 关闭一次: {closed:?}"
    );
}

#[test]
fn l02_gated_pending_then_ready_in_the_same_future() {
    strict_reset_observations();
    let (jobs, rules) = main_input();
    install_gate();
    let root = main_chain(true);
    let future = Runtime::execute::<_, _, Out2<Vec<ItemResult>, Report>>(&root, (jobs, rules));
    let boxed = advance_to_pending(future, 1);
    let events = take_events();
    assert_eq!(
        count(&events, "temp-dropped"),
        0,
        "挂起时未完成任何轮: {events:?}"
    );
    assert_eq!(count(&events, "item-result-dropped"), 0, "无 item 结果析构");
    assert_eq!(count(&events, "job-dropped"), 0, "输入仍由 Execution 持有");
    assert_eq!(
        count(&events, "item-state:0"),
        1,
        "skip item 已进入: {events:?}"
    );
    assert_eq!(count(&events, "item-state:1"), 1, "当前 item 已进入");
    assert_eq!(
        count(&events, "item-state:2"),
        0,
        "后续 item 未执行: {events:?}"
    );

    release_gate();
    let (results, report) = drive_pinned(boxed).expect("same future resumes to Ready");
    let observed: Vec<u32> = results.iter().map(|item| item.progress).collect();
    assert_eq!(observed, vec![0, 2, 2], "恢复后恰好执行剩余轮次");
    assert_eq!((report.count, report.total), (3, 4));
    let events = take_events();
    assert_eq!(count(&events, "temp-dropped"), 3, "三轮各一次");
    assert_eq!(count(&events, "job-dropped"), 3);
}

#[test]
fn l03_single_execution_domain_and_closed_scopes() {
    strict_reset_observations();
    boundary_address_reset();
    boundary_creation_reset();
    closed_scope_reset();
    let (jobs, rules) = main_input();
    let (results, report) = drive(Runtime::execute::<_, _, Out2<Vec<ItemResult>, Report>>(
        &main_chain(false),
        (jobs, rules),
    ))
    .expect("main chain succeeds");
    drop(results);
    drop(report);

    // 唯一执行域：创建计数恰好 +1，且每个真实创建点都带同一身份／Coordinator／Container 地址。
    assert_eq!(
        super::context::creation_counts::contexts(),
        1,
        "恰一个新 Context"
    );
    assert_eq!(
        super::context::creation_counts::coordinators(),
        1,
        "恰一个新 Coordinator"
    );
    assert_eq!(
        super::context::creation_counts::containers(),
        1,
        "恰一个新 Container"
    );
    let addresses = boundary_address_snapshot();
    assert!(!addresses.is_empty(), "nested 边界已创建: {addresses:?}");
    let identity = addresses[0].1;
    let coordinator = addresses[0].2;
    let container = addresses[0].3;
    for (scope, candidate_identity, candidate_coordinator, candidate_container) in &addresses {
        assert!(
            std::ptr::eq(*candidate_identity, identity),
            "{scope} 使用唯一 Execution 身份"
        );
        assert!(
            std::ptr::eq(*candidate_coordinator, coordinator),
            "唯一 Coordinator"
        );
        assert!(
            std::ptr::eq(*candidate_container, container),
            "唯一 Container"
        );
    }

    assert_role_parent_chain();

    // 成功后全部已创建 Scope 都 Closed，且本地引用集合为空。
    let closed = closed_scope_snapshot();
    let creations = boundary_creation_snapshot();
    for (scope, _, _) in &creations {
        assert!(
            closed.contains(scope),
            "{scope} 在成功收口后已关闭: {closed:?}"
        );
    }
    assert_eq!(
        closed.iter().filter(|scope| scope.seq() == 0).count(),
        1,
        "Root 关闭一次: {closed:?}"
    );
}

// ---------------------------------------------------------------- L06／L07

#[test]
fn l06_empty_each_and_unit_root() {
    // 空 Each：不调用 body，产生完整空 Vec。
    strict_reset_observations();
    let empty = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &subflow(false),
        (Vec::new(), Rules { step: 1, target: 2 }),
    ))
    .expect("empty each");
    assert!(empty.is_empty(), "空集合产生空 Vec");
    let events = take_events();
    assert_eq!(
        count(&events, "state-dropped"),
        0,
        "空 Each 不调用 body: {events:?}"
    );
    assert_eq!(count(&events, "rules-dropped"), 1, "shared 仍析构一次");

    // 一个 item／一轮 Loop 边界对照。
    strict_reset_observations();
    let single = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &subflow(false),
        (
            vec![Job {
                id: 9,
                progress: 2,
                skip: false,
            }],
            Rules { step: 1, target: 2 },
        ),
    ))
    .expect("single item");
    assert_eq!(single.len(), 1);
    let events = take_events();
    assert_eq!(count(&events, "temp-dropped:9:1"), 1, "恰一轮: {events:?}");
    assert_eq!(count(&events, "temp-dropped"), 1);

    // Root 显式 Unit：零 take、零 unit DataId、清理实际临时值。
    strict_reset_observations();
    let (jobs, rules) = main_input();
    drive(Runtime::execute::<_, _, Unit>(&unit_chain(), (jobs, rules))).expect("unit root");
    assert!(
        take_log_snapshot().is_empty(),
        "unit 收口零 take: {:?}",
        take_log_snapshot()
    );
    let snapshots = take_snapshots_checked();
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(after_close.planned_takes, 0);
    assert_eq!(after_close.root_state, ScopeState::Closed);
    let events = take_events();
    assert_eq!(
        count(&events, "temp-dropped"),
        3,
        "临时值全部清理: {events:?}"
    );
    assert_eq!(
        count(&events, "item-result-dropped"),
        3,
        "未选中结果在退出时清理"
    );
    assert_eq!(count(&events, "report-dropped"), 1);
}

#[test]
fn l07_same_definition_and_arc_node_are_reused() {
    strict_reset_observations();
    boundary_address_reset();
    // 同一完成态定义执行两次：各自独立执行身份，结果重算。
    let root = main_chain(true);
    install_gate();
    let first = advance_to_pending(
        Runtime::execute::<_, _, Out2<Vec<ItemResult>, Report>>(&root, main_input()),
        1,
    );
    let second = advance_to_pending(
        Runtime::execute::<_, _, Out2<Vec<ItemResult>, Report>>(&root, main_input()),
        1,
    );
    let addresses = boundary_address_snapshot();
    assert!(addresses.len() >= 2, "两次执行各自的创建点: {addresses:?}");
    assert!(
        !std::ptr::eq(addresses[0].3, addresses[addresses.len() - 1].3),
        "两次执行 Container 不同: {addresses:?}"
    );
    release_gate();
    let (first_results, first_report) = drive_pinned(first).expect("first execution");
    let (second_results, second_report) = drive_pinned(second).expect("second execution");
    assert_eq!(
        first_results
            .iter()
            .map(|item| item.progress)
            .collect::<Vec<_>>(),
        second_results
            .iter()
            .map(|item| item.progress)
            .collect::<Vec<_>>(),
        "同定义两次执行结果重算一致"
    );
    assert_eq!((first_report.count, first_report.total), (3, 4));
    assert_eq!((second_report.count, second_report.total), (3, 4));
    // 复用定义：完整 RefId（定义级）相同，DataId（执行级）互不相同。
    let exports = super::test_support::take_export_pre_cleanup();
    // 按**执行域身份**分组（同一次 Execution 内的所有层共享同一身份指针）。
    let addresses = boundary_address_snapshot();
    let identity_of = |scope: &super::identity::ScopeId| {
        addresses
            .iter()
            .find(|(child, _, _, _)| child == scope)
            .map(|(_, identity, _, _)| *identity)
            .unwrap_or(std::ptr::null())
    };
    let mut groups: Vec<(
        *const (),
        Vec<&super::test_support::ExportPreCleanupSnapshot>,
    )> = Vec::new();
    for snapshot in exports
        .iter()
        .filter(|snapshot| snapshot.phase == super::test_support::ExportSnapshotPhase::Before)
    {
        let Some(child) = snapshot.child.as_ref() else {
            continue;
        };
        let identity = identity_of(child);
        match groups
            .iter_mut()
            .find(|(candidate, _)| *candidate == identity)
        {
            Some((_, snapshots)) => snapshots.push(snapshot),
            None => groups.push((identity, vec![snapshot])),
        }
    }
    let mut ref_sets: Vec<Vec<super::ref_id::RefId>> = Vec::new();
    let mut data_sets: Vec<Vec<super::identity::DataId>> = Vec::new();
    for (_, snapshots) in &groups {
        let mut refs = Vec::new();
        let mut data = Vec::new();
        for snapshot in snapshots {
            if let Some(bindings) = snapshot.child_refs.as_ref() {
                for (position, target) in bindings {
                    refs.push(position.clone());
                    if let super::scope::TargetSnapshot::Data(id) = target {
                        data.push(id.clone());
                    }
                }
            }
        }
        refs.sort_by_key(super::ref_id::RefId::seq);
        data.sort_by_key(super::identity::DataId::seq);
        ref_sets.push(refs);
        data_sets.push(data);
    }
    assert_eq!(ref_sets.len(), 2, "两次执行各自的 Export: {groups:?}");
    assert_eq!(ref_sets[0], ref_sets[1], "同一定义的完整 RefId 相同");
    assert!(
        data_sets[0].iter().all(|id| !data_sets[1].contains(id)),
        "两次执行的 DataId 互不相同: {data_sets:?}"
    );

    // 同一 Arc 具体 Node 在两个 Step 中复用。
    strict_reset_observations();
    let shared = Arc::new(ProgressNode);
    let reused = shared_node_flow(shared.clone());
    let (left, right) = drive(Runtime::execute::<_, _, Out2<State, State>>(
        &reused,
        (State {
            item: 7,
            initial: 0,
            progress: 0,
            step: 1,
            target: 2,
            skip: false,
        },),
    ))
    .expect("shared arc node");
    assert_eq!(
        (left.progress, right.progress),
        (1, 1),
        "同一 Node 两处复用"
    );
    drop(left);
    drop(right);

    // 一次失败不污染另一次成功执行。
    strict_reset_observations();
    let failing = drive(Runtime::execute::<_, _, Data<ItemResult>>(
        &failing_item_flow(),
        (
            Job {
                id: 8,
                progress: 0,
                skip: false,
            },
            Rules { step: 1, target: 2 },
        ),
    ))
    .expect_err("failing execution");
    assert_eq!(failing.stage(), RootErrorStage::Body, "{failing:?}");
    let ok = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &subflow(false),
        (
            vec![Job {
                id: 10,
                progress: 2,
                skip: false,
            }],
            Rules { step: 1, target: 2 },
        ),
    ))
    .expect("later execution unaffected");
    assert_eq!(ok.len(), 1);
}

/// 两个 Step 复用同一 `Arc<具体 Node>`。
fn shared_node_flow(shared: Arc<ProgressNode>) -> Flow<(State,), Out2<State, State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("start");
    let first: DataRef<State> = flow
        .then::<_, ArcNodeSig<(State,), Data<State>>, _>(shared.clone(), state.clone())
        .expect("first arc step");
    let second: DataRef<State> = flow
        .then::<_, ArcNodeSig<(State,), Data<State>>, _>(shared, state)
        .expect("second arc step");
    flow.finish::<Out2<State, State>, _>((first, second))
        .expect("finish")
}

/// item Flow 的失败变体：第二个 Step 的 Node 返回执行错误（输入借用仍成立）。
fn failing_item_flow() -> Flow<(Job, Rules), Data<ItemResult>> {
    let (mut flow, (job, rules)) = FlowBuilder::<(Job, Rules)>::start().expect("start");
    let job_for_fail = job.clone();
    let rules_for_fail = rules.clone();
    let _state: DataRef<State> = flow
        .then::<_, SyncFnSig<(Job, Rules), Data<State>>, _>(
            state_from_job as fn(&Job, &Rules) -> Result<State, BodyError>,
            (job, rules),
        )
        .expect("state step");
    let failed: DataRef<ItemResult> = flow
        .then::<_, SyncFnSig<(Job, Rules), Data<ItemResult>>, _>(
            failing_item_node as fn(&Job, &Rules) -> Result<ItemResult, BodyError>,
            (job_for_fail, rules_for_fail),
        )
        .expect("failing step");
    flow.finish::<Data<ItemResult>, _>(failed).expect("finish")
}

fn failing_item_node(job: &Job, rules: &Rules) -> Result<ItemResult, BodyError> {
    let _ = (job, rules);
    Err(BodyError::new("item body failed"))
}

/// V21-11 复用形状：以 `State` 为 element 的 Each 及其 SubFlow（单输入）。
pub(crate) mod item_flow_and_subflow_helpers {
    use super::*;

    /// Each body：Node 推进一轮，再由 Node 产生 owned `ItemResult`。
    pub(crate) fn item_state_flow() -> Flow<(State,), Data<ItemResult>> {
        let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body start");
        let progressed: DataRef<State> = flow
            .then::<_, ArcNodeSig<(State,), Data<State>>, _>(Arc::new(ProgressNode), state)
            .expect("progress step");
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

    /// `Each<EachOnly<State>, ItemResult>`。
    pub(crate) fn each_over_states() -> Each<EachOnly<State>, ItemResult> {
        let mut each: EachBuilder<EachOnly<State>, ItemResult> =
            EachBuilder::start().expect("each");
        each.then_body::<_, OrchSig<State, Data<ItemResult>>>(item_state_flow())
            .expect("each body");
        each.finish().expect("each finish")
    }

    /// `Flow<(Vec<State>,), Data<Vec<ItemResult>>>`：Each 为唯一 Step。
    pub(crate) fn subflow_over_states() -> Flow<(Vec<State>,), Data<Vec<ItemResult>>> {
        let (mut flow, states) = FlowBuilder::<(Vec<State>,)>::start().expect("sub start");
        let collected: DataRef<Vec<ItemResult>> = flow
            .then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(each_over_states(), states)
            .expect("each step");
        flow.finish::<Data<Vec<ItemResult>>, _>(collected)
            .expect("sub finish")
    }
}
