//! V21-11 验收样本（二）：§4.2 补充组合 L04／L05／L08／L09。
//!
//! 全部经真实 `Runtime::execute`；故障注入只改操作前元数据（DEFENCE），判断与清理走生产路径。

use std::sync::Arc;

use super::builder::TypedCallBuilder;
use super::context::BodyError;
use super::data_ref::DataRef;
use super::each::{Each, EachBuilder, EachOnly};
use super::flow::{Flow, FlowBuilder};
use super::loop_orchestrator::{Iter1, Loop, LoopBuilder, LoopFault, Retry1, install_loop_fault};
use super::match_orchestrator::MatchBuilder;
use super::orchestrator::ScopeRole;
use super::runtime::{RootErrorStage, Runtime};
use super::scope::TargetSnapshot;
use super::signature::{Data, OrchSig, SyncFnSig};
use super::test_support::{
    ExportFaultHit, ExportSnapshotPhase, PromoteStateRecord, RoundCollectOperation,
    RoundCollectPreCleanupSnapshot, RoundCollectSnapshotPhase, advance_to_pending,
    boundary_creation_reset, boundary_creation_snapshot, closed_scope_reset, closed_scope_snapshot,
    drive, drive_pinned, install_gate, item_target_snapshot, record, release_gate,
    strict_reset_observations, take_events, take_export_fault_hits, take_export_pre_cleanup,
    take_log_snapshot, take_promote_states, take_round_collect_pre_cleanup,
};
use super::v21_11_tests::{ItemResult, ProgressNode, Route, State};

// ---------------------------------------------------------------- §4.2.1 业务类型与 Node

/// Round 内产生的作业项。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Work {
    pub(crate) item: u32,
    pub(crate) value: u32,
}

/// Each 在 Round 内收集的结果。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct WorkResult {
    pub(crate) item: u32,
    pub(crate) value: u32,
}

fn works_of_state(state: &State) -> Result<Vec<Work>, BodyError> {
    record("works-produced");
    Ok(vec![
        Work {
            item: state.item,
            value: state.progress,
        },
        Work {
            item: state.item,
            value: state.progress + 1,
        },
    ])
}

fn work_route(state: &State) -> Result<Route, BodyError> {
    Ok(Route(u32::from(state.skip)))
}

fn work_result(work: &Work) -> Result<WorkResult, BodyError> {
    Ok(WorkResult {
        item: work.item,
        value: work.value,
    })
}

#[allow(clippy::ptr_arg)] // 业务 Data 就是 `Vec<WorkResult>`，Node 只接收其只读借用。
fn state_after_work(results: &Vec<WorkResult>, state: &State) -> Result<State, BodyError> {
    record(&format!("round-collected:{}", results.len()));
    let bump = results.iter().map(|item| item.value).max().unwrap_or(0) + 1;
    Ok(State {
        item: state.item,
        initial: state.initial,
        progress: (state.progress + bump).min(state.target),
        step: state.step,
        target: state.target,
        skip: state.skip,
    })
}

/// §4.2.1：`Each<EachOnly<Work>, WorkResult>` 作为 Match branch。
fn work_each() -> Each<EachOnly<Work>, WorkResult> {
    let mut each: EachBuilder<EachOnly<Work>, WorkResult> =
        EachBuilder::start().expect("work each");
    each.then_body::<_, SyncFnSig<(Work,), Data<WorkResult>>>(
        work_result as fn(&Work) -> Result<WorkResult, BodyError>,
    )
    .expect("work each body");
    each.finish().expect("work each finish")
}

/// §4.2.1 Round body：Loop→Flow→Match→Each 的中间层，最后产生新 State。
fn loop_match_each_body() -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body start");
    let works: DataRef<Vec<Work>> = flow
        .then::<_, SyncFnSig<(State,), Data<Vec<Work>>>, _>(
            works_of_state as fn(&State) -> Result<Vec<Work>, BodyError>,
            state.clone(),
        )
        .expect("works step");
    let route: DataRef<Route> = flow
        .then::<_, SyncFnSig<(State,), Data<Route>>, _>(
            work_route as fn(&State) -> Result<Route, BodyError>,
            state.clone(),
        )
        .expect("route step");
    let mut matched: MatchBuilder<Route, Vec<Work>, Data<Vec<WorkResult>>> =
        MatchBuilder::start().expect("work match");
    matched
        .branch::<_, OrchSig<Vec<Work>, Data<Vec<WorkResult>>>>(Route(0), work_each())
        .expect("each branch");
    matched
        .default::<_, SyncFnSig<(Vec<Work>,), Data<Vec<WorkResult>>>>(
            (|works: &Vec<Work>| {
                Ok(works
                    .iter()
                    .map(|work| WorkResult {
                        item: work.item,
                        value: work.value,
                    })
                    .collect())
            }) as fn(&Vec<Work>) -> Result<Vec<WorkResult>, BodyError>,
        )
        .expect("default branch");
    let matched = matched.finish().expect("work match finish");
    let results: DataRef<Vec<WorkResult>> = flow
        .then::<_, OrchSig<(Route, Vec<Work>), Data<Vec<WorkResult>>>, _>(matched, (route, works))
        .expect("match step");
    let produced: DataRef<State> = flow
        .then::<_, SyncFnSig<(Vec<WorkResult>, State), Data<State>>, _>(
            state_after_work as fn(&Vec<WorkResult>, &State) -> Result<State, BodyError>,
            (results, state),
        )
        .expect("state step");
    flow.finish::<Data<State>, _>(produced)
        .expect("body finish")
}

/// §4.2.1 的 Iter Loop（Root 可直接执行）。
fn loop_match_each_root() -> Loop<Iter1<State>> {
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter loop");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(loop_match_each_body())
        .expect("iter body");
    builder.finish().expect("iter finish")
}

// ---------------------------------------------------------------- §4.2.2 item alias 作 Iter 初态

/// identity Flow 内的借用探针：真实借用 item alias 后等待闸门（无闸门时立即返回）。
pub(crate) struct AliasProbeNode;

impl super::node::NodeCall1<State, Data<Route>> for AliasProbeNode {
    fn call<'a>(&'a self, state: &'a State) -> super::signature::NodeFut<'a, Route> {
        Box::pin(async move {
            record("l04-alias-probe");
            super::test_support::gate_wait().await;
            Ok(Route(state.progress))
        })
    }
}

/// 把 item 目标在 cap 内重新暴露为同一 Data 的 identity Flow。
///
/// 内含一个借用探针 Step：它让样本能在 Export 之前把执行推进到该 child 已存在的
/// Pending 现场（R11-12 的完整 ScopeId 目标选择），再以**输入位置本身**作为输出。
fn identity_item_flow() -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("identity start");
    let _probe: DataRef<Route> = flow
        .then::<_, super::signature::NodeSig<(State,), Data<Route>>, _>(
            AliasProbeNode,
            state.clone(),
        )
        .expect("probe step");
    flow.finish::<Data<State>, _>(state)
        .expect("identity finish")
}

/// §4.2.2：Each 的 body 先 identity 重新暴露 item alias，再由 Mode 推进，最后显式产生 owned 结果。
fn item_alias_body(mode: ItemAliasMode) -> Flow<(State,), Data<ItemResult>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body start");
    let alias: DataRef<State> = flow
        .then::<_, OrchSig<State, Data<State>>, _>(identity_item_flow(), state.clone())
        .expect("identity step");
    let progressed: DataRef<State> = match mode {
        ItemAliasMode::Iter | ItemAliasMode::AliasRound => {
            let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter loop");
            let body = match mode {
                ItemAliasMode::Iter => round_body_with_arc(),
                _ => identity_iter_body(),
            };
            builder
                .then_body::<_, OrchSig<State, Data<State>>>(body)
                .expect("iter body");
            let iter = builder.finish().expect("iter finish");
            flow.then::<_, OrchSig<State, Data<State>>, _>(iter, alias)
                .expect("iter step")
        }
        ItemAliasMode::Owned => flow
            .then::<_, SyncFnSig<(State,), Data<State>>, _>(
                (|state: &State| {
                    Ok(State {
                        item: state.item,
                        initial: state.initial,
                        progress: (state.progress + state.step).min(state.target),
                        step: state.step,
                        target: state.target,
                        skip: state.skip,
                    })
                }) as fn(&State) -> Result<State, BodyError>,
                alias,
            )
            .expect("owned step"),
    };
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

#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // 三个模式由不同样本使用，保持形状完整
pub(crate) enum ItemAliasMode {
    /// item alias 直接作为 Iter 初态，Round 由 Node 推进。
    Iter,
    /// item alias 直接作为 Iter 初态，Round body 是 identity（selected 本身就是 item 目标）。
    AliasRound,
    /// 对照：item alias 先由一个普通 Node 产生新 owned 再交给最终 Node。
    Owned,
}

fn round_body_with_arc() -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("round start");
    let produced: DataRef<State> = flow
        .then::<_, super::signature::ArcNodeSig<(State,), Data<State>>, _>(
            Arc::new(ProgressNode),
            state,
        )
        .expect("progress step");
    flow.finish::<Data<State>, _>(produced)
        .expect("round finish")
}

/// §4.2.2 的 Each：元素就是 State（item = CollectionItem of State）。
fn item_alias_each(mode: ItemAliasMode) -> Each<EachOnly<State>, ItemResult> {
    let mut each: EachBuilder<EachOnly<State>, ItemResult> = EachBuilder::start().expect("each");
    each.then_body::<_, OrchSig<State, Data<ItemResult>>>(item_alias_body(mode))
        .expect("each body");
    each.finish().expect("each finish")
}

// ---------------------------------------------------------------- §4.2.3 Retry 组合

/// Retry 的有状态路由：第 1 轮 Continue、之后 Finish（有限序列，不构成 Runtime 次数策略）。
pub(crate) struct RetryRouteNode(std::cell::Cell<u32>);

impl RetryRouteNode {
    fn new() -> Self {
        Self(std::cell::Cell::new(0))
    }
}

impl super::node::NodeCall1<State, Data<Route>> for RetryRouteNode {
    fn call<'a>(&'a self, _state: &'a State) -> super::signature::NodeFut<'a, Route> {
        let round = self.0.get();
        self.0.set(round + 1);
        Box::pin(async move { Ok(Route(u32::from(round > 0))) })
    }
}

/// §4.2.3：Retry 的 body Flow 内含 Match，由普通 Node 的业务字段表达有限序列。
fn retry_body() -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("retry body start");
    let route: DataRef<Route> = flow
        .then::<_, super::signature::NodeSig<(State,), Data<Route>>, _>(
            RetryRouteNode::new(),
            state.clone(),
        )
        .expect("route step");
    let mut matched: MatchBuilder<Route, State, Data<State>> =
        MatchBuilder::start().expect("retry match");
    matched
        .branch::<_, SyncFnSig<(State,), Data<State>>>(
            Route(1),
            (|state: &State| Ok(finish_state(state))) as fn(&State) -> Result<State, BodyError>,
        )
        .expect("finish branch");
    matched
        .default::<_, SyncFnSig<(State,), Data<State>>>(
            (|state: &State| {
                record("retry-continue");
                Ok(State {
                    item: state.item,
                    initial: state.initial,
                    progress: (state.progress + state.step).min(state.target),
                    step: state.step,
                    target: state.target,
                    skip: state.skip,
                })
            }) as fn(&State) -> Result<State, BodyError>,
        )
        .expect("continue branch");
    let matched = matched.finish().expect("retry match finish");
    let produced: DataRef<State> = flow
        .then::<_, OrchSig<(Route, State), Data<State>>, _>(matched, (route, state.clone()))
        .expect("match step");
    flow.finish::<Data<State>, _>(produced)
        .expect("retry body finish")
}

fn finish_state(state: &State) -> State {
    State {
        item: state.item,
        initial: state.initial,
        progress: state.target,
        step: state.step,
        target: state.target,
        skip: state.skip,
    }
}

fn retry_root() -> Loop<Retry1<State, State>> {
    let mut builder: LoopBuilder<Retry1<State, State>> = LoopBuilder::start().expect("retry loop");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(retry_body())
        .expect("retry body");
    builder.finish().expect("retry finish")
}

/// imported-alias 对照的路由 Node：第 1 轮走 Continue 分支、第 2 轮走 identity Finish 分支。
pub(crate) struct RetryAliasRouteNode(std::cell::Cell<u32>);

impl RetryAliasRouteNode {
    fn new() -> Self {
        Self(std::cell::Cell::new(0))
    }
}

impl super::node::NodeCall1<State, Data<Route>> for RetryAliasRouteNode {
    fn call<'a>(&'a self, _state: &'a State) -> super::signature::NodeFut<'a, Route> {
        let round = self.0.get();
        self.0.set(round + 1);
        Box::pin(async move { Ok(Route(u32::from(round > 0))) })
    }
}

/// Continue 轮的新 owned 值：progress 0 < target，表达 Continue。
fn zero_progress(state: &State) -> Result<State, BodyError> {
    Ok(State {
        item: state.item,
        initial: state.initial,
        progress: 0,
        step: state.step,
        target: state.target,
        skip: state.skip,
    })
}

/// imported-alias 对照的 body：第 1 轮新 owned Continue；第 2 轮 identity 原样暴露 imported
/// 初态（已表达 Finish），Promote 不转移 imported 责任。
fn retry_alias_body() -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body start");
    let route: DataRef<Route> = flow
        .then::<_, super::signature::NodeSig<(State,), Data<Route>>, _>(
            RetryAliasRouteNode::new(),
            state.clone(),
        )
        .expect("route step");
    let mut matched: MatchBuilder<Route, State, Data<State>> =
        MatchBuilder::start().expect("match");
    matched
        .branch::<_, SyncFnSig<(State,), Data<State>>>(
            Route(0),
            zero_progress as fn(&State) -> Result<State, BodyError>,
        )
        .expect("continue branch");
    matched
        .branch::<_, OrchSig<State, Data<State>>>(Route(1), identity_iter_body())
        .expect("finish branch");
    let matched = matched.finish().expect("match finish");
    let produced: DataRef<State> = flow
        .then::<_, OrchSig<(Route, State), Data<State>>, _>(matched, (route, state))
        .expect("match step");
    flow.finish::<Data<State>, _>(produced)
        .expect("body finish")
}

fn retry_alias_root() -> Loop<Retry1<State, State>> {
    let mut builder: LoopBuilder<Retry1<State, State>> = LoopBuilder::start().expect("retry loop");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(retry_alias_body())
        .expect("retry body");
    builder.finish().expect("retry finish")
}

// ---------------------------------------------------------------- 观察辅助

fn count(events: &[String], prefix: &str) -> usize {
    events
        .iter()
        .filter(|event| event.starts_with(prefix))
        .count()
}

fn round_snapshots() -> Vec<super::test_support::RoundCollectPreCleanupSnapshot> {
    take_round_collect_pre_cleanup()
        .into_iter()
        .filter(|snapshot| snapshot.phase == RoundCollectSnapshotPhase::Before)
        .collect()
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

fn loop_of_round(
    creations: &[(
        super::identity::ScopeId,
        super::identity::ScopeId,
        ScopeRole,
    )],
    round: &super::identity::ScopeId,
) -> super::identity::ScopeId {
    creation_parent(creations, round).expect("round has a recorded parent")
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

fn round_source(snapshot: &RoundCollectPreCleanupSnapshot) -> super::identity::ScopeId {
    snapshot.source.clone().expect("round source recorded")
}

/// 指定收口操作的全部 `Before` 观察。
fn operation_snapshots(
    snapshots: &[RoundCollectPreCleanupSnapshot],
    operation: RoundCollectOperation,
) -> Vec<&RoundCollectPreCleanupSnapshot> {
    snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.phase == RoundCollectSnapshotPhase::Before && snapshot.operation == operation
        })
        .collect()
}

// ---------------------------------------------------------------- L05

#[test]
fn l05_loop_match_each_returns_state_and_keeps_responsibility_chain() {
    strict_reset_observations();
    boundary_creation_reset();
    closed_scope_reset();
    let final_state = drive(Runtime::execute::<_, _, Data<State>>(
        &loop_match_each_root(),
        (state_with(0, 2),),
    ))
    .expect("loop-match-each root");
    assert_eq!(
        final_state.progress, 2,
        "正常 State 返回：有限推进到 target"
    );
    let events = take_events();
    assert_eq!(
        count(&events, "works-produced"),
        1,
        "一轮产生一次: {events:?}"
    );
    assert_eq!(
        count(&events, "round-collected:2"),
        1,
        "Match→Each 收集到两个结果"
    );
    assert_eq!(
        count(&events, "each-finish-owned:1"),
        1,
        "collector 完成并移交一份 Vec: {events:?}"
    );
    // 顺序证据：works → item Consume／collector 完成 → branch 结果 → Round Promote。
    let works_at = super::test_support::at(&events, "works-produced");
    let collect_at = super::test_support::at(&events, "collector-after:2");
    let each_done_at = super::test_support::at(&events, "each-finish-owned:1");
    let promoted_at = super::test_support::at(&events, "loop-collect:promoted");
    assert!(
        works_at < collect_at && collect_at < each_done_at && each_done_at < promoted_at,
        "实际相邻顺序: {events:?}"
    );

    // 角色／parent 链：Round 直接归 Loop、Each（branch 内）归 branch 包装、branch 归 Match。
    let creations = boundary_creation_snapshot();
    let role_of = |scope: &super::identity::ScopeId| {
        creations
            .iter()
            .find(|(child, _, _)| child == scope)
            .map(|(_, _, role)| *role)
    };
    for (scope, parent, role) in &creations {
        match role {
            ScopeRole::Round => assert!(
                role_of(parent) == Some(ScopeRole::Loop) || parent.seq() == 0,
                "Round 直接归 Loop（L05 中 Loop 是 Root）: {scope} -> {parent}"
            ),
            ScopeRole::Loop => assert!(
                matches!(role_of(parent), Some(ScopeRole::Branch)),
                "Loop 在 branch 包装内: {scope}"
            ),
            ScopeRole::Each => assert!(
                matches!(role_of(parent), Some(ScopeRole::Branch)),
                "Each 是 Match branch 的 Step: {scope}"
            ),
            _ => {}
        }
    }
    // item 直接归 Each，且 Round 内的 item 来自 branch 内的 Each。
    for (scope, parent, role) in &creations {
        if *role == ScopeRole::Item {
            assert_eq!(role_of(parent), Some(ScopeRole::Each), "Item→Each: {scope}");
        }
    }

    // 无额外 parent Ref slot：Loop 的本地引用只有其声明输出一个位置。
    let rounds = round_snapshots();
    assert!(!rounds.is_empty());
    for snapshot in &rounds {
        let controller_refs = snapshot
            .controller_refs
            .as_ref()
            .expect("round-collect 快照带 controller refs");
        assert_eq!(
            controller_refs.len(),
            1,
            "Loop 只声明一个输出位置: {snapshot:?}"
        );
    }
    // 成功后所有创建点关闭。
    let closed = closed_scope_snapshot();
    for (scope, _, _) in &creations {
        assert!(closed.contains(scope), "{scope} 已关闭: {closed:?}");
    }
    drop(final_state);
}

// ---------------------------------------------------------------- L08

#[test]
fn l08_iter_imported_owned_same_data_id_and_final_ref_single_assignment() {
    // imported 初态 → owned 新状态 → same-DataId Continue → owned Finish。
    strict_reset_observations();
    let state_in = state_with(0, 2);
    let final_state = drive(Runtime::execute::<_, _, Data<State>>(
        &iter_alternating_root(),
        (state_in,),
    ))
    .expect("iter root");
    assert_eq!(final_state.progress, 2, "推进到 Finish");
    let rounds = round_snapshots();
    assert_eq!(rounds.len(), 3, "三轮收口: {rounds:?}");
    let creations = boundary_creation_snapshot();
    let root_scope = closed_scope_snapshot()
        .into_iter()
        .find(|scope| scope.seq() == 0)
        .expect("root scope closed once");
    let first = &rounds[0];
    let loop_scope = loop_of_round(&creations, &round_source(first));
    assert_eq!(
        loop_scope, root_scope,
        "Loop 作为 Root 执行时控制状态由 RootScope 负责: {creations:?}"
    );
    // 第 1 轮：导入初态的完整身份、原 owner（Root）与存活。
    let imported_id = match first.state_target.clone().expect("首轮 state target") {
        TargetSnapshot::Data(id) => id,
        other => panic!("首轮目标是完整 Data: {other:?}"),
    };
    assert_eq!(
        first.state_target_owner,
        Some(root_scope.clone()),
        "imported 初态仍由 Root 负责: {first:?}"
    );
    assert!(
        first.state_target_alive,
        "imported 初态在首轮 Promote 前存活"
    );
    // 每一轮的 promoted 值都是新实例，imported 初态身份不参与替换。
    assert_ne!(
        imported_id,
        first.selected_data.clone().expect("首轮 selected")
    );
    // 第二轮 before：旧 current = 第一轮 Promote 的同一实例（owner = Loop、存活），
    // selected 又是同一实例（same-DataId 重新暴露，不新建）。
    let first_selected = first.selected_data.clone().expect("首轮 selected");
    let second = &rounds[1];
    assert_eq!(
        second.state_target,
        Some(TargetSnapshot::Data(first_selected.clone())),
        "第二轮 Promote 前旧 current = 第一轮 selected"
    );
    assert_eq!(
        second.state_target_owner,
        Some(loop_scope.clone()),
        "旧 current 由控制器负责"
    );
    assert!(second.state_target_alive, "旧 current 在替换前存活");
    assert!(
        second
            .controller_owned
            .as_ref()
            .expect("controller owned")
            .contains(&first_selected),
        "控制器实际负责旧 current: {second:?}"
    );
    assert_eq!(
        second.selected_target,
        Some(TargetSnapshot::Data(first_selected.clone())),
        "same-DataId 重新暴露同一实例"
    );
    // 第三轮 before：旧 current（同一实例）的 owner／存活，selected 是新 owned Finish 值。
    let third = &rounds[2];
    assert_eq!(
        third.state_target,
        Some(TargetSnapshot::Data(first_selected.clone())),
        "第三轮 Promote 前旧 current 仍是同一实例"
    );
    assert_eq!(
        third.state_target_owner,
        Some(loop_scope.clone()),
        "旧 current owner 是控制器"
    );
    assert!(third.state_target_alive, "旧 current 在替换前存活");
    assert!(
        third
            .controller_owned
            .as_ref()
            .expect("controller owned")
            .contains(&first_selected),
        "控制器实际负责旧 current: {third:?}"
    );
    let third_selected = third.selected_data.clone().expect("末轮 selected");
    assert_ne!(third_selected, first_selected, "Finish 产生新 owned 值");
    // Promote 提交现场（完整元数据）：三次 Promote 的责任转移与待回收旧值。
    let records = take_promote_states();
    let loop_records: Vec<&PromoteStateRecord> = records
        .iter()
        .filter(|record| record.controller == loop_scope)
        .collect();
    assert_eq!(loop_records.len(), 3, "三次 Promote: {records:?}");
    assert!(loop_records[0].transferred, "首轮新 owned 转移责任");
    assert_eq!(
        loop_records[0].pending,
        vec![imported_id.clone()],
        "被替换的 imported 初态由控制器登记待回收（Loop 即 Root）"
    );
    assert!(
        !loop_records[1].transferred,
        "same-DataId 重新暴露不重复转移"
    );
    assert_eq!(
        loop_records[1].pending,
        vec![imported_id.clone()],
        "同一实例替换不新增待回收"
    );
    assert!(loop_records[2].transferred, "Finish 新 owned 转移责任");
    assert_eq!(
        loop_records[2].pending,
        vec![imported_id.clone(), first_selected.clone()],
        "旧 Loop-owned 值进入待回收（累计，不重复）"
    );
    assert_eq!(
        loop_records[2].state_target,
        Some(TargetSnapshot::Data(third_selected.clone())),
        "控制状态指向末轮 selected"
    );
    // final Ref 单赋值：Loop 的最终输出只有在最终绑定处绑定一次。
    let events = take_events();
    assert_eq!(
        count(&events, "loop-finished"),
        1,
        "Loop 只完成一次: {events:?}"
    );
    // 旧值处置逐实例：imported 初态由 Root 关闭销毁；旧 Loop-owned 值在退出时精确回收；
    // Round3 的值是最终输出，由 Root 移交、Application drop 时才析构。
    assert_eq!(
        count(&events, "state-dropped:0:0"),
        1,
        "imported 初态由 Root 关闭时销毁一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:1"),
        1,
        "旧 Loop-owned 值精确回收一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:2"),
        0,
        "最终输出尚未析构: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped"),
        2,
        "无重复销毁与漏销毁: {events:?}"
    );
    // final Ref 的唯一绑定：Root 实际 take 的实例就是最后一轮 Promote 选中的实例。
    let taken = take_log_snapshot();
    assert_eq!(taken, vec![third_selected.clone()], "最终声明的实际绑定");
    drop(final_state);
    let events = take_events();
    assert_eq!(
        count(&events, "state-dropped:0:2"),
        1,
        "应用 drop 才析构最终输出: {events:?}"
    );

    // 对照：imported 初态已表达 Finish，经 identity body 一轮 same-DataId Finish。
    strict_reset_observations();
    let already_done = drive(Runtime::execute::<_, _, Data<State>>(
        &iter_identity_root(),
        (state_with(2, 2),),
    ))
    .expect("identity iter root");
    assert_eq!(
        already_done.progress, 2,
        "已完成的 imported 初态直接 Finish"
    );
    let rounds = round_snapshots();
    assert_eq!(rounds.len(), 1, "一轮 Finish: {rounds:?}");
    let events = take_events();
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "一次 Promote");
    assert_eq!(count(&events, "loop-finished"), 1);
    drop(already_done);
}

/// 三轮：Round1 新 owned Continue、Round2 same-DataId 重新暴露 Continue、Round3 新 owned Finish。
fn iter_alternating_root() -> Loop<Iter1<State>> {
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(routed_iter_body())
        .expect("iter body");
    builder.finish().expect("iter finish")
}

fn iter_identity_root() -> Loop<Iter1<State>> {
    let mut builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    builder
        .then_body::<_, OrchSig<State, Data<State>>>(identity_iter_body())
        .expect("iter body");
    builder.finish().expect("iter finish")
}

fn identity_iter_body() -> Flow<(State,), Data<State>> {
    let (flow, state) = FlowBuilder::<(State,)>::start().expect("body start");
    flow.finish::<Data<State>, _>(state).expect("body finish")
}

fn routed_iter_body() -> Flow<(State,), Data<State>> {
    let (mut flow, state) = FlowBuilder::<(State,)>::start().expect("body start");
    let route: DataRef<Route> = flow
        .then::<_, super::signature::NodeSig<(State,), Data<Route>>, _>(
            RoundRouteNode::new(),
            state.clone(),
        )
        .expect("route step");
    let mut matched: MatchBuilder<Route, State, Data<State>> =
        MatchBuilder::start().expect("match");
    matched
        .branch::<_, SyncFnSig<(State,), Data<State>>>(
            Route(1),
            new_continue as fn(&State) -> Result<State, BodyError>,
        )
        .expect("new continue branch");
    matched
        .branch::<_, OrchSig<State, Data<State>>>(Route(2), identity_iter_body())
        .expect("identity branch");
    matched
        .default::<_, SyncFnSig<(State,), Data<State>>>(
            new_finish as fn(&State) -> Result<State, BodyError>,
        )
        .expect("finish branch");
    let matched = matched.finish().expect("match finish");
    let produced: DataRef<State> = flow
        .then::<_, OrchSig<(Route, State), Data<State>>, _>(matched, (route, state))
        .expect("match step");
    flow.finish::<Data<State>, _>(produced)
        .expect("body finish")
}

/// 有状态路由 Node：第 1 轮新 owned Continue、第 2 轮 same-DataId、第 3 轮 Finish。
/// 计数只服务样本的有限序列，不构成 Runtime 次数策略。
pub(crate) struct RoundRouteNode(std::cell::Cell<u32>);

impl RoundRouteNode {
    fn new() -> Self {
        Self(std::cell::Cell::new(0))
    }
}

impl super::node::NodeCall1<State, Data<Route>> for RoundRouteNode {
    fn call<'a>(&'a self, state: &'a State) -> super::signature::NodeFut<'a, Route> {
        let round = self.0.get();
        self.0.set(round + 1);
        let _ = state;
        Box::pin(async move {
            Ok(Route(match round {
                0 => 1,
                1 => 2,
                _ => 0,
            }))
        })
    }
}

fn new_continue(state: &State) -> Result<State, BodyError> {
    Ok(State {
        item: state.item,
        initial: state.initial,
        progress: state.progress + 1,
        step: state.step,
        target: state.target,
        skip: state.skip,
    })
}

fn new_finish(state: &State) -> Result<State, BodyError> {
    Ok(State {
        item: state.item,
        initial: state.initial,
        progress: state.target,
        step: state.step,
        target: state.target,
        skip: state.skip,
    })
}

// ---------------------------------------------------------------- L09

#[test]
fn l09_retry_continue_then_finish_keeps_original_input() {
    strict_reset_observations();
    let final_state = drive(Runtime::execute::<_, _, Data<State>>(
        &retry_root(),
        (state_with(0, 2),),
    ))
    .expect("retry root");
    assert_eq!(final_state.progress, 2, "Continue→Finish 有限序列");
    let events = take_events();
    assert_eq!(
        count(&events, "retry-continue"),
        1,
        "恰好一次 Continue: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:1"),
        1,
        "Continue 的新值被丢弃: {events:?}"
    );
    // 每轮 original input 的 DataId 不变：Round 的导入目标指向同一完整 Data。
    let rounds = round_snapshots();
    assert_eq!(rounds.len(), 2, "两轮: {rounds:?}");
    let ids: Vec<super::identity::DataId> = rounds
        .iter()
        .map(|snapshot| {
            let refs = snapshot
                .source_refs
                .as_ref()
                .expect("round source refs present");
            refs.iter()
                .find_map(|(_, target)| match target {
                    TargetSnapshot::Data(id) => Some(id.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("expected an imported complete data: {snapshot:?}"))
        })
        .collect();
    assert_eq!(ids[0], ids[1], "Continue 后仍导入同一 original input");
    // Continue 的新值由 Round 负责并在丢弃时销毁；Finish 前控制状态仍未初始化。
    let discards = operation_snapshots(&rounds, RoundCollectOperation::Discard);
    let promotes = operation_snapshots(&rounds, RoundCollectOperation::Promote);
    assert_eq!(discards.len(), 1, "一次 Discard: {rounds:?}");
    assert_eq!(promotes.len(), 1, "一次 Finish Promote: {rounds:?}");
    assert_eq!(
        discards[0]
            .source_owned
            .as_ref()
            .expect("discard source owned")
            .len(),
        1,
        "Continue 新值归 Round 负责: {discards:?}"
    );
    assert_eq!(
        promotes[0].state_target, None,
        "Finish 前控制状态尚未初始化: {promotes:?}"
    );
    let creations = boundary_creation_snapshot();
    let loop_scope = loop_of_round(&creations, &round_source(&rounds[0]));
    assert_eq!(
        promotes[0].controller.as_ref(),
        Some(&loop_scope),
        "Finish 轮 Promote 由 Loop 收口: {promotes:?}"
    );
    assert_eq!(
        promotes[0].controller_owned.as_deref(),
        Some(&ids[0..1]),
        "Finish 前控制器只负责 original input: {promotes:?}"
    );
    // Finish 轮的 Promote 现场：只有一次 Promote，转移到控制状态且无待回收旧值。
    let records = take_promote_states();
    let loop_records: Vec<&PromoteStateRecord> = records
        .iter()
        .filter(|record| record.controller == loop_scope)
        .collect();
    assert_eq!(
        loop_records.len(),
        1,
        "Retry 只有 Finish 轮 Promote: {records:?}"
    );
    let record = loop_records[0];
    assert!(record.transferred, "Finish 新 owned 转移责任: {record:?}");
    assert!(record.pending.is_empty(), "Retry 无隐式旧值: {record:?}");
    let taken = take_log_snapshot();
    assert_eq!(
        taken,
        vec![
            record
                .state_target
                .clone()
                .and_then(|target| match target {
                    TargetSnapshot::Data(id) => Some(id),
                    _ => None,
                })
                .expect("finish state target is a complete Data")
        ],
        "最终绑定就是 Finish 轮 Promote 的值"
    );
    // 逐实例处置：imported 初态由 Root 关闭销毁、Continue 新值丢弃销毁一次、最终输出
    // 由 Application drop 才析构。
    assert_eq!(
        count(&events, "state-dropped:0:0"),
        1,
        "imported 初态销毁一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:2"),
        0,
        "最终输出尚未析构: {events:?}"
    );
    assert_eq!(count(&events, "state-dropped"), 2, "无隐式累积: {events:?}");
    drop(final_state);
    let events = take_events();
    assert_eq!(
        count(&events, "state-dropped:0:2"),
        1,
        "应用 drop 才析构最终输出: {events:?}"
    );
}

/// Retry imported-alias 对照：两轮 body 都是 identity，第 1 轮 Continue 丢弃、第 2 轮 Finish
/// Promote；imported 原值全程不转移责任、不被删除，最终经 Root 提取后才由应用析构。
#[test]
fn l09b_retry_imported_alias_continue_and_finish_keep_original_alive() {
    strict_reset_observations();
    let final_state = drive(Runtime::execute::<_, _, Data<State>>(
        &retry_alias_root(),
        (state_with(2, 2),),
    ))
    .expect("retry alias root");
    assert_eq!(
        final_state.progress, 2,
        "imported alias 原样作为最终结果（progress=target 表达 Finish）"
    );
    let events = take_events();
    assert_eq!(
        count(&events, "loop-collect:discarded"),
        1,
        "第一轮 Continue 丢弃 imported alias: {events:?}"
    );
    assert_eq!(
        count(&events, "loop-collect:promoted"),
        1,
        "第二轮 Finish: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:0"),
        1,
        "Continue 新值（progress 0）随丢弃销毁一次: {events:?}"
    );
    assert_eq!(
        count(&events, "state-dropped:0:2"),
        0,
        "imported 原值未被任何轮次销毁: {events:?}"
    );
    assert_eq!(count(&events, "state-dropped"), 1, "无额外销毁: {events:?}");
    let rounds = take_round_collect_pre_cleanup();
    let discards = operation_snapshots(&rounds, RoundCollectOperation::Discard);
    let promotes = operation_snapshots(&rounds, RoundCollectOperation::Promote);
    assert_eq!(discards.len(), 1, "一次 Discard: {rounds:?}");
    assert_eq!(promotes.len(), 1, "一次 Finish Promote: {rounds:?}");
    let imported_id = discards[0]
        .source_refs
        .as_ref()
        .expect("round source refs present")
        .iter()
        .find_map(|(_, target)| match target {
            TargetSnapshot::Data(id) => Some(id.clone()),
            _ => None,
        })
        .expect("round imports the original input");
    assert_ne!(
        round_source(promotes[0]),
        round_source(discards[0]),
        "两轮是不同 Round"
    );
    let promoted_ids: Vec<super::identity::DataId> = promotes[0]
        .source_refs
        .as_ref()
        .expect("finish round refs")
        .iter()
        .filter_map(|(_, target)| match target {
            TargetSnapshot::Data(id) => Some(id.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !promoted_ids.is_empty(),
        "Finish 轮 refs 非空: {promotes:?}"
    );
    assert!(
        promoted_ids.iter().all(|id| id == &imported_id),
        "Finish 轮所有绑定都是同一 original input: {promoted_ids:?}"
    );
    let records = take_promote_states();
    assert_eq!(records.len(), 1, "只有 Finish 轮 Promote: {records:?}");
    let record = &records[0];
    assert!(!record.transferred, "imported alias 不转移责任: {record:?}");
    assert_eq!(
        record.controller_owned,
        vec![imported_id.clone()],
        "控制器仍恰好负责 imported 原实例（不新增、不重复）: {record:?}"
    );
    assert!(record.pending.is_empty(), "无待回收旧值: {record:?}");
    assert_eq!(
        record.state_target,
        Some(TargetSnapshot::Data(imported_id.clone())),
        "控制状态就是 imported 实例"
    );
    let taken = take_log_snapshot();
    assert_eq!(
        taken,
        vec![imported_id.clone()],
        "Root 提取 imported 原实例"
    );
    drop(final_state);
    let events = take_events();
    assert_eq!(
        count(&events, "state-dropped:0:2"),
        1,
        "应用 drop 才析构 imported 实例: {events:?}"
    );
}

// ---------------------------------------------------------------- L04

#[test]
fn l04_item_target_flows_into_descendants_without_clone_or_move() {
    strict_reset_observations();
    let results = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &item_alias_each(ItemAliasMode::AliasRound),
        (vec![state_with(0, 0)],),
    ))
    .expect("item alias each");
    assert_eq!(results.len(), 1, "一个 item");
    assert_eq!(
        results[0].progress, 0,
        "cap 内 identity 再暴露同一 item 目标"
    );
    let events = take_events();
    assert_eq!(
        count(&events, "item-result-dropped"),
        0,
        "返回结果仍由 Application 持有"
    );
    // item 目标没有 Clone／move：集合元素只在 Loop 完成之后的退出清理里析构一次。
    assert_eq!(
        count(&events, "state-dropped:0:0"),
        1,
        "item 元素恰好析构一次: {events:?}"
    );
    let finished_at = super::test_support::at(&events, "loop-finished");
    let element_drop_at = super::test_support::at(&events, "state-dropped:0:0");
    assert!(
        finished_at < element_drop_at,
        "元素析构发生在 Loop 完成之后（链上未 move／clone）: {events:?}"
    );
    // item 绑定目标（完整 metadata）：来源集合、下标、cap 与声明类型。
    let targets = item_target_snapshot();
    assert_eq!(targets.len(), 1, "一个 item 绑定目标");
    let bound = &targets[0];
    let creations = boundary_creation_snapshot();
    let (collection, index, cap, collection_type, element_type) = match &bound.target {
        TargetSnapshot::CollectionItem {
            collection,
            index,
            lifetime_cap,
            collection_type,
            element_type,
        } => (
            collection.clone(),
            *index,
            lifetime_cap.clone(),
            *collection_type,
            *element_type,
        ),
        other => panic!("item 目标必须是 CollectionItem: {other:?}"),
    };
    assert_eq!(index, 0, "源下标");
    assert_eq!(
        collection_type,
        std::any::TypeId::of::<Vec<State>>(),
        "来源集合声明类型"
    );
    assert_eq!(
        element_type,
        std::any::TypeId::of::<State>(),
        "元素声明类型"
    );
    assert_eq!(
        creation_role(&creations, &cap),
        Some(ScopeRole::Item),
        "cap 是实际 ItemScope: {creations:?}"
    );
    assert_eq!(bound.item, cap, "cap 就是本 Item");
    let root_scope = closed_scope_snapshot()
        .into_iter()
        .find(|scope| scope.seq() == 0)
        .expect("root scope closed once");
    // Round 的导入／identity 再暴露都是同一 CollectionItem 目标（无 Clone／move／独立 item DataId）。
    let rounds = round_snapshots();
    let started = rounds
        .iter()
        .find(|snapshot| snapshot.state_target.is_some())
        .or_else(|| rounds.first())
        .expect("round snapshot");
    let expected_target = TargetSnapshot::CollectionItem {
        collection: collection.clone(),
        index,
        lifetime_cap: cap.clone(),
        collection_type,
        element_type,
    };
    assert_eq!(
        started.state_target,
        Some(expected_target.clone()),
        "state target 就是同一 item 目标"
    );
    assert_eq!(
        started.selected_target,
        Some(expected_target),
        "identity 再暴露仍是同一目标"
    );
    let collection_owner = started
        .selected_collection_owner
        .clone()
        .expect("item alias 的来源集合 owner");
    assert_eq!(
        collection_owner, root_scope,
        "来源集合 owner 仍是 Root（完整身份）"
    );
    let taken = take_log_snapshot();
    assert_eq!(
        taken.len(),
        1,
        "无独立 item DataId，只提取结果 Vec: {taken:?}"
    );
    drop(results);
}

#[test]
fn l04_cap_outside_export_is_rejected_before_commit() {
    strict_reset_observations();
    install_loop_fault(LoopFault::PromoteDestCapOutside);
    let error = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &item_alias_each(ItemAliasMode::AliasRound),
        (vec![state_with(0, 0)],),
    ))
    .expect_err("cap 外的 item 目标不能在提交前移交");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    assert!(error.scope_error().is_some(), "保留实际拒绝原因: {error:?}");
    assert!(
        take_log_snapshot().is_empty(),
        "Root 零 take: {:?}",
        take_log_snapshot()
    );
    let snapshots = super::v21_11_tests::take_snapshots_checked();
    assert!(
        !snapshots
            .iter()
            .any(|snapshot| snapshot.phase == super::test_support::RootSnapshotPhase::AfterCommit),
        "无成功提交"
    );
    // 对照：同一 chain 不注入时成功（cap 内）。
    let ok = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &item_alias_each(ItemAliasMode::AliasRound),
        (vec![state_with(0, 0)],),
    ))
    .expect("cap 内正例");
    assert_eq!(ok[0].progress, 0);
}

/// L04 DEFENCE（Export 目的端）：在 identity child 已存在的 Pending 现场，按**完整
/// ScopeId** 把它的 CollectionItem 目标 cap 收紧为 child 自身 → 目的（caller）在 cap
/// 之外，由真实 Export 目的端检查拒绝。
#[test]
fn l04_item_alias_export_destination_outside_cap_is_rejected() {
    strict_reset_observations();
    install_gate();
    let root = item_alias_each(ItemAliasMode::AliasRound);
    let future = Runtime::execute::<_, _, Data<Vec<ItemResult>>>(&root, (vec![state_with(0, 0)],));
    let boxed = advance_to_pending(future, 1);
    // 目标由完整 ScopeId 选择：identity Flow 是 pending 现场最后创建的 Flow，
    // 挂在 item body Flow 之下、Item 之下。
    let creations = boundary_creation_snapshot();
    let item_scope = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Item)
        .map(|(scope, _, _)| scope.clone())
        .expect("item created");
    let identity_flow = creations
        .iter()
        .rfind(|(_, _, role)| *role == ScopeRole::Flow)
        .map(|(scope, _, _)| scope.clone())
        .expect("identity flow created");
    let body_flow = creation_parent(&creations, &identity_flow).expect("identity flow parent");
    assert_eq!(
        creation_role(&creations, &body_flow),
        Some(ScopeRole::Flow),
        "identity 挂在 item body Flow 下: {creations:?}"
    );
    assert_eq!(
        creation_parent(&creations, &body_flow),
        Some(item_scope.clone()),
        "body Flow 直接归 Item: {creations:?}"
    );
    super::orchestrator::install_export_fault(super::orchestrator::ExportFault::TightenItemCap {
        child: identity_flow.clone(),
    });
    release_gate();
    let error = drive_pinned(boxed).expect_err("cap 外的 item 目标 Export 必须被拒绝");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    match error.scope_error() {
        Some(super::internal_error::ScopeError::ItemCapEscape {
            cap, destination, ..
        }) => {
            assert_eq!(*cap, identity_flow, "cap 收紧后的实际 child");
            assert_eq!(*destination, body_flow, "Export 目的（caller）完整身份");
        }
        other => panic!("expected ItemCapEscape, got {other:?}"),
    }
    // 命中记录：child／caller 完整身份，注入必须命中一次。
    assert_eq!(
        take_export_fault_hits(),
        vec![ExportFaultHit {
            kind: "tighten-item-cap",
            child: identity_flow.clone(),
            caller: body_flow.clone(),
            index: None,
        }],
        "Export 故障命中完整身份"
    );
    // 拒绝前后／cleanup 前的完整双侧快照相等（唯一允许的差异是观察阶段）。
    let exports = take_export_pre_cleanup();
    let after = exports
        .iter()
        .rev()
        .find(|snapshot| snapshot.phase == ExportSnapshotPhase::AfterReject)
        .expect("cap 目的端拒绝必有 AfterReject 观察")
        .clone();
    let before = exports
        .iter()
        .rev()
        .find(|snapshot| {
            snapshot.phase == ExportSnapshotPhase::Before && snapshot.child == after.child
        })
        .expect("同一 child 的 Before 观察")
        .clone();
    let mut normalized = before.clone();
    normalized.phase = after.phase;
    assert_eq!(
        normalized, after,
        "拒绝前后完整元数据相等（含冲突目标／owner／存活）"
    );
    assert!(take_log_snapshot().is_empty(), "Root 零 take");
    // 对照：同一链不注入时成功（cap 内）。
    strict_reset_observations();
    let ok = drive(Runtime::execute::<_, _, Data<Vec<ItemResult>>>(
        &item_alias_each(ItemAliasMode::AliasRound),
        (vec![state_with(0, 0)],),
    ))
    .expect("cap 内正例");
    assert_eq!(ok[0].progress, 0);
}
