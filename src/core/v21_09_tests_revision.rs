//! V21-09 验收样本（三）：首次复审 R11～R15 的补齐证据。
//!
//! 对照[任务书 §14](../docs/tasks/V21_09_Loop_Round_And_State_Promotion.md)的关闭条件：
//! R11（Loop Export 拒绝）、R12（item alias 完整链路）、R13（多轮 owned 取消）、
//! R14（正常推进／ownership 矩阵）、R15（权限／原子性／保存诊断）。

use std::cell::{Cell, RefCell};

use super::builder::{CallSite, TypedCallBuilder};
use super::context::{BodyError, ExecutionContext, InvocationKind, TerminationKind};
use super::each::{Each, EachBuilder, EachOnly};
use super::flow::{Flow, FlowBuilder};
use super::identity::{DataId, ScopeId};
use super::internal_error::ScopeError;
use super::loop_orchestrator::{
    Iter1, Loop, LoopBuilder, LoopFault, Retry1, install_foreign_state_probe, install_loop_fault,
};
use super::orchestrator::{OrchCall, ScopeRole};
use super::ref_id::{RefId, RefIdAllocator, RefIdSource};
use super::scope::{ScopeState, TargetSnapshot};
use super::signature::{AsyncFnSig, Data, OrchSig, SyncFnSig};
use super::test_support::{
    FinalBindSnapshotPhase, RootView, advance_to_pending, at, boundary_creation_reset,
    boundary_creation_snapshot, count, definition_in_root, drive, gate_wait, install_gate, saw,
    saw_prefix, take_final_bind_pre_cleanup, take_shared_events,
};
use super::v21_09_tests::{Item, State};

// ---------------------------------------------------------------- 业务与 Node

/// Retry Flow body 的临时值（Drop 见证随 Round 关闭清理）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Temp(pub u32);

impl Drop for Temp {
    fn drop(&mut self) {
        super::test_support::record("rev-temp-dropped");
    }
}

/// Retry 的完整 Flow body 输出：由 Node 新产生（owned）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Decision {
    pub(crate) round: u32,
    pub(crate) finish: bool,
}

impl Drop for Decision {
    fn drop(&mut self) {
        super::test_support::record("decision-dropped");
    }
}

impl super::loop_orchestrator::LoopControl for Decision {
    fn loop_decision(&self) -> super::loop_orchestrator::LoopDecision {
        if self.finish {
            super::loop_orchestrator::LoopDecision::Finish
        } else {
            super::loop_orchestrator::LoopDecision::Continue
        }
    }
}

/// Retry 导入 alias 的业务类型：`done` 由业务字段表达完成。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Job {
    pub(crate) id: u32,
    pub(crate) done: bool,
}

impl super::loop_orchestrator::LoopControl for Job {
    fn loop_decision(&self) -> super::loop_orchestrator::LoopDecision {
        if self.done {
            super::loop_orchestrator::LoopDecision::Finish
        } else {
            super::loop_orchestrator::LoopDecision::Continue
        }
    }
}

// 每轮执行的角色计数（只用于样本断言）。
thread_local! {
    static ROUNDS: Cell<u32> = const { Cell::new(0) };
}

/// 复位轮次计数（供其它样本在真实 Retry body 前复位）。
pub(crate) fn rounds_reset() {
    ROUNDS.with(|slot| slot.set(0));
}

fn next_round() -> u32 {
    ROUNDS.with(|slot| {
        let value = slot.get() + 1;
        slot.set(value);
        value
    })
}

/// 完整 Flow body 的首步：`Item -> Temp`（真实临时 owned 值）。
async fn flow_body_seed(item: &Item) -> Result<Temp, BodyError> {
    super::test_support::record(&format!("flow-body-seed:{}", item.id));
    Ok(Temp(item.id))
}

/// 完整 Flow body 的次步：`Item + Temp -> Decision`。
fn flow_body_decide(item: &Item, temp: &Temp) -> Result<Decision, BodyError> {
    let round = next_round();
    super::test_support::record(&format!("flow-body-round:{round}"));
    super::test_support::record(&format!("flow-body-input-addr:{:p}", item as *const Item));
    let _ = temp;
    Ok(Decision {
        round,
        finish: round >= 2,
    })
}

/// Iter 多轮 body：第 `arm_at` 轮之后在 gate 上真实 Pending。
async fn gated_after(arm_at: u32, state: &State) -> Result<State, BodyError> {
    let round = next_round();
    super::test_support::record(&format!("multi-round:{round}"));
    if round >= arm_at {
        super::test_support::record(&format!("multi-round-pending:{round}"));
        install_gate();
        gate_wait().await;
    }
    let value = state.value + 1;
    Ok(State {
        value,
        finish: value >= 8,
    })
}

async fn gated_after_1(state: &State) -> Result<State, BodyError> {
    gated_after(1, state).await
}

async fn gated_after_2(state: &State) -> Result<State, BodyError> {
    gated_after(2, state).await
}

fn read_state(state: &State) -> Result<u32, BodyError> {
    super::test_support::record("read-state-step");
    Ok(state.value)
}

fn read_decision(decision: &Decision) -> Result<u32, BodyError> {
    Ok(decision.round)
}

fn read_job(job: &Job) -> Result<u32, BodyError> {
    Ok(job.id)
}

// ---------------------------------------------------------------- 夹具

fn iter_fixture_for<F>(body: F) -> (Flow<(State,), Data<u32>>, RefId)
where
    F: super::builder::BuildSite<
            super::signature::SyncFnSig<(State,), Data<State>>,
            super::data_ref::DataRef<State>,
            BuildOutput = super::data_ref::DataRef<State>,
        > + super::builder::IntoCallSite<
            super::signature::SyncFnSig<(State,), Data<State>>,
            super::data_ref::DataRef<State>,
            BuildOutput = super::data_ref::DataRef<State>,
        >,
{
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, super::signature::SyncFnSig<(State,), Data<State>>>(body)
        .expect("iter body");
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

fn drive_state(
    parent: &Flow<(State,), Data<u32>>,
    position: &RefId,
    value: u32,
) -> Result<(), BodyError> {
    let position = position.clone();
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        move |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                State {
                    value,
                    finish: false,
                },
            )
            .expect("state input");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
}

/// Retry：完整 Flow body（2 步，含临时值）→ Loop CallSite 输出位置。
pub(crate) fn retry_flow_body_fixture() -> (Flow<(Item,), Data<u32>>, RefId, RefId) {
    let (mut parent, input) = FlowBuilder::<(Item,)>::start().expect("parent");
    let position = input.position().clone();
    // 包装的单 Step 是一个完整 Flow：先由异步 Node 产生临时值，再由同步 Node 产生 Decision。
    let (mut body, body_handles) = FlowBuilder::<(Item,)>::start().expect("body flow");
    let (item_in,) = (body_handles,);
    let temp: super::data_ref::DataRef<Temp> = body
        .then::<_, AsyncFnSig<(Item,), Data<Temp>>, _>(flow_body_seed, item_in.clone())
        .expect("seed step");
    let decision: super::data_ref::DataRef<Decision> = body
        .then::<_, SyncFnSig<(Item, Temp), Data<Decision>>, _>(
            flow_body_decide as fn(&Item, &Temp) -> Result<Decision, BodyError>,
            (item_in, temp),
        )
        .expect("decide step");
    let body = body
        .finish::<Data<Decision>, _>(decision)
        .expect("body finish");

    let mut retry: LoopBuilder<Retry1<Item, Decision>> = LoopBuilder::start().expect("retry");
    retry
        .then_body::<_, OrchSig<Item, Data<Decision>>>(body)
        .expect("retry flow body");
    let orchestrator = retry.finish().expect("retry finish");
    let produced: super::data_ref::DataRef<Decision> = parent
        .then::<_, OrchSig<Item, Data<Decision>>, _>(orchestrator, input)
        .expect("then retry");
    let loop_caller = produced.position().clone();
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(Decision,), Data<u32>>, _>(
            read_decision as fn(&Decision) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");
    (parent, position, loop_caller)
}

/// Retry：body 重新暴露导入 alias（单轮 Finish）。
fn retry_imported_alias_fixture() -> (Flow<(Job,), Data<u32>>, RefId) {
    let (mut parent, input) = FlowBuilder::<(Job,)>::start().expect("parent");
    let position = input.position().clone();
    let mut retry: LoopBuilder<Retry1<Job, Job>> = LoopBuilder::start().expect("retry");
    let (reenexpose, handles) = FlowBuilder::<(Job,)>::start().expect("reexpose flow");
    let (job,) = (handles,);
    let reenexpose = reenexpose
        .finish::<Data<Job>, _>(job)
        .expect("reexpose finish");
    retry
        .then_body::<_, OrchSig<Job, Data<Job>>>(reenexpose)
        .expect("alias body");
    let orchestrator = retry.finish().expect("retry finish");
    let produced: super::data_ref::DataRef<Job> = parent
        .then::<_, OrchSig<Job, Data<Job>>, _>(orchestrator, input)
        .expect("then retry");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(Job,), Data<u32>>, _>(
            read_job as fn(&Job) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    (
        parent.finish::<Data<u32>, _>(read).expect("parent finish"),
        position,
    )
}

/// Each（item 作为 Loop 初始状态）+ 包装 body 的单 Step 是 inner Flow：
/// 先由 Loop（identity body 重新暴露 item）产出 alias，再由显式 Node 产生 owned 结果。
fn item_alias_fixture() -> (Flow<(Vec<State>,), Data<usize>>, RefId) {
    let (mut flow, collection) = FlowBuilder::<(Vec<State>,)>::start().expect("flow");
    let collection_position = collection.position().clone();
    let mut each: EachBuilder<EachOnly<State>, State> = EachBuilder::start().expect("each");
    // body 包装的单 Step：inner Flow（2 步）。
    let (mut inner, inner_handles) = FlowBuilder::<(State,)>::start().expect("inner flow");
    let item_in = inner_handles;
    let mut loop_builder: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("loop");
    // Loop 的 body：重新暴露导入 item（selected 是 CollectionItem alias）。
    let (reenexpose, reexpose_handles) = FlowBuilder::<(State,)>::start().expect("loop body flow");
    let loop_body = reenexpose
        .finish::<Data<State>, _>(reexpose_handles)
        .expect("loop body finish");
    loop_builder
        .then_body::<_, OrchSig<State, Data<State>>>(loop_body)
        .expect("loop body");
    let orchestrator = loop_builder.finish().expect("loop finish");
    let alias: super::data_ref::DataRef<State> = inner
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator, item_in)
        .expect("inner then loop");
    // 显式业务 Node：把 alias 转成新的 owned State（供 Each collector 消费）。
    let owned: super::data_ref::DataRef<State> = inner
        .then::<_, SyncFnSig<(State,), Data<State>>, _>(
            derive_state as fn(&State) -> Result<State, BodyError>,
            alias,
        )
        .expect("derive step");
    let inner = inner.finish::<Data<State>, _>(owned).expect("inner finish");
    each.then_body::<_, OrchSig<State, Data<State>>>(inner)
        .expect("each body");
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

/// 显式业务 Node：从 alias 借用产生新的 owned State（借来的 item 不取得 ownership）。
fn derive_state(state: &State) -> Result<State, BodyError> {
    super::test_support::record("derive-owned");
    Ok(State {
        value: state.value + 100,
        finish: true,
    })
}

/// 深层的真实错误：descendant Flow 内 Node 返回结构化错误。
fn deep_error(state: &State) -> Result<State, BodyError> {
    super::test_support::record(&format!("deep-error:{}", state.value));
    Err(BodyError::from(ScopeError::Invariant {
        violated: "deep nested failure probe",
    }))
}

// ---------------------------------------------------------------- R11：Loop Export 拒绝

#[test]
fn r11_loop_export_conflict_is_rejected_at_the_loop_callsite() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    // retry_flow_body_fixture 返回的第三个值是 **Loop CallSite 的 caller 输出位置**。
    let (parent, input_position, loop_caller) = retry_flow_body_fixture();
    let error = drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &input_position, Item { id: 1 })
                .expect("input");
            // 预占 Loop 的 caller 输出位置：Loop 自己的 Export 必须在提交前拒绝。
            ctx.register_owned(&ctx.root_scope(), &loop_caller, 99u32)
                .expect("occupy loop caller");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect_err("loop export conflict must be rejected");

    let events = take_shared_events();
    // Loop 自身推进与最终绑定已完成，但 Export 在 Loop CallSite 上被拒。
    assert!(saw(&events, "loop-finished"), "{events:?}");
    assert!(
        saw_prefix(&events, "frame-exit:boundary"),
        "Loop boundary 退出: {events:?}"
    );
    // 父 Flow 的后续 Step（read／marker）都不执行。
    assert!(!saw_prefix(&events, "flow-body-round:3"), "{events:?}");
    // Loop 的最终输出没有被交给 caller：由 Loop 收口清理销毁一次。
    assert_eq!(
        count(&events, "decision-dropped"),
        2,
        "两轮的 Decision 各销毁一次: {events:?}"
    );
    // 最终绑定的输出没有被交给 caller：最后一次销毁发生在 loop-finished 之后、由 Loop 清理完成。
    let last_drop = events
        .iter()
        .rposition(|event| event == "decision-dropped")
        .expect("drop recorded");
    assert!(
        last_drop > at(&events, "loop-finished"),
        "Export 拒绝发生在最终绑定之后、Loop 清理之前: {events:?}"
    );
    // 冲突诊断：Scope 是 Root、RefId 恰为 Loop 的 caller 输出位置，底层变体为 RefAlreadyBound。
    match error.scope_error() {
        Some(ScopeError::RefAlreadyBound { scope, position }) => {
            // 冲突发生在父（Root）Scope：Root 是本 Execution 的第一个 Scope。
            assert_eq!(scope.seq(), 0, "冲突 Scope 是父 Root Scope");
            assert_eq!(*position, loop_caller, "冲突位置是 Loop 的 caller 输出");
        }
        other => panic!("expected RefAlreadyBound, got {other:?}"),
    }
}

#[test]
fn r11_final_bind_local_conflict_keeps_state_with_full_baselines() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::FinalPositionOccupied);
    let (parent, position) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: true,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    let error = drive_state(&parent, &position, 3).expect_err("local bind conflict");
    assert!(
        matches!(
            error.scope_error(),
            Some(ScopeError::RefAlreadyBound { .. })
        ),
        "底层变体: {error:?}"
    );

    let snapshots = take_final_bind_pre_cleanup();
    let before = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == FinalBindSnapshotPhase::Before)
        .expect("before snapshot");
    let after = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == FinalBindSnapshotPhase::AfterReject)
        .expect("after-reject snapshot");
    assert!(before.observation_error.is_none(), "{before:?}");
    assert!(after.observation_error.is_none(), "{after:?}");
    // 逐项相等：refs／owned／状态／state target／pending／位置身份／owner／存活／序号。
    assert_eq!(before.controller, after.controller);
    assert_eq!(before.controller_state, after.controller_state);
    assert_eq!(before.controller_refs, after.controller_refs, "refs 未变");
    assert_eq!(
        before.controller_owned, after.controller_owned,
        "owned 未变"
    );
    assert_eq!(before.state_target, after.state_target, "state target 未变");
    assert_eq!(before.state_pending, after.state_pending, "pending 未变");
    assert_eq!(before.position_data, after.position_data, "位置身份未变");
    assert_eq!(before.position_owner, after.position_owner);
    assert_eq!(before.position_alive, after.position_alive);
    assert_eq!(
        before.next_data_id, after.next_data_id,
        "不消耗 DataId 序号"
    );
    // 位置上的旧 target 未被覆盖：仍是预占值。
    assert!(before.position_alive, "预占值仍存活");
    let events = take_shared_events();
    assert!(
        !saw(&events, "loop-finished"),
        "拒绝不产生最终输出: {events:?}"
    );
}

#[test]
fn r11_final_bind_uninitialized_and_bad_metadata_are_rejected() {
    // 未初始化：StateUninitialized。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::FinalStateUninitialized);
    let (parent, position) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: true,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    let error = drive_state(&parent, &position, 3).expect_err("uninitialized state rejected");
    match error.scope_error() {
        Some(ScopeError::StateUninitialized { state }) => {
            assert_eq!(state.owner().seq(), 1, "控制器是实际 LoopScope");
        }
        other => panic!("expected StateUninitialized, got {other:?}"),
    }
    assert_final_bind_rejection_unchanged();

    // 坏 metadata（声明类型被改）：拒绝且不产生部分输出。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::FinalStateBadMetadata);
    let (parent, position) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: true,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    let error = drive_state(&parent, &position, 3).expect_err("bad metadata rejected");
    // 坏 metadata 的实际拒绝点与字段（与未初始化分支区分）。
    match error.scope_error() {
        Some(ScopeError::TypeMismatch {
            position: at,
            expected,
            actual,
        }) => {
            assert!(
                at.seq() > 0
                    && *expected == std::any::type_name::<u8>()
                    && *actual == std::any::type_name::<State>(),
                "{error:?}"
            );
        }
        other => panic!("expected TypeMismatch, got {other:?}"),
    }
    assert_final_bind_rejection_unchanged();
    assert!(
        !saw(&take_shared_events(), "loop-finished"),
        "拒绝不产生最终输出"
    );
}

// ---------------------------------------------------------------- R12：item alias 完整链路

#[test]
fn r12_item_alias_full_chain_positive() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    let (flow, collection_position) = item_alias_fixture();
    let collection_id: RefCell<Option<DataId>> = RefCell::new(None);
    drive(definition_in_root(
        flow.definition(),
        Vec::new(),
        |ctx, _root| {
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
            *collection_id.borrow_mut() = Some(id);
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("item alias chain");

    let events = take_shared_events();
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "{events:?}");
    assert!(
        saw(&events, "derive-owned"),
        "显式 Node 产生 owned 结果: {events:?}"
    );
    // 收口观察：初始 current-state 与 selected 都是同一 CollectionItem（cap = ItemScope）。
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let promote = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .expect("promote snapshot");
    let item = match promote.state_target.as_ref() {
        Some(TargetSnapshot::CollectionItem {
            collection,
            index,
            lifetime_cap,
            ..
        }) => (collection.clone(), *index, lifetime_cap.clone()),
        other => panic!("expected item state target, got {other:?}"),
    };
    assert_eq!(item.1, 0, "item 下标");
    assert_eq!(
        Some(item.0.clone()),
        collection_id.borrow().clone(),
        "集合身份不变"
    );
    assert!(
        item.2.seq() > 0 && item.2 != promote.source.clone().expect("round"),
        "cap 是 ItemScope（不是 Round）"
    );
    // selected 与 state target 是同一 item（identity body 重新暴露导入目标）。
    assert!(
        promote.selected_data.is_none(),
        "被选目标是 item，不扁平化为集合 DataId: {promote:?}"
    );
    // 无 item-owned：Round 的 owned 只含 body 新产生的 owned 结果。
    assert_eq!(
        promote.source_owned.as_ref().map(Vec::len),
        Some(0),
        "alias 不产生 item-owned: {promote:?}"
    );
}

#[test]
fn r12_item_state_rejections_are_split_and_structured() {
    // (1) 错元素类型。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::CorruptStateInputType);
    let (flow, collection_position) = item_alias_fixture();
    let error = drive_item(&flow, &collection_position).expect_err("wrong element type");
    match error.scope_error() {
        Some(ScopeError::TypeMismatch {
            expected, actual, ..
        }) => {
            assert!(expected.contains("Vec<u8>"), "声明类型精确: {expected}");
            assert!(actual.contains("Vec<"), "实际类型精确: {actual}");
            assert!(actual.contains("State"), "实际元素类型: {actual}");
        }
        other => panic!("expected TypeMismatch, got {other:?}"),
    }
    assert_eq!(round_creations(), 0, "在建立 Round 之前拒绝");

    // (2) 错下标（元素类型保持正确）。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::CorruptStateInputIndex);
    let (flow, collection_position) = item_alias_fixture();
    let error = drive_item(&flow, &collection_position).expect_err("wrong index");
    assert!(
        !matches!(error.scope_error(), Some(ScopeError::TypeMismatch { .. })),
        "下标反例不得被类型判据遮住: {error:?}"
    );
    match error.scope_error() {
        Some(ScopeError::ItemIndexOutOfRange { index, .. }) => {
            assert_eq!(*index, usize::MAX, "下标字段精确: {error:?}");
        }
        other => panic!("expected ItemIndexOutOfRange, got {other:?}"),
    }
    assert_eq!(round_creations(), 0);

    // (3) Closed cap。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::StateCapClosed);
    let (flow, collection_position) = item_alias_fixture();
    let error = drive_item(&flow, &collection_position).expect_err("closed cap");
    match error.scope_error() {
        Some(ScopeError::ItemOutsideCap { cap, requester, .. }) => {
            assert!(
                cap.seq() > 0 && requester.seq() > 0,
                "身份字段精确: {error:?}"
            );
        }
        other => panic!("expected ItemOutsideCap, got {other:?}"),
    }
    assert!(
        !saw_prefix(&take_shared_events(), "loop-collect:promoted"),
        "提交前拒绝"
    );

    // (4) cap 外（非祖先 Scope）。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::StateCapOutside);
    let (flow, collection_position) = item_alias_fixture();
    let error = drive_item(&flow, &collection_position).expect_err("cap outside");
    match error.scope_error() {
        Some(ScopeError::ItemOutsideCap { cap, requester, .. }) => {
            assert_ne!(*cap, *requester, "cap 不是请求方自身: {error:?}");
            assert!(
                requester.seq() > 0 && cap.seq() > 0,
                "身份字段精确: {error:?}"
            );
            // 请求方是真实 LoopScope（本 Execution 中第一个由 Loop 建立的 Scope）。
            assert_eq!(
                requester.seq(),
                boundary_creation_snapshot()
                    .into_iter()
                    .find(|(_, _, role)| *role == ScopeRole::Loop)
                    .expect("loop scope")
                    .0
                    .seq(),
                "请求方是实际 LoopScope: {error:?}"
            );
        }
        other => panic!("expected ItemOutsideCap, got {other:?}"),
    }
    // 最终绑定的故障后基线与拒绝后状态逐项相等。
    let snapshots = take_final_bind_pre_cleanup();
    let before = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == FinalBindSnapshotPhase::Before)
        .expect("before");
    let after = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == FinalBindSnapshotPhase::AfterReject)
        .expect("after");
    assert!(before.observation_error.is_none() && after.observation_error.is_none());
    assert_eq!(before.controller_refs, after.controller_refs, "refs 未变");
    assert_eq!(
        before.controller_owned, after.controller_owned,
        "owned 未变"
    );
    assert_eq!(before.state_target, after.state_target, "state target 未变");
    assert_eq!(before.state_pending, after.state_pending, "pending 未变");
    assert_eq!(before.next_data_id, after.next_data_id, "拒绝不取号");
    assert!(
        !saw_prefix(&take_shared_events(), "loop-finished"),
        "不产生最终输出"
    );

    // (5) foreign Execution state（许可检查）。
    super::test_support::reset_observations();
    boundary_creation_reset();
    let foreign = foreign_state();
    install_foreign_state_probe(foreign);
    install_loop_fault(LoopFault::ForeignState);
    let (flow, collection_position) = item_alias_fixture();
    let events_before = take_shared_events();
    let _ = events_before;
    let _ = drive_item(&flow, &collection_position);
    let probes = super::test_support::take_generic_promote_probe();
    assert_eq!(probes.len(), 1, "{probes:?} двух");
    match probes[0].outcome.as_ref() {
        Some(ScopeError::StateForeignExecution { .. }) => {}
        other => panic!("expected StateForeignExecution, got {other:?}"),
    }
    let events = take_shared_events();
    assert!(
        events
            .iter()
            .any(|event| event.starts_with("permit-probe:")
                && event.contains("StateForeignExecution")),
        "probe recorded: {events:?}"
    );
}

fn drive_item(flow: &Flow<(Vec<State>,), Data<usize>>, position: &RefId) -> Result<(), BodyError> {
    let position = position.clone();
    drive(definition_in_root(
        flow.definition(),
        Vec::new(),
        move |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &position,
                vec![State {
                    value: 1,
                    finish: true,
                }],
            )
            .expect("collection");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
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

fn round_creations() -> usize {
    boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, role)| *role == ScopeRole::Round)
        .count()
}

/// 另一个 Execution 里登记的一份控制状态（foreign 反例用）。
fn foreign_state() -> super::scope::ControlStateId {
    // 另一个 Execution 的 RootScope 上登记一份控制状态；其 Execution 随后析构，
    // 保留的句柄只剩 owner／seq，用于验证 foreign Execution 判据。
    let mut execution = super::runtime::RootExecution::start();
    let root = execution.context().root_scope();
    let ctx = execution.context_mut();
    ctx.register_uninitialized_state::<State>(&root)
        .expect("foreign state")
}

// ---------------------------------------------------------------- R13：多轮 owned 取消

#[test]
fn r13_multi_round_cancel_with_loop_owned_current() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, super::signature::AsyncFnSig<(State,), Data<State>>>(gated_after_2)
        .expect("body");
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

    // 真实 Root 链：把 erased Loop Future 推进到第 2 轮 Pending（第 1 轮已 Promote）。
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
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let site = parent.definition().steps()[0].site();
    let boxed = match site {
        CallSite::Orchestrator(site) => advance_to_pending(site.invoke(&mut guard, &root), 1),
        CallSite::Node(_) => panic!("loop step is an orchestrator site"),
    };
    let creations = boundary_creation_snapshot();
    // 第 1 轮已 Promote：current 是 Loop-owned；第 2 轮真实 Pending。
    let events = take_shared_events();
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "{events:?}");
    assert!(saw(&events, "multi-round-pending:2"), "{events:?}");
    let roles: Vec<ScopeRole> = creations.iter().map(|(_, _, role)| *role).collect();
    assert_eq!(
        roles,
        vec![ScopeRole::Loop, ScopeRole::Round, ScopeRole::Round],
        "{creations:?}"
    );
    let loop_seq = creations[0].0.seq();
    let first_round_seq = creations[1].0.seq();
    let second_round_seq = creations[2].0.seq();
    drop(boxed);
    let events = take_shared_events();
    // 最深层先退出，逐层 cleanup-start→end→frame-exit，内层先于外层。
    let leaf_exit = events
        .iter()
        .position(|event| event.starts_with("frame-exit:leaf"))
        .expect("leaf exit");
    let round_exit = events
        .iter()
        .position(|event| {
            event.starts_with("frame-exit:boundary:")
                && event.ends_with(&second_round_seq.to_string())
        })
        .expect("round exit");
    let loop_exit = events
        .iter()
        .position(|event| {
            event.starts_with("frame-exit:boundary:") && event.ends_with(&loop_seq.to_string())
        })
        .expect("loop exit");
    assert!(
        leaf_exit < round_exit && round_exit < loop_exit,
        "{events:?}"
    );
    for seq in [second_round_seq, loop_seq] {
        assert!(
            saw_prefix(&events, &format!("cleanup-start:{seq}")),
            "cleanup-start 与实际 Scope 配对: {events:?}"
        );
        assert!(
            saw_prefix(&events, &format!("cleanup-end:{seq}")),
            "cleanup-end 与实际 Scope 配对: {events:?}"
        );
    }
    // 第 1 轮（已 Closed）不再重复清理。
    assert!(
        !saw_prefix(&events, &format!("guard-cleanup:{first_round_seq}")),
        "已关闭来源不重复销毁: {events:?}"
    );
    // 真实 Closed 状态：两轮 Round 都已关闭；第 2 轮在 Pending 时仍存活才可能有自己的清理。
    assert_eq!(
        guard.state(&creations[1].0).expect("state"),
        ScopeState::Closed
    );
    assert_eq!(
        guard.state(&creations[2].0).expect("state"),
        ScopeState::Closed
    );
    assert!(
        saw_prefix(&events, &format!("guard-cleanup:{second_round_seq}")),
        "第 2 轮在取消时仍存活并参与清理: {events:?}"
    );
    // 失效本地 resolve：第 1／2 轮关闭后从它们解析任何位置都必须被拒绝。
    let probe_position = RefIdAllocator::new(RefIdSource::new())
        .allocate()
        .expect("probe position");
    assert!(
        guard
            .resolve::<State>(&creations[1].0, &probe_position)
            .is_err(),
        "已关闭 Round 的本地 resolve 必须被拒绝"
    );
    assert!(
        guard
            .resolve::<State>(&creations[2].0, &probe_position)
            .is_err(),
        "取消关闭的 Round 本地 resolve 必须被拒绝"
    );
    // 取消定位：最深取消 Scope 是第 2 轮。
    let saved = super::test_support::take_termination_saved();
    if let Some(termination) = saved.first() {
        assert_eq!(
            termination.kind,
            TerminationKind::Cancelled,
            "{termination:?}"
        );
        assert_eq!(
            termination.scope.as_ref().map(ScopeId::seq),
            Some(second_round_seq),
            "最深取消定位: {termination:?}"
        );
    }
    // Loop-owned current（S1）由 Loop 清理销毁一次；Root 的 S0 保留到 Root 关闭。
    let drops = state_drops(&events);
    assert_eq!(
        drops,
        vec![1],
        "Loop-owned current 取消时清理一次: {events:?}"
    );
    drop(guard);
    drop(execution);
}

fn state_drops(events: &[String]) -> Vec<u32> {
    let mut drops: Vec<u32> = events
        .iter()
        .filter_map(|event| event.strip_prefix("state-dropped:"))
        .filter_map(|value| value.parse::<u32>().ok())
        .collect();
    drops.sort_unstable();
    drops
}

#[test]
fn r13_owning_root_future_cancel_orders_layers_and_isolates_executions() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    // 第 2 轮 Pending 的 owning 形态。
    let (mut flow, input) = FlowBuilder::<(State,)>::start().expect("flow");
    let input_position = input.position().clone();
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, super::signature::AsyncFnSig<(State,), Data<State>>>(gated_after_2)
        .expect("body");
    let orchestrator = iter.finish().expect("finish");
    let produced: super::data_ref::DataRef<State> = flow
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator, input)
        .expect("then iter");
    let read: super::data_ref::DataRef<u32> = flow
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let flow = flow.finish::<Data<u32>, _>(read).expect("flow finish");

    // Ready 对照：同一 fixture 用不挂起的 body 正常完成（见 r14 的 Ready 样本）。
    // Pending 后丢弃拥有 Context 的 Root Future 本体。
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    let boxed = advance_to_pending(
        definition_in_root::<
            fn(&mut ExecutionContext, &ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            flow.definition(),
            vec![root_input_state(&input_position, 0)],
            |_, _| {},
            None,
        ),
        1,
    );
    let creations = boundary_creation_snapshot();
    let loop_seq = creations[0].0.seq();
    let second_round_seq = creations[2].0.seq();
    drop(boxed);
    let events = take_shared_events();
    for desc in ["frame-exit:leaf", "frame-exit:boundary", "frame-exit:root"] {
        assert!(saw_prefix(&events, desc), "{desc} 缺失必须失败: {events:?}");
    }
    let round_exit = events
        .iter()
        .position(|event| event.ends_with(&format!("boundary:{second_round_seq}")))
        .expect("round exit");
    let loop_exit = events
        .iter()
        .position(|event| event.ends_with(&format!("boundary:{loop_seq}")))
        .expect("loop exit");
    let root_exit = events
        .iter()
        .position(|event| event.starts_with("frame-exit:root"))
        .expect("root exit");
    assert!(
        round_exit < loop_exit && loop_exit < root_exit,
        "{events:?}"
    );
    let container_drop = events
        .iter()
        .position(|event| event == "container-drop")
        .expect("container drop 缺失必须失败");
    assert!(
        root_exit < container_drop,
        "Root 退出先于 Container 析构: {events:?}"
    );

    // 另一 Execution 不受影响：随后独立运行一次完整推进。
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    let (parent2, position2) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: true,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    drive_state(&parent2, &position2, 3).expect("independent execution");
    let events = take_shared_events();
    assert!(
        saw(&events, "loop-finished"),
        "独立 Execution 正常完成: {events:?}"
    );
}

// ---------------------------------------------------------------- R14：正常推进矩阵

#[test]
fn r14_iter_main_pending_then_ready_with_domain_and_lifecycle_checks() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, super::signature::AsyncFnSig<(State,), Data<State>>>(gated_after_1)
        .expect("body");
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
    let marker: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(
            |value: &u32| {
                super::test_support::record("parent-after-loop");
                Ok(*value)
            },
            read,
        )
        .expect("marker");
    let parent = parent
        .finish::<Data<u32>, _>(marker)
        .expect("parent finish");

    // Pending 阶段：同一 Future 尚未完成，不得建立下一轮或父后步。
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    install_gate();
    let boxed = advance_to_pending(
        definition_in_root::<
            fn(&mut ExecutionContext, &ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            parent.definition(),
            vec![root_input_state(&position, 0)],
            |_, _| {},
            None,
        ),
        1,
    );
    let pending_events = take_shared_events();
    assert_eq!(
        count(&pending_events, "multi-round:1"),
        1,
        "{pending_events:?}"
    );
    assert!(
        !saw_prefix(&pending_events, "multi-round:2"),
        "Pending 时不建立下一轮"
    );
    assert!(
        !saw(&pending_events, "read-state-step"),
        "Pending 时父后步不执行"
    );
    drop(boxed);
    super::test_support::release_gate();

    // Ready 阶段：用不挂起的同形状 body 正常推进到完成，并核对唯一执行域与生命周期。
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    let (parent_ready, position_ready) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: true,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    let parent = parent_ready;
    let position = position_ready;
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
    .expect("ready completion");
    let events = take_shared_events();
    assert!(
        saw(&events, "read-state-step"),
        "Ready 后父后步执行: {events:?}"
    );
    assert!(saw(&events, "loop-finished"), "{events:?}");
    // 唯一执行域：所有真实创建点共享同一 Execution／Coordinator／Container 地址。
    let addresses = super::test_support::boundary_address_snapshot();
    assert!(!addresses.is_empty());
    let (_, identity, coordinator, container) = &addresses[0];
    assert!(
        addresses
            .iter()
            .all(|(_, i, c, t)| i == identity && c == coordinator && t == container),
        "{addresses:?}"
    );
    // 最终声明输出只绑定一次：控制器 refs 中每个位置只出现一次，且数量等于输入数＋1。
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let last = snapshots
        .iter()
        .rfind(|s| s.operation == super::test_support::RoundCollectOperation::Promote)
        .expect("promote snapshot");
    let controller_refs = last.controller_refs.as_ref().expect("controller refs");
    assert_eq!(
        controller_refs.len(),
        1,
        "最终 Promote 之前 Loop 只绑定声明输入（输出尚未绑定）: {controller_refs:?}"
    );
    // final Ref 只绑定一次：第二次绑定由 `bind_state_output` 的位置已绑定判据拒绝
    // （见 r11 的最终绑定冲突样本），因此不存在重复绑定路径。
    // 各 State 实例 Drop 一次（S0 由 Root、S1 由 Loop、S2 最终移交）。
    let drops = state_drops(&events);
    let unique: std::collections::HashSet<&u32> = drops.iter().collect();
    assert_eq!(drops.len(), unique.len(), "{drops:?}");
}

#[test]
fn r14_retry_flow_body_and_imported_alias_finish() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    let (parent, input_position, _loop_caller) = retry_flow_body_fixture();
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &input_position, Item { id: 6 })
                .expect("input");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("retry flow body scenario");
    let events = take_shared_events();
    // 完整 Flow body：两轮（Continue／Finish），每轮临时值随 Round 清理。
    assert_eq!(count(&events, "flow-body-round:1"), 1, "{events:?}");
    assert_eq!(count(&events, "flow-body-round:2"), 1, "{events:?}");
    assert_eq!(count(&events, "flow-body-seed:6"), 2, "两轮各自产生临时值");
    assert_eq!(count(&events, "rev-temp-dropped"), 2, "临时值随 Round 清理");
    // 原输入地址与 DataId 各轮不变。
    let addresses: Vec<&String> = events
        .iter()
        .filter(|event| event.starts_with("flow-body-input-addr:"))
        .collect();
    assert_eq!(addresses.len(), 2, "{events:?}");
    assert!(addresses.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(count(&events, "loop-collect:discarded"), 1, "首轮丢弃");
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "次轮保留");

    // Retry imported alias：单轮 Finish，原 owner 不变并最终 Export。
    super::test_support::reset_observations();
    boundary_creation_reset();
    let (parent, position) = retry_imported_alias_fixture();
    let job_id: RefCell<Option<DataId>> = RefCell::new(None);
    // 读取步骤的输出位置：由受控的 Site 接口取得（与 Export 使用同一来源）。
    let read_position = match parent
        .definition()
        .steps()
        .last()
        .expect("read step")
        .site()
    {
        CallSite::Node(site) => site.outputs().first().cloned().expect("read output"),
        CallSite::Orchestrator(site) => site.outputs().first().cloned().expect("read output"),
    };
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            let id = ctx
                .register_owned(&ctx.root_scope(), &position, Job { id: 11, done: true })
                .expect("job");
            *job_id.borrow_mut() = Some(id);
        },
        Some(move |view: &mut RootView<'_, '_>| {
            assert_eq!(*view.resolve::<u32>(&read_position)?, 11, "alias 原样导出");
            Ok(())
        }),
    ))
    .expect("retry imported alias scenario");
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let promote = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .expect("promote snapshot");
    assert_eq!(
        promote.selected_data,
        job_id.borrow().clone(),
        "{promote:?}"
    );
    assert_eq!(
        promote.state_pending.as_ref().map(Vec::len),
        Some(0),
        "{promote:?}"
    );
    assert_eq!(count(&take_shared_events(), "loop-finished"), 1);
}

#[test]
fn r14_delayed_recycle_keeps_pending_until_alias_and_exit_fallback() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    install_loop_fault(LoopFault::DelayedRecycleAlias);
    let (parent, position) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: state.value + 1 >= 3,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    drive_state(&parent, &position, 0).expect("delayed recycle scenario");
    let events = take_shared_events();
    // 旧值被合法本地 alias 保留：回收条件不满足，不销毁旧值。
    assert!(saw_prefix(&events, "delayed-alias:"), "{events:?}");
    // 延迟回收：受控回收时旧值仍被 alias 引用，因此不在该轮销毁，而由控制器退出兜底清理。
    assert!(
        at(&events, "state-dropped:1") > at(&events, "loop-finished"),
        "alias 存活期间不销毁旧值、退出时清理: {events:?}"
    );
    let drops = state_drops(&events);
    let unique: std::collections::HashSet<&u32> = drops.iter().collect();
    assert_eq!(drops.len(), unique.len(), "各实例只销毁一次: {drops:?}");
    assert_eq!(drops, vec![0, 1, 2, 3], "S0／S1／S2／S3 各一次: {drops:?}");
}

#[test]
fn r14_deep_nested_error_cleans_existing_current_and_state_identity_is_not_shared() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    // Loop 的 body 是完成态 Flow，第二轮的 descendant 内真实失败：此前已有 Loop-owned current。
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let position = input.position().clone();
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, super::signature::SyncFnSig<(State,), Data<State>>>(
        deep_failing_body as fn(&State) -> Result<State, BodyError>,
    )
    .expect("body");
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
    let error = drive_state(&parent, &position, 0).expect_err("deep error propagates");
    assert!(
        matches!(
            error.scope_error(),
            Some(ScopeError::Invariant { violated }) if *violated == "deep nested failure probe"
        ),
        "保留实际深层诊断: {error:?}"
    );
    let events = take_shared_events();
    assert_eq!(
        count(&events, "loop-collect:promoted"),
        1,
        "首轮已保留: {events:?}"
    );
    assert!(
        !saw_prefix(&events, "multi-round:3"),
        "失败后不再建立下一轮: {events:?}"
    );
    let drops = state_drops(&events);
    let unique: std::collections::HashSet<&u32> = drops.iter().collect();
    assert_eq!(drops.len(), unique.len(), "current 清理一次: {drops:?}");
    assert!(
        drops.contains(&1),
        "已有 Loop-owned current 被清理: {drops:?}"
    );
}

fn deep_failing_body(state: &State) -> Result<State, BodyError> {
    let round = next_round();
    super::test_support::record(&format!("deep-round:{round}"));
    if round >= 2 {
        return deep_error(state);
    }
    Ok(State {
        value: state.value + 1,
        finish: false,
    })
}

// ---------------------------------------------------------------- R15：权限、原子性与保存诊断

#[test]
fn r15_permit_branches_are_rejected_before_commit() {
    for (fault, expected, label) in [
        (
            LoopFault::PermitProbeWrongRound,
            "round collect requires the permitted round scope",
            "wrong round",
        ),
        (
            LoopFault::PermitProbeWrongParent,
            "round frame caller is not the registered loop scope",
            "wrong parent",
        ),
        (
            LoopFault::PermitProbeWrongSelected,
            "round promote requires the registered wrapper's only declared output",
            "wrong selected",
        ),
    ] {
        super::test_support::reset_observations();
        boundary_creation_reset();
        install_loop_fault(fault);
        let (parent, position) = iter_fixture_for(
            (|state: &State| {
                Ok(State {
                    value: state.value + 1,
                    finish: true,
                })
            }) as fn(&State) -> Result<State, BodyError>,
        );
        drive_state(&parent, &position, 3).expect("legal path still completes");
        let events = take_shared_events();
        let probe = events
            .iter()
            .find(|event| event.starts_with("permit-probe:"))
            .expect("permit probe recorded");
        assert!(probe.contains("Rejected"), "{label}: {probe}");
        assert!(probe.contains(expected), "{label}: {probe}");
        assert!(
            saw_prefix(&events, "permit-probe-round-state:Some(Active)"),
            "{label}: 来源尚未冻结: {events:?}"
        );
        assert_eq!(
            count(&events, "loop-collect:promoted"),
            1,
            "{label}: 拒绝后合法路径仍成功一次: {events:?}"
        );
    }
}

#[test]
fn r15_generic_promote_probe_matches_both_scopes_and_keeps_state() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::GenericPromoteFullProbe);
    let (parent, position) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: true,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    drive_state(&parent, &position, 3).expect("legal path completes");
    let events = take_shared_events();
    let creations = boundary_creation_snapshot();
    let loop_seq = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Loop)
        .expect("loop scope")
        .0
        .seq();
    let round_seq = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Round)
        .expect("round scope")
        .0
        .seq();
    let probe = events
        .iter()
        .find(|event| event.starts_with("generic-promote:"))
        .expect("probe recorded");
    assert!(probe.contains("OutsideInvocation"), "越权必须被拒: {probe}");
    assert!(
        probe.contains(&format!("seq = {loop_seq}")),
        "冲突 Scope 是实际 LoopScope: {probe}"
    );
    assert!(
        probe.contains(&format!("seq = {round_seq}")),
        "current 是实际 RoundScope: {probe}"
    );
    // 完整前后快照：Before 与 AfterReject 成对且逐项相等（双侧与 state 均未变）。
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let before = snapshots
        .iter()
        .find(|s| s.phase == super::test_support::RoundCollectSnapshotPhase::Before)
        .expect("before");
    let after = snapshots
        .iter()
        .find(|s| s.phase == super::test_support::RoundCollectSnapshotPhase::AfterReject)
        .expect("after");
    assert_eq!(
        before.source_refs, after.source_refs,
        "source refs 必须相等"
    );
    assert_eq!(
        before.source_owned, after.source_owned,
        "source owned 必须相等"
    );
    assert_eq!(
        before.controller_refs, after.controller_refs,
        "controller refs 必须相等"
    );
    assert_eq!(
        before.controller_owned, after.controller_owned,
        "controller owned 必须相等"
    );
    assert_eq!(
        before.state_target, after.state_target,
        "state target 必须相等"
    );
    assert_eq!(
        before.state_pending, after.state_pending,
        "state pending 必须相等"
    );
    assert_eq!(before.next_data_id, after.next_data_id, "next id 必须相等");
    assert!(
        before.observation_error.is_none() && after.observation_error.is_none(),
        "观察失败必须显式"
    );
    assert_eq!(
        before.source_state,
        Some(ScopeState::Active),
        "来源 Round 仍 Active"
    );
    assert_eq!(
        count(&events, "loop-collect:promoted"),
        1,
        "合法窄许可随后成功"
    );
}

#[test]
fn r15_promote_target_and_type_preconditions_run_with_both_guard_branches() {
    // 存活前置：被选输出被真实销毁 → prepare 拒绝；cleanup 成功后来源真实 Closed（Closed 分支）。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::PromoteSelectedDestroyed);
    let (parent, position) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: true,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    let error = drive_state(&parent, &position, 3).expect_err("destroyed target rejected");
    assert!(
        matches!(error.scope_error(), Some(ScopeError::TargetNotAlive { .. })),
        "存活判据的实际变体: {error:?}"
    );
    let events = take_shared_events();
    // 被销毁的 entry 仍在 owned 里：准备拒绝后清理也失败 → 来源未 Closed（保留责任由 guard 清理）。
    assert!(
        saw(&events, "round-guard:open"),
        "来源未 Closed: {events:?}"
    );
    assert!(
        saw_prefix(&events, "round-cleanup-failure"),
        "清理失败独立报告: {events:?}"
    );
    assert_promote_rejection_unchanged();

    // 类型前置：控制状态声明类型不匹配 → prepare 类型判据拒绝；owned 完好，cleanup 成功，
    // 来源真实 Closed（另一种 guard 处置）。
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::PromoteStateTypeMismatch);
    let (parent, position) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: true,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    let error = drive_state(&parent, &position, 3).expect_err("type mismatch rejected");
    assert!(
        matches!(error.scope_error(), Some(ScopeError::TypeMismatch { .. })),
        "type precondition: {error:?}"
    );
    assert_promote_rejection_unchanged();
    let events = take_shared_events();
    assert!(
        saw(&events, "round-guard:closed"),
        "cleanup 成功后来源 Closed: {events:?}"
    );
    assert!(
        !saw_prefix(&events, "round-cleanup-failure"),
        "cleanup 成功: {events:?}"
    );
    assert!(!saw_prefix(&events, "loop-finished"), "不产生最终输出");
}

#[test]
fn r15_parent_recycle_error_has_its_own_saved_cause() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    install_loop_fault(LoopFault::RecycleNotActive);
    let (parent, position) = iter_fixture_for(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: state.value + 1 >= 3,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    );
    let _ = drive_state(&parent, &position, 0).expect_err("recycle failure");
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "{saved:?}");
    let termination = &saved[0];
    // body 错误与 parent 回收错误的来源可分：此处定位在 **LoopScope**、诊断是回收前置拒绝。
    let loop_seq = boundary_creation_snapshot()
        .into_iter()
        .find(|(_, _, role)| *role == ScopeRole::Loop)
        .expect("loop scope")
        .0
        .seq();
    assert_eq!(
        termination.scope.as_ref().map(ScopeId::seq),
        Some(loop_seq),
        "回收错误的定位是 Loop 而不是 Round: {termination:?}"
    );
    match &termination.scope_error {
        Some(ScopeError::ScopeNotActive { .. }) => {}
        other => panic!("expected ScopeNotActive, got {other:?}"),
    }
}

#[test]
fn r14_control_state_identity_is_per_invocation() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    // 同一个完成态 Loop 的两处调用 × 两次 Execution：4 个互不相同的 ControlState 身份。
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, SyncFnSig<(State,), Data<State>>>(
        (|state: &State| {
            Ok(State {
                value: state.value + 1,
                finish: true,
            })
        }) as fn(&State) -> Result<State, BodyError>,
    )
    .expect("body");
    let orchestrator: Loop<Iter1<State>> = iter.finish().expect("finish");
    let (mut parent, handles) = FlowBuilder::<(State, State)>::start().expect("parent");
    let (first, second) = handles;
    let first_position = first.position().clone();
    let second_position = second.position().clone();
    let first_out: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator.clone(), first)
        .expect("first site");
    let second_out: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<State, Data<State>>, _>(orchestrator, second)
        .expect("second site");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            first_out,
        )
        .expect("read");
    let _ = second_out;
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");
    for run in 0..2 {
        let first_position = first_position.clone();
        let second_position = second_position.clone();
        drive(definition_in_root(
            parent.definition(),
            Vec::new(),
            move |ctx, _root| {
                ctx.register_owned(
                    &ctx.root_scope(),
                    &first_position,
                    State {
                        value: run,
                        finish: false,
                    },
                )
                .expect("first input");
                ctx.register_owned(
                    &ctx.root_scope(),
                    &second_position,
                    State {
                        value: run + 1,
                        finish: false,
                    },
                )
                .expect("second input");
            },
            None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
        ))
        .expect("execution");
    }
    let events = take_shared_events();
    let mut states: Vec<String> = events
        .iter()
        .filter(|event| event.starts_with("loop-state:"))
        .cloned()
        .collect();
    assert_eq!(states.len(), 4, "两处调用 × 两次 Execution: {states:?}");
    states.sort();
    states.dedup();
    assert_eq!(states.len(), 4, "state 登记身份不串: {states:?}");
}

#[test]
fn r15_saved_termination_and_cleanup_are_exact_and_not_overwritten() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    ROUNDS.with(|slot| slot.set(0));
    let (parent, position) =
        iter_fixture_for(deep_failing_body as fn(&State) -> Result<State, BodyError>);
    let _ = drive_state(&parent, &position, 0).expect_err("body error");
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "首错只保存一次: {saved:?}");
    let termination = &saved[0];
    assert_eq!(termination.kind, TerminationKind::BodyError);
    assert_eq!(termination.note, "scope operation failed");
    match &termination.scope_error {
        Some(ScopeError::Invariant { violated }) => {
            assert_eq!(*violated, "deep nested failure probe", "首错内容精确")
        }
        other => panic!("expected Invariant, got {other:?}"),
    }
    // 实际失败 Scope 是第 2 轮的 RoundScope。
    let round_seq = boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, role)| *role == ScopeRole::Round)
        .nth(1)
        .expect("second round")
        .0
        .seq();
    assert_eq!(
        termination.scope.as_ref().map(ScopeId::seq),
        Some(round_seq),
        "首错定位到实际失败 Scope"
    );
}

fn assert_final_bind_rejection_unchanged() {
    let snapshots = take_final_bind_pre_cleanup();
    assert_eq!(snapshots.len(), 2, "one rejection observation pair");
    let before = &snapshots[0];
    let after = &snapshots[1];
    assert_eq!(before.phase, FinalBindSnapshotPhase::Before);
    assert_eq!(after.phase, FinalBindSnapshotPhase::AfterReject);
    assert!(before.observation_error.is_none(), "{before:?}");
    assert!(after.observation_error.is_none(), "{after:?}");
    let mut normalized = after.clone();
    normalized.phase = before.phase;
    assert_eq!(
        *before, normalized,
        "final bind rejection must preserve all observed state"
    );
}

fn assert_promote_rejection_unchanged() {
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    assert_eq!(snapshots.len(), 2, "one shared prepare rejection pair");
    let before = &snapshots[0];
    let after = &snapshots[1];
    assert_eq!(
        before.phase,
        super::test_support::RoundCollectSnapshotPhase::Before
    );
    assert_eq!(
        after.phase,
        super::test_support::RoundCollectSnapshotPhase::AfterReject
    );
    assert!(before.observation_error.is_none(), "{before:?}");
    assert!(after.observation_error.is_none(), "{after:?}");
    let mut normalized = after.clone();
    normalized.phase = before.phase;
    assert_eq!(
        *before, normalized,
        "shared prepare rejection must preserve both sides, ownership and state"
    );
}
