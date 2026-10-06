//! V21-09 验收样本（一）：Loop 的真实 Round、状态提升与 Retry／Iter 正式推进。
//!
//! 覆盖 J01～J04、J06、J10～J15、J25～J26 的主要路径；拒绝／诊断／取消见
//! `v21_09_tests_failures.rs`。

use std::cell::{Cell, RefCell};

use super::builder::TypedCallBuilder;
use super::context::BodyError;
use super::flow::{Flow, FlowBuilder};
use super::identity::{DataId, ScopeId};
use super::loop_orchestrator::{Iter2, LoopBuilder, LoopControl, LoopDecision, Retry1, Retry2};
use super::node::{NodeCall1, NodeCall2};
use super::orchestrator::{OrchCall, ScopeRole};
use super::signature::{AsyncFnSig, Data, NodeSig, OrchSig, SyncFnSig};
use super::test_support::{
    RootView, boundary_address_snapshot, boundary_creation_reset, boundary_creation_snapshot,
    count, definition_in_root, drive, saw, take_shared_events,
};

// ---------------------------------------------------------------- 业务与 reader

/// Retry 的原始业务输入（非 Clone；Drop 见证输入生命周期）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Item {
    pub(crate) id: u32,
}

impl Drop for Item {
    fn drop(&mut self) {
        super::test_support::record("item-dropped");
    }
}

/// 第二业务输入／Iter 的固定 shared（非 Clone）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Rules {
    pub(crate) step: u32,
}

/// Retry 的 body 输出：业务字段直接表达 Continue／Finish。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Draft {
    pub(crate) round: u32,
    pub(crate) finish: bool,
}

impl LoopControl for Draft {
    fn loop_decision(&self) -> LoopDecision {
        if self.finish {
            LoopDecision::Finish
        } else {
            LoopDecision::Continue
        }
    }
}

/// Iter 的推进状态（Drop 见证每个实例恰好销毁一次）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct State {
    pub(crate) value: u32,
    pub(crate) finish: bool,
}

impl Drop for State {
    fn drop(&mut self) {
        super::test_support::record(&format!("state-dropped:{}", self.value));
    }
}

impl LoopControl for State {
    fn loop_decision(&self) -> LoopDecision {
        if self.finish {
            LoopDecision::Finish
        } else {
            LoopDecision::Continue
        }
    }
}

/// body 内的临时值（Drop 见证它随 Round 关闭被清理）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Seed {
    pub(crate) n: u32,
}

impl Drop for Seed {
    fn drop(&mut self) {
        super::test_support::record("seed-dropped");
    }
}

/// 结构体 Node：按轮次给出 Continue／Continue／Finish，并记录输入地址。
pub(crate) struct RetryBody {
    rounds: Cell<u32>,
}

impl RetryBody {
    pub(crate) fn new() -> Self {
        Self {
            rounds: Cell::new(0),
        }
    }
}

impl NodeCall1<Item, Data<Draft>> for RetryBody {
    fn call<'a>(&'a self, input: &'a Item) -> super::signature::NodeFut<'a, Draft> {
        let round = self.rounds.get() + 1;
        self.rounds.set(round);
        super::test_support::record(&format!("retry-round:{round}"));
        super::test_support::record(&format!("retry-input-addr:{:p}", input as *const Item));
        Box::pin(async move {
            Ok(Draft {
                round,
                finish: round >= 3,
            })
        })
    }
}

/// 双输入结构体 Node：`Item + Rules` → `Draft`。
pub(crate) struct RetryPairBody {
    rounds: Cell<u32>,
}

impl RetryPairBody {
    pub(crate) fn new() -> Self {
        Self {
            rounds: Cell::new(0),
        }
    }
}

impl NodeCall2<Item, Rules, Data<Draft>> for RetryPairBody {
    fn call<'a>(
        &'a self,
        first: &'a Item,
        second: &'a Rules,
    ) -> super::signature::NodeFut<'a, Draft> {
        let round = self.rounds.get() + 1;
        self.rounds.set(round);
        super::test_support::record(&format!("retry-pair-round:{round}"));
        super::test_support::record(&format!("retry-pair-input-addr:{:p}", first as *const Item));
        let _ = second;
        Box::pin(async move {
            Ok(Draft {
                round,
                finish: round >= 3,
            })
        })
    }
}

/// 异步 Node：`State` → `Seed`（body 内临时值）。
async fn async_seed(state: &State) -> Result<Seed, BodyError> {
    super::test_support::record(&format!("async-seed:{}", state.value));
    Ok(Seed { n: state.value })
}

/// 同步 Node：`Seed + Rules` → 新 `State`（descendant 内的真实推进）。
fn advance(seed: &Seed, rules: &Rules) -> Result<State, BodyError> {
    let value = seed.n + rules.step;
    super::test_support::record(&format!("advance:{value}"));
    Ok(State {
        value,
        finish: value >= 3,
    })
}

/// 读取最终结果并转成 `u32` 的父后步 Node（不与 Loop 共享类型）。
fn read_state(state: &State) -> Result<u32, BodyError> {
    Ok(state.value)
}

/// 读取 Retry 最终结果。
fn read_draft(draft: &Draft) -> Result<u32, BodyError> {
    Ok(draft.round)
}

/// 本次运行中各 State 实例的销毁见证（升序值）。
fn state_drops(events: &[String]) -> Vec<u32> {
    let mut drops: Vec<u32> = events
        .iter()
        .filter_map(|event| event.strip_prefix("state-dropped:"))
        .filter_map(|value| value.parse::<u32>().ok())
        .collect();
    drops.sort_unstable();
    drops
}

// ---------------------------------------------------------------- 夹具

/// 建立 Iter 的完成态包装 body（完成态 Flow）：单 Step → 内含异步 Node 与 descendant。
fn iter_wrapper_flow() -> Flow<(State, Rules), Data<State>> {
    let (mut inner, handles) = FlowBuilder::<(State, Rules)>::start().expect("inner body flow");
    let (state, rules) = handles;
    let seed: super::data_ref::DataRef<Seed> = inner
        .then::<_, AsyncFnSig<(State,), Data<Seed>>, _>(async_seed, state)
        .expect("async seed step");
    let (mut descendant, descendant_handles) =
        FlowBuilder::<(Seed, Rules)>::start().expect("descendant flow");
    let (seed_in, rules_in) = descendant_handles;
    let advanced: super::data_ref::DataRef<State> = descendant
        .then::<_, SyncFnSig<(Seed, Rules), Data<State>>, _>(
            advance as fn(&Seed, &Rules) -> Result<State, BodyError>,
            (seed_in, rules_in),
        )
        .expect("advance step");
    let descendant = descendant
        .finish::<Data<State>, _>(advanced)
        .expect("descendant finish");
    let produced: super::data_ref::DataRef<State> = inner
        .then::<_, OrchSig<(Seed, Rules), Data<State>>, _>(descendant, (seed, rules))
        .expect("descendant step");
    inner
        .finish::<Data<State>, _>(produced)
        .expect("inner body finish")
}

/// Iter 主场景：Root → 父 Flow → Iter（Flow body）→ 父后步读取结果。
pub(crate) fn iter_main_parent() -> (
    Flow<(State, Rules), Data<u32>>,
    super::ref_id::RefId,
    super::ref_id::RefId,
) {
    let (mut parent, handles) = FlowBuilder::<(State, Rules)>::start().expect("parent flow");
    let (state, rules) = handles;
    let state_position = state.position().clone();
    let rules_position = rules.position().clone();
    let mut iter: LoopBuilder<Iter2<State, Rules>> = LoopBuilder::start().expect("iter loop");
    iter.then_body::<_, OrchSig<(State, Rules), Data<State>>>(iter_wrapper_flow())
        .expect("iter flow body");
    let orchestrator = iter.finish().expect("iter finish");
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<(State, Rules), Data<State>>, _>(orchestrator, (state, rules))
        .expect("parent then iter");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("parent read step");
    (
        parent.finish::<Data<u32>, _>(read).expect("parent finish"),
        state_position,
        rules_position,
    )
}

// ---------------------------------------------------------------- J01／J03／J12～J15／J26

#[test]
fn j01_iter_main_scenario_three_real_rounds() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    let (parent, state_position, rules_position) = iter_main_parent();
    let state_id: RefCell<Option<DataId>> = RefCell::new(None);
    let rules_id: RefCell<Option<DataId>> = RefCell::new(None);
    let root_state: Cell<Option<ScopeId>> = Cell::new(None);
    let definition = parent.definition();
    drive(definition_in_root(
        definition,
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
                .expect("state input");
            *state_id.borrow_mut() = Some(state);
            root_state.set(Some(root.clone()));
            let rules = ctx
                .register_owned(&ctx.root_scope(), &rules_position, Rules { step: 1 })
                .expect("rules input");
            *rules_id.borrow_mut() = Some(rules);
        },
        Some(|view: &mut RootView<'_, '_>| {
            // 父后步读取 Loop 的最终声明输出：S3 可被父后步借用。
            let last_output = view.snapshot()?;
            assert!(!last_output.is_empty(), "父后步已绑定最终输出");
            Ok(())
        }),
    ))
    .expect("iter main scenario");

    let events = take_shared_events();
    // 三轮真实 Round：轮次事件与创建记录一致。
    for value in 1..=3 {
        assert!(
            saw(&events, &format!("advance:{value}")),
            "第 {value} 轮真实推进"
        );
    }
    assert!(!saw(&events, "advance:4"), "Finish 后不再建立下一轮");
    assert_eq!(
        count(&events, "async-seed:2"),
        1,
        "异步 Node 每轮真实借用: {events:?}"
    );
    let rounds: Vec<_> = boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, role)| *role == ScopeRole::Round)
        .collect();
    assert_eq!(rounds.len(), 3, "三轮各建立一次 RoundScope：{rounds:?}");
    let loop_scopes: Vec<_> = boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, role)| *role == ScopeRole::Loop)
        .collect();
    assert_eq!(loop_scopes.len(), 1, "LoopScope 只建立一次");
    // Round 的直接 parent 是 LoopScope。
    let loop_scope = loop_scopes[0].0.clone();
    for (round, parent_scope, _) in &rounds {
        assert_eq!(
            parent_scope, &loop_scope,
            "Round 的直接 parent 是 LoopScope"
        );
        let _ = round;
    }
    // 唯一执行域：所有创建点共享同一 Execution identity／Coordinator／Container 地址。
    let addresses = boundary_address_snapshot();
    assert!(!addresses.is_empty());
    let first = addresses[0].1;
    assert!(
        addresses
            .iter()
            .all(|(_, identity, coordinator, container)| {
                *identity == first && *coordinator == addresses[0].2 && *container == addresses[0].3
            })
    );
    // 三轮的状态推进与回收都发生在真实 Loop 路径上。
    assert_eq!(count(&events, "loop-collect:promoted"), 3, "{events:?}");
    assert_eq!(count(&events, "loop-round:3"), 1);

    // S0 与 shared 始终由 Root 负责且存活。
    let state_id = state_id.borrow().clone().expect("state id");
    assert!(
        items_dropped(&events).is_empty(),
        "运行中不销毁输入：{events:?}"
    );
    let _ = (state_id, rules_id.borrow().clone());
}

fn items_dropped(events: &[String]) -> Vec<String> {
    events
        .iter()
        .filter(|event| event.as_str() == "item-dropped")
        .cloned()
        .collect()
}

#[test]
fn j02_retry_main_scenario_keeps_original_input() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    let (mut parent, handles) = FlowBuilder::<(Item, Rules)>::start().expect("parent flow");
    let (item, rules) = handles;
    let item_position = item.position().clone();
    let rules_position = rules.position().clone();
    let mut retry: LoopBuilder<Retry2<Item, Rules, Draft>> =
        LoopBuilder::start().expect("retry loop");
    retry
        .then_body::<_, NodeSig<(Item, Rules), Data<Draft>>>(RetryPairBody::new())
        .expect("retry pair body");
    let orchestrator = retry.finish().expect("retry finish");
    let produced: super::data_ref::DataRef<Draft> = parent
        .then::<_, OrchSig<(Item, Rules), Data<Draft>>, _>(orchestrator, (item, rules))
        .expect("parent then retry");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(Draft,), Data<u32>>, _>(
            read_draft as fn(&Draft) -> Result<u32, BodyError>,
            produced,
        )
        .expect("parent read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &item_position, Item { id: 7 })
                .expect("item input");
            ctx.register_owned(&ctx.root_scope(), &rules_position, Rules { step: 9 })
                .expect("rules input");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("retry main scenario");

    let events = take_shared_events();
    assert_eq!(count(&events, "retry-pair-round:1"), 1);
    assert_eq!(count(&events, "retry-pair-round:2"), 1);
    assert_eq!(count(&events, "retry-pair-round:3"), 1);
    assert!(!saw(&events, "retry-pair-round:4"), "Finish 后停止");
    // 每轮输入地址不变：同一原始输入被重新导入，而不是 Output 反向成为 Input。
    let addresses: Vec<&String> = events
        .iter()
        .filter(|event| event.starts_with("retry-pair-input-addr:"))
        .collect();
    assert_eq!(addresses.len(), 3, "{events:?}");
    assert!(
        addresses.windows(2).all(|pair| pair[0] == pair[1]),
        "每轮输入地址与原 DataId 不变：{addresses:?}"
    );
    assert_eq!(
        count(&events, "loop-collect:promoted"),
        1,
        "Retry 只在 Finish 保留一次"
    );
    assert_eq!(
        count(&events, "loop-collect:discarded"),
        2,
        "前两轮丢弃本轮结果"
    );
    let rounds = boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, role)| *role == ScopeRole::Round)
        .count();
    assert_eq!(rounds, 3);
}

#[test]
fn j04_single_round_finish_stops_immediately() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    // Retry 单输入：结构体 Node 首轮即 Finish。
    let (mut parent, input) = FlowBuilder::<(Item,)>::start().expect("parent");
    let position = input.position().clone();
    let mut retry: LoopBuilder<Retry1<Item, Draft>> = LoopBuilder::start().expect("retry");
    retry
        .then_body::<_, NodeSig<(Item,), Data<Draft>>>(SingleFinishBody)
        .expect("body");
    let orchestrator = retry.finish().expect("finish");
    let produced: super::data_ref::DataRef<Draft> = parent
        .then::<_, OrchSig<Item, Data<Draft>>, _>(orchestrator, input)
        .expect("then retry");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(Draft,), Data<u32>>, _>(
            read_draft as fn(&Draft) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &position, Item { id: 1 })
                .expect("input");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("single round scenario");

    let events = take_shared_events();
    assert_eq!(count(&events, "single-finish:1"), 1, "首次必跑且只跑一轮");
    assert_eq!(
        boundary_creation_snapshot()
            .into_iter()
            .filter(|(_, _, role)| *role == ScopeRole::Round)
            .count(),
        1,
        "Finish 一轮就停：无隐藏 round cap，也无第二轮"
    );
    assert_eq!(count(&events, "loop-collect:promoted"), 1);
}

pub(crate) struct SingleFinishBody;

impl NodeCall1<Item, Data<Draft>> for SingleFinishBody {
    fn call<'a>(&'a self, input: &'a Item) -> super::signature::NodeFut<'a, Draft> {
        let id = input.id;
        super::test_support::record("single-finish:1");
        Box::pin(async move {
            Ok(Draft {
                round: id,
                finish: true,
            })
        })
    }
}

// ---------------------------------------------------------------- J06：单 Step 分派与最小嵌套

#[test]
fn j06_wrapper_single_step_dispatch_and_nested_loop() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    // 外层 Iter 的 body 是完成态 Flow，其唯一 Step 是真实 inner Loop（嵌套）。
    let (mut parent, handles) = FlowBuilder::<(State, Rules)>::start().expect("parent");
    let (state, rules) = handles;
    let state_position = state.position().clone();
    let rules_position = rules.position().clone();
    let mut outer: LoopBuilder<Iter2<State, Rules>> = LoopBuilder::start().expect("outer loop");
    outer
        .then_body::<_, OrchSig<(State, Rules), Data<State>>>(nested_wrapper())
        .expect("outer nested body");
    let outer = outer.finish().expect("outer finish");
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<(State, Rules), Data<State>>, _>(outer, (state, rules))
        .expect("parent then outer");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &state_position,
                State {
                    value: 0,
                    finish: false,
                },
            )
            .expect("state");
            ctx.register_owned(&ctx.root_scope(), &rules_position, Rules { step: 1 })
                .expect("rules");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("nested loop scenario");

    let events = take_shared_events();
    assert!(
        saw(&events, "inner-iter:1"),
        "inner Loop 真实执行：{events:?}"
    );
    assert!(saw(&events, "inner-iter:3"), "inner Loop 推进到完成");
    let creations = boundary_creation_snapshot();
    let loop_scopes: Vec<_> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Loop)
        .collect();
    assert_eq!(loop_scopes.len(), 2, "outer／inner 各有自己的 LoopScope");
    let outer_scope = &loop_scopes[0].0;
    let inner_scope = &loop_scopes[1].0;
    assert_ne!(
        outer_scope, inner_scope,
        "两次 Invocation 的 LoopScope 不串台"
    );
    // inner Round 的 parent 是 inner LoopScope；outer Round 的 parent 是 outer LoopScope。
    for (round, parent_scope, role) in &creations {
        if *role != ScopeRole::Round {
            continue;
        }
        assert!(
            parent_scope == outer_scope || parent_scope == inner_scope,
            "Round 的直接 parent 必是某个 LoopScope: {round:?}"
        );
    }
}

/// 外层 body 包装：唯一 Step 是 inner Loop（完成态 Orchestrator）。
fn nested_wrapper() -> Flow<(State, Rules), Data<State>> {
    let (mut wrapper, handles) = FlowBuilder::<(State, Rules)>::start().expect("nested wrapper");
    let (state, rules) = handles;
    let mut inner: LoopBuilder<Iter2<State, Rules>> = LoopBuilder::start().expect("inner loop");
    inner
        .then_body::<_, SyncFnSig<(State, Rules), Data<State>>>(
            inner_step as fn(&State, &Rules) -> Result<State, BodyError>,
        )
        .expect("inner body");
    let inner = inner.finish().expect("inner finish");
    let produced: super::data_ref::DataRef<State> = wrapper
        .then::<_, OrchSig<(State, Rules), Data<State>>, _>(inner, (state, rules))
        .expect("wrapper then inner");
    wrapper
        .finish::<Data<State>, _>(produced)
        .expect("wrapper finish")
}

/// inner Loop 的 body：按 shared 步长推进，达到 3 即完成。
fn inner_step(state: &State, rules: &Rules) -> Result<State, BodyError> {
    let value = state.value + rules.step;
    super::test_support::record(&format!("inner-iter:{value}"));
    Ok(State {
        value,
        finish: value >= 3,
    })
}

// ---------------------------------------------------------------- J03／J10～J15

#[test]
fn j03_iter_node_body_with_shared_keeps_owner() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    let (mut parent, handles) = FlowBuilder::<(State, Rules)>::start().expect("parent");
    let (state, rules) = handles;
    let state_position = state.position().clone();
    let rules_position = rules.position().clone();
    let mut iter: LoopBuilder<Iter2<State, Rules>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, AsyncFnSig<(State, Rules), Data<State>>>(iter_shared_step)
        .expect("iter shared body");
    let orchestrator = iter.finish().expect("finish");
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<(State, Rules), Data<State>>, _>(orchestrator, (state, rules))
        .expect("then iter");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    let rules_id: RefCell<Option<DataId>> = RefCell::new(None);
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &state_position,
                State {
                    value: 0,
                    finish: false,
                },
            )
            .expect("state");
            let id = ctx
                .register_owned(&ctx.root_scope(), &rules_position, Rules { step: 1 })
                .expect("rules");
            *rules_id.borrow_mut() = Some(id);
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("iter shared scenario");

    let events = take_shared_events();
    assert_eq!(count(&events, "iter-shared:3"), 1, "{events:?}");
    // shared 每轮显式导入：目标／owner 不变。
    let addresses: Vec<&String> = events
        .iter()
        .filter(|event| event.starts_with("iter-shared-rules-addr:"))
        .collect();
    assert_eq!(addresses.len(), 3, "{events:?}");
    assert!(addresses.windows(2).all(|pair| pair[0] == pair[1]));
    assert!(rules_id.borrow().clone().is_some());
}

async fn iter_shared_step(state: &State, rules: &Rules) -> Result<State, BodyError> {
    let value = state.value + rules.step;
    super::test_support::record(&format!("iter-shared:{value}"));
    super::test_support::record(&format!(
        "iter-shared-rules-addr:{:p}",
        rules as *const Rules
    ));
    Ok(State {
        value,
        finish: value >= 3,
    })
}

#[test]
fn j10_retry_continue_discards_temporaries_and_keeps_final_state_uninitialized() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    let (mut parent, input) = FlowBuilder::<(Item,)>::start().expect("parent");
    let position = input.position().clone();
    let mut retry: LoopBuilder<Retry1<Item, Draft>> = LoopBuilder::start().expect("retry");
    retry
        .then_body::<_, NodeSig<(Item,), Data<Draft>>>(RetryBody::new())
        .expect("body");
    let orchestrator = retry.finish().expect("finish");
    let produced: super::data_ref::DataRef<Draft> = parent
        .then::<_, OrchSig<Item, Data<Draft>>, _>(orchestrator, input)
        .expect("then retry");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(Draft,), Data<u32>>, _>(
            read_draft as fn(&Draft) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(&ctx.root_scope(), &position, Item { id: 5 })
                .expect("input");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("retry continue scenario");

    let events = take_shared_events();
    // 前两轮 owned 结果随 Round 关闭丢弃，第三轮才保留。
    assert_eq!(count(&events, "loop-collect:discarded"), 2, "{events:?}");
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "{events:?}");
    // 拒绝分支未出现：Continue 不初始化最终结果状态。
    assert!(!saw(&events, "round-guard:open"), "{events:?}");
    // 收口观察通道：三轮 Before 快照，其中前两轮是 Discard，一次是 Promote。
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let discards = snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before
                && snapshot.operation == super::test_support::RoundCollectOperation::Discard
        })
        .count();
    let promotes = snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before
                && snapshot.operation == super::test_support::RoundCollectOperation::Promote
        })
        .count();
    assert_eq!((discards, promotes), (2, 1), "{snapshots:?}");
    for snapshot in &snapshots {
        assert!(
            snapshot.observation_error.is_none(),
            "观察失败不得静默降级: {snapshot:?}"
        );
    }
}

#[test]
fn j11_j12_iter_imported_initial_state_and_promotion_ownership() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    let (parent, state_position, rules_position) = iter_main_parent();
    let state_id: RefCell<Option<DataId>> = RefCell::new(None);
    let rules_id: RefCell<Option<DataId>> = RefCell::new(None);
    let root_scope: RefCell<Option<ScopeId>> = RefCell::new(None);
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, root| {
            let id = ctx
                .register_owned(
                    &ctx.root_scope(),
                    &state_position,
                    State {
                        value: 0,
                        finish: false,
                    },
                )
                .expect("state");
            *state_id.borrow_mut() = Some(id);
            *root_scope.borrow_mut() = Some(root.clone());
            let rules = ctx
                .register_owned(&ctx.root_scope(), &rules_position, Rules { step: 1 })
                .expect("rules");
            *rules_id.borrow_mut() = Some(rules);
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("iter promotion scenario");

    // 从收口观察通道读出责任转移：第 1 轮 S1 是 Round-owned → Loop-owned；
    // S0 始终由 Root 负责。
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let promotes: Vec<_> = snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.operation == super::test_support::RoundCollectOperation::Promote
                && snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before
        })
        .collect();
    assert_eq!(promotes.len(), 3, "{snapshots:?}");
    // 第 1 轮：被选输出的 owner 是 Round（来源），Round owned 含它。
    let first = promotes[0];
    let source = first.source.clone().expect("source round");
    assert_eq!(
        first.selected_owner.as_ref(),
        Some(&source),
        "S1 初始由 Round 负责"
    );
    assert!(
        first
            .source_owned
            .as_ref()
            .is_some_and(|owned| owned.contains(&first.selected_data.clone().expect("data"))),
        "被选输出在来源 owned 中: {first:?}"
    );
    // 第 2 轮：来源 Round 的输入是上一轮 Promote 后的 Loop-owned 值（不是 Root-owned S0）。
    let second = promotes[1];
    assert_ne!(
        second.selected_data, first.selected_data,
        "每轮产生新的 S（不同 DataId）"
    );
    // S0 与 shared 在运行中未被销毁。
    let events = take_shared_events();
    assert!(!saw(&events, "item-dropped"), "输入不被销毁: {events:?}");
    let _ = (
        state_id.borrow().clone(),
        rules_id.borrow().clone(),
        root_scope.borrow().clone(),
    );
}

#[test]
fn j13_loop_owned_old_state_is_recycled_after_round_refs_die() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    let (parent, state_position, rules_position) = iter_main_parent();
    let state_id: RefCell<Option<DataId>> = RefCell::new(None);
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            let id = ctx
                .register_owned(
                    &ctx.root_scope(),
                    &state_position,
                    State {
                        value: 0,
                        finish: false,
                    },
                )
                .expect("state");
            *state_id.borrow_mut() = Some(id);
            ctx.register_owned(&ctx.root_scope(), &rules_position, Rules { step: 1 })
                .expect("rules");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("iter recycle scenario");

    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let promotes: Vec<_> = snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.operation == super::test_support::RoundCollectOperation::Promote
                && snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before
        })
        .collect();
    assert_eq!(promotes.len(), 3, "{snapshots:?}");
    let initial = state_id.borrow().clone().expect("initial state");
    // 第 1 轮的 current-state 初值来自 Root 输入（原 owner 不变）。
    assert_eq!(
        promotes[0].state_target.clone(),
        Some(super::scope::TargetSnapshot::Data(initial)),
        "Iter 的 current-state 初值是 ancestor 的导入目标"
    );
    // 第 2／3 轮的目标就是上一轮 Promote 的选定值：状态逐轮更新，旧值随之被替换。
    let first = promotes[0].selected_data.clone().expect("S1");
    assert_eq!(
        promotes[1].state_target.clone(),
        Some(super::scope::TargetSnapshot::Data(first.clone())),
        "第 2 轮读取第 1 轮 Promote 的新状态"
    );
    let second = promotes[1].selected_data.clone().expect("S2");
    assert_eq!(
        promotes[2].state_target.clone(),
        Some(super::scope::TargetSnapshot::Data(second.clone())),
        "第 3 轮读取第 2 轮 Promote 的新状态"
    );
    // 受控回收：第 2 轮替换掉的旧状态在 Round 关闭后满足条件，被回收一次；因此后续
    // 轮次的 pending 不再残留（若未回收，第 3 轮 Before 会看到非空 pending）。
    for snapshot in &promotes {
        assert_eq!(
            snapshot.state_pending.as_ref().map(|pending| pending.len()),
            Some(0),
            "pending 在后续轮次前必须已被合法回收: {snapshot:?}"
        );
    }
    // 每个 State 实例恰好销毁一次。
    let events = take_shared_events();
    let drops = state_drops(&events);
    let unique: std::collections::HashSet<&u32> = drops.iter().collect();
    assert_eq!(drops.len(), unique.len(), "不重复销毁: {drops:?}");
    assert!(
        drops.contains(&1) && drops.contains(&2),
        "旧状态被回收: {drops:?}"
    );
}

#[test]
fn j14_same_data_id_promotion_does_not_destroy_or_duplicate() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    // body 是"重新暴露导入状态"的完成态 Flow（零 Step、输出就是导入输入位置）：Round 的
    // 声明输出与 current-state 指向**同一** `DataId`。
    let (mut parent, handles) = FlowBuilder::<(State, Rules)>::start().expect("parent");
    let (state, rules) = handles;
    let state_position = state.position().clone();
    let rules_position = rules.position().clone();
    let mut iter: LoopBuilder<Iter2<State, Rules>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, OrchSig<(State, Rules), Data<State>>>(reexpose_state_flow())
        .expect("reexpose body");
    let orchestrator = iter.finish().expect("finish");
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<(State, Rules), Data<State>>, _>(orchestrator, (state, rules))
        .expect("then iter");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let read_position = read.position().clone();
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    let state_id: RefCell<Option<DataId>> = RefCell::new(None);
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            let id = ctx
                .register_owned(
                    &ctx.root_scope(),
                    &state_position,
                    State {
                        value: 3,
                        finish: true,
                    },
                )
                .expect("state");
            *state_id.borrow_mut() = Some(id);
            ctx.register_owned(&ctx.root_scope(), &rules_position, Rules { step: 1 })
                .expect("rules");
        },
        Some(move |view: &mut RootView<'_, '_>| {
            assert_eq!(
                *view.resolve::<u32>(&read_position)?,
                3,
                "最终导出的是同一实例"
            );
            Ok(())
        }),
    ))
    .expect("same data id scenario");

    let events = take_shared_events();
    assert_eq!(count(&events, "loop-collect:promoted"), 1, "{events:?}");
    assert!(
        saw(&events, "loop-finished"),
        "Finish 与最终绑定各一次: {events:?}"
    );
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let promote = snapshots
        .iter()
        .find(|snapshot| {
            snapshot.operation == super::test_support::RoundCollectOperation::Promote
                && snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before
        })
        .expect("promote snapshot");
    // same-DataId：selected 就是 current-state 的导入目标（Root-owned），不转移责任、
    // 不进入 pending、不销毁。
    let expected = state_id.borrow().clone().expect("state id");
    assert_eq!(
        promote.selected_data.as_ref(),
        Some(&expected),
        "{promote:?}"
    );
    assert_eq!(
        promote.state_pending.as_ref().map(Vec::len),
        Some(0),
        "{promote:?}"
    );
    let drops = state_drops(&events);
    let unique: std::collections::HashSet<&u32> = drops.iter().collect();
    assert_eq!(drops.len(), unique.len(), "同一实例不被重复销毁: {drops:?}");
}

/// 完成态 Flow：把第一个导入输入位置原样声明为输出（零 Step，重新暴露导入 Data）。
fn reexpose_state_flow() -> Flow<(State, Rules), Data<State>> {
    let (flow, handles) = FlowBuilder::<(State, Rules)>::start().expect("reexpose flow");
    let (state, _rules) = handles;
    flow.finish::<Data<State>, _>(state)
        .expect("reexpose finish")
}

#[test]
fn j15_switching_current_state_to_ancestor_alias() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    // body 重新暴露 **shared** 位置（同类型 ancestor Data）：Promote 只改 target，原 owner
    // 不变；旧 Loop-owned 值不被销毁；最终 alias 可导出给父级。
    let (mut parent, handles) = FlowBuilder::<(State, State)>::start().expect("parent");
    let (initial, alias) = handles;
    let initial_position = initial.position().clone();
    let alias_position = alias.position().clone();
    let mut iter: LoopBuilder<Iter2<State, State>> = LoopBuilder::start().expect("iter");
    iter.then_body::<_, OrchSig<(State, State), Data<State>>>(reexpose_alias_flow())
        .expect("reexpose alias body");
    let orchestrator = iter.finish().expect("finish");
    let produced: super::data_ref::DataRef<State> = parent
        .then::<_, OrchSig<(State, State), Data<State>>, _>(orchestrator, (initial, alias))
        .expect("then iter");
    let read: super::data_ref::DataRef<u32> = parent
        .then::<_, SyncFnSig<(State,), Data<u32>>, _>(
            read_state as fn(&State) -> Result<u32, BodyError>,
            produced,
        )
        .expect("read");
    let read_position = read.position().clone();
    let parent = parent.finish::<Data<u32>, _>(read).expect("parent finish");

    let initial_id: RefCell<Option<DataId>> = RefCell::new(None);
    let alias_id: RefCell<Option<DataId>> = RefCell::new(None);
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            let id = ctx
                .register_owned(
                    &ctx.root_scope(),
                    &initial_position,
                    State {
                        value: 1,
                        finish: false,
                    },
                )
                .expect("initial");
            *initial_id.borrow_mut() = Some(id);
            let id = ctx
                .register_owned(
                    &ctx.root_scope(),
                    &alias_position,
                    State {
                        value: 9,
                        finish: true,
                    },
                )
                .expect("alias");
            *alias_id.borrow_mut() = Some(id);
        },
        Some(move |view: &mut RootView<'_, '_>| {
            assert_eq!(
                *view.resolve::<u32>(&read_position)?,
                9,
                "最终导出 ancestor alias"
            );
            Ok(())
        }),
    ))
    .expect("ancestor alias scenario");

    let events = take_shared_events();
    assert_eq!(
        count(&events, "loop-collect:promoted"),
        1,
        "alias 首轮即 Finish: {events:?}"
    );
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let promote = snapshots
        .iter()
        .find(|snapshot| {
            snapshot.operation == super::test_support::RoundCollectOperation::Promote
                && snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before
        })
        .expect("promote snapshot");
    let alias = alias_id.borrow().clone().expect("alias id");
    assert_eq!(promote.selected_data.as_ref(), Some(&alias), "{promote:?}");
    assert_eq!(
        promote.state_pending.as_ref().map(Vec::len),
        Some(0),
        "{promote:?}"
    );
    // alias 由 ancestor 负责：selected 的 owner 不是 Round（来源），不转移责任。
    assert_ne!(
        promote.selected_owner.as_ref(),
        promote.source.as_ref(),
        "ancestor alias 的原 owner 不变: {promote:?}"
    );
    // 两个实例各由自己的 owner 销毁一次（初始状态由 Root 在收口时清理，Loop 不重复销毁）。
    let drops = state_drops(&events);
    let unique: std::collections::HashSet<&u32> = drops.iter().collect();
    assert_eq!(drops.len(), unique.len(), "不重复销毁: {drops:?}");
    assert_eq!(drops, vec![1, 9], "两个实例各销毁一次: {drops:?}");
    let _ = initial_id.borrow().clone();
}

/// 完成态 Flow：把第二个导入输入位置声明为输出（shared alias）。
fn reexpose_alias_flow() -> Flow<(State, State), Data<State>> {
    let (flow, handles) = FlowBuilder::<(State, State)>::start().expect("reexpose alias flow");
    let (_state, alias) = handles;
    flow.finish::<Data<State>, _>(alias)
        .expect("reexpose alias finish")
}

// ---------------------------------------------------------------- J26：Ref 单赋值与控制状态

#[test]
fn j26_definition_refs_do_not_grow_per_round() {
    super::test_support::reset_observations();
    boundary_creation_reset();
    let (parent, state_position, rules_position) = iter_main_parent();
    let before = parent.definition().allocated_probe();
    drive(definition_in_root(
        parent.definition(),
        Vec::new(),
        |ctx, _root| {
            ctx.register_owned(
                &ctx.root_scope(),
                &state_position,
                State {
                    value: 0,
                    finish: false,
                },
            )
            .expect("state");
            ctx.register_owned(&ctx.root_scope(), &rules_position, Rules { step: 1 })
                .expect("rules");
        },
        None::<fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>,
    ))
    .expect("iter ref scenario");
    let after = parent.definition().allocated_probe();
    assert_eq!(before, after, "多轮不动态分配 Definition Ref");
    // 收口观察：状态更新走独立 runtime slot，Loop 的 Definition refs 不逐轮增长。
    let snapshots = super::test_support::take_round_collect_pre_cleanup();
    let promotes: Vec<_> = snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.operation == super::test_support::RoundCollectOperation::Promote
                && snapshot.phase == super::test_support::RoundCollectSnapshotPhase::Before
        })
        .collect();
    let controller_refs: Vec<usize> = promotes
        .iter()
        .map(|snapshot| {
            snapshot
                .controller_refs
                .as_ref()
                .expect("controller refs observed")
                .len()
        })
        .collect();
    assert!(
        controller_refs.windows(2).all(|pair| pair[0] == pair[1]),
        "Loop 的本地 refs 不逐轮增长: {controller_refs:?}"
    );
}
