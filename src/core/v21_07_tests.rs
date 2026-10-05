//! V21-07 验收样本（M01～M24）：Match 的真实 branch 调用与生命周期。
//!
//! 这些样本使用 crate 内可见的 Definition／Flow／Match／协议类型，不构成公开 API 承诺。
//! 编译负例（M13／M24）放在 `tests/ui/`，按真实 `src/core` 或已构建 rlib 独立编译。
//!
//! 共享 poll／gate／事件、真实创建点记录、Root 驱动与输入登记来自 [`super::test_support`]；
//! 业务夹具与调用计数留在本模块。

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

use super::builder::{CallSite, Definition, TypedCallBuilder, run_site};
use super::context::creation_counts;
use super::context::{BodyError, InvocationGuard, InvocationKind, TerminationKind};
use super::data_ref::DataRef;
use super::flow::{Flow, FlowBuilder};
use super::identity::{DataId, ScopeId};
use super::internal_error::ScopeError;
use super::match_orchestrator::{BranchStepKind, Match, MatchBuilder};
use super::node::NodeCall1;
use super::orchestrator::{OrchCall, ScopeRole};
use super::ref_id::{RefId, RefIdSource};
use super::runtime::RootExecution;
use super::scope::ScopeState;
use super::signature::{BuildError, Data, NodeFut, Out2, Unit};
use super::test_support::{
    RootInput, advance_to_pending, boundary_address_snapshot, boundary_creation_snapshot, drive,
    drive_pinned, export_attempt_snapshot, gate_wait, install_export_conflict, install_gate,
    match_failure_scope_snapshot, match_stage_snapshot, record, release_gate, root_input,
    run_definition_in_root, run_definition_plain, take_events, take_shared_events,
};

// ---- 业务夹具 ----

/// 非 Clone 的业务输入（带 Drop 见证，用于 ancestor 保留证据）。
#[derive(Debug, PartialEq)]
struct Seed(u32);

impl Drop for Seed {
    fn drop(&mut self) {
        record("seed-dropped");
    }
}

/// 非 Clone、非 Hash 的路由 key：只用 `PartialEq`／`Eq`。
#[derive(PartialEq, Eq)]
struct Route(u8);

/// 带 Drop 见证的业务值。
struct W(&'static str);

impl W {
    fn new(name: &'static str) -> Self {
        record(name);
        W(name)
    }
}

impl Drop for W {
    fn drop(&mut self) {
        record(&format!("{}-dropped", self.0));
    }
}

use super::test_support::{at, count, nth, saw};

/// 每个样本在起点重置 gate／事件／观测记录。
fn reset() {
    super::test_support::reset_observations();
}

// ---- 路由、branch 与后续 Step 夹具 ----

/// 路由 Node：`&Seed -> Route`（Seed % 4）。
fn route_of(seed: &Seed) -> Result<Route, BodyError> {
    record("route");
    Ok(Route((seed.0 % 4) as u8))
}

/// 一个只记录标签的见证 Node。
fn witness(label: &'static str) -> impl Fn(&Seed) -> Result<W, BodyError> {
    move |_seed: &Seed| Ok(W::new(label))
}

/// 把一个已产生的见证值改标签的 Node。
fn relabel(label: &'static str) -> impl Fn(&W) -> Result<W, BodyError> {
    move |_value: &W| Ok(W::new(label))
}

/// 同步函数 branch。
fn branch_sync(_seed: &Seed) -> Result<W, BodyError> {
    record("branch-sync");
    Ok(W::new("sync-out"))
}

/// 借用结束见证：持真实 `&A`，Drop 时记录借用结束（字段本身不读取，持有即证据）。
struct BorrowGuard<'a, A> {
    #[allow(dead_code)]
    borrow: &'a A,
}

impl<A> Drop for BorrowGuard<'_, A> {
    fn drop(&mut self) {
        record("borrow-end");
    }
}

/// 异步函数 branch（跨 Pending）：await 前持有真实借用，await 后实际读取输入。
async fn branch_async(seed: &Seed) -> Result<W, BodyError> {
    record("branch-async");
    let borrow = BorrowGuard { borrow: seed };
    gate_wait().await;
    let value = seed.0;
    drop(borrow);
    Ok(W::new(if value == 0 {
        "async-zero"
    } else {
        "async-out"
    }))
}

/// 结构体 Node branch。
struct BranchStruct;

impl NodeCall1<Seed, Data<W>> for BranchStruct {
    fn call<'a>(&'a self, _seed: &'a Seed) -> NodeFut<'a, W> {
        Box::pin(async move {
            record("branch-struct");
            Ok(W::new("struct-out"))
        })
    }
}

/// Arc 包装的结构体 Node branch。
struct BranchArc;

impl NodeCall1<Seed, Data<W>> for BranchArc {
    fn call<'a>(&'a self, _seed: &'a Seed) -> NodeFut<'a, W> {
        Box::pin(async move {
            record("branch-arc");
            Ok(W::new("arc-out"))
        })
    }
}

/// 持 `Rc` 的非 Send 结构体 Node branch（证明没有新增 Send bound）。
struct BranchRc(Rc<Cell<u32>>);

impl NodeCall1<Seed, Data<W>> for BranchRc {
    fn call<'a>(&'a self, _seed: &'a Seed) -> NodeFut<'a, W> {
        Box::pin(async move {
            let seen = self.0.get() + 1;
            self.0.set(seen);
            record("branch-rc");
            Ok(W::new("rc-out"))
        })
    }
}

/// 结构体 unit branch。
struct BranchUnit;

impl NodeCall1<Seed, Unit> for BranchUnit {
    fn call<'a>(&'a self, _seed: &'a Seed) -> NodeFut<'a, ()> {
        Box::pin(async move {
            record("branch-unit");
            Ok(())
        })
    }
}

/// 失败 branch（真实业务错误）。
fn branch_failing(_seed: &Seed) -> Result<W, BodyError> {
    record("branch-failing");
    Err(BodyError::new("branch business failure"))
}

/// 后续 Step：合并两个见证值。
fn later_combine(first: &W, second: &W) -> Result<W, BodyError> {
    record("later-combine");
    Ok(W::new(if first.0 == second.0 {
        "same"
    } else {
        "combined"
    }))
}

/// 失败的后续 Step（M20）。
fn later_failing(_first: &W, _second: &W) -> Result<W, BodyError> {
    record("later-failing");
    Err(BodyError::new("later step failure"))
}

/// 单输出的嵌套 Flow：`(W,) -> Data<W>`。
fn nested_flow(label: &'static str) -> Flow<(W,), Data<W>> {
    let (mut builder, value) = FlowBuilder::<(W,)>::start().expect("builder");
    let out: DataRef<W> = builder.then(relabel(label), value).expect("step");
    builder.finish(out).expect("finish")
}

/// 双输出完成态 Flow branch：两个新见证 + 一个未导出临时值；`nested` 时嵌一层 SubFlow。
fn flow_pair(label: &'static str, nested: bool) -> Flow<(Seed,), Out2<W, W>> {
    let temp_label: &'static str = match label {
        "a" => "a-temp",
        "b" => "b-temp",
        _ => "d-temp",
    };
    let first_label: &'static str = match label {
        "a" => "a-first",
        "b" => "b-first",
        _ => "d-first",
    };
    let second_label: &'static str = match label {
        "a" => "a-second",
        "b" => "b-second",
        _ => "d-second",
    };
    let (mut builder, seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    let temp: DataRef<W> = builder
        .then(witness(temp_label), seed.clone())
        .expect("temp");
    let first: DataRef<W> = if nested {
        builder
            .then(nested_flow(first_label), temp)
            .expect("nested call")
    } else {
        builder.then(relabel(first_label), temp).expect("first")
    };
    let second: DataRef<W> = builder.then(witness(second_label), seed).expect("second");
    builder.finish((first, second)).expect("finish")
}

/// 单输出完成态 Flow branch（供 Data<K> 的 Match 使用）。
fn flow_single(label: &'static str) -> Flow<(Seed,), Data<W>> {
    let (mut builder, seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    let out: DataRef<W> = builder.then(witness(label), seed).expect("step");
    builder.finish(out).expect("finish")
}

/// 零 Step passthrough Flow：`(Seed,) -> Data<Seed>`（把 imported A 原样再暴露）。
fn passthrough() -> Flow<(Seed,), Data<Seed>> {
    let (builder, seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    builder.finish(seed).expect("finish")
}

/// `&W -> ()` 的结构体 unit Node（普通 unit 函数会被构建期拒绝）。
struct UnitAfter;

impl NodeCall1<W, Unit> for UnitAfter {
    fn call<'a>(&'a self, _value: &'a W) -> NodeFut<'a, ()> {
        Box::pin(async move {
            record("unit-after");
            Ok(())
        })
    }
}

/// 单输出 unit Flow：产生一个未导出见证后显式 unit 收口。
fn flow_unit(label: &'static str) -> Flow<(Seed,), Unit> {
    let (mut builder, seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    let temp: DataRef<W> = builder.then(witness(label), seed).expect("temp");
    let done: () = builder.then(UnitAfter, temp).expect("unit");
    builder.finish(done).expect("finish")
}

// ---- Match 夹具 ----

/// `K = Out2<W, W>`：两个 Flow branch + default，可选嵌套 SubFlow。
fn pair_match(nested: bool) -> Match<Route, Seed, Out2<W, W>> {
    let mut builder = MatchBuilder::<Route, Seed, Out2<W, W>>::start().expect("match");
    builder
        .branch(Route(0), flow_pair("a", nested))
        .expect("branch a");
    builder
        .branch(Route(1), flow_pair("b", false))
        .expect("branch b");
    builder.default(flow_pair("d", false)).expect("default");
    builder.finish().expect("match finish")
}

/// `K = Data<W>`：四类 Node branch。
fn data_match() -> Match<Route, Seed, Data<W>> {
    let mut builder = MatchBuilder::<Route, Seed, Data<W>>::start().expect("match");
    builder.branch(Route(0), branch_sync).expect("sync");
    builder.branch(Route(1), branch_async).expect("async");
    builder.branch(Route(2), BranchStruct).expect("struct");
    builder.branch(Route(3), Arc::new(BranchArc)).expect("arc");
    builder.finish().expect("match finish")
}

/// `K = Unit`：结构体 unit branch + default unit Flow。
fn unit_match() -> Match<Route, Seed, Unit> {
    let mut builder = MatchBuilder::<Route, Seed, Unit>::start().expect("match");
    builder.branch(Route(0), BranchUnit).expect("unit branch");
    builder.finish().expect("match finish")
}

/// 只有一个 key 命中 branch 的 Match（`K = Data<W>`）。
fn single_match() -> Match<Route, Seed, Data<W>> {
    let mut builder = MatchBuilder::<Route, Seed, Data<W>>::start().expect("match");
    builder
        .branch(Route(0), flow_single("solo"))
        .expect("branch");
    builder.finish().expect("match finish")
}

/// 零 Step passthrough branch 的 Match：输出 imported `Seed`。
fn passthrough_match() -> Match<Route, Seed, Data<Seed>> {
    let mut builder = MatchBuilder::<Route, Seed, Data<Seed>>::start().expect("match");
    builder.branch(Route(0), passthrough()).expect("branch");
    builder.finish().expect("match finish")
}

// ---- 父 Flow 夹具 ----

/// 父 Flow：Router → Match → 后续合并 Step（`K = Out2<W, W>`）。
struct PairParent {
    flow: Flow<(Seed,), Data<W>>,
    seed: DataRef<Seed>,
    route: DataRef<Route>,
    first: DataRef<W>,
    second: DataRef<W>,
    later: DataRef<W>,
}

impl PairParent {
    fn build(
        matched: &Match<Route, Seed, Out2<W, W>>,
        later: fn(&W, &W) -> Result<W, BodyError>,
    ) -> Self {
        let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
        let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
        let (first, second): (DataRef<W>, DataRef<W>) = parent
            .then(matched.clone(), (route.clone(), seed.clone()))
            .expect("match step");
        let later_out: DataRef<W> = parent
            .then(later, (first.clone(), second.clone()))
            .expect("later step");
        let flow = parent.finish(later_out.clone()).expect("finish");
        Self {
            flow,
            seed,
            route,
            first,
            second,
            later: later_out,
        }
    }

    /// Match Step（下标 1）的调用点。
    fn match_site(&self) -> &CallSite {
        self.flow.definition().steps()[1].site()
    }
}

/// 单输出父 Flow：Router → Match(`Data<W>`) → 后续无。
struct SingleParent {
    flow: Flow<(Seed,), Data<W>>,
    seed: DataRef<Seed>,
    route: DataRef<Route>,
    out: DataRef<W>,
}

impl SingleParent {
    fn build(matched: &Match<Route, Seed, Data<W>>) -> Self {
        let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
        let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
        let out: DataRef<W> = parent
            .then(matched.clone(), (route.clone(), seed.clone()))
            .expect("match step");
        let flow = parent.finish(out.clone()).expect("finish");
        Self {
            flow,
            seed,
            route,
            out,
        }
    }

    fn match_site(&self) -> &CallSite {
        self.flow.definition().steps()[1].site()
    }
}

// ---- M01：完成态与共同 Signature ----

#[test]
fn m01_completed_signatures_and_unit_positions() {
    reset();
    // 三种共同输出分类各自完成，端口数量与分类一致；Unit 零位置。
    assert_eq!(unit_match().definition().output_ports().len(), 0);
    assert_eq!(single_match().definition().output_ports().len(), 1);
    assert_eq!(pair_match(false).definition().output_ports().len(), 2);
    // 声明输入只有 R／A 两个位置，顺序为 (R, A)。
    let matched = pair_match(false);
    let inputs = matched.definition().inputs();
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0].expected(), std::any::TypeId::of::<Route>());
    assert_eq!(inputs[1].expected(), std::any::TypeId::of::<Seed>());

    // Unit Match 接入父 Flow：只运行 unit branch，不产生业务 Data。
    let matched = unit_match();
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let done: () = parent
        .then(matched.clone(), (route, seed))
        .expect("unit match step");
    let flow = parent.finish(done).expect("finish");
    let unit_seed = seed_from(&flow, &Seed(0));
    let outcome = run_definition_plain(flow.definition(), vec![root_input(&unit_seed, Seed(0))]);
    let events = take_events();
    assert!(outcome.is_ok(), "{outcome:?}");
    assert!(saw(&events, "branch-unit"), "{events:?}");
    assert!(!saw(&events, "branch-unit-dropped"), "{events:?}");
}

/// 父 Flow 的声明输入位置（取第一个输入）。
fn seed_from<K: super::signature::OutKind>(
    flow: &Flow<(Seed,), K>,
    _value: &Seed,
) -> DataRef<Seed> {
    let position = flow.definition().inputs()[0].position().clone();
    DataRef::from_position(position)
}

// ---- M02：四类 Node branch 与 unit 函数拒绝 ----

#[test]
fn m02_four_node_kinds_run_by_key_and_unit_functions_are_rejected() {
    reset();
    let matched = data_match();
    let shared = Rc::new(Cell::new(0));
    let parent = SingleParent::build(&matched);
    // seed % 4 依次命中 0／1／2／3 四条不同 branch。
    for (value, expected) in [
        (Seed(4), "sync-out"),
        (Seed(5), "async-out"),
        (Seed(6), "struct-out"),
        (Seed(7), "arc-out"),
    ] {
        reset();
        let out_position = parent.out.position().clone();
        let outcome = run_definition_in_root(
            parent.flow.definition(),
            vec![root_input(&parent.seed, Seed(value.0))],
            move |view| {
                let observed = view.resolve::<W>(&out_position)?;
                assert_eq!(observed.0, expected);
                Ok(())
            },
        );
        assert!(outcome.is_ok(), "{outcome:?}");
        let events = take_events();
        assert_eq!(count(&events, "route"), 1, "{events:?}");
        assert_eq!(count(&events, "branch-sync"), usize::from(value.0 % 4 == 0));
        assert_eq!(
            count(&events, "branch-async"),
            usize::from(value.0 % 4 == 1)
        );
        assert_eq!(
            count(&events, "branch-struct"),
            usize::from(value.0 % 4 == 2)
        );
        assert_eq!(count(&events, "branch-arc"), usize::from(value.0 % 4 == 3));
    }

    // 非 Send 结构体 branch 在没有 Send bound 的前提下正常执行。
    let parent = SingleParent::build(&{
        let mut builder = MatchBuilder::<Route, Seed, Data<W>>::start().expect("match");
        builder.branch(Route(0), branch_sync).expect("sync");
        builder
            .branch(Route(2), BranchRc(Rc::clone(&shared)))
            .expect("rc");
        builder.finish().expect("finish")
    });
    reset();
    let outcome = run_definition_plain(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(6))],
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "branch-rc"), 1, "{events:?}");
    assert_eq!(shared.get(), 1);

    // 普通 unit 函数在 typed 边界（顶层 K = Data<()>）被构建期拒绝，且登记状态不变。
    let mut builder = MatchBuilder::<Route, Seed, Data<()>>::start().expect("match");
    fn ordinary_unit(_seed: &Seed) -> Result<(), BodyError> {
        Ok(())
    }
    let before = builder.registered_probe();
    let before_allocated = builder.allocated_probe();
    let rejected = builder.branch(Route(1), ordinary_unit);
    assert_eq!(rejected, Err(BuildError::UnsupportedFunctionUnitOutput));
    assert_eq!(builder.registered_probe(), before);
    assert_eq!(builder.allocated_probe(), before_allocated);
    // 该 K 的 Data<()> 声明在完成时也被拒绝（unit 业务类型不产生业务 Data）。
    assert_eq!(
        builder.finish().err(),
        Some(BuildError::UnitDataOutputNotSupported)
    );

    // 结构体 unit Node 是合法 unit branch（显式 Unit 分类），失败后仍可继续登记并完成。
    let mut builder = MatchBuilder::<Route, Seed, Unit>::start().expect("match");
    builder
        .branch(Route(0), BranchUnit)
        .expect("struct unit ok");
    builder.branch(Route(1), BranchUnit).expect("retry ok");
    assert!(builder.finish().is_ok());
}
// ---- 共用观测驱动 ----

/// 一次真实调用点驱动的观测结果：结果、Root 只读快照与首次终止诊断。
struct StepOutcome {
    result: Result<(), BodyError>,
    refs: Vec<(RefId, DataId)>,
    owned: Vec<DataId>,
    termination: Option<(
        TerminationKind,
        &'static str,
        Option<ScopeId>,
        Option<String>,
    )>,
}

/// 在 Root frame 中驱动一个调用点，并在失败后立即读取 Root 只读快照与首次诊断。
fn drive_step_observing(inputs: Vec<RootInput>, site: &CallSite) -> StepOutcome {
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    for (position, register) in inputs {
        register(execution.context_mut(), &position);
    }
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let result = drive(async { run_site(&mut guard, &root, site).await });
    let (refs, owned) = guard.snapshot_probe(&root).expect("root snapshot");
    let termination = guard.termination().map(|termination| {
        (
            termination.kind(),
            termination.note(),
            termination.scope().cloned(),
            termination
                .scope_error()
                .map(|diagnostic| format!("{diagnostic}")),
        )
    });
    drop(guard);
    drop(execution);
    StepOutcome {
        result,
        refs,
        owned,
        termination,
    }
}

/// 某个位置是否在快照里绑定。
fn bound(refs: &[(RefId, DataId)], position: &RefId) -> bool {
    refs.iter().any(|(candidate, _)| candidate == position)
}

// ---- 额外夹具 ----

/// 混合双输出 Flow branch：`(Seed,) -> Out2<W, u32>`。
fn flow_pair_mixed(label: &'static str) -> Flow<(Seed,), Out2<W, u32>> {
    let (mut builder, seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    let value: DataRef<W> = builder.then(witness(label), seed.clone()).expect("value");
    let number: DataRef<u32> = builder.then(seed_count, seed).expect("number");
    builder.finish((value, number)).expect("finish")
}

/// `&Seed -> u32`。
fn seed_count(seed: &Seed) -> Result<u32, BodyError> {
    Ok(seed.0)
}

/// 单个 tuple Data 的 Node：`&Seed -> (W, W)`（一位 Data，不拆位）。
fn tuple_node(_seed: &Seed) -> Result<(W, W), BodyError> {
    record("tuple-node");
    Ok((W::new("tuple-1"), W::new("tuple-2")))
}

/// 失败的 Flow branch：内部 Step 直接返回业务错误。
fn flow_failing(label: &'static str) -> Flow<(Seed,), Data<W>> {
    let (mut builder, seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    let out: DataRef<W> = builder.then(branch_failing, seed).expect("failing step");
    let _ = label;
    builder.finish(out).expect("finish")
}

// ---- M03：Flow branch 与双路径 ----

#[test]
fn m03_branch_call_site_kinds_and_parent_composition() {
    reset();
    let matched = pair_match(false);
    assert_eq!(matched.sites_probe().len(), 3);
    // 每个替代的执行面都是 branch 包装调用边界；包装内部那一次真实调用决定类别。
    assert!(
        matched
            .sites_probe()
            .iter()
            .all(|site| matches!(site, CallSite::Orchestrator(_))),
        "branch 包装本身是 Orchestrator 调用点"
    );
    assert_eq!(
        matched.branch_steps_probe(),
        vec![
            BranchStepKind::Orchestrator,
            BranchStepKind::Orchestrator,
            BranchStepKind::Orchestrator
        ],
        "Flow branch 的真实调用是 CallSite::Orchestrator"
    );
    let node_match = data_match();
    assert_eq!(
        node_match.branch_steps_probe(),
        vec![
            BranchStepKind::Node,
            BranchStepKind::Node,
            BranchStepKind::Node,
            BranchStepKind::Node
        ],
        "Node branch 的真实调用是 CallSite::Node"
    );

    // 父 Flow 的同一个 typed then 依次接入 Node（路由）、Match、Node（后步）与 Flow child。
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let (first, second): (DataRef<W>, DataRef<W>) = parent
        .then(matched.clone(), (route, seed.clone()))
        .expect("match step");
    let combined: DataRef<W> = parent
        .then(later_combine, (first, second))
        .expect("later step");
    let tail: DataRef<W> = parent
        .then(nested_flow("tail-out"), combined)
        .expect("flow child");
    let flow = parent.finish(tail).expect("finish");

    let outcome = run_definition_plain(flow.definition(), vec![root_input(&seed, Seed(4))]);
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert!(saw(&events, "tail-out"), "{events:?}");
    let creations = boundary_creation_snapshot();
    let roles: Vec<ScopeRole> = creations.iter().map(|(_, _, role)| *role).collect();
    // Match 调用、被选 branch 包装、branch 内 Flow child、以及父 Flow 的尾部 Flow child。
    assert_eq!(
        roles,
        vec![
            ScopeRole::Match,
            ScopeRole::Branch,
            ScopeRole::Flow,
            ScopeRole::Flow
        ],
        "{events:?}"
    );
    // parent 链：Match→Root，其余逐级挂在真实调用边界下。
    assert_eq!(creations[0].1.seq(), 0);
    assert_eq!(creations[1].1, creations[0].0);
    assert_eq!(creations[2].1, creations[1].0);
    assert_eq!(creations[3].1.seq(), 0);
}

// ---- M04：只选一个 branch ----

#[test]
fn m04_only_the_selected_branch_creates_scopes_and_runs() {
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_combine);
    // Seed(4) % 4 == 0 → 命中 Route(0) 的 branch "a"。
    let outcome = run_definition_plain(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(4))],
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    for selected in ["a-temp", "a-first", "a-second", "later-combine"] {
        assert_eq!(count(&events, selected), 1, "{events:?}");
    }
    for unselected in [
        "b-temp", "b-first", "b-second", "d-temp", "d-first", "d-second",
    ] {
        assert_eq!(count(&events, unselected), 0, "{events:?}");
    }
    let creations = boundary_creation_snapshot();
    assert_eq!(
        creations
            .iter()
            .map(|(_, _, role)| *role)
            .collect::<Vec<_>>(),
        vec![ScopeRole::Match, ScopeRole::Branch, ScopeRole::Flow],
        "只应建立 MatchScope、被选 BranchScope 与其 Flow child"
    );
    assert_eq!(creations[0].2, ScopeRole::Match);
    assert_eq!(creations[1].2, ScopeRole::Branch);
    // parent 链：Match 的 parent 是本执行的 RootScope（seq 0），Branch 的 parent 是 MatchScope。
    assert_eq!(creations[0].1.seq(), 0);
    assert_eq!(creations[1].1, creations[0].0);
    // 本调用区间 ScopeId.seq() 连续增量：Root 0 → Match 1 → Branch 2 → Flow 3（无其它创建）。
    assert_eq!(creations[0].0.seq(), 1);
    assert_eq!(creations[1].0.seq(), 2);
    assert_eq!(creations[2].0.seq(), 3);
    // 后步只等待被选分支：默认分支与另一 branch 都未运行（上面逐项为 0）。
    assert_eq!(count(&events, "later-combine"), 1, "{events:?}");
}

// ---- M05：default 与无匹配 ----

#[test]
fn m05_default_prefers_key_then_default_and_empty_table_reports_unbound_ports() {
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_combine);

    // 命中 key：只运行匹配 branch，不运行 default。
    let outcome = run_definition_plain(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(5))],
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "b-first"), 1, "{events:?}");
    assert_eq!(count(&events, "d-first"), 0, "{events:?}");

    // 未命中但有 default：只运行 default。
    reset();
    let outcome = run_definition_plain(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(6))],
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "d-first"), 1, "{events:?}");
    assert_eq!(count(&events, "a-first"), 0, "{events:?}");
    assert_eq!(count(&events, "b-first"), 0, "{events:?}");

    // 空登记表 + 非空共同 K：真实无匹配错误；共同端口在失败时刻仍未绑定，父输出未提交，后步为零。
    reset();
    let empty = MatchBuilder::<Route, Seed, Out2<W, W>>::start()
        .expect("match")
        .finish()
        .expect("empty match completes");
    let parent = PairParent::build(&empty, later_combine);
    let outcome = drive_step_observing(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
    );
    let error = outcome
        .result
        .expect_err("empty match is an execution error");
    assert_eq!(
        error.note(),
        "match has no branch for the route and no default"
    );
    assert_eq!(
        outcome.termination.as_ref().map(|entry| entry.0),
        Some(TerminationKind::BodyError)
    );
    assert_eq!(
        outcome.termination.as_ref().map(|entry| entry.1),
        Some("match has no branch for the route and no default")
    );
    // 真实失败时刻的 MatchScope 快照：没有任何共同端口被绑定，也没有 Match-owned 业务值。
    let snapshots = match_failure_scope_snapshot();
    assert_eq!(snapshots.len(), 1, "真实 Match body 记录了一次失败时刻快照");
    let (_fail_scope, refs, owned) = &snapshots[0];
    let common: Vec<RefId> = empty
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    assert!(
        !refs.iter().any(|(position, _)| common.contains(position)),
        "共同端口未自行绑定: {refs:?}"
    );
    assert!(owned.is_empty(), "Match 未自行产生业务值: {owned:?}");
    // caller（Root）的 Match 输出位置在失败后仍未绑定，父 Flow 后续 Step 未运行。
    assert!(!bound(&outcome.refs, parent.first.position()));
    assert!(!bound(&outcome.refs, parent.second.position()));
    assert!(!bound(&outcome.refs, parent.later.position()));
    assert_eq!(outcome.owned.len(), 2, "Root 只对两个预置输入负责");
    let events = take_events();
    assert_eq!(count(&events, "later-combine"), 0, "{events:?}");

    // 空表 + Data K：同样的失败时刻证据（无匹配诊断、共同端口未绑定、caller 未提交、后步零）。
    reset();
    let empty = MatchBuilder::<Route, Seed, Data<W>>::start()
        .expect("match")
        .finish()
        .expect("empty data match");
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let produced: DataRef<W> = parent
        .then(empty.clone(), (route.clone(), seed.clone()))
        .expect("empty match step");
    let after: DataRef<W> = parent
        .then(nested_flow("after-empty"), produced.clone())
        .expect("later step");
    let flow = parent.finish(after).expect("finish");
    let outcome = drive_step_observing(
        vec![root_input(&seed, Seed(4)), root_input(&route, Route(0))],
        flow.definition().steps()[1].site(),
    );
    let error = outcome
        .result
        .expect_err("empty data match is an execution error");
    assert_eq!(
        error.note(),
        "match has no branch for the route and no default"
    );
    let snapshots = match_failure_scope_snapshot();
    assert_eq!(snapshots.len(), 1);
    let (_fail_scope, refs, owned) = &snapshots[0];
    let common: Vec<RefId> = empty
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    assert_eq!(common.len(), 1);
    assert!(
        !refs.iter().any(|(position, _)| common.contains(position)),
        "Data K 的共同端口未自行绑定: {refs:?}"
    );
    assert!(owned.is_empty(), "Match 未自行产生业务值: {owned:?}");
    assert!(!bound(&outcome.refs, produced.position()));
    let events = take_events();
    assert_eq!(count(&events, "after-empty"), 0, "{events:?}");
    let _ = flow;
}

// ---- M06：选中错误不回退 ----

#[test]
fn m06_selected_failure_propagates_without_fallback() {
    reset();
    let mut builder = MatchBuilder::<Route, Seed, Data<W>>::start().expect("match");
    builder
        .branch(Route(0), branch_failing)
        .expect("failing node branch");
    builder
        .branch(Route(1), flow_single("ok-branch"))
        .expect("ok branch");
    builder
        .default(flow_single("default-branch"))
        .expect("default");
    let matched = builder.finish().expect("finish");
    let parent = SingleParent::build(&matched);
    let outcome = drive_step_observing(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
    );
    let error = outcome
        .result
        .as_ref()
        .expect_err("selected branch error propagates");
    println!(
        "ERR: note={} scope={:?}",
        error.note(),
        error.scope_error().map(|e| format!("{e}"))
    );
    assert_eq!(error.note(), "branch business failure", "保留原错误");
    let events = take_events();
    assert_eq!(count(&events, "branch-failing"), 1, "{events:?}");
    assert_eq!(count(&events, "ok-branch"), 0, "{events:?}");
    assert_eq!(count(&events, "default-branch"), 0, "{events:?}");
    assert!(
        !bound(&outcome.refs, parent.out.position()),
        "没有正常输出提交"
    );
    // 首次诊断保留实际失败 Scope（被选 branch 的 BranchScope）。
    let creations = boundary_creation_snapshot();
    let branch_scope = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Branch)
        .map(|(scope, _, _)| scope.clone())
        .expect("branch scope exists");
    assert_eq!(
        outcome
            .termination
            .as_ref()
            .and_then(|entry| entry.2.clone()),
        Some(branch_scope)
    );

    // Flow branch 内部的真实 Step 错误同样传播，不改选 default。
    reset();
    let mut builder = MatchBuilder::<Route, Seed, Data<W>>::start().expect("match");
    builder
        .branch(Route(0), flow_failing("flow-fail"))
        .expect("flow branch");
    builder
        .default(flow_single("default-branch"))
        .expect("default");
    let matched = builder.finish().expect("finish");
    let parent = SingleParent::build(&matched);
    let outcome = drive_step_observing(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
    );
    assert_eq!(
        outcome
            .result
            .expect_err("flow branch error propagates")
            .note(),
        "branch business failure"
    );
    let events = take_events();
    assert_eq!(count(&events, "default-branch"), 0, "{events:?}");
    assert!(!bound(&outcome.refs, parent.out.position()));
}

// ---- M07：复杂判断由 Node，Match 只做 lookup ----

#[test]
fn m07_route_comes_from_a_node_and_inputs_are_not_moved() {
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_combine);
    let seed_position = parent.seed.position().clone();
    let seed_id = {
        // 先在一次真实执行中取得 Root 输入的 DataId，再核对同一次执行中它仍然归 Root 所有。
        let observed = std::cell::RefCell::new(None);
        let outcome = run_definition_in_root(
            parent.flow.definition(),
            vec![root_input(&parent.seed, Seed(5))],
            |view| {
                let snapshot = view.snapshot()?;
                let id = snapshot
                    .iter()
                    .find(|(position, _)| position == &seed_position)
                    .map(|(_, id)| id.clone())
                    .expect("seed is bound");
                // 路由与业务输入都是 Root-owned：Match 只导入 target，不复制／move 业务值。
                assert_eq!(view.probe().owner_probe(&id)?, view.root().clone());
                *observed.borrow_mut() = Some(id);
                Ok(())
            },
        );
        assert!(outcome.is_ok(), "{outcome:?}");
        observed.into_inner().expect("seed data id")
    };
    let events = take_events();
    assert_eq!(count(&events, "route"), 1, "{events:?}");
    // Seed（非 Clone）与 Route（非 Clone、非 Hash）都只在借用下参与：数据身份未被替换。
    assert!(seed_id.seq() < 64, "单次执行内的身份序号空间有限");
    assert_eq!(count(&events, "b-first"), 1, "{events:?}");
}

// ---- M08：Unit／单 Data／异构双输出 ----

#[test]
fn m08_unit_single_tuple_and_heterogeneous_outputs() {
    reset();
    // Unit：Flow branch 产生未导出临时值后显式 unit 收口；不产生业务 Data。
    let mut builder = MatchBuilder::<Route, Seed, Unit>::start().expect("match");
    builder
        .branch(Route(0), flow_unit("unit-temp"))
        .expect("unit branch");
    let matched = builder.finish().expect("finish");
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let done: () = parent
        .then(matched.clone(), (route, seed.clone()))
        .expect("unit match step");
    let flow = parent.finish(done).expect("finish");
    let observed = std::cell::RefCell::new(0usize);
    let outcome = run_definition_in_root(
        flow.definition(),
        vec![root_input(&seed, Seed(4))],
        |view| {
            let (_, owned) = view.snapshot_full()?;
            *observed.borrow_mut() = owned.len();
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "unit-temp"), 1, "{events:?}");
    assert_eq!(count(&events, "unit-temp-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "unit-after"), 1, "{events:?}");
    // Root 只负责业务输入与真正产生 Data 的 Step：Seed 输入 + Router 的 Route 输出共 2 个；
    // unit 收口（Match 的 Unit 输出与 unit-after）都不产生 DataId。
    assert_eq!(observed.into_inner(), 2);

    // 单个 tuple Data：一位位置，不拆成两位。
    reset();
    let mut builder = MatchBuilder::<Route, Seed, Data<(W, W)>>::start().expect("match");
    builder.branch(Route(0), tuple_node).expect("tuple branch");
    let matched = builder.finish().expect("finish");
    assert_eq!(matched.definition().output_ports().len(), 1);
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let pair: DataRef<(W, W)> = parent
        .then(matched.clone(), (route, seed.clone()))
        .expect("tuple match step");
    let names: DataRef<usize> = parent.then(tuple_len, pair.clone()).expect("read tuple");
    let flow = parent.finish(names).expect("finish");
    let outcome = run_definition_in_root(
        flow.definition(),
        vec![root_input(&seed, Seed(4))],
        |view| {
            assert_eq!(*view.resolve::<usize>(&_names_position(&flow))?, 2);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "tuple-node"), 1, "{events:?}");

    // 异构双输出：Out2<W, u32> 两个位置类型不同。
    reset();
    let mut builder = MatchBuilder::<Route, Seed, Out2<W, u32>>::start().expect("match");
    builder
        .branch(Route(0), flow_pair_mixed("mixed"))
        .expect("mixed branch");
    let matched = builder.finish().expect("finish");
    assert_eq!(matched.definition().output_ports().len(), 2);
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let (value, number): (DataRef<W>, DataRef<u32>) = parent
        .then(matched.clone(), (route, seed.clone()))
        .expect("mixed match step");
    let sum: DataRef<u32> = parent
        .then(mixed_sum, (number.clone(), value.clone()))
        .expect("read");
    let flow = parent.finish(sum).expect("finish");
    let outcome = run_definition_in_root(
        flow.definition(),
        vec![root_input(&seed, Seed(4))],
        |view| {
            // 4 + "mixed".len() == 9
            assert_eq!(*view.resolve::<u32>(&_sum_position(&flow))?, 9);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "mixed"), 1, "{events:?}");
}

/// 读取 tuple 长度。
fn tuple_len(pair: &(W, W)) -> Result<usize, BodyError> {
    record("tuple-len");
    Ok(usize::from(!pair.0.0.is_empty()) + usize::from(!pair.1.0.is_empty()))
}

/// 混合输出的读取 Step：`(&u32, &W) -> u32`。
fn mixed_sum(number: &u32, value: &W) -> Result<u32, BodyError> {
    record("mixed-sum");
    Ok(*number + value.0.len() as u32)
}

/// 取父 Flow 第一个输出位置（`Data<usize>`）。
fn _names_position(flow: &Flow<(Seed,), Data<usize>>) -> RefId {
    flow.definition().output_ports()[0].position().clone()
}

/// 取父 Flow 第一个输出位置（`Data<u32>`）。
fn _sum_position(flow: &Flow<(Seed,), Data<u32>>) -> RefId {
    flow.definition().output_ports()[0].position().clone()
}

// ---- M09：Scope import／新输出责任转移 ----

#[test]
fn m09_scope_chain_ownership_and_later_steps() {
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_combine);
    let first_position = parent.first.position().clone();
    let second_position = parent.second.position().clone();
    let mut observed_ids = Vec::new();
    let outcome = run_definition_in_root(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(4))],
        |view| {
            let (refs, owned) = view.snapshot_full()?;
            for position in [&first_position, &second_position] {
                let id = refs
                    .iter()
                    .find(|(candidate, _)| candidate == position)
                    .map(|(_, id)| id.clone())
                    .expect("Match output is bound");
                assert!(owned.contains(&id), "Match 输出由 Root 负责");
                assert_eq!(view.probe().owner_probe(&id)?, view.root().clone());
                observed_ids.push(id);
            }
            // 本地引用：Seed（ancestor-owned）仍然可读。
            let seed_position = parent.seed.position().clone();
            let _ = view.resolve::<Seed>(&seed_position)?;
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(observed_ids.len(), 2);
    assert_ne!(observed_ids[0], observed_ids[1], "两个输出是不同 DataId");
    let events = take_events();
    assert_eq!(count(&events, "a-temp-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "a-first-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "a-second-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "later-combine"), 1, "{events:?}");
    // 真实 parent 链与实际 Scope 状态：Match 与 Branch 都已关闭。
    let creations = boundary_creation_snapshot();
    assert_eq!(
        creations
            .iter()
            .map(|(_, _, role)| *role)
            .collect::<Vec<_>>(),
        vec![ScopeRole::Match, ScopeRole::Branch, ScopeRole::Flow]
    );
    assert_eq!(creations[1].1, creations[0].0);
    assert_eq!(creations[2].1, creations[1].0);
}

// ---- M10：imported 输出与别名规则 ----

#[test]
fn m10_imported_passthrough_keeps_owner_and_alias_forms() {
    reset();
    // 零 Step passthrough Flow branch：把 imported A 原样再暴露，不产生新 DataId。
    let matched = passthrough_match();
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let exposed: DataRef<Seed> = parent
        .then(matched.clone(), (route, seed.clone()))
        .expect("passthrough match step");
    let flow = parent.finish(exposed.clone()).expect("finish");
    let seed_position = seed.position().clone();
    let exposed_position = exposed.position().clone();
    let observed = std::cell::RefCell::new((None, None));
    let outcome = run_definition_in_root(
        flow.definition(),
        vec![root_input(&seed, Seed(4))],
        |view| {
            let snapshot = view.snapshot()?;
            let seed_id = snapshot
                .iter()
                .find(|(position, _)| position == &seed_position)
                .map(|(_, id)| id.clone())
                .expect("seed bound");
            let exposed_id = snapshot
                .iter()
                .find(|(position, _)| position == &exposed_position)
                .map(|(_, id)| id.clone())
                .expect("passthrough output bound");
            // imported alias：两个位置指向同一个完整 DataId，owner 仍为 Root。
            assert_eq!(seed_id, exposed_id);
            assert_eq!(view.probe().owner_probe(&seed_id)?, view.root().clone());
            *observed.borrow_mut() = (Some(seed_id), Some(exposed_id));
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let (seed_id, exposed_id) = observed.into_inner();
    assert!(seed_id.is_some() && seed_id == exposed_id);
    let events = take_events();
    // 导入目标不产生第二份所有权：Root 清理时 Seed 只被销毁一次，且没有额外业务值产生。
    assert_eq!(count(&events, "seed-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "two"), 0, "{events:?}");
    // 合法双输出 imported alias：branch 把唯一业务输入同时接到 child 的两个输入位置，
    // 因此两个输出端口指向同一完整 DataId（输出 RefId 互异）。
    reset();
    let (child, (left, right)) = FlowBuilder::<(Seed, Seed)>::start().expect("child");
    let child: Flow<(Seed, Seed), Out2<Seed, Seed>> =
        child.finish((left, right)).expect("child finish");
    let (mut branch, input) = FlowBuilder::<(Seed,)>::start().expect("branch");
    let outputs: (DataRef<Seed>, DataRef<Seed>) = branch
        .then(child, (input.clone(), input))
        .expect("fanout call");
    let wrapper: Flow<(Seed,), Out2<Seed, Seed>> = branch.finish(outputs).expect("branch finish");
    let mut builder = MatchBuilder::<Route, Seed, Out2<Seed, Seed>>::start().expect("match");
    builder
        .register_wrapper_probe(Route(0), wrapper)
        .expect("alias branch");
    let matched = builder.finish().expect("finish");
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let (first, second): (DataRef<Seed>, DataRef<Seed>) = parent
        .then(matched.clone(), (route, seed.clone()))
        .expect("alias match step");
    let first_position = first.position().clone();
    let second_position = second.position().clone();
    assert_ne!(first_position, second_position, "两个输出 RefId 互异");
    let flow = parent.finish((first, second)).expect("finish");
    let seed_position = seed.position().clone();
    let alias = std::cell::RefCell::new(None);
    let outcome = run_definition_in_root(
        flow.definition(),
        vec![root_input(&seed, Seed(40))],
        |view| {
            let snapshot = view.snapshot()?;
            let id_of = |position: &RefId| {
                snapshot
                    .iter()
                    .find(|(candidate, _)| candidate == position)
                    .map(|(_, id)| id.clone())
                    .expect("position bound")
            };
            let seed_id = id_of(&seed_position);
            let first_id = id_of(&first_position);
            let second_id = id_of(&second_position);
            // 两项指向同一完整 DataId，输出位置互异，ancestor owner 唯一。
            assert_eq!(first_id, second_id);
            assert_eq!(first_id, seed_id);
            assert_eq!(view.probe().owner_probe(&seed_id)?, view.root().clone());
            assert_eq!(
                *view.resolve::<Seed>(&first_position)?,
                *view.resolve::<Seed>(&second_position)?
            );
            *alias.borrow_mut() = Some(seed_id);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    assert!(alias.into_inner().is_some());
    let events = take_events();
    // 两层 Export 之后可读；最终业务值（imported ancestor）只 Drop 一次。
    assert_eq!(count(&events, "seed-dropped"), 1, "{events:?}");

    // 构建期结构证据：同一位置不能在两个输出端口上各选一次（两个本地 Ref 指向同一位置）。
    let (mut two, (left, right)) = FlowBuilder::<(Seed, Seed)>::start().expect("builder");
    let draft = two
        .then(
            |_a: &Seed, _b: &Seed| -> Result<W, BodyError> { Ok(W::new("two")) },
            (left.clone(), right.clone()),
        )
        .expect("two input step");
    assert_eq!(
        two.finish((draft.clone(), draft.clone())).err(),
        Some(BuildError::DuplicateOutputPosition(
            draft.position().clone()
        )),
        "同一位置不能在两处被选为输出端口"
    );
}

// ---- M11：定义复用与共同位置单赋值 ----

#[test]
fn m11_definition_reuse_across_calls_and_executions() {
    reset();
    let matched = pair_match(false);
    let definition_ports: Vec<RefId> = matched
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    assert_eq!(matched.sites_probe().len(), 3);

    let mut captured = Vec::new();
    for (value, expected) in [(Seed(4), "a-first"), (Seed(5), "b-first")] {
        reset();
        let parent = PairParent::build(&matched, later_combine);
        let first_position = parent.first.position().clone();
        let second_position = parent.second.position().clone();
        let seen = std::cell::RefCell::new(Vec::new());
        let outcome = run_definition_in_root(
            parent.flow.definition(),
            vec![root_input(&parent.seed, Seed(value.0))],
            |view| {
                let (refs, _) = view.snapshot_full()?;
                for position in [&first_position, &second_position] {
                    let id = refs
                        .iter()
                        .find(|(candidate, _)| candidate == position)
                        .map(|(_, id)| id.clone())
                        .expect("match output bound");
                    seen.borrow_mut().push(id);
                }
                // 共同端口在本 Scope 中各自只被绑定一次：位置序列唯一。
                let mut positions: Vec<RefId> =
                    refs.iter().map(|(position, _)| position.clone()).collect();
                let unique = positions.len();
                positions.sort_by_key(|position| position.seq());
                positions.dedup_by_key(|position| position.seq());
                assert_eq!(positions.len(), unique, "本地位置单赋值");
                Ok(())
            },
        );
        assert!(outcome.is_ok(), "{outcome:?}");
        let events = take_events();
        assert_eq!(count(&events, expected), 1, "{events:?}");
        let creations = boundary_creation_snapshot();
        captured.push((
            creations
                .iter()
                .map(|(scope, _, _)| scope.clone())
                .collect::<Vec<ScopeId>>(),
            seen.into_inner(),
        ));
    }
    // 两次 Execution 得到不同的 Scope 与 DataId；定义与共同端口完全不变。
    assert_ne!(captured[0].0[0], captured[1].0[0]);
    assert_ne!(captured[0].1[0], captured[1].1[0]);
    assert_ne!(captured[0].1[1], captured[1].1[1]);
    assert_eq!(
        matched
            .definition()
            .output_ports()
            .iter()
            .map(|port| port.position().clone())
            .collect::<Vec<RefId>>(),
        definition_ports
    );
    assert_eq!(matched.sites_probe().len(), 3);
}

// ---- M12：重复 key／default 拒绝 ----

#[test]
fn m12_duplicate_key_and_default_are_rejected_without_mutation() {
    reset();
    let mut builder = MatchBuilder::<Route, Seed, Data<W>>::start().expect("match");
    builder.branch(Route(0), branch_sync).expect("first branch");
    let registered = builder.registered_probe();
    let allocated = builder.allocated_probe();
    // 重复 key：明确错误，原条目不被覆盖，登记数量与 Match 分配器不变。
    assert_eq!(
        builder.branch(Route(0), branch_failing),
        Err(BuildError::DuplicateBranchKey)
    );
    assert_eq!(builder.registered_probe(), registered);
    assert_eq!(builder.allocated_probe(), allocated);
    // 合法登记不增加 Match Definition 的分配计数（包装使用自己的来源）。
    builder
        .branch(Route(1), branch_async)
        .expect("second branch");
    assert_eq!(builder.allocated_probe(), allocated);
    // 第二个 default：明确错误。
    builder.default(flow_single("d1")).expect("first default");
    assert_eq!(
        builder.default(flow_single("d2")),
        Err(BuildError::SecondDefault)
    );
    assert_eq!(builder.registered_probe(), registered + 2);
    let matched = builder.finish().expect("finish");
    // 成功对照：Route(0) 仍选中原 branch（未被重复登记覆盖），default 也未提前运行。
    let parent = SingleParent::build(&matched);
    reset();
    let outcome = run_definition_plain(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(4))],
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "branch-sync"), 1, "{events:?}");
    assert_eq!(count(&events, "d1"), 0, "{events:?}");
    reset();
    let outcome = run_definition_plain(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(2))],
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "d1"), 1, "{events:?}");
}

// ---- M13：擦除后内部声明／pack 不一致 ----

#[test]
fn m13_erased_pack_mismatch_is_rejected_before_the_body() {
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_combine);
    let route_position = parent.route.position().clone();
    // 在真实 Match 调用点注入错误输入 pack（声明是双输入，注入单输入 pack）。
    match parent.match_site() {
        CallSite::Orchestrator(site) => {
            site.inject_pack_probe(super::orchestrator::probe_targets1::<Route>(route_position));
        }
        CallSite::Node(_) => panic!("Match step must be an orchestrator call site"),
    }
    let outcome = drive_step_observing(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
    );
    let error = outcome
        .result
        .expect_err("erased pack mismatch is rejected");
    assert_eq!(error.note(), "orchestrator input pack type mismatch");
    assert_eq!(
        outcome.termination.as_ref().map(|entry| entry.1),
        Some("orchestrator input pack type mismatch")
    );
    // 业务体之前拒绝：没有任何 branch 被调用，也没有 BranchScope。
    let events = take_events();
    assert_eq!(count(&events, "a-temp"), 0, "{events:?}");
    assert_eq!(count(&events, "b-temp"), 0, "{events:?}");
    assert_eq!(count(&events, "d-temp"), 0, "{events:?}");
    assert!(
        boundary_creation_snapshot()
            .iter()
            .all(|(_, _, role)| *role == ScopeRole::Match),
        "只建立了 MatchScope"
    );
}

// ---- M14：共同输出装配与原子构建 ----

#[test]
fn m14_common_port_assembly_is_single_allocation_and_outside_declared() {
    reset();
    let mut builder = MatchBuilder::<Route, Seed, Out2<W, W>>::start().expect("match");
    let after_inputs = builder.allocated_probe();
    assert_eq!(after_inputs, 2, "只有 R／A 两个输入位置被分配");
    builder
        .branch(Route(0), flow_pair("a", false))
        .expect("branch");
    assert_eq!(
        builder.allocated_probe(),
        after_inputs,
        "登记 branch 不消耗 Match 序号"
    );
    let matched = builder.finish().expect("finish");
    // 一次整组分配：Out2 两位。
    assert_eq!(matched.definition().output_ports().len(), 2);
    // 共同端口只进入 output_ports：不进 declared()／steps()。
    let declared: Vec<RefId> = matched
        .definition()
        .declared()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    assert_eq!(declared.len(), 2, "declared() 只有 R／A");
    assert!(
        matched.definition().steps().is_empty(),
        "Match Definition 没有 Step"
    );
    for port in matched.definition().output_ports() {
        assert!(
            !declared.contains(port.position()),
            "共同端口不在 declared()"
        );
    }
    for (index, entry) in matched.sites_probe().iter().enumerate() {
        if let CallSite::Orchestrator(site) = entry {
            // 每个替代的输出都映射到同一组共同位置。
            assert_eq!(
                site.outputs(),
                matched
                    .definition()
                    .output_ports()
                    .iter()
                    .map(|p| p.position().clone())
                    .collect::<Vec<RefId>>()
                    .as_slice()
            );
            assert_eq!(site.inputs().len(), 1);
        }
        let _ = index;
    }

    // 整组耗尽：共同端口分配失败在登记之前发生，端口与序号都无部分变化。
    // 完成期整组校验：首项类型合法、后项声明类型错 → 在任何端口分配之前拒绝。
    let source = RefIdSource::with_start(0);
    let mut builder = MatchBuilder::<Route, Seed, Out2<W, W>>::with_source(Arc::clone(&source));
    builder
        .branch(Route(0), flow_pair("a", false))
        .expect("branch");
    let legal: Vec<super::signature::DeclaredPort> = builder.branch_ports_probe(0).to_vec();
    assert_eq!(legal.len(), 2, "首项与后项都在登记证据里");
    let mut wrong = legal;
    wrong[1] = super::signature::DeclaredPort::new::<u32>(wrong[1].position().clone());
    builder.override_branch_ports_probe(0, wrong);
    assert_eq!(
        builder.finish().err(),
        Some(BuildError::BranchOutputType {
            branch: 0,
            position: 1,
            expected: std::any::type_name::<W>(),
            actual: std::any::type_name::<u32>(),
        }),
        "后项声明类型错必须在完成期整组拒绝"
    );
    // 拒绝发生在共同端口分配之前：来源只走了 R／A 两个输入位置。
    assert_eq!(
        super::ref_id::RefIdAllocator::new(source).next_probe(),
        2,
        "完成失败不发布共同端口、不消耗序号"
    );

    // 整组耗尽：输入之后 next = MAX - 1 —— 单项可分配、双项整组失败且不消耗序号。
    let single_source = RefIdSource::with_start(u64::MAX - 3);
    let single = MatchBuilder::<Route, Seed, Data<W>>::with_source(Arc::clone(&single_source));
    assert_eq!(
        single.allocated_probe(),
        u64::MAX - 1,
        "两个输入位置之后 next = MAX - 1"
    );
    assert!(single.finish().is_ok(), "单项端口仍可分配");
    assert_eq!(
        super::ref_id::RefIdAllocator::new(Arc::clone(&single_source)).next_probe(),
        u64::MAX,
        "单项分配消耗一个序号"
    );
    let pair_source = RefIdSource::with_start(u64::MAX - 3);
    let pair = MatchBuilder::<Route, Seed, Out2<W, W>>::with_source(Arc::clone(&pair_source));
    assert_eq!(
        pair.finish().err(),
        Some(BuildError::OutputPositionExhausted),
        "双项整组失败"
    );
    assert_eq!(
        super::ref_id::RefIdAllocator::new(pair_source).next_probe(),
        u64::MAX - 1,
        "整组失败不消耗剩余序号"
    );

    // Flow 的完成校验完全不变：把 Match 共同端口当作 Flow 输出会被拒绝（外来位置）。
    let foreign = matched.definition().output_ports()[0].position().clone();
    let alias = DataRef::<W>::from_position(foreign.clone());
    let (flow_builder, _seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    assert_eq!(
        flow_builder.finish(alias).err(),
        Some(BuildError::ForeignPosition(foreign))
    );
}

// ---- M15：受控选择调用边界 ----

/// 注入后的 Match 必须在建立 BranchScope 之前以指定不变量拒绝，且不运行任何业务体。
fn assert_metadata_rejected(matched: Match<Route, Seed, Out2<W, W>>, violated: &'static str) {
    assert_metadata_rejected_without_body(matched, violated, "a-temp")
}

/// 同上，并额外核对某个被注入 branch 的业务体事件为 0。
fn assert_metadata_rejected_without_body(
    matched: Match<Route, Seed, Out2<W, W>>,
    violated: &'static str,
    forbidden: &str,
) {
    reset();
    let parent = PairParent::build(&matched, later_combine);
    let outcome = drive_step_observing(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
    );
    let error = outcome.result.expect_err("metadata mismatch is rejected");
    match error.scope_error() {
        Some(ScopeError::Invariant { violated: actual }) => assert_eq!(*actual, violated),
        other => panic!("expected an internal invariant, got {other:?}"),
    }
    let events = take_events();
    assert_eq!(count(&events, forbidden), 0, "业务体之前拒绝: {events:?}");
    assert!(
        boundary_creation_snapshot()
            .iter()
            .all(|(_, _, role)| *role == ScopeRole::Match),
        "只建立 MatchScope，没有建立 BranchScope"
    );
    assert!(
        match_stage_snapshot().is_empty(),
        "拒绝发生在 body 之前，Match body 没有进入选择执行阶段"
    );
}

#[test]
fn m15_controlled_selection_rejects_foreign_and_mismatched_metadata() {
    reset();
    let wrapper = flow_pair("probe", false);
    let wrapper2 = flow_pair("probe", false);

    // 1) caller 输入不是本 Match 的 A 位置：用**同一个**登记包装的副本重建 site
    //    （child Definition 身份相同），只改 caller 元数据。
    let injected = pair_match(false);
    let route_position = injected.definition().inputs()[0].position().clone();
    let common: Vec<RefId> = injected
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let injected = injected.with_replaced_site_probe(
        0,
        CallSite::Orchestrator(Box::new(super::orchestrator::OrchSite::<
            Flow<(Seed,), Out2<W, W>>,
            (Seed,),
            Out2<W, W>,
        >::new(
            wrapper.clone(),
            vec![route_position],
            common.clone(),
            ScopeRole::Branch,
        ))),
    );
    assert_metadata_rejected(
        injected,
        "registered alternative input is not the match branch input position",
    );

    // 2) caller 输出不是共同端口（少一位），child 身份仍相同。
    let injected = pair_match(false);
    let common: Vec<RefId> = injected
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let a_position = injected.definition().inputs()[1].position().clone();
    let _ = &a_position;
    let injected = injected.with_replaced_site_probe(
        0,
        CallSite::Orchestrator(Box::new(super::orchestrator::OrchSite::<
            Flow<(Seed,), Out2<W, W>>,
            (Seed,),
            Out2<W, W>,
        >::new(
            wrapper.clone(),
            vec![a_position],
            common[..1].to_vec(),
            ScopeRole::Branch,
        ))),
    );
    assert_metadata_rejected(
        injected,
        "registered alternative output is not the common match output ports",
    );

    // 3) 构建期端口证据与实际对象不一致（声明端口被替换为空）。
    let injected = pair_match(false).with_ports_probe(0, Vec::new());
    assert_metadata_rejected(
        injected,
        "registered alternative metadata does not match the invoked branch",
    );

    // 4) 端口证据来自另一来源（类型相同、完整 RefId 不同）。
    let other = pair_match(false);
    let foreign_ports = other.definition().output_ports().to_vec();
    let injected = pair_match(false).with_ports_probe(0, foreign_ports);
    assert_metadata_rejected(
        injected,
        "registered alternative metadata does not match the invoked branch",
    );

    // 5) 实际被调用对象的输出类型与共同 K 不符（另一来源、错类型 child）。
    let injected = pair_match(false);
    let a_position = injected.definition().inputs()[1].position().clone();
    let common: Vec<RefId> = injected
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let (mut wrong, a) = FlowBuilder::<(Seed,)>::start().expect("wrong builder");
    let wrong_first: DataRef<W> = wrong
        .then(witness("wrong-first"), a.clone())
        .expect("first");
    let wrong_second: DataRef<u32> = wrong.then(seed_count, a).expect("second");
    let wrong: Flow<(Seed,), Out2<W, u32>> =
        wrong.finish((wrong_first, wrong_second)).expect("finish");
    let injected = injected.with_replaced_site_probe(
        0,
        CallSite::Orchestrator(Box::new(super::orchestrator::OrchSite::<
            Flow<(Seed,), Out2<W, u32>>,
            (Seed,),
            Out2<W, u32>,
        >::new(
            wrong,
            vec![a_position],
            common,
            ScopeRole::Branch,
        ))),
    );
    assert_metadata_rejected_without_body(
        injected,
        "registered alternative is not the registered branch definition",
        "wrong-first",
    );

    // 6) **同类型、不同来源**的 child：输入／输出与端口类型完全一致，仍是外来 Definition。
    let injected = pair_match(false);
    let a_position = injected.definition().inputs()[1].position().clone();
    let common: Vec<RefId> = injected
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let injected = injected.with_replaced_site_probe(
        0,
        CallSite::Orchestrator(Box::new(super::orchestrator::OrchSite::<
            Flow<(Seed,), Out2<W, W>>,
            (Seed,),
            Out2<W, W>,
        >::new(
            wrapper2.clone(),
            vec![a_position],
            common,
            ScopeRole::Branch,
        ))),
    );
    assert_metadata_rejected_without_body(
        injected,
        "registered alternative is not the registered branch definition",
        "probe",
    );

    // 7) 外来 caller 输入位置（另一 Match 的来源）同样被拒绝。
    let injected = pair_match(false);
    let common: Vec<RefId> = injected
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let other = pair_match(false);
    let foreign_input = other.definition().inputs()[1].position().clone();
    let injected = injected.with_replaced_site_probe(
        0,
        CallSite::Orchestrator(Box::new(super::orchestrator::OrchSite::<
            Flow<(Seed,), Out2<W, W>>,
            (Seed,),
            Out2<W, W>,
        >::new(
            wrapper.clone(),
            vec![foreign_input],
            common,
            ScopeRole::Branch,
        ))),
    );
    assert_metadata_rejected(
        injected,
        "registered alternative input is not the match branch input position",
    );
}

/// 只调用受控入口、不做任何选择的测试编排体（验证越界为内部不变量）。
struct IndexProbe {
    definition: Definition,
}

impl OrchCall<(Route, Seed), Data<W>> for IndexProbe {
    type Pack = super::orchestrator::Targets2<Route, Seed>;

    fn definition(&self) -> &Definition {
        &self.definition
    }

    fn run<'a>(
        &'a self,
        mut scope: super::orchestrator::OrchScope<'a, Self::Pack, Data<W>>,
    ) -> NodeFut<'a, ()> {
        Box::pin(async move { scope.run_registered_site(0).await })
    }
}

#[test]
fn m15_index_out_of_range_is_an_internal_invariant() {
    reset();
    let mut definition = Definition::new();
    let _route = definition.declare_input::<Route>("route").expect("route");
    let declared_seed = definition.declare_input::<Seed>("seed").expect("seed");
    let produced: DataRef<W> = definition
        .then(witness("probe-step"), declared_seed)
        .expect("probe step");
    definition
        .declare_output_port_for(&produced, "out")
        .expect("probe output port");
    let probe = IndexProbe { definition };
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let out: DataRef<W> = parent
        .then(probe, (route, seed.clone()))
        .expect("probe step");
    let flow = parent.finish(out).expect("finish");
    let outcome = run_definition_plain(flow.definition(), vec![root_input(&seed, Seed(4))]);
    let error = outcome.expect_err("out-of-range index is rejected");
    assert!(matches!(
        error.scope_error(),
        Some(ScopeError::Invariant {
            violated: "registered alternative index is out of range"
        })
    ));
}

// ---- M16：顺序与跨 Pending 借用 ----

#[test]
fn m16_ordered_pending_and_borrow_across_await() {
    reset();
    install_gate();
    let matched = data_match();
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let route: DataRef<Route> = parent.then(route_of, seed.clone()).expect("router");
    let out: DataRef<W> = parent
        .then(matched.clone(), (route, seed.clone()))
        .expect("match step");
    let tail: DataRef<W> = parent.then(nested_flow("tail"), out).expect("later step");
    let flow = parent.finish(tail).expect("finish");
    let inputs = vec![root_input(&seed, Seed(5))];
    // 同一 Future：先在 gate 上 Pending，父 Flow 后步必须仍为 0。
    let boxed = advance_to_pending(
        super::test_support::definition_in_root::<
            fn(&mut super::context::ExecutionContext, &ScopeId),
            fn(&mut super::test_support::RootView<'_, '_>) -> Result<(), BodyError>,
        >(flow.definition(), inputs, |_, _| {}, None),
        1,
    );
    let pending_events = take_events();
    assert_eq!(
        count(&pending_events, "branch-async"),
        1,
        "{pending_events:?}"
    );
    assert_eq!(
        count(&pending_events, "tail"),
        0,
        "Pending 期间父 Flow 后步为零"
    );
    // 借用结束后才提交：release 后同一 Future 到 Ready，后步读取 branch 输出。
    release_gate();
    let outcome = drive_pinned(boxed);
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "tail"), 1, "{events:?}");
    // 复用真实 &Seed 的局部见证：借用结束恰好一次，且先于输出登记／后步。
    assert_eq!(count(&events, "borrow-end"), 1, "{events:?}");
    assert!(
        at(&events, "borrow-end") < at(&events, "tail"),
        "{events:?}"
    );
    assert!(
        at(&events, "borrow-end") < at(&events, "async-out"),
        "{events:?}"
    );
}

// ---- 取消夹具 ----

/// 被选 branch：先产生一个 owned 见证，再由持借用的异步 Node 跨 Pending。
fn flow_pair_gated() -> Flow<(Seed,), Out2<W, W>> {
    let (mut builder, seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    let first: DataRef<W> = builder
        .then(witness("g-first"), seed.clone())
        .expect("first");
    let second: DataRef<W> = builder.then(branch_async, seed).expect("pending step");
    builder.finish((first, second)).expect("finish")
}

/// 只有一个 gated branch 的 Match。
fn gated_match() -> Match<Route, Seed, Out2<W, W>> {
    let mut builder = MatchBuilder::<Route, Seed, Out2<W, W>>::start().expect("match");
    builder
        .branch(Route(0), flow_pair_gated())
        .expect("gated branch");
    builder.finish().expect("finish")
}

// ---- M17：Branch 出口后项失败 ----

#[test]
fn m17_branch_export_failure_rejects_the_whole_group() {
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_combine);
    // 在真实 Match body 内预占第二个共同端口：Export 仍是真实 OrchSite→finalize 路径。
    install_export_conflict(1);
    let outcome = drive_step_observing(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
    );
    let error = outcome
        .result
        .expect_err("branch export is rejected as a whole");
    assert_eq!(error.note(), "scope operation failed");
    assert!(matches!(
        error.scope_error(),
        Some(ScopeError::RefAlreadyBound { .. })
    ));
    // 提交前责任观察：branch 的两个新 owned 输出是不同 DataId，都曾在 branch 名下。
    let branch_scope = boundary_creation_snapshot()
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Branch)
        .map(|(scope, _, _)| scope.clone())
        .expect("branch scope recorded");
    let attempts = export_attempt_snapshot();
    let (child, refs, owned) = attempts
        .iter()
        .find(|(scope, _, _)| *scope == branch_scope)
        .expect("the branch boundary records its pre-commit state");
    assert_eq!(owned.len(), 2, "两个新 owned 输出: {owned:?}");
    assert_ne!(owned[0], owned[1]);
    for id in owned {
        assert!(
            refs.iter().any(|(_, candidate)| candidate == id),
            "两个输出在 child 本地都有位置: {refs:?}"
        );
    }
    // 真正接受该次 Export 的 MatchScope：拒绝前后完整 refs／owned 逐项不变（无部分绑定／转移）。
    let stages = match_stage_snapshot();
    let before = stages
        .iter()
        .find(|(stage, _, _, _)| *stage == "before-branch")
        .expect("before-branch 阶段快照存在");
    let after = stages
        .iter()
        .find(|(stage, _, _, _)| *stage == "after-branch")
        .expect("after-branch 阶段快照存在");
    assert_eq!(
        before.2, after.2,
        "MatchScope 绑定无部分改变: {before:?} / {after:?}"
    );
    assert_eq!(
        before.3, after.3,
        "MatchScope 责任无部分改变: {before:?} / {after:?}"
    );
    // 两项 branch 输出从未出现在 MatchScope 的绑定或责任里。
    for (_, _, _, owned) in [before, after] {
        for id in owned {
            assert!(!bound(&outcome.refs, parent.first.position()) || id != &outcome.refs[0].1);
        }
    }
    assert!(
        !after
            .2
            .iter()
            .any(|(_, id)| *id == owned[0] || *id == owned[1]),
        "被拒绝的 branch 输出未绑定到 MatchScope: {after:?}"
    );
    assert!(
        !after.3.iter().any(|id| *id == owned[0] || *id == owned[1]),
        "被拒绝的 branch 责任未转移给 MatchScope: {after:?}"
    );
    // 整组拒绝：caller（Root）的输出位置都没有绑定。
    assert!(!bound(&outcome.refs, parent.first.position()));
    assert!(!bound(&outcome.refs, parent.second.position()));
    assert!(!bound(&outcome.refs, parent.later.position()));
    assert_eq!(outcome.owned.len(), 2, "Root 只对两个预置输入负责");
    let events = take_events();
    assert_eq!(count(&events, "a-temp-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "a-first-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "a-second-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "later-combine"), 0, "{events:?}");
    let _ = child;
    // 没有半份 child：Match 与 Branch 都按正常边界退出。
    let roles: Vec<ScopeRole> = boundary_creation_snapshot()
        .iter()
        .map(|(_, _, role)| *role)
        .collect();
    assert_eq!(
        roles,
        vec![ScopeRole::Match, ScopeRole::Branch, ScopeRole::Flow]
    );
}

// ---- M18：Match 出口后项失败 ----

#[test]
fn m18_match_export_failure_keeps_caller_untouched() {
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_combine);
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, parent.seed.position(), Seed(4))
        .expect("seed");
    execution
        .context_mut()
        .register_owned(&root, parent.route.position(), Route(0))
        .expect("route");
    // 预占 caller 的后项输出位置：Branch 已成功关闭，失败发生在 MatchScope→caller 出口。
    execution
        .context_mut()
        .register_owned(&root, parent.second.position(), 0u8)
        .expect("preoccupied caller port");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    // 故障预占完成后的 caller 完整快照（refs + owned）。
    let caller_before = guard.snapshot_probe(&root).expect("root snapshot");
    let result = drive(async { run_site(&mut guard, &root, parent.match_site()).await });
    let caller_after = guard.snapshot_probe(&root).expect("root snapshot");
    let error = result.expect_err("match export is rejected");
    assert_eq!(error.note(), "scope operation failed");
    assert!(matches!(
        error.scope_error(),
        Some(ScopeError::RefAlreadyBound { .. })
    ));
    // caller 完整快照前后逐项不变：只有预占值，没有 Match 输出被转移。
    assert_eq!(caller_before, caller_after, "caller 完整快照不变");
    assert!(!bound(&caller_after.0, parent.first.position()));
    assert!(bound(&caller_after.0, parent.second.position()));
    assert!(!bound(&caller_after.0, parent.later.position()));
    // Branch 已成功关闭，两项新 owned 在失败时由 Match 负责，随后整组拒绝未转给 caller。
    let stages = match_stage_snapshot();
    let body_ok = stages
        .iter()
        .find(|(stage, _, _, _)| *stage == "body-ok")
        .expect("body-ok 阶段快照存在");
    assert_eq!(
        body_ok.3.len(),
        2,
        "两项新 owned 归 MatchScope: {body_ok:?}"
    );
    for id in &body_ok.3 {
        assert!(!caller_after.1.contains(id), "失败后仍未移交 caller");
        assert!(
            !caller_after.0.iter().any(|(_, candidate)| candidate == id),
            "失败后 caller 没有指向它们"
        );
    }
    let branch_scope = boundary_creation_snapshot()
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Branch)
        .map(|(scope, _, _)| scope.clone())
        .expect("branch scope recorded");
    assert!(
        matches!(guard.state(&branch_scope), Ok(ScopeState::Closed)),
        "Branch 已成功关闭"
    );
    let events = take_events();
    assert_eq!(count(&events, "a-first-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "a-second-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "a-temp-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "later-combine"), 0, "{events:?}");
    assert_eq!(count(&events, "seed-dropped"), 0, "ancestor 仍归 Root");
}

// ---- M19：body／input 失败与不可恢复 ----

/// 期望被拒绝位置所在的 Scope（用真实创建记录定位，不写死身份）。
enum ExpectedScope {
    /// 由真实创建记录按角色定位的 child Scope。
    Created(ScopeRole),
    /// 上述记录的直接 parent（例如 RootScope）。
    ParentOfCreated(ScopeRole),
}

/// 期望的首次 Scope 诊断。
enum ExpectedScopeError {
    /// 业务错误／pack 类失败：首次诊断不带 Scope 诊断。
    None,
    /// 两层 Export 拒绝：`RefAlreadyBound`，并保存实际冲突位置与所在 Scope。
    RefAlreadyBound {
        position: RefId,
        scope: ExpectedScope,
    },
}

/// 按期望定位实际 ScopeId。
fn expected_scope_id(scope: &ExpectedScope) -> ScopeId {
    let creations = boundary_creation_snapshot();
    match scope {
        ExpectedScope::Created(role) => creations
            .iter()
            .find(|(_, _, candidate)| candidate == role)
            .map(|(found, _, _)| found.clone())
            .expect("预期的 child Scope 出现在真实创建记录里"),
        ExpectedScope::ParentOfCreated(role) => creations
            .iter()
            .find(|(_, _, candidate)| candidate == role)
            .map(|(_, parent, _)| parent.clone())
            .expect("预期的 child Scope 及其 parent 出现在真实创建记录里"),
    }
}

/// 期望的首次终止诊断。
struct ExpectedDiagnosis {
    kind: TerminationKind,
    note: &'static str,
    role: ScopeRole,
    /// 被选 branch 的 body 在首次失败前允许运行的次数。
    body_runs: usize,
    scope_error: ExpectedScopeError,
}

/// 读取当前 guard 的四字段首次诊断。
#[allow(clippy::type_complexity)]
fn diagnosis(
    guard: &InvocationGuard<'_>,
) -> Option<(
    TerminationKind,
    &'static str,
    Option<ScopeId>,
    Option<ScopeError>,
)> {
    guard.termination().map(|termination| {
        (
            termination.kind(),
            termination.note(),
            termination.scope().cloned(),
            termination.scope_error().cloned(),
        )
    })
}

/// 四字段诊断的字符串视图（用于"后续不被覆盖"的比较；首次诊断本身用类型化视图核对）。
#[allow(clippy::type_complexity)]
fn diagnosis_text(
    guard: &InvocationGuard<'_>,
) -> Option<(
    TerminationKind,
    &'static str,
    Option<ScopeId>,
    Option<String>,
)> {
    guard.termination().map(|termination| {
        (
            termination.kind(),
            termination.note(),
            termination.scope().cloned(),
            termination
                .scope_error()
                .map(|diagnostic| format!("{diagnostic}")),
        )
    })
}

/// 在真实 Root frame 中驱动一个调用点：核对首次诊断的四字段与实际 Scope 角色，
/// 并证明终止后不能再执行业务、普通 commit 被拒、诊断不被覆盖、body 不新增。
fn drive_and_assert_unrecoverable(
    inputs: Vec<RootInput>,
    site: &CallSite,
    expected: ExpectedDiagnosis,
    branch_event: &str,
) {
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    for (position, register) in inputs {
        register(execution.context_mut(), &position);
    }
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let first = drive(async { run_site(&mut guard, &root, site).await });
    assert!(first.is_err(), "调用必须失败");
    let before = diagnosis(&guard).expect("首次诊断必须存在");
    let before_text = diagnosis_text(&guard);
    assert_eq!(before.0, expected.kind, "终止类别");
    assert_eq!(before.1, expected.note, "首次原因");
    let recorded = before.2.clone().expect("首次诊断必须带实际 Scope");
    let role = boundary_creation_snapshot()
        .iter()
        .find(|(scope, _, _)| *scope == recorded)
        .map(|(_, _, role)| *role)
        .expect("诊断 Scope 是真实调用边界建立的 Scope");
    assert_eq!(role, expected.role, "首次诊断的实际 Scope 角色");
    // 首次 ScopeError 必须正确：两类 Export 拒绝保存 RefAlreadyBound 与实际冲突位置，
    // 业务／pack 类失败不带 Scope 诊断（不能只比较"后续不变"）。
    match (&before.3, &expected.scope_error) {
        (None, ExpectedScopeError::None) => {}
        (
            Some(ScopeError::RefAlreadyBound { scope, position }),
            ExpectedScopeError::RefAlreadyBound {
                position: expected_position,
                scope: expected_scope,
            },
        ) => {
            assert_eq!(position, expected_position, "首次诊断保存实际冲突位置");
            assert_eq!(
                scope,
                &expected_scope_id(expected_scope),
                "首次诊断保存实际冲突位置所在 Scope"
            );
        }
        (observed, _) => panic!("首次 ScopeError 不符: {observed:?}"),
    }
    let events = take_events();
    assert_eq!(
        count(&events, branch_event),
        expected.body_runs,
        "{events:?}"
    );
    // 终止后：业务调用被拒、普通 commit 被拒、四字段不被覆盖、body 不新增。
    let again = drive(async { run_site(&mut guard, &root, site).await });
    assert!(again.is_err(), "终止后不能再执行普通业务调用");
    assert!(
        guard.finalize(&root, &[], &mut Vec::new()).is_err(),
        "终止后普通 commit 必须被拒绝"
    );
    assert_eq!(diagnosis_text(&guard), before_text);
    let events_after = take_events();
    assert_eq!(
        count(&events_after, branch_event),
        0,
        "拒绝路径不新增业务体: {events_after:?}"
    );
}

#[test]
fn m19_failure_classes_are_reported_once_and_are_not_recoverable() {
    // 1) 无匹配：空登记表 + 非空 K。
    reset();
    let empty = MatchBuilder::<Route, Seed, Out2<W, W>>::start()
        .expect("match")
        .finish()
        .expect("empty match");
    let parent = PairParent::build(&empty, later_combine);
    drive_and_assert_unrecoverable(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
        ExpectedDiagnosis {
            kind: TerminationKind::BodyError,
            note: "match has no branch for the route and no default",
            role: ScopeRole::Match,
            body_runs: 0,
            scope_error: ExpectedScopeError::None,
        },
        "a-temp",
    );

    // 2) 输入 pack 装配拒绝（擦除后元数据不一致，业务体之前）。
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_combine);
    let route_position = parent.route.position().clone();
    match parent.match_site() {
        CallSite::Orchestrator(site) => {
            site.inject_pack_probe(super::orchestrator::probe_targets1::<Route>(route_position));
        }
        CallSite::Node(_) => panic!("Match step must be an orchestrator call site"),
    }
    drive_and_assert_unrecoverable(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
        ExpectedDiagnosis {
            kind: TerminationKind::BodyError,
            note: "orchestrator input pack type mismatch",
            role: ScopeRole::Match,
            body_runs: 0,
            scope_error: ExpectedScopeError::None,
        },
        "a-temp",
    );

    // 3) 被选 Node branch 的真实业务错误（实际 Scope 是 BranchScope）。
    reset();
    let mut builder = MatchBuilder::<Route, Seed, Data<W>>::start().expect("match");
    builder
        .branch(Route(0), branch_failing)
        .expect("failing node branch");
    builder
        .default(flow_single("default-branch"))
        .expect("default");
    let matched = builder.finish().expect("finish");
    let parent = SingleParent::build(&matched);
    drive_and_assert_unrecoverable(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
        ExpectedDiagnosis {
            kind: TerminationKind::BodyError,
            note: "branch business failure",
            role: ScopeRole::Branch,
            body_runs: 1,
            scope_error: ExpectedScopeError::None,
        },
        "branch-failing",
    );

    // 4) 被选 Flow branch 内部 Step 的真实业务错误（实际 Scope 是 Flow child）。
    reset();
    let mut builder = MatchBuilder::<Route, Seed, Data<W>>::start().expect("match");
    builder
        .branch(Route(0), flow_failing("flow-fail"))
        .expect("flow branch");
    builder
        .default(flow_single("default-branch"))
        .expect("default");
    let matched = builder.finish().expect("finish");
    let parent = SingleParent::build(&matched);
    drive_and_assert_unrecoverable(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
        ExpectedDiagnosis {
            kind: TerminationKind::BodyError,
            note: "branch business failure",
            role: ScopeRole::Flow,
            body_runs: 1,
            scope_error: ExpectedScopeError::None,
        },
        "branch-failing",
    );

    // 5) Branch→Match 出口整组拒绝（真实 OrchSite→finalize）。
    reset();
    let matched = pair_match(false);
    let conflict_position = matched.definition().output_ports()[1].position().clone();
    let parent = PairParent::build(&matched, later_combine);
    install_export_conflict(1);
    drive_and_assert_unrecoverable(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
        ],
        parent.match_site(),
        ExpectedDiagnosis {
            kind: TerminationKind::BodyError,
            note: "scope operation failed",
            role: ScopeRole::Branch,
            body_runs: 1,
            scope_error: ExpectedScopeError::RefAlreadyBound {
                position: conflict_position,
                scope: ExpectedScope::Created(ScopeRole::Match),
            },
        },
        "a-temp",
    );

    // 6) Match→caller 出口整组拒绝（真实上层边界 finalize；caller 后项被预占）。
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_combine);
    drive_and_assert_unrecoverable(
        vec![
            root_input(&parent.seed, Seed(4)),
            root_input(&parent.route, Route(0)),
            root_input(&parent.second, W::new("prebound")),
        ],
        parent.match_site(),
        ExpectedDiagnosis {
            kind: TerminationKind::BodyError,
            note: "scope operation failed",
            role: ScopeRole::Match,
            body_runs: 1,
            scope_error: ExpectedScopeError::RefAlreadyBound {
                position: parent.second.position().clone(),
                scope: ExpectedScope::ParentOfCreated(ScopeRole::Match),
            },
        },
        "a-temp",
    );
}

// ---- M20：later parent 失败 ----

#[test]
fn m20_later_parent_failure_cleans_its_own_values() {
    reset();
    let matched = pair_match(false);
    let parent = PairParent::build(&matched, later_failing);
    let outcome = run_definition_plain(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(4))],
    );
    let error = outcome.expect_err("later step fails");
    assert_eq!(error.note(), "later step failure");
    let events = take_events();
    assert_eq!(count(&events, "later-failing"), 1, "{events:?}");
    // Match 已完成后父后步失败：已接收的 owned 由 parent 清理一次，Closed 的 child 不重清理。
    assert_eq!(count(&events, "a-first-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "a-second-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "a-temp-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "later-combine"), 0, "{events:?}");
    assert_eq!(count(&events, "later-failing"), 1, "{events:?}");
    // ancestor（Seed）在 child 失败时不被误删：它只在 Root 清理时销毁一次，且在失败之后。
    assert_eq!(count(&events, "seed-dropped"), 1, "{events:?}");
    let position_of = |needle: &str| events.iter().position(|event| event == needle);
    assert!(
        position_of("later-failing") < position_of("seed-dropped"),
        "{events:?}"
    );
}

// ---- M21：erased Match Future 取消 ----

#[test]
fn m21_erased_match_future_cancellation() {
    // 同一 erased Match 入口的三态对照：未 poll / Pending 后丢弃 / Ready。
    let matched = gated_match();
    let parent = PairParent::build(&matched, later_combine);
    let seed_position = parent.seed.position().clone();
    let route_position = parent.route.position().clone();
    let site = parent.match_site();

    // 未 poll：同一入口的 Future 本体从不推进，直接丢弃不产生任何业务或清理事件。
    reset();
    install_gate();
    {
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(&root, &seed_position, Seed(4))
            .expect("seed");
        execution
            .context_mut()
            .register_owned(&root, &route_position, Route(0))
            .expect("route");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let unpolled = match site {
            CallSite::Orchestrator(site) => Box::pin(site.invoke(&mut guard, &root)),
            CallSite::Node(_) => panic!("Match step must be an orchestrator call site"),
        };
        drop(unpolled);
        assert_eq!(take_events(), Vec::<String>::new());
        assert!(take_shared_events().is_empty(), "未 poll 不进入任何边界");
        drop(guard);
    }

    // Pending 后丢弃真实 Future 本体：完整次序、Closed／parent、最深取消 Scope、ancestor 保留。
    reset();
    install_gate();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, &seed_position, Seed(4))
        .expect("seed");
    execution
        .context_mut()
        .register_owned(&root, &route_position, Route(0))
        .expect("route");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let boxed = match site {
        CallSite::Orchestrator(site) => advance_to_pending(site.invoke(&mut guard, &root), 1),
        CallSite::Node(_) => panic!("Match step must be an orchestrator call site"),
    };
    let pending = take_events();
    assert_eq!(count(&pending, "g-first"), 1, "{pending:?}");
    assert_eq!(count(&pending, "branch-async"), 1, "{pending:?}");
    assert_eq!(count(&pending, "later-combine"), 0, "{pending:?}");
    assert_eq!(count(&pending, "borrow-end"), 0, "{pending:?}");
    drop(boxed);
    let after = take_events();
    assert_eq!(
        count(&after, "g-first-dropped"),
        1,
        "各业务值一次 Drop: {after:?}"
    );
    assert_eq!(
        count(&after, "seed-dropped"),
        0,
        "ancestor 仍由 Root 负责: {after:?}"
    );

    let shared = take_shared_events();
    let creations = boundary_creation_snapshot();
    assert_eq!(
        creations
            .iter()
            .map(|(_, _, role)| *role)
            .collect::<Vec<_>>(),
        vec![ScopeRole::Match, ScopeRole::Branch, ScopeRole::Flow]
    );
    assert_eq!(creations[1].1, creations[0].0, "Branch 挂在 MatchScope 下");
    assert_eq!(
        creations[2].1, creations[1].0,
        "Flow child 挂在 BranchScope 下"
    );
    // 全部实际 Scope 已退出（frame 退出 = Closed 边界），次序由内到外；Root 未退出。
    let flow_seq = creations[2].0.seq();
    let branch_seq = creations[1].0.seq();
    let match_seq = creations[0].0.seq();
    assert!(
        at(&shared, "frame-exit:leaf") < at(&shared, &format!("frame-exit:boundary:{flow_seq}")),
        "{shared:?}"
    );
    assert!(
        at(&shared, &format!("frame-exit:boundary:{flow_seq}"))
            < at(&shared, &format!("frame-exit:boundary:{branch_seq}")),
        "{shared:?}"
    );
    assert!(
        at(&shared, &format!("frame-exit:boundary:{branch_seq}"))
            < at(&shared, &format!("frame-exit:boundary:{match_seq}")),
        "{shared:?}"
    );
    assert!(
        !shared.iter().any(|event| event.contains("frame-exit:root")),
        "Root frame 仍存活: {shared:?}"
    );
    assert!(
        !shared.iter().any(|event| event.contains("context-drop")),
        "{shared:?}"
    );
    // 借用结束恰好一次（业务日志与共享日志各记一次），且在**同一条共享序列**内先于最深一层
    // （被取消的 Leaf）的清理事件：Leaf guard 的清理标签是 `none`（叶子沿用 caller Scope，
    // 不 own Scope）。
    assert_eq!(count(&after, "borrow-end"), 1, "借用结束一次: {after:?}");
    assert_eq!(
        count(&shared, "borrow-end"),
        1,
        "共享序列里的借用结束一次: {shared:?}"
    );
    assert!(
        at(&shared, "borrow-end") < at(&shared, "cleanup-start:none"),
        "借用结束先于被取消 Leaf 的清理: {shared:?}"
    );
    assert!(
        at(&shared, "cleanup-start:none") < at(&shared, "cleanup-end:none"),
        "{shared:?}"
    );
    // 被取消 Leaf 的 frame 退出先于它所在 Scope（Flow child）的清理。
    assert!(
        at(&shared, "cleanup-end:none") < nth(&shared, &format!("frame-exit:leaf:{flow_seq}"), 1),
        "{shared:?}"
    );
    assert!(
        nth(&shared, &format!("frame-exit:leaf:{flow_seq}"), 1)
            < at(&shared, &format!("cleanup-start:{flow_seq}")),
        "{shared:?}"
    );
    // 每一层：cleanup-start → cleanup-end → frame-exit，再由内到外进入上一层。
    for seq in [flow_seq, branch_seq, match_seq] {
        assert!(
            at(&shared, &format!("cleanup-start:{seq}"))
                < at(&shared, &format!("cleanup-end:{seq}")),
            "Scope {seq} 清理开始先于结束: {shared:?}"
        );
        assert!(
            at(&shared, &format!("cleanup-end:{seq}"))
                < at(&shared, &format!("frame-exit:boundary:{seq}")),
            "Scope {seq} 清理结束先于 frame 退出: {shared:?}"
        );
    }
    assert!(
        at(&shared, &format!("frame-exit:boundary:{flow_seq}"))
            < at(&shared, &format!("cleanup-start:{branch_seq}")),
        "Flow 退出后才进入 Branch 清理: {shared:?}"
    );
    assert!(
        at(&shared, &format!("frame-exit:boundary:{branch_seq}"))
            < at(&shared, &format!("cleanup-start:{match_seq}")),
        "Branch 退出后才进入 Match 清理: {shared:?}"
    );
    // 取消定位等于实际最深 Scope（Flow child），类别为 Cancelled。
    let termination = guard.termination().expect("终止记录");
    assert_eq!(termination.kind(), TerminationKind::Cancelled);
    assert_eq!(
        termination.scope(),
        Some(&creations[2].0),
        "取消定位是实际最深 Scope"
    );
    // 已建立的全部 Scope 都 Closed，parent 关系与实际一致；Root 仍 Active。
    for (scope, parent, _) in &creations {
        assert_eq!(
            guard.parent_of(scope).expect("parent known"),
            Some(parent.clone())
        );
        assert!(
            matches!(guard.state(scope), Ok(ScopeState::Closed)),
            "取消后 child Scope 已关闭"
        );
    }
    assert!(matches!(guard.state(&root), Ok(ScopeState::Active)));
    // 借用已结束、ancestor 保留：Root 仍可解析 Seed 且 owner 不变。
    let seed_id = guard
        .snapshot_probe(&root)
        .expect("root snapshot")
        .0
        .into_iter()
        .find(|(position, _)| position == &seed_position)
        .map(|(_, id)| id)
        .expect("seed still bound");
    assert_eq!(guard.owner_probe(&seed_id).expect("owner"), root);
    drop(guard);
    drop(execution);

    // Ready 对照：**同一个 erased invoke 本体**，不挂起时直接推进到 Ready。
    reset();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, &seed_position, Seed(4))
        .expect("seed");
    execution
        .context_mut()
        .register_owned(&root, &route_position, Route(0))
        .expect("route");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let outcome = match site {
        CallSite::Orchestrator(site) => drive(site.invoke(&mut guard, &root)),
        CallSite::Node(_) => panic!("Match step must be an orchestrator call site"),
    };
    assert!(outcome.is_ok(), "{outcome:?}");
    let ready = take_events();
    assert_eq!(count(&ready, "borrow-end"), 1, "{ready:?}");
    assert_eq!(
        count(&ready, "g-first-dropped"),
        0,
        "Ready 后两个输出已交给 caller（Root）: {ready:?}"
    );
    let bound_positions = guard.snapshot_probe(&root).expect("root snapshot").0;
    for position in [parent.first.position(), parent.second.position()] {
        assert!(
            bound_positions
                .iter()
                .any(|(candidate, _)| candidate == position),
            "Ready 后 Match 输出绑定在 caller: {bound_positions:?}"
        );
    }
    drop(guard);
    drop(execution);
    let after_ready = take_shared_events();
    assert_eq!(count(&after_ready, "g-first-dropped"), 1, "{after_ready:?}");
}

// ---- M22：Root-owning Future 取消 ----

#[test]
fn m22_root_owning_future_cancellation() {
    reset();
    install_gate();
    let matched = gated_match();
    let parent = PairParent::build(&matched, later_combine);

    // 未 poll 对照：真正拥有 RootExecution 的同一入口从不推进，直接丢弃。
    reset();
    install_gate();
    let unpolled = Box::pin(super::test_support::definition_in_root::<
        fn(&mut super::context::ExecutionContext, &ScopeId),
        fn(&mut super::test_support::RootView<'_, '_>) -> Result<(), BodyError>,
    >(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(4))],
        |_, _| {},
        None,
    ));
    drop(unpolled);
    let untouched = take_shared_events();
    assert!(
        !untouched.iter().any(|event| event.starts_with("frame-exit")
            || event.starts_with("cleanup")
            || event.contains("context-drop")
            || event.contains("container-drop")),
        "未 poll 的执行不创建 Context／Scope: {untouched:?}"
    );
    let untouched_business = take_events();
    assert_eq!(
        count(&untouched_business, "g-first"),
        0,
        "{untouched_business:?}"
    );
    assert_eq!(
        count(&untouched_business, "later-combine"),
        0,
        "{untouched_business:?}"
    );

    // Pending 后丢弃真正拥有 RootExecution 的 Root Future。
    reset();
    install_gate();
    let boxed = advance_to_pending(
        super::test_support::definition_in_root::<
            fn(&mut super::context::ExecutionContext, &ScopeId),
            fn(&mut super::test_support::RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            parent.flow.definition(),
            vec![root_input(&parent.seed, Seed(4))],
            |_, _| {},
            None,
        ),
        1,
    );
    let pending = take_events();
    assert_eq!(count(&pending, "g-first"), 1, "{pending:?}");
    assert_eq!(count(&pending, "branch-async"), 1, "{pending:?}");
    assert_eq!(count(&pending, "later-combine"), 0, "{pending:?}");
    assert_eq!(count(&pending, "borrow-end"), 0, "{pending:?}");
    let creations = boundary_creation_snapshot();
    assert_eq!(
        creations
            .iter()
            .map(|(_, _, role)| *role)
            .collect::<Vec<_>>(),
        vec![ScopeRole::Match, ScopeRole::Branch, ScopeRole::Flow]
    );
    drop(boxed);
    let after = take_events();
    assert_eq!(
        count(&after, "g-first-dropped"),
        1,
        "各业务值一次 Drop: {after:?}"
    );
    assert_eq!(count(&after, "borrow-end"), 1, "借用结束一次: {after:?}");
    let shared = take_shared_events();
    assert_eq!(
        count(&shared, "seed-dropped"),
        1,
        "ancestor 由 Root 清理一次: {shared:?}"
    );
    // 已建立的全部 Scope 各退出一次，次序由内到外，且都先于 Root frame 退出。
    let flow_seq = creations[2].0.seq();
    let branch_seq = creations[1].0.seq();
    let match_seq = creations[0].0.seq();
    let exit = |seq: u64| format!("frame-exit:boundary:{seq}");
    for seq in [flow_seq, branch_seq, match_seq] {
        assert_eq!(
            count(&shared, &exit(seq)),
            1,
            "每个实际 Scope 恰好退出一次: {shared:?}"
        );
    }
    assert!(
        at(&shared, "frame-exit:leaf") < at(&shared, &exit(flow_seq)),
        "{shared:?}"
    );
    assert!(
        at(&shared, &exit(flow_seq)) < at(&shared, &exit(branch_seq)),
        "{shared:?}"
    );
    assert!(
        at(&shared, &exit(branch_seq)) < at(&shared, &exit(match_seq)),
        "{shared:?}"
    );
    assert!(
        at(&shared, &exit(match_seq)) < at(&shared, "frame-exit:root"),
        "{shared:?}"
    );
    // 借用结束 → 最深一层（Leaf）清理 → 逐层 cleanup-start／end → frame-exit，全部在
    // **同一条共享序列**内比较。
    assert_eq!(
        count(&shared, "borrow-end"),
        1,
        "共享序列里的借用结束一次: {shared:?}"
    );
    assert!(
        at(&shared, "borrow-end") < at(&shared, "cleanup-start:none"),
        "借用结束先于被取消 Leaf 的清理: {shared:?}"
    );
    assert!(
        at(&shared, "cleanup-start:none") < at(&shared, "cleanup-end:none"),
        "{shared:?}"
    );
    assert!(
        at(&shared, "cleanup-end:none") < nth(&shared, &format!("frame-exit:leaf:{flow_seq}"), 1),
        "{shared:?}"
    );
    for seq in [flow_seq, branch_seq, match_seq] {
        assert!(
            at(&shared, &format!("cleanup-start:{seq}"))
                < at(&shared, &format!("cleanup-end:{seq}")),
            "Scope {seq} 清理开始先于结束: {shared:?}"
        );
        assert!(
            at(&shared, &format!("cleanup-end:{seq}")) < at(&shared, &exit(seq)),
            "Scope {seq} 清理结束先于 frame 退出: {shared:?}"
        );
    }
    assert!(
        at(&shared, &exit(flow_seq)) < at(&shared, &format!("cleanup-start:{branch_seq}")),
        "Flow 退出后才进入 Branch 清理: {shared:?}"
    );
    // 取消 Leaf 退出 → Flow 开始清理（同一共享序列；Leaf 的第二次 `frame-exit:leaf` 属于被取消者）。
    assert!(
        nth(&shared, &format!("frame-exit:leaf:{flow_seq}"), 1)
            < at(&shared, &format!("cleanup-start:{flow_seq}")),
        "被取消 Leaf 退出后才开始 Flow 清理: {shared:?}"
    );
    // Branch 退出 → Match 开始清理。
    assert!(
        at(&shared, &exit(branch_seq)) < at(&shared, &format!("cleanup-start:{match_seq}")),
        "Branch 退出后才开始 Match 清理: {shared:?}"
    );
    // Match 退出 → Root 开始清理 → 清理结束 → Root frame 退出。
    let root_seq = creations[0].1.seq();
    assert!(
        at(&shared, &exit(match_seq)) < at(&shared, &format!("cleanup-start:{root_seq}")),
        "Match 退出后才开始 Root 清理: {shared:?}"
    );
    assert!(
        at(&shared, &format!("cleanup-start:{root_seq}"))
            < at(&shared, &format!("cleanup-end:{root_seq}")),
        "Root 清理开始先于结束: {shared:?}"
    );
    assert!(
        at(&shared, &format!("cleanup-end:{root_seq}"))
            < at(&shared, &format!("frame-exit:root:{root_seq}")),
        "Root 清理结束先于 Root frame 退出: {shared:?}"
    );
    assert!(
        at(&shared, "frame-exit:root") < at(&shared, "context-drop"),
        "{shared:?}"
    );
    assert!(
        at(&shared, "frame-exit:root") < at(&shared, "container-drop"),
        "{shared:?}"
    );
    assert!(
        at(&shared, &exit(branch_seq)) < at(&shared, "context-drop"),
        "{shared:?}"
    );

    // Ready 对照：同一 owning-root 入口不挂起时正常完成。
    reset();
    let outcome = run_definition_plain(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(4))],
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let ready = take_events();
    assert_eq!(count(&ready, "later-combine"), 1, "{ready:?}");
    assert_eq!(count(&ready, "g-first-dropped"), 1, "{ready:?}");
    assert_eq!(count(&ready, "seed-dropped"), 1, "{ready:?}");

    // 另一 Execution 不受影响：取消之后新执行仍从 seq 1 开始并正常给出结果。
    reset();
    let before_counts = creation_counts::snapshot();
    let outcome = run_definition_in_root(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(4))],
        |view| {
            let creations = boundary_creation_snapshot();
            assert_eq!(
                creations
                    .iter()
                    .map(|(scope, _, role)| (scope.seq(), *role))
                    .collect::<Vec<_>>(),
                vec![
                    (1, ScopeRole::Match),
                    (2, ScopeRole::Branch),
                    (3, ScopeRole::Flow)
                ],
                "新 Execution 的身份空间与取消执行无关"
            );
            let first_position = parent.first.position().clone();
            let _ = view.resolve::<W>(&first_position)?;
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let counts = creation_counts::snapshot();
    assert_eq!(counts.0, before_counts.0 + 1);
    assert_eq!(counts.1, before_counts.1 + 1);
    assert_eq!(counts.2, before_counts.2 + 1);
}

// ---- M23：主场景与唯一执行域 ----

#[test]
fn m23_main_scenario_unique_execution_domain() {
    reset();
    let matched = pair_match(true);
    let definition_ports: Vec<RefId> = matched
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let before_counts = creation_counts::snapshot();
    let parent = PairParent::build(&matched, later_combine);
    let first_position = parent.first.position().clone();
    let second_position = parent.second.position().clone();
    let mut addresses_equal = false;
    let mut run_ids = Vec::new();
    let mut run_scopes = Vec::new();
    let outcome = run_definition_in_root(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(4))],
        |view| {
            let before = view.snapshot_full()?;
            // 父 Flow 后步已借用两个输出：值读数与 owner。
            let first_id = view
                .snapshot()?
                .into_iter()
                .find(|(position, _)| position == &first_position)
                .map(|(_, id)| id)
                .expect("first bound");
            assert_eq!(view.probe().owner_probe(&first_id)?, view.root().clone());
            let _ = view.resolve::<W>(&first_position)?;
            let _ = view.resolve::<W>(&second_position)?;
            assert_eq!(view.snapshot_full()?, before, "观察不改变 refs 或 owned");
            assert!(view.root_is_active());
            assert_eq!(view.probe().frame_depth(), 1, "所有 child 已退出");
            // 真实创建点：角色／parent／seq 增量与实际调用域地址逐项一致。
            let creations = boundary_creation_snapshot();
            let roles: Vec<ScopeRole> = creations.iter().map(|(_, _, role)| *role).collect();
            // Root → Match 调用 → 被选 Branch → branch 内 Flow child → 其嵌套 SubFlow。
            assert_eq!(
                roles,
                vec![
                    ScopeRole::Match,
                    ScopeRole::Branch,
                    ScopeRole::Flow,
                    ScopeRole::Flow
                ]
            );
            assert_eq!(creations[0].1.seq(), 0);
            assert_eq!(creations[1].1, creations[0].0);
            assert_eq!(creations[2].1, creations[1].0);
            assert_eq!(creations[3].1, creations[2].0);
            assert_eq!(creations[0].0.seq(), 1);
            assert_eq!(creations[1].0.seq(), 2);
            assert_eq!(creations[2].0.seq(), 3);
            assert_eq!(creations[3].0.seq(), 4);
            let identity = view.probe().identity_probe();
            let coordinator = view.probe().coordinator_probe();
            let container = view.probe().container_probe();
            addresses_equal = boundary_address_snapshot()
                .iter()
                .all(|(_, i, c, k)| *i == identity && *c == coordinator && *k == container);
            run_ids.push(first_id);
            run_scopes.push(creations[0].0.clone());
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    assert!(addresses_equal, "所有真实边界与 Root 共用执行域地址");
    // 未选路径 body 为零；嵌套 SubFlow 已在被选 branch 内运行。
    let events = take_events();
    for selected in ["a-temp", "a-first", "a-second", "later-combine"] {
        assert_eq!(count(&events, selected), 1, "{events:?}");
    }
    for unselected in ["b-temp", "b-first", "d-temp", "d-first"] {
        assert_eq!(count(&events, unselected), 0, "{events:?}");
    }
    assert_eq!(count(&events, "a-first-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "a-second-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "combined-dropped"), 1, "{events:?}");
    assert_eq!(count(&events, "seed-dropped"), 1, "{events:?}");
    // 唯一执行域：本次 Root 创建之前取基线，三类组件各 +1。
    let after_counts = creation_counts::snapshot();
    assert_eq!(after_counts.0, before_counts.0 + 1, "one Context");
    assert_eq!(after_counts.1, before_counts.1 + 1, "one Coordinator");
    assert_eq!(after_counts.2, before_counts.2 + 1, "one Container");

    // 同一定义与 Arc 句柄的第二次 Execution：选择另一 branch，结果重算、身份不同。
    reset();
    let parent = PairParent::build(&matched, later_combine);
    let mut second_ids = Vec::new();
    let mut second_scopes = Vec::new();
    let first_position = parent.first.position().clone();
    let outcome = run_definition_in_root(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(5))],
        |view| {
            second_ids.push(
                view.snapshot()?
                    .into_iter()
                    .find(|(position, _)| position == &first_position)
                    .map(|(_, id)| id)
                    .expect("b branch output bound"),
            );
            second_scopes.push(
                boundary_creation_snapshot()
                    .first()
                    .expect("match scope recorded")
                    .0
                    .clone(),
            );
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    let events = take_events();
    assert_eq!(count(&events, "b-first"), 1, "{events:?}");
    assert_eq!(count(&events, "a-first"), 0, "{events:?}");
    assert_ne!(run_ids[0], second_ids[0], "跨 Execution 不复用 DataId");
    assert_ne!(
        run_scopes[0], second_scopes[0],
        "跨 Execution 不复用 ScopeId"
    );
    assert_eq!(
        matched
            .definition()
            .output_ports()
            .iter()
            .map(|port| port.position().clone())
            .collect::<Vec<RefId>>(),
        definition_ports,
        "固定定义未被修改"
    );
    assert_eq!(count(&events, "seed-dropped"), 1, "{events:?}");
}

// ---- M24：回归／可见性／审计 ----

#[test]
fn m24_registry_holds_build_time_metadata_only() {
    reset();
    let matched = pair_match(false);
    let definition = matched.definition() as *const Definition;
    let clone = matched.clone();
    assert_eq!(
        clone.definition() as *const Definition,
        definition,
        "Clone 只共享不可变定义"
    );
    let common: Vec<RefId> = matched
        .definition()
        .output_ports()
        .iter()
        .map(|port| port.position().clone())
        .collect();
    let branch_input = matched.definition().inputs()[1].position().clone();
    let audit = |candidate: &Match<Route, Seed, Out2<W, W>>| {
        for site in candidate.sites_probe() {
            match site {
                CallSite::Orchestrator(site) => {
                    assert_eq!(site.inputs(), std::slice::from_ref(&branch_input));
                    assert_eq!(site.outputs(), common.as_slice());
                }
                CallSite::Node(_) => panic!("every branch wrapper is an orchestrator site"),
            }
        }
        assert_eq!(candidate.registered_probe(), 3);
        assert_eq!(candidate.branch_steps_probe().len(), 3);
    };
    audit(&matched);
    let parent = PairParent::build(&matched, later_combine);
    let outcome = run_definition_plain(
        parent.flow.definition(),
        vec![root_input(&parent.seed, Seed(4))],
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    // 运行后登记表与定义不变：没有运行态身份或选择结果被缓存。
    audit(&matched);
    assert_eq!(matched.definition() as *const Definition, definition);
}
