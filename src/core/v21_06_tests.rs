//! V21-06 验收样本（F01～F24）：完整 Flow Definition 与 SubFlow 顺序执行。
//!
//! 这些样本使用 crate 内可见的 Definition／Flow／协议类型，不构成公开 API 承诺。
//! 编译负例（F14／F15／F24）放在 `tests/ui/`，按真实 `src/core` 或已构建 rlib 独立编译。
//!
//! 共享 poll／gate／事件／child Scope 观测与 Root 输入登记来自 [`super::test_support`]；
//! 业务夹具与业务调用计数留在本模块。

use std::any::TypeId;
use std::cell::RefCell;

use super::builder::{Definition, TypedCallBuilder, run_definition};
use super::context::{
    BodyError, ExecutionContext, InvocationGuard, InvocationKind, TerminationKind,
};
use super::data_ref::DataRef;
use super::flow::{Flow, FlowBuilder};
use super::identity::{DataId, ScopeId};
use super::node::NodeCall1;
use super::orchestrator::{OrchCall, PackFor};
use super::ref_id::RefId;
use super::runtime::RootExecution;
use super::signature::{BuildError, Data, InputTypes, NodeFut, Out2, OutKind, Unit};
use super::test_support::{
    RootInput, advance_to_pending, boundary_address_reset, boundary_address_snapshot,
    boundary_child_scope_reset, boundary_child_scope_snapshot, child_scope_reset, drive,
    drive_pinned, export_attempt_snapshot, gate_wait, install_gate, record, release_gate,
    root_input, take_events, take_shared_events,
};

// ---- Root 驱动夹具（面向完整 Flow 的薄委托） ----
//
// 共同主体、观察视图与三个入口已提升到 `test_support` 并按 `&Definition` 参数化
// （V21-07 §4.6／§8.5）：这里只保留 Flow 侧的薄委托，把 `flow.definition()` 交给共同驱动。
// 断言语义、prepare／observe 时机、空声明收口与错误路径均不变。

/// Root 关闭前的只读观察视图（共享实现）。
type FlowRootView<'a, 'ctx> = super::test_support::RootView<'a, 'ctx>;

/// 在 Root 中执行完整 Flow 的异步主体。
async fn flow_in_root<I, K, P, F>(
    flow: &Flow<I, K>,
    inputs: Vec<RootInput>,
    prepare: P,
    observe: Option<F>,
) -> Result<(), BodyError>
where
    I: super::flow::FlowInputs + InputTypes,
    K: OutKind,
    I::Pack: PackFor<I>,
    P: FnOnce(&mut ExecutionContext, &ScopeId),
    F: FnOnce(&mut FlowRootView<'_, '_>) -> Result<(), BodyError>,
{
    super::test_support::definition_in_root(flow.definition(), inputs, prepare, observe).await
}

/// Root 中执行完整 Flow：可携带预备钩子（例如预占 caller 输出位置）。
fn run_flow_prepared<I, K, P>(
    flow: &Flow<I, K>,
    inputs: Vec<RootInput>,
    prepare: P,
) -> Result<(), BodyError>
where
    I: super::flow::FlowInputs + InputTypes,
    K: OutKind,
    I::Pack: PackFor<I>,
    P: FnOnce(&mut ExecutionContext, &ScopeId),
{
    drive(flow_in_root::<
        I,
        K,
        P,
        fn(&mut FlowRootView<'_, '_>) -> Result<(), BodyError>,
    >(flow, inputs, prepare, None))
}

/// Root 中执行完整 Flow，关闭前观察。
fn run_flow_in_root<I, K, F>(
    flow: &Flow<I, K>,
    inputs: Vec<RootInput>,
    observe: F,
) -> Result<(), BodyError>
where
    I: super::flow::FlowInputs + InputTypes,
    K: OutKind,
    I::Pack: PackFor<I>,
    F: FnOnce(&mut FlowRootView<'_, '_>) -> Result<(), BodyError>,
{
    drive(flow_in_root(flow, inputs, |_, _| {}, Some(observe)))
}

/// Root 中执行完整 Flow，不做额外观察。
fn run_flow_root<I, K>(flow: &Flow<I, K>, inputs: Vec<RootInput>) -> Result<(), BodyError>
where
    I: super::flow::FlowInputs + InputTypes,
    K: OutKind,
    I::Pack: PackFor<I>,
{
    run_flow_prepared(flow, inputs, |_, _| {})
}

// ---- 业务夹具 ----

/// 同步函数 Node：`&u32 -> u64`。
fn f_inc(a: &u32) -> Result<u64, BodyError> {
    record("inc");
    Ok(u64::from(*a) + 1)
}

/// 同步函数 Node：`(&u32, &u64) -> String`。
fn f_join2(a: &u32, b: &u64) -> Result<String, BodyError> {
    record("join2");
    Ok(format!("{a}/{b}"))
}

/// 异步函数 Node：`&u64 -> String`，可在挂起点暂停。
async fn f_text(a: &u64) -> Result<String, BodyError> {
    record("text");
    gate_wait().await;
    Ok(format!("#{a}"))
}

/// 异步函数 Node：`&u64 -> usize`。
async fn f_count(a: &u64) -> Result<usize, BodyError> {
    record("count");
    Ok(*a as usize)
}

/// 异步函数 Node：`&usize -> String`，在挂起点后返回执行错误。
async fn f_fail(a: &usize) -> Result<String, BodyError> {
    record("fail");
    gate_wait().await;
    let _ = a;
    Err(BodyError::new("node failed"))
}

/// 异步函数 Node：`&FTracked -> usize`，在挂起点后返回执行错误。
async fn f_fail_tracked(a: &FTracked) -> Result<usize, BodyError> {
    record("fail-tracked");
    gate_wait().await;
    let _ = a;
    Err(BodyError::new("later failure"))
}

/// 同步函数 Node：`(&u32, &u32) -> String`，同类型两个位置。
fn f_distinct(a: &u32, b: &u32) -> Result<String, BodyError> {
    record("distinct");
    Ok(format!("{a}|{b}"))
}

/// 同步函数 Node：`&FLocal -> u32`（非 `Clone` 输入）。
fn f_local(a: &FLocal) -> Result<u32, BodyError> {
    record("local");
    Ok(a.value())
}

/// 同步函数 Node：`&(u32, u64) -> String`，把一位业务 tuple Data 整体借用。
fn f_tuple(a: &(u32, u64)) -> Result<String, BodyError> {
    record("tuple");
    Ok(format!("{}/{}", a.0, a.1))
}

/// 结构体 Node：`&String -> usize`。
struct FLength;

impl NodeCall1<String, Data<usize>> for FLength {
    fn call<'a>(&'a self, a: &'a String) -> NodeFut<'a, usize> {
        Box::pin(async move {
            record("len");
            Ok(a.len())
        })
    }
}

/// 显式 unit 结构体 Node：`&usize -> ()`。
struct FTouch;

impl NodeCall1<usize, Unit> for FTouch {
    fn call<'a>(&'a self, a: &'a usize) -> NodeFut<'a, ()> {
        Box::pin(async move {
            record(if *a == 0 { "touch:zero" } else { "touch" });
            Ok(())
        })
    }
}

/// 产出带 Drop 见证的 owned 值：`&u32 -> FTracked`。
struct FTemp(&'static str);

impl NodeCall1<u32, Data<FTracked>> for FTemp {
    fn call<'a>(&'a self, _a: &'a u32) -> NodeFut<'a, FTracked> {
        Box::pin(async move { Ok(FTracked(self.0)) })
    }
}

/// 从带 Drop 见证的输入产出新的带见证值：`&FTracked -> FTracked`。
struct FTempFromTracked(&'static str);

impl NodeCall1<FTracked, Data<FTracked>> for FTempFromTracked {
    fn call<'a>(&'a self, _a: &'a FTracked) -> NodeFut<'a, FTracked> {
        Box::pin(async move { Ok(FTracked(self.0)) })
    }
}

/// 读取带 Drop 见证的值：`&FTracked -> usize`。
struct FTempRead(&'static str);

impl NodeCall1<FTracked, Data<usize>> for FTempRead {
    fn call<'a>(&'a self, a: &'a FTracked) -> NodeFut<'a, usize> {
        Box::pin(async move {
            record(self.0);
            Ok(a.0.len())
        })
    }
}

/// 带 Drop 见证的 owned 业务值（不实现 `Clone`）。
struct FTracked(&'static str);

impl Drop for FTracked {
    fn drop(&mut self) {
        record(self.0);
    }
}

/// 非 `Clone`、非 `Send` 的输入值。
struct FLocal(u32);

impl FLocal {
    fn value(&self) -> u32 {
        self.0
    }
}

// ---- 常用完整 Flow ----

/// 单输入 `u32` → `f_inc` → 单 Data 输出 `u64`。
fn flow_inc() -> (Flow<(u32,), Data<u64>>, DataRef<u32>) {
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let output: DataRef<u64> = builder.then(f_inc, input.clone()).expect("step");
    (builder.finish(output).expect("finish"), input)
}

/// 零 Step、passthrough 的完整 Flow：输出位置就是声明输入位置（imported 重新暴露）。
fn flow_passthrough_u32() -> (Flow<(u32,), Data<u32>>, DataRef<u32>) {
    let (builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    (builder.finish(input.clone()).expect("finish"), input)
}

/// 零 Step、passthrough 的完整 Flow（`String`）。
fn flow_passthrough_string() -> (Flow<(String,), Data<String>>, DataRef<String>) {
    let (builder, input) = FlowBuilder::<(String,)>::start().expect("builder");
    (builder.finish(input.clone()).expect("finish"), input)
}

/// 零 Step、两个 passthrough 位置的完整 Flow（`FTracked`，不实现 `Clone`）。
#[allow(clippy::type_complexity)]
fn flow_passthrough_tracked_pair() -> (
    Flow<(FTracked, FTracked), Out2<FTracked, FTracked>>,
    DataRef<FTracked>,
    DataRef<FTracked>,
) {
    let (builder, (first, second)) = FlowBuilder::<(FTracked, FTracked)>::start().expect("builder");
    let flow = builder
        .finish((first.clone(), second.clone()))
        .expect("finish two positions");
    (flow, first, second)
}

/// 单输入 `FLocal`（非 `Clone`）→ `f_local` → 单 Data 输出 `u32`。
fn local_flow() -> (Flow<(FLocal,), Data<u32>>, DataRef<FLocal>) {
    let (mut builder, input) = FlowBuilder::<(FLocal,)>::start().expect("builder");
    let output: DataRef<u32> = builder.then(f_local, input.clone()).expect("step");
    (builder.finish(output).expect("finish"), input)
}

/// 每个样本起点：清空本线程的业务事件、共享事件与 child 观测。
fn reset() {
    take_events();
    take_shared_events();
    child_scope_reset();
    boundary_child_scope_reset();
    boundary_address_reset();
}

// ---- F01：未声明输出与显式 unit ----

#[test]
fn f01_unit_finish_is_an_explicit_completion_without_positions() {
    reset();
    let (mut builder, input) = FlowBuilder::<(FTracked,)>::start().expect("builder");
    let produced: DataRef<FTracked> = builder
        .then(FTempFromTracked("unit-temp-dropped"), input.clone())
        .expect("step");
    assert_eq!(builder.definition_probe().output_ports().len(), 0);
    // 完成操作不分配新位置：finish 前后的已分配序号一致。
    let allocated_before = builder.allocated_probe();
    let flow: Flow<(FTracked,), Unit> = builder.finish(()).expect("finish unit");
    assert!(flow.definition().output_ports().is_empty());
    assert_eq!(flow.definition().allocated_probe(), allocated_before);
    let temporary_position = produced.position().clone();

    let outcome = run_flow_in_root(
        &flow,
        vec![root_input(&input, FTracked("f01-input-dropped"))],
        move |view| {
            assert!(view.root_is_active());
            // 关闭前观察：完整 refs／owned 快照，先读临时值再复核快照相等。
            let before = view.snapshot_full()?;
            assert_eq!(
                before.0.len(),
                2,
                "registered input plus the unexported temporary"
            );
            assert_eq!(before.1.len(), 2, "both values are Root-owned");
            assert_eq!(
                view.resolve::<FTracked>(&temporary_position)?.0,
                "unit-temp-dropped"
            );
            assert_eq!(
                view.snapshot_full()?,
                before,
                "observation changes no refs or owned"
            );
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "unit flow must run: {outcome:?}");
    // 空声明收口清理完成：输入与未导出临时值各由合法责任方 Drop 一次。
    let mut events = take_events();
    events.sort();
    assert_eq!(events, vec!["f01-input-dropped", "unit-temp-dropped"]);
}

// ---- F02：输入／输出 Signature 范围 ----

#[test]
fn f02_signature_matrix_runs_for_single_and_double_inputs() {
    reset();
    // 单输入 × unit。
    let (flow_unit, unit_input) = {
        let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
        let _: DataRef<u64> = builder.then(f_inc, input.clone()).expect("step");
        (builder.finish(()).expect("finish unit"), input)
    };
    assert!(run_flow_root(&flow_unit, vec![root_input(&unit_input, 1u32)]).is_ok());

    // 单输入 × 两个独立位置（同一输入喂两步）。
    let (flow_two, two_input, text, count) = {
        let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
        let widened: DataRef<u64> = builder.then(f_inc, input.clone()).expect("step");
        let text: DataRef<String> = builder.then(f_text, widened.clone()).expect("step");
        let count: DataRef<usize> = builder.then(f_count, widened.clone()).expect("step");
        let flow = builder
            .finish((text.clone(), count.clone()))
            .expect("finish two");
        (flow, input, text, count)
    };
    let (text_position, count_position) = (text.position().clone(), count.position().clone());
    let outcome = run_flow_in_root(&flow_two, vec![root_input(&two_input, 3u32)], move |view| {
        assert_eq!(*view.resolve::<String>(&text_position)?, "#4");
        assert_eq!(*view.resolve::<usize>(&count_position)?, 4);
        Ok(())
    });
    assert!(outcome.is_ok(), "two-position flow must run: {outcome:?}");

    // 双输入 × 单 Data 输出。
    let (flow_join, first, second, joined) = {
        let (mut builder, (first, second)) = FlowBuilder::<(u32, u64)>::start().expect("builder");
        let joined: DataRef<String> = builder
            .then(f_join2, (first.clone(), second.clone()))
            .expect("join");
        (
            builder.finish(joined.clone()).expect("finish"),
            first,
            second,
            joined,
        )
    };
    let joined_position = joined.position().clone();
    let outcome = run_flow_in_root(
        &flow_join,
        vec![root_input(&first, 2u32), root_input(&second, 9u64)],
        move |view| {
            assert_eq!(*view.resolve::<String>(&joined_position)?, "2/9");
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "double-input flow must run: {outcome:?}");

    // 双输入 × unit 输出（第六种组合）。
    let (flow_join_unit, unit_first, unit_second, _joined) = {
        let (mut builder, (first, second)) = FlowBuilder::<(u32, u64)>::start().expect("builder");
        let joined: DataRef<String> = builder
            .then(f_join2, (first.clone(), second.clone()))
            .expect("join");
        (
            builder.finish(()).expect("finish unit"),
            first,
            second,
            joined,
        )
    };
    let outcome = run_flow_in_root(
        &flow_join_unit,
        vec![
            root_input(&unit_first, 7u32),
            root_input(&unit_second, 8u64),
        ],
        |_| Ok(()),
    );
    assert!(
        outcome.is_ok(),
        "double input unit flow must run: {outcome:?}"
    );

    // 双输入 × 两个独立位置（passthrough，非 Clone 类型）。
    let (flow_pair, left, right) = flow_passthrough_tracked_pair();
    let (left_position, right_position) = (left.position().clone(), right.position().clone());
    let outcome = run_flow_in_root(
        &flow_pair,
        vec![
            root_input(&left, FTracked("left-dropped")),
            root_input(&right, FTracked("right-dropped")),
        ],
        move |view| {
            assert_eq!(view.resolve::<FTracked>(&left_position)?.0, "left-dropped");
            assert_eq!(
                view.resolve::<FTracked>(&right_position)?.0,
                "right-dropped"
            );
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "passthrough pair must run: {outcome:?}");
    // Root 关闭时清理剩余 owned：两个 passthrough 别名各 Drop 一次（清理顺序不承诺）。
    let events = take_events();
    let prefix = ["inc", "inc", "text", "count", "join2", "join2"];
    assert_eq!(&events[..prefix.len()], &prefix, "{events:?}");
    let mut tail = events[prefix.len()..].to_vec();
    tail.sort();
    assert_eq!(tail, vec!["left-dropped", "right-dropped"], "{events:?}");
}

// ---- F03：同一 then 双协议 ----

#[test]
fn f03_completed_flow_shares_the_same_then_with_functions_and_structs() {
    reset();
    // child：零 Step passthrough Flow（String 位置）。
    let (child, child_input) = flow_passthrough_string();
    assert_eq!(child.definition().step_count(), 0);

    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let widened: DataRef<u64> = builder.then(f_inc, input.clone()).expect("function leaf");
    let text: DataRef<String> = builder.then(f_text, widened.clone()).expect("async leaf");
    let passed: DataRef<String> = builder
        .then(child.clone(), text.clone())
        .expect("completed flow as child");
    let count: DataRef<usize> = builder.then(FLength, passed.clone()).expect("struct leaf");
    let flow = builder
        .finish((text.clone(), count.clone()))
        .expect("finish");
    assert_eq!(flow.definition().step_count(), 4);
    let _ = child_input;

    let (text_position, count_position) = (text.position().clone(), count.position().clone());
    let outcome = run_flow_in_root(&flow, vec![root_input(&input, 1u32)], move |view| {
        assert_eq!(*view.resolve::<String>(&text_position)?, "#2");
        assert_eq!(*view.resolve::<usize>(&count_position)?, 2);
        Ok(())
    });
    assert!(outcome.is_ok(), "mixed dispatch must run: {outcome:?}");
    assert_eq!(take_events(), vec!["inc", "text", "len"]);
}

// ---- F04：强类型 SubFlow 接线 ----

#[test]
fn f04_subflow_wiring_keeps_child_and_caller_positions_separate() {
    reset();
    let (single, single_input) = flow_inc();
    let (double, _double_left, _double_right) = flow_passthrough_tracked_pair();

    // 单一 child 的内部输入位置与 caller 位置不是同一身份。
    let child_position = single.definition().inputs()[0].position().clone();
    let mut parent = Definition::new();
    let caller_input = parent.declare_input::<u32>("a").expect("position");
    assert_ne!(&child_position, caller_input.position());
    let widened: DataRef<u64> = parent
        .then(single.clone(), caller_input.clone())
        .expect("single input child");
    let _ = single_input;

    // 双输入 child 用两个 caller 位置接线。
    let pair_first = parent.declare_input::<FTracked>("t1").expect("position");
    let pair_second = parent.declare_input::<FTracked>("t2").expect("position");
    let (first_out, second_out): (DataRef<FTracked>, DataRef<FTracked>) = parent
        .then(double.clone(), (pair_first.clone(), pair_second.clone()))
        .expect("double input child");
    assert_ne!(first_out.position(), second_out.position());
    assert_ne!(
        double.definition().inputs()[0].position(),
        pair_first.position()
    );
    assert_ne!(
        double.definition().inputs()[1].position(),
        pair_second.position()
    );
    let _ = widened;
    assert_eq!(parent.step_count(), 2);
    take_events();
}

// ---- F05：空 Step 完整 Flow ----

#[test]
fn f05_zero_step_flows_pass_through_or_close_as_unit() {
    reset();
    // child 模式：passthrough 保留 imported owner，child 关闭。
    let (child, _child_input) = flow_passthrough_u32();
    let mut parent = Definition::new();
    let caller_input = parent.declare_input::<u32>("a").expect("position");
    let alias: DataRef<u32> = parent
        .then(child.clone(), caller_input.clone())
        .expect("passthrough child");
    let alias_position = alias.position().clone();
    let caller_position = caller_input.position().clone();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let result = drive(async move {
        execution
            .context_mut()
            .register_owned(&root, &caller_position, 5u32)
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        // 直接经调用边界执行，拿到真实 child ScopeId 以核对关闭。
        let child_scope = match parent.steps()[0].site() {
            super::builder::CallSite::Orchestrator(site) => site.invoke(&mut guard, &root).await?,
            _ => panic!("a completed flow must be stored as an orchestrator call site"),
        };
        assert!(matches!(
            guard.state(&child_scope)?,
            super::scope::ScopeState::Closed
        ));
        assert_eq!(guard.parent_of(&child_scope)?, Some(root.clone()));
        // 后续位置与 caller 位置解析到同一实例（imported alias，责任未转移）。
        let snapshot = guard.snapshot_probe(&root)?.0;
        let id_of = |position: &RefId| {
            snapshot
                .iter()
                .find(|(candidate, _)| candidate == position)
                .map(|(_, id)| id.clone())
                .expect("position bound")
        };
        assert_eq!(id_of(&caller_position), id_of(&alias_position));
        assert_eq!(guard.owner_probe(&id_of(&alias_position))?, root);
        assert_eq!(*guard.resolve::<u32>(&root, &alias_position)?, 5);
        assert_eq!(guard.frame_depth(), 1);
        guard.complete();
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "passthrough child must run: {result:?}");

    // Root 模式 A（零 Step）：非空输入、显式 unit Signature，step_count 为 0，直接空声明收口。
    reset();
    let (zero_builder, zero_input) = FlowBuilder::<(FTracked,)>::start().expect("builder");
    let zero_step: Flow<(FTracked,), Unit> = zero_builder.finish(()).expect("finish unit");
    assert_eq!(
        zero_step.definition().step_count(),
        0,
        "no Step was appended"
    );
    assert!(
        zero_step.definition().output_ports().is_empty(),
        "unit declares no port"
    );
    let zero_position = zero_input.position().clone();
    let outcome = run_flow_in_root(
        &zero_step,
        vec![root_input(&zero_input, FTracked("zero-step-input-dropped"))],
        move |view| {
            let before = view.snapshot_full()?;
            assert_eq!(before.0.len(), 1, "only the registered Root input");
            assert_eq!(before.1.len(), 1);
            assert_eq!(
                view.resolve::<FTracked>(&zero_position)?.0,
                "zero-step-input-dropped"
            );
            assert_eq!(
                view.snapshot_full()?,
                before,
                "observation changes no refs or owned"
            );
            assert!(view.root_is_active());
            assert_eq!(view.probe().frame_depth(), 1);
            Ok(())
        },
    );
    assert!(
        outcome.is_ok(),
        "zero step unit root flow must close: {outcome:?}"
    );
    assert_eq!(take_events(), vec!["zero-step-input-dropped"]);

    // Root 模式 B（一个 Step 产出未导出临时值）：完整快照比较 + 见证值各清理一次。
    reset();
    let (mut builder, input) = FlowBuilder::<(FTracked,)>::start().expect("builder");
    let temp: DataRef<FTracked> = builder
        .then(FTempFromTracked("root-temp-dropped"), input.clone())
        .expect("step");
    let flow = builder.finish(()).expect("finish unit");
    let (temp_position, input_position) = (temp.position().clone(), input.position().clone());
    let outcome = run_flow_in_root(
        &flow,
        vec![root_input(&input, FTracked("root-input-dropped"))],
        move |view| {
            let before = view.snapshot_full()?;
            assert_eq!(before.0.len(), 2, "input plus declared temporary");
            assert_eq!(before.1.len(), 2);
            assert_eq!(
                view.resolve::<FTracked>(&temp_position)?.0,
                "root-temp-dropped"
            );
            assert_eq!(
                view.resolve::<FTracked>(&input_position)?.0,
                "root-input-dropped"
            );
            assert_eq!(
                view.snapshot_full()?,
                before,
                "observation changes no refs or owned"
            );
            assert!(view.root_is_active());
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "root unit flow must close: {outcome:?}");
    let mut events = take_events();
    events.sort();
    assert_eq!(events, vec!["root-input-dropped", "root-temp-dropped"]);
}

// ---- F06：无依赖也顺序 ----

#[test]
fn f06_steps_wait_for_the_previous_future_even_without_data_dependency() {
    reset();
    install_gate();
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let widened: DataRef<u64> = builder.then(f_inc, input.clone()).expect("step 1");
    let text: DataRef<String> = builder
        .then(f_text, widened.clone())
        .expect("step 2 (gated)");
    let count: DataRef<usize> = builder.then(f_count, widened.clone()).expect("step 3");
    let _ = text;
    let flow = builder.finish(count.clone()).expect("finish");
    let count_position = count.position().clone();

    // 同一个 Future：先 poll 到 Pending，再释放挂起点驱动到 Ready。
    let future = {
        let count_position = count_position.clone();
        flow_in_root(
            &flow,
            vec![root_input(&input, 6u32)],
            |_, _| {},
            Some(move |view: &mut FlowRootView<'_, '_>| {
                assert_eq!(*view.resolve::<usize>(&count_position)?, 7);
                Ok(())
            }),
        )
    };
    let boxed = advance_to_pending(future, 1);
    // 第二步仍挂起：没有数据依赖的第三步业务体没有启动。
    assert_eq!(take_events(), vec!["inc", "text"]);
    release_gate();
    let outcome = drive_pinned(boxed);
    assert!(
        outcome.is_ok(),
        "the same future must complete: {outcome:?}"
    );
    // Pending 窗口的事件已在上面取走；恢复后只有第三步启动。
    assert_eq!(take_events(), vec!["count"]);
}

// ---- F07：位置与只读复用 ----

#[test]
fn f07_positions_distinguish_instances_and_allow_read_only_reuse() {
    reset();
    install_gate();
    // 同类型两个位置读到不同实例；同一句柄可被多个 Step 重复读取。
    let (mut builder, (first, second)) = FlowBuilder::<(u32, u32)>::start().expect("builder");
    let joined: DataRef<String> = builder
        .then(f_distinct, (first.clone(), second.clone()))
        .expect("two positions");
    let len: DataRef<usize> = builder.then(FLength, joined.clone()).expect("reuse");
    let again: DataRef<usize> = builder.then(FLength, joined.clone()).expect("reuse again");
    let flow = builder
        .finish((len.clone(), again.clone()))
        .expect("finish");
    let (len_position, again_position) = (len.position().clone(), again.position().clone());
    let outcome = run_flow_in_root(
        &flow,
        vec![root_input(&first, 1u32), root_input(&second, 9u32)],
        move |view| {
            assert_eq!(*view.resolve::<usize>(&len_position)?, 3);
            assert_eq!(*view.resolve::<usize>(&again_position)?, 3);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "position reuse must run: {outcome:?}");
    assert_eq!(take_events(), vec!["distinct", "len", "len"]);

    // 非 Clone 输入整体借用，重复读取不要求业务 Clone。
    reset();
    let (flow, input) = local_flow();
    let outcome = run_flow_in_root(&flow, vec![root_input(&input, FLocal(11))], |_| Ok(()));
    assert!(outcome.is_ok(), "non-Clone input must run: {outcome:?}");
    assert_eq!(take_events(), vec!["local"]);
}

// ---- F08：P02 正常责任交接 ----

#[test]
fn f08_subflow_transfers_new_outputs_and_keeps_imported_owner() {
    reset();
    // child：先产出未导出的临时值，再产出声明输出。
    let (mut child_builder, child_input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let temporary: DataRef<FTracked> = child_builder
        .then(FTemp("child-temp-dropped"), child_input.clone())
        .expect("temporary");
    let produced: DataRef<u64> = child_builder
        .then(f_inc, child_input.clone())
        .expect("child output");
    let child = child_builder
        .finish(produced.clone())
        .expect("finish child");
    assert_eq!(child.definition().step_count(), 2);
    let _ = temporary;

    // parent：Step 1 调用 child，Step 2 读取 child 导出的新输出。
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let from_child: DataRef<u64> = builder
        .then(child.clone(), input.clone())
        .expect("child call");
    let count: DataRef<usize> = builder
        .then(f_count, from_child.clone())
        .expect("read after close");
    let flow = builder.finish(count.clone()).expect("finish parent");
    let (from_child_position, count_position) =
        (from_child.position().clone(), count.position().clone());
    let input_position = input.position().clone();
    let child_input_position = child.definition().inputs()[0].position().clone();
    let child_output_inner = child.definition().output_ports()[0].position().clone();

    let outcome = run_flow_in_root(&flow, vec![root_input(&input, 3u32)], move |view| {
        // 新输出已归 parent（Root），后续位置读到 child 的新值。
        assert_eq!(*view.resolve::<usize>(&count_position)?, 4);
        let produced_id = view.data_id_of(&from_child_position)?;
        assert_eq!(view.probe().owner_probe(&produced_id)?, *view.root());
        assert!(view.probe().alive_probe(&produced_id));
        // imported 输入仍归 Root，且与 child 新输出不是同一实例。
        let imported_id = view.data_id_of(&input_position)?;
        assert_ne!(imported_id, produced_id);
        assert_eq!(view.probe().owner_probe(&imported_id)?, *view.root());
        assert!(view.probe().alive_probe(&imported_id));
        // child 已关闭：从真实调用边界取得实际 child 身份，核对数量、父子、状态与本地引用失效。
        let children = boundary_child_scope_snapshot();
        assert_eq!(children.len(), 1, "one real boundary child was established");
        let child_scope = children[0].clone();
        assert_eq!(
            view.probe().parent_of(&child_scope)?,
            Some(view.root().clone())
        );
        assert!(matches!(
            view.state(&child_scope)?,
            super::scope::ScopeState::Closed
        ));
        assert!(
            view.probe()
                .resolve::<u32>(&child_scope, &child_input_position)
                .is_err(),
            "child-local input refs stop resolving after close"
        );
        assert!(
            view.probe()
                .resolve::<u64>(&child_scope, &child_output_inner)
                .is_err(),
            "child-local output refs stop resolving after close"
        );
        assert_eq!(view.probe().frame_depth(), 1);
        Ok(())
    });
    assert!(outcome.is_ok(), "subflow accounting must run: {outcome:?}");
    // child 的临时值在 child 关闭时析构一次；后续 Step 仍能读取导出的新输出。
    assert_eq!(take_events(), vec!["inc", "child-temp-dropped", "count"]);
}

// ---- F09：多输出与 imported alias ----

#[test]
fn f09_two_positions_alias_one_data_id_without_a_second_owner() {
    reset();
    let (passthrough_one, _) = flow_passthrough_tracked();
    let (passthrough_two, _, _) = flow_passthrough_tracked_pair();

    let (mut builder, input) = FlowBuilder::<(FTracked,)>::start().expect("builder");
    let aliased: DataRef<FTracked> = builder
        .then(passthrough_one.clone(), input.clone())
        .expect("passthrough alias");
    let (left, right): (DataRef<FTracked>, DataRef<FTracked>) = builder
        .then(passthrough_two.clone(), (input.clone(), aliased.clone()))
        .expect("two-position alias");
    let read: DataRef<usize> = builder
        .then(FTempRead("read-alias"), aliased.clone())
        .expect("read");
    let flow = builder.finish(read.clone()).expect("finish");
    let (input_position, alias_position) = (input.position().clone(), aliased.position().clone());
    let (left_position, right_position) = (left.position().clone(), right.position().clone());
    let read_position = read.position().clone();

    let outcome = run_flow_in_root(
        &flow,
        vec![root_input(&input, FTracked("alias-dropped"))],
        move |view| {
            let canonical = view.data_id_of(&input_position)?;
            for position in [&alias_position, &left_position, &right_position] {
                assert_eq!(
                    view.data_id_of(position)?,
                    canonical,
                    "imported alias shares one instance"
                );
            }
            assert_eq!(
                view.probe().owner_probe(&canonical)?,
                *view.root(),
                "one owner only"
            );
            assert_eq!(view.resolve::<FTracked>(&left_position)?.0, "alias-dropped");
            assert_eq!(
                view.resolve::<FTracked>(&right_position)?.0,
                "alias-dropped"
            );
            assert_eq!(
                *view.resolve::<usize>(&read_position)?,
                "alias-dropped".len()
            );
            // 每个 Step 输出位置各有一个本地绑定，但四个别名位置只指向一个实例。
            let snapshot = view.snapshot()?;
            assert_eq!(snapshot.len(), 5);
            let mut instances: Vec<DataId> = snapshot.iter().map(|(_, id)| id.clone()).collect();
            instances.sort_by_key(DataId::seq);
            instances.dedup();
            assert_eq!(
                instances.len(),
                2,
                "one aliased instance plus the read output"
            );
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "alias sample must run: {outcome:?}");
    let events = take_events();
    assert_eq!(events, vec!["read-alias", "alias-dropped"], "{events:?}");
}

/// 零 Step、单位置 passthrough Flow（`FTracked`）。
fn flow_passthrough_tracked() -> (Flow<(FTracked,), Data<FTracked>>, DataRef<FTracked>) {
    let (builder, input) = FlowBuilder::<(FTracked,)>::start().expect("builder");
    (builder.finish(input.clone()).expect("finish"), input)
}

// ---- F10：同一 child 多调用位置 ----

#[test]
fn f10_the_same_child_definition_runs_at_two_call_positions() {
    reset();
    let (child, _) = flow_inc();
    let (mut builder, (first, second)) = FlowBuilder::<(u32, u32)>::start().expect("builder");
    let first_child = builder.then(child.clone(), first.clone()).expect("call 1");
    let second_child = builder.then(child.clone(), second.clone()).expect("call 2");
    let flow = builder
        .finish((first_child.clone(), second_child.clone()))
        .expect("finish");
    let (first_position, second_position) = (
        first_child.position().clone(),
        second_child.position().clone(),
    );
    let outcome = run_flow_in_root(
        &flow,
        vec![root_input(&first, 3u32), root_input(&second, 5u32)],
        move |view| {
            assert_eq!(*view.resolve::<u64>(&first_position)?, 4);
            assert_eq!(*view.resolve::<u64>(&second_position)?, 6);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "reuse sample must run: {outcome:?}");
    assert_eq!(take_events(), vec!["inc", "inc"]);

    // 直接经两个调用位置执行，核对 child ScopeId 不同且都关闭。
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let result = drive(async move {
        execution
            .context_mut()
            .register_owned(&root, first.position(), 1u32)
            .expect("root input 1");
        execution
            .context_mut()
            .register_owned(&root, second.position(), 2u32)
            .expect("root input 2");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let mut children = Vec::new();
        for step in flow.definition().steps() {
            match step.site() {
                super::builder::CallSite::Orchestrator(site) => {
                    children.push(site.invoke(&mut guard, &root).await?);
                }
                _ => panic!("completed flow steps are orchestrator call sites"),
            }
        }
        assert_eq!(children.len(), 2);
        assert_ne!(
            children[0], children[1],
            "each call position gets its own ScopeId"
        );
        for child_scope in children {
            assert!(matches!(
                guard.state(&child_scope)?,
                super::scope::ScopeState::Closed
            ));
        }
        assert_eq!(guard.frame_depth(), 1);
        guard.complete();
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "call sites must run: {result:?}");
}

// ---- F11：同一定义多 Execution 与边界复用 ----

#[test]
fn f11_one_definition_supports_multiple_executions_and_both_call_modes() {
    reset();
    let (flow, input) = flow_inc();
    let definition_ptr = std::ptr::from_ref(flow.definition());
    let input_position = input.position().clone();
    let output_position = flow.definition().output_ports()[0].position().clone();
    let observed = RefCell::new(Vec::new());
    for value in [1u32, 2u32] {
        let (position, produced) = (input_position.clone(), output_position.clone());
        let sink = &observed;
        let outcome = run_flow_in_root(&flow, vec![root_input(&input, value)], move |view| {
            assert_eq!(*view.resolve::<u64>(&produced)?, u64::from(value) + 1);
            sink.borrow_mut().push((
                value,
                view.data_id_of(&position)?,
                view.data_id_of(&produced)?,
            ));
            Ok(())
        });
        assert!(outcome.is_ok(), "repeated execution must run: {outcome:?}");
    }
    let observed = observed.into_inner();
    assert_eq!(observed.len(), 2);
    // 同一逻辑 RefId（两次都用同一位置）；不同 DataId 身份；结果随输入重新计算。
    assert_eq!(observed[0].0, 1);
    assert_eq!(observed[1].0, 2);
    assert_ne!(
        observed[0].1, observed[1].1,
        "input identity differs per execution"
    );
    assert_ne!(
        observed[0].2, observed[1].2,
        "output identity differs per execution"
    );
    assert_eq!(observed[0].1.seq(), 0);
    assert_eq!(
        observed[1].1.seq(),
        0,
        "each execution restarts the identity space"
    );
    assert_eq!(observed[0].2.seq(), 1);
    assert_eq!(observed[1].2.seq(), 1);
    assert_eq!(std::ptr::from_ref(flow.definition()), definition_ptr);

    // 同一 Flow 定义分别作 Root 与 child：绑定与 owner 随调用边界变化。
    let mut parent = Definition::new();
    let caller_input = parent.declare_input::<u32>("a").expect("position");
    let child_out: DataRef<u64> = parent
        .then(flow.clone(), caller_input.clone())
        .expect("child mode");
    let child_out_position = child_out.position().clone();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let result = drive(async move {
        execution
            .context_mut()
            .register_owned(&root, caller_input.position(), 4u32)
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let child_scope = match parent.steps()[0].site() {
            super::builder::CallSite::Orchestrator(site) => site.invoke(&mut guard, &root).await?,
            _ => panic!("child call site"),
        };
        // child 模式：建立直接 child，输入 Import，输出责任转移到 Root。
        assert_eq!(guard.parent_of(&child_scope)?, Some(root.clone()));
        assert!(matches!(
            guard.state(&child_scope)?,
            super::scope::ScopeState::Closed
        ));
        assert_eq!(*guard.resolve::<u64>(&root, &child_out_position)?, 5);
        assert_eq!(guard.frame_depth(), 1);
        guard.complete();
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "child mode must run: {result:?}");

    // Root 模式：使用 RootScope，不建立额外 child。
    let outcome = run_flow_in_root(&flow, vec![root_input(&input, 9u32)], |view| {
        assert_eq!(view.probe().parent_of(view.root())?, None);
        Ok(())
    });
    assert!(outcome.is_ok(), "root mode must run: {outcome:?}");

    // 固定 Node 句柄可共享到两个调用位置（同一个 `Arc` 用于两步）。
    reset();
    let shared = std::sync::Arc::new(FLength);
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let widened: DataRef<u64> = builder.then(f_inc, input.clone()).expect("widen");
    let text: DataRef<String> = builder.then(f_text, widened.clone()).expect("text");
    let first: DataRef<usize> = builder
        .then(shared.clone(), text.clone())
        .expect("shared 1");
    let second: DataRef<usize> = builder
        .then(shared.clone(), text.clone())
        .expect("shared 2");
    let flow = builder
        .finish((first.clone(), second.clone()))
        .expect("finish");
    let outcome = run_flow_in_root(&flow, vec![root_input(&input, 7u32)], |_| Ok(()));
    assert!(outcome.is_ok(), "shared node handles must run: {outcome:?}");
    assert_eq!(take_events(), vec!["inc", "text", "len", "len"]);
}

// ---- F12：输出归属／声明拒绝 ----

#[test]
fn f12_finish_rejects_foreign_and_undeclared_output_positions() {
    reset();
    // 外来位置：另一条来源序列的同类型引用。
    let (left, _) = FlowBuilder::<(u32,)>::start().expect("builder");
    let (_right, right_input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let foreign_error = left
        .finish(right_input.clone())
        .expect_err("a foreign position must be rejected");
    assert_eq!(
        foreign_error,
        BuildError::ForeignPosition(right_input.position().clone())
    );

    // 同来源但未产出／未登记的位置（裸输出端口）：共享完成校验拒绝。
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let ghost = definition
        .declare_output_port::<u32>("ghost")
        .expect("bare port");
    let before_allocated = definition.allocated_probe();
    let ports_before = definition.output_ports().len();
    let error = definition
        .check_finish_outputs(&[super::signature::DeclaredPort::new::<u32>(
            ghost.position().clone(),
        )])
        .expect_err("a bare output port is not a legal completion source");
    assert_eq!(
        error,
        BuildError::UndeclaredPosition(ghost.position().clone())
    );
    assert_eq!(
        definition.allocated_probe(),
        before_allocated,
        "no position is consumed"
    );
    assert_eq!(
        definition.output_ports().len(),
        ports_before,
        "no partial declaration"
    );

    // 端口路径的重复声明与完成路径共用 DuplicateOutputPosition。
    let duplicate = definition
        .declare_output_port_for::<u32>(&input, "again")
        .and_then(|()| definition.declare_output_port_for::<u32>(&input, "twice"))
        .expect_err("the same position may not be declared twice");
    assert_eq!(
        duplicate,
        BuildError::DuplicateOutputPosition(input.position().clone())
    );

    // 合法来源：已声明输入与已产出 Step 位置都能通过整组校验。
    let mut legal = Definition::new();
    let legal_input = legal.declare_input::<u32>("a").expect("input");
    let produced: DataRef<u64> = legal.then(f_inc, legal_input.clone()).expect("step");
    legal
        .check_finish_outputs(&[
            super::signature::DeclaredPort::new::<u32>(input.position().clone()),
            super::signature::DeclaredPort::new::<u32>(legal_input.position().clone()),
            super::signature::DeclaredPort::new::<u64>(produced.position().clone()),
        ])
        .expect_err("a foreign position inside the group is still rejected");
    legal
        .check_finish_outputs(&[
            super::signature::DeclaredPort::new::<u32>(legal_input.position().clone()),
            super::signature::DeclaredPort::new::<u64>(produced.position().clone()),
        ])
        .expect("declared input and produced position are legal");
    take_events();
}

// ---- F13：完成声明的完整校验与判重 ----

#[test]
fn f13_completion_validates_the_whole_selection_and_rejects_duplicates() {
    reset();
    // 重复选择同一个输入位置（含 clone 句柄）。
    let (builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let duplicate_input = builder
        .finish((input.clone(), input.clone()))
        .expect_err("selecting one position twice must be rejected");
    assert_eq!(
        duplicate_input,
        BuildError::DuplicateOutputPosition(input.position().clone())
    );

    // 重复选择同一个 Step 输出位置。
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let widened: DataRef<u64> = builder.then(f_inc, input.clone()).expect("step");
    let duplicate_step = builder
        .finish((widened.clone(), widened.clone()))
        .expect_err("selecting one produced position twice must be rejected");
    assert_eq!(
        duplicate_step,
        BuildError::DuplicateOutputPosition(widened.position().clone())
    );

    // 后项非法：整组校验不写端口、不消耗位置、不执行任何业务体。
    let mut definition = Definition::new();
    let legal_input = definition.declare_input::<u32>("a").expect("input");
    let produced: DataRef<u64> = definition.then(f_inc, legal_input.clone()).expect("step");
    let foreign = {
        let (mut probe, probe_input) = FlowBuilder::<(u32,)>::start().expect("probe");
        let widened: DataRef<u64> = probe.then(f_inc, probe_input.clone()).expect("step");
        let _ = probe.finish(widened.clone()).expect("probe flow");
        widened
    };
    let ports_before = definition.output_ports().len();
    let allocated_before = definition.allocated_probe();
    let error = definition
        .check_finish_outputs(&[
            super::signature::DeclaredPort::new::<u64>(produced.position().clone()),
            super::signature::DeclaredPort::new::<u64>(foreign.position().clone()),
        ])
        .expect_err("the second output is foreign");
    assert_eq!(
        error,
        BuildError::ForeignPosition(foreign.position().clone())
    );
    assert_eq!(
        definition.output_ports().len(),
        ports_before,
        "no half completion"
    );
    assert_eq!(
        definition.allocated_probe(),
        allocated_before,
        "no Ref consumed"
    );
    assert!(
        take_events().is_empty(),
        "no business body runs during completion"
    );

    // 合法整组：校验通过后按选择顺序声明端口，输出 Signature 与端口一致。
    definition
        .check_finish_outputs(&[super::signature::DeclaredPort::new::<u64>(
            produced.position().clone(),
        )])
        .expect("legal selection");
    definition.declare_finish_outputs(vec![super::signature::DeclaredPort::new::<u64>(
        produced.position().clone(),
    )]);
    assert_eq!(definition.output_ports().len(), 1);
    assert_eq!(definition.output_ports()[0].expected(), TypeId::of::<u64>());

    // 错类型选择：位置的真实声明类型与选择类型不一致时必须拒绝（内部套类型同样被拒绝）。
    let (builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let forged = DataRef::<String>::from_position(input.position().clone());
    let error = builder
        .finish(forged)
        .expect_err("a mismatched input type must be refused");
    assert!(
        matches!(error, BuildError::OutputTypeMismatch { .. }),
        "{error:?}"
    );
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let produced: DataRef<u64> = builder.then(f_inc, input.clone()).expect("step");
    let forged = DataRef::<String>::from_position(produced.position().clone());
    let error = builder
        .finish(forged)
        .expect_err("a mismatched produced type must be refused");
    assert!(
        matches!(error, BuildError::OutputTypeMismatch { .. }),
        "{error:?}"
    );

    // 首项合法、后项错类型：整组拒绝，端口不部分声明、序号不消耗、无 body 运行。
    let mut definition = Definition::new();
    let legal_input = definition.declare_input::<u32>("a").expect("input");
    let produced: DataRef<u64> = definition.then(f_inc, legal_input.clone()).expect("step");
    let ports_before = definition.output_ports().len();
    let allocated_before = definition.allocated_probe();
    let error = definition
        .check_finish_outputs(&[
            super::signature::DeclaredPort::new::<u64>(produced.position().clone()),
            super::signature::DeclaredPort::with_type(
                legal_input.position().clone(),
                TypeId::of::<String>(),
                "String",
            ),
        ])
        .expect_err("the second output declares a different type than the position");
    assert!(
        matches!(error, BuildError::OutputTypeMismatch { .. }),
        "{error:?}"
    );
    assert_eq!(
        definition.output_ports().len(),
        ports_before,
        "no half completion"
    );
    assert_eq!(
        definition.allocated_probe(),
        allocated_before,
        "no Ref consumed"
    );
    assert!(
        take_events().is_empty(),
        "no business body runs during completion"
    );

    // 合法 Flow：两个不同位置正常完成。
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let widened: DataRef<u64> = builder.then(f_inc, input.clone()).expect("step");
    let flow = builder
        .finish((widened.clone(), input.clone()))
        .expect("distinct positions complete");
    assert_eq!(flow.definition().output_ports().len(), 2);
    take_events();
}

// ---- F16：tuple Data 与两个位置 ----

#[test]
fn f16_a_tuple_data_stays_one_position_while_two_positions_stay_separate() {
    reset();
    let (mut builder, tuple_input) = FlowBuilder::<((u32, u64),)>::start().expect("builder");
    let text: DataRef<String> = builder
        .then(f_tuple, tuple_input.clone())
        .expect("tuple leaf");
    let flow = builder.finish(text.clone()).expect("finish");
    assert_eq!(flow.definition().inputs().len(), 1);
    assert_eq!(
        flow.definition().inputs()[0].expected(),
        TypeId::of::<(u32, u64)>()
    );
    let text_position = text.position().clone();
    let outcome = run_flow_in_root(
        &flow,
        vec![root_input(&tuple_input, (2u32, 3u64))],
        move |view| {
            assert_eq!(*view.resolve::<String>(&text_position)?, "2/3");
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "tuple data input must run: {outcome:?}");

    // 两个独立位置：按位置区分，不按类型猜实例。
    let (mut builder, (first, second)) = FlowBuilder::<(u32, u64)>::start().expect("builder");
    let joined: DataRef<String> = builder
        .then(f_join2, (first.clone(), second.clone()))
        .expect("two positions");
    let flow = builder.finish(joined.clone()).expect("finish");
    assert_eq!(flow.definition().inputs().len(), 2);
    assert_ne!(
        TypeId::of::<Data<(u32, u64)>>(),
        TypeId::of::<Out2<u32, u64>>()
    );
    let joined_position = joined.position().clone();
    let outcome = run_flow_in_root(
        &flow,
        vec![root_input(&first, 4u32), root_input(&second, 5u64)],
        move |view| {
            assert_eq!(*view.resolve::<String>(&joined_position)?, "4/5");
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "two positions must run: {outcome:?}");
    assert_eq!(take_events(), vec!["tuple", "join2"]);
}

// ---- F17：R7 unit 与显式 unit 回归 ----

/// 普通同步 unit 函数（R7 方案 A：构建期拒绝）。
fn f_unit_sync(_a: &u32) -> Result<(), BodyError> {
    record("unit:sync");
    Ok(())
}

/// 普通异步 unit 函数（R7 方案 A：构建期拒绝）。
async fn f_unit_async(_a: &u32) -> Result<(), BodyError> {
    record("unit:async");
    Ok(())
}

#[test]
fn f17_plain_unit_functions_stay_rejected_while_explicit_unit_runs() {
    reset();
    // 普通同步／异步 unit 函数：构建拒绝，不追加 Step、不消耗位置、不运行 body。
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let steps_before = builder.definition_probe().step_count();
    let allocated_before = builder.allocated_probe();
    let error = builder
        .then(f_unit_sync, input.clone())
        .expect_err("ordinary sync unit output stays unsupported");
    assert_eq!(error, BuildError::UnsupportedFunctionUnitOutput);
    let error = builder
        .then(f_unit_async, input.clone())
        .expect_err("ordinary async unit output stays unsupported");
    assert_eq!(error, BuildError::UnsupportedFunctionUnitOutput);
    assert_eq!(builder.definition_probe().step_count(), steps_before);
    assert_eq!(builder.allocated_probe(), allocated_before);
    assert!(take_events().is_empty(), "rejected wiring runs no body");

    // 显式 unit：零 Step unit SubFlow + 结构体 unit Node 都正常执行；unit 不分配 DataId。
    let (unit_child, _) = {
        let (builder, child_input) = FlowBuilder::<(u32,)>::start().expect("builder");
        (builder.finish(()).expect("finish unit child"), child_input)
    };
    assert_eq!(unit_child.definition().step_count(), 0);
    let (mut parent, (first, probe)) = FlowBuilder::<(u32, u32)>::start().expect("builder");
    let unit_step: Result<(), BuildError> = parent.then(unit_child.clone(), first.clone());
    assert!(unit_step.is_ok(), "unit SubFlow stays supported");
    let widened: DataRef<u64> = parent.then(f_inc, first.clone()).expect("widen");
    let count: DataRef<usize> = parent.then(f_count, widened.clone()).expect("count");
    let _touch: Result<(), BuildError> = parent.then(FTouch, count.clone());
    let flow = parent.finish(count.clone()).expect("finish parent");
    let count_position = count.position().clone();
    // 未使用的声明输入作为 DataId 序号探针：unit 步骤没有消耗任何数据身份。
    let probe_position = probe.position().clone();
    let outcome = run_flow_in_root(&flow, vec![root_input(&first, 2u32)], move |view| {
        assert_eq!(*view.resolve::<usize>(&count_position)?, 3);
        let probe_id = view.register(&probe_position, 0u8)?;
        assert_eq!(
            probe_id.seq(),
            3,
            "root input, widen output, count output, probe: unit allocates no DataId"
        );
        Ok(())
    });
    assert!(outcome.is_ok(), "unit signatures must run: {outcome:?}");
    assert_eq!(take_events(), vec!["inc", "count", "touch"]);
}

// ---- F18：后序错误清理已接收输出 ----

#[test]
fn f18_a_later_failure_cleans_received_outputs_exactly_once() {
    reset();
    install_gate();
    // child：产出新的 owned 输出。
    let (mut child_builder, child_input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let produced: DataRef<FTracked> = child_builder
        .then(FTemp("received-dropped"), child_input.clone())
        .expect("child output");
    let child = child_builder
        .finish(produced.clone())
        .expect("finish child");

    // parent：接收 child 输出 → 产出自己的临时值 → 下一真实 Node 经 Pending 失败 → 后续 Step 不运行。
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let received: DataRef<FTracked> = builder
        .then(child.clone(), input.clone())
        .expect("child call");
    let temporary: DataRef<FTracked> = builder
        .then(FTemp("parent-temp-dropped"), input.clone())
        .expect("parent temporary");
    let after: DataRef<usize> = builder
        .then(f_fail_tracked, received.clone())
        .expect("failing leaf");
    let _ = (after, temporary);
    let later: DataRef<usize> = builder
        .then(FTempRead("after-failure"), received.clone())
        .expect("later step");
    let flow = builder.finish(later.clone()).expect("finish parent");

    let outcome = run_flow_root(&flow, vec![root_input(&input, 1u32)]);
    let error = outcome.expect_err("the gated node must fail the flow");
    assert_eq!(error.note(), "later failure");
    let events = take_events();
    assert_eq!(
        events[0],
        "received-dropped"
            .replace("received-dropped", "fail-tracked")
            .as_str()
    );
    let mut drops = events[1..].to_vec();
    drops.sort();
    assert_eq!(
        drops,
        vec!["parent-temp-dropped", "received-dropped"],
        "each value drops exactly once: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "after-failure"),
        "later steps must not run: {events:?}"
    );
}

// ---- F19：child 失败与 ancestor 保留 ----

#[test]
fn f19_a_child_failure_exits_descendants_before_owners_and_keeps_ancestors() {
    reset();
    install_gate();
    // mid：真实 Node 在挂起点后失败。
    let (mut mid_builder, mid_input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let widened: DataRef<u64> = mid_builder.then(f_inc, mid_input.clone()).expect("step");
    let count: DataRef<usize> = mid_builder.then(f_count, widened.clone()).expect("step");
    let failing: DataRef<String> = mid_builder.then(f_fail, count.clone()).expect("step");
    let mid = mid_builder.finish(failing.clone()).expect("finish mid");

    // root：Step 1 调用 mid。
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let from_mid: DataRef<String> = builder.then(mid.clone(), input.clone()).expect("mid call");
    let flow = builder.finish(from_mid.clone()).expect("finish root");
    let input_position = input.position().clone();

    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let result = drive(async move {
        let input_id = execution
            .context_mut()
            .register_owned(&root, &input_position, 6u32)
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let error = run_definition(&mut guard, flow.definition())
            .await
            .expect_err("the deepest node fails");
        assert_eq!(error.note(), "node failed");
        let termination = guard.termination().expect("first cause is preserved");
        assert_eq!(termination.kind(), TerminationKind::BodyError);
        assert_eq!(termination.note(), "node failed");
        let failing_scope = termination.scope().expect("actual call scope").clone();
        assert_ne!(
            failing_scope, root,
            "the failure is located in the descendant call"
        );
        // 实际 child 身份与父子关系：最深 frame 就是 mid 的 Boundary child。
        let children = boundary_child_scope_snapshot();
        assert_eq!(
            children.len(),
            1,
            "the mid subflow established one real boundary child"
        );
        let mid_child = children[0].clone();
        assert_eq!(failing_scope, mid_child);
        assert_eq!(guard.parent_of(&mid_child)?, Some(root.clone()));
        assert!(matches!(
            guard.state(&mid_child)?,
            super::scope::ScopeState::Closed
        ));
        assert!(matches!(
            guard.state(&root)?,
            super::scope::ScopeState::Active
        ));
        assert!(guard.alive_probe(&input_id));
        assert_eq!(guard.owner_probe(&input_id)?, root);
        assert_eq!(guard.frame_depth(), 1, "only the Root frame remains");
        // descendant 退出／清理先于 owner：owner 的清理与 frame 退出此时尚未发生。
        let before_owner = take_shared_events();
        let descendant_exit = before_owner
            .iter()
            .position(|event| event.starts_with("frame-exit:boundary:"))
            .expect("the boundary frame exits before its owner");
        assert!(
            before_owner[descendant_exit]
                .starts_with(&format!("frame-exit:boundary:{}", mid_child.seq())),
            "the exiting boundary frame is the observed child: {before_owner:?}"
        );
        assert!(
            !before_owner
                .iter()
                .any(|event| event.starts_with("frame-exit:root")),
            "the owner frame is still alive: {before_owner:?}"
        );
        guard.failed_with(&error);
        Ok::<(), BodyError>(())
    });
    assert!(
        result.is_ok(),
        "the driver records the failure itself and closes the root frame: {result:?}"
    );
    // owner 的清理／frame 退出只出现在 descendant 退出之后的批次。
    let after_owner = take_shared_events();
    let owner_exit = after_owner
        .iter()
        .position(|event| event.starts_with("frame-exit:root"))
        .expect("owner cleanup and frame exit happen after the descendant exited");
    assert!(
        after_owner[..owner_exit]
            .iter()
            .any(|event| event.starts_with("cleanup-start:0")),
        "owner cleanup precedes its frame exit: {after_owner:?}"
    );

    // 另一 Execution 不受影响。
    reset();
    install_gate();
    let (mut ok_builder, ok_input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let widened: DataRef<u64> = ok_builder.then(f_inc, ok_input.clone()).expect("step");
    let text: DataRef<String> = ok_builder.then(f_text, widened.clone()).expect("step");
    let ok_flow = ok_builder.finish(text.clone()).expect("finish");
    let outcome = run_flow_root(&ok_flow, vec![root_input(&ok_input, 2u32)]);
    assert!(outcome.is_ok(), "another execution must run: {outcome:?}");
    assert_eq!(take_events(), vec!["inc", "text"]);
}

// ---- F20：真实 Flow 的 Export 后项失败 ----

#[test]
fn f20_alias_supplement_imported_output_is_not_partially_bound() {
    reset();
    // child：首项重新暴露 imported 输入（可观察 DataId），后项产出新的带见证值。
    let (mut child_builder, (first, second)) = FlowBuilder::<(u32, u32)>::start().expect("builder");
    let alias: DataRef<u32> = first.clone();
    let produced: DataRef<FTracked> = child_builder
        .then(FTemp("child-second-dropped"), second.clone())
        .expect("second output");
    let child = child_builder
        .finish((alias.clone(), produced.clone()))
        .expect("finish child");

    // parent：Step 1 调用 child；第二个 caller 输出位置被预先占用。
    let (mut builder, (left, right)) = FlowBuilder::<(u32, u32)>::start().expect("builder");
    let (first_out, second_out): (DataRef<u32>, DataRef<FTracked>) = builder
        .then(child.clone(), (left.clone(), right.clone()))
        .expect("child call");
    let first_position = first_out.position().clone();
    let second_position = second_out.position().clone();
    let flow = builder.finish(()).expect("finish parent");
    let (left_position, right_position) = (left.position().clone(), right.position().clone());

    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let result = drive(async move {
        let left_id = execution
            .context_mut()
            .register_owned(&root, &left_position, 1u32)
            .expect("root input 1");
        let right_id = execution
            .context_mut()
            .register_owned(&root, &right_position, 2u32)
            .expect("root input 2");
        let occupied = execution
            .context_mut()
            .register_owned(&root, &second_position, FTracked("prebound-kept"))
            .expect("pre-bound caller position");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let baseline = guard.snapshot_probe(&root)?;
        let error = run_definition(&mut guard, flow.definition())
            .await
            .expect_err("a pre-bound caller position must fail the whole export");
        assert_eq!(error.note(), "scope operation failed");
        let diagnostic = guard
            .termination()
            .and_then(|termination| termination.scope_error())
            .map(|diagnostic| format!("{diagnostic}"))
            .expect("the export precheck diagnostic is preserved");
        assert!(diagnostic.contains("is already bound"), "{diagnostic}");
        // 整组拒绝：caller 快照逐项不变（首项 imported alias 没有被绑定，也没有责任转移）。
        assert_eq!(guard.snapshot_probe(&root)?, baseline, "no partial commit");
        let refs = baseline.0.clone();
        assert_eq!(
            refs.iter().filter(|(_, id)| *id == left_id).count(),
            1,
            "the imported input is only bound at its own position"
        );
        assert!(guard.alive_probe(&left_id) && guard.alive_probe(&right_id));
        assert_eq!(guard.owner_probe(&left_id)?, root);
        assert!(guard.alive_probe(&occupied));
        assert_eq!(guard.owner_probe(&occupied)?, root);
        // 后项新值由 child 责任清理一次；首项（imported）不需要清理。
        let events = take_events();
        assert_eq!(
            events
                .iter()
                .filter(|event| *event == "child-second-dropped")
                .count(),
            1,
            "{events:?}"
        );
        assert!(
            !events.iter().any(|event| event == "prebound-kept"),
            "the pre-bound caller value must still be alive: {events:?}"
        );
        let _ = (first_position, second_position, right_id);
        guard.failed_with(&error);
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "driver records the reject: {result:?}");
    let events = take_events();
    assert_eq!(
        events
            .iter()
            .filter(|event| *event == "prebound-kept")
            .count(),
        1,
        "{events:?}"
    );
}

#[test]
fn f20_a_late_export_validation_failure_rejects_two_new_owned_outputs() {
    reset();
    // child：两个**新 owned 输出**（首项合法，尽可转移），各有 Drop 见证。
    let (mut child_builder, (first, second)) = FlowBuilder::<(u32, u32)>::start().expect("builder");
    let first_out: DataRef<FTracked> = child_builder
        .then(FTemp("child-first-dropped"), first.clone())
        .expect("first output");
    let second_out: DataRef<FTracked> = child_builder
        .then(FTemp("child-second-dropped"), second.clone())
        .expect("second output");
    let child = child_builder
        .finish((first_out.clone(), second_out.clone()))
        .expect("finish child");
    let first_inner = child.definition().output_ports()[0].position().clone();
    let second_inner = child.definition().output_ports()[1].position().clone();

    // parent：Step 1 调用 child；第二个 caller 输出位置被预先占用（后项在提交前失败）。
    let (mut builder, (left, right)) = FlowBuilder::<(u32, u32)>::start().expect("builder");
    let (first_caller, second_caller): (DataRef<FTracked>, DataRef<FTracked>) = builder
        .then(child.clone(), (left.clone(), right.clone()))
        .expect("child call");
    let conflict_position = second_caller.position().clone();
    let flow = builder.finish(()).expect("finish parent");
    let (left_position, right_position) = (left.position().clone(), right.position().clone());
    let _ = first_caller;

    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let result = drive(async move {
        let left_id = execution
            .context_mut()
            .register_owned(&root, &left_position, 1u32)
            .expect("root input 1");
        let right_id = execution
            .context_mut()
            .register_owned(&root, &right_position, 2u32)
            .expect("root input 2");
        let occupied = execution
            .context_mut()
            .register_owned(&root, &conflict_position, FTracked("prebound-kept"))
            .expect("pre-bound caller position");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let baseline = guard.snapshot_probe(&root)?;
        let error = run_definition(&mut guard, flow.definition())
            .await
            .expect_err("a pre-bound caller position must fail the whole export");
        assert_eq!(error.note(), "scope operation failed");
        let diagnostic = guard
            .termination()
            .and_then(|termination| termination.scope_error())
            .map(|diagnostic| format!("{diagnostic}"))
            .expect("the export precheck diagnostic is preserved");
        assert!(diagnostic.contains("is already bound"), "{diagnostic}");
        // 提交前由真实调用边界记录 child 的本地绑定与责任集合：两项新输出都在 child 名下。
        let attempts = export_attempt_snapshot();
        assert_eq!(attempts.len(), 1, "one export attempt was observed");
        let (child_scope, child_refs, child_owned) = attempts[0].clone();
        let id_of = |position: &RefId| {
            child_refs
                .iter()
                .find(|(candidate, _)| candidate == position)
                .map(|(_, id)| id.clone())
                .expect("child-local position is bound")
        };
        let (first_id, second_id) = (id_of(&first_inner), id_of(&second_inner));
        assert_ne!(first_id, second_id, "two distinct new instances");
        assert!(child_owned.contains(&first_id) && child_owned.contains(&second_id));
        // 整组拒绝：caller 快照逐项不变（两项都没有绑定或转移给 caller）。
        assert_eq!(guard.snapshot_probe(&root)?, baseline, "no partial commit");
        assert!(
            !baseline
                .0
                .iter()
                .any(|(_, id)| id == &first_id || id == &second_id),
            "neither new output reached a caller position"
        );
        assert!(
            !guard.alive_probe(&first_id) && !guard.alive_probe(&second_id),
            "the child cleaned both new outputs"
        );
        let events = take_events();
        for witness in ["child-first-dropped", "child-second-dropped"] {
            assert_eq!(
                events.iter().filter(|event| *event == witness).count(),
                1,
                "each new owned output drops exactly once: {events:?}"
            );
        }
        // 占位值未被销毁或转移，仍归 caller。
        assert!(guard.alive_probe(&occupied));
        assert_eq!(guard.owner_probe(&occupied)?, root);
        assert!(guard.alive_probe(&left_id) && guard.alive_probe(&right_id));
        assert_eq!(guard.owner_probe(&left_id)?, root);
        let _ = child_scope;
        guard.failed_with(&error);
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "driver records the reject: {result:?}");
    let events = take_events();
    assert_eq!(
        events
            .iter()
            .filter(|event| *event == "prebound-kept")
            .count(),
        1,
        "{events:?}"
    );
}

// ---- F21／F22：真实 Flow Future 的取消 ----

/// 借用见证守卫：Drop 即"借用结束"。
struct FBorrowGuard;

impl Drop for FBorrowGuard {
    fn drop(&mut self) {
        record("borrow-end");
    }
}

/// 在挂起点持有输入借用的 Leaf：`&u64 -> String`。
async fn f_borrow_hold(a: &u64) -> Result<String, BodyError> {
    record("borrow-start");
    let _guard = FBorrowGuard;
    gate_wait().await;
    Ok(format!("#{a}"))
}

/// 内层完整 Flow：临时值 + 持有借用的 gated Leaf。
fn gated_inner_flow() -> Flow<(u32,), Data<String>> {
    let (mut inner, inner_input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let temporary: DataRef<FTracked> = inner
        .then(FTemp("inner-temp-dropped"), inner_input.clone())
        .expect("inner temporary");
    let widened: DataRef<u64> = inner.then(f_inc, inner_input.clone()).expect("widen");
    let held: DataRef<String> = inner
        .then(f_borrow_hold, widened.clone())
        .expect("gated leaf");
    let _ = (temporary, widened);
    inner.finish(held.clone()).expect("finish inner")
}

/// 外层完整 Flow：临时值 + 调用内层完整 Flow。
fn gated_outer_flow(inner: &Flow<(u32,), Data<String>>) -> Flow<(u32,), Data<String>> {
    let (mut outer, outer_input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let temporary: DataRef<FTracked> = outer
        .then(FTemp("outer-temp-dropped"), outer_input.clone())
        .expect("outer temporary");
    let held: DataRef<String> = outer
        .then(inner.clone(), outer_input.clone())
        .expect("inner call");
    let _ = &temporary;
    outer.finish(held.clone()).expect("finish outer")
}

/// Root 侧包装 Flow：祖先临时值 + 调用外层完整 Flow。
///
/// 取消样本在 Root frame（ancestor Scope）中直接丢弃这一步的 erased Future，
/// 得到 "ancestor → 外层 Flow child → 内层 Flow child → Leaf" 的真实链。
#[allow(clippy::type_complexity)]
fn wrapper_with_outer(
    outer: &Flow<(u32,), Data<String>>,
) -> (Flow<(u32,), Data<String>>, DataRef<u32>) {
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let temporary: DataRef<FTracked> = builder
        .then(FTemp("root-temp-dropped"), input.clone())
        .expect("root temporary");
    let called: DataRef<String> = builder
        .then(outer.clone(), input.clone())
        .expect("outer call");
    let _ = &temporary;
    (
        builder.finish(called.clone()).expect("finish wrapper"),
        input,
    )
}

/// 事件序列中 `name` 的下标（缺失即失败）。
fn position_of(events: &[String], name: &str) -> usize {
    events
        .iter()
        .position(|event| event == name)
        .unwrap_or_else(|| panic!("missing `{name}` in {events:?}"))
}

#[test]
fn f21_dropping_the_erased_outer_flow_future_cleans_both_layers_inner_first() {
    reset();
    install_gate();
    let inner = gated_inner_flow();
    let outer = gated_outer_flow(&inner);
    let (wrapper, wrapper_input) = wrapper_with_outer(&outer);

    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let input_position = wrapper_input.position().clone();
    let driver_input = input_position.clone();
    let driver_wrapper = wrapper.clone();
    let result = drive(async move {
        let input_id = execution
            .context_mut()
            .register_owned(&root, &driver_input, 5u32)
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        // 真实调用链：ancestor Root frame → 外层完整 Flow child → 内层完整 Flow child → 借用 Leaf。
        let outer_future = match driver_wrapper.definition().steps()[1].site() {
            super::builder::CallSite::Orchestrator(site) => site.invoke(&mut guard, &root),
            _ => panic!("the inner call is an orchestrator site"),
        };
        let boxed = advance_to_pending(outer_future, 1);
        assert_eq!(
            take_events(),
            vec!["inc", "borrow-start"],
            "the deepest leaf holds the borrow at the pending stop"
        );
        let children = boundary_child_scope_snapshot();
        assert_eq!(children.len(), 2, "outer and inner Flow boundaries exist");

        // 丢弃外层 erased Future 本体：两层 Boundary 自动退出，由内到外。
        drop(boxed);
        let (outer_child, inner_child) = (children[0].clone(), children[1].clone());
        assert_eq!(guard.parent_of(&outer_child)?, Some(root.clone()));
        assert_eq!(guard.parent_of(&inner_child)?, Some(outer_child.clone()));
        let events = take_shared_events();
        let borrow_end = position_of(&events, "borrow-end");
        let window = &events[borrow_end..];
        let offset = |name: &str| position_of(window, name);
        let leaf_exit = window
            .iter()
            .position(|event| event.starts_with("frame-exit:leaf:"))
            .expect("the deepest leaf frame exits");
        let inner_cleanup = offset("inner-temp-dropped");
        let boundary_exits: Vec<usize> = window
            .iter()
            .enumerate()
            .filter(|(_, event)| event.starts_with("frame-exit:boundary:"))
            .map(|(index, _)| index)
            .collect();
        assert_eq!(
            boundary_exits.len(),
            2,
            "both Flow boundaries exit: {window:?}"
        );
        let outer_cleanup = offset("outer-temp-dropped");
        assert!(
            leaf_exit < inner_cleanup
                && inner_cleanup < boundary_exits[0]
                && boundary_exits[0] < outer_cleanup
                && outer_cleanup < boundary_exits[1],
            "inner cleanup／frame exit before outer cleanup／frame exit: {window:?}"
        );
        // Pending 取消窗口内每层 owned 值恰好 Drop 一次（不借用 Ready 段的次数）。
        for witness in ["inner-temp-dropped", "outer-temp-dropped"] {
            assert_eq!(
                window.iter().filter(|event| *event == witness).count(),
                1,
                "`{witness}` must drop exactly once in the cancellation window: {window:?}"
            );
        }
        // 两层 Closed、ancestor 责任保留、取消定位在实际最深 frame Scope。
        for child in [&outer_child, &inner_child] {
            assert!(matches!(
                guard.state(child)?,
                super::scope::ScopeState::Closed
            ));
        }
        assert_eq!(
            guard
                .termination()
                .and_then(|termination| termination.scope()),
            Some(&inner_child),
            "cancellation is located at the actual deepest frame scope"
        );
        assert!(guard.alive_probe(&input_id));
        assert_eq!(guard.owner_probe(&input_id)?, root);
        assert!(matches!(
            guard.state(&root)?,
            super::scope::ScopeState::Active
        ));
        assert_eq!(guard.frame_depth(), 1);
        assert!(guard.is_terminated());
        guard
            .abort(&root)
            .expect("controlled cleanup stays available");
        guard.exit_for_probe();
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "cancel sample must settle: {result:?}");
    let after = take_shared_events();
    // 外层 Future 已被丢弃：ancestor 的 frame 退出与 Context／Container 析构发生在两层退出之后。
    assert!(
        after
            .iter()
            .any(|event| event.starts_with("frame-exit:root")),
        "{after:?}"
    );
    assert!(
        after.iter().any(|event| event == "context-drop")
            && after.iter().any(|event| event == "container-drop"),
        "{after:?}"
    );

    // 同一入口的未 poll 对照：不运行任何业务体，也不留借用症状。
    reset();
    install_gate();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let unpolled_input = input_position.clone();
    let unpolled_wrapper = wrapper.clone();
    let result = drive(async move {
        execution
            .context_mut()
            .register_owned(&root, &unpolled_input, 5u32)
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let unpolled = match unpolled_wrapper.definition().steps()[1].site() {
            super::builder::CallSite::Orchestrator(site) => site.invoke(&mut guard, &root),
            _ => panic!("orchestrator site"),
        };
        drop(Box::pin(unpolled));
        assert!(
            take_events().is_empty(),
            "an unpolled future runs no business body"
        );
        assert!(
            boundary_child_scope_snapshot().is_empty(),
            "no child was established"
        );
        guard.complete();
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "{result:?}");

    // 同一入口的 Ready 后对照：正常完成时内层清理仍先于外层。
    reset();
    install_gate();
    let outcome = run_flow_root(&wrapper, vec![root_input(&wrapper_input, 5u32)]);
    assert!(outcome.is_ok(), "ready run must complete: {outcome:?}");
    let events = take_events();
    assert!(
        position_of(&events, "inner-temp-dropped") < position_of(&events, "outer-temp-dropped"),
        "{events:?}"
    );
}

#[test]
fn f22_dropping_the_root_owning_future_orders_boundaries_before_storage_teardown() {
    reset();
    install_gate();
    // Root-owning Future 内部的完整链：Root frame → 外层 Flow child → 内层 Flow child → Leaf。
    let inner = gated_inner_flow();
    let outer = gated_outer_flow(&inner);
    let (root_flow, root_input_handle) = wrapper_with_outer(&outer);
    let input_position = root_input_handle.position().clone();

    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, &input_position, 3u32)
        .expect("root input");
    let boxed = advance_to_pending(
        super::builder::run_definition_root(execution, root_flow.definition()),
        1,
    );
    assert_eq!(take_events(), vec!["inc", "borrow-start"]);
    assert_eq!(boundary_child_scope_snapshot().len(), 2);
    drop(boxed);
    let events = take_shared_events();
    let borrow_end = position_of(&events, "borrow-end");
    let window = &events[borrow_end..];
    let boundary_exits: Vec<usize> = window
        .iter()
        .enumerate()
        .filter(|(_, event)| event.starts_with("frame-exit:boundary:"))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(
        boundary_exits.len(),
        2,
        "both Flow boundaries exit: {window:?}"
    );
    let inner_cleanup = position_of(window, "inner-temp-dropped");
    let outer_cleanup = position_of(window, "outer-temp-dropped");
    let root_cleanup = position_of(window, "root-temp-dropped");
    let root_exit = window
        .iter()
        .position(|event| event.starts_with("frame-exit:root"))
        .expect("the root frame exits");
    let context_drop = position_of(window, "context-drop");
    let container_drop = position_of(window, "container-drop");
    assert!(
        borrow_end - borrow_end == 0
            && inner_cleanup < boundary_exits[0]
            && boundary_exits[0] < outer_cleanup
            && outer_cleanup < boundary_exits[1]
            && boundary_exits[1] < root_cleanup
            && root_cleanup < root_exit
            && root_exit < context_drop
            && context_drop < container_drop,
        "borrows release, both boundaries exit, then root frame and storage teardown: {window:?}"
    );
    assert_eq!(
        window
            .iter()
            .filter(|event| *event == "root-temp-dropped")
            .count(),
        1
    );
    // Pending 取消窗口内三层 owned 值各恰好 Drop 一次。
    for witness in [
        "inner-temp-dropped",
        "outer-temp-dropped",
        "root-temp-dropped",
    ] {
        assert_eq!(
            window.iter().filter(|event| *event == witness).count(),
            1,
            "`{witness}` must drop exactly once in the cancellation window: {window:?}"
        );
    }

    // 未 poll 就丢弃：不运行任何业务体。
    reset();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, &input_position, 3u32)
        .expect("root input");
    let unpolled = Box::pin(super::builder::run_definition_root(
        execution,
        root_flow.definition(),
    ));
    drop(unpolled);
    assert!(
        take_events().is_empty(),
        "an unpolled future runs no business body"
    );

    // Ready 之后丢弃：结果已收口，各值 Drop 一次。
    reset();
    install_gate();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, &input_position, 3u32)
        .expect("root input");
    let completed = Box::pin(super::builder::run_definition_root(
        execution,
        root_flow.definition(),
    ));
    drive_pinned(completed);
    let events = take_events();
    let mut drops: Vec<&String> = events
        .iter()
        .filter(|event| event.ends_with("-dropped"))
        .collect();
    drops.sort();
    assert_eq!(
        drops,
        vec![
            "inner-temp-dropped",
            "outer-temp-dropped",
            "root-temp-dropped"
        ],
        "{events:?}"
    );
    assert!(
        position_of(&events, "inner-temp-dropped") < position_of(&events, "outer-temp-dropped"),
        "{events:?}"
    );
}

// ---- F23：错误捕获不能恢复 ----

/// 首次终止诊断的完整快照：类别、note、实际调用 Scope、原 `ScopeError`。
#[allow(clippy::type_complexity)]
fn first_diagnostic(
    guard: &InvocationGuard<'_>,
) -> (
    Option<TerminationKind>,
    Option<&'static str>,
    Option<ScopeId>,
    Option<String>,
) {
    (
        guard.termination().map(|termination| termination.kind()),
        guard.termination().map(|termination| termination.note()),
        guard
            .termination()
            .and_then(|termination| termination.scope())
            .cloned(),
        guard
            .termination()
            .and_then(|termination| termination.scope_error())
            .map(|diagnostic| format!("{diagnostic}")),
    )
}

#[test]
fn f23_captured_errors_leave_the_execution_terminated() {
    reset();
    // 情形 1：业务调用前的输入解析失败（真实 Flow 的声明输入未登记）。
    let (flow, _) = flow_inc();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let result = drive(async move {
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let error = run_definition(&mut guard, flow.definition())
            .await
            .expect_err("an unbound input position fails before the body");
        let first = first_diagnostic(&guard);
        assert_eq!(first.0, Some(TerminationKind::BodyError));
        assert_eq!(
            first.2.as_ref(),
            Some(&root),
            "the reject happened in the Root frame"
        );
        assert!(
            first.3.is_some(),
            "the original scope diagnostic is preserved"
        );
        let before = take_events();
        assert!(run_definition(&mut guard, flow.definition()).await.is_err());
        assert_eq!(
            take_events(),
            before,
            "no business body runs after termination"
        );
        assert!(guard.finalize(&root, &[], &mut Vec::new()).is_err());
        // 首次诊断的四个字段都不被后续拒绝覆盖。
        assert_eq!(
            first_diagnostic(&guard),
            first,
            "the first diagnosis is immutable"
        );
        assert!(
            guard.snapshot_probe(&root).is_ok(),
            "read-only diagnostics stay available"
        );
        guard.failed_with(&error);
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "{result:?}");

    // 情形 2：child 输入装配失败（caller 位置未登记）。
    reset();
    let (child, _) = flow_inc();
    let mut parent = Definition::new();
    let caller_input = parent.declare_input::<u32>("a").expect("position");
    let _: DataRef<u64> = parent
        .then(child.clone(), caller_input.clone())
        .expect("child call");
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let result = drive(async move {
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let error = match parent.steps()[0].site() {
            super::builder::CallSite::Orchestrator(site) => site
                .invoke(&mut guard, &root)
                .await
                .expect_err("an unbound caller input fails the child assembly"),
            _ => panic!("child call site"),
        };
        assert_eq!(error.note(), "scope operation failed");
        let first = first_diagnostic(&guard);
        assert_eq!(first.0, Some(TerminationKind::BodyError));
        assert_eq!(
            first.1,
            Some("orchestrator input assembly failed"),
            "the boundary records the assembly failure as the first cause"
        );
        assert!(
            first.3.is_some(),
            "the original scope diagnostic is preserved"
        );
        // 实际 Scope 就是这次拒绝所在调用边界建立的 child。
        let children = boundary_child_scope_snapshot();
        assert_eq!(children.len(), 1, "the boundary established one child");
        assert_eq!(first.2.as_ref(), Some(&children[0]));
        let before = take_events();
        let again = match parent.steps()[0].site() {
            super::builder::CallSite::Orchestrator(site) => site.invoke(&mut guard, &root).await,
            _ => panic!("child call site"),
        };
        assert!(again.is_err());
        assert_eq!(
            take_events(),
            before,
            "no boundary or body runs after termination"
        );
        assert!(guard.finalize(&root, &[], &mut Vec::new()).is_err());
        assert_eq!(
            first_diagnostic(&guard),
            first,
            "the first diagnosis is immutable"
        );
        guard.failed_with(&error);
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "{result:?}");

    // 情形 3：真实 Flow Export 输出拒绝被捕获。
    reset();
    let (mut child_builder, (first, second)) = FlowBuilder::<(u32, u32)>::start().expect("builder");
    let _ = second;
    let produced: DataRef<u64> = child_builder.then(f_inc, first.clone()).expect("step");
    let alias: DataRef<u32> = first.clone();
    let exporting = child_builder
        .finish((produced.clone(), alias.clone()))
        .expect("finish child");
    let (mut parent, (left, right)) = FlowBuilder::<(u32, u32)>::start().expect("builder");
    let (_first_out, second_out): (DataRef<u64>, DataRef<u32>) = parent
        .then(exporting.clone(), (left.clone(), right.clone()))
        .expect("child call");
    let parent = parent.finish(()).expect("finish parent");
    let conflict_position = second_out.position().clone();
    let (left_position, right_position) = (left.position().clone(), right.position().clone());
    let (own_flow, _) = flow_inc();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let result = drive(async move {
        execution
            .context_mut()
            .register_owned(&root, &left_position, 1u32)
            .expect("root input 1");
        execution
            .context_mut()
            .register_owned(&root, &right_position, 2u32)
            .expect("root input 2");
        execution
            .context_mut()
            .register_owned(&root, &conflict_position, 9u32)
            .expect("pre-bound caller position");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let error = run_definition(&mut guard, parent.definition())
            .await
            .expect_err("the child export group is rejected");
        // 在显式 failed_with 之前：Context 已终止，首次类别／原因／Scope／ScopeError 都在。
        let first = first_diagnostic(&guard);
        assert_eq!(first.0, Some(TerminationKind::BodyError));
        assert_eq!(first.1, Some("scope operation failed"));
        assert!(
            first.3.is_some(),
            "the export precheck diagnostic is preserved"
        );
        // 实际 Scope 是这次 Export 拒绝所在调用边界建立的 child。
        let children = boundary_child_scope_snapshot();
        assert_eq!(children.len(), 1, "the child boundary was observed");
        assert_eq!(first.2.as_ref(), Some(&children[0]));
        assert!(matches!(
            guard.state(&root)?,
            super::scope::ScopeState::Active
        ));
        // 后续业务调用与普通 commit 都被拒绝，body 为 0。
        let ran_before_reject = take_events();
        assert_eq!(
            ran_before_reject,
            vec!["inc"],
            "the child business body ran before the export reject"
        );
        assert!(
            run_definition(&mut guard, own_flow.definition())
                .await
                .is_err()
        );
        assert!(
            take_events().is_empty(),
            "no business body runs after termination"
        );
        assert!(guard.finalize(&root, &[], &mut Vec::new()).is_err());
        assert_eq!(
            first_diagnostic(&guard),
            first,
            "the first diagnosis is immutable"
        );
        assert!(guard.snapshot_probe(&root).is_ok());
        guard.failed_with(&error);
        Ok::<(), BodyError>(())
    });
    assert!(result.is_ok(), "{result:?}");
    take_events();
}

// ---- F24：回归与私有边界 ----

#[test]
fn f24_nested_flows_share_one_execution_domain_and_data_id_sequence() {
    reset();
    // 嵌套层共用同一身份序列：child 新输出的 DataId 紧接 parent 输入，且单调。
    let (child, _) = flow_inc();
    let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let from_child: DataRef<u64> = builder
        .then(child.clone(), input.clone())
        .expect("child call");
    let count: DataRef<usize> = builder
        .then(f_count, from_child.clone())
        .expect("independent step");
    let flow = builder.finish(count.clone()).expect("finish");
    let (input_position, child_position, count_position) = (
        input.position().clone(),
        from_child.position().clone(),
        count.position().clone(),
    );
    let observed = (
        input_position.clone(),
        child_position.clone(),
        count_position.clone(),
    );
    let outcome = run_flow_in_root(&flow, vec![root_input(&input, 4u32)], move |view| {
        let input_id = view.data_id_of(&observed.0)?;
        let child_id = view.data_id_of(&observed.1)?;
        let count_id = view.data_id_of(&observed.2)?;
        assert_eq!(input_id.seq() + 1, child_id.seq());
        assert_eq!(child_id.seq() + 1, count_id.seq());
        assert_eq!(view.probe().owner_probe(&child_id)?, *view.root());
        assert_eq!(view.probe().owner_probe(&count_id)?, *view.root());
        assert_eq!(view.snapshot()?.len(), 3);
        Ok(())
    });
    assert!(outcome.is_ok(), "nested flow must run: {outcome:?}");
    assert_eq!(take_events(), vec!["inc", "count"]);

    // 另一个 Execution 从同一身份起点重新开始：DataId 不跨 Execution 使用。
    reset();
    let second_position = input_position.clone();
    let outcome = run_flow_in_root(&flow, vec![root_input(&input, 4u32)], move |view| {
        let input_id = view.data_id_of(&second_position)?;
        assert_eq!(
            input_id.seq(),
            0,
            "a new execution restarts the identity sequence"
        );
        Ok(())
    });
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(take_events(), vec!["inc", "count"]);
}

// ---- §5 主场景：真实父／子 Flow 链与唯一执行域证据 ----

/// §5 主场景：Root Flow → 同步 Node → Arc Node（跨 Pending）→ SubFlow（含嵌套 Flow 与未导出临时值）
/// → 后续 Node 读到已关闭 child 的输出 → 显式 unit 结构体 Node → 声明 Root 输出（只观察，不 take）。
///
/// 同时核对：观察前后完整 refs／owned 快照、各 owned 值 Drop 见证一次、执行身份／Coordinator／
/// Container 地址稳定与创建计数各 +1。
#[test]
fn main_scenario_root_flow_with_nested_subflows_and_single_execution_domain() {
    reset();
    install_gate();

    // 嵌套 Flow：把数值翻倍导出；另有未导出临时值。
    let (mut nested, nested_input) = FlowBuilder::<(u64, FTracked)>::start().expect("builder");
    let nested_temp: DataRef<FTracked> = nested
        .then(
            FTempFromTracked("nested-temp-dropped"),
            nested_input.1.clone(),
        )
        .expect("nested temporary");
    let doubled: DataRef<u64> = nested
        .then(f_double, nested_input.0.clone())
        .expect("nested output");
    let nested = nested.finish(doubled.clone()).expect("finish nested");
    assert_ne!(
        nested_temp.position(),
        nested.definition().output_ports()[0].position(),
        "the nested temporary is not an exported position"
    );

    // SubFlow：导出 (D, E) 两个位置，并保留未导出临时值。
    let (mut sub, (sub_number, sub_shared)) =
        FlowBuilder::<(u64, FTracked)>::start().expect("builder");
    let sub_temp: DataRef<FTracked> = sub
        .then(FTempFromTracked("sub-temp-dropped"), sub_shared.clone())
        .expect("sub temporary");
    let d: DataRef<u64> = sub
        .then(nested.clone(), (sub_number.clone(), sub_shared.clone()))
        .expect("nested call");
    let e: DataRef<usize> = sub.then(f_half, d.clone()).expect("second output");
    let sub = sub.finish((d.clone(), e.clone())).expect("finish sub");
    assert_ne!(sub_temp.position(), d.position());

    // Root Flow：输入 FTracked（跨 Pending 的 Arc Node）→ SubFlow → 后续读取 → 显式 unit。
    let (mut root, root_handle) = FlowBuilder::<(FTracked,)>::start().expect("builder");
    let widened: DataRef<u64> = root
        .then(std::sync::Arc::new(FWiden), root_handle.clone())
        .expect("arc node across pending");
    let (from_d, from_e): (DataRef<u64>, DataRef<usize>) = root
        .then(sub.clone(), (widened.clone(), root_handle.clone()))
        .expect("subflow call");
    let combined: DataRef<usize> = root
        .then(f_combine, (from_d.clone(), from_e.clone()))
        .expect("read after close");
    let _touch: Result<(), BuildError> = root.then(FTouch, combined.clone());
    let flow = root.finish(combined.clone()).expect("finish root");

    let (input_position, d_position, e_position, combined_position) = (
        root_handle.position().clone(),
        from_d.position().clone(),
        from_e.position().clone(),
        combined.position().clone(),
    );
    // 唯一执行域：计数基线在**创建这次 Execution 之前**取得；地址从本次执行的实际 Root
    // 与真实 nested 调用（边界记录）共同观测并逐项比较；只驱动一次实际 Execution。
    let counts_before = super::context::creation_counts::snapshot();
    let mut execution = RootExecution::start();
    let root_scope = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root_scope, &input_position, FTracked("root-input-dropped"))
        .expect("root input");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root_scope, true)
        .expect("root frame");
    let outcome = drive(async { run_definition(&mut guard, flow.definition()).await });
    assert!(outcome.is_ok(), "main scenario must run: {outcome:?}");
    // 创建计数增量各为一（基线取自创建之前）。
    let counts_after = super::context::creation_counts::snapshot();
    assert_eq!(counts_after.0, counts_before.0 + 1, "one Context");
    assert_eq!(counts_after.1, counts_before.1 + 1, "one Coordinator");
    assert_eq!(counts_after.2, counts_before.2 + 1, "one Container");
    let (d_position, e_position, combined_position) = (
        d_position.clone(),
        e_position.clone(),
        combined_position.clone(),
    );
    // 关闭前观察（同一执行域）：完整 refs／owned 快照、真实 nested 地址与 Root 地址逐项比较。
    let observation = (|| -> Result<(), BodyError> {
        let view = FlowRootView {
            guard: &mut guard,
            root: root_scope.clone(),
        };
        let before = view.snapshot_full()?;
        // 输入长度 18 → 翻倍 36 → 折半 18 → 合计 54。
        assert_eq!(*view.resolve::<u64>(&d_position)?, 36);
        assert_eq!(*view.resolve::<usize>(&e_position)?, 18);
        assert_eq!(*view.resolve::<usize>(&combined_position)?, 54);
        assert_eq!(
            view.snapshot_full()?,
            before,
            "observation changes no refs or owned"
        );
        assert!(view.root_is_active());
        assert_eq!(
            view.probe().frame_depth(),
            1,
            "all child frames have exited"
        );
        let identity = view.probe().identity_probe();
        let coordinator = view.probe().coordinator_probe();
        let container = view.probe().container_probe();
        assert!(!identity.is_null() && !coordinator.is_null() && !container.is_null());
        let recorded = boundary_address_snapshot();
        assert_eq!(
            recorded.len(),
            2,
            "outer and inner Flow boundaries were observed"
        );
        for (child, child_identity, child_coordinator, child_container) in recorded {
            assert_eq!(
                child_identity, identity,
                "nested call {child} shares the execution identity"
            );
            assert_eq!(child_coordinator, coordinator, "shared Coordinator");
            assert_eq!(child_container, container, "shared Container");
        }
        Ok(())
    })();
    assert!(
        observation.is_ok(),
        "observation must succeed: {observation:?}"
    );
    // 收口固定走空声明输出与空 ExportSlot。
    guard
        .finalize(&root_scope, &[], &mut Vec::new())
        .map_err(BodyError::from)
        .expect("empty-declaration root closure");
    guard.complete();
    drop(execution);
    // 收口之后：各子层与 Root 的 owned 值各由合法责任方清理一次。
    let events = take_events();
    let mut drops: Vec<&String> = events
        .iter()
        .filter(|event| event.ends_with("-dropped"))
        .collect();
    drops.sort();
    assert_eq!(
        drops,
        vec![
            "nested-temp-dropped",
            "root-input-dropped",
            "sub-temp-dropped"
        ],
        "{events:?}"
    );

    // 声明输出只观察、不 take：收口后 Root 已关闭且没有 owned 返回应用层。
    reset();
    let outcome = run_flow_in_root(
        &flow,
        vec![root_input(&root_handle, FTracked("root-2-dropped"))],
        move |view| {
            // 长度 14 → 28 → 14 → 42：同一 Flow 定义在两次 Execution 中重新计算。
            assert_eq!(*view.resolve::<u64>(&d_position)?, 28);
            assert_eq!(*view.resolve::<usize>(&e_position)?, 14);
            assert_eq!(*view.resolve::<usize>(&combined_position)?, 42);
            let (refs, owned) = view.snapshot_full()?;
            assert_eq!(refs.len(), 5, "input, widen, two subflow outputs, combined");
            assert_eq!(owned.len(), 5);
            assert!(!refs.is_empty());
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "second root run must settle: {outcome:?}");
    let _ = input_position;
}

/// 同步函数 Node：`&u64 -> u64`（乘二）。
fn f_double(a: &u64) -> Result<u64, BodyError> {
    record("double");
    Ok(*a * 2)
}

/// 异步函数 Node（跨 Pending）：`&FTracked -> u64`。
async fn f_widen_tracked(a: &FTracked) -> Result<u64, BodyError> {
    record("widen");
    gate_wait().await;
    Ok(a.0.len() as u64)
}

/// Arc 包装的跨 Pending Node。
struct FWiden;

impl NodeCall1<FTracked, Data<u64>> for FWiden {
    fn call<'a>(&'a self, a: &'a FTracked) -> NodeFut<'a, u64> {
        Box::pin(f_widen_tracked(a))
    }
}

/// 同步函数 Node：`&u64 -> usize`（折半）。
fn f_half(a: &u64) -> Result<usize, BodyError> {
    record("half");
    Ok((*a / 2) as usize)
}

/// 双输入同步 Node：`(&u64, &usize) -> usize`。
fn f_combine(a: &u64, b: &usize) -> Result<usize, BodyError> {
    record("combine");
    Ok(*a as usize + *b)
}
