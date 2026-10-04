//! V21-05 验收样本（E01～E24）。
//!
//! 这些样本是**内部**验收证据：它们使用 crate 内可见的 Definition／CallSite／协议类型，
//! 不构成公开 API 承诺，也不使用 `--cfg test` 之外的装配方式。编译负例（E07～E11、E24）
//! 单独放在 `tests/ui/`，按真实 `src/core` 装配独立编译。

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll, Waker};

use super::builder::{CallSite, Definition, TypedCallBuilder, run_definition};
use super::context::{BodyError, ExecutionContext, InvocationKind, TerminationKind};
use super::data_ref::DataRef;
use super::identity::ScopeId;
use super::node::{NodeCall0, NodeCall1, NodeCall2};
use super::orchestrator::{OrchCall, OrchScope, Targets1, Targets2};
use super::ref_id::RefId;
use super::runtime::RootExecution;
use super::signature::{BuildError, Data, NodeFut, Out2, Unit};

// ---- 测试基础设施（事件、挂起点、Future 驱动） ----

thread_local! {
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// 记录一个可观察事件（顺序即证据）。
///
/// 同时写入 V21-04 既有的 `context::creation_counts` 事件日志：业务 Drop 见证因此与
/// guard 清理／frame 退出事件处在同一条可比较序列上（R12 的 frame 次序证据）。
fn record(event: &str) {
    EVENTS.with(|events| events.borrow_mut().push(event.to_string()));
    super::context::creation_counts::record_event(event);
}

/// 取走 Context 侧共享日志（含 guard 清理与 frame 退出事件）。
fn take_shared_events() -> Vec<String> {
    super::context::creation_counts::take_events()
}

/// 取走当前线程的事件序列。
fn take_events() -> Vec<String> {
    EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

thread_local! {
    static PENDING_GATE: RefCell<Option<Rc<Cell<bool>>>> = const { RefCell::new(None) };
    static BODY_CALLS: Cell<usize> = const { Cell::new(0) };
}

/// 安装一个挂起点；异步业务体在 await 前调用 [`gate_wait`]。
fn install_gate() {
    PENDING_GATE.with(|gate| *gate.borrow_mut() = Some(Rc::new(Cell::new(false))));
}

/// 释放已安装的挂起点。
fn release_gate() {
    PENDING_GATE.with(|gate| {
        if let Some(open) = gate.borrow().as_ref() {
            open.set(true);
        }
    });
}

/// 在挂起点上等待：安装时先 Pending，释放后下一次 poll 返回。
async fn gate_wait() {
    let open = PENDING_GATE.with(|gate| gate.borrow().clone());
    if let Some(open) = open {
        std::future::poll_fn(|_| {
            if open.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

/// 推进到 Ready；每次 Pending 先释放挂起点。
fn drive<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let mut pending = 0usize;
    loop {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 64, "future is not making progress");
                release_gate();
            }
        }
    }
}

/// 推进一个已 boxed 的 Future 到 Ready；每次 Pending 先释放挂起点。
fn drive_pinned<F: Future + ?Sized>(mut boxed: Pin<Box<F>>) -> F::Output {
    let mut pending = 0usize;
    loop {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match boxed.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 64, "future is not making progress");
                release_gate();
            }
        }
    }
}

/// 推进到第 `stops` 次 Pending 后停下，返回仍持有 Future 本体的 Box。
///
/// 丢弃这个 Box 才是"丢弃 Future 本体"；只丢一个 `Pin<&mut F>` 或引用不算。
fn advance_to_pending<F: Future>(future: F, stops: usize) -> Pin<Box<F>> {
    let mut boxed = Box::pin(future);
    for stop in 1..=stops {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match boxed.as_mut().poll(&mut cx) {
            Poll::Pending => {}
            Poll::Ready(_) => panic!("future completed before pending stop {stop}"),
        }
    }
    boxed
}

/// 非 `Clone`、非 `Send` 的业务值：用于证明数据路径不引入额外 bound。
struct LocalOnly {
    value: u32,
    marker: Rc<Cell<u32>>,
}

impl LocalOnly {
    fn new(value: u32) -> Self {
        Self {
            value,
            marker: Rc::new(Cell::new(0)),
        }
    }

    fn value(&self) -> u32 {
        self.marker.set(self.marker.get() + 1);
        self.value
    }
}

/// 一个 Root 输入：声明位置 + 把值登记到该位置。
type RootInput = (
    super::ref_id::RefId,
    Box<dyn FnOnce(&mut ExecutionContext, &super::ref_id::RefId)>,
);

/// Root 内运行一个 Definition：登记输入、进入 Root frame、执行并正常收口。
///
/// 观察回调在 Root 关闭前运行，可以读取输出位置与 Scope 状态。
fn run_definition_in_root<F>(
    definition: &Definition,
    mut inputs: Vec<RootInput>,
    observe: F,
) -> Result<(), BodyError>
where
    F: FnOnce(&mut InvocationGuardView<'_, '_>) -> Result<(), BodyError>,
{
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let observe = RefCell::new(Some(observe));
    drive(async move {
        for (position, register) in inputs.drain(..) {
            register(execution.context_mut(), &position);
        }
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("fresh execution accepts a root frame");
        let outcome = run_definition(&mut guard, definition).await;
        match outcome {
            Ok(()) => {
                let mut view = InvocationGuardView {
                    guard: &mut guard,
                    root: root.clone(),
                };
                let observed =
                    observe.borrow_mut().take().expect("observer consumed once")(&mut view);
                match observed {
                    Ok(()) => {
                        guard
                            .finalize(&root, &[], &mut Vec::new())
                            .map_err(BodyError::from)?;
                        guard.complete();
                        Ok(())
                    }
                    Err(error) => {
                        guard.failed_with(&error);
                        Err(error)
                    }
                }
            }
            Err(error) => {
                guard.failed_with(&error);
                Err(error)
            }
        }
    })
}

/// 把 `value` 登记到声明输入位置的便捷构造。
fn root_input<T: 'static>(position: &DataRef<T>, value: T) -> RootInput {
    let position = position.position().clone();
    (
        position,
        Box::new(
            move |ctx: &mut ExecutionContext, position: &super::ref_id::RefId| {
                ctx.register_owned(&ctx.root_scope(), position, value)
                    .expect("root input");
            },
        ),
    )
}

/// Root 关闭前的只读／受限观察视图。
struct InvocationGuardView<'a, 'ctx> {
    guard: &'a mut super::context::InvocationGuard<'ctx>,
    root: ScopeId,
}

impl InvocationGuardView<'_, '_> {
    /// Root Scope。
    fn root(&self) -> &ScopeId {
        &self.root
    }

    /// 读取 Root Scope 上的本地位置。
    fn resolve<T: 'static>(&self, position: &super::ref_id::RefId) -> Result<&T, BodyError> {
        Ok(self.guard.resolve::<T>(&self.root, position)?)
    }

    /// Scope 状态。
    fn state(&self, scope: &ScopeId) -> Result<super::scope::ScopeState, BodyError> {
        Ok(self.guard.state(scope)?)
    }

    /// Root Scope 是否仍 Active（叶子失败不直接 abort caller）。
    fn root_is_active(&self) -> bool {
        matches!(self.state(&self.root), Ok(super::scope::ScopeState::Active))
    }
}

// ---- E01：统一 then 与异构保存 ----

async fn e01_leaf(input: &u32) -> Result<String, BodyError> {
    record("node:fn");
    Ok(format!("{input}"))
}

struct E01Suffix(String);

impl NodeCall1<String, Data<u32>> for E01Suffix {
    fn call<'a>(&'a self, a: &'a String) -> NodeFut<'a, u32> {
        let suffix = self.0.clone();
        Box::pin(async move {
            record("node:arc");
            Ok(a.len() as u32 + suffix.len() as u32)
        })
    }
}

struct E01Nested {
    inner: Definition,
}

impl E01Nested {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<u32>("c").expect("declare input");
        let produced = inner
            .then(e01_nested_step, input.clone())
            .expect("nested step");
        inner
            .declare_output_port_for(&produced, "d")
            .expect("declare port");
        Self { inner }
    }
}

async fn e01_nested_step(value: &u32) -> Result<u64, BodyError> {
    record("node:orch-step");
    Ok(u64::from(*value) * 2)
}

impl OrchCall<(u32,), Data<u64>> for E01Nested {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Data<u64>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            record("orch");
            scope.run_steps().await
        })
    }
}

#[test]
fn e01_unified_then_stores_heterogeneous_steps_and_dispatches_both_paths() {
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let produced: DataRef<String> = definition
        .then(e01_leaf, input.clone())
        .expect("function leaf");
    let counted: DataRef<u32> = definition
        .then(Arc::new(E01Suffix("!!".into())), produced.clone())
        .expect("arc node");
    let total: DataRef<u64> = definition
        .then(E01Nested::build(), counted.clone())
        .expect("orchestrator");

    assert_eq!(definition.step_count(), 3);
    // 异构保存：两类调用点分别由独立 runner 承担，没有业务层面的统一运行时 trait。
    match definition.steps()[0].site() {
        CallSite::Node(_) => {}
        _ => panic!("the first step must be a node site"),
    }
    match definition.steps()[2].site() {
        CallSite::Orchestrator(_) => {}
        _ => panic!("the third step must be an orchestrator site"),
    }

    let total_position = total.position().clone();
    let outcome =
        run_definition_in_root(&definition, vec![root_input(&input, 7u32)], move |view| {
            let observed = view.resolve::<u64>(&total_position)?;
            assert_eq!(*observed, 6);
            Ok(())
        });
    assert!(outcome.is_ok(), "dispatch sample must run: {outcome:?}");
    assert_eq!(
        take_events(),
        vec!["node:fn", "node:arc", "orch", "node:orch-step"]
    );
}

// ---- E02：输入 Signature 映射 ----

async fn e02_zero() -> Result<u8, BodyError> {
    Ok(3)
}

async fn e02_one(a: &u32) -> Result<u32, BodyError> {
    Ok(*a + 1)
}

async fn e02_two(a: &u32, b: &u32) -> Result<u32, BodyError> {
    Ok(*a * 100 + *b)
}

async fn e02_double(a: &u32, b: &u32) -> Result<u32, BodyError> {
    // 重复使用同一 Ref 是合法只读依赖。
    Ok(*a + *b)
}

#[test]
fn e02_input_mapping_covers_arity_and_repeated_refs() {
    let mut definition = Definition::new();
    let first = definition.declare_input::<u32>("a").expect("input");
    let second = definition.declare_input::<u32>("a2").expect("input");
    let zero: DataRef<u8> = definition.then(e02_zero, ()).expect("zero inputs");
    let one: DataRef<u32> = definition.then(e02_one, first.clone()).expect("one input");
    let two: DataRef<u32> = definition
        .then(e02_two, (first.clone(), second.clone()))
        .expect("two inputs");
    let repeated: DataRef<u32> = definition
        .then(e02_double, (first.clone(), first.clone()))
        .expect("repeated ref");
    assert_eq!(definition.step_count(), 4);

    let zero_position = zero.position().clone();
    let one_position = one.position().clone();
    let two_position = two.position().clone();
    let repeated_position = repeated.position().clone();
    let outcome = run_definition_in_root(
        &definition,
        vec![root_input(&first, 1u32), root_input(&second, 5u32)],
        move |view| {
            assert_eq!(*view.resolve::<u8>(&zero_position)?, 3);
            assert_eq!(*view.resolve::<u32>(&one_position)?, 2);
            assert_eq!(*view.resolve::<u32>(&two_position)?, 105);
            assert_eq!(*view.resolve::<u32>(&repeated_position)?, 2);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "arity mapping must run: {outcome:?}");
}
// ---- E03：同步与异步函数 item 适配与 Definition 重用 ----

fn e03_sync(a: &u32) -> Result<String, BodyError> {
    Ok(format!("v{a}"))
}

#[allow(clippy::ptr_arg)] // 声明位置的业务类型就是 String，按引用读取它
async fn e03_async_one(a: &String) -> Result<LocalOnly, BodyError> {
    gate_wait().await;
    Ok(LocalOnly::new(a.len() as u32))
}

#[allow(clippy::ptr_arg)] // 声明位置的业务类型就是 String，按引用读取它
async fn e03_async_two(a: &u32, b: &String) -> Result<usize, BodyError> {
    gate_wait().await;
    Ok(*a as usize + b.len())
}

fn e03_build() -> (Definition, DataRef<u32>, DataRef<LocalOnly>, DataRef<usize>) {
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let first: DataRef<String> = definition.then(e03_sync, input.clone()).expect("sync");
    let second: DataRef<LocalOnly> = definition
        .then(e03_async_one, first.clone())
        .expect("async one input");
    let third: DataRef<usize> = definition
        .then(e03_async_two, (input.clone(), first.clone()))
        .expect("async two inputs");
    (definition, input, second, third)
}

#[test]
fn e03_function_items_execute_through_pending_and_a_definition_is_reusable() {
    install_gate();
    let (definition, input, single, total) = e03_build();
    let total_position = total.position().clone();
    let single_position = single.position().clone();
    for round in 0..2 {
        let outcome = run_definition_in_root(&definition, vec![root_input(&input, 4u32)], {
            let total_position = total_position.clone();
            let single_position = single_position.clone();
            move |view| {
                // 异步函数在整个 Pending 之后仍读到同一份输入借用结果。
                assert_eq!(view.resolve::<LocalOnly>(&single_position)?.value(), 2);
                assert_eq!(*view.resolve::<usize>(&total_position)?, 6);
                Ok(())
            }
        });
        assert!(outcome.is_ok(), "reuse round {round}: {outcome:?}");
    }
    let output_step = definition.steps()[2].site();
    match output_step {
        CallSite::Node(site) => {
            assert_eq!(site.inputs().len(), 2);
            assert_eq!(site.outputs().len(), 1);
        }
        _ => panic!("the async two-input step must be a node site"),
    }
    take_events();
}

// ---- E04：配置结构体／Arc 异步 Node ----

struct E04Configured {
    suffix: String,
}

impl NodeCall1<LocalOnly, Data<LocalOnly>> for E04Configured {
    fn call<'a>(&'a self, a: &'a LocalOnly) -> NodeFut<'a, LocalOnly> {
        Box::pin(async move {
            // &self 配置与输入借用一起跨 await。
            gate_wait().await;
            let base = a.value();
            Ok(LocalOnly::new(base + self.suffix.len() as u32))
        })
    }
}

struct E04Zero(u32);

impl NodeCall0<Data<LocalOnly>> for E04Zero {
    fn call<'a>(&'a self) -> NodeFut<'a, LocalOnly> {
        Box::pin(async move { Ok(LocalOnly::new(self.0)) })
    }
}

#[test]
fn e04_configured_struct_and_arc_nodes_borrow_self_across_await() {
    install_gate();
    let mut definition = Definition::new();
    let input = definition
        .declare_input::<LocalOnly>("local")
        .expect("input");
    let configured: DataRef<LocalOnly> = definition
        .then(
            Arc::new(E04Configured {
                suffix: "abc".into(),
            }),
            input.clone(),
        )
        .expect("arc configured node");
    let zero: DataRef<LocalOnly> = definition
        .then(
            E04Configured {
                suffix: "xy".into(),
            },
            configured.clone(),
        )
        .expect("by-value configured node");
    let plain: DataRef<LocalOnly> = definition.then(E04Zero(1), ()).expect("zero input");

    let arc_position = configured.position().clone();
    let by_value_position = zero.position().clone();
    let plain_position = plain.position().clone();
    let outcome = run_definition_in_root(
        &definition,
        vec![root_input(&input, LocalOnly::new(10))],
        move |view| {
            assert_eq!(
                view.resolve::<LocalOnly>(&arc_position)?.value(),
                13,
                "10 + \"abc\".len()"
            );
            assert_eq!(
                view.resolve::<LocalOnly>(&by_value_position)?.value(),
                15,
                "13 + \"xy\".len() through the shared Arc handle"
            );
            assert_eq!(view.resolve::<LocalOnly>(&plain_position)?.value(), 1);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "configured nodes must run: {outcome:?}");
    take_events();
}

// ---- E05：零输入／unit 四组合与普通函数 unit 拒绝 ----

struct E05UnitZero;
struct E05UnitOne;

impl NodeCall0<Unit> for E05UnitZero {
    fn call<'a>(&'a self) -> NodeFut<'a, ()> {
        Box::pin(async move {
            record("unit:zero");
            Ok(())
        })
    }
}

impl NodeCall1<u32, Unit> for E05UnitOne {
    fn call<'a>(&'a self, _a: &'a u32) -> NodeFut<'a, ()> {
        Box::pin(async move {
            record("unit:one");
            Ok(())
        })
    }
}

struct E05FailingUnit;

impl NodeCall1<u32, Unit> for E05FailingUnit {
    fn call<'a>(&'a self, _a: &'a u32) -> NodeFut<'a, ()> {
        Box::pin(async move { Err(BodyError::new("unit body failed")) })
    }
}

fn e05_unit_sync_zero() -> Result<(), BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(())
}

fn e05_unit_sync_one(_a: &u32) -> Result<(), BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(())
}

fn e05_unit_sync_two(_a: &u32, _b: &u32) -> Result<(), BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(())
}

async fn e05_unit_async_zero() -> Result<(), BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(())
}

async fn e05_unit_async_one(_a: &u32) -> Result<(), BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(())
}

async fn e05_unit_async_two(_a: &u32, _b: &u32) -> Result<(), BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(())
}

#[test]
fn e05_plain_function_unit_outputs_are_rejected_before_any_allocation() {
    BODY_CALLS.with(|calls| calls.set(0));
    let mut definition = Definition::new();
    let first = definition.declare_input::<u32>("a").expect("input");
    let second = definition.declare_input::<u32>("b").expect("input");
    let steps = definition.step_count();
    let declared = definition.declared().len();
    let allocated = definition.allocated_probe();

    // 同步 0／1／2 输入
    assert_eq!(
        definition.then(e05_unit_sync_zero, ()).unwrap_err(),
        BuildError::UnsupportedFunctionUnitOutput
    );
    assert_eq!(
        definition
            .then(e05_unit_sync_one, first.clone())
            .unwrap_err(),
        BuildError::UnsupportedFunctionUnitOutput
    );
    assert_eq!(
        definition
            .then(e05_unit_sync_two, (first.clone(), second.clone()))
            .unwrap_err(),
        BuildError::UnsupportedFunctionUnitOutput
    );
    // 异步 0／1／2 输入
    assert_eq!(
        definition.then(e05_unit_async_zero, ()).unwrap_err(),
        BuildError::UnsupportedFunctionUnitOutput
    );
    assert_eq!(
        definition
            .then(e05_unit_async_one, first.clone())
            .unwrap_err(),
        BuildError::UnsupportedFunctionUnitOutput
    );
    assert_eq!(
        definition
            .then(e05_unit_async_two, (first.clone(), second.clone()))
            .unwrap_err(),
        BuildError::UnsupportedFunctionUnitOutput
    );

    // 拒绝发生在追加 Step、声明登记与输出序号消耗之前；业务函数一次也没有执行。
    assert_eq!(definition.step_count(), steps);
    assert_eq!(definition.declared().len(), declared);
    assert_eq!(definition.allocated_probe(), allocated);
    BODY_CALLS.with(|calls| assert_eq!(calls.get(), 0));
}

#[test]
fn e05_unit_signatures_run_without_a_data_id_and_data_signatures_still_register() {
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let probe = definition.declare_input::<u32>("probe").expect("probe");
    let data: DataRef<u8> = definition
        .then(|| -> Result<u8, BodyError> { Ok(9) }, ())
        .expect("data");
    let unit_zero: Result<(), BuildError> = definition.then(E05UnitZero, ());
    let unit_one: Result<(), BuildError> = definition.then(E05UnitOne, input.clone());
    assert!(unit_zero.is_ok() && unit_one.is_ok());
    // unit 声明不分配位置：声明表只增加了 data 输出。
    assert_eq!(definition.declared().len(), 3);
    assert_eq!(definition.step_count(), 3);

    let data_position = data.position().clone();
    let probe_position = probe.position().clone();
    let outcome =
        run_definition_in_root(&definition, vec![root_input(&input, 1u32)], move |view| {
            assert_eq!(*view.resolve::<u8>(&data_position)?, 9);
            // unit 步骤没有消耗 DataId 序号：probe 紧接在 Root 输入之后。
            let root = view.root().clone();
            let id = view
                .guard
                .register_owned(&root, &probe_position, 5u32)
                .expect("probe registration");
            assert_eq!(id.seq(), 2);
            Ok(())
        });
    assert!(outcome.is_ok(), "unit signatures must run: {outcome:?}");
    assert_eq!(take_events(), vec!["unit:zero", "unit:one"]);

    // unit 失败仍终止执行。
    let mut failing = Definition::new();
    let failing_input = failing.declare_input::<u32>("a").expect("input");
    failing
        .then(E05FailingUnit, failing_input.clone())
        .expect("unit step");
    let outcome =
        run_definition_in_root(&failing, vec![root_input(&failing_input, 1u32)], |_| Ok(()));
    assert!(outcome.is_err());
    take_events();
}

// ---- E06：Orchestrator 多输入／多输出与 tuple 边界 ----

struct E06Pair {
    inner: Definition,
}

impl E06Pair {
    fn build() -> Self {
        let mut inner = Definition::new();
        let first = inner.declare_input::<u32>("a").expect("inner input");
        let second = inner.declare_input::<String>("b").expect("inner input");
        let sum: DataRef<u32> = inner
            .then(e06_sum, (first.clone(), second.clone()))
            .expect("inner step");
        let len: DataRef<usize> = inner.then(e06_len, second.clone()).expect("inner step");
        inner.declare_output_port_for(&sum, "sum").expect("port");
        inner.declare_output_port_for(&len, "len").expect("port");
        Self { inner }
    }
}

#[allow(clippy::ptr_arg)] // 声明位置的业务类型就是 String，按引用读取它
async fn e06_sum(a: &u32, b: &String) -> Result<u32, BodyError> {
    Ok(*a + b.len() as u32)
}

#[allow(clippy::ptr_arg)] // 声明位置的业务类型就是 String，按引用读取它
async fn e06_len(b: &String) -> Result<usize, BodyError> {
    Ok(b.len())
}

impl OrchCall<(u32, String), Out2<u32, usize>> for E06Pair {
    type Pack = Targets2<u32, String>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(
        &'a self,
        mut scope: OrchScope<'a, Self::Pack, Out2<u32, usize>>,
    ) -> NodeFut<'a, ()> {
        Box::pin(async move {
            let pack = scope.pack();
            let child = scope.child().clone();
            let borrowed = {
                let ctx: &ExecutionContext = scope.ctx_probe();
                (*pack.first(ctx, &child)?, pack.second(ctx, &child)?.len())
            };
            assert_eq!(borrowed, (1, 2));
            scope.run_steps().await
        })
    }
}

struct E06Unit {
    inner: Definition,
}

impl E06Unit {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<u32>("a").expect("inner input");
        inner
            .then(E05UnitOne, input.clone())
            .expect("inner unit step");
        Self { inner }
    }
}

impl OrchCall<(u32,), Unit> for E06Unit {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Unit>) -> NodeFut<'a, ()> {
        Box::pin(async move { scope.run_steps().await })
    }
}

async fn e06_tuple(whole: &(u32, u64)) -> Result<u64, BodyError> {
    Ok(whole.0 as u64 + whole.1)
}

#[test]
fn e06_orchestrator_signature_keeps_two_heterogeneous_outputs() {
    let mut definition = Definition::new();
    let number = definition.declare_input::<u32>("a").expect("input");
    let text = definition.declare_input::<String>("b").expect("input");
    let pair: (DataRef<u32>, DataRef<usize>) = definition
        .then(E06Pair::build(), (number.clone(), text.clone()))
        .expect("pair orchestrator");
    let unit: Result<(), BuildError> = definition.then(E06Unit::build(), number.clone());
    assert!(unit.is_ok());
    let whole = definition
        .declare_input::<(u32, u64)>("tuple")
        .expect("tuple input");
    let tuple_sum: DataRef<u64> = definition
        .then(e06_tuple, whole.clone())
        .expect("tuple node reads one position");

    let first_position = pair.0.position().clone();
    let second_position = pair.1.position().clone();
    let outcome = run_definition_in_root(
        &definition,
        vec![
            root_input(&number, 1u32),
            root_input(&text, String::from("yz")),
            root_input(&whole, (2u32, 3u64)),
        ],
        move |view| {
            assert_eq!(*view.resolve::<u32>(&first_position)?, 3);
            assert_eq!(*view.resolve::<usize>(&second_position)?, 2);
            let tuple_position = tuple_sum.position().clone();
            assert_eq!(*view.resolve::<u64>(&tuple_position)?, 5);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "pair orchestrator must run: {outcome:?}");
    take_events();
}

// ---- E12：构建期位置与分配拒绝 ----

struct E12Registered {
    inner: Definition,
}

impl E12Registered {
    fn build() -> Self {
        let mut inner = Definition::new();
        inner.declare_input::<u32>("a").expect("inner input");
        inner.declare_input::<u32>("b").expect("inner input");
        inner.declare_output_port::<u32>("x").expect("port");
        inner.declare_output_port::<usize>("y").expect("port");
        Self { inner }
    }
}

impl OrchCall<(u32, u32), Out2<u32, usize>> for E12Registered {
    type Pack = Targets2<u32, u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, _scope: OrchScope<'a, Self::Pack, Out2<u32, usize>>) -> NodeFut<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

fn e12_echo(a: &u32) -> Result<u32, BodyError> {
    Ok(*a)
}

#[test]
fn e12_foreign_and_undeclared_positions_are_rejected_before_any_step() {
    let mut left = Definition::new();
    let left_input = left.declare_input::<u32>("a").expect("input");
    let mut right = Definition::new();
    let right_input = right.declare_input::<u32>("a").expect("input");

    // 另一条来源序列的同类型引用。
    let steps = left.step_count();
    let declared = left.declared().len();
    let allocated = left.allocated_probe();
    assert!(matches!(
        left.then(e12_echo, right_input.clone()).unwrap_err(),
        BuildError::ForeignPosition(_)
    ));
    assert_eq!(left.step_count(), steps);
    assert_eq!(left.declared().len(), declared);
    assert_eq!(left.allocated_probe(), allocated);

    // 同一来源但未登记为合法位置（只声明为输出端口）。
    let ghost = left.declare_output_port::<u32>("ghost").expect("port");
    let steps = left.step_count();
    let declared = left.declared().len();
    let allocated = left.allocated_probe();
    assert!(matches!(
        left.then(e12_echo, ghost.clone()).unwrap_err(),
        BuildError::UndeclaredPosition(_)
    ));
    assert_eq!(left.step_count(), steps);
    assert_eq!(left.declared().len(), declared);
    assert_eq!(left.allocated_probe(), allocated);

    // 合法接线：已声明输入与此前 Step 的输出都能继续接线。
    let produced: DataRef<u32> = left
        .then(e12_echo, left_input.clone())
        .expect("declared input");
    let chained: DataRef<u32> = left
        .then(e12_echo, produced.clone())
        .expect("previous output");
    let outcome = run_definition_in_root(&left, vec![root_input(&left_input, 8u32)], move |view| {
        assert_eq!(*view.resolve::<u32>(chained.position())?, 8);
        Ok(())
    });
    assert!(outcome.is_ok(), "legal wiring must run: {outcome:?}");
    take_events();
}

#[test]
fn e12_checked_group_allocation_fails_as_a_whole_on_exhaustion() {
    use super::ref_id::RefIdSource;

    let source = RefIdSource::with_start(u64::MAX - 2);
    let mut definition = Definition::new_with_source(source);
    let input = definition.declare_input::<u32>("a").expect("input");
    let single: DataRef<u32> = definition
        .then(e12_echo, input.clone())
        .expect("one output");
    let steps = definition.step_count();
    let declared = definition.declared().len();
    let allocated = definition.allocated_probe();
    assert_eq!(allocated, u64::MAX);

    // 双输出 checked 分配耗尽：整组失败，Step／声明表／序号都不变。
    assert_eq!(
        definition
            .then(E12Registered::build(), (input.clone(), input.clone()))
            .unwrap_err(),
        BuildError::OutputPositionExhausted
    );
    assert_eq!(definition.step_count(), steps);
    assert_eq!(definition.declared().len(), declared);
    assert_eq!(definition.allocated_probe(), allocated);

    let outcome =
        run_definition_in_root(&definition, vec![root_input(&input, 3u32)], move |view| {
            assert_eq!(*view.resolve::<u32>(single.position())?, 3);
            Ok(())
        });
    assert!(
        outcome.is_ok(),
        "single output wiring still runs: {outcome:?}"
    );
    take_events();
}

// ---- E13：Definition／Execution 分离 ----

#[test]
fn e13_definition_positions_are_stable_across_executions() {
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let probe = definition.declare_input::<u32>("probe").expect("probe");
    let produced: DataRef<u32> = definition.then(e12_echo, input.clone()).expect("step");
    let site_inputs = match definition.steps()[0].site() {
        CallSite::Node(site) => site.inputs().to_vec(),
        _ => panic!("node site"),
    };
    let produced_position = produced.position().clone();
    let probe_position = probe.position().clone();

    let mut identities = Vec::new();
    for round in 0..2 {
        let captured = Rc::new(RefCell::new(None));
        let captured_in = Rc::clone(&captured);
        let probe_position = probe_position.clone();
        let produced_position = produced_position.clone();
        let baseline = site_inputs.clone();
        let outcome =
            run_definition_in_root(&definition, vec![root_input(&input, 4u32)], move |view| {
                assert_eq!(baseline.as_slice(), baseline.as_slice());
                assert_eq!(*view.resolve::<u32>(&produced_position)?, 4);
                let root = view.root().clone();
                let id = view
                    .guard
                    .register_owned(&root, &probe_position, round)
                    .expect("probe");
                *captured_in.borrow_mut() = Some(id);
                Ok(())
            });
        assert!(outcome.is_ok(), "round {round}: {outcome:?}");
        identities.push(captured.borrow().clone().expect("captured id"));
    }
    use std::sync::Arc as StdArc;
    assert!(
        !StdArc::ptr_eq(identities[0].execution(), identities[1].execution()),
        "each execution keeps its own identity root"
    );
    take_events();
}

// ---- E14：Orchestrator 正常出口 ----

struct E14Orch {
    inner: Definition,
}

impl E14Orch {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<u32>("a").expect("inner input");
        let produced: DataRef<u32> = inner.then(e12_echo, input.clone()).expect("inner step");
        let temporary: DataRef<TrackedProbe> =
            inner.then(e14_tracked, input.clone()).expect("temp step");
        let _ = temporary;
        inner
            .declare_output_port_for(&produced, "out")
            .expect("port");
        Self { inner }
    }
}

fn e14_tracked(_a: &u32) -> Result<TrackedProbe, BodyError> {
    Ok(TrackedProbe { _value: 0 })
}

struct TrackedProbe {
    _value: u32,
}

impl Drop for TrackedProbe {
    fn drop(&mut self) {
        record("temp-dropped");
    }
}

impl OrchCall<(u32,), Data<u32>> for E14Orch {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move { scope.run_steps().await })
    }
}

struct E14Expose {
    inner: Definition,
}

impl E14Expose {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<u32>("a").expect("inner input");
        inner
            .declare_output_port_for(&input, "same")
            .expect("re-expose imported data");
        Self { inner }
    }
}

impl OrchCall<(u32,), Data<u32>> for E14Expose {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, _scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

#[test]
fn e14_orchestrator_transfers_new_outputs_and_keeps_imported_owner() {
    let drops = Rc::new(Cell::new(0));
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let output: DataRef<u32> = definition
        .then(E14Orch::build(), input.clone())
        .expect("orchestrator");
    let exposed: DataRef<u32> = definition
        .then(E14Expose::build(), input.clone())
        .expect("exposing orchestrator");

    let output_position = output.position().clone();
    let exposed_position = exposed.position().clone();
    let input_position = input.position().clone();
    let child_probe = Rc::new(RefCell::new(None));
    let child_probe_out = Rc::clone(&child_probe);
    let site = definition.steps()[0].site();
    let outcome = drive(async move {
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(&root, &input_position, 21u32)
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let child = match site {
            CallSite::Orchestrator(orchestrator) => orchestrator
                .invoke(&mut guard, &root)
                .await
                .expect("orchestrator call"),
            _ => panic!("orchestrator site"),
        };
        *child_probe_out.borrow_mut() = Some(child);
        assert_eq!(*guard.resolve::<u32>(&root, &output_position)?, 21);
        assert_eq!(
            *guard.resolve::<u32>(&root, &input_position)?,
            21,
            "imported input owner stays on the caller"
        );
        // 未声明为端口的临时值在 child 关闭时被清理。
        assert!(matches!(
            guard.state(&child_probe_out.borrow().as_ref().unwrap().clone()),
            Ok(super::scope::ScopeState::Closed)
        ));
        assert_eq!(take_events(), vec!["temp-dropped"]);
        guard
            .finalize(&root, &[], &mut Vec::new())
            .map_err(BodyError::from)?;
        guard.complete();
        Ok::<(), BodyError>(())
    });
    assert!(outcome.is_ok(), "orchestrator exit sample: {outcome:?}");
    let _ = drops;
    take_events();

    // 重新暴露完整 imported Data：caller 新位置绑定到同一份 Data。
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let exposed_position = exposed_position.clone();
    let input_position = input.position().clone();
    let outcome = drive(async move {
        execution
            .context_mut()
            .register_owned(&root, &input_position, 34u32)
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        run_definition(&mut guard, &definition).await?;
        assert_eq!(*guard.resolve::<u32>(&root, &exposed_position)?, 34);
        assert_eq!(*guard.resolve::<u32>(&root, &input_position)?, 34);
        guard
            .finalize(&root, &[], &mut Vec::new())
            .map_err(BodyError::from)?;
        guard.complete();
        Ok::<(), BodyError>(())
    });
    assert!(outcome.is_ok(), "re-exposed import sample: {outcome:?}");
    take_events();
}

// ---- E15：多层双路径（§5.1 集成场景） ----

struct E15Rules {
    weight: u32,
    marker: Rc<Cell<u32>>,
}

impl E15Rules {
    fn new(weight: u32) -> Self {
        Self {
            weight,
            marker: Rc::new(Cell::new(0)),
        }
    }
}

async fn e15_lift(a: &u32) -> Result<String, BodyError> {
    record("e15:fn");
    Ok(format!("v{a}"))
}

struct E15ConfigNode;

impl NodeCall1<String, Data<u32>> for E15ConfigNode {
    fn call<'a>(&'a self, a: &'a String) -> NodeFut<'a, u32> {
        Box::pin(async move {
            gate_wait().await;
            record("e15:arc");
            Ok(a.len() as u32)
        })
    }
}

struct E15Inner {
    inner: Definition,
}

impl E15Inner {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<usize>("d").expect("inner input");
        let produced: DataRef<u64> = inner.then(e15_widen, input.clone()).expect("inner step");
        inner.declare_output_port_for(&produced, "e").expect("port");
        Self { inner }
    }
}

fn e15_widen(d: &usize) -> Result<u64, BodyError> {
    record("e15:nested");
    Ok(*d as u64 * 3)
}

impl OrchCall<(usize,), Data<u64>> for E15Inner {
    type Pack = Targets1<usize>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Data<u64>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            record("e15:nested-orch");
            scope.run_steps().await
        })
    }
}

struct E15Outer {
    inner: Definition,
}

async fn e15_inner_step(c: &u32, rules: &E15Rules) -> Result<usize, BodyError> {
    record("e15:inner-fn");
    rules.marker.set(rules.marker.get() + 1);
    Ok(*c as usize + rules.weight as usize)
}

impl E15Outer {
    fn build() -> Self {
        let mut inner = Definition::new();
        let count = inner.declare_input::<u32>("c").expect("inner input");
        let rules = inner
            .declare_input::<E15Rules>("rules")
            .expect("inner input");
        let scaled: DataRef<usize> = inner
            .then(e15_inner_step, (count.clone(), rules.clone()))
            .expect("inner step");
        let widened: DataRef<u64> = inner
            .then(E15Inner::build(), scaled.clone())
            .expect("nested orchestrator");
        inner.declare_output_port_for(&scaled, "d").expect("port");
        inner.declare_output_port_for(&widened, "e").expect("port");
        Self { inner }
    }
}

impl OrchCall<(u32, E15Rules), Out2<usize, u64>> for E15Outer {
    type Pack = Targets2<u32, E15Rules>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(
        &'a self,
        mut scope: OrchScope<'a, Self::Pack, Out2<usize, u64>>,
    ) -> NodeFut<'a, ()> {
        Box::pin(async move {
            record("e15:outer");
            scope.run_steps().await
        })
    }
}

struct E15UnitNode;

impl NodeCall2<usize, u64, Unit> for E15UnitNode {
    fn call<'a>(&'a self, d: &'a usize, e: &'a u64) -> NodeFut<'a, ()> {
        Box::pin(async move {
            record("e15:unit");
            assert!(*d > 0 && *e > 0);
            Ok(())
        })
    }
}

#[test]
fn e15_multi_layer_dual_path_scenario_runs_on_one_context() {
    install_gate();
    let rules = E15Rules::new(5);
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let rules_input = definition
        .declare_input::<E15Rules>("rules")
        .expect("rules input");
    let lifted: DataRef<String> = definition.then(e15_lift, input.clone()).expect("fn step");
    let counted: DataRef<u32> = definition
        .then(Arc::new(E15ConfigNode), lifted.clone())
        .expect("arc step");
    let pair: (DataRef<usize>, DataRef<u64>) = definition
        .then(E15Outer::build(), (counted.clone(), rules_input.clone()))
        .expect("outer orchestrator");
    let unit: Result<(), BuildError> =
        definition.then(E15UnitNode, (pair.0.clone(), pair.1.clone()));
    assert!(unit.is_ok());

    let rules_for_root = E15Rules::new(5);
    let rules_marker = Rc::clone(&rules_for_root.marker);
    let first = pair.0.position().clone();
    let second = pair.1.position().clone();
    let input_position = input.position().clone();
    let outcome = run_definition_in_root(
        &definition,
        vec![
            root_input(&input, 3u32),
            (
                rules_input.position().clone(),
                Box::new(
                    move |ctx: &mut ExecutionContext, position: &super::ref_id::RefId| {
                        ctx.register_owned(&ctx.root_scope(), position, rules_for_root)
                            .expect("rules input");
                    },
                ),
            ),
        ],
        move |view| {
            assert_eq!(*view.resolve::<usize>(&first)?, 7, "len(\"v3\") + weight");
            assert_eq!(*view.resolve::<u64>(&second)?, 21);
            assert_eq!(*view.resolve::<u32>(&input_position)?, 3);
            assert!(view.root_is_active());
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "multi-layer scenario: {outcome:?}");
    assert_eq!(rules_marker.get(), 1);
    assert!(
        rules.marker.get() == 0,
        "the unused rules instance is untouched"
    );
    assert_eq!(
        take_events(),
        vec![
            "e15:fn",
            "e15:arc",
            "e15:outer",
            "e15:inner-fn",
            "e15:nested-orch",
            "e15:nested",
            "e15:unit",
        ]
    );
}

// ---- E16：整组输出失败 ----

struct E16Pair {
    inner: Definition,
}

impl E16Pair {
    fn build() -> Self {
        let mut inner = Definition::new();
        inner.declare_input::<u32>("a").expect("inner input");
        inner.declare_output_port::<u32>("x").expect("port");
        inner.declare_output_port::<usize>("y").expect("port");
        Self { inner }
    }
}

impl OrchCall<(u32,), Out2<u32, usize>> for E16Pair {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, _scope: OrchScope<'a, Self::Pack, Out2<u32, usize>>) -> NodeFut<'a, ()> {
        // 有意不登记任何输出：整组 Export 预检必须拒绝，且不留部分提交。
        Box::pin(async move { Ok(()) })
    }
}

#[test]
fn e16_group_export_failure_leaves_no_partial_caller_change() {
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let pair: (DataRef<u32>, DataRef<usize>) = definition
        .then(E16Pair::build(), input.clone())
        .expect("pair orchestrator");
    let first = pair.0.position().clone();
    let second = pair.1.position().clone();
    let site = definition.steps()[0].site();
    let outcome = drive(async move {
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(&root, &input.position().clone(), 1u32)
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let error = match site {
            CallSite::Orchestrator(orchestrator) => orchestrator
                .invoke(&mut guard, &root)
                .await
                .expect_err("missing declared outputs must fail"),
            _ => panic!("orchestrator site"),
        };
        assert_eq!(error.note(), "scope operation failed");
        assert!(guard.resolve::<u32>(&root, &first).is_err());
        assert!(guard.resolve::<usize>(&root, &second).is_err());
        assert!(matches!(
            guard.state(&root),
            Ok(super::scope::ScopeState::Active)
        ));
        guard
            .finalize(&root, &[], &mut Vec::new())
            .map_err(BodyError::from)?;
        guard.complete();
        Ok::<(), BodyError>(())
    });
    assert!(
        outcome.is_err(),
        "the caller observes the failure: {outcome:?}"
    );
    take_events();
}

// ---- E17：正式 Node 执行错误 ----

struct E17FailAfterPending;

impl NodeCall1<LocalOnly, Data<u32>> for E17FailAfterPending {
    fn call<'a>(&'a self, a: &'a LocalOnly) -> NodeFut<'a, u32> {
        Box::pin(async move {
            let witness = BorrowWitness(a);
            gate_wait().await;
            record("e17:after-pending");
            let _ = witness.0.value();
            Err(BodyError::new("async node failed after pending"))
        })
    }
}

/// 持有输入借用的见证：Drop 即"借用结束"事件。
struct BorrowWitness<'a>(&'a LocalOnly);

impl Drop for BorrowWitness<'_> {
    fn drop(&mut self) {
        record("borrow-end");
    }
}

fn e17_later(_a: &u32) -> Result<u32, BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(0)
}

#[test]
fn e17_async_node_error_keeps_original_cause_and_stops_the_sequence() {
    install_gate();
    BODY_CALLS.with(|calls| calls.set(0));
    let mut definition = Definition::new();
    let input = definition.declare_input::<LocalOnly>("a").expect("input");
    let failing: DataRef<u32> = definition
        .then(E17FailAfterPending, input.clone())
        .expect("failing step");
    let later: DataRef<u32> = definition
        .then(e17_later, failing.clone())
        .expect("later step");
    let later_position = later.position().clone();
    let input_position = input.position().clone();
    let outcome = drive(async move {
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(&root, &input_position, LocalOnly::new(4))
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let error = run_definition(&mut guard, &definition)
            .await
            .expect_err("the failing node must stop the sequence");
        assert_eq!(error.note(), "async node failed after pending");
        assert_eq!(
            guard.termination().map(|termination| termination.note()),
            Some("async node failed after pending")
        );
        assert_eq!(
            guard.termination().map(|termination| termination.kind()),
            Some(TerminationKind::BodyError)
        );
        assert!(guard.resolve::<u32>(&root, &later_position).is_err());
        assert!(
            matches!(guard.state(&root), Ok(super::scope::ScopeState::Active)),
            "a leaf failure does not abort the caller scope"
        );
        guard.failed_with(&error);
        Ok::<(), BodyError>(())
    });
    let _ = outcome;
    BODY_CALLS.with(|calls| assert_eq!(calls.get(), 0, "later steps never run"));
    assert_eq!(
        take_events(),
        vec!["e17:after-pending", "borrow-end"],
        "the input borrow ends when the failing body returns"
    );
}

// ---- E18：Orchestrator 错误与 unit 退出 ----

struct E18Failing {
    inner: Definition,
}

impl E18Failing {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<u32>("a").expect("inner input");
        let failing: DataRef<u32> = inner
            .then(e18_fails, input.clone())
            .expect("failing inner step");
        inner
            .declare_output_port_for(&failing, "out")
            .expect("port");
        Self { inner }
    }
}

fn e18_fails(_a: &u32) -> Result<u32, BodyError> {
    Err(BodyError::new("inner step failed"))
}

impl OrchCall<(u32,), Data<u32>> for E18Failing {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move { scope.run_steps().await })
    }
}

struct E18UnitOrch {
    inner: Definition,
}

impl E18UnitOrch {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<u32>("a").expect("inner input");
        let temporary: DataRef<TrackedProbe> = inner
            .then(e14_tracked, input.clone())
            .expect("temporary step");
        let _ = temporary;
        Self { inner }
    }
}

impl OrchCall<(u32,), Unit> for E18UnitOrch {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Unit>) -> NodeFut<'a, ()> {
        Box::pin(async move { scope.run_steps().await })
    }
}

#[test]
fn e18_orchestrator_error_and_unit_exit_behave_distinctly() {
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let produced: DataRef<u32> = definition
        .then(E18Failing::build(), input.clone())
        .expect("failing orchestrator");
    let unit: Result<(), BuildError> = definition.then(E18UnitOrch::build(), input.clone());
    assert!(unit.is_ok());
    let produced_position = produced.position().clone();
    let outcome = run_definition_in_root(&definition, vec![root_input(&input, 1u32)], |_| Ok(()));
    assert!(outcome.is_err(), "child failure stops the sequence");
    let _ = produced_position;
    take_events();

    // unit 编排体正常返回：清理自身临时值，不创建 unit Data，也不产生输出位置。
    let mut unit_only = Definition::new();
    let unit_input = unit_only.declare_input::<u32>("a").expect("input");
    let before = unit_only.declared().len();
    let unit: Result<(), BuildError> = unit_only.then(E18UnitOrch::build(), unit_input.clone());
    assert!(unit.is_ok());
    assert_eq!(
        unit_only.declared().len(),
        before,
        "unit declares no position"
    );
    let outcome = run_definition_in_root(&unit_only, vec![root_input(&unit_input, 2u32)], |view| {
        assert!(view.root_is_active());
        Ok(())
    });
    assert!(outcome.is_ok(), "unit orchestrator exit: {outcome:?}");
    assert_eq!(take_events(), vec!["temp-dropped"]);
}

// ---- E19：erased 叶子 Future 取消 ----

struct E19Node;

impl NodeCall1<LocalOnly, Data<u32>> for E19Node {
    fn call<'a>(&'a self, a: &'a LocalOnly) -> NodeFut<'a, u32> {
        Box::pin(async move {
            record("borrow-start");
            let witness = BorrowWitness(a);
            gate_wait().await;
            let value = witness.0.value();
            record("body-ready");
            Ok(value)
        })
    }
}

fn e19_build() -> (Definition, DataRef<LocalOnly>, RefId) {
    let mut definition = Definition::new();
    let input = definition.declare_input::<LocalOnly>("a").expect("input");
    let output: DataRef<u32> = definition.then(E19Node, input.clone()).expect("gated node");
    let output_position = output.position().clone();
    (definition, input, output_position)
}

#[test]
fn e19_dropping_the_owned_erased_leaf_future_releases_the_borrow_first() {
    install_gate();
    let (definition, input, _output_position) = e19_build();
    let input_position = input.position().clone();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, &input_position, LocalOnly::new(7))
        .expect("root input");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let site = definition.steps()[0].site();
    let pending = match site {
        CallSite::Node(node) => node.invoke(&mut guard, &root),
        _ => panic!("node site"),
    };
    // 真实推进到 Pending：borrowed &self／输入仍在 Future 状态机里。
    let boxed = advance_to_pending(pending, 1);
    assert_eq!(take_events(), vec!["borrow-start"]);
    // 丢弃 owned boxed 本体（不是 Pin 借用）。
    drop(boxed);
    let events = take_events();
    assert_eq!(
        events.first().map(String::as_str),
        Some("borrow-end"),
        "the input borrow ends before the outer cleanup: {events:?}"
    );
    assert!(
        !events.contains(&"body-ready".to_string()),
        "the cancelled body never completed"
    );
    // 取消定位等于被取消调用实际使用的 frame Scope（叶子沿用 caller Scope）。
    assert_eq!(
        guard
            .termination()
            .and_then(|termination| termination.scope()),
        Some(&root)
    );
    assert_eq!(
        guard.frame_depth(),
        1,
        "the leaf frame exited, the root frame stays"
    );
    assert!(matches!(
        guard.state(&root),
        Ok(super::scope::ScopeState::Active)
    ));

    // 对照：未 poll 就丢弃，不产生 body 事件。
    install_gate();
    take_events();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, input.position(), LocalOnly::new(1))
        .expect("root input");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let site = definition.steps()[0].site();
    if let CallSite::Node(node) = site {
        let never_polled = node.invoke(&mut guard, &root);
        drop(never_polled);
    }
    assert_eq!(
        take_events(),
        Vec::<String>::new(),
        "未 poll 的 Future 不运行 body"
    );

    // 对照：真实推进到 Ready 之后再丢弃 owned 本体 —— 不记取消、不重复登记或清理。
    install_gate();
    take_events();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, input.position(), LocalOnly::new(3))
        .expect("root input");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let site = definition.steps()[0].site();
    let ready_position = match site {
        CallSite::Node(node) => {
            let ready = drive_pinned(node.invoke(&mut guard, &root));
            assert!(ready.is_ok(), "the leaf completes: {ready:?}");
            node.outputs()[0].clone()
        }
        _ => panic!("node site"),
    };
    // 输出只登记一次：位置已绑定，重复登记会被拒绝。
    assert!(guard.resolve::<u32>(&root, &ready_position).is_ok());
    assert!(
        guard.termination().is_none(),
        "a completed leaf is not a cancellation"
    );
    assert_eq!(
        guard.cleanup_events(),
        0,
        "no cleanup ran for a completed leaf"
    );
    assert_eq!(guard.frame_depth(), 1);
    assert!(matches!(
        guard.state(&root),
        Ok(super::scope::ScopeState::Active)
    ));
}

// ---- E20：erased 编排 Future 取消 ----

thread_local! {
    static CHILD_SCOPES: RefCell<Vec<ScopeId>> = const { RefCell::new(Vec::new()) };
}

/// 内层 child 的 owned 临时值：Drop 即"内层清理"事件。
struct InnerTemp;

impl Drop for InnerTemp {
    fn drop(&mut self) {
        record("inner-temp-dropped");
    }
}

/// 外层 child 的 owned 临时值：Drop 即"外层清理"事件。
struct OuterTemp;

impl Drop for OuterTemp {
    fn drop(&mut self) {
        record("outer-temp-dropped");
    }
}

fn e20_inner_temp(_a: &LocalOnly) -> Result<InnerTemp, BodyError> {
    Ok(InnerTemp)
}

fn e20_outer_temp(_a: &LocalOnly) -> Result<OuterTemp, BodyError> {
    Ok(OuterTemp)
}

struct E20Deep {
    inner: Definition,
}

impl E20Deep {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<LocalOnly>("a").expect("inner input");
        // 未声明为端口的 owned 临时值：先真实产出，取消时随内层 child 清理析构一次。
        let temporary: DataRef<InnerTemp> = inner
            .then(e20_inner_temp, input.clone())
            .expect("inner temporary");
        let _ = temporary;
        let output: DataRef<u32> = inner.then(E19Node, input.clone()).expect("gated node");
        inner.declare_output_port_for(&output, "out").expect("port");
        Self { inner }
    }
}

impl OrchCall<(LocalOnly,), Data<u32>> for E20Deep {
    type Pack = Targets1<LocalOnly>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            CHILD_SCOPES.with(|scopes| scopes.borrow_mut().push(scope.child().clone()));
            scope.run_steps().await
        })
    }
}

struct E20Outer {
    inner: Definition,
}

impl E20Outer {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<LocalOnly>("a").expect("inner input");
        // 外层同样先真实产出自己的 owned 临时值，再进入更深的编排调用。
        let temporary: DataRef<OuterTemp> = inner
            .then(e20_outer_temp, input.clone())
            .expect("outer temporary");
        let _ = temporary;
        let deep: DataRef<u32> = inner
            .then(E20Deep::build(), input.clone())
            .expect("deep orchestrator");
        inner.declare_output_port_for(&deep, "out").expect("port");
        Self { inner }
    }
}

impl OrchCall<(LocalOnly,), Data<u32>> for E20Outer {
    type Pack = Targets1<LocalOnly>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            CHILD_SCOPES.with(|scopes| scopes.borrow_mut().push(scope.child().clone()));
            scope.run_steps().await
        })
    }
}

#[test]
fn e20_dropping_the_outer_orchestrator_future_cleans_inner_to_outer() {
    install_gate();
    CHILD_SCOPES.with(|scopes| scopes.borrow_mut().clear());
    take_shared_events();
    let mut definition = Definition::new();
    let input = definition.declare_input::<LocalOnly>("a").expect("input");
    let output: DataRef<u32> = definition
        .then(E20Outer::build(), input.clone())
        .expect("outer orchestrator");
    let output_position = output.position().clone();
    let input_position = input.position().clone();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let input_id = execution
        .context_mut()
        .register_owned(&root, &input_position, LocalOnly::new(5))
        .expect("root input");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let site = definition.steps()[0].site();
    let pending = match site {
        CallSite::Orchestrator(orchestrator) => orchestrator.invoke(&mut guard, &root),
        _ => panic!("orchestrator site"),
    };
    let boxed = advance_to_pending(pending, 1);
    drop(boxed);
    let events = take_shared_events();
    let position_of = |name: &str| events.iter().position(|event| event == name);
    // 共享日志同时包含业务见证与 Context 的清理／frame 事件。
    let borrow_start = position_of("borrow-start").expect("the leaf body started");
    let borrow_end = position_of("borrow-end").expect("the deepest borrow must end first");
    assert!(borrow_start < borrow_end, "{events:?}");
    // 可比事件序列：最深借用结束 → 内层 child 清理 → 外层 child 清理。
    let inner_drop =
        position_of("inner-temp-dropped").expect("the inner child cleans its owned value");
    let outer_drop =
        position_of("outer-temp-dropped").expect("the outer child cleans its owned value");
    assert!(
        borrow_end < inner_drop && inner_drop < outer_drop,
        "events must be inner-to-outer: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| *event == "inner-temp-dropped")
            .count(),
        1,
        "each layer's owned value drops exactly once: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| *event == "outer-temp-dropped")
            .count(),
        1,
        "each layer's owned value drops exactly once: {events:?}"
    );
    // frame 次序：借用结束 → 内层清理 → 内层 Boundary frame 退出 → 外层清理 → 外层 Boundary frame 退出。
    let outer_child = CHILD_SCOPES.with(|scopes| scopes.borrow().first().cloned());
    let inner_child = CHILD_SCOPES.with(|scopes| scopes.borrow().last().cloned());
    let (outer_child, inner_child) = (
        outer_child.expect("outer child recorded"),
        inner_child.expect("inner child recorded"),
    );
    let cancellation_window = &events[borrow_end..];
    let seq = |name: String| {
        cancellation_window
            .iter()
            .position(|event| *event == name)
            .unwrap_or_else(|| panic!("missing `{name}` in {cancellation_window:?}"))
    };
    let leaf_exit = seq(format!("frame-exit:leaf:{}", inner_child.seq()));
    let inner_exit = seq(format!("frame-exit:boundary:{}", inner_child.seq()));
    let outer_exit = seq(format!("frame-exit:boundary:{}", outer_child.seq()));
    let inner_cleanup = cancellation_window
        .iter()
        .position(|event| event == "inner-temp-dropped")
        .expect("inner cleanup inside the cancellation window");
    let outer_cleanup = cancellation_window
        .iter()
        .position(|event| event == "outer-temp-dropped")
        .expect("outer cleanup inside the cancellation window");
    assert!(
        leaf_exit < inner_cleanup
            && inner_cleanup < inner_exit
            && inner_exit < outer_cleanup
            && outer_cleanup < outer_exit,
        "frame exits must follow the inner-to-outer cleanup order: {cancellation_window:?}"
    );

    let deepest = CHILD_SCOPES.with(|scopes| scopes.borrow().last().cloned());
    let deepest = deepest.expect("the nested orchestrator recorded its child scope");
    assert_eq!(
        guard
            .termination()
            .and_then(|termination| termination.scope()),
        Some(&deepest),
        "cancellation is located at the actual frame scope of the deepest cancelled call"
    );
    assert_eq!(
        guard.termination().map(|termination| termination.note()),
        Some("pending future dropped")
    );
    // 内到外：内层 child 先关闭，外层 child 后关闭，Root 保留。
    for child in CHILD_SCOPES.with(|scopes| scopes.borrow().clone()) {
        assert!(
            matches!(guard.state(&child), Ok(super::scope::ScopeState::Closed)),
            "child scope {child} must be closed by cancellation cleanup"
        );
    }
    assert!(matches!(
        guard.state(&root),
        Ok(super::scope::ScopeState::Active)
    ));
    assert!(guard.resolve::<u32>(&root, &output_position).is_err());
    // Root 原输入仍存活且仍归 Root（只读诊断，不借业务入口）。
    assert!(guard.alive_probe(&input_id));
    assert_eq!(guard.owner_probe(&input_id).expect("owner"), root);
    assert_eq!(guard.frame_depth(), 1, "only the root frame remains");
}

// ---- E21：终止后调用与诊断 ----

#[test]
fn e21_terminated_execution_rejects_further_calls_and_commits() {
    install_gate();
    let (definition, input, _output) = e19_build();
    let input_position = input.position().clone();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, &input_position, LocalOnly::new(2))
        .expect("root input");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let site = definition.steps()[0].site();
    if let CallSite::Node(node) = site {
        let pending = node.invoke(&mut guard, &root);
        drop(advance_to_pending(pending, 1));
    }
    take_events();
    // 终止后：新的 typed CallSite 执行被拒绝，body 计数保持 0。
    let mut blocked = 0usize;
    let mut other = definition.steps()[0].site();
    let rejected = match &mut other {
        CallSite::Node(node) => {
            let future = node.invoke(&mut guard, &root);
            let result = drive_sync(future);
            blocked += 1;
            result.is_err()
        }
        _ => false,
    };
    assert!(rejected, "a terminated execution rejects new leaf calls");
    assert_eq!(blocked, 1);
    assert_eq!(
        take_events(),
        Vec::<String>::new(),
        "no body ran after termination"
    );
    // 普通提交也被拒绝，诊断仍可读取且不解除终止。
    let commit = guard.finalize(&root, &[], &mut Vec::new());
    assert!(
        commit.is_err(),
        "ordinary commit after termination is rejected"
    );
    assert!(guard.is_terminated());
    assert_eq!(
        guard.termination().map(|termination| termination.note()),
        Some("pending future dropped")
    );
}

/// 同步驱动一个已经构造好的 Future（不释放挂起点，只观察是否立即失败）。
fn drive_sync<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("a terminated execution must fail immediately"),
    }
}

// ---- E22：擦除后的防御校验 ----

/// 构造两个来自独立来源、从未登记过的位置（E22 注入用）。
fn probe_ref_ids() -> (RefId, RefId) {
    use super::ref_id::{RefIdAllocator, RefIdSource};
    let allocator = RefIdAllocator::new(RefIdSource::new());
    (
        allocator.allocate().expect("probe position"),
        allocator.allocate().expect("probe position"),
    )
}

thread_local! {
    static E22_BODY_CALLS: Cell<usize> = const { Cell::new(0) };
}

struct E22Orch {
    inner: Definition,
}

impl E22Orch {
    fn build() -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<u32>("a").expect("inner input");
        let produced: DataRef<u32> = inner.then(e12_echo, input.clone()).expect("inner step");
        inner
            .declare_output_port_for(&produced, "out")
            .expect("port");
        Self { inner }
    }
}

impl OrchCall<(u32,), Data<u32>> for E22Orch {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            E22_BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
            scope.run_steps().await
        })
    }
}

/// E22 注入闭包：在真实调用点上替换输入 pack。
type PackInjector = Box<dyn Fn(&dyn super::orchestrator::OrchestratorSite)>;

fn e22_scenario(expected_note: &'static str, expected_diagnostic: &str, inject: PackInjector) {
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let output: DataRef<u32> = definition
        .then(E22Orch::build(), input.clone())
        .expect("orchestrator");
    let output_position = output.position().clone();
    let site = definition.steps()[0].site();
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, input.position(), 1u32)
        .expect("root input");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let error = match site {
        CallSite::Orchestrator(orchestrator) => {
            inject(orchestrator.as_ref());
            drive_sync(orchestrator.invoke(&mut guard, &root))
                .expect_err("the corrupted pack must be rejected")
        }
        _ => panic!("orchestrator site"),
    };
    assert_eq!(error.note(), expected_note);
    assert!(guard.resolve::<u32>(&root, &output_position).is_err());
    assert!(matches!(
        guard.state(&root),
        Ok(super::scope::ScopeState::Active)
    ));
    if expected_note == "scope operation failed" {
        // 位置校验失败保留真实内部诊断（Scope + 位置 + 预期／实际类型）。
        let diagnostic = guard
            .termination()
            .and_then(|termination| termination.scope_error())
            .expect("the boundary records the input position diagnostic");
        let text = format!("{diagnostic}");
        assert!(
            text.contains(expected_diagnostic),
            "diagnostic must contain `{expected_diagnostic}`: {text}"
        );
    }
    guard.failed("defensive sample");
}

#[test]
fn e22_corrupted_pack_type_is_rejected_before_the_body() {
    E22_BODY_CALLS.with(|calls| calls.set(0));
    e22_scenario(
        "orchestrator input pack type mismatch",
        "",
        Box::new(|site: &dyn super::orchestrator::OrchestratorSite| {
            // 错误的具体 Targets… 类型：downcast 必须失败。
            let (first, second) = probe_ref_ids();
            let bogus = super::orchestrator::probe_targets2::<u32, u32>(first, second);
            site.inject_pack_probe(bogus);
        }),
    );
    E22_BODY_CALLS.with(|calls| assert_eq!(calls.get(), 0, "body never runs for a bad pack type"));
}

#[test]
fn e22_corrupted_pack_position_is_rejected_before_the_body() {
    E22_BODY_CALLS.with(|calls| calls.set(0));
    e22_scenario(
        "scope operation failed",
        "is not bound",
        Box::new(|site: &dyn super::orchestrator::OrchestratorSite| {
            // 正确类型、错误位置：位置在本 child Scope 未绑定。
            let (first, _) = probe_ref_ids();
            let bogus = super::orchestrator::probe_targets1::<u32>(first);
            site.inject_pack_probe(bogus);
        }),
    );
    E22_BODY_CALLS.with(|calls| assert_eq!(calls.get(), 0, "body never runs for a bad position"));
}

// ---- E23：同一个 Arc 跨定义复用 ----

struct E23Shared {
    calls: Rc<Cell<u32>>,
    factor: u32,
}

impl NodeCall1<u32, Data<u32>> for E23Shared {
    fn call<'a>(&'a self, a: &'a u32) -> NodeFut<'a, u32> {
        Box::pin(async move {
            self.calls.set(self.calls.get() + 1);
            Ok(*a * self.factor)
        })
    }
}

fn e23_definition(shared: &Arc<E23Shared>) -> (Definition, DataRef<u32>, DataRef<u32>) {
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let produced: DataRef<u32> = definition
        .then(Arc::clone(shared), input.clone())
        .expect("shared node");
    (definition, input, produced)
}

#[allow(clippy::arc_with_non_send_sync)] // 本次运行允许非 Send／Sync 的业务 Node 与 Data
#[test]
fn e23_one_arc_serves_two_definitions_without_sharing_execution_data() {
    let calls = Rc::new(Cell::new(0));
    let shared = Arc::new(E23Shared {
        calls: Rc::clone(&calls),
        factor: 3,
    });
    let (first_definition, first_input, first_output) = e23_definition(&shared);
    let (second_definition, second_input, second_output) = e23_definition(&shared);
    assert_eq!(
        Arc::strong_count(&shared),
        3,
        "two definitions and the test handle"
    );

    let first_position = first_output.position().clone();
    let outcome = run_definition_in_root(
        &first_definition,
        vec![root_input(&first_input, 2u32)],
        move |view| {
            assert_eq!(*view.resolve::<u32>(&first_position)?, 6);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "first definition: {outcome:?}");
    let identity_first = Arc::as_ptr(&shared);
    let second_position = second_output.position().clone();
    let outcome = run_definition_in_root(
        &second_definition,
        vec![root_input(&second_input, 4u32)],
        move |view| {
            assert_eq!(*view.resolve::<u32>(&second_position)?, 12);
            Ok(())
        },
    );
    assert!(outcome.is_ok(), "second definition: {outcome:?}");
    assert_eq!(Arc::as_ptr(&shared), identity_first, "同一个 Node 句柄");
    assert_eq!(
        calls.get(),
        2,
        "both definitions used the same node instance"
    );
    assert_eq!(
        Arc::strong_count(&shared),
        3,
        "no implicit Arc clone in the data path"
    );
    take_events();
}

// ---- E15（Root 收口）：最小 adapter 驱动以空声明输出关闭 Root ----

#[test]
fn e15_root_driver_closes_with_empty_declared_outputs() {
    let mut definition = Definition::new();
    let produced: DataRef<u8> = definition
        .then(|| -> Result<u8, BodyError> { Ok(5) }, ())
        .expect("zero input node");
    assert_eq!(definition.step_count(), 1);
    let _ = produced;
    let exit = drive(super::builder::run_definition_root(
        RootExecution::start(),
        &definition,
    ));
    assert!(exit.terminated().is_none());
    assert!(exit.close_error().is_none());
    assert_eq!(
        exit.cleanup_events(),
        0,
        "a normally completed root closes through finalization, not guard cleanup"
    );
}

// ---- R11：零输入 Arc 接线（数据与显式 unit） ----

#[test]
fn e04_zero_input_arc_node_supports_data_and_unit_outputs() {
    let mut definition = Definition::new();
    let data: DataRef<LocalOnly> = definition
        .then(Arc::new(E04Zero(42)), ())
        .expect("zero input arc data node");
    let unit: Result<(), BuildError> = definition.then(Arc::new(E05UnitZero), ());
    assert!(unit.is_ok());
    assert_eq!(definition.step_count(), 2);
    let data_position = data.position().clone();
    let outcome = run_definition_in_root(&definition, Vec::new(), move |view| {
        assert_eq!(view.resolve::<LocalOnly>(&data_position)?.value(), 42);
        Ok(())
    });
    assert!(outcome.is_ok(), "zero-input arc node: {outcome:?}");
    assert_eq!(take_events(), vec!["unit:zero"]);
}

// ---- R10：Signature ↔ 内部声明输入的构建期一致 ----

struct E09SingleMismatch {
    inner: Definition,
}

impl E09SingleMismatch {
    /// 内部声明 String 输入，但实现按 `(u32,)` 声明自己的 Signature。
    fn build() -> Self {
        let mut inner = Definition::new();
        inner.declare_input::<String>("t").expect("inner input");
        inner.declare_output_port::<u32>("out").expect("port");
        Self { inner }
    }
}

impl OrchCall<(u32,), Data<u32>> for E09SingleMismatch {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, _scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

struct E09SecondMismatch {
    inner: Definition,
}

impl E09SecondMismatch {
    /// 双输入：第一项一致、第二项声明类型与 Signature 不一致。
    fn build() -> Self {
        let mut inner = Definition::new();
        inner.declare_input::<u32>("a").expect("inner input");
        inner.declare_input::<String>("b").expect("inner input");
        inner.declare_output_port::<u32>("out").expect("port");
        Self { inner }
    }
}

impl OrchCall<(u32, u64), Data<u32>> for E09SecondMismatch {
    type Pack = Targets2<u32, u64>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, _scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

#[test]
fn e09_signature_and_inner_declaration_mismatch_is_rejected_before_allocation() {
    let mut definition = Definition::new();
    let number = definition.declare_input::<u32>("a").expect("input");
    let other = definition.declare_input::<u64>("b").expect("input");
    let steps = definition.step_count();
    let declared = definition.declared().len();
    let allocated = definition.allocated_probe();

    // 单项：Signature 说 u32，内部声明 String。
    assert_eq!(
        definition
            .then(E09SingleMismatch::build(), number.clone())
            .unwrap_err(),
        BuildError::SignatureMismatch {
            index: 0,
            expected: "u32",
            actual: "alloc::string::String",
        }
    );
    // 双项后项不一致：第一项一致、第二项 Signature 是 u64 而内部声明 String。
    assert_eq!(
        definition
            .then(E09SecondMismatch::build(), (number.clone(), other.clone()))
            .unwrap_err(),
        BuildError::SignatureMismatch {
            index: 1,
            expected: "u64",
            actual: "alloc::string::String",
        }
    );
    assert_eq!(definition.step_count(), steps);
    assert_eq!(definition.declared().len(), declared);
    assert_eq!(definition.allocated_probe(), allocated);
}

// ---- R12.1：E16 后项故障（首项合法、次项缺失）与 caller 位置冲突 ----

struct E16Late {
    inner: Definition,
    drops: Rc<Cell<usize>>,
}

impl E16Late {
    fn build(drops: &Rc<Cell<usize>>) -> Self {
        let mut inner = Definition::new();
        let input = inner.declare_input::<u32>("a").expect("inner input");
        let produced: DataRef<TrackedDrop> = inner
            .then(e16_tracked, input.clone())
            .expect("first output step");
        inner
            .declare_output_port_for(&produced, "first")
            .expect("first port");
        // 第二个端口没有真实 Node 产出：导出预检必须在首项通过后才在次项失败。
        inner
            .declare_output_port::<usize>("second")
            .expect("second port");
        Self {
            inner,
            drops: Rc::clone(drops),
        }
    }
}

fn e16_tracked(a: &u32) -> Result<TrackedDrop, BodyError> {
    Ok(TrackedDrop { _value: *a })
}

/// 只记录析构次数的 owned 值。
struct TrackedDrop {
    _value: u32,
}

impl Drop for TrackedDrop {
    fn drop(&mut self) {
        record("late-temp-dropped");
    }
}

/// 只统计析构次数的 caller 侧值：证明失败的整组导出没有销毁 caller 原有数据。
struct PreBoundValue {
    drops: Rc<Cell<usize>>,
}

impl Drop for PreBoundValue {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

impl OrchCall<(u32,), Out2<TrackedDrop, usize>> for E16Late {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(
        &'a self,
        mut scope: OrchScope<'a, Self::Pack, Out2<TrackedDrop, usize>>,
    ) -> NodeFut<'a, ()> {
        Box::pin(async move {
            let _ = self.drops.get();
            // 只执行真实子调用；输出准备完全由 Node 产出决定。
            scope.run_steps().await
        })
    }
}

#[test]
fn e16_second_output_failure_prevents_the_first_output_commit() {
    let drops = Rc::new(Cell::new(0));
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let pair: (DataRef<TrackedDrop>, DataRef<usize>) = definition
        .then(E16Late::build(&drops), input.clone())
        .expect("late-failing orchestrator");
    let first = pair.0.position().clone();
    let second = pair.1.position().clone();
    let site = definition.steps()[0].site();
    let outcome = drive(async move {
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(&root, &input.position().clone(), 1u32)
            .expect("root input");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let baseline_snapshot = guard.snapshot_probe(&root).expect("caller baseline");
        let error = match site {
            CallSite::Orchestrator(orchestrator) => orchestrator
                .invoke(&mut guard, &root)
                .await
                .expect_err("the second output is missing"),
            _ => panic!("orchestrator site"),
        };
        assert_eq!(error.note(), "scope operation failed");
        // 首项虽已由真实 Node 产出，但整组导出失败 → caller 两个位置都没有绑定。
        assert!(guard.resolve::<TrackedDrop>(&root, &first).is_err());
        assert!(guard.resolve::<usize>(&root, &second).is_err());
        // 只读快照：caller 的引用与责任集合逐项与失败前一致（无部分绑定、无责任转移）。
        assert_eq!(
            guard.snapshot_probe(&root).expect("caller snapshot"),
            baseline_snapshot,
            "a failed group export must not change caller refs or owned"
        );
        assert!(matches!(
            guard.state(&root),
            Ok(super::scope::ScopeState::Active)
        ));
        // 首项新值仍归 child：受控清理后恰好析构一次。
        assert_eq!(take_events(), vec!["late-temp-dropped"]);
        guard.failed("late export failure");
        Ok::<(), BodyError>(())
    });
    let _ = outcome;
    assert_eq!(
        drops.get(),
        0,
        "the orchestrator body itself holds no value"
    );
}

#[test]
fn e16_caller_position_conflict_is_rejected_without_partial_commit() {
    let drops = Rc::new(Cell::new(0));
    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let pair: (DataRef<TrackedDrop>, DataRef<usize>) = definition
        .then(E16Late::build(&drops), input.clone())
        .expect("late-failing orchestrator");
    let first = pair.0.position().clone();
    let prebound_drops = Rc::new(Cell::new(0));
    let prebound = Rc::clone(&prebound_drops);
    let site = definition.steps()[0].site();
    let outcome = drive(async move {
        let prebound_drops = prebound;
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(&root, &input.position().clone(), 1u32)
            .expect("root input");
        // 先把 caller 的首个输出位置占住：整组导出预检必须整体拒绝。
        let occupied = execution
            .context_mut()
            .register_owned(
                &root,
                &first.clone(),
                PreBoundValue {
                    drops: Rc::clone(&prebound_drops),
                },
            )
            .expect("pre-bound caller position");

        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let conflict_baseline = guard.snapshot_probe(&root).expect("caller baseline");
        let error = match site {
            CallSite::Orchestrator(orchestrator) => orchestrator
                .invoke(&mut guard, &root)
                .await
                .expect_err("a pre-bound caller position must fail the whole export"),
            _ => panic!("orchestrator site"),
        };
        assert_eq!(error.note(), "scope operation failed");
        // 终止后不再走业务 resolve；用只读诊断与 Drop 观测证明"整组拒绝、无部分提交"。
        let scope_error = guard
            .termination()
            .and_then(|termination| termination.scope_error())
            .map(|diagnostic| format!("{diagnostic}"))
            .expect("the export precheck diagnostic is preserved");
        assert!(
            scope_error.contains("is already bound"),
            "the whole export plan is rejected at the precheck: {scope_error}"
        );
        // 原绑定仍在 caller、仍存活；child 的新值没有转移并已析构一次。
        assert!(guard.alive_probe(&occupied));
        assert_eq!(
            guard.owner_probe(&occupied).expect("owner"),
            root,
            "the pre-bound value still belongs to the caller"
        );
        assert_eq!(prebound_drops.get(), 0, "the caller's value is untouched");
        assert_eq!(
            guard.snapshot_probe(&root).expect("caller snapshot"),
            conflict_baseline,
            "a conflicting caller position must not change caller refs or owned"
        );
        assert_eq!(take_events(), vec!["late-temp-dropped"]);
        guard.failed("caller position conflict");
        Ok::<(), BodyError>(())
    });
    let _ = outcome;
}

// ---- R12.4：E22 已绑定但实际类型错误的目标（双输入后项） ----

struct E22Typed {
    inner: Definition,
}

impl E22Typed {
    fn build() -> Self {
        let mut inner = Definition::new();
        let first = inner.declare_input::<u32>("a").expect("inner input");
        let second = inner.declare_input::<u64>("b").expect("inner input");
        let produced: DataRef<u32> = inner
            .then(e22_pair_echo, (first.clone(), second.clone()))
            .expect("inner step");
        inner
            .declare_output_port_for(&produced, "out")
            .expect("port");
        Self { inner }
    }
}

fn e22_pair_echo(a: &u32, b: &u64) -> Result<u32, BodyError> {
    Ok(*a + *b as u32)
}

impl OrchCall<(u32, u64), Data<u32>> for E22Typed {
    type Pack = Targets2<u32, u64>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            E22_BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
            scope.run_steps().await
        })
    }
}

#[test]
fn e22_bound_target_with_wrong_type_is_rejected_by_validate_type() {
    E22_BODY_CALLS.with(|calls| calls.set(0));
    let orchestrator = E22Typed::build();
    // 真实已绑定的 child 位置：第一个声明输入（u32）。把 pack 的后项指向它即类型不符。
    let bound_position = orchestrator.definition().inputs()[0].position().clone();
    let mut definition = Definition::new();
    let first = definition.declare_input::<u32>("a").expect("input");
    let second = definition.declare_input::<u64>("b").expect("input");
    let output: DataRef<u32> = definition
        .then(orchestrator, (first.clone(), second.clone()))
        .expect("typed orchestrator");
    let output_position = output.position().clone();
    let site = definition.steps()[0].site();

    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, first.position(), 1u32)
        .expect("root input");
    execution
        .context_mut()
        .register_owned(&root, second.position(), 2u64)
        .expect("root input");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let error = match site {
        CallSite::Orchestrator(orchestrator) => {
            // 正确 pack 类型、后项指向一个存活且已绑定的 u32 位置：validate_type 必须拒绝。
            let bogus = super::orchestrator::probe_targets2::<u32, u64>(
                bound_position.clone(),
                bound_position,
            );
            orchestrator.inject_pack_probe(bogus);
            drive_sync(orchestrator.invoke(&mut guard, &root))
                .expect_err("a wrong target type must be rejected before the body")
        }
        _ => panic!("orchestrator site"),
    };
    assert_eq!(error.note(), "scope operation failed");
    let diagnostic = guard
        .termination()
        .and_then(|termination| termination.scope_error())
        .map(|diagnostic| format!("{diagnostic}"))
        .expect("the type diagnostic is preserved");
    assert!(
        diagnostic.contains("u64") && diagnostic.contains("u32"),
        "the diagnostic records expected and actual types: {diagnostic}"
    );
    assert!(
        diagnostic.contains("RefId"),
        "the diagnostic records the offending position: {diagnostic}"
    );
    assert!(guard.resolve::<u32>(&root, &output_position).is_err());
    E22_BODY_CALLS.with(|calls| assert_eq!(calls.get(), 0, "body never runs for a wrong type"));
    take_events();
}

// ---- R9：输出冲突预检覆盖 0／1／2 输入叶子 ----

struct E17Zero;
struct E17One;
struct E17Two;

impl NodeCall0<Data<u32>> for E17Zero {
    fn call<'a>(&'a self) -> NodeFut<'a, u32> {
        Box::pin(async move {
            BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
            Ok(0)
        })
    }
}

impl NodeCall1<u32, Data<u32>> for E17One {
    fn call<'a>(&'a self, a: &'a u32) -> NodeFut<'a, u32> {
        Box::pin(async move {
            BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
            Ok(*a)
        })
    }
}

impl NodeCall2<u32, u32, Data<u32>> for E17Two {
    fn call<'a>(&'a self, a: &'a u32, b: &'a u32) -> NodeFut<'a, u32> {
        Box::pin(async move {
            BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
            Ok(*a + *b)
        })
    }
}

/// 一个 arity 的输出冲突预检样本：(Definition, 输出位置, 需要预先绑定的输入)。
type LeafCase<'a> = (&'a Definition, RefId, Vec<(RefId, u32)>);

#[test]
fn e17_output_conflict_precheck_covers_every_leaf_arity() {
    BODY_CALLS.with(|calls| calls.set(0));
    // 三个 arity 的叶子：0／1／2 输入，各自输出位置先被占住。
    let mut zero_def = Definition::new();
    let zero: DataRef<u32> = zero_def.then(E17Zero, ()).expect("zero input leaf");
    let mut one_def = Definition::new();
    let one_input = one_def.declare_input::<u32>("a").expect("input");
    let one: DataRef<u32> = one_def
        .then(E17One, one_input.clone())
        .expect("one input leaf");
    let mut two_def = Definition::new();
    let first = two_def.declare_input::<u32>("a").expect("input");
    let second = two_def.declare_input::<u32>("b").expect("input");
    let two: DataRef<u32> = two_def
        .then(E17Two, (first.clone(), second.clone()))
        .expect("two input leaf");

    // 一个合法（输出位置未被占用）的零输入 Definition：证明终止后不再执行任何 body。
    let mut follow_up_definition = Definition::new();
    let _follow_up: DataRef<u32> = follow_up_definition
        .then(E17Zero, ())
        .expect("follow-up leaf");
    let follow_up_def = &follow_up_definition;
    let cases: Vec<LeafCase<'_>> = vec![
        (&zero_def, zero.position().clone(), Vec::new()),
        (
            &one_def,
            one.position().clone(),
            vec![(one_input.position().clone(), 1u32)],
        ),
        (
            &two_def,
            two.position().clone(),
            vec![
                (first.position().clone(), 1u32),
                (second.position().clone(), 2u32),
            ],
        ),
    ];

    for (definition, output_position, inputs) in cases {
        let follow_up_def = &follow_up_def;
        let before = BODY_CALLS.with(|calls| calls.get());
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        for (position, value) in &inputs {
            execution
                .context_mut()
                .register_owned(&root, position, *value)
                .expect("root input");
        }
        let occupied = execution
            .context_mut()
            .register_owned(&root, &output_position, 7u32)
            .expect("pre-bound output position");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let site = definition.steps()[0].site();
        let error = match site {
            CallSite::Node(node) => drive_sync(node.invoke(&mut guard, &root))
                .expect_err("an already bound output position must be rejected before the body"),
            _ => panic!("node site"),
        };
        // 预检拒绝：诊断保留真实 RefAlreadyBound，业务体没有运行，caller Scope 仍 Active。
        let scope_error = error
            .scope_error()
            .map(|diagnostic| format!("{diagnostic}"))
            .expect("the leaf precheck keeps the scope diagnostic");
        assert!(
            scope_error.contains("is already bound"),
            "precheck diagnostic: {scope_error}"
        );
        assert_eq!(
            BODY_CALLS.with(|calls| calls.get()),
            before,
            "the body must not run when the output position is known to conflict"
        );
        assert!(matches!(
            guard.state(&root),
            Ok(super::scope::ScopeState::Active)
        ));
        assert!(guard.alive_probe(&occupied));
        assert_eq!(guard.owner_probe(&occupied).expect("owner"), root);
        // 调用方**捕获错误且不代为标记**：终止必须已由 adapter 自身保存。
        let termination = guard.termination();
        assert_eq!(
            termination.map(|termination| termination.kind()),
            Some(TerminationKind::BodyError),
            "the precheck failure itself terminates as an execution error"
        );
        assert_eq!(
            termination.map(|termination| termination.note()),
            Some("leaf output precheck failed")
        );
        assert_eq!(
            termination.and_then(|termination| termination.scope()),
            Some(&root),
            "termination is located at the scope this call actually uses"
        );
        assert!(
            termination
                .and_then(|termination| termination.scope_error())
                .is_some(),
            "the original scope diagnostic is preserved by the establish-failure path"
        );
        // 终止不可恢复：同一 guard 内再运行一个合法 Definition 也不会执行 body。
        let follow_before = BODY_CALLS.with(|calls| calls.get());
        let follow_up: Result<(), BodyError> = drive(run_definition(&mut guard, follow_up_def));
        assert!(
            follow_up.is_err(),
            "a terminated execution rejects follow-up calls"
        );
        assert_eq!(
            BODY_CALLS.with(|calls| calls.get()),
            follow_before,
            "no business body runs after the precheck termination"
        );
        // 普通提交被拒绝，caller 自有 Data 仍存活且仍归 caller。
        assert!(guard.finalize(&root, &[], &mut Vec::new()).is_err());
        assert!(guard.alive_probe(&occupied));
        assert_eq!(guard.owner_probe(&occupied).expect("owner"), root);
    }
}

// ---- R9 补证：业务之后的真实登记失败（DataId 耗尽） ----

#[test]
fn e17_registration_exhaustion_after_body_marks_execution_error() {
    BODY_CALLS.with(|calls| calls.set(0));
    // 既有 test-only 身份起点：只用于触发 DataId 耗尽；adapter／存储／guard 路径未替换。
    let identity = super::identity::ExecutionIdentity::with_starts(u64::MAX - 1, 0);
    let mut context = ExecutionContext::new(identity);
    let root = context.root_scope();

    let mut definition = Definition::new();
    let input = definition.declare_input::<u32>("a").expect("input");
    let output: DataRef<u32> = definition.then(E17One, input.clone()).expect("leaf");
    let output_position = output.position().clone();
    let input_position = input.position().clone();

    let mut guard = context
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    // 真实 register_owned 消耗最后一个可分配的 DataId。
    let input_id = guard
        .register_owned(&root, &input_position, 1u32)
        .expect("root input consumes the last id");
    let caller_snapshot = guard.snapshot_probe(&root).expect("caller baseline");

    let error =
        drive(run_definition(&mut guard, &definition)).expect_err("registration exhaustion");
    // 业务体运行一次才失败：这是"业务之后的登记失败"，不是预检拒绝。
    assert_eq!(BODY_CALLS.with(|calls| calls.get()), 1);
    let diagnostic = error
        .scope_error()
        .map(|diagnostic| format!("{diagnostic}"))
        .expect("the storage diagnostic is preserved");
    assert!(
        diagnostic.contains("sequence space exhausted"),
        "the original storage error is preserved: {diagnostic}"
    );
    // 显式执行错误退出（不是取消），终止原因与 Scope 诊断都保留。
    let termination = guard.termination();
    assert_eq!(
        termination.map(|termination| termination.kind()),
        Some(TerminationKind::BodyError)
    );
    assert_eq!(
        termination.map(|termination| termination.note()),
        Some("scope operation failed")
    );
    assert_eq!(
        termination.and_then(|termination| termination.scope()),
        Some(&root),
        "termination is located at the scope this call actually uses"
    );
    assert!(
        termination
            .and_then(|termination| termination.scope_error())
            .is_some()
    );
    // 输出没有登记：位置仍未绑定，caller 的引用与责任集合与失败前一致。
    let (refs, owned) = guard.snapshot_probe(&root).expect("caller snapshot");
    assert_eq!((refs, owned), caller_snapshot);
    assert!(guard.resolve::<u32>(&root, &output_position).is_err());
    // Leaf 不清理 caller Scope：caller 自有 Data 仍存活且仍归 Root。
    assert!(guard.alive_probe(&input_id));
    assert_eq!(guard.owner_probe(&input_id).expect("owner"), root);
    assert!(matches!(
        guard.state(&root),
        Ok(super::scope::ScopeState::Active)
    ));
    assert!(guard.finalize(&root, &[], &mut Vec::new()).is_err());
    take_events();
}
