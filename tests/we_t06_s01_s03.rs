//! WE-T06 A 阶段集成证据：S01～S03（新公开 API 的端到端验收）。
//!
//! 对应任务书 §4 的 S01／S02／S03 行与 §8 的 X07／X04：
//!
//! - S01 Root 线性 owned 流：单／多输入，后续 Node 读取前一步产物，单／tuple／unit 输出，
//!   含无数据依赖 unit Step 的严格定义顺序，两次 `execute` 的定义／Ref 身份独立；
//! - S02 Node／Query：函数 item、函数引用、async closure、借用／owned／`Arc<具体 Struct>`
//!   Node，unit／多输入，非 Send 数据，真实跨 await 借用，动态 Failure source，非静态
//!   Node 的合法寿命；
//! - S03 Chain／Fragment：closure factory 与接收 Flow 的普通函数，多层 capture、真实
//!   Import／Export、child-local Ref 不泄漏（运行期可达）、非输出 owned 清理、Fragment
//!   复用，以及 child Body 内的六操作能力映射。
//!
//! 本文件自足：只使用 `srflow` 的新公开面，不引用 `srflow::legacy`，不触碰 crate 私有路径。
//!
//! 运行：`cargo test --offline --test we_t06_s01_s03 -- --nocapture`
// `let _: () = …` 是刻意的 unit 形状断言（Node／Chain 输出），按测试意图保留。
#![allow(clippy::unused_unit, clippy::let_unit_value)]

use futures::executor::block_on;
use srflow::{BodyError, BodyFailure, Data, Flow, Node, Query, Ref, RunError, Runtime};
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll};

// ================================================================ 通用观察辅助

/// 业务 Data 携带的事件记录（非 Send／非 Sync，无需锁）：随真实数据流传播。
#[derive(Clone)]
struct Journal(Rc<RefCell<Vec<String>>>);

impl Journal {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(Vec::new())))
    }

    fn push(&self, event: impl Into<String>) {
        self.0.borrow_mut().push(event.into());
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.borrow_mut())
    }
}

/// 结构体 Node 的调用顺序／次数记录：记录静态标签，证明真实调用次数与顺序。
#[derive(Clone)]
struct Steps(Rc<RefCell<Vec<&'static str>>>);

impl Steps {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(Vec::new())))
    }

    fn push(&self, label: &'static str) {
        self.0.borrow_mut().push(label);
    }

    fn take(&self) -> Vec<&'static str> {
        std::mem::take(&mut *self.0.borrow_mut())
    }
}

/// 真实 Pending：第一次 poll 返回 Pending 并自唤醒，第二次 poll 完成；不用睡眠替代。
struct PendingOnce {
    polls: Arc<AtomicUsize>,
    yielded: bool,
}

impl Future for PendingOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

// ================================================================ 业务 Data

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Report(u32);

#[derive(Data)]
struct Item(u32);

#[derive(Data)]
struct Score(u32);

/// Choose 的选择键：只声明 `Data` 并手写 `PartialEq`（不派生 Eq／Hash／Clone／Copy）。
#[derive(Data)]
struct Route {
    code: u8,
}

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
    }
}

/// Root 输入：业务值 + 事件记录句柄。
#[derive(Data)]
struct Seed {
    value: u32,
    journal: Journal,
}

/// 非 Send／非 Sync 业务输入：字段含 `Rc`；手写 marker 与派生等价。
struct NonSendInput {
    value: u32,
    tag: Rc<u32>,
}

impl Data for NonSendInput {}

/// 非 Send 的 owned 输出：证明非 Send 数据也能作为 Root owned 输出离开 Runtime。
struct NonSendSum {
    total: u32,
    tag: Rc<u32>,
}

impl Data for NonSendSum {}

/// 非输出的 child owned 产物：Drop 记录一次真实析构。
struct Tracked {
    label: &'static str,
    journal: Journal,
}

impl Data for Tracked {}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.journal.push(format!("drop:{}", self.label));
    }
}

// ================================================================ 业务错误

/// 动态 Retry 原因（保留动态对象，不被字符串化重建）。
#[derive(Debug)]
struct NotReady {
    value: u32,
}

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "candidate {} is not ready", self.value)
    }
}

impl Error for NotReady {}

/// 动态 Failure 理由：携带运行时产生的动态字符串。
#[derive(Debug)]
struct TooLarge {
    detail: String,
}

impl fmt::Display for TooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rejected: {}", self.detail)
    }
}

impl Error for TooLarge {}

// ================================================================ 函数 Node

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

async fn plus_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

/// 多输入函数 Node：输入声明为 flat Ref tuple，视图是各叶子的引用 tuple。
async fn pair_sum(query: Query<(&Amount, &Amount)>) -> Result<Amount, BodyError> {
    let (left, right) = query.get();
    Ok(Amount(left.0 + right.0))
}

async fn to_report(query: Query<&Amount>) -> Result<Report, BodyError> {
    Ok(Report(query.get().0))
}

/// 非 Send owned 数据在两个 Node 之间继续传递（不要求 Clone／Copy／Send）。
async fn duplicate_sum(query: Query<&NonSendSum>) -> Result<NonSendSum, BodyError> {
    let sum = query.get();
    Ok(NonSendSum {
        total: sum.total,
        tag: Rc::clone(&sum.tag),
    })
}

/// 零输入 Node：不登记占位业务值，成功输出是显式 `()`。
async fn note(query: Query<()>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

async fn seed_reader(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value + 1))
}

/// 父层步骤：在业务 Data 携带的记录里写下一个真实事件。
async fn journal_note(query: Query<&Seed>) -> Result<(), BodyError> {
    query.get().journal.push("parent:next");
    Ok(())
}

/// child 内产生非声明输出的 owned 值。
async fn track_temp(query: Query<&Seed>) -> Result<Tracked, BodyError> {
    let seed = query.get();
    Ok(Tracked {
        label: "temp",
        journal: seed.journal.clone(),
    })
}

async fn items_of(query: Query<&Seed>) -> Result<Vec<Item>, BodyError> {
    let value = query.get().value;
    Ok(vec![Item(value), Item(value + 1)])
}

async fn score_of(query: Query<&Item>) -> Result<Score, BodyError> {
    Ok(Score(query.get().0 * 10))
}

async fn route_of(query: Query<&Seed>) -> Result<Route, BodyError> {
    Ok(Route {
        code: (query.get().value % 2) as u8,
    })
}

async fn amount_of(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value))
}

async fn zero_amount(query: Query<&Seed>) -> Result<Amount, BodyError> {
    let _ = query.get();
    Ok(Amount(0))
}

async fn double_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

async fn bump_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

thread_local! {
    /// Retry attempt 计数：第一次 attempt 返回 Retry，第二次成功（真实重启）。
    static ATTEMPT: Cell<u32> = const { Cell::new(0) };
}

async fn judge_retry_once(query: Query<&Amount>) -> Result<Report, BodyError> {
    let attempt = ATTEMPT.with(|counter| {
        let next = counter.get() + 1;
        counter.set(next);
        next
    });
    let value = query.get().0;
    if attempt == 1 {
        Err(BodyError::retry(NotReady { value }))
    } else {
        Ok(Report(value * 10))
    }
}

/// 进入第二轮时返回 `IterBreak`，交付进入该轮时的 current。
async fn judge_break(query: Query<&Amount>) -> Result<Report, BodyError> {
    let value = query.get().0;
    if value >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(Report(value))
    }
}

// ================================================================ 结构体 Node

/// 具体 Struct Node：+1 并记录一次真实调用；`label` 区分不同实例／调用形式。
struct CountingStep {
    label: &'static str,
    steps: Steps,
}

impl Node for CountingStep {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        self.steps.push(self.label);
        Ok(Amount(query.get().0 + 1))
    }
}

/// 非 static 配置的结构体 Node：句柄借用调用者的本地配置。
struct Configured<'a> {
    delta: &'a u32,
    steps: Steps,
}

impl Node for Configured<'_> {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        self.steps.push("configured");
        Ok(Amount(query.get().0 + *self.delta))
    }
}

/// 多输入结构体 Node（统一 `Query<&Self::Input>` 协议）。
struct PairSum {
    steps: Steps,
}

impl Node for PairSum {
    type Input = (Amount, Amount);
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        self.steps.push("pair");
        let (left, right) = query.get();
        Ok(Amount(left.0 + right.0))
    }
}

/// 零输入结构体 Node：unit 输入，不产生业务槽。
struct UnitNote {
    steps: Steps,
}

impl Node for UnitNote {
    type Input = ();
    type Output = ();

    async fn run(&self, query: Query<&Self::Input>) -> Result<(), BodyError> {
        self.steps.push("unit");
        let _ = query.get();
        Ok(())
    }
}

/// 异构多输入结构体 Node：`(Amount, Report)` → `Report`。
struct Combo {
    steps: Steps,
}

impl Node for Combo {
    type Input = (Amount, Report);
    type Output = Report;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Report, BodyError> {
        self.steps.push("combo");
        let (amount, report) = query.get();
        Ok(Report(amount.0 + report.0))
    }
}

// ================================================================ Fragment 形态

/// Fragment：closure factory，携带 Node 句柄的借用（非 static 依赖）。
fn step_fragment<'n>(
    node: &'n CountingStep,
    input: Ref<Amount>,
) -> impl FnOnce(&mut Flow<'n>) + 'n {
    move |sub| {
        let out_refs = sub.then_with_output(node, input);
        sub.output_with(out_refs);
    }
}

/// Fragment：child 内同时产生一个非声明输出的 owned 值（由 child 边界清理）。
fn temp_step_fragment<'n>(
    node: &'n CountingStep,
    seed: Ref<Seed>,
) -> impl FnOnce(&mut Flow<'n>) + 'n {
    move |sub| {
        let _temp = sub.then_with_output(track_temp, seed);
        let produced = sub.then_with_output(seed_reader, seed);
        let out_refs = sub.then_with_output(node, produced);
        sub.output_with(out_refs);
    }
}

/// Fragment：接收 `&mut Flow<'n>` 的普通函数。
fn call_in_body<'n>(
    flow: &mut Flow<'n>,
    node: &'n CountingStep,
    input: Ref<Amount>,
) -> Ref<Amount> {
    flow.then_with_output(node, input)
}

// ================================================================ S01

/// S01 正例：单／多输入、后续 Node 读取前一步产物、单／tuple／unit 输出。
#[test]
fn s01_root_linear_owned_flow_shapes_and_values() {
    let runtime = Runtime::new();

    // 单输入：每一步都读取上一步的产物。
    let single = block_on(runtime.execute::<_, Amount, _>(
        |flow, amount| {
            let doubled = flow.then_with_output(double, amount);
            let out_refs = flow.then_with_output(plus_one, doubled);
            flow.output_with(out_refs);
        },
        Amount(20),
    ))
    .expect("single-input linear flow runs");
    assert_eq!(single.0, 41, "20 * 2 + 1");

    // 多输入：同一 Node 读取两个 Root 位置，再交给后续步骤。
    let summed = block_on(runtime.execute::<_, Amount, _>(
        |flow, (left, right)| {
            let total = flow.then_with_output(pair_sum, (left, right));
            let out_refs = flow.then_with_output(plus_one, total);
            flow.output_with(out_refs);
        },
        (Amount(2), Amount(5)),
    ))
    .expect("multi-input linear flow runs");
    assert_eq!(summed.0, 8, "(2 + 5) + 1");

    // tuple 输出：Root 一次交付两个 owned 值。
    let (amount, report) = block_on(runtime.execute::<_, (Amount, Report), _>(
        |flow, amount| {
            let stepped = flow.then_with_output(plus_one, amount);
            let report = flow.then_with_output(to_report, stepped);
            let out_refs = (stepped, report);
            flow.output_with(out_refs);
        },
        Amount(1),
    ))
    .expect("tuple root output runs");
    assert_eq!((amount.0, report.0), (2, 2));

    // unit 输出：零输入 Root 的第二参数与 closure 输入均为 `()`。
    let unit: () = block_on(runtime.execute(
        |flow, ()| {
            let _: () = flow.then_with_output(note, ());
            let () = ();
            flow.output_with(());
        },
        (),
    ))
    .expect("unit root output runs");
    assert_eq!(unit, ());
}

/// S01 边界：无数据依赖的 unit Step 也必须按定义顺序执行，未使用的输出同样执行。
#[test]
fn s01_definition_order_is_strict_including_unit_steps() {
    thread_local! {
        static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    }

    async fn first(query: Query<&Amount>) -> Result<Amount, BodyError> {
        EVENTS.with(|log| log.borrow_mut().push("start:first".into()));
        let value = query.get().0;
        EVENTS.with(|log| log.borrow_mut().push("end:first".into()));
        Ok(Amount(value + 1))
    }

    async fn stamp(_query: Query<()>) -> Result<(), BodyError> {
        EVENTS.with(|log| log.borrow_mut().push("stamp".into()));
        Ok(())
    }

    async fn second(query: Query<&Amount>) -> Result<Report, BodyError> {
        EVENTS.with(|log| log.borrow_mut().push("start:second".into()));
        let value = query.get().0;
        EVENTS.with(|log| log.borrow_mut().push("end:second".into()));
        Ok(Report(value))
    }

    let runtime = Runtime::new();
    let output = block_on(runtime.execute::<_, Report, _>(
        |flow, amount| {
            let stepped = flow.then_with_output(first, amount);
            // 无数据依赖的 unit Step：位于两个有依赖的步骤之间，仍必须按序执行。
            let _: () = flow.then_with_output(stamp, ());
            let report = flow.then_with_output(second, stepped);
            // 输出未被任何后续步骤使用：仍必须执行一次。
            let _unused = flow.then_with_output(first, amount);
            let out_refs = report;
            flow.output_with(out_refs);
        },
        Amount(1),
    ))
    .expect("ordered run");
    assert_eq!(output.0, 2);

    let events = EVENTS.with(|log| std::mem::take(&mut *log.borrow_mut()));
    assert_eq!(
        events,
        vec![
            "start:first".to_string(),
            "end:first".to_string(),
            "stamp".to_string(),
            "start:second".to_string(),
            "end:second".to_string(),
            "start:first".to_string(),
            "end:first".to_string(),
        ],
        "steps run strictly in definition order; a unit step without data dependency still runs"
    );
}

/// S01 边界＋独立性：两次 execute 各建 Definition／各取新 Ref 身份，旧 Ref 绝不复用。
#[test]
fn s01_two_executions_build_independent_definitions_and_reject_an_old_ref() {
    thread_local! {
        static CALLS: Cell<u32> = const { Cell::new(0) };
    }

    async fn counted_double(query: Query<&Amount>) -> Result<Amount, BodyError> {
        CALLS.with(|counter| counter.set(counter.get() + 1));
        Ok(Amount(query.get().0 * 2))
    }

    async fn counted_plus_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }

    /// 同一个 Definition 逻辑，被两次 execute 复用。
    fn definition<'n>(flow: &mut Flow<'n>, amount: Ref<Amount>) -> Ref<Amount> {
        let doubled = flow.then_with_output(counted_double, amount);
        flow.then_with_output(counted_plus_one, doubled)
    }

    let runtime = Runtime::new();
    let mut first_ref: Option<Ref<Amount>> = None;
    let mut second_ref: Option<Ref<Amount>> = None;

    let first = block_on(runtime.execute::<_, Amount, _>(
        |flow, amount| {
            first_ref = Some(amount);
            let out_refs = definition(flow, amount);
            flow.output_with(out_refs);
        },
        Amount(1),
    ))
    .expect("first execution");
    assert_eq!(first.0, 3);

    let second = block_on(runtime.execute::<_, Amount, _>(
        |flow, amount| {
            second_ref = Some(amount);
            let out_refs = definition(flow, amount);
            flow.output_with(out_refs);
        },
        Amount(10),
    ))
    .expect("second execution");
    assert_eq!(second.0, 21);

    // 两次执行各自真实调用（无结果／状态复用），输入与产物一一对应。
    assert_eq!(
        CALLS.with(|counter| counter.get()),
        2,
        "each execution runs the node once on its own input"
    );

    // 显式身份断言：两次执行分配的 Root Ref 不是同一位置身份。
    let first_ref = first_ref.expect("captured the first execution root ref");
    let second_ref = second_ref.expect("captured the second execution root ref");
    assert_ne!(
        format!("{first_ref:?}"),
        format!("{second_ref:?}"),
        "each execution allocates a fresh Ref identity"
    );

    // 旧 Ref 在后续执行的 Definition 中被拒绝，且该次执行零业务调用。
    let stale = first_ref;
    let error = block_on(runtime.execute::<_, Amount, _>(
        move |flow, amount| {
            let fresh = flow.then_with_output(counted_plus_one, amount);
            let _stale_use = flow.then_with_output(counted_plus_one, stale);
            let out_refs = fresh;
            flow.output_with(out_refs);
        },
        Amount(7),
    ))
    .err()
    .expect("a Ref from a previous execution must be rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    assert!(
        error.to_string().contains("another definition"),
        "rejection names the foreign definition: {error}"
    );
    assert_eq!(
        CALLS.with(|counter| counter.get()),
        2,
        "a rejected Definition runs no business Node"
    );
}

// ================================================================ S02

/// S02 正例：全部 Node 调用形式、unit／多输入、非静态 Node 的合法寿命与非 Send 数据字段。
// 本用例刻意用 `Arc<CountingStep>` 验证“非 Send 数据可参与真实执行”：`CountingStep` 携带 `Rc`
// 字段（`Steps`），故 `Arc<CountingStep>` 必然非 Send／非 Sync，而该 Arc 只在当前线程使用。
#[allow(clippy::arc_with_non_send_sync)]
#[test]
fn s02_node_forms_query_shapes_and_call_counts() {
    let steps = Steps::new();
    let delta = 7u32;
    let bump_calls = Arc::new(AtomicUsize::new(0));

    // 借用形式：句柄借用调用者本地配置（非 static 寿命），在该次 execute 内合法。
    let configured = Configured {
        delta: &delta,
        steps: steps.clone(),
    };
    // owned 形式与 Arc<具体 Struct Node> 形式。
    let owned_step = CountingStep {
        label: "owned",
        steps: steps.clone(),
    };
    let arc_step = Arc::new(CountingStep {
        label: "arc",
        steps: steps.clone(),
    });
    let pair = PairSum {
        steps: steps.clone(),
    };
    let unit_note = UnitNote {
        steps: steps.clone(),
    };
    let combo = Combo {
        steps: steps.clone(),
    };

    // 函数引用形式与 async closure 形式（多输入 closure 的引用 tuple 视图）。
    let function_ref = &plus_one;
    let bump = async |query: Query<&Amount>| -> Result<Amount, BodyError> {
        bump_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(query.get().0 + 100))
    };
    let pair_closure = async |query: Query<(&Amount, &Amount)>| -> Result<Amount, BodyError> {
        let (left, right) = query.get();
        Ok(Amount(left.0 + right.0))
    };

    let runtime = Runtime::new();
    let output = block_on(runtime.execute::<_, Report, _>(
        |flow, amount| {
            let a = flow.then_with_output(double, amount); // 函数 item
            let b = flow.then_with_output(function_ref, a); // 函数引用
            let c = flow.then_with_output(bump, b); // async closure
            let d = flow.then_with_output(&configured, c); // 借用 + 非 static 配置
            let e = flow.then_with_output(owned_step, d); // owned 具体 Struct Node
            let f = flow.then_with_output(Arc::clone(&arc_step), e); // Arc<具体 Struct Node>
            let _: () = flow.then_with_output(&unit_note, ()); // unit 输入的结构体 Node
            let g = flow.then_with_output(&pair, (f, a)); // 多输入结构体 Node
            let h = flow.then_with_output(pair_closure, (g, f)); // 多输入 async closure
            let r = flow.then_with_output(to_report, h);
            let out_refs = flow.then_with_output(&combo, (h, r));
            flow.output_with(out_refs); // 异构多输入结构体 Node
        },
        Amount(1),
    ))
    .expect("node form matrix runs");
    assert_eq!(output.0, 452, "112 + 2 = 114; 114 + 112 = 226; 226 + 226");

    assert_eq!(
        steps.take(),
        vec!["configured", "owned", "arc", "unit", "pair", "combo"],
        "every Struct Node ran exactly once, in definition order"
    );
    assert_eq!(
        bump_calls.load(Ordering::SeqCst),
        1,
        "the async closure ran exactly once"
    );
}

/// S02 正例：非 Send 数据 + future 内跨真实 await 持有输入借用并产生 owned 输出。
#[test]
fn s02_non_send_data_and_borrow_held_across_a_real_await() {
    /// 跨 await 持有输入的只读借用；await 之后仍读取同一借用（含非 Send 的 `Rc` 字段）。
    struct BorrowAcrossAwait {
        polls: Arc<AtomicUsize>,
    }

    impl Node for BorrowAcrossAwait {
        type Input = NonSendInput;
        type Output = NonSendSum;

        async fn run(&self, query: Query<&Self::Input>) -> Result<NonSendSum, BodyError> {
            let borrowed = query.get();
            let before = borrowed.value;
            PendingOnce {
                polls: Arc::clone(&self.polls),
                yielded: false,
            }
            .await;
            let tag = *borrowed.tag;
            Ok(NonSendSum {
                total: before + borrowed.value + tag,
                tag: Rc::clone(&borrowed.tag),
            })
        }
    }

    let polls = Arc::new(AtomicUsize::new(0));
    let node = BorrowAcrossAwait {
        polls: Arc::clone(&polls),
    };
    let runtime = Runtime::new();
    let output = block_on(runtime.execute::<_, NonSendSum, _>(
        |flow, input| {
            let summed = flow.then_with_output(&node, input);
            let out_refs = flow.then_with_output(duplicate_sum, summed);
            flow.output_with(out_refs);
        },
        NonSendInput {
            value: 20,
            tag: Rc::new(2),
        },
    ))
    .expect("non-Send data with a cross-await borrow runs");
    assert_eq!(
        output.total, 42,
        "20 + 20 + 2, all read from the same borrow"
    );
    assert_eq!(
        *output.tag, 2,
        "the non-Send owned output left the Runtime intact"
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        2,
        "the node really suspended once instead of completing eagerly"
    );
}

/// S02 边界：动态 Failure 的 source chain 经公开两跳可读，且后续步骤零调用。
#[test]
fn s02_dynamic_failure_preserves_its_source_and_stops_the_path() {
    let reject_calls = Arc::new(AtomicUsize::new(0));
    let after_calls = Arc::new(AtomicUsize::new(0));

    let reject = async |query: Query<&Amount>| -> Result<Report, BodyError> {
        reject_calls.fetch_add(1, Ordering::SeqCst);
        let value = query.get().0;
        Err(BodyError::fail(TooLarge {
            detail: format!("amount {value}"),
        }))
    };
    let after = async |query: Query<&Report>| -> Result<Report, BodyError> {
        after_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Report(query.get().0))
    };

    let runtime = Runtime::new();
    let error = block_on(runtime.execute::<_, Report, _>(
        |flow, amount| {
            let doubled = flow.then_with_output(double, amount);
            let rejected = flow.then_with_output(reject, doubled);
            let out_refs = flow.then_with_output(after, rejected);
            flow.output_with(out_refs);
        },
        Amount(21),
    ))
    .err()
    .expect("a dynamic Failure must terminate the path");

    // 第一跳：RunError → BodyFailure。
    let failure = error
        .source()
        .and_then(|source| source.downcast_ref::<BodyFailure>())
        .expect("RunError::source exposes the BodyFailure");
    // 第二跳：BodyFailure → 原动态对象，动态字符串按真实输入产生。
    let reason = failure
        .source()
        .and_then(|source| source.downcast_ref::<TooLarge>())
        .expect("BodyFailure::source exposes the original dynamic object");
    assert_eq!(reason.detail, "amount 42");
    assert!(failure.cleanup_source().is_none());
    assert!(matches!(error, RunError::Body(_)), "{error}");

    assert_eq!(
        reject_calls.load(Ordering::SeqCst),
        1,
        "the failing node ran once"
    );
    assert_eq!(
        after_calls.load(Ordering::SeqCst),
        0,
        "the step behind the Failure did not run"
    );
}

// ================================================================ S03

/// S03 正例：closure factory 与接收 Flow 的普通函数两种 Fragment 形态。
#[test]
fn s03_fragment_factory_and_flow_receiving_function() {
    let steps = Steps::new();
    let node = CountingStep {
        label: "step",
        steps: steps.clone(),
    };
    let runtime = Runtime::new();
    let (first, second) = block_on(runtime.execute::<_, (Amount, Amount), _>(
        |flow, amount| {
            let doubled = flow.then_with_output(double, amount);
            let first: Ref<Amount> = flow.chain_output(step_fragment(&node, doubled));
            let second: Ref<Amount> = flow.chain_output(|sub| {
                let out_refs = call_in_body(sub, &node, first);
                sub.output_with(out_refs);
            });
            let out_refs = (first, second);
            flow.output_with(out_refs);
        },
        Amount(20),
    ))
    .expect("both fragment forms run");
    assert_eq!((first.0, second.0), (41, 42));
    assert_eq!(
        steps.take(),
        vec!["step", "step"],
        "each fragment form ran its node exactly once"
    );
}

/// S03 正例：深层 capture 的真实 Import／Export 与非输出 owned 的 child 清理。
#[test]
fn s03_deep_capture_imports_ancestor_and_cleans_non_output_owned() {
    let journal = Journal::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute::<_, Amount, _>(
        |flow, seed| {
            let exported: Ref<Amount> = flow.chain_output(|outer| {
                let inner_refs: Ref<Amount> = outer.chain_output(|inner| {
                    // 深层 capture：inner 直接引用 Root 的 seed（跨两层边界真实 Import）。
                    let _temp = inner.then_with_output(track_temp, seed);
                    let declared = inner.then_with_output(seed_reader, seed);
                    inner.output_with(declared);
                });
                outer.output_with(inner_refs);
            });
            let _: () = flow.then_with_output(journal_note, seed);
            let out_refs = exported;
            flow.output_with(out_refs);
        },
        Seed {
            value: 20,
            journal: journal.clone(),
        },
    ))
    .expect("deep capture run");
    assert_eq!(
        output.0, 21,
        "the exported child output is readable by the parent"
    );
    assert_eq!(
        journal.take(),
        vec!["drop:temp".to_string(), "parent:next".to_string()],
        "the non-output child owned value is destroyed before the next parent step"
    );
}

/// S03 边界：child-local Ref 不泄漏到父层，声明输出才是 parent 可见的新位置。
#[test]
fn s03_child_local_ref_is_not_visible_in_the_parent() {
    let runtime = Runtime::new();

    // 正例：chain 的声明输出在 parent 中得到一个新位置，父层可继续读取。
    let mut child_local: Option<Ref<Amount>> = None;
    let mut exported: Option<Ref<Amount>> = None;
    let output = block_on(runtime.execute::<_, Amount, _>(
        |flow, amount| {
            let out = flow.chain_output(|sub| {
                let produced = sub.then_with_output(plus_one, amount);
                child_local = Some(produced);
                let out_refs = produced;
                sub.output_with(out_refs);
            });
            exported = Some(out);
            let out_refs = flow.then_with_output(plus_one, out);
            flow.output_with(out_refs);
        },
        Amount(1),
    ))
    .expect("export run");
    assert_eq!(output.0, 3);
    let child_local = child_local.expect("captured the child-local ref");
    let exported = exported.expect("captured the parent-visible export");
    assert_ne!(
        format!("{child_local:?}"),
        format!("{exported:?}"),
        "the parent sees a new export position, not the child-local position"
    );

    // 精确拒绝：同一 Definition 内把 child-local Ref 当父层输入使用。
    thread_local! {
        static PARENT_CALLS: Cell<u32> = const { Cell::new(0) };
    }

    async fn counted_parent(query: Query<&Amount>) -> Result<Amount, BodyError> {
        PARENT_CALLS.with(|counter| counter.set(counter.get() + 1));
        Ok(Amount(query.get().0))
    }

    let mut leaked: Option<Ref<Amount>> = None;
    let error = block_on(runtime.execute::<_, (), _>(
        |flow, amount| {
            let _kept: Ref<Amount> = flow.chain_output(|sub| {
                let produced = sub.then_with_output(counted_parent, amount);
                leaked = Some(produced);
                let out_refs = produced;
                sub.output_with(out_refs);
            });
            let child_local = leaked.expect("leaked child ref");
            let _leak_use = flow.then_with_output(counted_parent, child_local);
            let () = ();
            flow.output_with(());
        },
        Amount(1),
    ))
    .expect_err("a child-local ref must not be usable in the parent body");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    assert!(
        error.to_string().contains("not lexically visible"),
        "rejection names the lexical visibility rule: {error}"
    );
    assert_eq!(
        PARENT_CALLS.with(|counter| counter.get()),
        0,
        "a rejected Definition runs no business Node"
    );
}

/// S03 正例：同一 Fragment 复用两次，每次建立自己的 child 边界并清理自己的 owned。
#[test]
fn s03_fragment_reuse_builds_a_fresh_child_boundary_per_use() {
    let steps = Steps::new();
    let node = CountingStep {
        label: "step",
        steps: steps.clone(),
    };
    let journal = Journal::new();
    let runtime = Runtime::new();
    let (left, right) = block_on(runtime.execute::<_, (Amount, Amount), _>(
        |flow, seed| {
            let left: Ref<Amount> = flow.chain_output(temp_step_fragment(&node, seed));
            let right: Ref<Amount> = flow.chain_output(temp_step_fragment(&node, seed));
            let out_refs = (left, right);
            flow.output_with(out_refs);
        },
        Seed {
            value: 5,
            journal: journal.clone(),
        },
    ))
    .expect("fragment reuse run");
    assert_eq!((left.0, right.0), (7, 7), "(5 + 1) + 1 per use");
    assert_eq!(
        steps.take(),
        vec!["step", "step"],
        "the reused fragment called its node once per use"
    );
    assert_eq!(
        journal.take(),
        vec!["drop:temp".to_string(), "drop:temp".to_string()],
        "each use cleaned its own child-owned temporary"
    );
}

/// 六操作映射 Body 的输出形状（避免匿名长 tuple 触发 `clippy::type_complexity`）。
type SixOperationOutputs = (
    Ref<Vec<Score>>,
    Ref<Amount>,
    Ref<Amount>,
    Ref<Report>,
    Ref<Amount>,
);

/// S03 正例：同一个 child Body 内映射全六类操作（then／chain／each／choose／retry／iter）。
#[test]
fn s03_chain_body_maps_all_six_operations() {
    ATTEMPT.with(|counter| counter.set(0));
    let runtime = Runtime::new();
    let (scores, picked, nested, report, state) =
        block_on(
            runtime.execute::<_, (Vec<Score>, Amount, Amount, Report, Amount), _>(
                |flow, seed| {
                    let out_refs: SixOperationOutputs = flow.chain_output(|sub| {
                        let items = sub.then_with_output(items_of, seed);
                        let scores: Ref<Vec<Score>> = sub.each_output(items, |each| {
                            let item = each.ref_of::<Item>();

                            let out_refs = each.then_with_output(score_of, item);
                            each.output_with(out_refs);
                        });
                        let amount = sub.then_with_output(amount_of, seed);
                        let nested: Ref<Amount> = sub.chain_output(|inner| {
                            let out_refs = inner.then_with_output(double_amount, amount);
                            inner.output_with(out_refs);
                        });
                        let route = sub.then_with_output(route_of, seed);
                        let picked: Ref<Amount> = sub.choose_with_output(route, |choice| {
                            choice.case(Route { code: 1 }, |branch| {
                                let out_refs = branch.then_with_output(double_amount, amount);
                                branch.output_with(out_refs);
                            });
                            choice.otherwise(|branch| {
                                let out_refs = branch.then_with_output(identity, amount);
                                branch.output_with(out_refs);
                            });
                        });
                        let report: Ref<Report> = sub.retry_output(1, |attempt| {
                            let candidate = attempt.then_with_output(amount_of, seed);
                            let out_refs = attempt.then_with_output(judge_retry_once, candidate);
                            attempt.output_with(out_refs);
                        });
                        let zero = sub.then_with_output(zero_amount, seed);
                        let state = sub.iter_output(zero, 4, |round| {
                            let current = round.current::<Amount>();
                            let _judged = round.then_with_output(judge_break, current);
                            let next = round.then_with_output(bump_amount, current);
                            round.next_with(next);
                        });
                        let t04_out_refs = (scores, picked, nested, report, state);
                        sub.output_with(t04_out_refs);
                    });
                    flow.output_with(out_refs);
                },
                Seed {
                    value: 3,
                    journal: Journal::new(),
                },
            ),
        )
        .expect("a chain body mapping all six operations runs");

    assert_eq!(
        scores.iter().map(|score| score.0).collect::<Vec<_>>(),
        vec![30, 40],
        "each aggregated both items in input order"
    );
    assert_eq!(picked.0, 6, "choose ran the matching case on amount 3");
    assert_eq!(nested.0, 6, "the nested chain returned its declared output");
    assert_eq!(
        report.0, 30,
        "retry restarted and succeeded on the second attempt"
    );
    assert_eq!(
        state.0, 1,
        "iter continued once, then delivered the current of the breaking round"
    );
    assert_eq!(
        ATTEMPT.with(|counter| counter.get()),
        2,
        "retry ran a full attempt restart"
    );
}

/// 六操作映射样本使用的恒等 Node（choose 的非命中分支）。
async fn identity(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}
