//! T07 验收：`Iter`（外部使用者视角）。
//!
//! 覆盖：Node／SubFlow Body、空／单项／多项、显式跨轮状态传递（顺序敏感）、不变上下文的保持、
//! 正常业务状态不触发提前停止、严格异步顺序（受控 Pending + 事件日志 + 下一轮收到上一轮真实状态）、
//! 首轮与中途错误传播且后续项不启动、非 `Clone` 的 Item／T／Body、`Send + !Sync` 的 Item／T、
//! 父 Flow 的两位置消费组合、同一实例的重复与交叠调用隔离。

use std::cell::Cell;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use futures::executor::block_on;
use srflow::{ExecutionError, Flow, FlowBuilder, Iter, Node, Runtime, consume, field};

/// 执行计数。
#[derive(Default)]
struct Runs(AtomicUsize);

impl Runs {
    fn bump(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// 事件日志。
#[derive(Default)]
struct Log(Mutex<Vec<String>>);

impl Log {
    fn push(&self, event: impl Into<String>) {
        self.0.lock().expect("log poisoned").push(event.into());
    }

    /// 逗号连接的事件序列，便于整体比较。
    fn text(&self) -> String {
        self.0.lock().expect("log poisoned").join(",")
    }
}

/// 手动交替驱动两个 Future，用于验证交叠调用隔离。
///
/// 轮询次数有上限：两个 Future 都没完成时明确失败，而不是无限空转。
fn block_on_both<A, B>(a: impl Future<Output = A>, b: impl Future<Output = B>) -> (A, B) {
    const MAX_ROUNDS: usize = 10_000;

    let mut a = pin!(a);
    let mut b = pin!(b);
    let mut cx = Context::from_waker(Waker::noop());
    let mut ra = None;
    let mut rb = None;
    let mut rounds = 0;
    while ra.is_none() || rb.is_none() {
        if ra.is_none()
            && let Poll::Ready(value) = a.as_mut().poll(&mut cx)
        {
            ra = Some(value);
        }
        if rb.is_none()
            && let Poll::Ready(value) = b.as_mut().poll(&mut cx)
        {
            rb = Some(value);
        }
        rounds += 1;
        if rounds > MAX_ROUNDS {
            panic!("两次调用在 {MAX_ROUNDS} 次轮询内仍未完成（实现可能一直处于 Pending）");
        }
    }
    (ra.unwrap(), rb.unwrap())
}

/// 有上限地轮询到 `Ready`；超过上限仍未完成时明确失败。
fn poll_until_ready<F: Future>(
    future: &mut Pin<&mut F>,
    cx: &mut Context<'_>,
    what: &str,
) -> F::Output {
    const MAX_POLLS: usize = 1_000;

    for _ in 0..MAX_POLLS {
        if let Poll::Ready(value) = future.as_mut().poll(cx) {
            return value;
        }
    }
    panic!("{what}：超过 {MAX_POLLS} 次轮询仍未完成（实现可能一直处于 Pending）");
}

async fn yield_once() {
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// 直到标志被置位才完成。
async fn wait_until(flag: &AtomicBool) {
    std::future::poll_fn(|_cx| {
        if flag.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await
}

/// 关键节点（Item）；刻意不实现 `Clone`。
struct Key(String);

/// 跨轮状态；刻意不实现 `Clone` 与 `Default`。
struct Prose {
    plan: String,
    prose: String,
    rounds: u32,
}

/// 按顺序累积正文的 Body，并记录每轮形成的状态摘要。
struct Advancing {
    log: Arc<Log>,
    runs: Arc<Runs>,
}

impl Node for Advancing {
    type Input = (Prose, Key);
    type Output = Prose;

    async fn run(&self, input: (Prose, Key)) -> Result<Prose, ExecutionError> {
        let (mut prose, key) = input;
        self.runs.bump();
        // 顺序敏感：结果依赖 Item 顺序，也与“看到上一轮状态”直接相关。
        prose.prose.push_str(&key.0);
        prose.rounds += 1;
        self.log.push(format!("{}/{}", prose.plan, prose.prose));
        Ok(prose)
    }
}

fn prose() -> Prose {
    Prose {
        plan: String::from("plan"),
        prose: String::new(),
        rounds: 0,
    }
}

fn keys(values: &[&str]) -> Vec<Key> {
    values
        .iter()
        .map(|value| Key(String::from(*value)))
        .collect()
}

/// 取错误而不要求 Output 实现 `Debug`（`Prose`／`Report` 都刻意不实现它）。
fn expect_error<T>(result: Result<T, ExecutionError>) -> ExecutionError {
    match result {
        Ok(_) => panic!("期望执行失败，但成功返回"),
        Err(error) => error,
    }
}

/// 带标记的状态，用于验证受控 Pending 时“下一轮收到的是上一轮真实输出”。
struct Stamped {
    tag: String,
}

/// 第一轮停在 Pending，并把收到的状态标记写进日志。
struct Gated {
    log: Arc<Log>,
    released: Arc<AtomicBool>,
}

impl Node for Gated {
    type Input = (Stamped, Key);
    type Output = Stamped;

    async fn run(&self, input: (Stamped, Key)) -> Result<Stamped, ExecutionError> {
        let (state, key) = input;
        self.log.push(format!("start({}:{})", key.0, state.tag));
        if key.0 == "1" {
            wait_until(&self.released).await;
        }
        yield_once().await;
        let next = Stamped {
            tag: format!("T{}", key.0),
        };
        self.log.push(format!("end({})", next.tag));
        Ok(next)
    }
}

/// 业务状态与进度分离：`needs_revision` 是正常状态，不应让迭代提前停止。
struct Report {
    needs_revision: bool,
    seen: String,
}

/// 第二轮把 `needs_revision` 置位，但必须继续处理第三个 Item。
struct Marking {
    log: Arc<Log>,
    runs: Arc<Runs>,
}

impl Node for Marking {
    type Input = (Report, Key);
    type Output = Report;

    async fn run(&self, input: (Report, Key)) -> Result<Report, ExecutionError> {
        let (mut report, key) = input;
        self.runs.bump();
        if key.0 == "2" {
            report.needs_revision = true;
        }
        report.seen.push_str(&key.0);
        self.log.push(format!("round({})", key.0));
        Ok(report)
    }
}

/// 技术错误。
#[derive(Debug)]
struct Boom;

impl fmt::Display for Boom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("boom")
    }
}

impl StdError for Boom {}

/// 记录轮次并在指定 Item 上返回技术错误。
struct Failing {
    log: Arc<Log>,
    fail_on: &'static str,
}

impl Node for Failing {
    type Input = (Prose, Key);
    type Output = Prose;

    async fn run(&self, input: (Prose, Key)) -> Result<Prose, ExecutionError> {
        let (mut prose, key) = input;
        self.log.push(format!("round({})", key.0));
        if key.0 == self.fail_on {
            return Err(ExecutionError::new(Boom));
        }
        prose.prose.push_str(&key.0);
        Ok(prose)
    }
}

/// Body 本身不可 `Clone`（`AtomicUsize` 没有 `Clone` 实现）。
///
/// `calls` 只用于**测试观察**（同一个 Body 实例被复用了多少次）；业务状态 `rounds` 必须由传入的
/// 上一轮 `T` 递增，不能依赖 Body 的隐藏状态。
struct Counting {
    calls: Arc<Runs>,
    _not_clone: AtomicUsize,
}

impl Node for Counting {
    type Input = (Prose, Key);
    type Output = Prose;

    async fn run(&self, input: (Prose, Key)) -> Result<Prose, ExecutionError> {
        let (mut prose, key) = input;
        self.calls.bump();
        prose.rounds += 1;
        prose.prose.push_str(&key.0);
        Ok(prose)
    }
}

/// 只计数、原样返回状态的 Node。
struct Counted {
    runs: Arc<Runs>,
}

impl Node for Counted {
    type Input = (Prose, Key);
    type Output = Prose;

    async fn run(&self, input: (Prose, Key)) -> Result<Prose, ExecutionError> {
        let (state, _key) = input;
        self.runs.bump();
        Ok(state)
    }
}

/// 含计数 Node 的 Flow Body。
fn counted_body(runs: Arc<Runs>) -> Flow<(Prose, Key), Prose> {
    let mut flow = FlowBuilder::<(Prose, Key)>::new();
    let input = flow.input();
    let counted = flow.then_move(Counted { runs }, input).unwrap();
    flow.output(counted).unwrap()
}

/// 在一个 Flow 内嵌套另一个 Flow（真正的 SubFlow）。
fn nested_body(runs: Arc<Runs>) -> Flow<(Prose, Key), Prose> {
    let inner = counted_body(runs);

    let mut outer = FlowBuilder::<(Prose, Key)>::new();
    let input = outer.input();
    let step = outer.then_move(inner, input).unwrap();
    outer.output(step).unwrap()
}

// ---- A01／A02：跨轮状态推进 ----

#[test]
fn a_node_body_advances_state_across_rounds() {
    let log = Arc::new(Log::default());
    let runs = Arc::new(Runs::default());
    let iter = Iter::new(Advancing {
        log: Arc::clone(&log),
        runs: Arc::clone(&runs),
    });

    let state = block_on(Runtime::new().execute(&iter, (keys(&["a", "b", "c"]), prose()))).unwrap();

    assert_eq!(runs.count(), 3, "三个 Item 各执行一轮");
    assert_eq!(state.prose, "abc", "顺序敏感地累积每轮状态");
    assert_eq!(state.plan, "plan", "不变上下文每轮都可用并保留到最后");
    assert_eq!(state.rounds, 3);
    assert_eq!(
        log.text(),
        "plan/a,plan/ab,plan/abc",
        "第 k 轮看到的是第 k-1 轮实际形成的状态"
    );
}

#[test]
fn a_normal_business_state_does_not_stop_iteration() {
    let log = Arc::new(Log::default());
    let runs = Arc::new(Runs::default());
    let iter = Iter::new(Marking {
        log: Arc::clone(&log),
        runs: Arc::clone(&runs),
    });

    let initial = Report {
        needs_revision: false,
        seen: String::new(),
    };
    let report =
        block_on(Runtime::new().execute(&iter, (keys(&["1", "2", "3"]), initial))).unwrap();

    assert_eq!(
        log.text(),
        "round(1),round(2),round(3)",
        "第二轮给出 needs_revision 后，第三轮仍然运行"
    );
    assert_eq!(runs.count(), 3, "总轮数等于 Item 数");
    assert!(report.needs_revision);
    assert_eq!(report.seen, "123", "业务标记不改变逐项推进");
}

#[test]
fn a_flow_body_advances_state_across_rounds() {
    /// Body Flow 的第一步：追加。
    struct Append;

    impl Node for Append {
        type Input = (Prose, Key);
        type Output = Prose;

        async fn run(&self, input: (Prose, Key)) -> Result<Prose, ExecutionError> {
            let (mut prose, key) = input;
            prose.prose.push_str(&key.0);
            Ok(prose)
        }
    }

    /// Body Flow 的第二步：计数轮次。
    struct Count;

    impl Node for Count {
        type Input = Prose;
        type Output = Prose;

        async fn run(&self, mut prose: Prose) -> Result<Prose, ExecutionError> {
            prose.rounds += 1;
            Ok(prose)
        }
    }

    let mut body = FlowBuilder::<(Prose, Key)>::new();
    let input = body.input();
    let appended = body.then_move(Append, input).unwrap();
    let counted = body.then_move(Count, appended).unwrap();
    let body = body.output(counted).unwrap();

    let state =
        block_on(Runtime::new().execute(&Iter::new(body), (keys(&["a", "b", "c", "d"]), prose())))
            .unwrap();
    assert_eq!(state.prose, "abcd");
    assert_eq!(state.rounds, 4);
    assert_eq!(state.plan, "plan");
}

// ---- A03：空集合 ----

#[test]
fn an_empty_input_returns_the_initial_state_by_value() {
    let runs = Arc::new(Runs::default());
    let iter = Iter::new(counted_body(Arc::clone(&runs)));

    // `Prose` 既没有 `Clone` 也没有 `Default`：空集合只能按值返回原始状态。
    let state = block_on(Runtime::new().execute(&iter, (Vec::new(), prose()))).unwrap();

    assert_eq!(runs.count(), 0, "空集合不执行 Body");
    assert_eq!(state.plan, "plan");
    assert_eq!(state.prose, "");
    assert_eq!(state.rounds, 0);
}

// ---- A04：严格异步顺序与真实状态回流 ----

#[test]
fn the_next_round_starts_only_after_the_previous_round_finishes() {
    let log = Arc::new(Log::default());
    let released = Arc::new(AtomicBool::new(false));
    let iter = Iter::new(Gated {
        log: Arc::clone(&log),
        released: Arc::clone(&released),
    });

    let runtime = Runtime::new();
    let initial = Stamped {
        tag: String::from("T0"),
    };
    let mut future = pin!(runtime.execute(&iter, (keys(&["1", "2"]), initial)));
    let mut cx = Context::from_waker(Waker::noop());

    // 第一轮停在 Pending：第二轮不得已经启动，且第一轮收到的是初始状态。
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert_eq!(
        log.text(),
        "start(1:T0)",
        "第一轮未完成时不得启动第二轮（提前启动多个 Future 会让这里失败）"
    );

    released.store(true, Ordering::SeqCst);
    let state =
        poll_until_ready(&mut future, &mut cx, "Iter 应在放行后的少数几次轮询内完成").unwrap();

    assert_eq!(
        log.text(),
        "start(1:T0),end(T1),start(2:T1),end(T2)",
        "严格交替，且第二轮收到的是第一轮实际返回的 T1"
    );
    assert_eq!(state.tag, "T2", "最终只返回最后一轮的状态");
}

// ---- A05：错误传播 ----

#[test]
fn a_first_round_error_propagates_and_later_items_do_not_start() {
    let log = Arc::new(Log::default());
    let error = expect_error(block_on(Runtime::new().execute(
        &Iter::new(Failing {
            log: Arc::clone(&log),
            fail_on: "1",
        }),
        (keys(&["1", "2", "3"]), prose()),
    )));

    assert!(
        error.source().is_some_and(|source| source.is::<Boom>()),
        "技术错误原样传播并保留来源"
    );
    assert_eq!(log.text(), "round(1)", "出错轮之后的 Item 不再启动");
    // 返回类型是 `Result<T, ExecutionError>`：没有承载“中间状态／部分成功”的公开路径。
}

#[test]
fn a_middle_round_error_propagates_and_later_items_do_not_start() {
    let log = Arc::new(Log::default());
    let error = expect_error(block_on(Runtime::new().execute(
        &Iter::new(Failing {
            log: Arc::clone(&log),
            fail_on: "2",
        }),
        (keys(&["1", "2", "3"]), prose()),
    )));

    assert!(error.source().is_some_and(|source| source.is::<Boom>()));
    assert_eq!(log.text(), "round(1),round(2)", "第三轮未启动");
}

// ---- A06：SubFlow Body、父 Flow child ----

#[test]
fn a_subflow_body_goes_through_the_runtime_for_every_round() {
    let runs = Arc::new(Runs::default());
    let state = block_on(Runtime::new().execute(
        &Iter::new(nested_body(Arc::clone(&runs))),
        (keys(&["a", "b", "c"]), prose()),
    ))
    .unwrap();

    assert_eq!(runs.count(), 3, "内层 SubFlow 的 Node 每轮执行一次");
    assert_eq!(state.plan, "plan");
}

#[test]
fn iter_works_as_a_flow_child_with_two_consumed_positions() {
    /// 父 Flow 的根结构。
    struct Brief {
        topics: Vec<String>,
        plan: String,
    }

    struct MakeKeys;

    impl Node for MakeKeys {
        type Input = Vec<String>;
        type Output = Vec<Key>;

        async fn run(&self, topics: Vec<String>) -> Result<Vec<Key>, ExecutionError> {
            Ok(topics.into_iter().map(Key).collect())
        }
    }

    struct MakeInitial;

    impl Node for MakeInitial {
        type Input = String;
        type Output = Prose;

        async fn run(&self, plan: String) -> Result<Prose, ExecutionError> {
            Ok(Prose {
                plan,
                prose: String::new(),
                rounds: 0,
            })
        }
    }

    struct Report;

    impl Node for Report {
        type Input = Prose;
        type Output = String;

        async fn run(&self, prose: Prose) -> Result<String, ExecutionError> {
            Ok(format!("{}/{}", prose.plan, prose.prose))
        }
    }

    // 两个不同位置（集合与初始状态）在 tuple 内分别消费；两者都不是 `Clone` 值。
    let body_calls = Arc::new(Runs::default());
    let mut flow = FlowBuilder::<Brief>::new();
    let brief = flow.input();
    let topics = flow.then(MakeKeys, field!(brief.topics)).unwrap();
    let initial = flow.then(MakeInitial, field!(brief.plan)).unwrap();
    let advanced = flow
        // Body 逐轮把关键节点追加进正文：状态确实跨轮流动。
        .then(
            Iter::new(Counting {
                calls: Arc::clone(&body_calls),
                _not_clone: AtomicUsize::new(0),
            }),
            (consume(topics), consume(initial)),
        )
        .unwrap();
    let report = flow.then_move(Report, advanced).unwrap();
    let flow = flow.output(report).unwrap();

    let brief = Brief {
        topics: vec![String::from("a"), String::from("b")],
        plan: String::from("plan"),
    };
    let output = block_on(Runtime::new().execute(&flow, brief)).unwrap();
    assert_eq!(output, "plan/ab");
    assert_eq!(body_calls.count(), 2, "两个 Item 各推进一轮");
}

// ---- A07：非 Clone、Send + !Sync ----

#[test]
fn non_clone_items_state_and_body_are_supported() {
    // `Key`／`Prose` 不可 `Clone`；`Counting` 含 `AtomicUsize` 字段，因此 Body 也不可 `Clone`。
    let calls = Arc::new(Runs::default());
    let iter = Iter::new(Counting {
        calls: Arc::clone(&calls),
        _not_clone: AtomicUsize::new(0),
    });

    let state = block_on(Runtime::new().execute(&iter, (keys(&["a", "b", "c"]), prose()))).unwrap();
    assert_eq!(state.prose, "abc", "顺序敏感累积");
    assert_eq!(
        state.rounds, 3,
        "轮次由传入状态递增，而不是 Body 的隐藏计数器"
    );
    assert_eq!(calls.count(), 3, "同一个 Body 实例被复用三次（观察值）");
}

#[test]
fn send_but_not_sync_items_and_state_are_supported() {
    struct Bump;

    impl Node for Bump {
        type Input = (Cell<u32>, Cell<u32>);
        type Output = Cell<u32>;

        async fn run(
            &self,
            (state, item): (Cell<u32>, Cell<u32>),
        ) -> Result<Cell<u32>, ExecutionError> {
            state.set(state.get() + item.get());
            Ok(state)
        }
    }

    let state = block_on(Runtime::new().execute(
        &Iter::new(Bump),
        (vec![Cell::new(1), Cell::new(2), Cell::new(3)], Cell::new(0)),
    ))
    .unwrap();
    assert_eq!(state.get(), 6);
}

// ---- A09：重复与交叠调用隔离 ----

#[test]
fn the_same_iter_definition_is_isolated_across_interleaved_calls() {
    struct Interleaving {
        log: Arc<Log>,
    }

    impl Node for Interleaving {
        type Input = (Prose, Key);
        type Output = Prose;

        async fn run(&self, input: (Prose, Key)) -> Result<Prose, ExecutionError> {
            let (mut prose, key) = input;
            self.log.push(format!("start({}:{})", key.0, prose.prose));
            yield_once().await;
            prose.prose.push_str(&key.0);
            self.log.push(format!("end({}:{})", key.0, prose.prose));
            Ok(prose)
        }
    }

    let log = Arc::new(Log::default());
    let iter = Iter::new(Interleaving {
        log: Arc::clone(&log),
    });

    let runtime = Runtime::new();
    // 用各自的 Item 标识区分两次调用，不依赖 Body 的共享状态判断归属。
    let (left, right) = block_on_both(
        runtime.execute(&iter, (keys(&["a1", "a2"]), prose())),
        runtime.execute(&iter, (keys(&["b1", "b2"]), prose())),
    );
    assert_eq!(left.unwrap().prose, "a1a2");
    assert_eq!(right.unwrap().prose, "b1b2");

    // 每次调用内部的状态推进序列必须各自独立。
    let per_call = |marker: &str| {
        log.text()
            .split(',')
            .filter(|event| event.contains(marker))
            .collect::<Vec<_>>()
            .join(",")
    };
    assert_eq!(
        per_call("a1"),
        "start(a1:),end(a1:a1),start(a2:a1),end(a2:a1a2)",
        "第一次调用从初始状态开始并逐轮推进"
    );
    assert_eq!(
        per_call("b1"),
        "start(b1:),end(b1:b1),start(b2:b1),end(b2:b1b2)",
        "第二次调用有自己独立的当前状态"
    );

    // 同一实例重复调用同样从新的初始状态开始。
    let again = block_on(runtime.execute(&iter, (keys(&["c"]), prose()))).unwrap();
    assert_eq!(again.prose, "c");
}
