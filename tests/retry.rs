//! T04 验收：`Retry`（外部使用者视角）。
//!
//! 覆盖：默认／显式上限与零上限、do-while、`RetryDecision` 两个方向、第一轮／中途／上限停止、
//! 耗尽返回最后正常 Output、技术错误传播、Condition 调用次数与最后一轮、每轮同一 Input、
//! Input 复制口径 `n − [n == limit]`、非 `Clone` Output／Body／Condition、`Send + !Sync` 业务值、
//! Body 为 Flow、Retry 作为 Flow child、重复／交叠调用隔离。

use std::cell::Cell;
use std::error::Error as StdError;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use futures::executor::block_on;
use srflow::{
    Executable, ExecutionError, Flow, FlowBuilder, Node, Retry, RetryDecision, Runtime, consume,
};

fn stop_if(accepted: &bool) -> RetryDecision {
    if *accepted {
        RetryDecision::Stop
    } else {
        RetryDecision::Retry
    }
}

fn stop_if_pair(output: &(u32, bool)) -> RetryDecision {
    if output.1 {
        RetryDecision::Stop
    } else {
        RetryDecision::Retry
    }
}

/// 手动交替驱动两个 Future（不依赖 executor 的并发特性），用于验证交叠调用隔离。
fn block_on_both<A, B>(a: impl Future<Output = A>, b: impl Future<Output = B>) -> (A, B) {
    let mut a = pin!(a);
    let mut b = pin!(b);
    let mut cx = Context::from_waker(Waker::noop());
    let mut ra = None;
    let mut rb = None;
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
    }
    (ra.unwrap(), rb.unwrap())
}

// ---- 可观察替身 ----

#[derive(Default)]
struct Runs(AtomicUsize);

impl Runs {
    /// 记一次执行并返回第几次（从 1 开始）。
    fn bump(&self) -> usize {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct Inputs(Mutex<Vec<u32>>);

impl Inputs {
    fn record(&self, input: u32) {
        self.0.lock().expect("inputs poisoned").push(input);
    }

    fn seen(&self) -> Vec<u32> {
        self.0.lock().expect("inputs poisoned").clone()
    }
}

/// 记录每轮收到的 Input，并按自身尝试计数决定是否接受。
struct Attempts {
    inputs: Arc<Inputs>,
    runs: Arc<Runs>,
    accept_from: usize,
}

impl Node for Attempts {
    type Input = u32;
    type Output = (u32, bool);

    async fn run(&self, input: u32) -> Result<(u32, bool), ExecutionError> {
        let attempt = self.runs.bump();
        self.inputs.record(input);
        Ok((input + attempt as u32, attempt >= self.accept_from))
    }
}

/// 按尝试计数决定接受；Body 类型不实现 `Clone`。
struct Counting {
    runs: Arc<Runs>,
    accept_from: usize,
}

impl Node for Counting {
    type Input = u32;
    type Output = bool;

    async fn run(&self, _input: u32) -> Result<bool, ExecutionError> {
        Ok(self.runs.bump() >= self.accept_from)
    }
}

// ---- A01：上限 ----

#[test]
fn default_limit_is_eight_body_runs() {
    let runs = Arc::new(Runs::default());
    let retry = Retry::new(
        Counting {
            runs: Arc::clone(&runs),
            accept_from: usize::MAX,
        },
        |_: &bool| RetryDecision::Retry,
    );
    assert_eq!(retry.limit().get(), 8);
    assert_eq!(
        Retry::<Counting, fn(&bool) -> RetryDecision>::DEFAULT_LIMIT.get(),
        8
    );

    let output = block_on(Runtime::new().execute(&retry, 0)).unwrap();
    assert!(!output, "一直要求 retry 时返回最后一轮正常 Output");
    assert_eq!(runs.count(), 8, "默认上限是 Body 总执行 8 次");
}

#[test]
fn explicit_limit_one_runs_the_body_once() {
    let runs = Arc::new(Runs::default());
    let retry = Retry::with_limit(
        Counting {
            runs: Arc::clone(&runs),
            accept_from: usize::MAX,
        },
        |_: &bool| RetryDecision::Retry,
        NonZeroUsize::new(1).unwrap(),
    );
    let output = block_on(Runtime::new().execute(&retry, 0)).unwrap();
    assert!(!output);
    assert_eq!(runs.count(), 1);
}

#[test]
fn explicit_limit_three_runs_at_most_three() {
    let runs = Arc::new(Runs::default());
    let retry = Retry::with_limit(
        Counting {
            runs: Arc::clone(&runs),
            accept_from: usize::MAX,
        },
        |_: &bool| RetryDecision::Retry,
        NonZeroUsize::new(3).unwrap(),
    );
    let _ = block_on(Runtime::new().execute(&retry, 0)).unwrap();
    assert_eq!(runs.count(), 3);
}

#[test]
fn zero_limit_cannot_be_represented() {
    // `limit` 用 `NonZeroUsize`，所以不存在“Body 执行零次”的合法配置。
    assert!(NonZeroUsize::new(0).is_none());
}

// ---- A03/A04/A05：Condition 方向与停止点 ----

#[test]
fn stop_on_the_first_round_does_not_run_again() {
    let runs = Arc::new(Runs::default());
    let retry = Retry::with_limit(
        Counting {
            runs: Arc::clone(&runs),
            accept_from: 1,
        },
        stop_if,
        NonZeroUsize::new(3).unwrap(),
    );
    assert!(block_on(Runtime::new().execute(&retry, 0)).unwrap());
    assert_eq!(runs.count(), 1);
}

#[test]
fn stop_midway_returns_that_round_output() {
    let runs = Arc::new(Runs::default());
    let retry = Retry::with_limit(
        Counting {
            runs: Arc::clone(&runs),
            accept_from: 2,
        },
        stop_if,
        NonZeroUsize::new(3).unwrap(),
    );
    assert!(block_on(Runtime::new().execute(&retry, 0)).unwrap());
    assert_eq!(runs.count(), 2);
}

#[test]
fn stop_exactly_at_the_limit_uses_that_round() {
    let runs = Arc::new(Runs::default());
    let retry = Retry::with_limit(
        Counting {
            runs: Arc::clone(&runs),
            accept_from: 3,
        },
        stop_if,
        NonZeroUsize::new(3).unwrap(),
    );
    assert!(block_on(Runtime::new().execute(&retry, 0)).unwrap());
    assert_eq!(runs.count(), 3);
}

#[test]
fn exhausting_the_limit_returns_the_last_normal_output_not_an_error() {
    let inputs = Arc::new(Inputs::default());
    let runs = Arc::new(Runs::default());
    let retry = Retry::with_limit(
        Attempts {
            inputs: Arc::clone(&inputs),
            runs: Arc::clone(&runs),
            accept_from: usize::MAX,
        },
        stop_if_pair,
        NonZeroUsize::new(3).unwrap(),
    );
    // 每轮 Output 都不同：input + attempt；最后一轮是 7 + 3。
    let output = block_on(Runtime::new().execute(&retry, 7)).unwrap();
    assert_eq!(output, (10, false));
    assert_eq!(runs.count(), 3);
}

// ---- A02：每轮同一 Input ----

#[test]
fn every_round_receives_the_same_original_input() {
    let inputs = Arc::new(Inputs::default());
    let runs = Arc::new(Runs::default());
    let retry = Retry::with_limit(
        Attempts {
            inputs: Arc::clone(&inputs),
            runs: Arc::clone(&runs),
            accept_from: usize::MAX,
        },
        stop_if_pair,
        NonZeroUsize::new(3).unwrap(),
    );
    let _ = block_on(Runtime::new().execute(&retry, 42)).unwrap();
    assert_eq!(
        inputs.seen(),
        vec![42, 42, 42],
        "上一轮 Output 不作为下一轮 Input"
    );
}

// ---- A06：技术错误 ----

#[derive(Debug)]
struct BackendDown;

impl std::fmt::Display for BackendDown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("backend down")
    }
}

impl std::error::Error for BackendDown {}

struct FailsOnSecond {
    runs: Arc<Runs>,
}

impl Node for FailsOnSecond {
    type Input = u32;
    type Output = u32;

    async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
        if self.runs.bump() >= 2 {
            Err(ExecutionError::new(BackendDown))
        } else {
            Ok(input)
        }
    }
}

#[test]
fn a_technical_error_propagates_without_calling_the_condition() {
    let runs = Arc::new(Runs::default());
    let condition_calls = Arc::new(Runs::default());
    let observed = Arc::clone(&condition_calls);
    let retry = Retry::with_limit(
        FailsOnSecond {
            runs: Arc::clone(&runs),
        },
        move |_: &u32| {
            observed.bump();
            RetryDecision::Retry
        },
        NonZeroUsize::new(3).unwrap(),
    );

    let error = block_on(Runtime::new().execute(&retry, 1)).unwrap_err();
    assert!(error.source().unwrap().is::<BackendDown>(), "保留原始来源");
    assert_eq!(runs.count(), 2, "出错轮之后不再执行");
    assert_eq!(condition_calls.count(), 1, "出错轮不调用 Condition");
}

// ---- A03：Condition 调用次数与非 Clone Output ----

#[test]
fn condition_is_called_once_per_normal_output_including_the_last_round() {
    let runs = Arc::new(Runs::default());
    let condition_calls = Arc::new(Runs::default());
    let observed = Arc::clone(&condition_calls);
    let retry = Retry::with_limit(
        Counting {
            runs: Arc::clone(&runs),
            accept_from: usize::MAX,
        },
        move |_: &bool| {
            observed.bump();
            RetryDecision::Retry
        },
        NonZeroUsize::new(3).unwrap(),
    );
    let _ = block_on(Runtime::new().execute(&retry, 0)).unwrap();
    assert_eq!(
        condition_calls.count(),
        3,
        "每个正常 Output（含最后一轮）恰调用一次"
    );
}

#[test]
fn non_clone_output_is_returned_by_value() {
    struct NoClone(u32);
    struct Make;

    impl Node for Make {
        type Input = u32;
        type Output = NoClone;

        async fn run(&self, input: u32) -> Result<NoClone, ExecutionError> {
            Ok(NoClone(input))
        }
    }

    let retry = Retry::new(Make, |output: &NoClone| {
        if output.0 > 0 {
            RetryDecision::Stop
        } else {
            RetryDecision::Retry
        }
    });
    let output = block_on(Runtime::new().execute(&retry, 5)).unwrap();
    assert_eq!(output.0, 5);
}

// ---- A08：Input 复制口径 ----

#[derive(Default)]
struct CloneLog(AtomicUsize);

struct Counted {
    value: u32,
    log: Arc<CloneLog>,
}

impl Clone for Counted {
    fn clone(&self) -> Self {
        self.log.0.fetch_add(1, Ordering::SeqCst);
        Self {
            value: self.value,
            log: Arc::clone(&self.log),
        }
    }
}

struct AcceptCounted {
    runs: Arc<Runs>,
    accept_from: usize,
}

impl Node for AcceptCounted {
    type Input = Counted;
    type Output = bool;

    async fn run(&self, _input: Counted) -> Result<bool, ExecutionError> {
        Ok(self.runs.bump() >= self.accept_from)
    }
}

fn counted_case(limit: usize, accept_from: usize) -> (usize, usize) {
    let log = Arc::new(CloneLog::default());
    let runs = Arc::new(Runs::default());
    let retry = Retry::with_limit(
        AcceptCounted {
            runs: Arc::clone(&runs),
            accept_from,
        },
        stop_if,
        NonZeroUsize::new(limit).unwrap(),
    );
    let counted = Counted {
        value: 1,
        log: Arc::clone(&log),
    };
    let _ = block_on(Runtime::new().execute(&retry, counted)).unwrap();
    (runs.count(), log.0.load(Ordering::SeqCst))
}

#[test]
fn input_clone_cost_follows_n_minus_last_round_rule() {
    assert_eq!(
        counted_case(1, 1),
        (1, 0),
        "limit = 1：移动原始 Input，不复制"
    );
    assert_eq!(counted_case(3, 1), (1, 1), "第一轮停止：复制 1 次");
    assert_eq!(counted_case(3, 2), (2, 2), "第二轮停止：复制 2 次");
    assert_eq!(counted_case(3, usize::MAX), (3, 2), "执行满三轮：复制 2 次");
}

// ---- A08：非 Clone Body／Condition；Send + !Sync ----

#[test]
fn body_and_condition_need_not_be_clone() {
    /// 没有实现 `Clone` 的资源。
    struct Guard(String);

    let guard = Guard(String::from("resource"));
    // Body（Counting）没有实现 Clone；Condition 捕获非 Clone 值，闭包本身因此不可 Clone。
    let retry = Retry::new(
        Counting {
            runs: Arc::new(Runs::default()),
            accept_from: 1,
        },
        move |_: &bool| {
            let _ = &guard.0;
            RetryDecision::Stop
        },
    );
    assert!(block_on(Runtime::new().execute(&retry, 0)).unwrap());
}

#[test]
fn send_but_not_sync_input_and_output_are_supported() {
    /// `Send` 但不是 `Sync`、且没有实现 `Clone` 的 Output。
    struct NotSyncOut {
        hits: Cell<u32>,
    }

    struct Echo;

    impl Node for Echo {
        type Input = Cell<u32>;
        type Output = NotSyncOut;

        async fn run(&self, input: Cell<u32>) -> Result<NotSyncOut, ExecutionError> {
            input.set(input.get() + 1);
            Ok(NotSyncOut { hits: input })
        }
    }

    let retry = Retry::with_limit(
        Echo,
        |_output: &NotSyncOut| RetryDecision::Stop,
        NonZeroUsize::new(2).unwrap(),
    );
    let output = block_on(Runtime::new().execute(&retry, Cell::new(9))).unwrap();
    assert_eq!(output.hits.get(), 10);
}

// ---- A07：递归 Runtime 与组合 ----

/// 组合型 Body：自身通过 Runtime 调用 child，用于确认每轮都重新经过 Runtime。
struct Composite {
    calls: Arc<Runs>,
    child_runs: Arc<Runs>,
}

impl Executable for Composite {
    type Input = u32;
    type Output = u32;

    async fn execute(&self, runtime: &Runtime, input: u32) -> Result<u32, ExecutionError> {
        self.calls.bump();
        runtime
            .execute(
                &Counting {
                    runs: Arc::clone(&self.child_runs),
                    accept_from: 1,
                },
                input,
            )
            .await?;
        Ok(input)
    }
}

#[test]
fn every_round_body_call_goes_through_the_runtime() {
    let calls = Arc::new(Runs::default());
    let child_runs = Arc::new(Runs::default());
    let retry = Retry::with_limit(
        Composite {
            calls: Arc::clone(&calls),
            child_runs: Arc::clone(&child_runs),
        },
        |_: &u32| RetryDecision::Retry,
        NonZeroUsize::new(3).unwrap(),
    );

    let _ = block_on(Runtime::new().execute(&retry, 1)).unwrap();
    assert_eq!(calls.count(), 3, "Body 每轮都被执行一次");
    assert_eq!(child_runs.count(), 3, "每轮 Body 的 child 也经 Runtime");
}

struct AddOne;

impl Node for AddOne {
    type Input = u32;
    type Output = u32;

    async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
        Ok(input + 1)
    }
}

fn add_one_flow() -> Flow<u32, u32> {
    let mut flow = FlowBuilder::<u32>::new();
    let input = flow.input();
    let out = flow.then(AddOne, input).unwrap();
    flow.output(out).unwrap()
}

#[test]
fn body_can_be_a_flow() {
    let retry = Retry::with_limit(
        add_one_flow(),
        |output: &u32| {
            if *output >= 1 {
                RetryDecision::Stop
            } else {
                RetryDecision::Retry
            }
        },
        NonZeroUsize::new(2).unwrap(),
    );
    assert_eq!(block_on(Runtime::new().execute(&retry, 41)).unwrap(), 42);
}

#[test]
fn retry_is_an_ordinary_flow_child() {
    let mut flow = FlowBuilder::<u32>::new();
    let input = flow.input();
    let retried = flow
        .then(
            Retry::with_limit(
                Counting {
                    runs: Arc::new(Runs::default()),
                    accept_from: usize::MAX,
                },
                |_: &bool| RetryDecision::Retry,
                NonZeroUsize::new(2).unwrap(),
            ),
            input,
        )
        .unwrap();
    let flow = flow.output(retried).unwrap();
    assert!(!block_on(Runtime::new().execute(&flow, 3)).unwrap());
}

#[test]
fn retry_can_be_connected_with_a_consuming_binding() {
    let mut flow = FlowBuilder::<u32>::new();
    let input = flow.input();
    let retried = flow
        .then(
            Retry::with_limit(
                Counting {
                    runs: Arc::new(Runs::default()),
                    accept_from: 1,
                },
                stop_if,
                NonZeroUsize::new(2).unwrap(),
            ),
            consume(input),
        )
        .unwrap();
    let flow = flow.output(retried).unwrap();
    assert!(block_on(Runtime::new().execute(&flow, 5)).unwrap());
}

// ---- A08：重复与交叠调用隔离 ----

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

/// 每轮让出一次执行权，使并发调用真正交错；输出只由本轮 Input 决定，不依赖跨轮状态。
struct Doubling {
    runs: Arc<Runs>,
}

impl Node for Doubling {
    type Input = u32;
    type Output = u32;

    async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
        self.runs.bump();
        yield_once().await;
        Ok(input * 2)
    }
}

#[test]
fn the_same_retry_instance_is_isolated_across_interleaved_calls() {
    let runtime = Runtime::new();
    let runs = Arc::new(Runs::default());
    // 同一个实例：limit = 2，Condition 始终要求 Retry，因此每次调用恰好执行两轮。
    let retry = Retry::with_limit(
        Doubling {
            runs: Arc::clone(&runs),
        },
        |_: &u32| RetryDecision::Retry,
        NonZeroUsize::new(2).unwrap(),
    );

    // 同一个 `&retry`、两个不同输入，两轮执行交错进行。
    let (left, right) = block_on_both(runtime.execute(&retry, 100), runtime.execute(&retry, 200));
    assert_eq!(left.unwrap(), 200, "只用本次调用的 Input，不串另一个调用");
    assert_eq!(right.unwrap(), 400);
    assert_eq!(runs.count(), 4, "两次调用各执行两轮");

    // 同一实例重复调用同样没有跨调用状态。
    assert_eq!(block_on(runtime.execute(&retry, 300)).unwrap(), 600);
}
