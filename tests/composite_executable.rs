//! T01 验收：组合型 Executable 的强类型边界，以及 child 必须重新经过 Runtime。
//!
//! 测试用的组合型 Executable 定义在这里而不是生产核心中：它只用于证明执行协议，
//! 不是生产 Flow／Retry／Match。

use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};

use futures::executor::block_on;
use srflow::{Executable, ExecutionError, Node, Runtime};

/// 叶子：统计字符数。
struct CharCount;

impl Node for CharCount {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.chars().count())
    }
}

/// 叶子：翻倍。
struct Double;

impl Node for Double {
    type Input = usize;
    type Output = usize;

    async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
        Ok(input * 2)
    }
}

/// 技术失败原因，用于验证 child 错误来源被保留。
#[derive(Debug)]
struct BackendDown;

impl fmt::Display for BackendDown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("backend down")
    }
}

impl StdError for BackendDown {}

/// 叶子：永远执行失败。
struct FailingDouble;

impl Node for FailingDouble {
    type Input = usize;
    type Output = usize;

    async fn run(&self, _input: usize) -> Result<usize, ExecutionError> {
        Err(ExecutionError::new(BackendDown))
    }
}

/// 组合型 Executable：直接实现 `Executable`（不实现 `Node`），通过父级传入的 `Runtime`
/// 调用两个 child，并记录实际发起了几次 child 调用。
struct CountThenDouble {
    started: AtomicU32,
}

impl CountThenDouble {
    fn new() -> Self {
        Self {
            started: AtomicU32::new(0),
        }
    }

    fn started(&self) -> u32 {
        self.started.load(Ordering::SeqCst)
    }
}

impl Executable for CountThenDouble {
    type Input = String;
    type Output = usize;

    async fn execute(&self, runtime: &Runtime, input: String) -> Result<usize, ExecutionError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let count = runtime.execute(&CharCount, input).await?;
        self.started.fetch_add(1, Ordering::SeqCst);
        runtime.execute(&Double, count).await
    }
}

/// 组合型 Executable：中间 child 失败，用于验证错误传播、后续 child 不执行且不重试。
struct CountThenFailThenDouble {
    started: AtomicU32,
}

impl CountThenFailThenDouble {
    fn new() -> Self {
        Self {
            started: AtomicU32::new(0),
        }
    }

    fn started(&self) -> u32 {
        self.started.load(Ordering::SeqCst)
    }
}

impl Executable for CountThenFailThenDouble {
    type Input = String;
    type Output = usize;

    async fn execute(&self, runtime: &Runtime, input: String) -> Result<usize, ExecutionError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let count = runtime.execute(&CharCount, input).await?;
        self.started.fetch_add(1, Ordering::SeqCst);
        let doubled = runtime.execute(&FailingDouble, count).await?;
        self.started.fetch_add(1, Ordering::SeqCst);
        runtime.execute(&Double, doubled).await
    }
}

/// 组合型之上再套组合型：验证嵌套调用仍然经过 Runtime。
struct NestedComposite {
    inner: CountThenDouble,
}

impl Executable for NestedComposite {
    type Input = String;
    type Output = usize;

    async fn execute(&self, runtime: &Runtime, input: String) -> Result<usize, ExecutionError> {
        runtime.execute(&self.inner, input).await
    }
}

/// 编译期断言：给定值实现了 `Send`。
fn assert_send<T: Send>(_: &T) {}

#[test]
fn composite_declares_a_strongly_typed_contract() {
    fn assert_contract<E: Executable<Input = String, Output = usize>>() {}
    assert_contract::<CountThenDouble>();
    assert_contract::<NestedComposite>();
}

#[test]
fn composite_calls_children_through_runtime_in_order() {
    let runtime = Runtime::new();
    let composite = CountThenDouble::new();
    // "héllo" 是 5 个字符。结果 10 表明第二次 child 调用的 Input 来自第一次 child 的 Output：
    // `Double` 的 Input 是 usize，只能由 `CharCount` 经 Runtime 执行后产生。
    let output = block_on(runtime.execute(&composite, String::from("héllo"))).unwrap();
    assert_eq!(output, 10);
    // 两个孩子各被调用一次：没有重复执行。
    assert_eq!(composite.started(), 2);
}

#[test]
fn composite_can_be_reused_with_different_inputs() {
    let runtime = Runtime::new();
    let composite = CountThenDouble::new();
    assert_eq!(
        block_on(runtime.execute(&composite, "ab".to_string())).unwrap(),
        4
    );
    assert_eq!(
        block_on(runtime.execute(&composite, "abcd".to_string())).unwrap(),
        8
    );
    assert_eq!(composite.started(), 4);
}

#[test]
fn nested_composite_reaches_leaf_through_runtime() {
    let runtime = Runtime::new();
    let nested = NestedComposite {
        inner: CountThenDouble::new(),
    };
    assert_eq!(
        block_on(runtime.execute(&nested, "abcd".to_string())).unwrap(),
        8
    );
    assert_eq!(nested.inner.started(), 2);
}

#[test]
fn composite_execute_future_is_send() {
    let runtime = Runtime::new();
    let composite = CountThenDouble::new();
    let future = runtime.execute(&composite, String::from("ab"));
    assert_send(&future);
    assert_eq!(block_on(future).unwrap(), 4);
}

#[test]
fn child_error_propagates_without_running_later_children() {
    let runtime = Runtime::new();
    let composite = CountThenFailThenDouble::new();
    let error = block_on(runtime.execute(&composite, "abc".to_string()))
        .expect_err("child 失败时组合型 Executable 不得返回正常 Output");

    // 错误来自失败的 child，且来源被保留（不依赖 `Display` 文本）。
    assert!(error.source().unwrap().is::<BackendDown>());
    // 只发起了两次 child 调用：失败的 child 不重试，其后的 child 不执行。
    assert_eq!(composite.started(), 2);
}
