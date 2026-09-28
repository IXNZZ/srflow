//! T01 验收：从独立使用者视角，只实现 `Node` 即可经 `Runtime` 执行。
//!
//! 覆盖：正常 Output、业务否定 Output、叶子执行错误及其来源、重复调用与显式状态边界。
//! 这些测试只使用 crate 的公开 API（`srflow::{ExecutionError, Node, Runtime}`）。

use std::cell::Cell;
use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{ExecutionError, Node, Runtime};

/// 最小正常路径：Input → Output。
struct Square;

impl Node for Square {
    type Input = u32;
    type Output = u32;

    async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
        Ok(input * input)
    }
}

/// 业务否定结论：执行成功，只是业务判断为“不接受”。
struct LimitCheck {
    limit: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct CheckResult {
    accepted: bool,
}

impl Node for LimitCheck {
    type Input = u32;
    type Output = CheckResult;

    async fn run(&self, input: u32) -> Result<CheckResult, ExecutionError> {
        Ok(CheckResult {
            accepted: input <= self.limit,
        })
    }
}

/// 技术执行失败：外部依赖不可用。
#[derive(Debug)]
struct NetworkDown;

impl fmt::Display for NetworkDown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("network down")
    }
}

impl StdError for NetworkDown {}

struct FetchNumber;

impl Node for FetchNumber {
    type Input = u32;
    type Output = u32;

    async fn run(&self, _input: u32) -> Result<u32, ExecutionError> {
        Err(ExecutionError::new(NetworkDown))
    }
}

/// 显式持有状态：设计允许 Node 持有配置或计数器。
///
/// 这里把计数器放进 Output，是为了说明状态只有被 Node 自己显式使用时才可见；
/// 框架不会把它变成下一次调用的 Input。
struct CallingCounter {
    calls: AtomicUsize,
}

impl CallingCounter {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl Node for CallingCounter {
    type Input = u32;
    type Output = (u32, usize);

    async fn run(&self, input: u32) -> Result<(u32, usize), ExecutionError> {
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok((input, call_index))
    }
}

/// 非 `Sync` 的 Node：`Cell` 使它不是 `Sync`，但它的 `run` Future 不捕获 `self`，因此仍然是
/// `Send`。这类 Node 必须能经 `Runtime` 得到 `Send` Future。
struct NonSyncNode {
    calls: Cell<u32>,
}

impl NonSyncNode {
    fn new() -> Self {
        Self {
            calls: Cell::new(0),
        }
    }
}

// 刻意不用 `async fn`：`async fn` 会把 `&self` 捕获进 Future，从而要求 `Self: Sync`。
// 这个 Node 的全部意义就是“非 Sync 的 Self 也能给出 Send Future”，因此必须显式写返回类型。
#[allow(clippy::manual_async_fn)]
impl Node for NonSyncNode {
    type Input = u32;
    type Output = u32;

    fn run(&self, input: u32) -> impl Future<Output = Result<u32, ExecutionError>> + Send {
        // 不引用 self：即使 Self 不是 Sync，返回的 Future 依然是 Send。
        async move { Ok(input + 1) }
    }
}

/// 编译期断言：给定值实现了 `Send`。
fn assert_send<T: Send>(_: &T) {}

#[test]
fn node_only_execution_through_runtime() {
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(&Square, 7)).unwrap();
    assert_eq!(output, 49);
}

#[test]
fn one_runtime_serves_repeated_calls() {
    let runtime = Runtime::new();
    let node = Square;
    // 同一个 Runtime 可以反复发起执行，不需要复制它。
    assert_eq!(block_on(runtime.execute(&node, 2)).unwrap(), 4);
    assert_eq!(block_on(runtime.execute(&node, 3)).unwrap(), 9);
}

#[test]
fn runtime_execute_future_is_send_for_every_node_shape() {
    let runtime = Runtime::new();
    let square_future = runtime.execute(&Square, 7);
    assert_send(&square_future);
    assert_eq!(block_on(square_future).unwrap(), 49);

    // 非 Sync 的 Node 也必须得到 Send Future：Runtime 不额外捕获 executable。
    let node = NonSyncNode::new();
    let non_sync_future = runtime.execute(&node, 1);
    assert_send(&non_sync_future);
    assert_eq!(block_on(non_sync_future).unwrap(), 2);
    assert_eq!(
        node.calls.get(),
        0,
        "run 不使用该字段，它只是让类型不是 Sync"
    );
}

#[test]
fn negative_business_result_is_output_not_error() {
    let runtime = Runtime::new();
    let result = block_on(runtime.execute(&LimitCheck { limit: 3 }, 10));
    let output = result.expect("业务否定结论是正常 Output，不是执行错误");
    assert_eq!(output, CheckResult { accepted: false });
}

#[test]
fn leaf_execution_error_propagates_with_its_source() {
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(&FetchNumber, 1)).unwrap_err();
    // 来源类型必须可追溯；`Display` 文本不是契约，因此不做等值断言。
    let source = error.source().expect("底层错误必须被保留");
    assert!(source.is::<NetworkDown>(), "来源类型必须可追溯");
}

#[test]
fn same_executable_can_be_called_repeatedly() {
    let runtime = Runtime::new();
    let node = Square;
    // 相同 Input 得到相同 Output：框架没有把上一次调用的数据带进这一次。
    assert_eq!(block_on(runtime.execute(&node, 5)).unwrap(), 25);
    assert_eq!(block_on(runtime.execute(&node, 5)).unwrap(), 25);
    // 不同 Input 得到各自对应的 Output。
    assert_eq!(block_on(runtime.execute(&node, 3)).unwrap(), 9);
}

#[test]
fn node_state_is_visible_only_where_the_node_puts_it() {
    let runtime = Runtime::new();
    let node = CallingCounter::new();
    // 计数器是 Node 显式持有的状态，并且只因为它被写进 Output 才可见。
    assert_eq!(block_on(runtime.execute(&node, 5)).unwrap(), (5, 0));
    assert_eq!(block_on(runtime.execute(&node, 6)).unwrap(), (6, 1));
    // 第一个元素只由本次 Input 决定，与调用次数无关。
    assert_eq!(block_on(runtime.execute(&node, 7)).unwrap().0, 7);
}
