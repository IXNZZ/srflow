//! T02 验收：Flow 的执行语义、值存储所有权与 SubFlow。
//!
//! 覆盖：声明顺序与数据依赖无关、显式 Output（含非最后一步与 Flow Input 本身）、非 `Clone`
//! 值直通、复用读取的复制代价、fail-fast、重复与交叠调用互不串值、SubFlow 组合与错误传播。

use std::error::Error as StdError;
use std::fmt;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::task::Poll;

use futures::executor::block_on;
use srflow::{ExecutionError, Flow, FlowBuilder, Node, Runtime};

/// 以 4 bit/步 编码的执行顺序日志（低位是最早的步骤）。
#[derive(Default)]
struct StepLog {
    encoded: AtomicU32,
}

impl StepLog {
    fn record(&self, code: u32) {
        self.encoded
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                Some((value << 4) | code)
            })
            .ok();
    }

    fn steps(&self) -> Vec<u32> {
        let mut encoded = self.encoded.load(Ordering::SeqCst);
        let mut steps = Vec::new();
        while encoded != 0 {
            steps.push(encoded & 0xF);
            encoded >>= 4;
        }
        steps.reverse();
        steps
    }
}

/// 记录自己的执行，并返回输入文本的字符数。
struct RecordedLength {
    log: Arc<StepLog>,
    code: u32,
}

impl Node for RecordedLength {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        self.log.record(self.code);
        Ok(input.chars().count())
    }
}

/// 记录自己的执行，并原样返回数值。
struct RecordedPass {
    log: Arc<StepLog>,
    code: u32,
}

impl Node for RecordedPass {
    type Input = usize;
    type Output = usize;

    async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
        self.log.record(self.code);
        Ok(input)
    }
}

/// 技术失败原因，用于验证来源保留。
#[derive(Debug)]
struct BackendDown;

impl fmt::Display for BackendDown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("backend down")
    }
}

impl StdError for BackendDown {}

/// 记录自己的执行，然后失败。
struct RecordedFailure {
    log: Arc<StepLog>,
    code: u32,
}

impl Node for RecordedFailure {
    type Input = usize;
    type Output = usize;

    async fn run(&self, _input: usize) -> Result<usize, ExecutionError> {
        self.log.record(self.code);
        Err(ExecutionError::new(BackendDown))
    }
}

/// 没有记录、只做类型转换的叶子。
struct Length;

impl Node for Length {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.chars().count())
    }
}

struct Doubling;

impl Node for Doubling {
    type Input = usize;
    type Output = usize;

    async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
        Ok(input * 2)
    }
}

struct Render;

impl Node for Render {
    type Input = usize;
    type Output = String;

    async fn run(&self, input: usize) -> Result<String, ExecutionError> {
        Ok(format!("<{input}>"))
    }
}

/// 让出一次执行权，使并发调用真正交错。
async fn yield_once() {
    let mut yielded = false;
    poll_fn(move |cx| {
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

struct YieldingLength;

impl Node for YieldingLength {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        yield_once().await;
        Ok(input.chars().count())
    }
}

/// 克隆次数的共享计数器。
#[derive(Debug, Default)]
struct CloneCounter {
    clones: AtomicUsize,
}

impl CloneCounter {
    fn count(&self) -> usize {
        self.clones.load(Ordering::SeqCst)
    }
}

/// 每次克隆都记账的业务值。
#[derive(Debug)]
struct Tracked {
    text: String,
    counter: Arc<CloneCounter>,
}

impl Tracked {
    fn new(text: &str, counter: Arc<CloneCounter>) -> Self {
        Self {
            text: text.to_string(),
            counter,
        }
    }
}

impl Clone for Tracked {
    fn clone(&self) -> Self {
        self.counter.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            text: self.text.clone(),
            counter: Arc::clone(&self.counter),
        }
    }
}

struct MeasureTracked;

impl Node for MeasureTracked {
    type Input = Tracked;
    type Output = usize;

    async fn run(&self, input: Tracked) -> Result<usize, ExecutionError> {
        Ok(input.text.chars().count())
    }
}

/// 没有实现 `Clone` 的业务值。
#[derive(Debug, PartialEq, Eq)]
struct NonClone(String);

struct DescribeNonClone;

impl Node for DescribeNonClone {
    type Input = NonClone;
    type Output = usize;

    async fn run(&self, input: NonClone) -> Result<usize, ExecutionError> {
        Ok(input.0.chars().count())
    }
}

/// 子 Flow：字符数 → 翻倍。内部 Ref 不会离开这个函数。
fn counting_doubling_flow(log: Arc<StepLog>) -> Flow<String, usize> {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let length = flow
        .then_move(
            RecordedLength {
                log: Arc::clone(&log),
                code: 1,
            },
            input,
        )
        .unwrap();
    let doubled = flow.then_move(Doubling, length).unwrap();
    flow.output(doubled).unwrap()
}

/// 子 Flow：先成功一步，再失败。
fn failing_flow(log: Arc<StepLog>) -> Flow<String, usize> {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let length = flow
        .then_move(
            RecordedLength {
                log: Arc::clone(&log),
                code: 1,
            },
            input,
        )
        .unwrap();
    let failure = flow
        .then_move(
            RecordedFailure {
                log: Arc::clone(&log),
                code: 2,
            },
            length,
        )
        .unwrap();
    flow.output(failure).unwrap()
}

#[test]
fn steps_run_in_declaration_order_even_without_data_dependency() {
    let log = Arc::new(StepLog::default());
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    // 第二步读取的是 Flow Input，而不是第一步的 Output：它仍然必须排在第一步之后执行。
    let first = flow
        .then(
            RecordedLength {
                log: Arc::clone(&log),
                code: 1,
            },
            input,
        )
        .unwrap();
    let _second = flow
        .then(
            RecordedLength {
                log: Arc::clone(&log),
                code: 2,
            },
            input,
        )
        .unwrap();
    let flow = flow.output(first).unwrap();

    let output = block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap();
    assert_eq!(output, 4);
    assert_eq!(log.steps(), vec![1, 2]);
}

#[test]
fn output_can_select_a_non_final_step() {
    let log = Arc::new(StepLog::default());
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let length = flow
        .then(
            RecordedLength {
                log: Arc::clone(&log),
                code: 1,
            },
            input,
        )
        .unwrap();
    let _doubled = flow
        .then(
            RecordedPass {
                log: Arc::clone(&log),
                code: 2,
            },
            length,
        )
        .unwrap();
    // Output 选的是非最后一步产生的值：结果是字符数，不是加倍后的值。
    let flow = flow.output(length).unwrap();

    let output = block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap();
    assert_eq!(output, 4);
    assert_eq!(log.steps(), vec![1, 2]);
}

#[test]
fn flow_input_can_be_the_output_directly() {
    let flow = FlowBuilder::<NonClone>::new();
    let input = flow.input();
    let flow = flow.output(input).unwrap();

    let value = block_on(Runtime::new().execute(&flow, NonClone(String::from("passed")))).unwrap();
    assert_eq!(value, NonClone(String::from("passed")));
}

#[test]
fn non_clone_value_passes_through_a_node_with_then_move() {
    let mut flow = FlowBuilder::<NonClone>::new();
    let input = flow.input();
    let described = flow.then_move(DescribeNonClone, input).unwrap();
    let flow = flow.output(described).unwrap();

    let output = block_on(Runtime::new().execute(&flow, NonClone(String::from("abcd")))).unwrap();
    assert_eq!(output, 4);
}

#[test]
fn a_single_reader_never_copies_the_value() {
    let counter = Arc::new(CloneCounter::default());
    let mut flow = FlowBuilder::<Tracked>::new();
    let input = flow.input();
    // 即使选择复用读取，只有一次读取时也不会复制业务值。
    let measured = flow.then(MeasureTracked, input).unwrap();
    let flow = flow.output(measured).unwrap();

    let output =
        block_on(Runtime::new().execute(&flow, Tracked::new("abcd", Arc::clone(&counter))))
            .unwrap();

    assert_eq!(output, 4);
    assert_eq!(counter.count(), 0);
}

#[test]
fn reusing_one_position_copies_exactly_once_for_two_readers() {
    let counter = Arc::new(CloneCounter::default());
    let mut flow = FlowBuilder::<Tracked>::new();
    let input = flow.input();
    let first = flow.then(MeasureTracked, input).unwrap(); // 读取 1/2：给出副本
    let _second = flow.then(MeasureTracked, input).unwrap(); // 读取 2/2：直接移动原值
    let flow = flow.output(first).unwrap();

    let output =
        block_on(Runtime::new().execute(&flow, Tracked::new("abcd", Arc::clone(&counter))))
            .unwrap();

    assert_eq!(output, 4);
    assert_eq!(counter.count(), 1, "两个消费者只需要一次复制");
}

#[test]
fn output_reading_a_reused_position_counts_as_an_extra_copy() {
    let counter = Arc::new(CloneCounter::default());
    let mut flow = FlowBuilder::<Tracked>::new();
    let input = flow.input();
    let _first = flow.then(MeasureTracked, input).unwrap(); // 读取 1/3：给出副本
    let _second = flow.then(MeasureTracked, input).unwrap(); // 读取 2/3：给出副本
    // `output` 也选中同一位置：读取 3/3 并取走原值。含最终 Output 在内共读取 3 次、
    // 复制 2 次——最终 Output 的消费同样计入复制成本。
    let flow = flow.output(input).unwrap();

    let output =
        block_on(Runtime::new().execute(&flow, Tracked::new("abcd", Arc::clone(&counter))))
            .unwrap();

    assert_eq!(output.text, "abcd");
    assert_eq!(
        counter.count(),
        2,
        "两个复用读者加最终 Output 共读取 3 次，复制 2 次"
    );
}

#[test]
fn step_failure_stops_the_flow_without_partial_output() {
    let log = Arc::new(StepLog::default());
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let measured = flow
        .then_move(
            RecordedLength {
                log: Arc::clone(&log),
                code: 1,
            },
            input,
        )
        .unwrap();
    let failed = flow
        .then_move(
            RecordedFailure {
                log: Arc::clone(&log),
                code: 2,
            },
            measured,
        )
        .unwrap();
    let after = flow
        .then_move(
            RecordedPass {
                log: Arc::clone(&log),
                code: 3,
            },
            failed,
        )
        .unwrap();
    let flow = flow.output(after).unwrap();

    let error = block_on(Runtime::new().execute(&flow, String::from("abc"))).unwrap_err();

    // 错误保留来源，且不转换为业务 Output。
    assert!(matches!(error, ExecutionError::Failed(_)));
    assert!(error.source().unwrap().is::<BackendDown>());
    // 前一步的副作用已经发生（不回滚），失败之后的步骤不执行。
    assert_eq!(log.steps(), vec![1, 2]);
}

#[test]
fn flow_stores_children_with_different_input_and_output_types() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let length = flow.then_move(Length, input).unwrap(); // String -> usize
    let rendered = flow.then_move(Render, length).unwrap(); // usize -> String
    let measured = flow.then_move(Length, rendered).unwrap(); // String -> usize
    let flow = flow.output(measured).unwrap();

    // "abcd" -> 4 -> "<4>" -> 3
    let output = block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap();
    assert_eq!(output, 3);
}

#[test]
fn repeated_and_interleaved_calls_do_not_share_values() {
    let runtime = Runtime::new();
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let length = flow.then_move(YieldingLength, input).unwrap();
    let flow = flow.output(length).unwrap();

    // 顺序重复调用：每次执行使用独立的值存储。
    assert_eq!(
        block_on(runtime.execute(&flow, String::from("ab"))).unwrap(),
        2
    );
    assert_eq!(
        block_on(runtime.execute(&flow, String::from("abcd"))).unwrap(),
        4
    );

    // 两次调用交错进行：输出各自对应当次的 Input。
    let first = runtime.execute(&flow, String::from("abc"));
    let second = runtime.execute(&flow, String::from("abcde"));
    let (first, second) = block_on(futures::future::join(first, second));
    assert_eq!(first.unwrap(), 3);
    assert_eq!(second.unwrap(), 5);
}

#[test]
fn subflow_is_an_ordinary_child() {
    let log = Arc::new(StepLog::default());
    let mut parent = FlowBuilder::<String>::new();
    let input = parent.input();
    // 父级只连接子 Flow 的 Input／Output：子 Flow 内部的中间 Ref 不出现在这里。
    let counted = parent
        .then_move(counting_doubling_flow(Arc::clone(&log)), input)
        .unwrap();
    let parent = parent.output(counted).unwrap();

    let output = block_on(Runtime::new().execute(&parent, String::from("abcd"))).unwrap();
    // 结果 8 = 2 × 4 说明子 Flow 内部两步都执行了（只有第一步记录顺序日志）。
    assert_eq!(output, 8);
    assert_eq!(log.steps(), vec![1]);
}

#[test]
fn subflow_error_stops_the_parent_flow() {
    let log = Arc::new(StepLog::default());
    let mut parent = FlowBuilder::<String>::new();
    let input = parent.input();
    let counted = parent
        .then_move(failing_flow(Arc::clone(&log)), input)
        .unwrap();
    let after = parent
        .then_move(
            RecordedPass {
                log: Arc::clone(&log),
                code: 3,
            },
            counted,
        )
        .unwrap();
    let parent = parent.output(after).unwrap();

    let error = block_on(Runtime::new().execute(&parent, String::from("abc"))).unwrap_err();
    assert!(matches!(error, ExecutionError::Failed(_)));
    assert!(error.source().unwrap().is::<BackendDown>());
    assert_eq!(log.steps(), vec![1, 2], "子 Flow 失败后父级后续步骤不执行");
}
