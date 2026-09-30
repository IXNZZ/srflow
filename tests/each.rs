//! T06 验收：`Each`（外部使用者视角）。
//!
//! 覆盖：Node／Flow Body、空／单项／多项调用次数、同一 Body 实例被复用、严格异步顺序（受控 Pending
//! 与事件日志）、上一项 Output 不回流、输出与输入顺序对应、首项与中间项错误传播且后续项不启动、
//! 非 `Clone` 的 Item／Output／Body、`Send + !Sync` 的 Item／Output、SubFlow Body、Each 作为父 Flow
//! child、同一实例的重复与交叠调用隔离。

use std::cell::Cell;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use futures::executor::block_on;
use srflow::{Each, ExecutionError, Flow, FlowBuilder, Node, Runtime};

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

/// 事件日志：记录 Body 调用与完成的顺序。
#[derive(Default)]
struct Log(Mutex<Vec<String>>);

impl Log {
    fn push(&self, event: impl Into<String>) {
        self.0.lock().expect("log poisoned").push(event.into());
    }

    /// 事件序列快照。
    fn snapshot(&self) -> Vec<String> {
        self.0.lock().expect("log poisoned").clone()
    }

    /// 逗号连接的事件序列，便于整体比较。
    fn text(&self) -> String {
        self.0.lock().expect("log poisoned").join(",")
    }
}

/// 手动交替驱动两个 Future，用于验证交叠调用隔离。
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

/// 直到标志被置位才完成；用于让第一项受控地停在 Pending。
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

/// 记录 `start(item)`／`end(item)` 并在中间让出一次执行权的 Body。
struct Gated {
    log: Arc<Log>,
    released: Arc<AtomicBool>,
}

impl Node for Gated {
    type Input = String;
    type Output = String;

    async fn run(&self, item: String) -> Result<String, ExecutionError> {
        self.log.push(format!("start({item})"));
        if item == "1" {
            // 第一项停在 Pending，直到测试放行。
            wait_until(&self.released).await;
        }
        yield_once().await;
        self.log.push(format!("end({item})"));
        Ok(format!("O({item})"))
    }
}

/// 记录实际收到的 Item，并返回与输入不同的结果；用于证明上一项 Output 不回流。
struct Recording {
    log: Arc<Log>,
}

impl Node for Recording {
    type Input = String;
    type Output = String;

    async fn run(&self, input: String) -> Result<String, ExecutionError> {
        let output = format!("O({input})");
        self.log.push(input);
        Ok(output)
    }
}

/// 直接持有 `AtomicUsize`：Body 本身不可 `Clone`，计数器随每次调用递增。
struct Counting {
    calls: AtomicUsize,
}

impl Node for Counting {
    type Input = String;
    type Output = usize;

    async fn run(&self, _input: String) -> Result<usize, ExecutionError> {
        Ok(self.calls.fetch_add(1, Ordering::SeqCst) + 1)
    }
}

/// Body 技术错误。
#[derive(Debug)]
struct Boom;

impl fmt::Display for Boom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("boom")
    }
}

impl StdError for Boom {}

/// 记录调用并在指定 Item 上返回技术错误的 Body。
struct Failing {
    log: Arc<Log>,
    fail_on: &'static str,
}

impl Node for Failing {
    type Input = String;
    type Output = String;

    async fn run(&self, item: String) -> Result<String, ExecutionError> {
        self.log.push(format!("run({item})"));
        if item == self.fail_on {
            return Err(ExecutionError::new(Boom));
        }
        Ok(format!("O({item})"))
    }
}

/// 只计数、原样返回的 Node，用来证明 Flow Body 内部每个 Item 都经 Runtime 执行一次。
struct Counted {
    runs: Arc<Runs>,
}

impl Node for Counted {
    type Input = String;
    type Output = String;

    async fn run(&self, input: String) -> Result<String, ExecutionError> {
        self.runs.bump();
        Ok(input)
    }
}

/// 含计数 Node 的 Flow。
fn counted_flow(runs: Arc<Runs>) -> Flow<String, String> {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let counted = flow.then_move(Counted { runs }, input).unwrap();
    flow.output(counted).unwrap()
}

/// 在一个 Flow 内嵌套另一个 Flow（真正的 SubFlow）。
fn nested_flow(runs: Arc<Runs>) -> Flow<String, String> {
    let inner = counted_flow(runs);

    let mut outer = FlowBuilder::<String>::new();
    let input = outer.input();
    let step = outer.then_move(inner, input).unwrap();
    outer.output(step).unwrap()
}

fn items(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| String::from(*value)).collect()
}

// ---- A01：Node Body 与 Flow Body ----

#[test]
fn a_node_body_maps_every_item_in_order() {
    struct Length;

    impl Node for Length {
        type Input = String;
        type Output = usize;

        async fn run(&self, input: String) -> Result<usize, ExecutionError> {
            Ok(input.chars().count())
        }
    }

    let output =
        block_on(Runtime::new().execute(&Each::new(Length), items(&["a", "bbb", "cc", "dddd"])))
            .unwrap();
    assert_eq!(output, vec![1, 3, 2, 4], "结果与输入一一对应且保持顺序");
}

#[test]
fn a_flow_body_maps_every_item_in_order() {
    struct Upper;

    impl Node for Upper {
        type Input = String;
        type Output = String;

        async fn run(&self, input: String) -> Result<String, ExecutionError> {
            Ok(input.to_uppercase())
        }
    }

    let runs = Arc::new(Runs::default());
    let mut body = FlowBuilder::<String>::new();
    let input = body.input();
    let counted = body
        .then_move(
            Counted {
                runs: Arc::clone(&runs),
            },
            input,
        )
        .unwrap();
    let upper = body.then_move(Upper, counted).unwrap();
    let body = body.output(upper).unwrap();

    let output =
        block_on(Runtime::new().execute(&Each::new(body), items(&["a", "b", "c"]))).unwrap();
    assert_eq!(output, items(&["A", "B", "C"]));
    assert_eq!(runs.count(), 3, "Flow Body 对每个 Item 执行一次");
}

// ---- A02：调用次数由集合决定，同一 Body 实例被复用 ----

#[test]
fn empty_input_does_not_run_the_body() {
    let runs = Arc::new(Runs::default());
    let output =
        block_on(Runtime::new().execute(&Each::new(counted_flow(Arc::clone(&runs))), Vec::new()))
            .unwrap();
    assert!(output.is_empty(), "空集合返回空集合，不是错误");
    assert_eq!(runs.count(), 0, "空集合不执行 Body");
}

#[test]
fn single_item_runs_the_body_once() {
    let runs = Arc::new(Runs::default());
    let output = block_on(Runtime::new().execute(
        &Each::new(counted_flow(Arc::clone(&runs))),
        items(&["only"]),
    ))
    .unwrap();
    assert_eq!(output, items(&["only"]));
    assert_eq!(runs.count(), 1);
}

#[test]
fn many_items_run_the_body_once_each() {
    let runs = Arc::new(Runs::default());
    let output = block_on(Runtime::new().execute(
        &Each::new(counted_flow(Arc::clone(&runs))),
        items(&["a", "b", "c", "d"]),
    ))
    .unwrap();
    assert_eq!(output, items(&["a", "b", "c", "d"]));
    assert_eq!(runs.count(), 4, "没有独立 limit：次数等于 Item 数");
}

#[test]
fn the_same_body_instance_is_reused_across_items() {
    // Body 直接持有 `AtomicUsize`，因此它本身不可 `Clone`；若实现为每个 Item 新建或克隆 Body，
    // 计数器都会重新从 1 开始，输出将是 [1, 1, 1]。
    let each = Each::new(Counting {
        calls: AtomicUsize::new(0),
    });
    let output = block_on(Runtime::new().execute(&each, items(&["a", "b", "c"]))).unwrap();
    assert_eq!(output, vec![1, 2, 3], "三项共用同一个 Body 实例");
}

// ---- A03：严格异步顺序与不回流 ----

#[test]
fn the_next_item_starts_only_after_the_previous_call_finishes() {
    let log = Arc::new(Log::default());
    let released = Arc::new(AtomicBool::new(false));
    let each = Each::new(Gated {
        log: Arc::clone(&log),
        released: Arc::clone(&released),
    });

    let runtime = Runtime::new();
    let mut future = pin!(runtime.execute(&each, items(&["1", "2", "3"])));
    let mut cx = Context::from_waker(Waker::noop());

    // 第一项停在 Pending：此时第二项不得已经启动。
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert_eq!(
        log.text(),
        "start(1)",
        "第一项未完成时不得启动第二项（提前启动多个子 Future 会让这里失败）"
    );

    // 放行后继续轮询到结束：事件严格交替。
    released.store(true, Ordering::SeqCst);
    let output = loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut cx) {
            break result.unwrap();
        }
    };

    assert_eq!(
        log.text(),
        "start(1),end(1),start(2),end(2),start(3),end(3)",
        "逐项串行：前一项完成后才开始下一项"
    );
    assert_eq!(output, items(&["O(1)", "O(2)", "O(3)"]));
}

#[test]
fn the_previous_output_is_not_fed_into_the_next_item() {
    // `Input == Output`：类型系统无法阻止“把上一项 Output 当成下一项 Input”，只能靠行为证明。
    let log = Arc::new(Log::default());
    let output = block_on(Runtime::new().execute(
        &Each::new(Recording {
            log: Arc::clone(&log),
        }),
        items(&["a", "b", "c"]),
    ))
    .unwrap();

    assert_eq!(output, items(&["O(a)", "O(b)", "O(c)"]));
    assert_eq!(
        log.text(),
        "a,b,c",
        "每项收到的是原始 Item，而不是上一项的 Output"
    );
}

// ---- A04：第一处错误原样传播，后续项不启动 ----

#[test]
fn the_first_item_error_propagates_and_later_items_do_not_start() {
    let log = Arc::new(Log::default());
    let error = block_on(Runtime::new().execute(
        &Each::new(Failing {
            log: Arc::clone(&log),
            fail_on: "1",
        }),
        items(&["1", "2", "3"]),
    ))
    .unwrap_err();

    assert!(
        error.source().is_some_and(|source| source.is::<Boom>()),
        "技术错误原样传播并保留来源"
    );
    assert_eq!(log.text(), "run(1)", "出错项之后的 Item 不再启动");
}

#[test]
fn a_middle_item_error_propagates_and_later_items_do_not_start() {
    let log = Arc::new(Log::default());
    let error = block_on(Runtime::new().execute(
        &Each::new(Failing {
            log: Arc::clone(&log),
            fail_on: "2",
        }),
        items(&["1", "2", "3"]),
    ))
    .unwrap_err();

    assert!(error.source().is_some_and(|source| source.is::<Boom>()));
    assert_eq!(log.text(), "run(1),run(2)", "第三项未启动");
    // 返回类型是 `Result<Vec<O>, ExecutionError>`：没有承载“部分成功”的公开路径。
}

// ---- A05：Body 为 SubFlow，Each 作为父 Flow child ----

#[test]
fn a_subflow_body_goes_through_the_runtime_for_every_item() {
    let runs = Arc::new(Runs::default());
    let output = block_on(Runtime::new().execute(
        &Each::new(nested_flow(Arc::clone(&runs))),
        items(&["a", "b", "c"]),
    ))
    .unwrap();

    assert_eq!(output, items(&["a", "b", "c"]));
    assert_eq!(runs.count(), 3, "内层 SubFlow 的 Node 对每个 Item 执行一次");
}

#[test]
fn each_works_as_a_flow_child_with_a_consuming_binding() {
    struct Claim(String);

    struct Rendered(String);

    struct Render;

    impl Node for Render {
        type Input = Claim;
        type Output = Rendered;

        async fn run(&self, input: Claim) -> Result<Rendered, ExecutionError> {
            Ok(Rendered(format!("[{}]", input.0)))
        }
    }

    struct Join;

    impl Node for Join {
        type Input = Vec<Rendered>;
        type Output = String;

        async fn run(&self, input: Vec<Rendered>) -> Result<String, ExecutionError> {
            Ok(input
                .into_iter()
                .map(|rendered| rendered.0)
                .collect::<Vec<_>>()
                .join("|"))
        }
    }

    let mut flow = FlowBuilder::<Vec<Claim>>::new();
    let input = flow.input();
    // 消费读取：整个集合（元素非 `Clone`）交给 Each。
    let rendered = flow.then_move(Each::new(Render), input).unwrap();
    let joined = flow.then_move(Join, rendered).unwrap();
    let flow = flow.output(joined).unwrap();

    let claims = vec![Claim(String::from("a")), Claim(String::from("b"))];
    let output = block_on(Runtime::new().execute(&flow, claims)).unwrap();
    assert_eq!(output, "[a]|[b]");
}

// ---- A06：非 Clone、Send + !Sync 的 Item／Output ----

#[test]
fn non_clone_items_and_outputs_are_supported() {
    struct Claim(String);

    struct Rendered(String);

    struct Render;

    impl Node for Render {
        type Input = Claim;
        type Output = Rendered;

        async fn run(&self, input: Claim) -> Result<Rendered, ExecutionError> {
            Ok(Rendered(format!("<{}>", input.0)))
        }
    }

    let output = block_on(Runtime::new().execute(
        &Each::new(Render),
        vec![Claim(String::from("a")), Claim(String::from("b"))],
    ))
    .unwrap();
    assert_eq!(
        output
            .into_iter()
            .map(|rendered| rendered.0)
            .collect::<Vec<_>>(),
        vec![String::from("<a>"), String::from("<b>")]
    );
}

#[test]
fn send_but_not_sync_items_and_outputs_are_supported() {
    struct Bump;

    impl Node for Bump {
        type Input = Cell<u32>;
        type Output = Cell<u32>;

        async fn run(&self, input: Cell<u32>) -> Result<Cell<u32>, ExecutionError> {
            input.set(input.get() + 1);
            Ok(input)
        }
    }

    let output = block_on(Runtime::new().execute(
        &Each::new(Bump),
        vec![Cell::new(1), Cell::new(10), Cell::new(100)],
    ))
    .unwrap();
    assert_eq!(
        output
            .into_iter()
            .map(|cell| cell.get())
            .collect::<Vec<_>>(),
        vec![2, 11, 101]
    );
}

// ---- A08：重复与交叠调用隔离 ----

#[test]
fn the_same_each_definition_is_isolated_across_interleaved_calls() {
    struct Interleaving {
        log: Arc<Log>,
    }

    impl Node for Interleaving {
        type Input = String;
        type Output = String;

        async fn run(&self, item: String) -> Result<String, ExecutionError> {
            self.log.push(format!("start({item})"));
            yield_once().await;
            self.log.push(format!("end({item})"));
            Ok(format!("O({item})"))
        }
    }

    let log = Arc::new(Log::default());
    let each = Each::new(Interleaving {
        log: Arc::clone(&log),
    });

    let runtime = Runtime::new();
    // 同一实例、不同输入：用输入值区分两次调用，Body 共用状态只用于日志。
    let (left, right) = block_on_both(
        runtime.execute(&each, items(&["a1", "a2"])),
        runtime.execute(&each, items(&["b1", "b2"])),
    );
    assert_eq!(left.unwrap(), items(&["O(a1)", "O(a2)"]));
    assert_eq!(right.unwrap(), items(&["O(b1)", "O(b2)"]));

    // 两次调用会交错轮询，因此不固定全局顺序；固定的是每次调用**自己**的事件序列与事件总量。
    let mut all = log.snapshot();
    all.sort();
    assert_eq!(
        all,
        items(&[
            "end(a1)",
            "end(a2)",
            "end(b1)",
            "end(b2)",
            "start(a1)",
            "start(a2)",
            "start(b1)",
            "start(b2)",
        ]),
        "四次 Body 调用各产生 start／end 各一次，没有多跑"
    );
    let per_call = |prefix: &str| {
        log.snapshot()
            .into_iter()
            .filter(|event| event.contains(prefix))
            .collect::<Vec<_>>()
            .join(",")
    };
    assert_eq!(
        per_call("(a"),
        "start(a1),end(a1),start(a2),end(a2)",
        "同一次调用内仍是逐项串行"
    );
    assert_eq!(
        per_call("(b"),
        "start(b1),end(b1),start(b2),end(b2)",
        "另一次调用各自独立推进"
    );

    // 同一实例重复调用同样没有跨调用状态。
    let again = block_on(runtime.execute(&each, items(&["c"]))).unwrap();
    assert_eq!(again, items(&["O(c)"]));
}
