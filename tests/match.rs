//! T05 验收：`Match`（外部使用者视角）。
//!
//! 覆盖：首／中／末 case 命中且仅执行被选分支、登记顺序不构成优先级、default 只在未命中时执行、
//! 无 default 的 `NoMatch` 类型化识别、空 case 集合、重复键与重复 default 的构建期拒绝及失败后
//! 恢复、被选分支与 default 错误原样传播、非 `Clone` 的 `K`／`I`／`O`、`Send + !Sync` 的 `I`／`O`、
//! 异构分支（Node／Flow／Retry／嵌套 Match）、SubFlow 分支、Match 作为父 Flow child、同一实例的
//! 重复与交叠调用隔离。

use std::cell::Cell;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use futures::executor::block_on;
use srflow::{
    ExecutionError, Flow, FlowBuilder, Match, MatchBuildError, NoMatch, Node, Retry, RetryDecision,
    Runtime, consume, field,
};

/// 路由值。刻意不实现 `Clone`：路由只需要 `Eq`，Match 也不会要求键可复制。
#[derive(Debug, PartialEq, Eq)]
enum Route {
    A,
    B,
    C,
}

/// 依次构造三个键（`Route` 不是 `Clone`，需要新值时重新构造）。
fn routes() -> [Route; 3] {
    [Route::A, Route::B, Route::C]
}

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

/// 记录被调用次数并返回 `name:input` 的分支。
struct Labeled {
    name: &'static str,
    runs: Arc<Runs>,
}

impl Node for Labeled {
    type Input = String;
    type Output = String;

    async fn run(&self, input: String) -> Result<String, ExecutionError> {
        self.runs.bump();
        Ok(format!("{}:{input}", self.name))
    }
}

/// 分支技术错误。
#[derive(Debug)]
struct Boom;

impl fmt::Display for Boom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("boom")
    }
}

impl StdError for Boom {}

/// 记录调用次数并返回技术错误的分支。
struct Failing(Arc<Runs>);

impl Node for Failing {
    type Input = String;
    type Output = String;

    async fn run(&self, _input: String) -> Result<String, ExecutionError> {
        self.0.bump();
        Err(ExecutionError::new(Boom))
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

/// 让出一次执行权后再返回的分支，用来让两次调用真正交错。
struct Yielding {
    name: &'static str,
    runs: Arc<Runs>,
}

impl Node for Yielding {
    type Input = String;
    type Output = String;

    async fn run(&self, input: String) -> Result<String, ExecutionError> {
        self.runs.bump();
        yield_once().await;
        Ok(format!("{}:{input}", self.name))
    }
}

/// 只含一个计数分支的 Flow，用作异构分支。
fn labeled_flow(name: &'static str, runs: Arc<Runs>) -> Flow<String, String> {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let labeled = flow.then_move(Labeled { name, runs }, input).unwrap();
    flow.output(labeled).unwrap()
}

/// 在一个 Flow 内嵌套另一个 Flow（真正的 SubFlow）。
fn subflow_branch(runs: Arc<Runs>) -> Flow<String, String> {
    let inner = labeled_flow("subflow", runs);

    let mut outer = FlowBuilder::<String>::new();
    let input = outer.input();
    let step = outer.then_move(inner, input).unwrap();
    outer.output(step).unwrap()
}

// ---- A01／A02：唯一命中分支，登记顺序不构成优先级 ----

#[test]
fn first_middle_and_last_case_are_selected_independently() {
    let names = ["first", "middle", "last"];
    let counters: Vec<Arc<Runs>> = (0..3).map(|_| Arc::new(Runs::default())).collect();
    let default_runs = Arc::new(Runs::default());

    let mut builder = Match::<Route, String, String>::builder();
    for (index, key) in routes().into_iter().enumerate() {
        builder
            .case(
                key,
                Labeled {
                    name: names[index],
                    runs: Arc::clone(&counters[index]),
                },
            )
            .unwrap();
    }
    builder
        .default(Labeled {
            name: "default",
            runs: Arc::clone(&default_runs),
        })
        .unwrap();
    let matcher = builder.build();

    let runtime = Runtime::new();
    for (index, key) in routes().into_iter().enumerate() {
        let before: Vec<usize> = counters.iter().map(|counter| counter.count()).collect();
        let output = block_on(runtime.execute(&matcher, (key, String::from("x")))).unwrap();
        assert_eq!(output, format!("{}:x", names[index]));
        for (other, counter) in counters.iter().enumerate() {
            // 本次调用只允许被选分支的计数增加。
            assert_eq!(
                counter.count(),
                before[other] + usize::from(other == index),
                "只有被选分支执行"
            );
        }
        assert_eq!(default_runs.count(), 0, "命中时 default 不执行");
    }
}

#[test]
fn registration_order_does_not_create_priority() {
    let runtime = Runtime::new();

    // 正序与逆序登记同一组 case：对每个键的选择结果与执行次数必须一致。
    let forward_counters: Vec<Arc<Runs>> = (0..3).map(|_| Arc::new(Runs::default())).collect();
    let mut forward = Match::<Route, String, String>::builder();
    for (index, key) in routes().into_iter().enumerate() {
        forward
            .case(
                key,
                Labeled {
                    name: "case",
                    runs: Arc::clone(&forward_counters[index]),
                },
            )
            .unwrap();
    }
    let forward = forward.build();

    let reverse_counters: Vec<Arc<Runs>> = (0..3).map(|_| Arc::new(Runs::default())).collect();
    let mut reverse = Match::<Route, String, String>::builder();
    for (index, key) in routes().into_iter().enumerate().rev() {
        reverse
            .case(
                key,
                Labeled {
                    name: "case",
                    runs: Arc::clone(&reverse_counters[index]),
                },
            )
            .unwrap();
    }
    let reverse = reverse.build();

    for (index, key) in routes().into_iter().enumerate() {
        let input = String::from("y");
        let before: Vec<usize> = forward_counters
            .iter()
            .map(|counter| counter.count())
            .collect();
        assert_eq!(
            block_on(runtime.execute(&forward, (key, input))).unwrap(),
            "case:y"
        );
        for (other, counter) in forward_counters.iter().enumerate() {
            assert_eq!(
                counter.count(),
                before[other] + usize::from(other == index),
                "顺序不形成试错链"
            );
        }
    }

    for key in routes() {
        assert_eq!(
            block_on(runtime.execute(&reverse, (key, String::from("y")))).unwrap(),
            "case:y"
        );
    }
    for counter in &reverse_counters {
        assert_eq!(counter.count(), 1);
    }
}

// ---- A03：未命中、default 与 NoMatch ----

#[test]
fn default_runs_only_when_no_case_matches() {
    let case_runs = Arc::new(Runs::default());
    let default_runs = Arc::new(Runs::default());

    let mut builder = Match::<Route, String, String>::builder();
    builder
        .case(
            Route::A,
            Labeled {
                name: "case",
                runs: Arc::clone(&case_runs),
            },
        )
        .unwrap();
    builder
        .default(Labeled {
            name: "default",
            runs: Arc::clone(&default_runs),
        })
        .unwrap();
    let matcher = builder.build();

    let runtime = Runtime::new();
    assert_eq!(
        block_on(runtime.execute(&matcher, (Route::A, String::from("x")))).unwrap(),
        "case:x"
    );
    assert_eq!(default_runs.count(), 0);

    assert_eq!(
        block_on(runtime.execute(&matcher, (Route::B, String::from("x")))).unwrap(),
        "default:x"
    );
    assert_eq!(default_runs.count(), 1);
    assert_eq!(case_runs.count(), 1, "未命中不回头执行 case");
}

#[test]
fn missing_without_default_returns_typed_no_match() {
    fn assert_error_traits<T: StdError + Send + Sync + 'static>() {}
    assert_error_traits::<NoMatch>();

    let case_runs = Arc::new(Runs::default());
    let mut builder = Match::<Route, String, String>::builder();
    builder
        .case(
            Route::A,
            Labeled {
                name: "case",
                runs: Arc::clone(&case_runs),
            },
        )
        .unwrap();
    let matcher = builder.build();

    let error =
        block_on(Runtime::new().execute(&matcher, (Route::B, String::from("x")))).unwrap_err();
    assert!(
        matches!(error, ExecutionError::Failed(_)),
        "未命中是执行失败，不是框架不变量"
    );
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<NoMatch>())
            .is_some(),
        "外部可以按类型识别 NoMatch"
    );
    assert_eq!(case_runs.count(), 0, "未命中不执行任何分支");
}

#[test]
fn empty_case_set_runs_default_when_present() {
    let default_runs = Arc::new(Runs::default());
    let mut builder = Match::<Route, String, String>::builder();
    builder
        .default(Labeled {
            name: "default",
            runs: Arc::clone(&default_runs),
        })
        .unwrap();
    let matcher = builder.build();

    assert_eq!(
        block_on(Runtime::new().execute(&matcher, (Route::C, String::from("x")))).unwrap(),
        "default:x"
    );
    assert_eq!(default_runs.count(), 1);
}

#[test]
fn empty_case_set_without_default_returns_no_match() {
    let matcher = Match::<Route, String, String>::builder().build();

    let error =
        block_on(Runtime::new().execute(&matcher, (Route::C, String::from("x")))).unwrap_err();
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<NoMatch>())
            .is_some(),
        "空 case 集合没有 default 时稳定返回 NoMatch，而不是框架不变量错误"
    );
}

// ---- A04：分支错误原样传播 ----

#[test]
fn selected_branch_error_is_propagated_without_trying_default_or_other_cases() {
    let failing_runs = Arc::new(Runs::default());
    let other_runs = Arc::new(Runs::default());
    let default_runs = Arc::new(Runs::default());

    let mut builder = Match::<Route, String, String>::builder();
    builder
        .case(Route::A, Failing(Arc::clone(&failing_runs)))
        .unwrap();
    builder
        .case(
            Route::B,
            Labeled {
                name: "other",
                runs: Arc::clone(&other_runs),
            },
        )
        .unwrap();
    builder
        .default(Labeled {
            name: "default",
            runs: Arc::clone(&default_runs),
        })
        .unwrap();
    let matcher = builder.build();

    let error =
        block_on(Runtime::new().execute(&matcher, (Route::A, String::from("x")))).unwrap_err();
    assert!(
        error.source().is_some_and(|source| source.is::<Boom>()),
        "分支错误原样传播并保留来源"
    );
    assert_eq!(failing_runs.count(), 1);
    assert_eq!(other_runs.count(), 0, "不继续寻找其他 case");
    assert_eq!(default_runs.count(), 0, "命中分支失败不改走 default");
}

#[test]
fn default_error_is_propagated() {
    let default_runs = Arc::new(Runs::default());
    let case_runs = Arc::new(Runs::default());

    let mut builder = Match::<Route, String, String>::builder();
    builder
        .case(
            Route::A,
            Labeled {
                name: "case",
                runs: Arc::clone(&case_runs),
            },
        )
        .unwrap();
    builder.default(Failing(Arc::clone(&default_runs))).unwrap();
    let matcher = builder.build();

    let error =
        block_on(Runtime::new().execute(&matcher, (Route::B, String::from("x")))).unwrap_err();
    assert!(error.source().is_some_and(|source| source.is::<Boom>()));
    assert_eq!(default_runs.count(), 1);
    assert_eq!(case_runs.count(), 0);
}

// ---- A05：重复键与重复 default ----

#[test]
fn duplicate_case_key_is_rejected_and_keeps_existing_case() {
    let first_runs = Arc::new(Runs::default());
    let rejected_runs = Arc::new(Runs::default());
    let later_runs = Arc::new(Runs::default());

    let mut builder = Match::<Route, String, String>::builder();
    builder
        .case(
            Route::A,
            Labeled {
                name: "first",
                runs: Arc::clone(&first_runs),
            },
        )
        .unwrap();
    assert_eq!(
        builder.case(
            Route::A,
            Labeled {
                name: "rejected",
                runs: Arc::clone(&rejected_runs),
            },
        ),
        Err(MatchBuildError::DuplicateCase),
        "重复键不会静默覆盖"
    );

    // 失败没有留下部分登记：已有 case 仍然生效，构建器可以继续登记并执行。
    builder
        .case(
            Route::B,
            Labeled {
                name: "later",
                runs: Arc::clone(&later_runs),
            },
        )
        .unwrap();
    let matcher = builder.build();

    let runtime = Runtime::new();
    assert_eq!(
        block_on(runtime.execute(&matcher, (Route::A, String::from("x")))).unwrap(),
        "first:x"
    );
    assert_eq!(
        block_on(runtime.execute(&matcher, (Route::B, String::from("x")))).unwrap(),
        "later:x"
    );
    assert_eq!(rejected_runs.count(), 0, "被拒绝的分支从不执行");
    assert_eq!(first_runs.count(), 1);
    assert_eq!(later_runs.count(), 1);
}

#[test]
fn duplicate_default_is_rejected_and_keeps_existing_default() {
    let first_runs = Arc::new(Runs::default());
    let rejected_runs = Arc::new(Runs::default());

    let mut builder = Match::<Route, String, String>::builder();
    builder
        .default(Labeled {
            name: "first",
            runs: Arc::clone(&first_runs),
        })
        .unwrap();
    assert_eq!(
        builder.default(Labeled {
            name: "rejected",
            runs: Arc::clone(&rejected_runs),
        }),
        Err(MatchBuildError::DuplicateDefault),
        "第二次登记 default 不会被静默覆盖"
    );
    let matcher = builder.build();

    assert_eq!(
        block_on(Runtime::new().execute(&matcher, (Route::A, String::from("x")))).unwrap(),
        "first:x"
    );
    assert_eq!(first_runs.count(), 1);
    assert_eq!(rejected_runs.count(), 0);
}

// ---- A07：异构分支、SubFlow、嵌套控制型 Executable、Match 作为 Flow child ----

#[test]
fn node_flow_and_retry_branches_share_one_contract() {
    let node_runs = Arc::new(Runs::default());
    let flow_runs = Arc::new(Runs::default());
    let retry_runs = Arc::new(Runs::default());

    let mut builder = Match::<Route, String, String>::builder();
    builder
        .case(
            Route::A,
            Labeled {
                name: "node",
                runs: Arc::clone(&node_runs),
            },
        )
        .unwrap();
    builder
        .case(Route::B, labeled_flow("flow", Arc::clone(&flow_runs)))
        .unwrap();
    builder
        .case(
            Route::C,
            Retry::new(
                Labeled {
                    name: "retry",
                    runs: Arc::clone(&retry_runs),
                },
                |_: &String| RetryDecision::Stop,
            ),
        )
        .unwrap();
    let matcher = builder.build();

    let runtime = Runtime::new();
    assert_eq!(
        block_on(runtime.execute(&matcher, (Route::A, String::from("x")))).unwrap(),
        "node:x"
    );
    assert_eq!(
        block_on(runtime.execute(&matcher, (Route::B, String::from("x")))).unwrap(),
        "flow:x"
    );
    assert_eq!(
        block_on(runtime.execute(&matcher, (Route::C, String::from("x")))).unwrap(),
        "retry:x"
    );
    assert_eq!(node_runs.count(), 1);
    assert_eq!(flow_runs.count(), 1);
    assert_eq!(retry_runs.count(), 1);
}

#[test]
fn a_subflow_branch_runs_through_the_runtime() {
    let runs = Arc::new(Runs::default());

    let mut builder = Match::<Route, String, String>::builder();
    builder
        .case(Route::A, subflow_branch(Arc::clone(&runs)))
        .unwrap();
    let matcher = builder.build();

    assert_eq!(
        block_on(Runtime::new().execute(&matcher, (Route::A, String::from("x")))).unwrap(),
        "subflow:x"
    );
    assert_eq!(runs.count(), 1, "内层 SubFlow 的 Node 恰好执行一次");
}

#[test]
fn a_match_can_be_a_branch_of_another_match() {
    let inner_branch_runs = Arc::new(Runs::default());
    let inner_default_runs = Arc::new(Runs::default());
    let outer_default_runs = Arc::new(Runs::default());

    /// 外层 default：承接外层 Match 的业务 Input `(Route, String)`。
    struct OuterDefault(Arc<Runs>);

    impl Node for OuterDefault {
        type Input = (Route, String);
        type Output = String;

        async fn run(&self, input: (Route, String)) -> Result<String, ExecutionError> {
            self.0.bump();
            Ok(format!("outer-default:{}", input.1))
        }
    }

    let mut inner = Match::<Route, String, String>::builder();
    inner
        .case(
            Route::A,
            Labeled {
                name: "inner-case",
                runs: Arc::clone(&inner_branch_runs),
            },
        )
        .unwrap();
    inner
        .default(Labeled {
            name: "inner-default",
            runs: Arc::clone(&inner_default_runs),
        })
        .unwrap();
    let inner = inner.build();

    // Match 自身的 Input 是 `(K, I)`，因此把内层 Match 当作外层分支时，外层的业务 Input 就是
    // 内层需要的 `(Route, String)`。
    let mut outer = Match::<Route, (Route, String), String>::builder();
    outer.case(Route::B, inner).unwrap();
    outer
        .default(OuterDefault(Arc::clone(&outer_default_runs)))
        .unwrap();
    let outer = outer.build();

    let runtime = Runtime::new();
    assert_eq!(
        block_on(runtime.execute(&outer, (Route::B, (Route::A, String::from("x"))))).unwrap(),
        "inner-case:x",
        "被选分支是另一个 Match，它的路由在自己的 Input 上重新生效"
    );
    assert_eq!(
        block_on(runtime.execute(&outer, (Route::C, (Route::A, String::from("x"))))).unwrap(),
        "outer-default:x",
        "外层未命中的键改走外层 default，不影响内层"
    );
    assert_eq!(inner_branch_runs.count(), 1);
    assert_eq!(inner_default_runs.count(), 0);
    assert_eq!(outer_default_runs.count(), 1);
}

#[test]
fn match_works_as_a_flow_child_with_a_tuple_binding() {
    /// 父 Flow 的 Input；根结构不需要 `Clone`。
    struct Brief {
        topic: String,
    }

    /// 判断 Node：把路由值放进正常 Output，Match 不参与这个判断。
    struct Judge;

    impl Node for Judge {
        type Input = String;
        type Output = Route;

        async fn run(&self, topic: String) -> Result<Route, ExecutionError> {
            Ok(if topic.chars().count() > 3 {
                Route::A
            } else {
                Route::B
            })
        }
    }

    /// 业务 Input 的来源 Node。
    struct Compose;

    impl Node for Compose {
        type Input = String;
        type Output = String;

        async fn run(&self, topic: String) -> Result<String, ExecutionError> {
            Ok(format!("draft:{topic}"))
        }
    }

    let branch_runs = Arc::new(Runs::default());
    let mut builder = Match::<Route, String, String>::builder();
    builder
        .case(
            Route::A,
            Labeled {
                name: "branch",
                runs: Arc::clone(&branch_runs),
            },
        )
        .unwrap();
    let matcher = builder.build();

    let mut flow = FlowBuilder::<Brief>::new();
    let brief = flow.input();
    let route = flow.then(Judge, field!(brief.topic)).unwrap();
    let draft = flow.then(Compose, field!(brief.topic)).unwrap();
    let matched = flow
        .then(matcher, (consume(route), consume(draft)))
        .unwrap();
    let flow = flow.output(matched).unwrap();

    let brief = Brief {
        topic: String::from("topic"),
    };
    assert_eq!(
        block_on(Runtime::new().execute(&flow, brief)).unwrap(),
        "branch:draft:topic"
    );
    assert_eq!(branch_runs.count(), 1);
}

// ---- A08：非 Clone 的 K／I／O，Send + !Sync 的 I／O ----

#[test]
fn non_clone_key_input_and_output_are_routed() {
    #[derive(Debug, PartialEq, Eq)]
    enum Key {
        Only,
    }

    struct Payload(String);

    struct Rendered(String);

    struct Render;

    impl Node for Render {
        type Input = Payload;
        type Output = Rendered;

        async fn run(&self, input: Payload) -> Result<Rendered, ExecutionError> {
            Ok(Rendered(input.0))
        }
    }

    let mut builder = Match::<Key, Payload, Rendered>::builder();
    builder.case(Key::Only, Render).unwrap();
    let matcher = builder.build();

    let output =
        block_on(Runtime::new().execute(&matcher, (Key::Only, Payload(String::from("value")))))
            .unwrap();
    assert_eq!(output.0, "value");
}

#[test]
fn send_but_not_sync_input_and_output_are_supported() {
    #[derive(Debug, PartialEq, Eq)]
    enum Key {
        Only,
    }

    struct Bump;

    impl Node for Bump {
        type Input = Cell<u32>;
        type Output = Cell<u32>;

        async fn run(&self, input: Cell<u32>) -> Result<Cell<u32>, ExecutionError> {
            input.set(input.get() + 1);
            Ok(input)
        }
    }

    let mut builder = Match::<Key, Cell<u32>, Cell<u32>>::builder();
    builder.case(Key::Only, Bump).unwrap();
    let matcher = builder.build();

    let output = block_on(Runtime::new().execute(&matcher, (Key::Only, Cell::new(41)))).unwrap();
    assert_eq!(output.get(), 42);
}

// ---- A09：重复与交叠调用隔离 ----

#[test]
fn the_same_match_definition_is_isolated_across_interleaved_calls() {
    let a_runs = Arc::new(Runs::default());
    let b_runs = Arc::new(Runs::default());

    let mut builder = Match::<Route, String, String>::builder();
    builder
        .case(
            Route::A,
            Yielding {
                name: "a",
                runs: Arc::clone(&a_runs),
            },
        )
        .unwrap();
    builder
        .case(
            Route::B,
            Yielding {
                name: "b",
                runs: Arc::clone(&b_runs),
            },
        )
        .unwrap();
    let matcher = builder.build();

    let runtime = Runtime::new();
    let (left, right) = block_on_both(
        runtime.execute(&matcher, (Route::A, String::from("left"))),
        runtime.execute(&matcher, (Route::B, String::from("right"))),
    );
    assert_eq!(
        left.unwrap(),
        "a:left",
        "每次调用只使用自己的键与业务 Input"
    );
    assert_eq!(right.unwrap(), "b:right");
    assert_eq!(a_runs.count(), 1);
    assert_eq!(b_runs.count(), 1);

    // 同一实例重复调用同样没有跨调用状态。
    assert_eq!(
        block_on(runtime.execute(&matcher, (Route::A, String::from("again")))).unwrap(),
        "a:again"
    );
    assert_eq!(a_runs.count(), 2);
}
