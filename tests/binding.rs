//! T03 验收：Binding 与 Input 装配（外部使用者视角）。
//!
//! 覆盖：统一入口与 `then_move` 等价性、读取语义冲突、字段投影、2～4 元 tuple、命名与嵌套
//! 装配、跨 Flow Ref（藏在投影／tuple／装配中）构建期拒绝与登记原子性、非 `Clone` 根投影、
//! 大字段不被复制、`Send + !Sync` 业务值、Binding 不产生额外 child、fail-fast。

use std::cell::Cell;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{ExecutionError, FlowBuildError, FlowBuilder, Node, Runtime};
use srflow::{bind, consume, field};

/// 记录 child 执行顺序的日志。
#[derive(Default)]
struct Log {
    seen: Mutex<Vec<u32>>,
}

impl Log {
    fn record(&self, code: u32) {
        self.seen.lock().expect("log poisoned").push(code);
    }

    fn seen(&self) -> Vec<u32> {
        self.seen.lock().expect("log poisoned").clone()
    }
}

fn shared(log: &Arc<Log>) -> Arc<Log> {
    Arc::clone(log)
}

/// 记录执行并原样返回文本。
struct RecordText {
    log: Arc<Log>,
    code: u32,
}

impl Node for RecordText {
    type Input = String;
    type Output = String;

    async fn run(&self, input: String) -> Result<String, ExecutionError> {
        self.log.record(self.code);
        Ok(input)
    }
}

/// 字符数。
struct TextLen;

impl Node for TextLen {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.chars().count())
    }
}

/// 数值加一。
struct PlusOne;

impl Node for PlusOne {
    type Input = usize;
    type Output = usize;

    async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
        Ok(input + 1)
    }
}

/// 数值翻倍。
struct DoubleU32;

impl Node for DoubleU32 {
    type Input = u32;
    type Output = u32;

    async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
        Ok(input * 2)
    }
}

/// 两个 `u32` 相加。
struct Sum2U32;

impl Node for Sum2U32 {
    type Input = (u32, u32);
    type Output = u32;

    async fn run(&self, (a, b): (u32, u32)) -> Result<u32, ExecutionError> {
        Ok(a + b)
    }
}

/// 四数相加。
struct Sum4;

impl Node for Sum4 {
    type Input = (usize, usize, usize, usize);
    type Output = usize;

    async fn run(
        &self,
        (a, b, c, d): (usize, usize, usize, usize),
    ) -> Result<usize, ExecutionError> {
        Ok(a + b + c + d)
    }
}

/// 两个字符串拼接。
struct Concat;

impl Node for Concat {
    type Input = (String, String);
    type Output = String;

    async fn run(&self, (a, b): (String, String)) -> Result<String, ExecutionError> {
        Ok(format!("{a}{b}"))
    }
}

/// 一个用于命名装配的业务结构（字段类型不同）。
struct Pair {
    left: String,
    right: String,
}

/// 记录执行并拼接装配好的结构。
struct PairRecorded {
    log: Arc<Log>,
    code: u32,
}

impl Node for PairRecorded {
    type Input = Pair;
    type Output = String;

    async fn run(&self, input: Pair) -> Result<String, ExecutionError> {
        self.log.record(self.code);
        Ok(format!("{}+{}", input.left, input.right))
    }
}

/// 两个 `u32` 的命名结构（用于跨 Flow 装配拒绝测试）。
struct Tuple2 {
    a: u32,
    b: u32,
}

/// 记录执行后失败。
struct Failing {
    log: Arc<Log>,
    code: u32,
}

impl Node for Failing {
    type Input = Pair;
    type Output = Pair;

    async fn run(&self, _input: Pair) -> Result<Pair, ExecutionError> {
        self.log.record(self.code);
        Err(ExecutionError::new("backend down"))
    }
}

// ---- A01：统一入口与读取语义 ----

#[test]
fn then_move_is_equivalent_to_an_explicit_consume_binding() {
    let via_then_move = {
        let mut flow = FlowBuilder::<String>::new();
        let input = flow.input();
        let out = flow.then_move(TextLen, input).unwrap();
        let flow = flow.output(out).unwrap();
        block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap()
    };
    let via_consume = {
        let mut flow = FlowBuilder::<String>::new();
        let input = flow.input();
        let out = flow.then(TextLen, consume(input)).unwrap();
        let flow = flow.output(out).unwrap();
        block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap()
    };
    assert_eq!(via_then_move, via_consume);
}

#[test]
fn reuse_then_consume_across_steps_is_allowed() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let reused = flow.then(TextLen, input).unwrap();
    let _ = reused;
    let consumed = flow.then_move(TextLen, input).unwrap();
    let flow = flow.output(consumed).unwrap();

    assert_eq!(
        block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap(),
        4
    );
}

#[test]
fn consume_then_reuse_is_rejected_at_build_time() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let _ = flow.then(TextLen, consume(input)).unwrap();
    let error = flow.then(TextLen, input).unwrap_err();
    assert_eq!(error, FlowBuildError::SourceAlreadyConsumed);
}

// ---- A04：读取计划原子性与冲突 ----

#[test]
fn consuming_and_reusing_one_position_inside_one_binding_is_rejected() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let error = flow.then(Concat, (consume(input), input)).unwrap_err();
    assert_eq!(error, FlowBuildError::ReadModeConflict);
}

#[test]
fn consuming_one_position_twice_inside_one_binding_is_rejected() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let error = flow
        .then(Concat, (consume(input), consume(input)))
        .unwrap_err();
    assert_eq!(error, FlowBuildError::ReadModeConflict);
}

#[test]
fn a_rejected_binding_registers_nothing_and_leaves_the_builder_usable() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();

    let error = flow.then(Concat, (consume(input), input)).unwrap_err();
    assert_eq!(error, FlowBuildError::ReadModeConflict);

    // 这里能证明“消费没有被登记”（否则下面的复用会报 SourceAlreadyConsumed）；
    // 但“复用读取计数没有被多登记”在外部不可观察（多一次计数只会让读取走克隆路径、结果不变），
    // 由 flow.rs 内部测试 a_conflicting_entry_registers_nothing 直接断言登记状态。
    let length = flow.then(TextLen, input).unwrap();
    let flow = flow.output(length).unwrap();
    assert_eq!(
        block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap(),
        4
    );
}

// ---- A04：跨 Flow Ref ----

#[test]
fn foreign_ref_hidden_in_a_projection_is_rejected() {
    struct Wrapper {
        field: String,
    }

    let foreign_struct_flow = FlowBuilder::<Wrapper>::new();
    let foreign_struct = foreign_struct_flow.input();

    let mut flow = FlowBuilder::<String>::new();
    let _local = flow.input();

    // 同 slot（0）、同类型（String）的跨 Flow Ref，藏在字段投影里。
    let error = flow
        .then(TextLen, field!(foreign_struct.field))
        .unwrap_err();
    assert_eq!(error, FlowBuildError::ForeignRef);
}

#[test]
fn foreign_ref_consumed_from_another_flow_is_rejected() {
    let foreign_flow = FlowBuilder::<String>::new();
    let foreign = foreign_flow.input();

    let mut flow = FlowBuilder::<String>::new();
    let _local = flow.input();

    let error = flow.then(TextLen, consume(foreign)).unwrap_err();
    assert_eq!(error, FlowBuildError::ForeignRef);
}

#[test]
fn foreign_ref_in_a_tuple_is_rejected_even_when_it_comes_second() {
    let foreign_flow = FlowBuilder::<u32>::new();
    let foreign = foreign_flow.input();

    let mut flow = FlowBuilder::<u32>::new();
    let local = flow.input();

    let error = flow.then(Sum2U32, (local, foreign)).unwrap_err();
    assert_eq!(error, FlowBuildError::ForeignRef);

    // 失败后 local 未被登记，仍可继续使用。
    let doubled = flow.then(DoubleU32, local).unwrap();
    let flow = flow.output(doubled).unwrap();
    assert_eq!(block_on(Runtime::new().execute(&flow, 7)).unwrap(), 14);
}

#[test]
fn foreign_ref_in_a_named_assembly_is_rejected() {
    let foreign_flow = FlowBuilder::<u32>::new();
    let foreign = foreign_flow.input();

    let mut flow = FlowBuilder::<u32>::new();
    let local = flow.input();

    let error = flow
        .then(
            SumTuple2,
            bind!(Tuple2 {
                a: local,
                b: foreign
            }),
        )
        .unwrap_err();
    assert_eq!(error, FlowBuildError::ForeignRef);
}

/// 两个 `u32` 的命名结构求和。
struct SumTuple2;

impl Node for SumTuple2 {
    type Input = Tuple2;
    type Output = u32;

    async fn run(&self, input: Tuple2) -> Result<u32, ExecutionError> {
        Ok(input.a + input.b)
    }
}

// ---- A02：投影、tuple、命名与嵌套装配 ----

struct Story {
    plan: String,
    left: String,
    right: String,
}

#[test]
fn four_element_tuple_binding_forms_the_input() {
    let mut flow = FlowBuilder::<Story>::new();
    let story = flow.input();
    let left = flow.then(TextLen, field!(story.left)).unwrap();
    let right = flow.then(TextLen, field!(story.right)).unwrap();
    let left_plus = flow.then(PlusOne, left).unwrap();
    let right_plus = flow.then(PlusOne, right).unwrap();

    let total = flow
        .then(Sum4, (left, right, left_plus, right_plus))
        .unwrap();
    let flow = flow.output(total).unwrap();

    let story = Story {
        plan: String::new(),
        left: String::from("ab"),
        right: String::from("abc"),
    };
    // 2 + 3 + 3 + 4 = 12。
    assert_eq!(block_on(Runtime::new().execute(&flow, story)).unwrap(), 12);
}

struct Sources {
    plan: String,
    left: usize,
    right: usize,
    plus: usize,
}

struct SumSources;

impl Node for SumSources {
    type Input = Sources;
    type Output = usize;

    async fn run(&self, input: Sources) -> Result<usize, ExecutionError> {
        Ok(input.plan.len() + input.left + input.right + input.plus)
    }
}

#[test]
fn four_source_named_assembly_forms_the_input() {
    let mut flow = FlowBuilder::<Story>::new();
    let story = flow.input();
    let left = flow.then(TextLen, field!(story.left)).unwrap();
    let right = flow.then(TextLen, field!(story.right)).unwrap();
    let plus = flow.then(PlusOne, left).unwrap();

    let input = bind!(Sources {
        plan: field!(story.plan),
        left: left,
        right: right,
        plus: plus,
    });
    let total = flow.then(SumSources, input).unwrap();
    let flow = flow.output(total).unwrap();

    let story = Story {
        plan: String::from("plan"),
        left: String::from("ab"),
        right: String::from("abc"),
    };
    // plan=4 + left=2 + right=3 + plus=3 = 12。
    assert_eq!(block_on(Runtime::new().execute(&flow, story)).unwrap(), 12);
}

struct Inner {
    plan_len: String,
    left_len: usize,
    right_len: usize,
}

struct Nested {
    inner: Inner,
    tag: String,
}

struct SumInner;

impl Node for SumInner {
    type Input = Nested;
    type Output = usize;

    async fn run(&self, input: Nested) -> Result<usize, ExecutionError> {
        Ok(input.inner.plan_len.len()
            + input.inner.left_len
            + input.inner.right_len
            + input.tag.len())
    }
}

#[test]
fn named_and_nested_assembly_forms_a_real_node_input() {
    let mut flow = FlowBuilder::<Story>::new();
    let story = flow.input();
    let left_len = flow.then(TextLen, field!(story.left)).unwrap();
    let right_len = flow.then(TextLen, field!(story.right)).unwrap();

    let nested = bind!(Nested {
        inner: bind!(Inner {
            plan_len: field!(story.plan),
            left_len: left_len,
            right_len: right_len,
        }),
        tag: field!(story.left),
    });
    let total = flow.then(SumInner, nested).unwrap();
    let flow = flow.output(total).unwrap();

    let story = Story {
        plan: String::from("plan"),
        left: String::from("ab"),
        right: String::from("abc"),
    };
    // plan=4 + left=2 + right=3 + tag("ab").len()=2 = 11。
    assert_eq!(block_on(Runtime::new().execute(&flow, story)).unwrap(), 11);
}

// ---- A05：Binding 不产生额外 child ----

#[test]
fn binding_adds_no_extra_child_to_the_execution() {
    let log = Arc::new(Log::default());

    let mut flow = FlowBuilder::<Story>::new();
    let story = flow.input();
    let first = flow
        .then(
            RecordText {
                log: shared(&log),
                code: 1,
            },
            field!(story.left),
        )
        .unwrap();
    let second = flow
        .then(
            PairRecorded {
                log: shared(&log),
                code: 2,
            },
            bind!(Pair {
                left: first,
                right: field!(story.right),
            }),
        )
        .unwrap();
    let flow = flow.output(second).unwrap();

    let story = Story {
        plan: String::new(),
        left: String::from("ab"),
        right: String::from("cd"),
    };
    assert_eq!(
        block_on(Runtime::new().execute(&flow, story)).unwrap(),
        "ab+cd"
    );
    // 只有两个声明的 child 被执行；Binding 不产生额外 child。
    assert_eq!(log.seen(), vec![1, 2]);
}

// ---- A06：非 `Clone` 根与复制成本 ----

#[derive(Default)]
struct CloneCounter {
    clones: AtomicUsize,
}

struct Big {
    text: String,
    counter: Arc<CloneCounter>,
}

impl Clone for Big {
    fn clone(&self) -> Self {
        self.counter.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            text: self.text.clone(),
            counter: Arc::clone(&self.counter),
        }
    }
}

/// 没有实现 `Clone` 的根结构。
struct Container {
    small: u32,
    big: Big,
}

struct ReadSmall;

impl Node for ReadSmall {
    type Input = u32;
    type Output = u32;

    async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
        Ok(input * 2)
    }
}

struct ReadBig;

impl Node for ReadBig {
    type Input = Big;
    type Output = u32;

    async fn run(&self, input: Big) -> Result<u32, ExecutionError> {
        Ok(input.text.len() as u32)
    }
}

#[test]
fn projecting_a_small_field_does_not_clone_a_large_sibling() {
    let counter = Arc::new(CloneCounter::default());

    let mut flow = FlowBuilder::<Container>::new();
    let root = flow.input();
    let small = flow.then(ReadSmall, field!(root.small)).unwrap();
    let flow = flow.output(small).unwrap();

    let root = Container {
        small: 21,
        big: Big {
            text: "x".repeat(1024),
            counter: Arc::clone(&counter),
        },
    };
    assert_eq!(block_on(Runtime::new().execute(&flow, root)).unwrap(), 42);
    assert_eq!(
        counter.clones.load(Ordering::SeqCst),
        0,
        "投影小字段不得复制大兄弟字段"
    );
}

#[test]
fn projecting_a_field_copies_only_that_field() {
    let counter = Arc::new(CloneCounter::default());

    let mut flow = FlowBuilder::<Container>::new();
    let root = flow.input();
    let big = flow.then(ReadBig, field!(root.big)).unwrap();
    let flow = flow.output(big).unwrap();

    let root = Container {
        small: 1,
        big: Big {
            text: String::from("abcd"),
            counter: Arc::clone(&counter),
        },
    };
    assert_eq!(block_on(Runtime::new().execute(&flow, root)).unwrap(), 4);
    assert_eq!(
        counter.clones.load(Ordering::SeqCst),
        1,
        "只复制目标字段一次"
    );
}

/// 每次 `Clone` 都记账的根结构。
struct Counted {
    text: String,
    counter: Arc<CloneCounter>,
}

impl Clone for Counted {
    fn clone(&self) -> Self {
        self.counter.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            text: self.text.clone(),
            counter: Arc::clone(&self.counter),
        }
    }
}

struct CountedLen;

impl Node for CountedLen {
    type Input = Counted;
    type Output = usize;

    async fn run(&self, input: Counted) -> Result<usize, ExecutionError> {
        Ok(input.text.len())
    }
}

#[test]
fn a_share_read_before_a_projection_still_clones_the_whole_value() {
    let counter = Arc::new(CloneCounter::default());

    let mut flow = FlowBuilder::<Counted>::new();
    let input = flow.input();
    // 读取顺序 [复用, 投影]：复用不是该位置的最后一次读取，即使整值读取只有一次也要克隆。
    let whole = flow.then(CountedLen, input).unwrap();
    let field = flow.then(TextLen, field!(input.text)).unwrap();
    let _ = whole;
    let flow = flow.output(field).unwrap();

    let counted = Counted {
        text: String::from("abcd"),
        counter: Arc::clone(&counter),
    };
    assert_eq!(block_on(Runtime::new().execute(&flow, counted)).unwrap(), 4);
    assert_eq!(
        counter.clones.load(Ordering::SeqCst),
        1,
        "复用读取后面还有投影，必须克隆整值"
    );
}

#[test]
fn a_projection_before_the_final_share_read_copies_nothing() {
    let counter = Arc::new(CloneCounter::default());

    let mut flow = FlowBuilder::<Counted>::new();
    let input = flow.input();
    // 读取顺序 [投影, 复用]：复用是最后一次读取 → 直接移动，整值 0 复制。
    let field = flow.then(TextLen, field!(input.text)).unwrap();
    let _ = field;
    let whole = flow.then(CountedLen, input).unwrap();
    let flow = flow.output(whole).unwrap();

    let counted = Counted {
        text: String::from("abcd"),
        counter: Arc::clone(&counter),
    };
    assert_eq!(block_on(Runtime::new().execute(&flow, counted)).unwrap(), 4);
    assert_eq!(
        counter.clones.load(Ordering::SeqCst),
        0,
        "最后一次是复用，直接移动"
    );
}

#[test]
fn a_projection_does_not_consume_the_root_that_is_still_output() {
    let counter = Arc::new(CloneCounter::default());

    let mut flow = FlowBuilder::<Container>::new();
    let root = flow.input();
    let small = flow.then(ReadSmall, field!(root.small)).unwrap();
    let _ = small;
    // 根仍可被 output 消费：投影不消费它。
    let flow = flow.output(root).unwrap();

    let root = Container {
        small: 21,
        big: Big {
            text: "x".repeat(1024),
            counter: Arc::clone(&counter),
        },
    };
    let output = block_on(Runtime::new().execute(&flow, root)).unwrap();
    assert_eq!(output.small, 21);
    assert_eq!(counter.clones.load(Ordering::SeqCst), 0);
}

// ---- A09：`Send + !Sync` 业务值 ----

/// 同时是 `Send + !Sync` 且没有实现 `Clone` 的根结构，也被直接交给 Node。
struct NotSyncRoot {
    hits: Cell<u32>,
    text: String,
}

struct Bump;

impl Node for Bump {
    type Input = NotSyncRoot;
    type Output = u32;

    async fn run(&self, input: NotSyncRoot) -> Result<u32, ExecutionError> {
        input.hits.set(input.hits.get() + 1);
        Ok(input.hits.get())
    }
}

#[test]
fn send_but_not_sync_values_flow_through_projection_and_consume() {
    let mut flow = FlowBuilder::<NotSyncRoot>::new();
    let root = flow.input();
    let length = flow.then(TextLen, field!(root.text)).unwrap();
    let _ = length;
    let bumped = flow.then(Bump, consume(root)).unwrap();
    let flow = flow.output(bumped).unwrap();

    let root = NotSyncRoot {
        hits: Cell::new(10),
        text: String::from("abcd"),
    };
    assert_eq!(block_on(Runtime::new().execute(&flow, root)).unwrap(), 11);
}

// ---- A07 / A08：fail-fast ----

#[test]
fn a_failing_child_stops_the_flow_even_with_assembled_input() {
    let log = Arc::new(Log::default());

    let mut flow = FlowBuilder::<Story>::new();
    let story = flow.input();
    let first = flow
        .then(
            RecordText {
                log: shared(&log),
                code: 1,
            },
            field!(story.left),
        )
        .unwrap();
    let failing = flow
        .then(
            Failing {
                log: shared(&log),
                code: 2,
            },
            bind!(Pair {
                left: first,
                right: field!(story.right),
            }),
        )
        .unwrap();
    let after = flow
        .then(
            PairRecorded {
                log: shared(&log),
                code: 3,
            },
            consume(failing),
        )
        .unwrap();
    let flow = flow.output(after).unwrap();

    let story = Story {
        plan: String::new(),
        left: String::from("ab"),
        right: String::from("cd"),
    };
    assert!(block_on(Runtime::new().execute(&flow, story)).is_err());
    assert_eq!(log.seen(), vec![1, 2], "失败后后续 child 不得执行");
}
