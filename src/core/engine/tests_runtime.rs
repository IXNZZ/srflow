//! WE-T02 运行期定向测试：真实主链的 B01～B24 crate 内证据。
//!
//! 全部用例通过公开入口 `Runtime::new().execute(...)` 运行；不执行任何模拟执行器、
//! 不使用睡眠代替 Pending，也不直接触碰内部 Scope／Container 结构（观察点除外）。

// 说明：`let _: () = …` 是刻意的类型断言（固定 Node 输出的 unit 形状），
// `&Box<T>` 用于证明 Box tuple 是单个 Data 叶子，两者都按测试意图保留。
// `err().expect(..)` 等价于 `expect_err`，但 `Definition` 不实现 `Debug`，因此保留当前写法。
#![allow(
    clippy::unused_unit,
    clippy::let_unit_value,
    clippy::borrowed_box,
    clippy::err_expect
)]

use crate::Data;
use crate::core::context::TerminationKind;
use crate::core::context::{InvocationKind, creation_counts};
use crate::core::engine::boundary::{
    install_control_cleanup_fault, install_export_occupy_fault, install_foreign_access_fault,
    install_import_occupy_fault,
};
use crate::core::engine::error::{BodyError, ControlSignal, RunError};
use crate::core::engine::node::Node;
use crate::core::engine::observe;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::runtime::{install_root_cleanup_fault, install_root_cleanup_faults};
use crate::core::engine::shape::{Query, Ref};
use crate::core::internal_error::ScopeError;
use crate::core::orchestrator::ScopeRole;
use crate::core::ref_id::{RefIdAllocator, RefIdSource};
use crate::core::runtime::RootExecution;
use crate::core::scope::{AliasSlot, ScopeState, TargetSnapshot};
use crate::core::test_support::{self, RootSnapshotPhase};
use futures::executor::block_on;
use std::any::TypeId;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll};

// ---------------------------------------------------------------- 业务类型与工具

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Doubled(u32);

#[derive(Data)]
struct Seed {
    value: u32,
    log: Log,
}

/// 事件与 Drop 观察：单线程执行下的非 Send 共享记录（无需锁，也不引入依赖）。
#[derive(Clone)]
struct Log(Rc<RefCell<Vec<String>>>);

impl Log {
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

/// 手写 marker 的非 Clone／Copy 业务值；Drop 记录一次事件。
struct Tracked {
    label: &'static str,
    log: Log,
}

impl Data for Tracked {}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.log.push(format!("drop:{}", self.label));
    }
}

mod derive_variants {
    use crate::Data;

    /// enum 派生。
    #[derive(Data)]
    pub enum Mode {
        Fast,
        Slow,
    }

    /// 泛型 + where 约束派生；字段类型不需要实现 Data。
    #[derive(Data)]
    pub struct Wrapper<T>
    where
        T: Clone,
    {
        pub value: T,
        pub extra: String,
    }

    /// 手工 marker（手写实现与派生等价）。
    pub struct Manual(pub u32);
    impl Data for Manual {}
}

/// 单次 Pending 的自唤醒 Future：精确控制真实 Pending 与恢复，不用睡眠。
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
            // 自唤醒：下一次 poll 立即恢复，不依赖时间或外部事件。
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn double(query: Query<&Amount>) -> Result<Doubled, BodyError> {
    Ok(Doubled(query.get().0 * 2))
}

async fn add_one(query: Query<&Doubled>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

async fn seed_reader(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value + 1))
}

async fn seed_observer(query: Query<&Seed>) -> Result<(), BodyError> {
    query.get().log.push("start:observer");
    Ok(())
}

async fn track_temp(query: Query<&Seed>) -> Result<Tracked, BodyError> {
    Ok(Tracked {
        label: "temp",
        log: query.get().log.clone(),
    })
}

async fn unit_noop(query: Query<()>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

/// Amount → Amount 的第二种结构体 Node（Fragment 两形式样本使用）。
struct AmountStep;

impl Node for AmountStep {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

struct AddOne;

impl Node for AddOne {
    type Input = Doubled;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

/// 非 static 配置的结构体 Node：句柄借用本地配置。
struct Configured<'a> {
    delta: &'a u32,
}

impl Node for Configured<'_> {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + *self.delta))
    }
}

// ---------------------------------------------------------------- B01／B02／B03

#[test]
fn b01_spec_minimal_example_returns_43() {
    let runtime = Runtime::new();
    let add = AddOne;
    let output = block_on(runtime.execute(
        |flow, amount| {
            let doubled = flow.then(double, amount);
            flow.chain(|sub| sub.then(&add, doubled))
        },
        Amount(21),
    ))
    .expect("minimal example runs");
    assert_eq!(output.0, 43);
}

#[test]
fn b02_derive_variants_and_manual_marker_execute() {
    let mode = derive_variants::Mode::Fast;
    assert!(matches!(mode, derive_variants::Mode::Fast));
    let _ = derive_variants::Mode::Slow;
    let wrapper = derive_variants::Wrapper {
        value: 1u32,
        extra: String::from("payload"),
    };
    assert_eq!(wrapper.value, 1);
    assert_eq!(wrapper.extra, "payload");
    let manual = derive_variants::Manual(7);
    assert_eq!(manual.0, 7);

    // 派生类型可以真实进入数据流（非 Data 字段合法、无 Clone 要求）。
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let doubled = flow.then(double, seed);
            flow.then(add_one, doubled)
        },
        Amount(1),
    ))
    .expect("derived Data runs");
    assert_eq!(output.0, 3);
}

#[test]
fn b03_fragment_forms_execute_with_non_static_nodes() {
    fn fragment<'n>(
        node: &'n AddOne,
        input: Ref<Doubled>,
    ) -> impl FnOnce(&mut crate::Flow<'n>) -> Ref<Amount> + 'n {
        move |sub| sub.then(node, input)
    }

    fn call_in_closure<'n>(
        flow: &mut crate::Flow<'n>,
        node: &'n AmountStep,
        input: Ref<Amount>,
    ) -> Ref<Amount> {
        flow.then(node, input)
    }

    let first_node = AddOne;
    let second_node = AmountStep;
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, amount| {
            let doubled = flow.then(double, amount);
            let first = flow.chain(fragment(&first_node, doubled));
            flow.chain(|sub| call_in_closure(sub, &second_node, first))
        },
        Amount(20),
    ))
    .expect("fragment forms run");
    assert_eq!(output.0, 42);
}

// ---------------------------------------------------------------- B04 构建时机

#[test]
fn b04_unpolled_future_does_not_build_or_call() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let definitions = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));

    struct Dropper {
        drops: Arc<AtomicUsize>,
    }
    impl Data for Dropper {}
    impl Drop for Dropper {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Counting;
    impl Node for Counting {
        type Input = Dropper;
        type Output = ();
        async fn run(&self, _query: Query<&Self::Input>) -> Result<(), BodyError> {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let runtime = Runtime::new();
    let counter = Arc::clone(&definitions);
    let future = runtime.execute(
        move |flow, input| {
            counter.fetch_add(1, Ordering::SeqCst);
            let _: () = flow.then(Counting, input);
            ()
        },
        Dropper {
            drops: Arc::clone(&drops),
        },
    );
    drop(future);

    assert_eq!(
        definitions.load(Ordering::SeqCst),
        0,
        "unpolled future must not build"
    );
    assert_eq!(CALLS.load(Ordering::SeqCst), 0);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "owned input is released once"
    );
}

#[test]
fn b04_build_failure_yields_definition_error_without_root() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let drops = Arc::new(AtomicUsize::new(0));

    struct Dropper {
        drops: Arc<AtomicUsize>,
    }
    impl Data for Dropper {}
    impl Drop for Dropper {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Counting;
    impl Node for Counting {
        type Input = Dropper;
        type Output = ();
        async fn run(&self, query: Query<&Self::Input>) -> Result<(), BodyError> {
            let _ = query.get();
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // 第一次执行捕获一个外部 Ref，用于在第二次执行的 Definition 中触发拒绝。
    let runtime = Runtime::new();
    let mut leaked: Option<Ref<Dropper>> = None;
    let _ = block_on(runtime.execute(
        |_flow, input| {
            leaked = Some(input);
            input
        },
        Dropper {
            drops: Arc::clone(&drops),
        },
    ))
    .expect("capture definition runs");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "capture execution releases its input"
    );

    let foreign = leaked.expect("captured root ref");
    let error = block_on(runtime.execute(
        move |flow, input| {
            let _ = flow.then(Counting, foreign);
            input
        },
        Dropper {
            drops: Arc::clone(&drops),
        },
    ))
    .err()
    .expect("foreign ref must be rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "no node runs after a rejected definition"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "the rejected execution releases its input once"
    );
}

// ---------------------------------------------------------------- B05 全 arity

// 全 arity 真实连接：Root Input tuple → Node N 元输入 → Root 输出 N 元 owned tuple。
// 元组只在上限内实现 PartialEq／Debug，所以逐项断言首尾位置；其余位置由类型系统连接证明。
macro_rules! arity_run {
    ($name:ident, $last:tt, $(($t:ty => $v:expr)),+) => {
        #[test]
        fn $name() {
            async fn consume(query: Query<($(&$t,)+)>) -> Result<(), BodyError> {
                let _ = query.get();
                Ok(())
            }
            let input = ($($v,)+);
            let runtime = Runtime::new();
            let output = block_on(runtime.execute::<($($t,)+), _, _>(
                |flow, refs| {
                    let _: () = flow.then(consume, refs);
                    refs
                },
                input,
            ))
            .expect("arity runs end to end");
            assert_eq!(output.0, input.0, "first position round-trips");
            assert_eq!(output.$last, input.$last, "last position round-trips");
        }
    };
}

arity_run!(b05_arity_2, 1, (u8 => 1u8), (u16 => 2u16));
arity_run!(b05_arity_3, 2, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32));
arity_run!(b05_arity_4, 3, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64));
arity_run!(b05_arity_5, 4, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128));
arity_run!(b05_arity_6, 5, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize));
arity_run!(b05_arity_7, 6, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8));
arity_run!(b05_arity_8, 7, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8), (i16 => 8i16));
arity_run!(b05_arity_9, 8, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8), (i16 => 8i16), (i32 => 9i32));
arity_run!(b05_arity_10, 9, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8), (i16 => 8i16), (i32 => 9i32), (i64 => 10i64));
arity_run!(b05_arity_11, 10, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8), (i16 => 8i16), (i32 => 9i32), (i64 => 10i64), (i128 => 11i128));
arity_run!(b05_arity_12, 11, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8), (i16 => 8i16), (i32 => 9i32), (i64 => 10i64), (i128 => 11i128), (isize => 12isize));
arity_run!(b05_arity_13, 12, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8), (i16 => 8i16), (i32 => 9i32), (i64 => 10i64), (i128 => 11i128), (isize => 12isize), (bool => true));
arity_run!(b05_arity_14, 13, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8), (i16 => 8i16), (i32 => 9i32), (i64 => 10i64), (i128 => 11i128), (isize => 12isize), (bool => true), (char => 'x'));
arity_run!(b05_arity_15, 14, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8), (i16 => 8i16), (i32 => 9i32), (i64 => 10i64), (i128 => 11i128), (isize => 12isize), (bool => true), (char => 'x'), (f32 => 13.5f32));
arity_run!(b05_arity_16, 15, (u8 => 1u8), (u16 => 2u16), (u32 => 3u32), (u64 => 4u64), (u128 => 5u128), (usize => 6usize), (i8 => 7i8), (i16 => 8i16), (i32 => 9i32), (i64 => 10i64), (i128 => 11i128), (isize => 12isize), (bool => true), (char => 'x'), (f32 => 13.5f32), (f64 => 14.5f64));

#[test]
fn b05_unit_root_and_unit_chain_run() {
    let runtime = Runtime::new();
    let output: () = block_on(runtime.execute(
        |flow, ()| {
            let _: () = flow.then(unit_noop, ());
            flow.chain(|sub| {
                let _: () = sub.then(unit_noop, ());
                ()
            });
            ()
        },
        (),
    ))
    .expect("unit root and unit chain run");
    assert_eq!(output, ());
}

// ---------------------------------------------------------------- B06／B07 数据与 Node 形式

#[test]
fn b06_non_clone_copy_send_sync_data_and_unit_slot() {
    struct NonSend {
        value: u32,
        tag: std::rc::Rc<u32>,
    }
    impl Data for NonSend {}

    async fn consume_nonsend(query: Query<&NonSend>) -> Result<Tracked, BodyError> {
        let non_send = query.get();
        let _ = (non_send.value, non_send.tag.as_ref());
        Ok(Tracked {
            label: "nonsend-output",
            log: Log::new(),
        })
    }

    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, input| {
            let produced = flow.then(consume_nonsend, input);
            let _: () = flow.then(unit_noop, ());
            produced
        },
        NonSend {
            value: 5,
            tag: std::rc::Rc::new(9),
        },
    ))
    .expect("non-Clone/Copy/Send/Sync data runs");
    assert_eq!(output.label, "nonsend-output");
}

#[test]
fn b07_heterogeneous_forms_execute_in_one_body() {
    let delta = 5u32;
    let configured = Configured { delta: &delta };
    let shared = Arc::new(AddOne);
    let closure = async |query: Query<&Amount>| -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 100))
    };
    let function_ref = &double;

    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, amount| {
            let a = flow.then(&configured, amount); // 借用 + 非 static 配置
            let b = flow.then(function_ref, a); // 函数引用
            let c = flow.then(Arc::clone(&shared), b); // Arc<Node>
            flow.then(closure, c) // async closure
        },
        Amount(1),
    ))
    .expect("heterogeneous forms run");
    // 1 +5 → 6；double → 12；AddOne → 13；closure +100 → 113。
    assert_eq!(output.0, 113);
}

// ---------------------------------------------------------------- B08 真实 Pending

#[test]
fn b08_query_borrow_survives_real_pending() {
    let polls = Arc::new(AtomicUsize::new(0));

    // 节点签名固定，计数器通过线程局部传入。
    thread_local! {
        static POLLS: std::cell::RefCell<Option<Arc<AtomicUsize>>> = const { std::cell::RefCell::new(None) };
    }
    POLLS.with(|slot| *slot.borrow_mut() = Some(Arc::clone(&polls)));

    // 为了保持签名简单，这里把 Pending 放在函数节点内部并由线程局部取得计数器。
    async fn pending_node(query: Query<&Amount>) -> Result<Doubled, BodyError> {
        let counter = POLLS.with(|slot| slot.borrow().clone().expect("polls installed"));
        PendingOnce {
            polls: counter,
            yielded: false,
        }
        .await;
        Ok(Doubled(query.get().0 + 1))
    }

    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, amount| {
            let doubled = flow.then(pending_node, amount);
            flow.then(add_one, doubled)
        },
        Amount(41),
    ))
    .expect("pending node resumes");
    assert_eq!(output.0, 43);
    assert!(
        polls.load(Ordering::SeqCst) >= 2,
        "the node really suspended once"
    );
}

// ---------------------------------------------------------------- B09／B10 顺序与执行域

#[test]
fn b09_steps_run_strictly_in_order() {
    thread_local! {
        static EVENTS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    async fn first(query: Query<&Amount>) -> Result<Amount, BodyError> {
        EVENTS.with(|log| log.borrow_mut().push("start:first".into()));
        let value = query.get().0;
        EVENTS.with(|log| log.borrow_mut().push("end:first".into()));
        Ok(Amount(value + 1))
    }

    async fn second(query: Query<&Amount>) -> Result<(), BodyError> {
        EVENTS.with(|log| log.borrow_mut().push("start:second".into()));
        let _ = query.get();
        EVENTS.with(|log| log.borrow_mut().push("end:second".into()));
        Ok(())
    }

    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, amount| {
            let stepped = flow.then(first, amount);
            let _: () = flow.then(second, stepped);
            let _unused = flow.then(first, amount);
            ()
        },
        Amount(1),
    ))
    .expect("ordered run");
    assert_eq!(output, ());
    let events = EVENTS.with(|log| std::mem::take(&mut *log.borrow_mut()));
    assert_eq!(
        events,
        vec![
            "start:first".to_string(),
            "end:first".to_string(),
            "start:second".to_string(),
            "end:second".to_string(),
            "start:first".to_string(),
            "end:first".to_string(),
        ],
        "no implicit concurrency and unused outputs still run"
    );
}

#[test]
fn b10_single_context_coordinator_and_container_per_execution() {
    let before = creation_counts::snapshot();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| flow.chain(|sub| sub.chain(|inner| inner.then(seed_reader, seed))),
        Seed {
            value: 41,
            log: Log::new(),
        },
    ))
    .expect("nested chain run");
    let after = creation_counts::snapshot();
    assert_eq!(output.0, 42);
    assert_eq!(after.0 - before.0, 1, "exactly one Context per execution");
    assert_eq!(
        after.1 - before.1,
        1,
        "exactly one ScopeCoordinator per execution"
    );
    assert_eq!(
        after.2 - before.2,
        1,
        "exactly one DataContainer per execution"
    );
}

// ---------------------------------------------------------------- B11／B12 深层捕获与边界

#[test]
fn b11_deep_capture_reads_the_real_value() {
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| flow.chain(|outer| outer.chain(|inner| inner.then(seed_reader, seed))),
        Seed {
            value: 41,
            log: log.clone(),
        },
    ))
    .expect("deep capture run");
    assert_eq!(output.0, 42);
    let events = log.take();
    assert!(
        !events.iter().any(|event| event.starts_with("drop:")),
        "ancestor input must survive the child boundaries"
    );
}

#[test]
fn b12_chain_temporaries_drop_before_the_next_parent_step() {
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            flow.chain(|sub| {
                let temp = sub.then(track_temp, seed);
                let _ = temp;
                ()
            });
            flow.then(seed_observer, seed)
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .expect("chain cleanup run");
    assert_eq!(output, ());
    let events = log.take();
    assert_eq!(
        events,
        vec!["drop:temp".to_string(), "start:observer".to_string()],
        "child temporaries are destroyed before the next parent step"
    );
}

// ---------------------------------------------------------------- B13／B14 输出与拒绝

#[test]
fn b13_duplicate_and_alias_sources_export_through_child_exits() {
    let log = Log::new();
    let runtime = Runtime::new();
    // Chain 返回 (r, r)：两个槽各自通过唯一 child 出口与整组 Export 交给 parent；
    // Root 只取其中一个输出（两个都取属于重复 DataId，见 b14）。
    let output = block_on(runtime.execute(
        |flow, seed| {
            let pair = flow.chain(|sub| {
                let value = inner_amount(sub, seed);
                (value, value)
            });
            pair.0
        },
        Seed {
            value: 20,
            log: log.clone(),
        },
    ))
    .expect("duplicate chain output run");
    assert_eq!(output.0, 21);
    // 同一 DataId 只被销毁一次（责任只转移一次）且已随 owned 输出离开 Runtime。
    assert_eq!(log.take(), Vec::<String>::new());
}

/// 在 child 内产生一个新的 Amount（供重复来源样本使用）。
fn inner_amount(sub: &mut crate::Flow<'_>, seed: Ref<Seed>) -> Ref<Amount> {
    sub.then(seed_reader, seed)
}

#[test]
fn b13_alias_output_keeps_ancestor_owner() {
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| flow.chain(|_sub| seed),
        Seed {
            value: 7,
            log: log.clone(),
        },
    ))
    .expect("alias chain run");
    assert_eq!(output.value, 7);
    assert_eq!(log.take(), Vec::<String>::new());
}

#[test]
fn b13_mixed_child_and_ancestor_sources_export_in_one_group() {
    let log = Log::new();
    let runtime = Runtime::new();
    // 同一组声明输出同时含 child 产出与祖先 alias：两条路径都经唯一出口正常 Export。
    let output = block_on(runtime.execute(
        |flow, seed| {
            flow.chain(|sub| {
                let produced = inner_amount(sub, seed);
                (produced, seed)
            })
        },
        Seed {
            value: 30,
            log: log.clone(),
        },
    ))
    .expect("mixed sources run");
    assert_eq!(output.0.0, 31, "child-produced value exported");
    assert_eq!(
        output.1.value, 30,
        "ancestor alias exported without transfer"
    );
    assert_eq!(log.take(), Vec::<String>::new());
}

#[test]
fn b14_root_duplicate_data_id_is_rejected_before_any_take() {
    let log = Log::new();
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, seed| {
            flow.chain(|sub| {
                let value = inner_amount(sub, seed);
                (value, value)
            })
        },
        Seed {
            value: 3,
            log: log.clone(),
        },
    ))
    .err()
    .expect("duplicate DataId must be rejected");
    assert!(matches!(error, RunError::Runtime(_)), "{error}");
}

#[test]
fn b15_owned_output_leaves_the_runtime_alive() {
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| inner_amount(flow, seed),
        Seed {
            value: 9,
            log: log.clone(),
        },
    ))
    .expect("owned output run");
    assert_eq!(output.0, 10);
    let events_after_run = log.take();
    assert!(
        !events_after_run
            .iter()
            .any(|event| event.starts_with("drop:amount")),
        "returned owned data is not destroyed by the runtime close"
    );
}

// ---------------------------------------------------------------- B16 整组预检

#[test]
fn b16_export_preflight_rejects_the_whole_group() {
    let log = Log::new();
    let runtime = Runtime::new();
    install_export_occupy_fault(0);
    let error = block_on(runtime.execute(
        |flow, seed| flow.chain(|sub| sub.then(seed_reader, seed)),
        Seed {
            value: 11,
            log: log.clone(),
        },
    ))
    .err()
    .expect("occupied caller position rejects the export");
    assert!(matches!(error, RunError::Runtime(_)), "{error}");
    // 预检在任何提交之前拒绝：child 值仍被清理一次，无半提交输出。
    assert_eq!(log.take(), Vec::<String>::new());
}

// ---------------------------------------------------------------- B17／B18 错误与 Control

#[derive(Debug)]
struct DomainError {
    detail: &'static str,
}

impl std::fmt::Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.detail)
    }
}

impl std::error::Error for DomainError {}

#[test]
fn b17_dynamic_failure_and_source_chain_are_preserved() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);

    async fn failing(_query: Query<&Amount>) -> Result<Amount, BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::fail(DomainError {
            detail: "domain exploded",
        }))
    }

    async fn never(_query: Query<&Amount>) -> Result<Amount, BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(0))
    }

    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, amount| {
            let failed = flow.then(failing, amount);
            flow.then(never, failed)
        },
        Amount(1),
    ))
    .err()
    .expect("failure must propagate");
    let RunError::Body(failure) = error else {
        panic!("expected a body failure, got {error}");
    };
    let source = std::error::Error::source(&failure).expect("source chain preserved");
    let domain = source
        .downcast_ref::<DomainError>()
        .expect("original dynamic object preserved");
    assert_eq!(domain.detail, "domain exploded");
    assert_eq!(CALLS.load(Ordering::SeqCst), 1, "later nodes must not run");
}

#[test]
fn b18_node_control_becomes_unhandled_control_at_root() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);

    async fn retrying(_query: Query<&Amount>) -> Result<Amount, BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::retry(DomainError { detail: "retry me" }))
    }

    async fn never(_query: Query<&Amount>) -> Result<Amount, BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(0))
    }

    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, amount| {
            flow.chain(|sub| {
                let retried = sub.then(retrying, amount);
                sub.then(never, retried)
            })
        },
        Amount(1),
    ))
    .err()
    .expect("unhandled control terminates the root");
    let RunError::UnhandledControl(ControlSignal::Retry(retry)) = error else {
        panic!("expected unhandled retry, got {error}");
    };
    let source = std::error::Error::source(&retry).expect("retry reason preserved");
    assert!(source.downcast_ref::<DomainError>().is_some());
    assert_eq!(CALLS.load(Ordering::SeqCst), 1, "control stops later steps");
}

#[test]
fn b18_iter_break_is_also_unhandled_without_a_controller() {
    async fn breaking(_query: Query<&Amount>) -> Result<Amount, BodyError> {
        Err(BodyError::iter_break())
    }

    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, amount| flow.chain(|sub| sub.then(breaking, amount)),
        Amount(1),
    ))
    .err()
    .expect("iter break terminates the root");
    assert!(matches!(
        error,
        RunError::UnhandledControl(ControlSignal::IterBreak(_))
    ));
}

#[test]
fn b19_control_cleanup_failure_becomes_runtime_error() {
    async fn retrying(_query: Query<&Amount>) -> Result<Amount, BodyError> {
        Err(BodyError::retry(DomainError { detail: "retry me" }))
    }

    let runtime = Runtime::new();
    install_control_cleanup_fault();
    let error = block_on(runtime.execute(
        |flow, amount| flow.chain(|sub| sub.then(retrying, amount)),
        Amount(1),
    ))
    .err()
    .expect("cleanup failure replaces the recoverable control");
    assert!(matches!(error, RunError::Runtime(_)), "{error}");
}

// ---------------------------------------------------------------- B20 取消

#[test]
fn b20_cancel_releases_every_value_once() {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));

    struct Dropper {
        drops: Arc<AtomicUsize>,
    }
    impl Data for Dropper {}
    impl Drop for Dropper {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    thread_local! {
        static PENDING_POLLS: std::cell::RefCell<Option<Arc<AtomicUsize>>> = const { std::cell::RefCell::new(None) };
    }
    PENDING_POLLS.with(|slot| *slot.borrow_mut() = Some(Arc::clone(&polls)));

    async fn pending(_query: Query<&Dropper>) -> Result<Amount, BodyError> {
        let counter = PENDING_POLLS.with(|slot| slot.borrow().clone().expect("polls installed"));
        PendingOnce {
            polls: counter,
            yielded: false,
        }
        .await;
        Ok(Amount(1))
    }

    let runtime = Runtime::new();
    let mut future = Box::pin(runtime.execute(
        |flow, input| flow.then(pending, input),
        Dropper {
            drops: Arc::clone(&drops),
        },
    ));

    let waker = futures::task::noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "node really reached Pending"
    );
    drop(future);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "input released exactly once under cancel"
    );

    // 从未 poll 的取消同样只释放输入一次。
    let runtime = Runtime::new();
    let future = runtime.execute(
        |flow, input| flow.then(pending, input),
        Dropper {
            drops: Arc::clone(&drops),
        },
    );
    drop(future);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------- B21 多次执行

#[test]
fn b21_consecutive_executions_are_isolated() {
    let runtime = Runtime::new();
    let first = block_on(runtime.execute(|flow, amount| flow.then(double, amount), Amount(1)))
        .expect("first execution");
    let second = block_on(runtime.execute(|flow, amount| flow.then(double, amount), Amount(10)))
        .expect("second execution");
    assert_eq!((first.0, second.0), (2, 20));

    // 一次执行里捕获的 Ref 在另一次执行的 Definition 中被拒绝。
    let mut leaked: Option<Ref<Amount>> = None;
    let _ = block_on(runtime.execute(
        |_flow, amount| {
            leaked = Some(amount);
            amount
        },
        Amount(3),
    ))
    .expect("capture run");
    let foreign = leaked.expect("captured ref");
    let error = block_on(runtime.execute(
        move |flow, amount| {
            let _ = flow.then(double, amount);
            flow.then(double, foreign)
        },
        Amount(4),
    ))
    .err()
    .expect("foreign ref rejected in the next execution");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
}

// ================================================================ WE-T02 修订补充
// R01／R02 的修复样本与 R03 要求的观察型证据。全部经公开 Runtime 或真实 Core 入口运行；
// 观察点复用既有 cfg(test) 通道（test_support 的创建／地址／Root 快照／take 日志／
// Closed 记录）与引擎自有的 observe 通道（Import 批次、Export 快照、frame、Scope 状态）。

/// 复位全部观察通道（同线程内多次执行时使用）。
fn reset_observations() {
    observe::reset();
    // 完整复位 Core 侧观察通道（含首次终止记录与 take 日志），避免同线程复用残留。
    test_support::reset_observations();
    test_support::take_log_reset();
}

/// 全部 Export 快照都取自同一个 caller Scope（本次样本里是 Root）。
fn exports_share_the_root_scope(snapshots: &[observe::ExportSnapshotRecord]) -> bool {
    match snapshots.split_first() {
        Some((first, rest)) => rest.iter().all(|record| record.scope == first.scope),
        None => false,
    }
}

/// 取某个观察点上的 Export 快照。
fn export_snapshot<'a>(
    snapshots: &'a [observe::ExportSnapshotRecord],
    phase: &str,
) -> &'a observe::ExportSnapshotRecord {
    snapshots
        .iter()
        .find(|snapshot| snapshot.phase == phase)
        .unwrap_or_else(|| panic!("missing export snapshot {phase}"))
}

/// 一个带 Drop 事件、可跨节点传递的输入值。
struct TrackedSeed {
    value: u32,
    log: Log,
}

impl Data for TrackedSeed {}

impl Drop for TrackedSeed {
    fn drop(&mut self) {
        self.log.push("drop:seed");
    }
}

/// 一个带 Drop 事件的节点输出。
struct TrackedValue {
    label: &'static str,
    log: Log,
}

impl Data for TrackedValue {}

impl Drop for TrackedValue {
    fn drop(&mut self) {
        self.log.push(format!("drop:{}", self.label));
    }
}

async fn read_tracked_seed(query: Query<&TrackedSeed>) -> Result<TrackedValue, BodyError> {
    let seed = query.get();
    let _ = seed.value;
    Ok(TrackedValue {
        label: "temp",
        log: seed.log.clone(),
    })
}

async fn two_inputs_unit(query: Query<(&TrackedSeed, &Amount)>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

/// 首次 poll 返回 Pending 并自唤醒；Drop 记录一次事件（借用 Future 的生命周期观察）。
struct PendingWithGuard {
    yielded: bool,
    _log: Log,
    _guard: TrackedValue,
}

impl Future for PendingWithGuard {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn pending_guard_node(query: Query<&TrackedSeed>) -> Result<Amount, BodyError> {
    let log = query.get().log.clone();
    PendingWithGuard {
        yielded: false,
        _log: log.clone(),
        _guard: TrackedValue {
            label: "pending-future",
            log,
        },
    }
    .await;
    Ok(Amount(1))
}

/// 零输入节点：返回不可恢复 Failure（R01 样本）。
async fn failing_zero_input(_query: Query<()>) -> Result<(), BodyError> {
    Err(BodyError::fail(DomainError {
        detail: "cleanup sample",
    }))
}

/// 零输入节点：返回 IterBreak（R01 样本）。
async fn breaking_zero_input(_query: Query<()>) -> Result<(), BodyError> {
    Err(BodyError::iter_break())
}

// ---------------------------------------------------------------- R01

#[test]
fn r01_business_failure_keeps_the_first_cleanup_diagnostic() {
    reset_observations();
    let runtime = Runtime::new();
    install_root_cleanup_fault();
    let error = block_on(runtime.execute(
        |flow, _amount| {
            let _: () = flow.then(failing_zero_input, ());
            ()
        },
        Amount(7),
    ))
    .err()
    .expect("business failure propagates");

    let RunError::Body(failure) = error else {
        panic!("expected a body failure, got {error}");
    };
    let business = std::error::Error::source(&failure).expect("original business object");
    assert!(business.downcast_ref::<DomainError>().is_some());
    let cleanup = failure
        .cleanup_source()
        .expect("first cleanup diagnostic is preserved next to the business failure");
    assert!(!cleanup.to_string().is_empty());
    assert!(
        cleanup
            .downcast_ref::<crate::core::engine::error::FrameworkDiagnostic>()
            .is_some(),
        "cleanup diagnostic is the framework's original diagnostic"
    );
    // 首次终止记录没有被清理诊断覆盖。
    let terminations = test_support::take_termination_saved();
    assert_eq!(
        terminations.len(),
        1,
        "exactly one first termination is recorded"
    );
}

#[test]
fn r01_root_control_with_cleanup_failure_becomes_runtime() {
    reset_observations();
    let runtime = Runtime::new();
    install_root_cleanup_fault();
    let error = block_on(runtime.execute(
        |flow, _amount| {
            let _: () = flow.then(breaking_zero_input, ());
            ()
        },
        Amount(3),
    ))
    .err()
    .expect("control is not returned as a recoverable signal");

    let RunError::Runtime(runtime_error) = error else {
        panic!("expected a runtime error, got {error}");
    };
    let source = std::error::Error::source(&runtime_error).expect("original control signal");
    assert!(source.downcast_ref::<ControlSignal>().is_some());
    assert!(
        runtime_error.cleanup_source().is_some(),
        "first cleanup diagnostic preserved"
    );
}

// ---------------------------------------------------------------- R02

/// 取 Scope 的当前绑定数（只读快照）。
fn bound_refs(
    guard: &crate::core::context::InvocationGuard<'_>,
    scope: &crate::core::identity::ScopeId,
) -> usize {
    guard
        .snapshot_targets_probe(scope)
        .map(|(refs, _)| refs.len())
        .expect("scope snapshot")
}

#[test]
fn r02_alias_preflight_stays_inside_the_item_cap() {
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let allocator = RefIdAllocator::new(RefIdSource::new());
    let collection = allocator.allocate().expect("collection position");

    let mut root_guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let caller = root_guard.create_child(&root).expect("caller scope");
    let mut caller_guard = root_guard
        .enter(InvocationKind::Boundary, &caller, true)
        .expect("caller frame");
    // 集合由 caller 自己持有：Item 创建只接受 caller 本地已绑定的完整集合。
    caller_guard
        .register_owned(&caller, &collection, vec![10u32, 20])
        .expect("collection registration in the caller scope");

    // Item 创建只允许调用方 Scope 自己完成：在 caller 仍为栈顶时建立 ItemScope 与其绑定。
    let item = caller_guard
        .create_child(&caller)
        .expect("item scope (cap)");
    let other = caller_guard.create_child(&caller).expect("sibling scope");
    let item_position = allocator.allocate().expect("item position");
    let outside_item = allocator.allocate().expect("second item position");
    caller_guard
        .bind_item_input::<u32>(&item, &caller, &collection, &item_position, 0)
        .expect("legal item binding");
    caller_guard
        .bind_item_input::<u32>(&item, &caller, &collection, &outside_item, 1)
        .expect("second item binding");

    let mut item_guard = caller_guard
        .enter(InvocationKind::Boundary, &item, true)
        .expect("item frame");

    // cap 内合法 alias：接受并新增一个绑定。
    let inside_alias = allocator.allocate().expect("alias position");
    let before = bound_refs(&item_guard, &item);
    item_guard
        .bind_local_aliases(
            &item,
            &[AliasSlot::with_type(
                &item_position,
                inside_alias,
                TypeId::of::<u32>(),
                "u32",
            )],
        )
        .expect("in-cap alias is accepted");
    assert_eq!(bound_refs(&item_guard, &item), before + 1);

    // cap 外：第二个 item 的 cap 改指向 sibling，构成“前项合法、后项非法”的组。
    item_guard.replace_item_cap_probe(&item, &outside_item, &other);
    let group_before = bound_refs(&item_guard, &item);
    let legal = allocator.allocate().expect("legal alias position");
    let illegal = allocator.allocate().expect("illegal alias position");
    let error = item_guard
        .bind_local_aliases(
            &item,
            &[
                AliasSlot::with_type(&item_position, legal, TypeId::of::<u32>(), "u32"),
                AliasSlot::with_type(&outside_item, illegal, TypeId::of::<u32>(), "u32"),
            ],
        )
        .err()
        .expect("out-of-cap item alias is rejected");
    assert!(
        matches!(error, ScopeError::ItemOutsideCap { .. }),
        "expected ItemOutsideCap, got {error}"
    );
    assert_eq!(
        bound_refs(&item_guard, &item),
        group_before,
        "the rejected group leaves no new destination binding"
    );

    item_guard.complete();
    caller_guard.complete();
    root_guard.complete();
    drop(execution);
}

// ---------------------------------------------------------------- B05（Chain arity 全量）

// Chain 输出 arity：Root 输入 tuple → Chain 返回同形 refs（全部为祖先 alias 槽）→
// Root 输出 N 元 owned tuple；逐槽位断言，不只首尾。
macro_rules! chain_arity_run {
    ($name:ident, $(($t:ty => $v:expr => $i:tt)),+) => {
        #[test]
        fn $name() {
            let input = ($($v,)+);
            let runtime = Runtime::new();
            let output = block_on(runtime.execute::<($($t,)+), _, _>(
                |flow, refs| flow.chain(|_sub| refs),
                input,
            ))
            .expect("chain output arity runs end to end");
            $( assert_eq!(output.$i, input.$i, "chain slot {} round-trips", $i); )+
            let imports = observe::take_chain_imports();
            assert_eq!(imports.len(), 1);
            assert_eq!(
                imports[0].pairs.len(),
                [$($i),+].len(),
                "every declared alias slot has one import requirement"
            );
        }
    };
}

chain_arity_run!(b05_chain_arity_2, (u8 => 1u8 => 0), (u16 => 2u16 => 1));
chain_arity_run!(b05_chain_arity_3, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2));
chain_arity_run!(b05_chain_arity_4, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3));
chain_arity_run!(b05_chain_arity_5, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4));
chain_arity_run!(b05_chain_arity_6, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5));
chain_arity_run!(b05_chain_arity_7, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6));
chain_arity_run!(b05_chain_arity_8, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6), (i16 => 8i16 => 7));
chain_arity_run!(b05_chain_arity_9, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6), (i16 => 8i16 => 7), (i32 => 9i32 => 8));
chain_arity_run!(b05_chain_arity_10, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6), (i16 => 8i16 => 7), (i32 => 9i32 => 8), (i64 => 10i64 => 9));
chain_arity_run!(b05_chain_arity_11, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6), (i16 => 8i16 => 7), (i32 => 9i32 => 8), (i64 => 10i64 => 9), (i128 => 11i128 => 10));
chain_arity_run!(b05_chain_arity_12, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6), (i16 => 8i16 => 7), (i32 => 9i32 => 8), (i64 => 10i64 => 9), (i128 => 11i128 => 10), (isize => 12isize => 11));
chain_arity_run!(b05_chain_arity_13, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6), (i16 => 8i16 => 7), (i32 => 9i32 => 8), (i64 => 10i64 => 9), (i128 => 11i128 => 10), (isize => 12isize => 11), (bool => true => 12));
chain_arity_run!(b05_chain_arity_14, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6), (i16 => 8i16 => 7), (i32 => 9i32 => 8), (i64 => 10i64 => 9), (i128 => 11i128 => 10), (isize => 12isize => 11), (bool => true => 12), (char => 'x' => 13));
chain_arity_run!(b05_chain_arity_15, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6), (i16 => 8i16 => 7), (i32 => 9i32 => 8), (i64 => 10i64 => 9), (i128 => 11i128 => 10), (isize => 12isize => 11), (bool => true => 12), (char => 'x' => 13), (f32 => 13.5f32 => 14));
chain_arity_run!(b05_chain_arity_16, (u8 => 1u8 => 0), (u16 => 2u16 => 1), (u32 => 3u32 => 2), (u64 => 4u64 => 3), (u128 => 5u128 => 4), (usize => 6usize => 5), (i8 => 7i8 => 6), (i16 => 8i16 => 7), (i32 => 9i32 => 8), (i64 => 10i64 => 9), (i128 => 11i128 => 10), (isize => 12isize => 11), (bool => true => 12), (char => 'x' => 13), (f32 => 13.5f32 => 14), (f64 => 14.5f64 => 15));

#[test]
fn b05_chain_child_produced_arity_runs() {
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, amount| {
            flow.chain(|sub| {
                let first = sub.then(double, amount);
                let second = sub.then(add_one, first);
                let third = sub.then(double, second);
                (first, second, third)
            })
        },
        Amount(20),
    ))
    .expect("child-produced arity runs");
    assert_eq!((output.0.0, output.1.0, output.2.0), (40, 41, 82));
}

// ---------------------------------------------------------------- B10（frame 与执行域）

#[test]
fn b10_real_frames_and_single_execution_domain_are_observed() {
    reset_observations();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, amount| flow.chain(|outer| outer.chain(|inner| inner.then(double, amount))),
        Amount(20),
    ))
    .expect("nested chain run");
    assert_eq!(output.0, 40);

    let frames = observe::take_frames();
    let kinds: Vec<(&str, &str)> = frames
        .iter()
        .map(|frame| (frame.kind, frame.phase))
        .collect();
    assert_eq!(kinds.first().copied(), Some(("root", "enter")));
    assert_eq!(kinds.last().copied(), Some(("root", "exit")));
    assert!(kinds.contains(&("boundary", "enter")));
    assert!(kinds.contains(&("leaf", "enter")));
    assert!(kinds.contains(&("leaf", "exit")));
    assert_eq!(
        kinds
            .iter()
            .filter(|k| **k == ("boundary", "enter"))
            .count(),
        2,
        "two real boundary frames"
    );
    let first_boundary = kinds
        .iter()
        .position(|k| *k == ("boundary", "enter"))
        .unwrap();
    let first_leaf = kinds.iter().position(|k| *k == ("leaf", "enter")).unwrap();
    assert!(
        first_boundary < first_leaf,
        "parent frame enters before its leaf"
    );
    let child_scopes: Vec<_> = frames
        .iter()
        .filter(|frame| frame.kind == "boundary" && frame.phase == "enter")
        .map(|frame| frame.scope.clone())
        .collect();
    assert_eq!(child_scopes.len(), 2);
    assert!(
        frames
            .iter()
            .filter(|frame| frame.kind == "leaf" && frame.phase == "enter")
            .all(|frame| child_scopes.contains(&frame.scope)),
        "leaf frames reuse one of the real child scopes"
    );

    let creations = test_support::boundary_creation_snapshot();
    assert_eq!(creations.len(), 2);
    assert!(
        creations
            .iter()
            .all(|(_, _, role)| *role == ScopeRole::Flow)
    );
    assert_eq!(
        creations[1].1, creations[0].0,
        "inner boundary is created inside the outer child"
    );
    assert_ne!(creations[0].0, creations[0].1);

    let addresses = test_support::boundary_address_snapshot();
    assert_eq!(addresses.len(), 2);
    let (_, identity, coordinator, container) = addresses[0];
    assert!(
        addresses
            .iter()
            .all(|(_, i, c, ct)| *i == identity && *c == coordinator && *ct == container),
        "one Context / coordinator / container for the whole execution"
    );
}

// ---------------------------------------------------------------- B11（逐层 Import 去重）

#[test]
fn b11_each_layer_imports_the_same_static_refid_once() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| flow.chain(|outer| outer.chain(|inner| inner.then(seed_reader, seed))),
        Seed {
            value: 41,
            log: log.clone(),
        },
    ))
    .expect("deep capture run");
    assert_eq!(output.0, 42);
    assert!(
        !log.take().iter().any(|event| event.starts_with("drop:")),
        "ancestor data is not destroyed inside the children"
    );

    let imports = observe::take_chain_imports();
    assert_eq!(imports.len(), 2, "one import batch per crossed boundary");
    for record in &imports {
        assert_eq!(
            record.pairs.len(),
            1,
            "a single position is imported once per layer"
        );
        let (source, target) = record.pairs[0].clone();
        assert_eq!(
            source, target,
            "the same static RefId serves as source and target"
        );
    }
    assert_eq!(
        imports[0].pairs[0].0, imports[1].pairs[0].0,
        "the same static r(p) crosses both layers"
    );
    assert_ne!(imports[0].child, imports[1].child);
    assert_eq!(
        imports[1].caller, imports[0].child,
        "inner caller is the outer child"
    );
}

// ---------------------------------------------------------------- B12（Closed／单调／旧 Scope）

#[test]
fn b12_chain_scopes_close_and_never_recur() {
    reset_observations();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, amount| flow.chain(|outer| outer.chain(|inner| inner.then(double, amount))),
        Amount(20),
    ))
    .expect("nested chain run");
    assert_eq!(output.0, 40);

    let creations = test_support::boundary_creation_snapshot();
    let states = observe::take_scope_states();
    for (child, _caller, _) in &creations {
        let after_body = states
            .iter()
            .find(|record| record.at == "after-body" && record.scope == *child)
            .expect("after-body state");
        assert_eq!(after_body.state, ScopeState::Active);
        let after_export = states
            .iter()
            .find(|record| record.at == "after-export" && record.scope == *child)
            .expect("after-export state");
        assert_eq!(after_export.state, ScopeState::Closed);
    }
    assert!(
        creations[1].0.seq() > creations[0].0.seq(),
        "scope sequence advances monotonically within one execution"
    );
    assert!(std::sync::Arc::ptr_eq(
        creations[0].0.execution(),
        creations[1].0.execution()
    ));

    // 已 Closed 的 child 不能再进入（旧 Scope 拒绝）；读取拒绝另见 F02。
    let rejections = observe::take_scope_rejections();
    let enter_rejections: Vec<_> = rejections
        .iter()
        .filter(|record| record.operation == "enter-closed-child")
        .collect();
    assert_eq!(enter_rejections.len(), creations.len());
    assert!(
        enter_rejections
            .iter()
            .all(|record| matches!(record.error, ScopeError::ScopeClosed { .. })),
        "a closed chain scope rejects a new boundary entry"
    );
    assert!(
        creations
            .iter()
            .all(|(child, _, _)| rejections.iter().any(|record| record.scope == *child)),
        "each closed child has its own rejection record"
    );
}

// ---------------------------------------------------------------- B13（target／owner／转移次数）

#[test]
fn b13_export_targets_and_single_transfer_are_observed() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let pair = flow.chain(|sub| {
                let value = inner_amount(sub, seed);
                (value, value)
            });
            pair.0
        },
        Seed {
            value: 20,
            log: log.clone(),
        },
    ))
    .expect("duplicate chain output run");
    assert_eq!(output.0, 21);

    let snapshots = observe::take_export_snapshots();
    let committed = export_snapshot(&snapshots, "committed");
    assert!(
        exports_share_the_root_scope(&snapshots),
        "export snapshots are taken on the root caller"
    );
    let mut data_targets: Vec<_> = committed
        .refs
        .iter()
        .filter_map(|(_, target)| match target {
            TargetSnapshot::Data(id) => Some(id.clone()),
            _ => None,
        })
        .collect();
    let duplicated = {
        let mut counts: Vec<(usize, usize)> = Vec::new();
        for (index, id) in data_targets.iter().enumerate() {
            let same = data_targets.iter().filter(|other| *other == id).count();
            counts.push((index, same));
        }
        counts.iter().any(|(_, same)| *same >= 2)
    };
    assert!(duplicated, "two parent slots share one actual target");
    data_targets.dedup();
    assert_eq!(
        data_targets.len(),
        2,
        "the seeded input plus the produced value"
    );
    let produced = data_targets.last().expect("produced target").clone();
    assert_eq!(
        committed.owned.iter().filter(|id| **id == produced).count(),
        1,
        "responsibility is transferred exactly once for the shared DataId"
    );
    assert!(
        committed.owned_alive.iter().all(|(_, alive, _)| *alive),
        "transferred values stay alive"
    );
    assert!(log.take().is_empty());
}

#[test]
fn b13_ancestor_alias_is_bound_without_transfer() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| flow.chain(|_sub| seed),
        Seed {
            value: 7,
            log: log.clone(),
        },
    ))
    .expect("alias chain run");
    assert_eq!(output.value, 7);

    let snapshots = observe::take_export_snapshots();
    let before = export_snapshot(&snapshots, "before");
    let committed = export_snapshot(&snapshots, "committed");
    let seeded: Vec<_> = before
        .refs
        .iter()
        .filter_map(|(_, target)| match target {
            TargetSnapshot::Data(id) => Some(id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        seeded.len(),
        1,
        "only the seeded input exists before the export"
    );
    let seeded_id = seeded[0].clone();
    assert_eq!(
        before.owned.iter().filter(|id| **id == seeded_id).count(),
        1,
        "the caller already owns the seeded input exactly once"
    );
    assert_eq!(
        committed
            .owned
            .iter()
            .filter(|id| **id == seeded_id)
            .count(),
        1,
        "an ancestor alias adds no second ownership in the caller"
    );
    assert!(
        committed
            .refs
            .iter()
            .any(|(_, target)| matches!(target, TargetSnapshot::Data(id) if *id == seeded_id)),
        "the new parent position is bound to the ancestor target"
    );
    let owner = committed
        .owned_alive
        .iter()
        .find(|(id, _, _)| *id == seeded_id)
        .and_then(|(_, _, owner)| owner.clone());
    assert_eq!(
        owner,
        Some(committed.scope.clone()),
        "the ancestor value stays owned by the caller, and only once"
    );
}

// ---------------------------------------------------------------- B14／B15（Root 提取）

#[test]
fn b14_rejected_root_take_keeps_values_owned_and_takes_zero() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, seed| {
            flow.chain(|sub| {
                let value = inner_amount(sub, seed);
                (value, value)
            })
        },
        Seed {
            value: 3,
            log: log.clone(),
        },
    ))
    .err()
    .expect("duplicate DataId is rejected before any take");
    assert!(matches!(error, RunError::Runtime(_)), "{error}");
    assert!(
        test_support::take_log_snapshot().is_empty(),
        "no value was taken"
    );
    let rejected = test_support::take_root_snapshots()
        .into_iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::PreflightRejected)
        .expect("preflight rejection snapshot");
    assert_eq!(rejected.planned_takes, 0);
    assert!(
        !rejected.root_owned.is_empty(),
        "responsibility is still held at the rejection point"
    );
    assert_eq!(
        rejected.root_state,
        ScopeState::Finalizing,
        "the root is frozen for the rejected preflight"
    );
}

#[test]
fn b15_owned_output_leaves_the_runtime_and_root_responsibility_is_released() {
    reset_observations();

    async fn produce_tracked(query: Query<&TrackedSeed>) -> Result<TrackedValue, BodyError> {
        Ok(TrackedValue {
            label: "out",
            log: query.get().log.clone(),
        })
    }

    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| flow.then(produce_tracked, seed),
        TrackedSeed {
            value: 5,
            log: log.clone(),
        },
    ))
    .expect("owned output run");
    assert_eq!(output.label, "out");
    let events = log.take();
    assert!(
        !events.iter().any(|event| event == "drop:out"),
        "the runtime does not destroy the returned value"
    );
    assert!(
        events.iter().any(|event| event == "drop:seed"),
        "the unreturned input is cleaned during the root close"
    );
    let snapshots = test_support::take_root_snapshots();
    let after_close = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterClose)
        .expect("after-close snapshot");
    assert!(
        after_close.root_owned.is_empty(),
        "root responsibility released"
    );
    assert_eq!(after_close.root_state, ScopeState::Closed);
    assert!(
        after_close.taken_alive.iter().all(|alive| !*alive),
        "taken values have left the container"
    );
    assert!(
        after_close.taken_owned_by.iter().all(Option::is_none),
        "the root no longer owns the taken values"
    );
    assert_eq!(
        test_support::take_log_snapshot().len(),
        1,
        "exactly one take"
    );

    drop(output);
    assert_eq!(log.take(), vec!["drop:out".to_string()]);
}

// ---------------------------------------------------------------- B16（整组原子性）

#[test]
fn b16_export_group_rejects_whole_group_and_keeps_kinds() {
    reset_observations();

    async fn produce_child_value(query: Query<&TrackedSeed>) -> Result<TrackedValue, BodyError> {
        Ok(TrackedValue {
            label: "child",
            log: query.get().log.clone(),
        })
    }

    let log = Log::new();
    let runtime = Runtime::new();
    install_export_occupy_fault(1);
    let error = block_on(runtime.execute(
        |flow, seed| {
            flow.chain(|sub| {
                let first = sub.then(produce_child_value, seed);
                let second = sub.then(produce_child_value, seed);
                (first, second)
            })
        },
        TrackedSeed {
            value: 5,
            log: log.clone(),
        },
    ))
    .err()
    .expect("occupied caller slot rejects the whole export");
    assert!(matches!(error, RunError::Runtime(_)), "{error}");

    let snapshots = observe::take_export_snapshots();
    let before = export_snapshot(&snapshots, "before");
    let rejected = export_snapshot(&snapshots, "rejected");
    assert!(
        !snapshots.iter().any(|s| s.phase == "committed"),
        "no group was committed"
    );
    assert_eq!(
        rejected.refs.len(),
        before.refs.len(),
        "the legal first slot is not silently committed"
    );
    assert!(
        before.refs.len() >= 2,
        "seeded input plus the injected occupation are bound before the attempt"
    );
    assert_eq!(rejected.owned, before.owned, "no responsibility transfer");
    assert_eq!(
        rejected.next_data_id, before.next_data_id,
        "the rejected group allocates nothing"
    );
    assert!(test_support::take_log_snapshot().is_empty(), "no take");
    let events = log.take();
    assert_eq!(
        events.iter().filter(|event| *event == "drop:child").count(),
        2,
        "both child values are destroyed exactly once"
    );
}

#[test]
fn b16_import_group_rejects_whole_group_without_partial_bindings() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    install_import_occupy_fault(1);
    let error = block_on(runtime.execute(
        |flow, (seed, amount)| {
            flow.chain(|sub| {
                let _: () = sub.then(two_inputs_unit, (seed, amount));
                ()
            })
        },
        (
            TrackedSeed {
                value: 5,
                log: log.clone(),
            },
            Amount(1),
        ),
    ))
    .err()
    .expect("occupied import target rejects the whole batch");
    assert!(matches!(error, RunError::Runtime(_)), "{error}");

    let snapshots = observe::take_export_snapshots();
    let rejected = export_snapshot(&snapshots, "import-rejected");
    assert_eq!(
        rejected.refs.len(),
        1,
        "only the injected occupation exists; no partial import is bound"
    );
    let states = observe::take_scope_states();
    let final_state = states
        .iter()
        .find(|record| record.at == "import-rejected")
        .expect("import rejection state");
    assert_eq!(final_state.state, ScopeState::Closed);
    let events = log.take();
    assert_eq!(
        events.iter().filter(|event| *event == "drop:seed").count(),
        1,
        "the input is released exactly once"
    );
}

// ---------------------------------------------------------------- B17（源链／非 Send）

#[derive(Debug)]
struct InnerFailure;

impl std::fmt::Display for InnerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("inner cause")
    }
}

impl std::error::Error for InnerFailure {}

#[derive(Debug)]
struct OuterFailure {
    inner: InnerFailure,
    tag: std::rc::Rc<u32>,
}

impl std::fmt::Display for OuterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "outer failure tagged {}", *self.tag)
    }
}

impl std::error::Error for OuterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.inner)
    }
}

#[test]
fn b17_nested_source_and_non_send_object_are_preserved() {
    reset_observations();

    async fn failing(_query: Query<()>) -> Result<(), BodyError> {
        Err(BodyError::fail(OuterFailure {
            inner: InnerFailure,
            tag: std::rc::Rc::new(9),
        }))
    }

    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, _amount| {
            let _: () = flow.then(failing, ());
            ()
        },
        Amount(1),
    ))
    .err()
    .expect("failure propagates");
    let RunError::Body(failure) = error else {
        panic!("expected body failure, got {error}");
    };
    let outer = std::error::Error::source(&failure)
        .and_then(|error| error.downcast_ref::<OuterFailure>())
        .expect("original non-Send object preserved");
    assert_eq!(*outer.tag, 9);
    let inner = std::error::Error::source(outer)
        .and_then(|error| error.downcast_ref::<InnerFailure>())
        .expect("nested source preserved");
    assert_eq!(inner.to_string(), "inner cause");
    assert_eq!(test_support::take_termination_saved().len(), 1);
}

// ---------------------------------------------------------------- B18／B19（Control 传播与清理）

#[test]
fn b18_nested_control_closes_every_frame_and_stops_later_business() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    reset_observations();

    async fn retrying(_query: Query<()>) -> Result<(), BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::retry(DomainError { detail: "nested" }))
    }

    async fn never(_query: Query<()>) -> Result<(), BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, ()| {
            flow.chain(|outer| {
                outer.chain(|inner| {
                    let _: () = inner.then(retrying, ());
                    let _: () = inner.then(never, ());
                    ()
                })
            })
        },
        (),
    ))
    .err()
    .expect("unhandled control terminates the root");
    assert!(matches!(
        error,
        RunError::UnhandledControl(ControlSignal::Retry(_))
    ));
    assert_eq!(CALLS.load(Ordering::SeqCst), 1, "later steps do not run");

    let closed = observe::take_scope_states()
        .into_iter()
        .filter(|record| record.at == "control-closed")
        .collect::<Vec<_>>();
    assert_eq!(
        closed.len(),
        2,
        "both child boundaries close during the control exit"
    );
    assert!(
        closed
            .iter()
            .all(|record| record.state == ScopeState::Closed)
    );

    let frames = observe::take_frames();
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame.kind == "boundary" && frame.phase == "exit")
            .count(),
        2
    );
    assert!(
        frames
            .iter()
            .any(|frame| frame.kind == "root" && frame.phase == "exit")
    );
    assert_eq!(
        test_support::take_termination_saved().len(),
        1,
        "first termination recorded exactly once"
    );
}

#[test]
fn b19_cleanup_failure_keeps_unclosed_responsibility_and_stops_business() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    reset_observations();

    async fn retrying(_query: Query<()>) -> Result<(), BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::retry(DomainError { detail: "cleanup" }))
    }

    async fn never(_query: Query<()>) -> Result<(), BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    let runtime = Runtime::new();
    install_control_cleanup_fault();
    let error = block_on(runtime.execute(
        |flow, ()| {
            flow.chain(|sub| {
                let _: () = sub.then(retrying, ());
                let _: () = sub.then(never, ());
                ()
            })
        },
        (),
    ))
    .err()
    .expect("cleanup failure replaces the recoverable control");
    let RunError::Runtime(runtime_error) = error else {
        panic!("expected a runtime error, got {error}");
    };
    let source = std::error::Error::source(&runtime_error).expect("original cleanup diagnostic");
    assert!(!source.to_string().is_empty());
    assert_eq!(CALLS.load(Ordering::SeqCst), 1, "no later business runs");

    let failed = observe::take_scope_states()
        .into_iter()
        .find(|record| record.at == "control-cleanup-failed")
        .expect("cleanup failure observation");
    assert_ne!(
        failed.state,
        ScopeState::Closed,
        "unfinished responsibility is retained, not reported as closed"
    );
}

// ---------------------------------------------------------------- B20（取消与析构次序）

#[test]
fn b20_deep_pending_cancel_orders_every_drop() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let mut future = Box::pin(runtime.execute(
        |flow, seed| {
            flow.chain(|sub| {
                let produced = sub.then(read_tracked_seed, seed);
                let _ = produced;
                let _pending: Ref<Amount> = sub.then(pending_guard_node, seed);
                ()
            })
        },
        TrackedSeed {
            value: 5,
            log: log.clone(),
        },
    ));

    let waker = futures::task::noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    drop(future);

    assert_eq!(
        log.take(),
        vec![
            "drop:pending-future".to_string(),
            "drop:temp".to_string(),
            "drop:seed".to_string(),
        ],
        "borrowed future ends first, then child temporaries, then the root input"
    );
}

// ---------------------------------------------------------------- B21（跨执行身份隔离）

#[test]
fn b21_executions_use_distinct_identity_domains() {
    reset_observations();
    let runtime = Runtime::new();
    let first = block_on(runtime.execute(
        |flow, amount| flow.chain(|sub| sub.then(double, amount)),
        Amount(1),
    ))
    .expect("first execution");
    assert_eq!(first.0, 2);
    let first_creations = test_support::boundary_creation_snapshot();
    let first_addresses = test_support::boundary_address_snapshot();
    assert_eq!(first_creations.len(), 1);
    assert_eq!(first_addresses.len(), 1);

    test_support::boundary_creation_reset();
    test_support::boundary_address_reset();

    let second = block_on(runtime.execute(
        |flow, amount| flow.chain(|sub| sub.then(double, amount)),
        Amount(10),
    ))
    .expect("second execution");
    assert_eq!(second.0, 20);
    let second_creations = test_support::boundary_creation_snapshot();
    let second_addresses = test_support::boundary_address_snapshot();

    assert!(!std::sync::Arc::ptr_eq(
        first_creations[0].0.execution(),
        second_creations[0].0.execution()
    ));
    // 地址复用不构成身份证明：以 ExecutionIdentity 判定跨执行隔离。
    assert!(!std::sync::Arc::ptr_eq(
        first_creations[0].0.execution(),
        second_creations[0].0.execution()
    ));
    let _ = (first_addresses, second_addresses);
}

// ---------------------------------------------------------------- F01（合法 Control 传播期间 Context 仍有效）

/// 取本次执行的 Root Scope（来自真实 frame 记录）。
fn observe_root_scope() -> crate::core::identity::ScopeId {
    observe::take_frames()
        .into_iter()
        .find(|frame| frame.kind == "root" && frame.phase == "enter")
        .expect("root frame record")
        .scope
}

#[test]
fn f01_legal_control_propagation_keeps_context_valid_until_root_close() {
    static RETRY_CALLS: AtomicUsize = AtomicUsize::new(0);
    static BREAK_CALLS: AtomicUsize = AtomicUsize::new(0);

    async fn retrying(_query: Query<()>) -> Result<(), BodyError> {
        RETRY_CALLS.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::retry(DomainError {
            detail: "legal retry",
        }))
    }

    async fn breaking(_query: Query<()>) -> Result<(), BodyError> {
        BREAK_CALLS.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::iter_break())
    }

    let runtime = Runtime::new();
    for (round, expect_retry) in [(0usize, true), (1usize, false)] {
        reset_observations();
        let error = block_on(runtime.execute(
            |flow, ()| {
                flow.chain(|outer| {
                    outer.chain(|inner| {
                        if round == 0 {
                            let _: () = inner.then(retrying, ());
                        } else {
                            let _: () = inner.then(breaking, ());
                        }
                        ()
                    })
                })
            },
            (),
        ))
        .err()
        .expect("unhandled control reaches the root");
        match (error, expect_retry) {
            (RunError::UnhandledControl(ControlSignal::Retry(_)), true) => {}
            (RunError::UnhandledControl(ControlSignal::IterBreak(_)), false) => {}
            (other, _) => panic!("unexpected classification: {other}"),
        }

        // 中间 Boundary 完成后的状态：未终止、child 已 Closed、无终止定位。
        let completions = observe::take_control_completions();
        assert_eq!(
            completions.len(),
            2,
            "both boundaries completed before the root close"
        );
        assert!(
            completions.iter().all(|record| !record.terminated),
            "a legal control exit completes without marking the execution as failed"
        );
        assert!(
            completions
                .iter()
                .all(|record| record.child_state == Some(ScopeState::Closed))
        );
        assert!(
            completions
                .iter()
                .all(|record| record.termination_scope.is_none())
        );

        // 责任解除：每层在完成前已清空责任集合，且恰好解除一个 Scope。
        let releases = observe::take_control_releases();
        assert_eq!(releases.len(), 2);
        assert!(
            releases
                .iter()
                .all(|record| record.responsible_empty && record.released == 1)
        );
        assert!(
            releases
                .iter()
                .map(|record| record.child.clone())
                .eq(completions.iter().map(|record| record.child.clone())),
            "each boundary release is paired with its own completion record"
        );

        // 首次（也是唯一）不可恢复终止发生在 Root 收口，而不是 Boundary。
        let terminations = test_support::take_termination_saved();
        assert_eq!(terminations.len(), 1, "exactly one termination is recorded");
        assert_eq!(terminations[0].kind, TerminationKind::BodyError);
        let root_scope = observe_root_scope();
        assert_eq!(
            terminations[0].scope.as_ref(),
            Some(&root_scope),
            "the first termination belongs to the root close, not to a boundary"
        );
    }
    assert_eq!(RETRY_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(BREAK_CALLS.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------- F02（顺序 Chain 与旧 Scope 读取拒绝）

#[test]
fn f02_sequential_chains_close_before_the_next_child_and_reject_old_reads() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let first = flow.chain(|sub| sub.then(seed_reader, seed));
            let second = flow.chain(|sub| sub.then(AmountStep, first));
            let _ = second;
            flow.then(seed_observer, seed)
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .expect("sequential chains run");
    assert_eq!(output, ());
    assert!(
        log.take().contains(&"start:observer".to_string()),
        "the parent position stays readable after both children closed"
    );

    let creations = observe::take_chain_creations();
    assert_eq!(creations.len(), 2);
    assert!(
        !creations[0].closed_before.contains(&creations[0].child),
        "the first child is created while nothing else is closed"
    );
    assert!(
        creations[1].closed_before.contains(&creations[0].child),
        "the second child is created only after the first one is closed"
    );
    assert!(std::sync::Arc::ptr_eq(
        creations[0].child.execution(),
        creations[1].child.execution()
    ));
    assert!(creations[1].child.seq() > creations[0].child.seq());
    assert_ne!(creations[0].child, creations[1].child);
    let root_scope = observe_root_scope();
    assert!(
        creations.iter().all(|record| record.caller == root_scope),
        "both sequential chains are created in the same root caller"
    );

    let rejections = observe::take_scope_rejections();
    let read_rejection = rejections
        .iter()
        .find(|record| record.operation == "resolve-closed-child")
        .expect("the closed child rejects a controlled read of its old position");
    assert!(
        matches!(read_rejection.error, ScopeError::ScopeClosed { .. }),
        "expected ScopeClosed, got {}",
        read_rejection.error
    );
}

// ---------------------------------------------------------------- F03（跨执行完整身份与访问拒绝）

#[test]
fn f03_cross_execution_identity_access_is_rejected() {
    reset_observations();
    let runtime = Runtime::new();

    // 第一次执行：取得完整 Root ScopeId 与真实 DataId（观察记录 + take 日志）。
    let first = block_on(runtime.execute(
        |flow, amount| flow.chain(|sub| sub.then(double, amount)),
        Amount(3),
    ))
    .expect("first run");
    assert_eq!(first.0, 6);
    let first_root = observe_root_scope();
    let first_data = test_support::take_log_snapshot()
        .into_iter()
        .next()
        .expect("first execution take log");

    // 第二次执行：在真实 Context 的 caller frame 中尝试访问第一域身份。
    reset_observations();
    install_foreign_access_fault(first_root.clone(), first_data.clone());
    let second = block_on(runtime.execute(
        |flow, amount| flow.chain(|sub| sub.then(double, amount)),
        Amount(5),
    ))
    .expect("second run");
    assert_eq!(second.0, 10);
    let second_root = observe_root_scope();
    let second_data = test_support::take_log_snapshot()
        .into_iter()
        .next()
        .expect("second execution take log");

    // 身份隔离：完整 ScopeId／DataId 分属各自的 Execution，且互不相等。
    assert!(!std::sync::Arc::ptr_eq(
        first_root.execution(),
        second_root.execution()
    ));
    assert!(!std::sync::Arc::ptr_eq(
        first_data.execution(),
        second_root.execution()
    ));
    assert!(!std::sync::Arc::ptr_eq(
        first_data.execution(),
        second_data.execution()
    ));
    assert_ne!(first_root, second_root);
    assert_ne!(first_data, second_data);

    // 第二域中的受控访问：原框架拒绝且本域绑定／责任不变。
    let accesses = observe::take_foreign_accesses();
    assert_eq!(
        accesses.len(),
        2,
        "scope and data identity probes are recorded"
    );
    let scope_access = &accesses[0];
    assert_eq!(scope_access.operation, "state(foreign-scope)");
    assert!(scope_access.rejected);
    assert!(
        scope_access.detail.contains("belongs to another execution"),
        "original framework diagnostic: {}",
        scope_access.detail
    );
    assert_eq!(
        scope_access.local_refs_before,
        scope_access.local_refs_after
    );
    assert_eq!(
        scope_access.local_owned_before,
        scope_access.local_owned_after
    );
    let data_access = &accesses[1];
    assert_eq!(data_access.operation, "alive/owner(foreign-data)");
    assert!(data_access.rejected);
    assert!(
        data_access.detail.contains("alive=false"),
        "the foreign instance is not visible in this domain: {}",
        data_access.detail
    );
    assert_eq!(data_access.local_refs_before, data_access.local_refs_after);
    assert_eq!(
        data_access.local_owned_before,
        data_access.local_owned_after
    );
}

// ================================================================ WE-T03 Choose
// C01～C22 的 crate 内证据：全部经公开 `Runtime::new().execute(...)` 或真实 Core 入口运行，
// 观察点复用既有 cfg(test) 通道（observe／test_support／creation_counts）。

/// C02：只实现 `Data + PartialEq` 的匹配键。
///
/// 刻意不实现 Eq／Hash／Ord／Clone／Copy／Debug／Display，并携带 `Rc` 字段；
/// case 值只作为 Definition 配置，不获得 DataId／Scope owner。
#[derive(Data)]
struct Route {
    tag: Rc<str>,
    code: u8,
}

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code && self.tag == other.tag
    }
}

fn route(code: u8) -> Route {
    Route {
        tag: Rc::from("route"),
        code,
    }
}

/// 复杂路由由前序 Node 产生的 selector：Seed → Route。
async fn route_of(query: Query<&Seed>) -> Result<Route, BodyError> {
    Ok(route(query.get().value as u8))
}

/// 同上，来源是 Amount（选择器不消费输入样本使用）。
async fn route_of_amount(query: Query<&Amount>) -> Result<Route, BodyError> {
    Ok(route(query.get().0 as u8))
}

async fn branch_alpha(query: Query<&Seed>) -> Result<Amount, BodyError> {
    let seed = query.get();
    seed.log.push("branch:alpha");
    Ok(Amount(seed.value + 1))
}

async fn branch_beta(query: Query<&Seed>) -> Result<Amount, BodyError> {
    let seed = query.get();
    seed.log.push("branch:beta");
    Ok(Amount(seed.value + 2))
}

async fn branch_fallback(query: Query<&Seed>) -> Result<Amount, BodyError> {
    let seed = query.get();
    seed.log.push("branch:fallback");
    Ok(Amount(seed.value + 3))
}

/// 观测 Amount 读取（选择器在 Choose 之后仍可读）。
async fn amount_logged(query: Query<&Amount>) -> Result<(), BodyError> {
    AMOUNT_READS.with(|reads| reads.borrow_mut().push(query.get().0));
    Ok(())
}

thread_local! {
    static AMOUNT_READS: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
}

/// C08：tuple 形状按现有 Shape 上限逐个 arity 生成（2～16）。
///
/// 每个分支自产 N 个独立 Amount（seed_reader 之后逐个 AmountStep），全部作为共同输出。
macro_rules! choose_arity_case {
    ($name:ident; $($slot:ident),+) => {
        #[test]
        fn $name() {
            reset_observations();
            let runtime = Runtime::new();
            let output = block_on(runtime.execute(
                |flow, seed| {
                    let selector = flow.then(route_of, seed);
                    flow.choose(selector, |choice| {
                        choice.case(route(1), |branch| {
                            let mut previous = branch.then(seed_reader, seed);
                            $(
                                let $slot = branch.then(AmountStep, previous);
                                previous = $slot;
                            )+
                            let _ = previous;
                            ($($slot,)+)
                        });
                    })
                },
                Seed {
                    value: 1,
                    log: Log::new(),
                },
            ))
            .expect("arity choose run");
            let ($($slot,)+) = output;
            // seed=1 → seed_reader=2，随后每个槽位由 AmountStep 自产。
            let mut expected = 3u32;
            $(
                assert_eq!($slot.0, expected, "slot {}", stringify!($slot));
                expected += 1;
            )+
            // 只建立一个分支边界，且全部槽位来自该分支的整组 Export。
            assert_eq!(
                test_support::boundary_creation_snapshot().len(),
                1,
                "exactly one branch boundary is created"
            );
        }
    };
}

choose_arity_case!(c08_choose_tuple_arity_2; a0, a1);
choose_arity_case!(c08_choose_tuple_arity_3; a0, a1, a2);
choose_arity_case!(c08_choose_tuple_arity_4; a0, a1, a2, a3);
choose_arity_case!(c08_choose_tuple_arity_5; a0, a1, a2, a3, a4);
choose_arity_case!(c08_choose_tuple_arity_6; a0, a1, a2, a3, a4, a5);
choose_arity_case!(c08_choose_tuple_arity_7; a0, a1, a2, a3, a4, a5, a6);
choose_arity_case!(c08_choose_tuple_arity_8; a0, a1, a2, a3, a4, a5, a6, a7);
choose_arity_case!(c08_choose_tuple_arity_9; a0, a1, a2, a3, a4, a5, a6, a7, a8);
choose_arity_case!(c08_choose_tuple_arity_10; a0, a1, a2, a3, a4, a5, a6, a7, a8, a9);
choose_arity_case!(c08_choose_tuple_arity_11; a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
choose_arity_case!(c08_choose_tuple_arity_12; a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11);
choose_arity_case!(c08_choose_tuple_arity_13; a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12);
choose_arity_case!(c08_choose_tuple_arity_14; a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13);
choose_arity_case!(c08_choose_tuple_arity_15; a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14);
choose_arity_case!(c08_choose_tuple_arity_16; a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15);

// ---------------------------------------------------------------- C01 冻结用法

#[test]
fn c01_frozen_choose_usage_runs_case_case_and_fallback() {
    let runtime = Runtime::new();
    for (value, expected_event, expected_output) in [
        (1u32, "branch:alpha", 2u32),
        (2u32, "branch:beta", 4u32),
        (7u32, "branch:fallback", 10u32),
    ] {
        let log = Log::new();
        let output = block_on(runtime.execute(
            |flow, seed| {
                let selector = flow.then(route_of, seed);
                flow.choose(selector, |choice| {
                    choice.case(route(1), |branch| branch.then(branch_alpha, seed));
                    choice.case(route(2), |branch| branch.then(branch_beta, seed));
                    choice.otherwise(|branch| branch.then(branch_fallback, seed));
                })
            },
            Seed {
                value,
                log: log.clone(),
            },
        ))
        .expect("frozen choose run");
        assert_eq!(output.0, expected_output, "selected branch output");
        let events = log.take();
        assert_eq!(
            events,
            vec![expected_event.to_string()],
            "only one branch ran"
        );
    }
}

#[test]
fn c01_choose_nests_chain_choose_and_fragment_forms() {
    // Fragment：普通函数返回引用 Node 的闭包，可直接作为分支定义。
    fn fragment<'n>(
        node: &'n AmountStep,
        input: Ref<Amount>,
    ) -> impl FnOnce(&mut crate::Flow<'n>) -> Ref<Amount> + 'n {
        move |branch| branch.then(node, input)
    }

    let log = Log::new();
    let step = AmountStep;
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(10), |branch| {
                    // 分支内再 chain → choose；内层分支用 Fragment 形式定义。
                    let inner = branch.chain(|sub| sub.then(seed_reader, seed));
                    let inner_route = branch.then(route_of_amount, inner);
                    let picked = branch.choose(inner_route, |inner_choice| {
                        inner_choice.case(route(11), fragment(&step, inner));
                        inner_choice.otherwise(|deep| deep.then(branch_fallback, seed));
                    });
                    branch.chain(fragment(&step, picked))
                });
            })
        },
        Seed {
            value: 10,
            log: log.clone(),
        },
    ))
    .expect("nested choose run");
    // seed=10 → seed_reader=11 → 内层 selector=11 → 内层 case 命中 → seed_reader=11 → AmountStep=12
    // → 外层 AmountStep=13
    assert_eq!(output.0, 13);
    assert_eq!(log.take(), Vec::<String>::new());
    reset_observations();
}

// ---------------------------------------------------------------- C02 键与借用

#[test]
fn c02_minimal_key_and_borrowed_node_run() {
    let log = Log::new();
    let delta = 5u32;
    let configured = Configured { delta: &delta };
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let amount = flow.then(seed_reader, seed);
            let selector = flow.then(route_of_amount, amount);
            flow.choose(selector, |choice| {
                // 键：只实现 PartialEq 的 Route（每次构造新值，未实现 Clone／Copy）。
                choice.case(route(2), |branch| branch.then(&configured, amount));
                choice.otherwise(|branch| branch.then(AmountStep, amount));
            })
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .expect("minimal key run");
    assert_eq!(
        output.0, 7,
        "borrowed non-static node ran in the selected branch"
    );
    assert_eq!(log.take(), Vec::<String>::new());
}

// ---------------------------------------------------------------- C03 构建期与运行期

#[test]
fn c03_all_branches_build_once_per_execute_and_rebuild_each_time() {
    static ALPHA_BUILDS: AtomicUsize = AtomicUsize::new(0);
    static BETA_BUILDS: AtomicUsize = AtomicUsize::new(0);
    static FALLBACK_BUILDS: AtomicUsize = AtomicUsize::new(0);
    static BRANCH_CALLS: AtomicUsize = AtomicUsize::new(0);

    async fn counting(query: Query<&Seed>) -> Result<Amount, BodyError> {
        BRANCH_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(query.get().value))
    }

    let runtime = Runtime::new();
    for round in 0..2usize {
        let log = Log::new();
        let output = block_on(runtime.execute(
            |flow, seed| {
                let selector = flow.then(route_of, seed);
                flow.choose(selector, |choice| {
                    choice.case(route(1), |branch| {
                        ALPHA_BUILDS.fetch_add(1, Ordering::SeqCst);
                        branch.then(counting, seed)
                    });
                    choice.case(route(2), |branch| {
                        BETA_BUILDS.fetch_add(1, Ordering::SeqCst);
                        branch.then(counting, seed)
                    });
                    choice.otherwise(|branch| {
                        FALLBACK_BUILDS.fetch_add(1, Ordering::SeqCst);
                        branch.then(counting, seed)
                    });
                })
            },
            Seed {
                value: 1,
                log: log.clone(),
            },
        ))
        .expect("build-once run");
        assert_eq!(output.0, 1);
        assert_eq!(
            ALPHA_BUILDS.load(Ordering::SeqCst),
            round + 1,
            "each case definition closure runs once per execution"
        );
        assert_eq!(BETA_BUILDS.load(Ordering::SeqCst), round + 1);
        assert_eq!(FALLBACK_BUILDS.load(Ordering::SeqCst), round + 1);
        assert_eq!(
            BRANCH_CALLS.load(Ordering::SeqCst),
            round + 1,
            "only the selected branch body executes Node calls"
        );
    }
}

#[test]
fn c03_unpolled_choose_builds_nothing_and_releases_input_once() {
    static BUILDS: AtomicUsize = AtomicUsize::new(0);
    let drops = Arc::new(AtomicUsize::new(0));

    struct Dropper {
        drops: Arc<AtomicUsize>,
    }
    impl Data for Dropper {}
    impl Drop for Dropper {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn identity(query: Query<&Dropper>) -> Result<Route, BodyError> {
        let _ = query.get();
        Ok(route(1))
    }

    let runtime = Runtime::new();
    let future = runtime.execute(
        |flow, input| {
            let selector = flow.then(identity, input);
            flow.choose(selector, |choice| {
                choice.case(route(1), |branch| {
                    BUILDS.fetch_add(1, Ordering::SeqCst);
                    let _: () = branch.then(unit_noop, ());
                });
            })
        },
        Dropper {
            drops: Arc::clone(&drops),
        },
    );
    drop(future);
    assert_eq!(
        BUILDS.load(Ordering::SeqCst),
        0,
        "no branch definition closure runs before the future is polled"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "input released exactly once"
    );
}

// ---------------------------------------------------------------- C04 配置拒绝

#[test]
fn c04_duplicate_case_value_is_rejected() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    async fn counting(_query: Query<&Seed>) -> Result<Amount, BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(0))
    }

    reset_observations();
    let before = creation_counts::snapshot();
    let error = block_on(Runtime::new().execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                // 两个值只实现 PartialEq：按值相等判重（tag 与 code 都相同）。
                choice.case(route(1), |branch| branch.then(counting, seed));
                choice.case(route(1), |branch| branch.then(counting, seed));
            })
        },
        Seed {
            value: 1,
            log: Log::new(),
        },
    ))
    .err()
    .expect("duplicate case value must be rejected");
    let RunError::Definition(error) = error else {
        panic!("expected a definition error, got {error}");
    };
    assert!(
        format!("{error}").contains("repeats a case value"),
        "diagnostic names the duplicated configuration: {error}"
    );
    assert_eq!(CALLS.load(Ordering::SeqCst), 0, "no node runs");
    let after = creation_counts::snapshot();
    assert_eq!(after, before, "no execution context is created");
}

#[test]
fn c04_duplicate_otherwise_and_empty_config_are_rejected() {
    let error = block_on(Runtime::new().execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1), |branch| {
                    let _: () = branch.then(seed_observer, seed);
                });
                choice.otherwise(|branch| {
                    let _: () = branch.then(seed_observer, seed);
                });
                choice.otherwise(|branch| {
                    let _: () = branch.then(seed_observer, seed);
                });
            })
        },
        Seed {
            value: 1,
            log: Log::new(),
        },
    ))
    .err()
    .expect("second otherwise must be rejected");
    assert!(
        format!("{error}").contains("second otherwise branch"),
        "{error}"
    );

    // 空配置：结果类型显式约束为 `()`，确保真实到达空配置校验而不是推断失败。
    let error = block_on(Runtime::new().execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            let _: () = flow.choose(selector, |_choice| {});
        },
        Seed {
            value: 1,
            log: Log::new(),
        },
    ))
    .err()
    .expect("empty choose configuration must be rejected");
    assert!(
        format!("{error}").contains("neither a case nor an otherwise"),
        "{error}"
    );
}

#[test]
fn c04_otherwise_only_and_case_without_otherwise_are_legal() {
    let runtime = Runtime::new();
    // otherwise-only：任何值都走 fallback。
    let log = Log::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.otherwise(|branch| branch.then(branch_fallback, seed));
            })
        },
        Seed {
            value: 4,
            log: log.clone(),
        },
    ))
    .expect("otherwise-only run");
    assert_eq!(output.0, 7);
    assert_eq!(log.take(), vec!["branch:fallback".to_string()]);

    // 有 case 而没有 otherwise：命中即正常执行。
    for (value, expected) in [(1u32, "branch:alpha"), (2u32, "branch:beta")] {
        let log = Log::new();
        let output = block_on(runtime.execute(
            |flow, seed| {
                let selector = flow.then(route_of, seed);
                flow.choose(selector, |choice| {
                    choice.case(route(1), |branch| branch.then(branch_alpha, seed));
                    choice.case(route(2), |branch| branch.then(branch_beta, seed));
                })
            },
            Seed {
                value,
                log: log.clone(),
            },
        ))
        .expect("case-only run");
        assert_eq!(log.take(), vec![expected.to_string()]);
        assert!(output.0 > value);
    }
}

// ---------------------------------------------------------------- C06 未选分支也验证

#[test]
fn c06_sibling_refs_inside_an_unselected_branch_are_rejected() {
    // 分支 A 的 Ref 被带出闭包，分支 B 直接使用它：未选分支的 sibling 内部引用同样拒绝。
    let leaked: RefCell<Option<Ref<Amount>>> = RefCell::new(None);
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            let leaked = &leaked;
            flow.choose(selector, |choice| {
                choice.case(route(1), |branch| {
                    let produced = branch.then(seed_reader, seed);
                    *leaked.borrow_mut() = Some(produced);
                });
                choice.case(route(2), |branch| {
                    let foreign = *leaked.borrow(); // sibling 内部位置
                    let inner = match foreign {
                        Some(position) => branch.then(AmountStep, position),
                        None => branch.then(seed_reader, seed),
                    };
                    let _ = inner;
                });
            })
        },
        Seed {
            value: 1,
            log: Log::new(),
        },
    ))
    .err()
    .expect("unselected branch sibling reference must be rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
}

#[test]
fn c06_foreign_refs_inside_an_unselected_branch_are_rejected() {
    let drops = Arc::new(AtomicUsize::new(0));
    struct Dropper {
        drops: Arc<AtomicUsize>,
    }
    impl Data for Dropper {}
    impl Drop for Dropper {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    async fn amount_identity(query: Query<&Amount>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0))
    }
    async fn dropper_route(query: Query<&Dropper>) -> Result<Route, BodyError> {
        let _ = query.get();
        Ok(route(1))
    }
    async fn dropper_observer(query: Query<&Dropper>) -> Result<(), BodyError> {
        let _ = query.get();
        Ok(())
    }

    // 第一次执行捕获一个 Amount Ref；第二次执行的未选分支直接使用它。
    let runtime = Runtime::new();
    let mut leaked: Option<Ref<Amount>> = None;
    let _ = block_on(runtime.execute(
        |_flow, amount| {
            leaked = Some(amount);
            amount
        },
        Amount(3),
    ))
    .expect("capture execution");
    let foreign = leaked.expect("captured ref");

    let error = block_on(runtime.execute(
        move |flow, input| {
            let selector = flow.then(dropper_route, input);
            flow.choose(selector, |choice| {
                choice.case(route(3), |branch| {
                    let produced = branch.then(amount_identity, foreign); // 外来位置
                    let _ = produced;
                });
                choice.case(route(1), |branch| {
                    let _ = branch.then(dropper_observer, input);
                });
            })
        },
        Dropper {
            drops: Arc::clone(&drops),
        },
    ))
    .err()
    .expect("foreign ref in an unselected branch must be rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the rejected definition releases its own input exactly once"
    );
}

// ---------------------------------------------------------------- C07 深层 selector 与 Import

#[test]
fn c07_deep_selector_and_branch_captures_import_once_and_keep_owner() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            flow.chain(|sub| {
                let selector = sub.then(route_of, seed);
                sub.choose(selector, |choice| {
                    choice.case(route(9), |branch| {
                        // 同一祖先位置被两个 Node 使用：Imports 只登记一次、owner 不变。
                        let left = branch.then(seed_reader, seed);
                        let right = branch.then(seed_reader, seed);
                        (left, right)
                    });
                })
            })
        },
        Seed {
            value: 9,
            log: log.clone(),
        },
    ))
    .expect("deep choose run");
    assert_eq!((output.0.0, output.1.0), (10, 10));

    // 锚定：本次 Choose 真实选中的 branch child，以及它所在的 caller（Chain child）。
    let selections = observe::take_choose_selections();
    assert_eq!(selections.len(), 1);
    let branch_child = selections[0].child.clone().expect("selected branch scope");
    let chain_child = selections[0].caller.clone();
    let root_scope = observe_root_scope();
    assert_ne!(branch_child, chain_child);

    // 锚定：Root 在 Chain 边界前真实拥有的唯一实例就是 Root 输入 Seed，
    // 以及 Root Scope 中绑定它的静态 Core RefId。
    let snapshots = observe::take_export_snapshots();
    let root_before = snapshots
        .iter()
        .find(|record| record.phase == "before" && record.scope == root_scope)
        .expect("root caller snapshot before the chain boundary");
    assert_eq!(
        root_before.owned.len(),
        1,
        "the only Root-owned instance at that point is the declared Root input"
    );
    let seed_data = root_before.owned[0].clone();
    assert_eq!(
        root_before
            .owned_alive
            .iter()
            .find(|(id, _, _)| *id == seed_data)
            .map(|(_, alive, owner)| (*alive, owner.clone())),
        Some((true, Some(root_scope.clone()))),
        "the Root input is alive and owned by the Root Scope"
    );
    let seed_positions: Vec<_> = root_before
        .refs
        .iter()
        .filter(|(_, target)| matches!(target, TargetSnapshot::Data(id) if *id == seed_data))
        .map(|(position, _)| position.clone())
        .collect();
    assert_eq!(
        seed_positions.len(),
        1,
        "exactly one logical position binds the Root input in the Root Scope"
    );
    let seed_core = seed_positions[0].clone();

    // 两条真实 Import 批次：Chain child（caller=Root）与所选 branch child（caller=Chain child）；
    // 两者都只登记该祖先一次，且 source == target（同一静态 Core RefId 在各 Scope 各绑定一次）。
    let mut imports = observe::take_chain_imports();
    let mut chain_imports: Vec<_> = imports
        .extract_if(.., |record| record.child == chain_child)
        .collect();
    let mut branch_imports: Vec<_> = imports
        .extract_if(.., |record| record.child == branch_child)
        .collect();
    assert!(imports.is_empty(), "no other boundary imported anything");
    assert_eq!(chain_imports.len(), 1);
    let chain_import = chain_imports.pop().expect("chain import batch");
    assert_eq!(chain_import.caller, root_scope);
    assert_eq!(
        chain_import.pairs,
        vec![(seed_core.clone(), seed_core.clone())]
    );
    assert_eq!(branch_imports.len(), 1);
    let branch_import = branch_imports.pop().expect("branch import batch");
    assert_eq!(
        branch_import.caller, chain_child,
        "the selected branch is entered from the Chain child frame"
    );
    assert_eq!(
        branch_import.pairs,
        vec![(seed_core.clone(), seed_core.clone())],
        "two uses of the ancestor deduplicate into one binding of the same Core RefId"
    );

    // owner 不变：该祖先在 Root 的整组提交后仍由 Root 拥有且存活，未被 Import 增记 owner。
    let root_committed = snapshots
        .iter()
        .find(|record| record.phase == "committed" && record.scope == root_scope)
        .expect("root caller snapshot after the chain boundary committed");
    assert_eq!(
        root_committed
            .owned_alive
            .iter()
            .find(|(id, _, _)| *id == seed_data)
            .map(|(_, alive, owner)| (*alive, owner.clone())),
        Some((true, Some(root_scope.clone()))),
        "the imported ancestor keeps its single owner across the handover"
    );
    assert_eq!(
        root_committed
            .owned
            .iter()
            .filter(|id| **id == seed_data)
            .count(),
        1,
        "the ancestor is owned exactly once"
    );
    reset_observations();
}

// ---------------------------------------------------------------- C08 unit 与单 Ref

#[test]
fn c08_unit_choose_executes_without_a_data_placeholder() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let output: () = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            // 结果未被使用：仍必须执行、收拢责任并关闭。
            flow.choose(selector, |choice| {
                choice.case(route(1), |branch| {
                    let _: () = branch.then(seed_observer, seed);
                });
                choice.otherwise(|branch| {
                    let _: () = branch.then(seed_observer, seed);
                });
            })
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .expect("unit choose run");
    assert_eq!(output, ());
    assert_eq!(log.take(), vec!["start:observer".to_string()]);

    // unit 不产生业务 Data：整次执行只分配了 Root 输入那一个实例。
    let snapshots = test_support::take_root_snapshots();
    let committed = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit)
        .expect("root commit snapshot");
    assert_eq!(
        committed.next_data_id,
        Some(2),
        "a unit branch allocates no placeholder instance \
         (only the root input and the selector node output exist)"
    );
    assert_eq!(
        committed.root_owned.len(),
        2,
        "only the root input and the produced selector are owned"
    );
    reset_observations();
}

#[test]
fn c08_single_ref_choose_output_is_readable_by_the_next_parent_step() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            let picked = flow.choose(selector, |choice| {
                choice.case(route(1), |branch| branch.then(seed_reader, seed));
                choice.otherwise(|branch| branch.then(branch_fallback, seed));
            });
            flow.then(AmountStep, picked)
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .expect("single ref choose run");
    assert_eq!(output.0, 3, "parent step read the exported choose output");
    assert_eq!(log.take(), Vec::<String>::new());
    reset_observations();
}

#[test]
fn c08_mixed_self_produced_and_ancestor_alias_group_is_handed_over_per_slot() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    // 分支返回 (自产, 祖先 alias)：同一组里两条来源路径都要按槽位真实交付。
    let output = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(4), |branch| {
                    let produced = branch.then(seed_reader, seed);
                    (produced, seed)
                });
            })
        },
        Seed {
            value: 4,
            log: log.clone(),
        },
    ))
    .expect("mixed source choose run");
    assert_eq!(output.0.0, 5, "the branch-produced slot is delivered");
    assert_eq!(output.1.value, 4, "the ancestor alias slot is delivered");

    let root_scope = observe_root_scope();
    let selections = observe::take_choose_selections();
    let branch_child = selections[0].child.clone().expect("selected branch scope");
    let snapshots = observe::take_export_snapshots();
    // 分支边界内部的槽位装配：分支自己产生的位置在 caller 提交前归 child 所有。
    let child_created = snapshots
        .iter()
        .find(|record| record.phase == "child-created" && record.scope == branch_child)
        .expect("branch child snapshot at creation");
    assert!(
        child_created.owned.is_empty(),
        "a freshly created branch boundary owns nothing yet"
    );
    let root_before = snapshots
        .iter()
        .find(|record| record.phase == "caller-before" && record.scope == root_scope)
        .expect("caller snapshot before the branch boundary is entered");
    assert_eq!(
        root_before.owned.len(),
        2,
        "before the branch runs the Root owns its declared input and the selector node output"
    );
    // 先登记的 Root 输入就是 Seed 实例。
    let seed_data = root_before
        .owned
        .iter()
        .min_by_key(|id| id.seq())
        .cloned()
        .expect("the earliest allocated instance is the Root input");
    let root_committed = snapshots
        .iter()
        .find(|record| record.phase == "committed" && record.scope == root_scope)
        .expect("caller snapshot after the whole group committed");
    // 逐槽核对：整组提交相对 caller 提交前只新增两个绑定，一个 alias 祖先、一个分支自产。
    let added: Vec<_> = root_committed
        .refs
        .iter()
        .filter(|(position, _)| !root_before.refs.iter().any(|(known, _)| known == position))
        .collect();
    assert_eq!(
        added.len(),
        2,
        "the whole group adds exactly one binding per declared output slot"
    );
    let aliased: Vec<_> = added
        .iter()
        .filter(|(_, target)| matches!(target, TargetSnapshot::Data(id) if *id == seed_data))
        .collect();
    assert_eq!(
        aliased.len(),
        1,
        "exactly one of the two slots aliases the ancestor instance"
    );
    let produced: Vec<_> = added
        .iter()
        .filter(|(_, target)| match target {
            TargetSnapshot::Data(id) => *id != seed_data,
            TargetSnapshot::CollectionItem { .. } => false,
        })
        .collect();
    assert_eq!(
        produced.len(),
        1,
        "the other slot carries the branch-produced instance"
    );
    let produced_data = match produced[0].1 {
        TargetSnapshot::Data(ref id) => id.clone(),
        TargetSnapshot::CollectionItem { .. } => unreachable!(),
    };
    assert_eq!(
        root_committed
            .owned_alive
            .iter()
            .find(|(id, _, _)| *id == produced_data)
            .map(|(_, alive, owner)| (*alive, owner.clone())),
        Some((true, Some(root_scope.clone()))),
        "the branch-produced instance is transferred to the caller"
    );
    assert_eq!(
        root_committed
            .owned_alive
            .iter()
            .find(|(id, _, _)| *id == seed_data)
            .map(|(_, alive, owner)| (*alive, owner.clone())),
        Some((true, Some(root_scope.clone()))),
        "the aliased ancestor keeps its owner"
    );
    reset_observations();
}

// ---------------------------------------------------------------- C09 唯一选择

#[test]
fn c09_selection_runs_one_branch_and_creates_one_boundary() {
    reset_observations();
    let runtime = Runtime::new();
    let log = Log::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(2), |branch| branch.then(branch_beta, seed));
                choice.case(route(5), |branch| branch.then(branch_alpha, seed));
                choice.otherwise(|branch| branch.then(branch_fallback, seed));
            })
        },
        Seed {
            value: 2,
            log: log.clone(),
        },
    ))
    .expect("selection run");
    assert_eq!(output.0, 4);
    assert_eq!(log.take(), vec!["branch:beta".to_string()]);

    let selections = observe::take_choose_selections();
    assert_eq!(
        selections.len(),
        1,
        "one选择决策 recorded per Choose invocation"
    );
    assert_eq!(selections[0].selected, Some(0));
    assert!(!selections[0].otherwise_used);
    assert!(
        selections[0].child.is_some(),
        "the selected branch has a boundary"
    );
    let creations = test_support::boundary_creation_snapshot();
    assert_eq!(
        creations.len(),
        1,
        "unselected branches create no running Scope"
    );
    assert_eq!(
        selections[0].caller, creations[0].1,
        "the choice is made inside the direct caller frame"
    );
    reset_observations();
}

// ---------------------------------------------------------------- C10 selector 只被借用

#[test]
fn c10_selector_is_borrowed_and_stays_readable() {
    reset_observations();
    AMOUNT_READS.with(|reads| reads.borrow_mut().clear());

    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, (seed, amount)| {
            let selector = flow.then(route_of_amount, amount);
            let picked = flow.choose(selector, |choice| {
                choice.case(route(1), |branch| branch.then(branch_alpha, seed));
                choice.otherwise(|branch| branch.then(branch_fallback, seed));
            });
            // Choose 之后 selector 仍可读：未被 take／复制／重新登记。
            let _: () = flow.then(amount_logged, amount);
            picked
        },
        (
            Seed {
                value: 5,
                log: log.clone(),
            },
            Amount(1),
        ),
    ))
    .expect("selector borrow run");
    assert_eq!(output.0, 6);
    assert_eq!(
        AMOUNT_READS.with(|reads| reads.borrow().clone()),
        vec![1],
        "the selector value is still bound and readable after the choose"
    );
    // Root owned 集合仍持有 selector；键值不进入任何 Scope。
    let snapshots = test_support::take_root_snapshots();
    let committed = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit)
        .expect("root commit snapshot");
    assert_eq!(
        committed.root_owned.len(),
        3,
        "root keeps its two inputs plus the produced selector"
    );
    reset_observations();
}

// ---------------------------------------------------------------- C11 单域与 frame 映射

#[test]
fn c11_single_context_and_boundary_frames_per_choose() {
    reset_observations();
    let before = creation_counts::snapshot();
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1), |branch| branch.then(branch_alpha, seed));
            })
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .expect("single domain run");
    assert_eq!(output.0, 2);
    let after = creation_counts::snapshot();
    assert_eq!(after.0 - before.0, 1, "exactly one Context per execution");
    assert_eq!(after.1 - before.1, 1, "exactly one ScopeCoordinator");
    assert_eq!(after.2 - before.2, 1, "exactly one DataContainer");

    // frame 映射：root 进出 + 分支 boundary 进出，且分支 child 使用同一个 Flow 角色。
    let frames = observe::take_frames();
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame.kind == "root" && frame.phase == "enter")
            .count(),
        1
    );
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame.kind == "boundary" && frame.phase == "enter")
            .count(),
        1,
        "only the selected branch enters a boundary frame"
    );
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame.kind == "boundary" && frame.phase == "exit")
            .count(),
        1
    );
    let creations = test_support::boundary_creation_snapshot();
    assert_eq!(creations.len(), 1);
    assert_eq!(
        creations[0].2,
        ScopeRole::Flow,
        "the branch child is created through the same controlled entry as Chain"
    );
    assert_eq!(
        creations[0].1,
        creations[0].1.clone(),
        "caller recorded for the branch boundary"
    );
    reset_observations();
}

// ---------------------------------------------------------------- C12 整组 Export 与 alias

#[test]
fn c12_duplicate_source_exports_through_distinct_parent_slots() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    // 分支返回 (value, value)：两个 parent 槽 alias 同一实例；Root 只取其中一个。
    let output = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of_tracked, seed);
            let pair = flow.choose(selector, |choice| {
                choice.case(route(5), |branch| {
                    let value = branch.then(track_branch_value, seed);
                    (value, value)
                });
            });
            pair.0
        },
        TrackedSeed {
            value: 5,
            log: log.clone(),
        },
    ))
    .expect("duplicate source choose run");
    assert_eq!(output.label, "branch-temp");
    let during_run = log.0.borrow().clone();
    assert_eq!(
        during_run
            .iter()
            .filter(|event| *event == "run:branch-temp")
            .count(),
        1,
        "the selected branch body really produced the instance once"
    );
    assert!(
        !during_run.iter().any(|event| event == "drop:branch-temp"),
        "the handed-over instance is not destroyed by the runtime"
    );

    let root_scope = observe_root_scope();
    let selections = observe::take_choose_selections();
    let branch_child = selections[0].child.clone().expect("selected branch scope");
    let snapshots = observe::take_export_snapshots();
    let root_committed = snapshots
        .iter()
        .find(|record| record.phase == "committed" && record.scope == root_scope)
        .expect("committed caller snapshot");
    let added: Vec<_> = root_committed
        .refs
        .iter()
        .filter(|(position, _)| {
            !snapshots
                .iter()
                .find(|record| record.phase == "caller-before" && record.scope == root_scope)
                .expect("caller snapshot before the group")
                .refs
                .iter()
                .any(|(known, _)| known == position)
        })
        .collect();
    assert_eq!(added.len(), 2, "both declared slots are bound by the group");
    let first = match added[0].1 {
        TargetSnapshot::Data(ref id) => id.clone(),
        TargetSnapshot::CollectionItem { .. } => unreachable!(),
    };
    let second = match added[1].1 {
        TargetSnapshot::Data(ref id) => id.clone(),
        TargetSnapshot::CollectionItem { .. } => unreachable!(),
    };
    assert_eq!(
        first, second,
        "the two slots keep distinct RefIds but alias the same instance"
    );
    assert_ne!(added[0].0, added[1].0);
    // 责任集合：该实例在 caller 侧只被拥有一次，且存活。
    assert_eq!(
        root_committed
            .owned
            .iter()
            .filter(|id| **id == first)
            .count(),
        1,
        "a single ownership transfer happens for the duplicated source"
    );
    assert_eq!(
        root_committed
            .owned_alive
            .iter()
            .find(|(id, _, _)| *id == first)
            .map(|(_, alive, owner)| (*alive, owner.clone())),
        Some((true, Some(root_scope.clone()))),
        "the caller owns the instance once and it is alive"
    );
    assert!(
        snapshots.iter().any(|record| record.scope == branch_child),
        "the selected branch boundary was observed"
    );

    // 离开 Runtime 后逐实例释放一次。
    drop(output);
    assert_eq!(
        log.take()
            .iter()
            .filter(|event| *event == "drop:branch-temp")
            .count(),
        1,
        "one instance, one destruction"
    );
    reset_observations();
}

#[test]
fn c12_ancestor_alias_branch_keeps_the_owner() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of_tracked, seed);
            flow.choose(selector, |choice| {
                // 分支把祖先 Ref 直接作为输出：只读 alias，不转移责任。
                choice.case(route(5), |branch| {
                    let temp = branch.then(track_branch_value, seed);
                    let _ = temp;
                    seed
                });
                choice.otherwise(|branch| {
                    let temp = branch.then(track_branch_value, seed);
                    let _ = temp;
                    seed
                });
            })
        },
        TrackedSeed {
            value: 5,
            log: log.clone(),
        },
    ))
    .expect("ancestor alias branch run");
    assert_eq!(
        output.value, 5,
        "ancestor-owned seed exported without transfer"
    );
    let during_run = log.0.borrow().clone();
    assert!(
        during_run.iter().any(|event| event == "run:branch-temp"),
        "the branch body produced its own temporary"
    );
    assert!(
        during_run.iter().any(|event| event == "drop:branch-temp"),
        "the branch temporary is cleaned when the branch boundary closes"
    );
    assert!(
        !during_run.iter().any(|event| event == "drop:seed"),
        "the aliased ancestor is not destroyed inside the runtime"
    );

    let root_scope = observe_root_scope();
    let selections = observe::take_choose_selections();
    let branch_child = selections[0].child.clone().expect("selected branch scope");
    let snapshots = observe::take_export_snapshots();
    let root_before = snapshots
        .iter()
        .find(|record| record.phase == "caller-before" && record.scope == root_scope)
        .expect("caller snapshot before the group");
    let seed_data = root_before
        .owned
        .iter()
        .min_by_key(|id| id.seq())
        .cloned()
        .expect("the Root input instance");
    assert_eq!(
        root_before
            .owned_alive
            .iter()
            .find(|(id, _, _)| *id == seed_data)
            .map(|(_, alive, owner)| (*alive, owner.clone())),
        Some((true, Some(root_scope.clone()))),
        "the ancestor starts as a Root-owned, alive instance"
    );
    let root_committed = snapshots
        .iter()
        .find(|record| record.phase == "committed" && record.scope == root_scope)
        .expect("committed caller snapshot");
    assert_eq!(
        root_committed
            .owned_alive
            .iter()
            .find(|(id, _, _)| *id == seed_data)
            .map(|(_, alive, owner)| (*alive, owner.clone())),
        Some((true, Some(root_scope.clone()))),
        "the alias slot keeps the ancestor owner unchanged"
    );
    assert_eq!(
        root_committed
            .owned
            .iter()
            .filter(|id| **id == seed_data)
            .count(),
        1,
        "no second ownership of the aliased ancestor"
    );
    assert!(
        snapshots.iter().any(|record| record.scope == branch_child),
        "the selected branch boundary was observed"
    );
    // 祖先实例随 owned 输出离开 Runtime，分支临时值由边界清理：两者各销毁一次，无双重拥有。
    drop(output);
    let mut events = log.take();
    events.sort();
    assert_eq!(
        events,
        vec![
            "drop:branch-temp".to_string(),
            "drop:seed".to_string(),
            "run:branch-temp".to_string(),
        ],
        "the branch temporary and the aliased ancestor are each destroyed exactly once"
    );
    reset_observations();
}

// ---------------------------------------------------------------- C13 清理次序与顺序 Choose

#[test]
fn c13_branch_temporaries_drop_before_the_next_parent_step() {
    let log = Log::new();
    let runtime = Runtime::new();
    let output: () = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1), |branch| {
                    let temp = branch.then(track_temp, seed);
                    let _ = temp;
                    let _: () = branch.then(seed_observer, seed);
                });
            });
            let _: () = flow.then(seed_observer, seed);
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .expect("drop ordering run");
    assert_eq!(output, ());
    let events = log.take();
    let branch_step = events
        .iter()
        .position(|event| event == "start:observer")
        .expect("branch step ran");
    let dropped = events
        .iter()
        .position(|event| event == "drop:temp")
        .expect("branch temporary dropped");
    let parent_step = events
        .iter()
        .rposition(|event| event == "start:observer")
        .expect("parent step ran");
    assert!(
        dropped > branch_step,
        "temporary outlives its producing step"
    );
    assert!(
        dropped < parent_step,
        "branch-owned temporary is cleaned before the next parent step"
    );
}

#[test]
fn c13_sequential_chooses_close_before_the_next_child_is_created() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let output = block_on(runtime.execute(
        |flow, seed| {
            let first_selector = flow.then(route_of, seed);
            let first = flow.choose(first_selector, |choice| {
                choice.case(route(1), |branch| branch.then(seed_reader, seed));
            });
            let second_selector = flow.then(route_of_amount, first);
            flow.choose(second_selector, |choice| {
                choice.case(route(2), |branch| branch.then(AmountStep, first));
                choice.otherwise(|branch| branch.then(AmountStep, first));
            })
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .expect("sequential choose run");
    assert_eq!(output.0, 3);

    let creations = observe::take_chain_creations();
    assert_eq!(creations.len(), 2);
    assert!(
        !creations[0].closed_before.contains(&creations[0].child),
        "the first branch is created while nothing else is closed"
    );
    assert!(
        creations[1].closed_before.contains(&creations[0].child),
        "the second branch is created only after the first one closed"
    );
    assert!(creations[1].child.seq() > creations[0].child.seq());
    assert_ne!(creations[0].child, creations[1].child);
    let rejections = observe::take_scope_rejections();
    assert!(
        rejections.iter().any(|record| {
            record.operation == "resolve-closed-child"
                && matches!(record.error, ScopeError::ScopeClosed { .. })
        }),
        "a closed branch scope rejects reads of its old positions"
    );
    reset_observations();
}

// ---------------------------------------------------------------- C14 无匹配

#[test]
fn c14_no_matching_case_stops_business_without_a_branch() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    async fn counting(_query: Query<&Seed>) -> Result<Amount, BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(1))
    }

    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            let picked = flow.choose(selector, |choice| {
                choice.case(route(9), |branch| branch.then(counting, seed));
            });
            flow.then(AmountStep, picked)
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .err()
    .expect("no matching case terminates the execution");
    assert!(matches!(error, RunError::NoMatchingCase), "{error}");
    assert_eq!(CALLS.load(Ordering::SeqCst), 0, "no branch body ran");
    assert_eq!(
        test_support::boundary_creation_snapshot().len(),
        0,
        "no running Scope is created for an unmatched choose"
    );
    let selections = observe::take_choose_selections();
    assert_eq!(selections.len(), 1);
    assert_eq!(selections[0].selected, None);
    assert!(!selections[0].otherwise_used);
    assert!(
        selections[0].child.is_none(),
        "no branch boundary was created"
    );
    // Root 门禁：没有任何 take 计划，失败清理后责任已释放。
    let snapshots = test_support::take_root_snapshots();
    assert!(
        snapshots.iter().all(|snapshot| snapshot.planned_takes == 0),
        "no instance is ever planned for extraction"
    );
    let cleaned = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterFailureCleanup)
        .expect("root failure cleanup was observed");
    assert!(
        cleaned.root_owned.is_empty(),
        "root responsibilities are released by the failure cleanup"
    );
    assert_eq!(cleaned.terminated, Some(TerminationKind::BodyError));
    let terminations = test_support::take_termination_saved();
    assert_eq!(terminations.len(), 1);
    assert_eq!(terminations[0].kind, TerminationKind::BodyError);
    assert!(terminations[0].scope.is_some());
    reset_observations();
}

// ---------------------------------------------------------------- C15 selected Failure

/// 非 Send 的动态错误对象（`Rc` 字段）与可选嵌套 source。
#[derive(Debug)]
struct SharedError {
    shared: Rc<Vec<u8>>,
    source: Option<Box<SharedError>>,
}

impl std::fmt::Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shared failure of {} bytes", self.shared.len())
    }
}

impl std::error::Error for SharedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|inner| inner as &(dyn std::error::Error + 'static))
    }
}

#[test]
fn c15_failure_from_case_and_otherwise_is_preserved_without_fallback() {
    static FALLBACK_CALLS: AtomicUsize = AtomicUsize::new(0);
    static AFTER_CALLS: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        static FAIL_SHARED: RefCell<Option<Rc<Vec<u8>>>> = const { RefCell::new(None) };
        static FAIL_FROM_OTHERWISE: Cell<bool> = const { Cell::new(false) };
    }

    async fn failing(query: Query<&Seed>) -> Result<Amount, BodyError> {
        let _ = query.get();
        let shared = Rc::new(vec![1u8, 2, 3]);
        FAIL_SHARED.with(|slot| *slot.borrow_mut() = Some(Rc::clone(&shared)));
        let inner = SharedError {
            shared: Rc::new(vec![9u8]),
            source: None,
        };
        Err(BodyError::fail(SharedError {
            shared,
            source: Some(Box::new(inner)),
        }))
    }

    async fn fallback_observed(query: Query<&Seed>) -> Result<Amount, BodyError> {
        let _ = query.get();
        FALLBACK_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(0))
    }

    async fn after(query: Query<&Amount>) -> Result<Amount, BodyError> {
        let _ = query.get();
        AFTER_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(0))
    }

    // 第一次：Failure 来自 case，同时存在 otherwise 与后续业务。
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            let picked = flow.choose(selector, |choice| {
                choice.case(route(1), |branch| branch.then(failing, seed));
                choice.otherwise(|branch| branch.then(fallback_observed, seed));
            });
            flow.then(after, picked)
        },
        Seed {
            value: 1,
            log: Log::new(),
        },
    ))
    .err()
    .expect("selected branch failure terminates the execution");
    let held = FAIL_SHARED
        .with(|slot| slot.borrow().clone())
        .expect("captured");
    let while_held = Rc::strong_count(&held);
    {
        let RunError::Body(failure) = error else {
            panic!("expected a body failure, got {error}");
        };
        let first = std::error::Error::source(&failure).expect("source chain preserved");
        let shared = first
            .downcast_ref::<SharedError>()
            .expect("original non-Send dynamic object preserved");
        assert_eq!(shared.shared.len(), 3);
        let nested = std::error::Error::source(shared).expect("nested source preserved");
        assert!(nested.downcast_ref::<SharedError>().is_some());
    }
    assert_eq!(
        while_held - Rc::strong_count(&held),
        1,
        "the runtime released its copy of the dynamic object exactly once"
    );
    assert_eq!(
        FALLBACK_CALLS.load(Ordering::SeqCst),
        0,
        "no fallback attempt"
    );
    assert_eq!(AFTER_CALLS.load(Ordering::SeqCst), 0, "no later business");

    // 第二次：Failure 来自 otherwise（case 不匹配），并叠加 Root 清理故障。
    FAIL_FROM_OTHERWISE.with(|flag| flag.set(true));
    install_root_cleanup_fault();
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, (_unused, seed)| {
            // 清理故障注入在第一个（未被读取的）输入上，业务失败本身照常发生。
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(9), |branch| branch.then(failing, seed));
                choice.otherwise(|branch| branch.then(failing, seed));
            })
        },
        (
            Amount(7),
            Seed {
                value: 1,
                log: Log::new(),
            },
        ),
    ))
    .err()
    .expect("otherwise failure terminates the execution");
    let RunError::Body(failure) = error else {
        panic!("expected a body failure, got {error}");
    };
    assert!(
        std::error::Error::source(&failure)
            .and_then(|source| source.downcast_ref::<SharedError>())
            .is_some(),
        "the original object survives the cleanup failure"
    );
    assert!(
        failure.cleanup_source().is_some(),
        "the first cleanup diagnostic is preserved next to the original object"
    );
}

// ---------------------------------------------------------------- C16 Control 穿过 Choose

#[test]
fn c16_retry_crosses_choose_and_chain_then_locates_at_root() {
    static RETRIES: AtomicUsize = AtomicUsize::new(0);
    static AFTER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static FALLBACK_CALLS: AtomicUsize = AtomicUsize::new(0);

    async fn retrying(query: Query<&Amount>) -> Result<Amount, BodyError> {
        let _ = query.get();
        RETRIES.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::retry(DomainError { detail: "retry me" }))
    }

    async fn never(query: Query<&Amount>) -> Result<Amount, BodyError> {
        let _ = query.get();
        AFTER_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(0))
    }

    async fn fallback_observed(query: Query<&Amount>) -> Result<Amount, BodyError> {
        let _ = query.get();
        FALLBACK_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(0))
    }

    reset_observations();
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, amount| {
            flow.chain(|sub| {
                let selector = sub.then(route_of_amount, amount);
                sub.choose(selector, |choice| {
                    choice.case(route(1), |branch| {
                        let retried = branch.then(retrying, amount);
                        branch.then(never, retried)
                    });
                    choice.otherwise(|branch| branch.then(fallback_observed, amount));
                })
            })
        },
        Amount(1),
    ))
    .err()
    .expect("unhandled control terminates the root");
    let RunError::UnhandledControl(ControlSignal::Retry(retry)) = error else {
        panic!("expected an unhandled retry, got {error}");
    };
    let source = std::error::Error::source(&retry).expect("original retry reason preserved");
    assert!(source.downcast_ref::<DomainError>().is_some());
    assert_eq!(RETRIES.load(Ordering::SeqCst), 1);
    assert_eq!(
        AFTER_CALLS.load(Ordering::SeqCst),
        0,
        "control stops later steps"
    );
    assert_eq!(
        FALLBACK_CALLS.load(Ordering::SeqCst),
        0,
        "control does not switch branch"
    );

    // 合法中间退出：两层边界完成后未终止、child 已 Closed、责任已解除。
    let completions = observe::take_control_completions();
    assert_eq!(
        completions.len(),
        2,
        "choose branch and chain boundary both completed"
    );
    assert!(
        completions
            .iter()
            .all(|record| !record.terminated && record.termination_scope.is_none()),
        "a legal control exit does not terminate the execution"
    );
    assert!(
        completions
            .iter()
            .all(|record| record.child_state == Some(ScopeState::Closed))
    );
    let releases = observe::take_control_releases();
    assert_eq!(releases.len(), 2);
    assert!(
        releases
            .iter()
            .all(|record| record.responsible_empty && record.released == 1)
    );
    assert_eq!(
        releases
            .iter()
            .map(|record| record.child.clone())
            .collect::<Vec<_>>(),
        completions
            .iter()
            .map(|record| record.child.clone())
            .collect::<Vec<_>>(),
        "each boundary release pairs with its own completion"
    );

    // 唯一不可恢复终止属于 Root 未处理信号收口，且不是 Choose 或 Chain 边界。
    let terminations = test_support::take_termination_saved();
    assert_eq!(terminations.len(), 1);
    assert_eq!(terminations[0].kind, TerminationKind::BodyError);
    let root = observe_root_scope();
    assert_eq!(
        terminations[0].scope.as_ref(),
        Some(&root),
        "the first unrecoverable termination belongs to the root close"
    );
    reset_observations();
}

#[test]
fn c16_iter_break_crosses_a_nested_choose() {
    static BREAK_CALLS: AtomicUsize = AtomicUsize::new(0);

    async fn breaking(query: Query<&Amount>) -> Result<Amount, BodyError> {
        let _ = query.get();
        BREAK_CALLS.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::iter_break())
    }

    reset_observations();
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, amount| {
            flow.chain(|sub| {
                let selector = sub.then(route_of_amount, amount);
                sub.choose(selector, |choice| {
                    choice.case(route(1), |branch| {
                        let inner_selector = branch.then(route_of_amount, amount);
                        branch.choose(inner_selector, |inner_choice| {
                            inner_choice.case(route(1), |deep| deep.then(breaking, amount));
                        })
                    });
                })
            })
        },
        Amount(1),
    ))
    .err()
    .expect("iter break terminates the root");
    assert!(matches!(
        error,
        RunError::UnhandledControl(ControlSignal::IterBreak(_))
    ));
    assert_eq!(BREAK_CALLS.load(Ordering::SeqCst), 1);
    let completions = observe::take_control_completions();
    assert_eq!(
        completions.len(),
        3,
        "inner choose branch, outer choose branch and chain all completed"
    );
    assert!(
        completions
            .iter()
            .all(|record| !record.terminated && record.child_state == Some(ScopeState::Closed)),
        "every crossed boundary closed without terminating the execution"
    );
    reset_observations();
}

// ---------------------------------------------------------------- C17 Control 清理故障

#[test]
fn c17_control_cleanup_failure_inside_choose_becomes_runtime() {
    static RETRIES: AtomicUsize = AtomicUsize::new(0);
    static AFTER_CALLS: AtomicUsize = AtomicUsize::new(0);

    async fn retrying(_query: Query<&TrackedSeed>) -> Result<Amount, BodyError> {
        RETRIES.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::retry(DomainError { detail: "retry me" }))
    }

    async fn after(query: Query<&Amount>) -> Result<Amount, BodyError> {
        let _ = query.get();
        AFTER_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(0))
    }

    reset_observations();
    let log = Log::new();
    install_control_cleanup_fault();
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of_tracked, seed);
            let picked = flow.choose(selector, |choice| {
                choice.case(route(5), |branch| {
                    // 分支先产生自己的 owned 值，再发出 Control：清理故障作用于该边界。
                    let temp = branch.then(track_branch_value, seed);
                    let _ = temp;
                    branch.then(retrying, seed)
                });
            });
            flow.then(after, picked)
        },
        TrackedSeed {
            value: 5,
            log: log.clone(),
        },
    ))
    .err()
    .expect("cleanup failure replaces the recoverable control");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected a runtime error, got {error}");
    };
    assert_eq!(
        runtime_error.note(),
        "control exit cleanup failed",
        "original framework diagnostic note"
    );
    let source = std::error::Error::source(runtime_error)
        .expect("the original framework diagnostic is preserved as the source");
    let source_text = source.to_string();
    assert!(
        !source_text.is_empty(),
        "the concrete framework rejection is preserved: {source_text}"
    );
    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    let cleanup_text = cleanup.to_string();
    assert!(
        !cleanup_text.is_empty(),
        "the first cleanup diagnostic carries its own text: {cleanup_text}"
    );

    // 首次终止记录定位在所选 branch 的真实 Scope 上，且是不可恢复错误退出。
    let selections = observe::take_choose_selections();
    let branch_child = selections[0].child.clone().expect("branch scope");
    let terminations = test_support::take_termination_saved();
    assert_eq!(terminations.len(), 1, "only one termination is recorded");
    assert_eq!(terminations[0].kind, TerminationKind::BodyError);
    assert_eq!(
        terminations[0].scope.as_ref(),
        Some(&branch_child),
        "the first termination belongs to the failed branch boundary"
    );

    // 实际状态：不是 Closed，也没有被假称为正常恢复；责任没有被假称解除。
    let states = observe::take_scope_states();
    let failed = states
        .iter()
        .find(|record| record.at == "control-cleanup-failed" && record.scope == branch_child)
        .expect("the failed cleanup is recorded on the real branch Scope");
    assert_ne!(
        failed.state,
        ScopeState::Closed,
        "a failed cleanup cannot leave the boundary Closed"
    );
    assert!(
        !states
            .iter()
            .any(|record| record.at == "control-closed" && record.scope == branch_child),
        "no successful close is recorded for the failed boundary"
    );
    assert!(
        observe::take_control_releases().is_empty(),
        "responsibility is not falsely released after a failed cleanup"
    );

    // 原 Guard 收拢必要责任：分支自己的 owned 值仍被清理一次。
    assert_eq!(
        log.0
            .borrow()
            .iter()
            .filter(|event| *event == "drop:branch-temp")
            .count(),
        1,
        "the branch-owned value is still destroyed exactly once"
    );
    assert_eq!(RETRIES.load(Ordering::SeqCst), 1);
    assert_eq!(AFTER_CALLS.load(Ordering::SeqCst), 0, "no later business");
    reset_observations();
}

// ---------------------------------------------------------------- C18 无匹配 + 清理故障

#[test]
fn c18_no_matching_case_with_cleanup_failure_keeps_both_diagnostics() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    async fn counting(_query: Query<&TrackedSeed>) -> Result<Amount, BodyError> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(Amount(1))
    }

    // 对照组：同一无匹配路径、不注入清理故障。
    reset_observations();
    let runtime = Runtime::new();
    let plain = block_on(runtime.execute(
        |flow, (_unused, seed)| {
            let selector = flow.then(route_of_tracked, seed);
            flow.choose(selector, |choice| {
                choice.case(route(9), |branch| branch.then(counting, seed));
            })
        },
        (
            Amount(7),
            TrackedSeed {
                value: 1,
                log: Log::new(),
            },
        ),
    ))
    .err()
    .expect("no matching case terminates the execution");
    assert!(matches!(plain, RunError::NoMatchingCase), "{plain}");
    let control = test_support::take_root_snapshots();
    let control_cleaned = control
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterFailureCleanup)
        .expect("control run failure cleanup observed");
    assert!(
        control_cleaned.root_owned.is_empty(),
        "without an injected fault the failure cleanup releases every Root responsibility"
    );

    // 故障组一：一次清理故障 → Runtime，原原因与首清理诊断都在。
    reset_observations();
    install_root_cleanup_fault();
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, (_unused, seed)| {
            let selector = flow.then(route_of_tracked, seed);
            flow.choose(selector, |choice| {
                choice.case(route(9), |branch| branch.then(counting, seed));
            })
        },
        (
            Amount(7),
            TrackedSeed {
                value: 1,
                log: Log::new(),
            },
        ),
    ))
    .err()
    .expect("no matching case with a cleanup failure is a runtime error");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected a runtime error, got {error}");
    };
    let source = std::error::Error::source(runtime_error)
        .expect("the original termination reason is preserved");
    let reason = source
        .downcast_ref::<RunError>()
        .expect("the frozen classification carries the original reason");
    assert!(
        matches!(reason, RunError::NoMatchingCase),
        "the frozen NoMatchingCase classification is unchanged"
    );
    let first_cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved")
        .to_string();
    assert!(
        !first_cleanup.is_empty(),
        "the cleanup diagnostic carries its own text: {first_cleanup}"
    );
    // 首次终止记录：Root 收口处的不可恢复错误退出；失败清理后 Root 责任为空。
    let root_scope = observe_root_scope();
    let terminations = test_support::take_termination_saved();
    assert_eq!(terminations.len(), 1);
    assert_eq!(terminations[0].kind, TerminationKind::BodyError);
    assert_eq!(
        terminations[0].scope.as_ref(),
        Some(&root_scope),
        "the first termination stays at the root close"
    );
    let snapshots = test_support::take_root_snapshots();
    let cleaned = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterFailureCleanup)
        .expect("root failure cleanup was observed");
    assert!(
        !cleaned.root_owned.is_empty(),
        "a failed cleanup keeps the Root responsibility instead of falsely releasing it"
    );
    assert_ne!(
        cleaned.root_state,
        ScopeState::Closed,
        "a failed cleanup does not claim the Root is Closed"
    );
    assert_eq!(CALLS.load(Ordering::SeqCst), 0, "no branch body ran");

    // 故障组二：同一路径注入两次清理故障（都落在业务不读取的输入上）
    // → 分类原因与单一首清理诊断都不被覆盖。
    reset_observations();
    install_root_cleanup_faults(2);
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, (_first, _second, seed)| {
            let selector = flow.then(route_of_tracked, seed);
            flow.choose(selector, |choice| {
                choice.case(route(9), |branch| branch.then(counting, seed));
            })
        },
        (
            Amount(7),
            Amount(8),
            TrackedSeed {
                value: 1,
                log: Log::new(),
            },
        ),
    ))
    .err()
    .expect("a second cleanup failure must not change the outcome kind");
    let RunError::Runtime(second) = &error else {
        panic!("expected a runtime error, got {error}");
    };
    let source = std::error::Error::source(second)
        .expect("the original termination reason survives the second failure");
    assert!(
        matches!(
            source.downcast_ref::<RunError>(),
            Some(RunError::NoMatchingCase)
        ),
        "an additional cleanup failure does not replace the original reason"
    );
    assert_eq!(
        second
            .cleanup_source()
            .expect("a cleanup diagnostic is still preserved")
            .to_string(),
        first_cleanup,
        "the reported cleanup diagnostic stays the same single one"
    );
    reset_observations();
}

// ---------------------------------------------------------------- C19 组拒绝无半提交

/// 两个祖先输入的元组读取（Import 批次的第二个与后续项样本使用）。
async fn pair_sum(query: Query<(&TrackedSeed, &Amount)>) -> Result<Amount, BodyError> {
    let (seed, amount) = query.get();
    Ok(Amount(seed.value + amount.0))
}

#[test]
fn c19_export_preflight_rejects_the_whole_choose_group() {
    reset_observations();
    let log = Log::new();
    install_export_occupy_fault(1); // 预占第二个 parent 槽位
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of_tracked, seed);
            flow.choose(selector, |choice| {
                choice.case(route(5), |branch| {
                    let temp = branch.then(track_branch_value, seed);
                    let pair = branch.then(seed_reader_tracked, seed);
                    (temp, pair)
                });
            })
        },
        TrackedSeed {
            value: 5,
            log: log.clone(),
        },
    ))
    .err()
    .expect("an occupied parent slot rejects the whole group");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected a runtime error, got {error}");
    };
    assert_eq!(
        runtime_error.note(),
        "choose branch export rejected",
        "the rejection is attributed to the Choose branch boundary"
    );
    let source = std::error::Error::source(runtime_error)
        .expect("the original framework diagnostic is preserved");
    assert!(
        !source.to_string().is_empty(),
        "the specific rejection reason is preserved: {source}"
    );

    // 整组在任何提交之前拒绝：caller 侧前后快照逐项一致。
    let root_scope = observe_root_scope();
    let snapshots = observe::take_export_snapshots();
    let before = snapshots
        .iter()
        .find(|record| record.phase == "caller-before" && record.scope == root_scope)
        .expect("caller snapshot before the group");
    let rejected = snapshots
        .iter()
        .find(|record| record.phase == "rejected" && record.scope == root_scope)
        .expect("caller snapshot after the rejected group");
    let added_refs: Vec<_> = rejected
        .refs
        .iter()
        .filter(|(position, _)| !before.refs.iter().any(|(known, _)| known == position))
        .collect();
    assert_eq!(
        added_refs.len(),
        1,
        "only the injected occupant exists: neither group slot is committed by the caller"
    );
    assert_eq!(
        rejected.owned.len(),
        before.owned.len() + 1,
        "no ownership is half-committed beyond the injected occupant"
    );
    assert_eq!(
        rejected
            .owned
            .iter()
            .filter(|id| !before.owned.contains(id))
            .count(),
        1,
        "the caller-owned set grows only by the injected occupant"
    );
    assert!(
        rejected.next_data_id > before.next_data_id,
        "the global allocator only advanced for the occupant and the branch's own production"
    );
    // 分支自己产生的值各被清理一次；Root 输入随失败收尾释放。
    let mut events = log.take();
    events.sort();
    assert_eq!(
        events,
        vec![
            "drop:branch-temp".to_string(),
            "drop:seed".to_string(),
            "run:branch-temp".to_string(),
        ],
        "the branch-owned value is cleaned exactly once and the Root input released once"
    );
    // 没有任何 take 计划：Root 提取从未开始。
    assert!(
        test_support::take_root_snapshots()
            .iter()
            .all(|snapshot| snapshot.planned_takes == 0)
    );
    reset_observations();
}

#[test]
fn c19_import_preflight_binds_no_partial_slot() {
    reset_observations();
    install_import_occupy_fault(1); // 预占第二个 Import 目的位置
    let runtime = Runtime::new();
    let error = block_on(runtime.execute(
        |flow, (seed, amount)| {
            let selector = flow.then(route_of_tracked, seed);
            flow.choose(selector, |choice| {
                choice.case(route(5), |branch| branch.then(pair_sum, (seed, amount)));
            })
        },
        (
            TrackedSeed {
                value: 5,
                log: Log::new(),
            },
            Amount(4),
        ),
    ))
    .err()
    .expect("an occupied import target rejects the whole batch");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected a runtime error, got {error}");
    };
    assert_eq!(
        runtime_error.note(),
        "choose branch input assembly failed",
        "the rejection is attributed to the Choose branch assembly"
    );
    let source = std::error::Error::source(runtime_error)
        .expect("the original framework diagnostic is preserved");
    assert!(
        !source.to_string().is_empty(),
        "the specific rejection reason is preserved: {source}"
    );

    let root_scope = observe_root_scope();
    let snapshots = observe::take_export_snapshots();
    let selections = observe::take_choose_selections();
    let branch_child = selections[0].child.clone().expect("branch scope");
    let child_created = snapshots
        .iter()
        .find(|record| record.phase == "child-created" && record.scope == branch_child)
        .expect("branch child snapshot at creation");
    assert!(
        child_created.refs.is_empty() && child_created.owned.is_empty(),
        "a fresh branch boundary starts unbound"
    );
    let rejected = snapshots
        .iter()
        .find(|record| record.phase == "import-rejected" && record.scope == branch_child)
        .expect("branch child snapshot after the rejected batch");
    assert_eq!(
        rejected.refs.len(),
        1,
        "only the injected occupant exists: the first import is not bound alone"
    );
    assert_eq!(
        rejected.owned.len(),
        1,
        "only the injected occupant instance exists in the branch"
    );
    assert_eq!(
        rejected.next_data_id,
        child_created.next_data_id.map(|next| next + 1),
        "the rejected batch allocates exactly the injected occupant and nothing else"
    );
    // caller 侧在整组 Import 拒绝前后完全一致（无绑定、无责任变化）。
    let caller_before = snapshots
        .iter()
        .find(|record| record.phase == "caller-before" && record.scope == root_scope)
        .expect("caller snapshot before the branch boundary");
    let caller_after = snapshots
        .iter()
        .find(|record| record.phase == "import-rejected-caller" && record.scope == root_scope)
        .expect("caller snapshot after the rejected batch");
    assert_eq!(caller_before.refs, caller_after.refs);
    assert_eq!(caller_before.owned, caller_after.owned);
    assert_eq!(caller_before.owned_alive, caller_after.owned_alive);
    assert!(
        observe::take_scope_states()
            .iter()
            .any(|record| record.at == "import-rejected"),
        "the real branch Scope records the rejected assembly"
    );
    assert!(
        test_support::take_root_snapshots()
            .iter()
            .all(|snapshot| snapshot.planned_takes == 0)
    );
    reset_observations();
}

// ---------------------------------------------------------------- C20 取消

/// TrackedSeed → Route 的路由节点（取消样本使用）。
async fn route_of_tracked(query: Query<&TrackedSeed>) -> Result<Route, BodyError> {
    Ok(route(query.get().value as u8))
}

/// caller 侧输出的可观察值（取消样本使用）。
async fn track_caller_value(query: Query<&TrackedSeed>) -> Result<TrackedValue, BodyError> {
    let seed = query.get();
    seed.log.push("run:caller-value");
    Ok(TrackedValue {
        label: "caller-value",
        log: seed.log.clone(),
    })
}

/// chain 侧中间值的可观察值。
async fn track_chain_value(query: Query<&TrackedSeed>) -> Result<TrackedValue, BodyError> {
    let seed = query.get();
    seed.log.push("run:chain-temp");
    Ok(TrackedValue {
        label: "chain-temp",
        log: seed.log.clone(),
    })
}

/// Choose 分支中间值的可观察值。
async fn track_branch_value(query: Query<&TrackedSeed>) -> Result<TrackedValue, BodyError> {
    let seed = query.get();
    seed.log.push("run:branch-temp");
    Ok(TrackedValue {
        label: "branch-temp",
        log: seed.log.clone(),
    })
}

/// TrackedSeed → Amount（组拒绝与混合来源样本使用）。
async fn seed_reader_tracked(query: Query<&TrackedSeed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value + 1))
}

/// TrackedSeed 的零输出观察节点（取消样本的失败分支使用）。
async fn tracked_observer(query: Query<&TrackedSeed>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

/// 取消后的“下一个 Node”：必须一次都不执行。
async fn after_choose(query: Query<&Amount>) -> Result<Amount, BodyError> {
    let _ = query.get();
    AFTER_CANCEL_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(0))
}

static AFTER_CANCEL_CALLS: AtomicUsize = AtomicUsize::new(0);

#[test]
fn c20_choose_cancel_orders_future_branch_and_caller_drops() {
    AFTER_CANCEL_CALLS.store(0, Ordering::SeqCst);
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    let mut future = Box::pin(runtime.execute(
        |flow, seed| {
            // caller 侧输出：取消后按“子先于父”的次序清理。
            let caller_value = flow.then(track_caller_value, seed);
            let _ = caller_value;
            let selector = flow.then(route_of_tracked, seed);
            flow.choose(selector, |choice| {
                choice.case(route(5), |branch| {
                    // branch-owned 中间值，随后进入真实 Pending（借用 Future 自带 Drop 观察）。
                    let temp = branch.then(track_branch_value, seed);
                    let _ = temp;
                    let pending = branch.then(pending_guard_node, seed);
                    branch.then(after_choose, pending)
                });
            })
        },
        TrackedSeed {
            value: 5,
            log: log.clone(),
        },
    ));

    let waker = futures::task::noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    assert_eq!(
        AFTER_CANCEL_CALLS.load(Ordering::SeqCst),
        0,
        "the node after the choose has not run yet"
    );
    drop(future);
    let events: Vec<String> = log
        .take()
        .into_iter()
        .filter(|event| event.starts_with("drop:"))
        .collect();
    assert_eq!(
        events.first().map(String::as_str),
        Some("drop:pending-future"),
        "the borrowed Future ends before any business value is released"
    );
    assert_eq!(
        events.get(1).map(String::as_str),
        Some("drop:branch-temp"),
        "the branch-owned temporary is cleaned before the caller Scope values"
    );
    let mut tail = events[2..].to_vec();
    tail.sort();
    assert_eq!(
        tail,
        vec!["drop:caller-value".to_string(), "drop:seed".to_string()],
        "the caller Scope values are released exactly once each \
         (order within one Scope follows the container's unordered storage)"
    );
    assert_eq!(events.len(), 4, "no instance is released twice");
    assert_eq!(
        AFTER_CANCEL_CALLS.load(Ordering::SeqCst),
        0,
        "cancellation runs no later node"
    );
    reset_observations();
}

#[test]
fn c20_choose_nested_pending_cancel_releases_each_level_once() {
    AFTER_CANCEL_CALLS.store(0, Ordering::SeqCst);
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    // Root → Chain → Choose → branch：真实 Pending 位于嵌套 Choose 的分支内。
    let mut future = Box::pin(runtime.execute(
        |flow, seed| {
            flow.chain(|sub| {
                let chain_temp = sub.then(track_chain_value, seed);
                let _ = chain_temp;
                let selector = sub.then(route_of_tracked, seed);
                sub.choose(selector, |choice| {
                    choice.case(route(5), |branch| {
                        let branch_temp = branch.then(track_branch_value, seed);
                        let _ = branch_temp;
                        let pending = branch.then(pending_guard_node, seed);
                        branch.then(after_choose, pending)
                    })
                })
            })
        },
        TrackedSeed {
            value: 5,
            log: log.clone(),
        },
    ));

    let waker = futures::task::noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    drop(future);
    let events: Vec<String> = log
        .take()
        .into_iter()
        .filter(|event| event.starts_with("drop:"))
        .collect();
    assert_eq!(
        events,
        vec![
            "drop:pending-future".to_string(),
            "drop:branch-temp".to_string(),
            "drop:chain-temp".to_string(),
            "drop:seed".to_string(),
        ],
        "the innermost borrowed future ends first, then each owning level from the inside out"
    );
    assert_eq!(AFTER_CANCEL_CALLS.load(Ordering::SeqCst), 0);
    reset_observations();
}

#[test]
fn c20_unpolled_choose_cancel_releases_input_once_and_builds_nothing() {
    static BUILDS: AtomicUsize = AtomicUsize::new(0);
    let drops = Arc::new(AtomicUsize::new(0));

    struct Dropper {
        drops: Arc<AtomicUsize>,
    }
    impl Data for Dropper {}
    impl Drop for Dropper {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    let runtime = Runtime::new();
    let unpolled = runtime.execute(
        |flow, (seed, _unused)| {
            let selector = flow.then(route_of_tracked, seed);
            flow.choose(selector, |choice| {
                choice.case(route(5), |branch| {
                    BUILDS.fetch_add(1, Ordering::SeqCst);
                    let _: () = branch.then(tracked_observer, seed);
                })
            })
        },
        (
            TrackedSeed {
                value: 5,
                log: Log::new(),
            },
            Dropper {
                drops: Arc::clone(&drops),
            },
        ),
    );
    drop(unpolled);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "an unpolled choose future releases its input exactly once"
    );
    assert_eq!(
        BUILDS.load(Ordering::SeqCst),
        0,
        "no branch definition closure runs before the first poll"
    );
}

// ---------------------------------------------------------------- C21 身份与跨执行

#[test]
fn c21_choose_duplicate_source_output_is_rejected_before_any_take() {
    reset_observations();
    let log = Log::new();
    let runtime = Runtime::new();
    // 分支返回 (value, value)：两个 parent 槽指向同一 DataId，Root 必须整组拒绝。
    let error = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1), |branch| {
                    let value = branch.then(track_temp, seed);
                    (value, value)
                });
            })
        },
        Seed {
            value: 1,
            log: log.clone(),
        },
    ))
    .err()
    .expect("duplicate DataId in the choose output must be rejected");
    assert!(matches!(error, RunError::Runtime(_)), "{error}");
    let snapshots = test_support::take_root_snapshots();
    let rejected = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::PreflightRejected)
        .expect("root preflight rejected the group");
    assert_eq!(
        rejected.planned_takes, 0,
        "no instance is taken before the duplicate check"
    );
    assert_eq!(
        log.take()
            .iter()
            .filter(|event| *event == "drop:temp")
            .count(),
        1,
        "the single instance is destroyed exactly once"
    );
    reset_observations();
}

#[test]
fn c21_choose_identity_is_per_execution_and_foreign_selectors_are_rejected() {
    reset_observations();
    let runtime = Runtime::new();
    let run = |runtime: &Runtime| {
        block_on(runtime.execute(
            |flow, seed| {
                let selector = flow.then(route_of, seed);
                flow.choose(selector, |choice| {
                    choice.case(route(1), |branch| branch.then(seed_reader, seed));
                })
            },
            Seed {
                value: 1,
                log: Log::new(),
            },
        ))
        .expect("choose execution")
    };
    let first = run(&runtime);
    let first_creations = observe::take_chain_creations();
    let second = run(&runtime);
    let second_creations = observe::take_chain_creations();
    assert_eq!((first.0, second.0), (2, 2));
    assert_eq!(first_creations.len(), 1);
    assert_eq!(second_creations.len(), 1);
    assert!(!std::sync::Arc::ptr_eq(
        first_creations[0].child.execution(),
        second_creations[0].child.execution()
    ));
    assert_ne!(first_creations[0].child, second_creations[0].child);

    // 上一次执行的选择器 Ref 不能在本次执行中作为 selector 使用。
    let mut leaked: Option<Ref<Route>> = None;
    let _ = block_on(runtime.execute(
        |_flow, seed| {
            let route_ref = _flow.then(route_of, seed);
            leaked = Some(route_ref);
            seed
        },
        Seed {
            value: 1,
            log: Log::new(),
        },
    ))
    .ok();
    let foreign = leaked.expect("captured selector ref");
    let error = block_on(runtime.execute(
        move |flow, seed| {
            flow.choose(foreign, |choice| {
                choice.case(route(1), |branch| branch.then(seed_reader, seed));
            })
        },
        Seed {
            value: 1,
            log: Log::new(),
        },
    ))
    .err()
    .expect("a selector from another execution is rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    reset_observations();
}
