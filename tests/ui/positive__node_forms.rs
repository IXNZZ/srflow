//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_p02_node_forms.rs`）：Node 形式与 Query 形状（SPEC §5）。
//!
//! 原目的（X24/X04/S02／C13／C15）：函数 item、函数引用、async closure、借用 Struct Node
//! （非 `'static` 配置）、owned Struct Node、`Arc<具体 Struct Node>`、unit 输入 Node、多输入
//! Struct Node、非 Send 数据字段（`Rc`），以及「future 内跨真实 await 持有输入只读借用」；
//! 断言 `report = 223`、非 Send `sum = 44` 与真实 Pending 轮询 2 次。
//!
//! 迁移手法：旧两参 `flow.then(node, refs)` 全部改为取输出的显式连接 `flow.then_with_output`；
//! 只有 Root 闭包改为固定返回 `()` 并经 `output_with` 声明 owned `Out`（分别为 `Report`、
//! `NonSendSum`）。Node 形式矩阵与断言不变。
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Node, Query, Ref, Runtime};

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Report(u32);

/// 非 Send／非 Sync 业务数据：字段含 `Rc`（手写 marker，与派生等价）。
struct NonSendInput {
    value: u32,
    tag: Rc<RefCell<String>>,
}

impl Data for NonSendInput {}

#[derive(Data)]
struct NonSendSum(u32);

/// 真实 Pending：首次 poll 返回 Pending 并自唤醒，第二次完成；不用睡眠替代。
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

/// 函数 item。
async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

async fn plus_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

async fn to_report(query: Query<&Amount>) -> Result<Report, BodyError> {
    Ok(Report(query.get().0))
}

/// 借用 Struct Node：句柄借用调用者本地配置（非 `'static` 寿命）。
struct Configured<'a> {
    delta: &'a u32,
    log: Rc<RefCell<Vec<&'static str>>>,
}

impl Node for Configured<'_> {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        self.log.borrow_mut().push("borrowed");
        Ok(Amount(query.get().0 + *self.delta))
    }
}

/// owned 具体 Struct Node（`label` 区分 owned 与 `Arc` 两种调用形式）。
struct CountingStep {
    label: &'static str,
    log: Rc<RefCell<Vec<&'static str>>>,
}

impl Node for CountingStep {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        self.log.borrow_mut().push(self.label);
        Ok(Amount(query.get().0 + 1))
    }
}

/// 零输入 Struct Node：unit 输入不产生业务槽。
struct UnitNote {
    log: Rc<RefCell<Vec<&'static str>>>,
}

impl Node for UnitNote {
    type Input = ();
    type Output = ();

    async fn run(&self, query: Query<&Self::Input>) -> Result<(), BodyError> {
        self.log.borrow_mut().push("unit");
        let _ = query.get();
        Ok(())
    }
}

/// 多输入 Struct Node（统一 `Query<&Self::Input>` 协议）。
struct PairSum {
    log: Rc<RefCell<Vec<&'static str>>>,
}

impl Node for PairSum {
    type Input = (Amount, Amount);
    type Output = Report;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Report, BodyError> {
        self.log.borrow_mut().push("pair");
        let (left, right) = query.get();
        Ok(Report(left.0 + right.0))
    }
}

/// 跨 await 持有输入的只读借用，并在 await 之后继续读取同一借用（含非 Send 的 `Rc` 字段）。
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
        assert_eq!(borrowed.value, before, "借用跨 await 仍有效");
        let tag_len = borrowed.tag.borrow().len() as u32;
        Ok(NonSendSum(before + tag_len))
    }
}

fn main() {
    let runtime = Runtime::new();
    let log = Rc::new(RefCell::new(Vec::new()));
    let delta = 7u32;

    let configured = Configured {
        delta: &delta,
        log: Rc::clone(&log),
    };
    let owned_step = CountingStep {
        label: "owned",
        log: Rc::clone(&log),
    };
    let arc_step = Arc::new(CountingStep {
        label: "arc",
        log: Rc::clone(&log),
    });
    let unit_note = UnitNote {
        log: Rc::clone(&log),
    };
    let pair = PairSum {
        log: Rc::clone(&log),
    };

    let function_ref = &plus_one;
    let bump = async |query: Query<&Amount>| -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 100))
    };
    let pair_closure = async |query: Query<(&Amount, &Amount)>| -> Result<Amount, BodyError> {
        let (left, right) = query.get();
        Ok(Amount(left.0 + right.0))
    };

    let output = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, amount: Ref<Amount>| {
            let a = flow.then_with_output(double, amount); // 函数 item
            let b = flow.then_with_output(function_ref, a); // 函数引用
            let c = flow.then_with_output(bump, b); // async closure
            let d = flow.then_with_output(&configured, c); // 借用 Struct Node（非 static）
            let e = flow.then_with_output(owned_step, d); // owned 具体 Struct Node
            let f = flow.then_with_output(Arc::clone(&arc_step), e); // Arc<具体 Struct Node>
            let _: () = flow.then_with_output(&unit_note, ()); // unit 输入 Struct Node
            let g = flow.then_with_output(&pair, (f, a)); // 多输入 Struct Node
            let h = flow.then_with_output(pair_closure, (f, e)); // 多输入 async closure
            let out_refs = flow.then_with_output(to_report, h);
            flow.output_with(out_refs);
        },
        Amount(1),
    ))
    .expect("node form matrix runs");

    // 2 -> 3 -> 103 -> 110 -> 111 -> 112 -> pair(112,2)=114 -> closure(112,111)=223 -> 223
    assert_eq!(output.0, 223, "每个 Node 形式各执行一次");
    assert_eq!(
        log.borrow().as_slice(),
        ["borrowed", "owned", "arc", "unit", "pair"],
        "Struct Node 按定义顺序各执行一次"
    );

    // 非 Send 数据 + 跨真实 await 的借用。
    let polls = Arc::new(AtomicUsize::new(0));
    let node = BorrowAcrossAwait {
        polls: Arc::clone(&polls),
    };
    let sum = block_on(runtime.execute::<_, NonSendSum, _>(
        |flow: &mut Flow<'_>, input: Ref<NonSendInput>| {
            let out_refs = flow.then_with_output(&node, input);
            flow.output_with(out_refs);
        },
        NonSendInput {
            value: 40,
            tag: Rc::new(RefCell::new(String::from("abcd"))),
        },
    ))
    .expect("non-Send input with a real cross-await borrow runs");
    assert_eq!(sum.0, 44, "非 Send 输入跨 await 后仍可读");
    assert_eq!(polls.load(Ordering::SeqCst), 2, "真实 Pending 被轮询两次");

    println!("we_t06 p02 report={} non_send_sum={}", output.0, sum.0);
    println!("we_t06-positive-ok");
}
