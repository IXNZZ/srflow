//! T03 迁移自 `we_t04_e02_positive_shapes.rs`（WE-T04 E02 正例）：Retry 的 unit／单 Ref 输出与
//! Iter 的单 Ref State 在外部消费者中真实运行。新契约下 Root 输出改由 [`Flow::output_with`]
//! 声明（定义闭包固定返回 `()`），显式连接更名为 `then_with_output`；原断言、计数与运行语义不变。
//!
//! 覆盖任务书 §1／E02 的正例边界（负例见 `v22_t03_from_we_t04_e02_*` 编译拒绝夹具）：
//!
//! 1. Retry unit 输出：attempt 返回 `()`，不登记假业务槽；用业务计数观察 attempt 重做；
//! 2. Retry 单 Ref 输出：`attempt.then_with_output(…)` 返回单个 `Ref<Amount>`；
//! 3. Iter 单 Ref State：`initial`／`current`／`next`／最终输出同 Shape（单个 `Ref<Counter>`），
//!    第二轮 `IterBreak` 交付进入本轮时的 current，未接受的 next 不交付。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t04-positive-ok`。

use srflow::{BodyError, Data, Query, Ref, Runtime};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

/// 最小同步执行器：`Runtime::execute` 的 Future 在首次 poll 即就绪，故用 noop Waker 轮询即可；
/// 由此夹具不依赖 `futures`，与 `ui_check.py` 的 rustc 装配一致。
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[derive(Data)]
struct Amount(u32);
#[derive(Data)]
struct Counter(u32);
#[derive(Data)]
#[allow(dead_code)] // Break 判定节点只报告进度，字段不参与交付
struct Report(u32);

/// attempt 的 unit 输出观察：每次成功的 judge 计一次。
static JUDGED: AtomicUsize = AtomicUsize::new(0);

/// 每次 attempt 递增的候选序号（真实生成节点随尝试取样）。
static ATTEMPT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct NotReady(u32);

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not ready {}", self.0)
    }
}

impl Error for NotReady {}

/// 单 Ref 输出：每次 attempt 产生不同候选（重试复用原始依赖，靠计数器推进）。
async fn produce(query: Query<&Amount>) -> Result<Amount, BodyError> {
    let base = query.get().0;
    let attempt = ATTEMPT.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(Amount(base + attempt as u32))
}

/// unit 输出：只做业务副作用（计数），不产生业务槽。
async fn audit(query: Query<&Amount>) -> Result<(), BodyError> {
    let value = query.get().0;
    if value < 3 {
        Err(BodyError::retry(NotReady(value)))
    } else {
        JUDGED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// 单 Ref State 推进：每轮把 counter 加一。
async fn bump(query: Query<&Counter>) -> Result<Counter, BodyError> {
    Ok(Counter(query.get().0 + 1))
}

/// 单 Ref State 的 Break 判定：进入第二轮（current >= 1）时 Break。
async fn break_at(query: Query<&Counter>) -> Result<Report, BodyError> {
    let value = query.get().0;
    if value >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(Report(value))
    }
}

fn main() {
    let runtime = Runtime::new();

    // 1) Retry unit 输出：attempt 返回 ()，不产生 Ref<()>／占位 Data。
    let () = block_on(runtime.execute::<_, (), _>(
        |flow: &mut srflow::Flow<'_>, amount: Ref<Amount>| {
            let () = flow.retry(3, |attempt| {
                let candidate = attempt.then_with_output(&produce, amount);
                attempt.then_with_output(&audit, candidate)
            });
            flow.output_with(());
        },
        Amount(0),
    ))
    .expect("unit retry run");
    assert_eq!(JUDGED.load(Ordering::SeqCst), 1, "unit output relies on real runs");

    // 2) Retry 单 Ref 输出。
    ATTEMPT.store(0, Ordering::SeqCst);
    let amount = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut srflow::Flow<'_>, amount: Ref<Amount>| {
            let out_refs = flow.retry(3, |attempt| attempt.then_with_output(&produce, amount));
            flow.output_with(out_refs);
        },
        Amount(41),
    ))
    .expect("single-ref retry run");
    assert_eq!(amount.0, 42, "single Ref output delivered once");

    // 3) Iter 单 Ref State：第二轮 Break 交付进入本轮时的 current。
    let counter = block_on(runtime.execute::<_, Counter, _>(
        |flow: &mut srflow::Flow<'_>, counter: Ref<Counter>| {
            let out_refs = flow.iter(counter, 3, |current, round| {
                let _ = round.then_with_output(&break_at, current);
                round.then_with_output(&bump, current)
            });
            flow.output_with(out_refs);
        },
        Counter(0),
    ))
    .expect("single-ref iter run");
    assert_eq!(counter.0, 1, "IterBreak delivers the entry current of that round");

    println!("we_t04-positive-ok");
}
