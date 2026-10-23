//! T03 迁移自 `we_t04_e04_retry_budget_max_rejected.rs`（WE-T04 E04 负例，Definition 构建期拒绝）：
//! Retry 预算 `usize::MAX` 不可表达。新契约下 Root 输出改由 [`Flow::output_with`] 声明
//! （定义闭包固定返回 `()`），显式连接更名为 `then_with_output`；原断言（RunError::Definition、
//! 业务 Node 计数为 0）与运行语义保持不变。
//!
//! `Flow::retry` 的 `max_retries = N` 指首次 attempt 之外最多再执行 N 次；`usize::MAX` 无法
//! 表达 `N + 1` 次尝试的总数，必须在 Definition 构建期拒绝（SPEC §10；任务书 §3.4、E04）。
//! 这是 Definition 构建失败（`RunError::Definition`），**不是** rustc 诊断，也不建立 Root、
//! 不调用任何 Node——因此按 run/reject 判定，不伪装成编译期错误。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t04-reject-ok`。

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};

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
struct Prompt(u32);
#[derive(Data)]
#[allow(dead_code)] // 定义被构建期拒绝，候选值从不交付
struct Candidate(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Candidate(query.get().0 + 1))
}

fn main() {
    let runtime = Runtime::new();

    let error = block_on(runtime.execute::<_, Candidate, _>(
        |flow: &mut srflow::Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs = flow.retry(usize::MAX, |attempt| attempt.then_with_output(&generate, prompt));
            flow.output_with(out_refs);
        },
        Prompt(0),
    ))
    .err()
    .expect("usize::MAX retry budget is rejected at definition build");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "a rejected definition starts no business Node"
    );

    println!("e04 retry max budget: {error}");
    println!("we_t04-reject-ok");
}
