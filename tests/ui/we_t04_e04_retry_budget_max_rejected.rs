//! WE-T04 E04 负例（Definition 构建期拒绝）：Retry 预算 `usize::MAX` 不可表达。
//!
//! `Flow::retry` 的 `max_retries = N` 指首次 attempt 之外最多再执行 N 次；`usize::MAX` 无法
//! 表达 `N + 1` 次尝试的总数，必须在 Definition 构建期拒绝（SPEC §10；任务书 §3.4、E04）。
//! 这是 Definition 构建失败（`RunError::Definition`），**不是** rustc 诊断，也不建立 Root、
//! 不调用任何 Node——因此按 run/reject 判定，不伪装成编译期错误。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t04-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};

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

    let error = block_on(runtime.execute(
        |flow: &mut srflow::Flow<'_>, prompt: Ref<Prompt>| {
            flow.retry(usize::MAX, |attempt| attempt.then(&generate, prompt))
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
