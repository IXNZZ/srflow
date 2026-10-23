//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_invalid_budget.rs`）：预算／配置非法。
//!
//! 原目的（X24/X05／C13／C15）：两个场景都在 **Definition 构建期**被拒绝（`RunError::Definition`），
//! 业务 Node 零调用：`retry(usize::MAX, …)`（`RetryBudgetUnrepresentable`，诊断含 "max_retries"）与
//! `iter(…, 0, …)`（`IterZeroRounds`，诊断含 "max_iterations"）。
//!
//! 判定与归因：**Definition 构建期拒绝**，不是 rustc 拒绝——两处都是合法类型、非法配置值。
//!
//! 迁移手法：Root 闭包改为固定返回 `()` 并经 `output_with` 声明 owned `Out = Amount`；旧两参
//! `flow.then(node, refs)`（attempt 内与轮内各一次）改为 `then_with_output`。两个场景的预算值与
//! 零调用断言不变。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t06-build-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Amount(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn bump(query: Query<&Amount>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0 + 1))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    // 场景 1：Retry 预算无法表达。
    let retry_error = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, amount: Ref<Amount>| {
            let out_refs = flow.retry(usize::MAX, |attempt| attempt.then_with_output(&bump, amount));
            flow.output_with(out_refs);
        },
        Amount(1),
    ))
    .err()
    .expect("max_retries = usize::MAX is rejected at definition build time");
    assert!(
        matches!(retry_error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {retry_error}"
    );
    assert!(
        retry_error.to_string().contains("max_retries"),
        "诊断应指出无法表达的预算：{retry_error}"
    );

    // 场景 2：Iter 零轮。
    let iter_error = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, amount: Ref<Amount>| {
            let out_refs = flow.iter(amount, 0, |current, round| round.then_with_output(&bump, current));
            flow.output_with(out_refs);
        },
        Amount(1),
    ))
    .err()
    .expect("max_iterations = 0 is rejected at definition build time");
    assert!(
        matches!(iter_error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {iter_error}"
    );
    assert!(
        iter_error.to_string().contains("max_iterations"),
        "诊断应指出零轮预算：{iter_error}"
    );

    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "被拒绝的 Definition 不执行任何业务 Node"
    );

    println!("we_t06 invalid retry budget: {retry_error}");
    println!("we_t06 invalid iter budget:  {iter_error}");
    println!("we_t06-build-reject-ok");
}
