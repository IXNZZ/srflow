//! T03 迁移自 `we_t04_e04_iter_zero_rounds_rejected.rs`（WE-T04 E04 负例，Definition 构建期拒绝）：
//! Iter 预算 `max_iterations = 0` 非法。新契约下 Root 输出改由 [`Flow::output_with`] 声明
//! （定义闭包固定返回 `()`），显式连接更名为 `then_with_output`；原断言（RunError::Definition、
//! 业务 Node 计数为 0）与运行语义保持不变。
//!
//! `Flow::iter` 要求 `max_iterations = N > 0`，第一轮计入 N（SPEC §11；任务书 §3.4、E04）。
//! `N = 0` 必须在 Definition 验证阶段拒绝；“构建期拒绝”是由 §11 的 `N > 0` 加上 §4“构建失败
//! 返回 `RunError::Definition`，任何 Node 均不开始执行”导出的实现行为。这是 Definition
//! 构建失败，**不是** rustc 诊断，因此按 run/reject 判定。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t04-reject-ok`。

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};

use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};

/// 最小同步执行器：`Runtime::execute` 的 Future 在首次 poll 即就绪，故用 noop Waker 轮询即可；
/// 由此夹具不依赖 `futures`，与 `ui_check.py` 的 rustc 装配一致。
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = TaskContext::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[derive(Data)]
struct Context(u32);
#[derive(Data)]
#[allow(dead_code)] // 定义被构建期拒绝，报告字段从不交付
struct Report(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn judge(query: Query<&Context>) -> Result<Report, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Report(query.get().0))
}

fn main() {
    let runtime = Runtime::new();

    let error = block_on(runtime.execute::<_, Context, _>(
        |flow: &mut srflow::Flow<'_>, context: Ref<Context>| {
            let out_refs = flow.iter(context, 0, |current, round| {
                let _ = round.then_with_output(&judge, current);
                current
            });
            flow.output_with(out_refs);
        },
        Context(0),
    ))
    .err()
    .expect("zero-round iter budget is rejected at definition build");
    assert!(matches!(error, RunError::Definition(_)), "{error}");
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "a rejected definition starts no business Node"
    );

    println!("e04 iter zero rounds: {error}");
    println!("we_t04-reject-ok");
}
