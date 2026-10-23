//! T03 迁移自 `we_t04_e03_foreign_ref_rejected.rs`（WE-T04 E03 负例，Definition 构建期拒绝）：
//! 不属本 Body 的 `Ref` 不能进入 Retry／Iter 定义。新契约下 Root 输出改由 [`Flow::output_with`]
//! 声明（定义闭包固定返回 `()`），显式连接更名为 `then_with_output`；原断言（RunError::Definition、
//! 业务 Node 计数不变）与运行语义保持不变。
//!
//! 外部唯一的合法 `Ref` 来源是真实执行：把上一次执行**泄漏**的 `Ref` 用于另一次 `execute`
//! 的 Definition 时，必须在**任何业务 Node／RootExecution 开始前**被统一验证拒绝
//! （任务书 §3.4、E03；SPEC §9）。
//!
//! 本夹具真实编译并运行，覆盖两个 Retry 场景：
//!
//! - 场景 A：foreign `Ref` 作为 attempt 内某 Node 的**输入**；
//! - 场景 B：foreign `Ref` 作为 attempt 闭包的**声明输出**（“闭包返回不属于该 Body 的 Ref”）。
//!
//! 两者都必须返回 `RunError::Definition`，且业务 Node 计数保持不变（失败无 Root／Node）。
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
struct Candidate(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Candidate(query.get().0 + 1))
}

async fn judge(query: Query<&Candidate>) -> Result<Candidate, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Candidate(query.get().0 + 1))
}

fn main() {
    let runtime = Runtime::new();

    // 第一次执行：把执行期产生的 Node 输出 Ref 带出定义闭包。
    let mut leaked: Option<Ref<Candidate>> = None;
    let first = block_on(runtime.execute::<_, Candidate, _>(
        |flow, prompt: Ref<Prompt>| {
            let candidate = flow.then_with_output(&generate, prompt);
            leaked = Some(candidate);
            flow.output_with(candidate);
        },
        Prompt(0),
    ))
    .expect("first execution succeeds");
    assert_eq!(first.0, 1, "first execution ran one business Node");
    let foreign = leaked.expect("node output ref leaked");
    let calls_after_first = CALLS.load(Ordering::SeqCst);

    // 场景 A：foreign Ref 作为 attempt 内 Node 的输入 → 构建期拒绝。
    let err_a = block_on(runtime.execute::<_, Candidate, _>(
        move |flow: &mut srflow::Flow<'_>, _prompt: Ref<Prompt>| {
            let out_refs: Ref<Candidate> = flow.retry_output(3, |attempt|{
                let out_refs = attempt.then_with_output(&judge, foreign);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(1),
    ))
    .err()
    .expect("foreign Ref as attempt input is rejected before any Node runs");
    assert!(matches!(err_a, RunError::Definition(_)), "{err_a}");

    // 场景 B：foreign Ref 作为 attempt 的声明输出 → 构建期拒绝。
    let err_b = block_on(runtime.execute::<_, Candidate, _>(
        move |flow: &mut srflow::Flow<'_>, _prompt: Ref<Prompt>| {
            let out_refs: Ref<Candidate> = flow.retry_output(3, |_attempt|{
                let out_refs = foreign;
                _attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(2),
    ))
    .err()
    .expect("foreign Ref as attempt output is rejected");
    assert!(matches!(err_b, RunError::Definition(_)), "{err_b}");

    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        calls_after_first,
        "rejected definitions started no business Node"
    );
    println!("e03 foreign input: {err_a}");
    println!("e03 foreign output: {err_b}");
    println!("we_t04-reject-ok");
}
