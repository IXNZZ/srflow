//! T03 迁移自 `tests/ui/we_t02_foreign_ref_rejected.rs`（WE-T02 B21／B22 正例）：外部唯一的
//! Ref 来源是合法执行；跨 Definition 的 Ref 在构建期被拒绝。
//!
//! 外部消费者无法自行构造 `Ref`（内部句柄类型不可命名、构造入口非公开）。能带出定义闭包的
//! 只有执行期产生的合法 Ref；把这种 Ref 用于另一次 `execute` 的 Definition 时，必须在构建／
//! 验证阶段拒绝，且不执行业务 Node。
//!
//! T03 迁移点：两次 Root 闭包改为返回 `()`、业务输出由 `flow.output_with(refs)` 声明，
//! owned Out 由 `execute::<_, Amount, _>` 显式给出；显式连接改名为 `then_with_output`。
//! 第一次执行仍把 Root 输入 Ref 带出闭包（`leaked`），第二次执行仍把该外来 Ref 接进定义
//! 并声明为其输出，因此拒绝阶段（Definition）与断言 `RunError::Definition(_)` 逐字保留。
//!
//! 由主包真实 rlib 以外部消费者身份编译运行；文件内联最小 executor 取代原 `futures::executor::block_on`。
//!
//! 期望：编译并运行成功，退出码 0，stdout 含 `we_t02-positive-ok`。

use std::future::Future;
use std::task::{Context, Poll, Waker};

use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Amount(u32);

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

/// 最小 executor：把 `Runtime::execute` 的 Future 驱动到完成。
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

fn main() {
    let runtime = Runtime::new();

    // 第一次执行：把 Root 输入 Ref 带出定义闭包（外部仅有的合法 Ref 获取路径）。
    let mut leaked: Option<Ref<Amount>> = None;
    let first = block_on(runtime.execute::<_, Amount, _>(
        |flow, amount| {
            leaked = Some(amount);
            flow.output_with(amount);
        },
        Amount(21),
    ))
    .expect("first execution succeeds");
    assert_eq!(first.0, 21);
    let foreign = leaked.expect("captured root ref");

    // 第二次执行使用该 Ref：Definition 校验拒绝，业务 Node 不执行。
    let error = block_on(runtime.execute::<_, Amount, _>(
        move |flow, amount| {
            let _ = flow.then_with_output(double, amount);
            let out = flow.then_with_output(double, foreign);
            flow.output_with(out);
        },
        Amount(1),
    ))
    .err()
    .expect("foreign definition ref is rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");

    println!("we_t02-positive-ok");
}
