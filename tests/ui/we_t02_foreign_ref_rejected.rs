//! WE-T02 B21／B22 正例：外部唯一的 Ref 来源是合法执行；跨 Definition 的 Ref 被构建期拒绝。
//!
//! 外部消费者无法自行构造 `Ref`（内部句柄类型不可命名、构造入口非公开）。能带出定义
//! 闭包的只有执行期产生的合法 Ref；把这种 Ref 用于另一次 `execute` 的 Definition 时，
//! 必须在构建／验证阶段拒绝，且不执行业务 Node。
//!
//! 期望：编译并运行成功，退出码 0，stdout 含 `we_t02-positive-ok`。

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Amount(u32);

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();

    // 第一次执行：把 Root 输入 Ref 带出定义闭包（外部仅有的合法 Ref 获取路径）。
    let mut leaked: Option<Ref<Amount>> = None;
    let first = block_on(runtime.execute(
        |_flow, amount| {
            leaked = Some(amount);
            amount
        },
        Amount(21),
    ))
    .expect("first execution succeeds");
    assert_eq!(first.0, 21);
    let foreign = leaked.expect("captured root ref");

    // 第二次执行使用该 Ref：Definition 校验拒绝，业务 Node 不执行。
    let error = block_on(runtime.execute(
        move |flow, amount| {
            let _ = flow.then(double, amount);
            flow.then(double, foreign)
        },
        Amount(1),
    ))
    .err()
    .expect("foreign definition ref is rejected");
    assert!(matches!(error, RunError::Definition(_)), "{error}");

    println!("we_t02-positive-ok");
}
