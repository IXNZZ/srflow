//! V22-T04 夹具（任务书 §9.1“另建旧闭包返回 Ref 的编译拒绝”）：unit `retry` + 闭包尾表达式返回 `Ref`。
//!
//! 新契约下 `retry` 的 Body 固定返回 `()`（声明由 `attempt.output_with(…)` 给出），因此旧
//! “闭包返回 Ref”的写法直接放到 `retry` 上会被 rustc 拒绝（E0308）；读取 child 声明输出须走
//! `retry_output`。本夹具把该编译拒绝固定下来，避免旧写法被误认为仍然可用，也避免该不一致被
//! “与 `_output` 期望校验无关的错误”冒充。
//!
//! 期望主诊断：E0308（闭包返回 `Ref` 与 unit Body 不一致）。

use srflow::{BodyError, Data, Flow, Query, Ref};

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Other(u32);

async fn produce(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, amount: Ref<Amount>) {
    // 旧写法：闭包尾表达式返回单 Ref；新契约要求 unit Body。
    let _: (Ref<Amount>, Ref<Other>) = flow.retry(3, |attempt| {
        attempt.then_with_output(&produce, amount)
    });
}

fn main() {
    let _ = (Amount(1), Other(2));
}
