//! WE-T04 E02 负例（编译期拒绝）：Retry 输出与 attempt 返回**不一致**。
//!
//! `Flow::retry<O, F>` 中 `O` 同时是 parent 输出与 attempt 闭包的返回类型：二者是同一个
//! 类型变量，公开面无法声明“parent 输出一种 Shape、attempt 返回另一种 Shape”。这里用显式
//! 类型标注把 parent 期望的 2 元 Shape 固定下来，而 attempt 只返回单 `Ref`，类型系统因此
//! 拒绝（若二者真的能不一致，等于集合了任意额外执行路径，任务书 §1／E02 明确排除）。
//!
//! 期望主诊断：E0308（attempt 返回单 Ref 与标注的 2 元输出不一致）。

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
    // 标注把 retry 的 O 固定为 2 元；attempt 只返回单 Ref<Amount>。
    let _: (Ref<Amount>, Ref<Other>) = flow.retry(3, |attempt| {
        // 期望主诊断：E0308——返回单 Ref，与标注的 2 元输出不一致。
        attempt.then(&produce, amount)
    });
}

fn main() {
    let _ = (Amount(1), Other(2));
}
