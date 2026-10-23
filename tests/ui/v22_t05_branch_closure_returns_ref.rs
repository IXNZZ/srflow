//! T05 新增拒绝样本 ③：Choose 分支闭包返回 `Ref`（旧写法）编译拒绝。
//!
//! 新契约 `Choice::case`／`otherwise` 的分支 Body 是 **unit Body**（`F: FnOnce(&mut Flow<'n>)`），
//! 分支输出在 Body 内以 `output`／`output_with` 声明，闭包不返回 `Ref`。旧的“分支闭包尾表达式返回
//! RefShape”写法会与 `FnOnce(&mut Flow) -> ()` 冲突，被 rustc 以 E0308 拒绝。
//!
//! 期望主诊断：E0308（分支闭包返回 `Ref<Amount>` 与 unit Body 不一致）。

use srflow::{BodyError, Data, Flow, Query, Ref};

#[derive(Data)]
struct Amount(u32);

#[derive(Data, PartialEq)]
struct Route(u32);

async fn id_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, selector: Ref<Route>, amount: Ref<Amount>) {
    flow.choose_with(selector, |choice| {
        // 期望主诊断：E0308——分支闭包返回 Ref<Amount>，而 unit Body 要求 ()。
        choice.case(Route(0), |branch| branch.then_with_output(id_amount, amount));
        choice.otherwise(|branch| {
            let value = branch.then_with_output(id_amount, amount);
            branch.output_with(value);
        });
    });
}

fn main() {
    let _ = Amount(1);
}
