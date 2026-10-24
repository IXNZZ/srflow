//! T05 新增拒绝样本 ①：旧两参 `flow.choose(selector, |choice| …)` 在新契约下编译拒绝。
//!
//! 新契约 `Flow::choose<K, F>(&mut self, definition: F)` 只接收一个 definition 实参（selector 由
//! 上下文自动解析，或改用显式形式 `choose_with(selector, …)`）。把旧两参形式直接搬到 `choose`
//! 上会被 rustc 以 E0061（参数个数不符）拒绝；本夹具把该旧写法固定下来，避免被误认为仍可用。
//!
//! 期望主诊断：E0061（`choose` 只接受 1 个 definition 实参，却给了 2 个）。
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
    let _ = flow.choose::<Route, _>(selector, |choice| {
        choice.case(Route(0), |branch| {
            let value = branch.then_with_output(id_amount, amount);
            branch.output_with(value);
        });
        choice.otherwise(|branch| {
            let value = branch.then_with_output(id_amount, amount);
            branch.output_with(value);
        });
    });
}

fn main() {
    let _ = Amount(1);
}
