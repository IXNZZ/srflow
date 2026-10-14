//! WE-T05 P02 负例（编译期拒绝）：item body 输出**超过 16 个 flat 槽**。
//!
//! SPEC §3.2：“超过 16 个位置和 nested Ref tuple 不属于本版承诺”；SPEC §8 的多输出映射为
//! “每个位置分别形成 `Ref<Vec<对应类型>>`”，因此 17 槽 flat tuple 不是被承诺的 Shape，不实现
//! 形状协议（任务书 §1／§3.2）。本夹具不在 `each` 调用处使用 arity 命名（方法名不按 arity 分类）。
//!
//! 期望：编译期拒绝（主诊断码与起始行已由驱动实跑回填，见 `ui_manifest.tsv` 该行）。

use srflow::{BodyError, Data, Flow, Query, Ref};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Mark(u32);

async fn slot(query: Query<&Item>) -> Result<Mark, BodyError> {
    Ok(Mark(query.get().0))
}

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, items: Ref<Vec<Item>>) {
    let _ = flow.each(items, |item, each| {
        let c01 = each.then(&slot, item);
        let c02 = each.then(&slot, item);
        let c03 = each.then(&slot, item);
        let c04 = each.then(&slot, item);
        let c05 = each.then(&slot, item);
        let c06 = each.then(&slot, item);
        let c07 = each.then(&slot, item);
        let c08 = each.then(&slot, item);
        let c09 = each.then(&slot, item);
        let c10 = each.then(&slot, item);
        let c11 = each.then(&slot, item);
        let c12 = each.then(&slot, item);
        let c13 = each.then(&slot, item);
        let c14 = each.then(&slot, item);
        let c15 = each.then(&slot, item);
        let c16 = each.then(&slot, item);
        let c17 = each.then(&slot, item);
        (
            c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17,
        )
    });
}

fn main() {
    let _ = Mark(1);
}
