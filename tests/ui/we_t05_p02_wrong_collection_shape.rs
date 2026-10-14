//! WE-T05 P02 负例（编译期拒绝）：**集合类型错**——集合不是 `Ref<Vec<T>>`。
//!
//! SPEC §8：“items 为 `Ref<Vec<Item>>`，局部 item 为 `Ref<Item>`”；SPEC §3.2 的 Each 集合边界
//! 为 `Ref<Vec<T>>`。把单个元素的 `Ref<Item>` 当作集合传入不满足集合形状，编译期拒绝。
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
fn probe<'n>(flow: &mut Flow<'n>, single: Ref<Item>) {
    // 期望诊断：单个 Ref<Item> 不是 Ref<Vec<T>> 集合。
    let _ = flow.each(single, |item, each| each.then(&slot, item));
}

fn main() {
    let _ = Mark(1);
}
