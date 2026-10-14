//! WE-T05 P02 负例（编译期拒绝）：**元素类型错**——body 节点期望的元素类型与集合元素不一致。
//!
//! SPEC §8：item body 使用普通 ancestor capture 与普通 Node 输入校验；`item` 是 `Ref<Item>`
//! 的受限借用，只能作为匹配 `Item` 的输入。用一个声明 `Amount` 输入的节点接收 `Ref<Item>`
//! 破坏了输入类型一致性，编译期拒绝（`.then` 的输入位置）。
//!
//! 期望：编译期拒绝（主诊断码与起始行已由驱动实跑回填，见 `ui_manifest.tsv` 该行）。

use srflow::{BodyError, Data, Flow, Query, Ref};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Amount(u32);

async fn wants_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, items: Ref<Vec<Item>>) {
    // 期望诊断：item 是 Ref<Item>，节点期望 Ref<Amount>。
    let _ = flow.each(items, |item, each| each.then(&wants_amount, item));
}

fn main() {
    let _ = Amount(1);
}
