//! T03 迁移自 `we_t05_p02_collection_element_mismatch.rs`（WE-T05 P02 负例，编译期拒绝）：
//! **元素类型错**——body 节点期望的元素类型与集合元素不一致。显式连接更名为 `then_with_output`；
//! 原拒绝目的与范围不变。
//!
//! SPEC §8：item body 使用普通 ancestor capture 与普通 Node 输入校验；`item` 是 `Ref<Item>`
//! 的受限借用，只能作为匹配 `Item` 的输入。用一个声明 `Amount` 输入的节点接收 `Ref<Item>`
//! 破坏了输入类型一致性，编译期拒绝（显式连接的输入位置）。
//!
//! 期望：编译期拒绝（主诊断码、唯一锚点行与符号 token 见 T03 清单
//! `manifest_we_t05.tsv` 该行；驱动实跑回填）。

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
    let _ = flow.each(items, |item, each| each.then_with_output(&wants_amount, item));
}

fn main() {
    let _ = Amount(1);
}
