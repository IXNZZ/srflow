//! T05 迁移自 `v22_t03_from_we_t05_p02_collection_element_mismatch.rs`（WE-T05 P02 负例，编译期拒绝）：
//! **元素类型错**——body 节点期望的元素类型与集合元素不一致。
//!
//! T05 迁移点：`Flow::each` 的 Body 改为单参数 unit 闭包，item 经 `ref_of::<Item>()` 取得；
//! 显式连接仍为 `then_with_output`。原拒绝目的与范围不变（输入类型一致性在显式连接处拒绝）。
//!
//! 期望：编译期拒绝（主诊断码、唯一锚点行与符号 token 由驱动实跑回填）。

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
    flow.each(items, |sub| {
        let item = sub.ref_of::<Item>();
        sub.then_with_output(&wants_amount, item);
    });
}

fn main() {
    let _ = Amount(1);
}
