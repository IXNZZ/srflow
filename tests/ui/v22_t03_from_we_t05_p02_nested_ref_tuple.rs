//! T03 迁移自 `we_t05_p02_nested_ref_tuple.rs`（WE-T05 P02 负例，编译期拒绝）：nested Ref tuple
//! 不作为 Each 的 body 输出。新契约下 Root 输出声明与显式连接改名不涉及本例（body 直接返回
//! nested tuple，不含 `then`）；原拒绝目的与范围不变。
//!
//! SPEC §3.2：“超过 16 个位置和 **nested Ref tuple** 不属于本版承诺”；SPEC §3.3：tuple 只在
//! **扁平** Shape 中使用，native tuple 不作为默认 Data 叶子。外层第 1 槽
//! `(Ref<Item>, Ref<Item>)` 既不是 `Ref<T>` 也不是 Data，因此不实现 Each body 输出所需的形状协议。
//!
//! 期望：编译期拒绝（主诊断码、唯一锚点行与符号 token 见 T03 清单
//! `manifest_we_t05.tsv` 该行；驱动实跑回填）。

use srflow::{Data, Flow, Ref};

#[derive(Data)]
struct Item(u32);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, items: Ref<Vec<Item>>) {
    // 期望诊断：嵌套 Ref tuple 未实现 Each body 输出形状协议。
    let _ = flow.each(items, |item, _each| ((item, item), item));
}

fn main() {
    let _ = Item(1);
}
