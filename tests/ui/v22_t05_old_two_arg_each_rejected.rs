//! T05 新增拒绝样本 ②：旧双参 `flow.each(items, |item, sub| …)` 在新契约下编译拒绝。
//!
//! 新契约 `Flow::each<T, F>(&mut self, collection: Ref<Vec<T>>, definition: F) where F: FnOnce(&mut
//! Flow<'n>)`：item Body 只接收一个 `&mut Flow`（item 经 `sub.ref_of::<T>()` 取得）。把旧双参闭包
//! 直接搬到 `each` 上会被 rustc 以 E0593（闭包参数个数不符）拒绝；本夹具把该旧写法固定下来。
//!
//! 期望主诊断：E0593（item Body 期望 1 参数闭包，却给了 2 参数闭包）。

use srflow::{Data, Flow, Ref};

#[derive(Data)]
struct Item(u32);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, items: Ref<Vec<Item>>) {
    flow.each(items, |_item, _sub| {});
}

fn main() {
    let _ = Item(1);
}
