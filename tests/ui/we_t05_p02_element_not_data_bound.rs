//! WE-T05 P02 负例（编译期拒绝）：**元素类型错**——集合元素未满足 `T: Data`。
//!
//! SPEC §8：“item 为 `Ref<T>`，集合为 `Ref<Vec<T>>`，`T: Data`”；SPEC §3.1：Data 不是 blanket
//! 实现，非 unit 业务类型必须显式声明（`#[derive(Data)]` 或手写 `impl`）。这里用未声明 Data 的
//! 泛型元素 `T` 表达：`Ref<T>` 自身即要求 `T: Data`，因此该错误无法走到 `each` 调用内部，就在
//! **命名集合**处被拒绝（这也是“未声明 Data 的元素不能进入 Each”的编译期入口）。
//!
//! 期望：编译期拒绝（主诊断码与起始行已由驱动实跑回填，见 `ui_manifest.tsv` 该行）。

use srflow::{Flow, Ref};

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n, T>(flow: &mut Flow<'n>, items: Ref<Vec<T>>) {
    // 期望诊断：T 未满足 Data（Ref<T> / Ref<Vec<T>> 的 Data 要求）。
    let _ = flow.each(items, |item, _each| item);
}

fn main() {
    let _ = 1u32;
}
