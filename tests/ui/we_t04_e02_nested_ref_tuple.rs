//! WE-T04 E02 负例（编译期拒绝）：nested Ref tuple 不是承诺的形状。
//!
//! 形状协议只承认 `()`、单个 `Ref<T>` 与**扁平**的 2～16 元 `Ref` tuple；tuple 不默认成为
//! 业务 Data 叶子（任务书 §1：“2～16 flat tuple”）。嵌套元组 `((Ref<Item>, Ref<Item>), Ref<Item>)`
//! 的外层第 1 槽既不是 `Ref<T>` 也不是 Data，因此既不实现 `RefShape`／`ShapeOps`（Retry 输出），
//! 也不实现 `StateShape`（Iter 状态）。
//!
//! 期望主诊断：E0277（嵌套 tuple 未实现形状协议）。Retry 输出与 Iter 状态两处各产生一次。

use srflow::{Data, Flow, Ref};

#[derive(Data)]
struct Item(u32);

type Nested = ((Ref<Item>, Ref<Item>), Ref<Item>);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, nested: Nested, item: Ref<Item>) {
    // 期望诊断：E0277——嵌套 Ref tuple 未实现 RefShape／ShapeOps（Retry 输出）。
    let _: Nested = flow.retry(3, |_attempt| ((item, item), item));
    // 期望诊断：E0277——嵌套 Ref tuple 未实现 StateShape（Iter 状态）。
    let _ = flow.iter(nested, 3, |current, _round| current);
}

fn main() {
    let _ = Item(1);
}
