//! WE-T04 E02 负例（编译期拒绝）：Iter 状态超过 16 槽 flat tuple。
//!
//! `StateShape`（经 `tuple_state_shapes!` 宏）只为单个 `Ref` 与 2～16 元 flat `Ref` tuple
//! 实现。17 元 flat tuple State 不实现 `StateShape`／`RefShape`，`flow.iter` 在类型检查阶段
//! 被拒绝（任务书 §1：“Iter 支持单 Ref、2～16 flat tuple State”）。
//!
//! 期望主诊断：E0277（17 元 tuple 未实现 `StateShape`）。

use srflow::{Data, Flow, Ref};

#[derive(Data)]
struct Item(u32);

type Slot = Ref<Item>;
type Tuple17 = (
    Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot,
    Slot, Slot,
);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, t: Tuple17) {
    let _ = flow.iter(t, 3, |current, _round| current);
}

fn main() {
    let _ = Item(1);
}
