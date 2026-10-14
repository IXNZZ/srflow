//! WE-T02 B22 负例：`Ref<()>` 不可表达（`()` 不实现 `Data`）。
//!
//! `Ref<T: Data>` 的 `Data` 上下界明确排除 unit：unit 不分配业务槽，也不产生
//! `Ref<()>`（SPEC §3.2、§5.3）。本夹具以函数签名固定该拒绝位置，不构造任何值。
//!
//! 期望主诊断：E0277（`(): Data` 未满足）。

use srflow::Ref;

fn takes_unit(_: Ref<()>) {}

fn main() {
    let _ = takes_unit;
}
