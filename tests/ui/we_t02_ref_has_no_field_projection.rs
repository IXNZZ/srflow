//! WE-T02 B22 负例：`Ref<T>` 没有字段入口，不能做字段投影。
//!
//! `Ref<T>` 的字段是 crate 私有的；`Ref` 不 Deref 到 `T`，也没有公开投影方法。
//! 使用者无法从 `Ref` 直接取到业务值的字段。
//!
//! 期望主诊断：E0609（`Ref<T>` 上不存在字段 `0`）。

use srflow::{Data, Ref};

#[derive(Data)]
struct Amount(u32);

/// 字段投影必须被拒绝。
fn probe<T: Data>(value: Ref<T>) {
    let _ = value.0;
}

fn main() {
    let _ = probe::<Amount>;
}
