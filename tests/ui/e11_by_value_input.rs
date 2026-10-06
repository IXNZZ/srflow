//! E11 负例：按值参数的函数不能作为读取已有 Data 的 Node 接入。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/e11_by_value_input.rs -o /tmp/e11.rmeta
//! ```
//!
//! 预期错误在 `then` 处（E0277）：`fn(u32)` 不满足 `Fn(&u32)`，框架不会为通过该样本
//! 隐式 Clone 或移动输入。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::{Definition, TypedCallBuilder};
use core::context::BodyError;

fn by_value(a: u32) -> Result<u32, BodyError> {
    Ok(a)
}

fn main() {
    let mut definition = Definition::new();
    let number = definition.declare_input::<u32>("a").expect("position");
    let _ = definition.then(by_value, number);
}
