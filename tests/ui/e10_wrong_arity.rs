//! E10 负例：少给／多给参数，或在零输入 Node 上误给位置。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/e10_wrong_arity.rs -o /tmp/e10.rmeta
//! ```
//!
//! 预期三处 `then` 调用行各自 E0277：
//! - 双输入函数只给一个位置；
//! - 单输入函数给了两个位置；
//! - 零输入函数给了位置。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::{Definition, TypedCallBuilder};
use core::context::BodyError;

fn two(a: &u32, b: &u32) -> Result<u32, BodyError> {
    Ok(*a + *b)
}

fn one(a: &u32) -> Result<u32, BodyError> {
    Ok(*a)
}

fn zero() -> Result<u32, BodyError> {
    Ok(0)
}

fn main() {
    let mut definition = Definition::new();
    let first = definition.declare_input::<u32>("a").expect("position");
    let second = definition.declare_input::<u32>("b").expect("position");
    let _ = definition.then(two, first.clone());
    let _ = definition.then(one, (first.clone(), second.clone()));
    let _ = definition.then(zero, first);
}
