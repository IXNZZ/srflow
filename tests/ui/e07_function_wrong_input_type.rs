//! E07 负例：普通函数的输入类型与接线位置不符，必须在真实 `then` 处编译失败。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/e07_function_wrong_input_type.rs \
//!   -o /tmp/e07.rmeta
//! ```
//!
//! 夹具按真实 `src/core/mod.rs` 装配，不使用 `--cfg test`；预期错误在 `then` 调用行
//! （E0277：`fn(&u32)` 不满足 `Fn(&String)`）。正例见 `core::v21_05_tests::e03_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::{Definition, TypedCallBuilder};
use core::context::BodyError;

fn wants_u32(a: &u32) -> Result<u32, BodyError> {
    Ok(*a)
}

fn main() {
    let mut definition = Definition::new();
    let text = definition.declare_input::<String>("t").expect("position");
    let _ = definition.then(wants_u32, text);
}
