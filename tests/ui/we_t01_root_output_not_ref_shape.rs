//! WE-T01 A01 负例 8：Root 定义闭包的输出不是 RefShape。
//!
//! 意图：Root 输出形状只能是 `()`、单 Ref 或 2～16 flat Ref tuple。返回裸 `usize`
//! 不是已登记的逻辑位置，必须在编译期被拒绝，不能声明一个无来源的 Root 输出。
//!
//! 预期主诊断：E0277（`usize` 未实现 `RefShape`），落在本文件第 26 行
//! `let _ = build_definition::<Amount, _, _>(...)`（2026-10-11 实测）。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/we_t01_root_output_not_ref_shape.rs -o /tmp/we_t01_root_output.rmeta
//! ```

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::definition::build_definition;

struct Amount(u32);

impl Data for Amount {}

fn main() {
    // 期望主诊断：E0277——`usize` 不是 RefShape，不能作为 Root 输出。
    let _ = build_definition::<Amount, _, _>(|_flow, _amount| 1usize);
}
