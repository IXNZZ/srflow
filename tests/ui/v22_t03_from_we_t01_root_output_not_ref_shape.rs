//! WE-T01 A01 负例 8：Root 定义闭包的输出不是显式声明。
//!
//! 意图：Root 闭包固定返回 `()`，业务输出**只**经 `Flow::output_with`（或 `Flow::output`）
//! 声明。裸 owned 尾表达式 `1usize` 既不是已登记的逻辑位置，也不构成 Root 输出，
//! 必须在编译期被拒绝，不能声明一个无来源的 Root 输出。
//!
//! 预期主诊断：E0308（闭包体返回 `usize`，与固定的 `()` 返回类型不符），落在本文件
//! `main` 内的 Root 定义调用行。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/v22_t03_from_we_t01_root_output_not_ref_shape.rs -o /tmp/v22_t03_from_we_t01_root_output.rmeta
//! ```
//!
//! T03 迁移：原诊断 E0277（`usize` 未实现旧 `RefShape`）随公开签名切换变为 E0308
//! （unit 闭包约束）。拒绝阶段仍是编译期，目的不变：禁止以裸 owned 尾表达式冒充 Root
//! 输出。合法对照形如 ``let refs = ...; flow.output_with(refs);``（末尾分号、显式声明）。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::definition::build_definition;

struct Amount(u32);

impl Data for Amount {}

fn main() {
    // 期望主诊断：E0308——Root 闭包固定返回 `()`，裸 owned 尾表达式不是 Root 输出声明。
    let _ = build_definition::<Amount, Amount, _>(|_flow, _amount| 1usize);
}
