//! T03 负例：同一表达式内两次可变借用（嵌套写法）被借用检查拒绝。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/borrow__nested_mutable_builders.rs -o /tmp/v22_t03_from_t02_neg_e0499_nested.rmeta
//! ```
//!
//! 预期主诊断：E0499（不能同时把 `flow` 可变借用两次），落在 `output_with` 调用行。
//! 等价合法写法见 `tests_v22_t03_*.rs` 中“先取 Ref 再声明输出”的形式。
//!
//! T03 迁移：`provide_output`／`output_with` 在 T03 均为公开方法（E0624 私有判据已退场），
//! 本夹具因此转为**公开 API 的借用约束负例**：调用点与诊断阶段不变，原“嵌套两次可变借用被拒绝”
//! 的目的完整保留。
#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::flow::Flow;

struct Key(u32);
impl Data for Key {}

fn wire<'n>(flow: &mut Flow<'n>) {
    // 期望主诊断：E0499——内层 `provide_output` 借用未结束时外层 `output_with` 再次可变借用。
    flow.output_with(flow.provide_output(|| Key(5)));
}

fn main() {}
