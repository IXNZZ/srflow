//! T02 负例：同一表达式内两次可变借用（嵌套写法）被借用检查拒绝。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t02_neg_e0499_nested.rs -o /tmp/v22_t02_neg_e0499_nested.rmeta
//! ```
//!
//! 预期主诊断：E0499（不能同时把 `flow` 可变借用两次），落在 `output_with` 调用行。
//! 等价合法写法见 `tests_v22_t02.rs::b14_saved_ref_and_output_with_are_the_equivalent_form`。

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
