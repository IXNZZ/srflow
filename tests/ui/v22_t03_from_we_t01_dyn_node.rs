//! WE-T01 负例 9（可选）：`dyn Node` 不是合法对象——协议不支持 trait object。
//!
//! 意图：Node 协议含 `async fn run`（RPITIT）与关联类型，不构成对象安全 trait；
//! T01 明确不支持 `dyn Node` 作为登记形式。
//!
//! 预期主诊断：E0038（trait `Node` 不能成为 trait object），落在本文件的
//! `dyn Node` 类型标注行。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/v22_t03_from_we_t01_dyn_node.rs -o /tmp/v22_t03_from_we_t01_dyn_node.rmeta
//! ```

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::node::Node;

struct Amount(u32);

impl Data for Amount {}

fn main() {
    // 期望主诊断：E0038——Node 不是对象安全 trait。
    let _: Option<&dyn Node<Input = Amount, Output = Amount>> = None;
}
