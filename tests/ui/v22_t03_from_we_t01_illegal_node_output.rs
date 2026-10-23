//! WE-T01 A05 负例 4：非法 Node Output——`type Output = (Amount, Other)`。
//!
//! 意图：Node 只返回一个 owned Data 或 `()`；tuple 不是 `NodeOutput`，不得通过
//! tuple Output 悄悄增加多个输出位置。native tuple 只作为输入／RefShape 形状。
//!
//! 预期主诊断：E0277（`(Amount, Other)` 未实现 `NodeOutput`），
//! 落在本文件的 `type Output` 声明行。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/v22_t03_from_we_t01_illegal_node_output.rs -o /tmp/v22_t03_from_we_t01_illegal_node_output.rmeta
//! ```

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::error::BodyError;
use crate::core::engine::node::Node;
use crate::core::engine::shape::Query;

struct Amount(u32);

impl Data for Amount {}

struct Other(u32);

impl Data for Other {}

struct BadOutput;

impl Node for BadOutput {
    type Input = Amount;
    // 期望主诊断：E0277——tuple 未实现 NodeOutput。
    type Output = (Amount, Other);

    async fn run(&self, query: Query<&Self::Input>) -> Result<Self::Output, BodyError> {
        let _ = query.get();
        Ok((Amount(0), Other(0)))
    }
}

fn main() {
    let _ = std::mem::size_of::<BadOutput>();
}
