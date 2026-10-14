//! WE-T02 B22 负例：`dyn Node` 不是合法对象，不能作为登记形式。
//!
//! `Node` 含 `async fn run`（RPITIT）与关联类型，不构成对象安全 trait；SPEC §5.3
//! 明确 dyn Node 不在本版支持范围。
//!
//! 期望主诊断：E0038（`Node` 不能成为 trait object）。

use srflow::{Data, Node};

#[derive(Data)]
struct Amount(u32);

fn main() {
    // 期望主诊断：E0038——Node 不是对象安全 trait。
    let _: Option<&dyn Node<Input = Amount, Output = Amount>> = None;
}
