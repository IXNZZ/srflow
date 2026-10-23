//! WE-T02 B22 负例：`dyn Node` 不是合法对象，不能作为登记形式。
//!
//! `Node` 含 `async fn run`（RPITIT）与关联类型，不构成对象安全 trait；SPEC §5.3
//! 明确 dyn Node 不在本版支持范围。
//!
//! 期望主诊断：E0038（`Node` 不能成为 trait object）。
//! V22-T03 迁移：源 `tests/ui/we_t02_dyn_node.rs`。本夹具不使用 Root／Flow 公开签名，判据与
//! 拒绝码逐字承接：E0038。

use srflow::{Data, Node};

#[derive(Data)]
struct Amount(u32);

fn main() {
    // 期望主诊断：E0038——Node 不是对象安全 trait。
    let _: Option<&dyn Node<Input = Amount, Output = Amount>> = None;
}
