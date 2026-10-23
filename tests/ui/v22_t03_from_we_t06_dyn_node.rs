//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_dyn_node.rs`）：`dyn Node` 不受支持。
//!
//! 原目的（X24／C13／C15）：`Node` 协议含 `async fn run`（RPITIT）与关联类型，不构成
//! 对象安全 trait；SPEC §5.3 明确 `dyn Node` 不在本版支持范围。外部消费者以 crate 根的
//! 公开 `Node` 书写即可观察该拒绝。
//!
//! 迁移手法：T03 公开签名切换不触及 `Node` 的对象安全性与根导出，本夹具**逐字保留**，
//! 只更新文件头的迁移说明。归因依旧是 **rustc 编译拒绝**（trait 非对象安全）。
//!
//! 期望主诊断：E0038（`Node` cannot be made into an object）。

use srflow::{Data, Node};

#[derive(Data)]
struct Amount(u32);

fn main() {
    let _: Option<&dyn Node<Input = Amount, Output = Amount>> = None;
}
