//! T03 外部可见性负例：Factory 包装类型仍未从 crate 根导出。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_from_t02_neg_extern_provide_node.rs \
//!   -o /tmp/v22_t03_from_t02_neg_extern_provide_node.rmeta
//! ```
//!
//! 预期主诊断：E0432（未从根导出），落在 use 行。
//!
//! T03 迁移：`Flow::provide`／`Flow::provide_output` 在 T03 已公开（正例见
//! `v22_t03_from_t02_neg_extern_provide.rs`／`…_provide_output.rs`），但承载它们的零输入包装
//! `ProvideNode` 仍是内部实现细节，未根导出：本夹具的调用点、断言与诊断阶段不变。

#![allow(unused_imports)]

use srflow::ProvideNode;

fn main() {}
