//! T02 外部可见性负例：Factory 包装类型未从 crate 根导出。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t02_neg_extern_provide_node.rs \
//!   -o /tmp/v22_t02_neg_extern_provide_node.rmeta
//! ```
//!
//! 预期主诊断：E0432（未从根导出），落在 use 行。

#![allow(unused_imports)]

use srflow::ProvideNode;

fn main() {}
