//! F24 负例（外部视角）：业务侧不能取得完整 Flow／FlowBuilder 内部类型。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/f24_flow_names_are_not_reachable.rs -o /tmp/f24.rmeta
//! ```
//!
//! 预期失败：`core::flow` 对外不可见（E0432），其中类型不可解析（E0603）。

use srflow::core::flow::{Flow, FlowBuilder};

fn main() {
    let _ = std::mem::size_of::<Option<Flow<(u32,), u32>>>();
    let _ = std::mem::size_of::<Option<FlowBuilder<(u32,)>>>();
}
