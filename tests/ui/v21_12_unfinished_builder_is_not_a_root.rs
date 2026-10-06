//! V21-12 外部负例：未完成的公开 FlowBuilder 不能直接作为 Root 执行。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v21_12_unfinished_builder_is_not_a_root.rs -o /tmp/v21_12_unfinished.rmeta
//! ```
//! 预期失败：E0277——`Runtime::execute` 只接受完成态 Orchestrator。

use srflow::{Data, FlowBuilder, Runtime};

fn main() {
    let (builder, _input) = FlowBuilder::<u32>::start().expect("start");
    let _ = Runtime::execute::<_, _, Data<u32>>(&builder, (1u32,));
}
