//! V21-12 外部负例：零输入 Flow 不可表达（Flow Input 非空是硬约束）。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v21_12_zero_input_flow_is_not_supported.rs -o /tmp/v21_12_zero_flow.rmeta
//! ```
//! 预期失败：E0282——未提供输入类型，Builder 无法推断输入 Data 类型。

use srflow::FlowBuilder;

fn main() {
    let _ = FlowBuilder::start();
}
