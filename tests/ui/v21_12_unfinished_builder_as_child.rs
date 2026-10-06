//! V21-12 外部负例：未完成的 FlowBuilder 不能作为 child 接线。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v21_12_unfinished_builder_as_child.rs -o /tmp/v21_12_unfinished_child.rmeta
//! ```
//! 预期失败：E0277——`OrchSig` 只接受实现 `OrchCall` 的完成态。

use srflow::{Data, DataRef, FlowBuilder, OrchSig};

fn main() {
    let (mut root, input) = FlowBuilder::<u32>::start().expect("root");
    let (child, _child_input) = FlowBuilder::<u32>::start().expect("child");
    let _: Result<DataRef<u32>, _> = root.then::<_, OrchSig<u32, Data<u32>>, _>(child, input);
}
