//! V21-12 外部负例：`Arc<dyn Node>` 不受支持（只支持 `Arc<具体 Node>`）。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v21_12_arc_dyn_node_is_not_supported.rs -o /tmp/v21_12_arc_dyn.rmeta
//! ```
//! 预期失败：E0277——`Arc<dyn NodeCall1<..>>` 不满足具体 Node 协议约束。

use std::sync::Arc;

use srflow::{ArcNodeSig, BodyError, Data, DataRef, FlowBuilder, NodeCall1, NodeFut};

struct Doubler;
impl NodeCall1<u32, Data<u32>> for Doubler {
    fn call<'a>(&'a self, a: &'a u32) -> NodeFut<'a, u32> {
        Box::pin(async move { Ok(a * 2) })
    }
}

fn main() {
    let (mut flow, input) = FlowBuilder::<u32>::start().expect("start");
    let dynamic: Arc<dyn NodeCall1<u32, Data<u32>>> = Arc::new(Doubler);
    let _: DataRef<u32> = flow
        .then::<_, ArcNodeSig<(u32,), Data<u32>>, _>(dynamic, input)
        .unwrap();
    let _ = BodyError::new("unused");
}
