//! K24 负例：Node 只能接收业务借用，不能取得 Context／提取入口。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/k05_node_cannot_extract.rs -o /tmp/k05.rmeta
//! ```
//! 预期：E0050（trait 方法参数个数不符）——Node 调用签名只接受 `&A`，不接受 Context 参数。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::ExecutionContext;
use core::node::NodeCall1;
use core::signature::{Data, NodeFut};

struct Left(u32);
struct Probe;

impl NodeCall1<Left, Data<u32>> for Probe {
    // 期望：E0050——Node 拿不到 Context。
    fn call<'a>(&'a self, ctx: &'a mut ExecutionContext, left: &'a Left) -> NodeFut<'a, u32> {
        let _ = ctx;
        Box::pin(async move { Ok(left.0) })
    }
}

fn main() {}
