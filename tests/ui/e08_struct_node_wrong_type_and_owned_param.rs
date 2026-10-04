//! E08 负例：具体 Node 协议的错类型输入，以及把借用参数写成 owned 参数。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/e08_struct_node_wrong_type_and_owned_param.rs \
//!   -o /tmp/e08.rmeta
//! ```
//!
//! 夹具按真实 `src/core/mod.rs` 装配，不使用 `--cfg test`；预期错误：
//! - `then` 调用行：E0277（`Node1` 未实现 String 输入的协议）；
//! - `OwnedParam` 的协议实现处：E0053（诊断要求借用参数 `&'a A`）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::{Definition, TypedCallBuilder};
use core::context::BodyError;
use core::node::NodeCall1;
use core::signature::NodeFut;
use core::signature::Data;

struct Node1;

impl NodeCall1<u32, Data<u32>> for Node1 {
    fn call<'a>(&'a self, a: &'a u32) -> NodeFut<'a, u32> {
        Box::pin(async move { Ok(*a) })
    }
}

struct OwnedParam;

impl NodeCall1<u32, Data<u32>> for OwnedParam {
    fn call<'a>(&'a self, a: u32) -> NodeFut<'a, u32> {
        Box::pin(async move { Ok(a) })
    }
}

fn main() {
    let mut definition = Definition::new();
    let text = definition.declare_input::<String>("t").expect("position");
    let _ = definition.then(Node1, text);
    let mut second = Definition::new();
    let number = second.declare_input::<u32>("n").expect("position");
    let _ = second.then(OwnedParam, number);
}
