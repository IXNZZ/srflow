//! E09 负例：Orchestrator 的输入 Signature 与接线位置类型不符。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/e09_orchestrator_wrong_signature.rs \
//!   -o /tmp/e09.rmeta
//! ```
//!
//! 预期错误在 `then` 调用行（E0277：编排体声明 u32 输入，接线给 String 位置）。
//! 正例见 `core::v21_05_tests::e06_*` 与 `e15_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::{Definition, TypedCallBuilder};
use core::context::BodyError;
use core::signature::NodeFut;
use core::orchestrator::{OrchCall, OrchScope, Targets1};
use core::signature::Data;

struct Pair {
    inner: Definition,
}

impl OrchCall<(u32,), Data<u32>> for Pair {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, _scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

fn main() {
    let inner = Definition::new();
    let orchestrator = Pair { inner };
    let mut definition = Definition::new();
    let text = definition.declare_input::<String>("t").expect("position");
    let _ = definition.then(orchestrator, text);
}
