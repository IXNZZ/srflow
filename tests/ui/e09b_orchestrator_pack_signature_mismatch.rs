//! E09b 负例：Orchestrator 的输入 pack 类型必须与正式输入 Signature 一致。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/e09b_orchestrator_pack_signature_mismatch.rs \
//!   -o /tmp/e09b.rmeta
//! ```
//!
//! 夹具按真实 `src/core/mod.rs` 装配，不使用 `--cfg test`。预期错误在 `type Pack` 行
//! （E0277：`Targets1<String>` 不满足 `PackFor<(u32,)>`），错误 pack 类型在编译期就不成立，
//! 不会拖到执行期 Import 才失败。正例见 `core::v21_05_tests::e06_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::Definition;
use core::orchestrator::{OrchCall, OrchScope, Targets1};
use core::signature::{Data, NodeFut};

struct WrongPack {
    inner: Definition,
}

impl OrchCall<(u32,), Data<u32>> for WrongPack {
    type Pack = Targets1<String>;

    fn definition(&self) -> &Definition {
        &self.inner
    }

    fn run<'a>(&'a self, _scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

fn main() {
    let inner = Definition::new();
    let _ = WrongPack { inner };
}
