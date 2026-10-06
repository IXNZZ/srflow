//! G10 负例：Orchestrator 声明的输入 pack 与其输入 Signature 不符 → 编译失败（`PackFor`）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/g10_match_pack_bound_mismatch.rs \
//!   -o /tmp/g10.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`；预期错误（E0277：
//! `Targets1<u32>: PackFor<(u32, u64)>` 不成立）在 trait 约束处，说明 pack 与 Signature
//! 的绑定在**编译期**生效，不会留到执行期 Import。对应 `Match<R, A, K>` 使用
//! `Pack = Targets2<R, A>`（见 `match_orchestrator.rs`）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::Definition;
use core::context::BodyError;
use core::orchestrator::{OrchCall, OrchScope, Targets1};
use core::signature::{Data, NodeFut};

struct Bad;

impl OrchCall<(u32, u64), Data<u32>> for Bad {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        unimplemented!()
    }

    fn run<'a>(&'a self, _scope: OrchScope<'a, Self::Pack, Data<u32>>) -> NodeFut<'a, ()> {
        Box::pin(async move { Ok::<(), BodyError>(()) })
    }
}

fn main() {
    fn assert_pack<O: OrchCall<(u32, u64), Data<u32>>>() {}
    assert_pack::<Bad>();
}
