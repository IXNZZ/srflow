//! G11 负例：受控选择入口只接受**本 Definition 自己登记表**的索引，不能传入外来调用表。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/g11_match_selection_takes_no_foreign_table.rs \
//!   -o /tmp/g11.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`；预期错误在 `run_registered_site` 调用行
//! （E0061：该方法只接受一个索引参数）。正例见 `core::v21_07_tests::m15_index_out_of_range_*`
//! 与 M02～M23 的合法 Match。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::{BuildSite, CallSite, Definition, TypedCallBuilder};
use core::context::BodyError;
use core::orchestrator::{OrchCall, OrchScope, Targets1};
use core::signature::{Data, NodeFut, SyncFnSig, Unit};

fn foreign_node() -> Result<u32, BodyError> {
    Ok(123)
}

struct Injected {
    own: Definition,
    sites: Vec<CallSite>,
}

impl OrchCall<(u32,), Unit> for Injected {
    type Pack = Targets1<u32>;

    fn definition(&self) -> &Definition {
        &self.own
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Unit>) -> NodeFut<'a, ()> {
        Box::pin(async move { scope.run_registered_site(&self.sites, 0).await })
    }
}

fn main() {
    let mut own = Definition::new();
    let _ = own.declare_input::<u32>("own input").expect("input");
    let mut foreign = Definition::new();
    let out = foreign
        .declare_output_port::<u32>("foreign output")
        .expect("port");
    let site = <_ as BuildSite<SyncFnSig<(), Data<u32>>, ()>>::site(
        foreign_node,
        (),
        &[out.position().clone()],
    );
    let injected = Injected {
        own,
        sites: vec![site],
    };
    let mut parent = Definition::new();
    let input = parent.declare_input::<u32>("root input").expect("input");
    let _ = parent.then(injected, input);
}
