//! H26/R1 负例：普通 Orchestrator 不能取得可变 Context 或直接注入 owned 业务 Data。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h26_orchestrator_cannot_get_mutable_context.rs \
//!   -o /tmp/h26_mut_ctx.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`。`OrchScope` 只提供只读视图
//! （`ctx_probe`／`pack`／`child`）与受控执行入口（`run_steps`／`run_registered_site`）；
//! 它**不**提供把 `&mut ExecutionContext` 交给编排体的通用拆分。预期错误在 `into_parts`
//! 调用行（E0599：该方法不存在），构成"无 Node 直接生成业务 Data"的旁路已被移除的证据。

#[path = "../../src/core/mod.rs"]
mod core;

use std::marker::PhantomData;

use core::builder::Definition;
use core::orchestrator::{OrchCall, OrchScope, Targets1};
use core::signature::{Data, NodeFut};

struct Item(u32);
struct Out(u32);

/// 普通 Orchestrator：尝试取得可变 Context 并直接登记业务值（预期编译失败）。
struct InjectingBody {
    definition: Definition,
}

impl OrchCall<(Item,), Data<Out>> for InjectingBody {
    type Pack = Targets1<Item>;

    fn definition(&self) -> &Definition {
        &self.definition
    }

    fn run<'a>(&'a self, scope: OrchScope<'a, Self::Pack, Data<Out>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            // 三条都会失败：`OrchScope` 不提供可变 Context 或 owned 注入入口。
            let parts = scope.into_parts();
            let _ = scope.ctx_mut();
            let _ = scope.register_owned(&Out(1234));
            let _: PhantomData<()> = PhantomData;
            let _ = parts;
            Ok(())
        })
    }
}

fn main() {
    let mut definition = Definition::new();
    let _ = definition.declare_input::<Item>("item").expect("input");
    let _ = definition
        .declare_output_port::<Out>("out")
        .expect("output");
    let _ = InjectingBody { definition };
}
