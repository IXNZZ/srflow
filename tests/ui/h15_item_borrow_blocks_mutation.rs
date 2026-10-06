//! H15 负例：真实 item 借用未结束时不能做会移动／清理的 Scope mutation。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h15_item_borrow_blocks_mutation.rs \
//!   -o /tmp/h15_item_borrow.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`。body 是自定义 Orchestrator，它在真实
//! ItemScope 内解析包装导入的 `CollectionItem` 输入（`resolve::<Item>` 返回 `&Item`），
//! 随后在借用仍活跃时调用会变更 Scope 的入口。预期错误在实际 mutation 行（E0502）。
//! 借用结束后可运行的正例见 `core::v21_08_tests::h15_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use std::marker::PhantomData;

use core::builder::{Definition, TypedCallBuilder};
use core::each::{EachBuilder, EachOnly};
use core::flow::FlowBuilder;
use core::orchestrator::{OrchCall, OrchScope, Targets1};
use core::signature::{Data, NodeFut, OrchSig};

struct Item(u32);
struct Out(u32);

/// 自定义 body：持 item 借用后尝试可变 Scope 操作。
struct BorrowingBody {
    definition: Definition,
}

impl OrchCall<(Item,), Data<Out>> for BorrowingBody {
    type Pack = Targets1<Item>;

    fn definition(&self) -> &Definition {
        &self.definition
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, Data<Out>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            // 只用合法受控读取能力：`ctx_probe` 提供只读 Context，pack 按声明类型解析。
            let borrowed: &Item = {
                let probe = scope.ctx_probe();
                scope.pack().first(probe, scope.child())?
            };
            // 借用跨 await 后仍要读取；随后调用会变更 Scope 的顺序 Step：必须在实际
            // mutation 行报 E0502（`run_steps` 需要 `&mut self`）。
            std::future::ready(()).await;
            scope.run_steps().await?;
            let _ = std::hint::black_box(borrowed.0);
            Ok(())
        })
    }
}

fn main() {
    let mut body_definition = Definition::new();
    let _ = body_definition.declare_input::<Item>("item").expect("input");
    let _ = body_definition
        .declare_output_port::<Out>("out")
        .expect("output");
    let body = BorrowingBody {
        definition: body_definition,
    };

    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let mut builder = EachBuilder::<EachOnly<Item>, Out>::start().expect("each");
    builder
        .then_body::<_, OrchSig<Item, Data<Out>>>(body)
        .expect("body");
    let each = builder.finish().expect("finish");
    let _ = parent.then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection);
    let _: PhantomData<()> = PhantomData;
}
