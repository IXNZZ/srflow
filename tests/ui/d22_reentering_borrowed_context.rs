//! D22 借用冲突负例：首个 child Future 仍独占借用 Context 时，无法再进入第二个异步调用。
//!
//! 单独编译并确认在第二次真实异步调用入口因 E0499 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/d22_reentering_borrowed_context.rs \
//!   -o /tmp/d22_reentering_borrowed_context.rmeta
//! ```
//!
//! 两个 child Future 都通过真实非 test 入口 `core::context::invoke_leaf` 建立、同时存活，
//! 且首个在之后仍被使用。正例见 `core::context` 测试模块的
//! `d04_frame_stack_restores_and_orders`：顺序 await 各层调用并逐层恢复 caller frame。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::{ExecutionContext, invoke_leaf};
use core::ref_id::{RefIdAllocator, RefIdSource};
use core::runtime::RootExecution;

struct Item(u32);

struct Gate;

impl Gate {
    async fn wait(&self) {
        std::future::pending::<()>().await;
    }
}

async fn holding_leaf(input: &Item, gate: Gate) -> Item {
    gate.wait().await;
    Item(input.0 + 1)
}

fn main() {
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let root = execution.context().root_scope();
    let input = ids.allocate().unwrap();
    let first_output = ids.allocate().unwrap();
    let second_output = ids.allocate().unwrap();

    let ctx: &mut ExecutionContext = execution.context_mut();
    ctx.register_owned(&root, &input, Item(1)).unwrap();

    // 第一个 child Future 持有 Context 的可变借用。
    let first = invoke_leaf(
        ctx,
        &root,
        &input,
        &first_output,
        Gate,
        holding_leaf,
    );

    // 首个 Future 仍将在之后使用：第二个异步调用入口预期 E0499。
    let second = invoke_leaf(
        ctx,
        &root,
        &input,
        &second_output,
        Gate,
        holding_leaf,
    );

    let _ = (first, second);
}