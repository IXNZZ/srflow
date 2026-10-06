//! D21 借用冲突负例：输入借用的 Future 仍将在之后使用时，Context mutation 被拒绝。
//!
//! 单独编译并确认在真实 mutation 行因 E0502 失败：
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/d21_active_input_future_blocks_mutation.rs \
//!   -o /tmp/d21_active_input_future_blocks_mutation.rmeta
//! ```
//!
//! 夹具装配真实非 test 内部模块；共享重借用、`resolve` 与 `register_owned` 都是生产入口。
//! 正例见 `core::context` 测试模块的 `d06_leaf_await_registers_output_once_and_unit_leaves_allocate_nothing`：
//! 同一结构下 await 语句结束、借用作用域关闭后才登记输出。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::ExecutionContext;
use core::ref_id::{RefIdAllocator, RefIdSource};
use core::runtime::RootExecution;

struct Item(u32);

struct Gate;

impl Gate {
    async fn wait(&self) {
        std::future::pending::<()>().await;
    }
}

/// 只接收 `&Item` 的业务形状叶子：持有输入借用。
async fn holding_leaf(input: &Item, gate: Gate) -> Item {
    gate.wait().await;
    Item(input.0)
}

fn main() {
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let root = execution.context().root_scope();
    let input = ids.allocate().unwrap();
    let output = ids.allocate().unwrap();
    let probe = ids.allocate().unwrap();

    let ctx: &mut ExecutionContext = execution.context_mut();
    ctx.register_owned(&root, &input, Item(1)).unwrap();

    // 共享重借用被 Future 持有：`&Item` 随 Future 存活。
    let future = {
        let shared: &ExecutionContext = &*ctx;
        let borrowed = shared.resolve::<Item>(&root, &input).unwrap();
        holding_leaf(borrowed, Gate)
    };

    // 该 Future 仍将在之后使用，因此这里不能可变借用 Context：预期 E0502。
    ctx.register_owned(&root, &probe, Item(2)).unwrap();

    // 把 Future 使用到最后，阻止借用检查器提前结束借用期。
    let _still_borrowed = future;
    let _ = output;
}