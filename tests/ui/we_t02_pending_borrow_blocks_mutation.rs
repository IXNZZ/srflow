//! WE-T02 B08 内部借用证据：Node 输入的只读借用存活期间，不能对 Context 做可变操作。
//!
//! 本夹具是**内部 path 装配**样本（`#[path]` 引用真实 `src/core/mod.rs`），不冒充外部
//! 消费者：它以真实 `RootExecution` / `ExecutionContext` / `InvocationGuard` 固定
//! 「`resolve` 返回的 `&T` 未结束时，需要 `&mut` 的受控入口不可调用」这一编译期边界。
//! 对外公开面侧不存在该入口的编译证据（业务侧拿不到 Context），故此处以 core 模式验证。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/we_t02_pending_borrow_blocks_mutation.rs -o /tmp/x.rmeta
//! ```
//!
//! 期望主诊断：E0502（`borrowed` 仍被借用时又可变借用 guard），落在本文件调用
//! `guard.register_owned(&root, &other, 7u32)` 的行。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::context::InvocationKind;
use crate::core::ref_id::{RefIdAllocator, RefIdSource};
use crate::core::runtime::RootExecution;

fn main() {
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let allocator = RefIdAllocator::new(RefIdSource::new());
    let position = allocator.allocate().expect("fresh source allocates");
    let other = allocator.allocate().expect("fresh source allocates");

    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("a fresh execution accepts its root frame");
    let _ = guard
        .register_owned(&root, &position, 5u32)
        .expect("owned value registers into the root scope");

    // 只读借用在此存活。
    let borrowed: &u32 = guard
        .resolve::<u32>(&root, &position)
        .expect("the registered value resolves");

    // 期望主诊断：E0502——borrowed 未结束时不能可变借用 Context。
    guard
        .register_owned(&root, &other, 7u32)
        .expect("second registration");

    // 让只读借用实际跨越上面的可变调用，避免提前结束借用。
    println!("we_t02-core-borrow-ok {borrowed}");
}
