//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_child_local_leak.rs`）：child-local 位置泄漏到 parent。
//!
//! 原目的（X24/X05／C13／C15）：Chain 建立真实 child 数据生命周期边界：child 内部的逻辑位置
//! 属于 child Body，只能经该结构的**声明输出**转为 parent 可见的新 Ref（SPEC §7）。把 child
//! 内部的 `Ref` 直接带到 parent 使用，必须在 Definition 构建期按 `OutOfScope(ChildLocal)` 拒绝
//! （诊断含 "is not lexically visible"），业务 Node 零调用。
//!
//! 判定与归因：**Definition 构建期拒绝**（`RunError::Definition`），不是 rustc 拒绝。
//!
//! 迁移手法：Root 闭包改为固定返回 `()` 并经 `output_with` 声明 owned `Out = Amount`；旧两参
//! `flow.then(node, refs)`（含 child 内与 parent 内各一次）改为 `then_with_output`。泄漏路径、
//! `Cell` 捕获与零调用断言不变。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t06-build-reject-ok`。

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Item(u32);

#[derive(Data)]
struct Mark(u32);

#[derive(Data)]
struct Amount(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn touch(query: Query<&Item>) -> Result<Mark, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Mark(query.get().0))
}

async fn review(query: Query<&Mark>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    // child 内部的 Ref 经共享单元带出 chain 的边界（Rust 层面完全合法）。
    let leaked: Cell<Option<Ref<Mark>>> = Cell::new(None);
    let error = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, item: Ref<Item>| {
            let declared = flow.chain(|sub| {
                let internal = sub.then_with_output(&touch, item);
                leaked.set(Some(internal));
                internal
            });
            // declared 是 child 的声明输出（合法）；leaked 是 child-local 内部位置（非法）。
            let _ = declared;
            let out_refs = flow.then_with_output(&review, leaked.get().expect("internal ref was captured"));
            flow.output_with(out_refs);
        },
        Item(5),
    ))
    .err()
    .expect("a child-local Ref used in the parent is rejected at build time");

    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );
    assert!(
        error.to_string().contains("lexically visible"),
        "诊断应指出词法可见性越界：{error}"
    );
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "被拒绝的 Definition 不执行任何业务 Node"
    );

    println!("we_t06 child-local leak: {error}");
    println!("we_t06-build-reject-ok");
}
