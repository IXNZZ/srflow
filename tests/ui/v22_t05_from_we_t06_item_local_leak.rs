//! T05 迁移自 `v22_t03_from_we_t06_item_local_leak.rs`（原 WE-T06）：item-local 位置泄漏到 parent。
//!
//! 原目的（X24/X05／C13／C15）：Each 的 item 是集合元素的受限借用，其 owner 是 item Body：只在
//! item 内词法可见（SPEC §8）。把 item 带到 item Body 之外（parent／Root）使用，必须在 Definition
//! 构建期按 `OutOfScope(ChildLocal)` 拒绝（诊断含 "is not lexically visible"），业务 Node 零调用。
//!
//! 判定与归因：**Definition 构建期拒绝**（`RunError::Definition`），不是 rustc 拒绝。
//!
//! 迁移手法：Each 改为 item Body 单参数写法，item 经 `sub.ref_of::<Item>()` 取回后写入
//! `leaked`（仍是 item-local 受限借用）；聚合声明改用
//! `flow.each_output::<Item, Ref<Vec<Mark>>, _>`，parent 内 `review` 的显式连接仍为
//! `then_with_output`。泄漏路径与零调用断言不变。
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

async fn review(query: Query<&Item>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    let leaked: Cell<Option<Ref<Item>>> = Cell::new(None);
    let error = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let declared = flow.each_output::<Item, Ref<Vec<Mark>>, _>(items, |sub| {
                let item = sub.ref_of::<Item>();
                leaked.set(Some(item));
                let marked = sub.then_with_output(&touch, item);
                sub.output_with(marked);
            });
            // declared 是 Each 的声明输出（合法）；leaked 是 item-local 受限借用（非法）。
            let _ = declared;
            let out_refs = flow.then_with_output(&review, leaked.get().expect("item-local ref was captured"));
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ))
    .err()
    .expect("an item-local Ref used in the parent is rejected at build time");

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

    println!("we_t06 item-local leak: {error}");
    println!("we_t06-build-reject-ok");
}
