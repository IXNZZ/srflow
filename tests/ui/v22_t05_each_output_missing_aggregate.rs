//! T05 新增拒绝样本 ④：`each_output` 传入**未聚合** `Ref<A>`（漏 `Ref<Vec<A>>`）。
//!
//! `each_output` 的 `O` 表示**聚合后**的 RefShape：item Body 声明 `Ref<Amount>` 时，合法 `O` 是
//! `Ref<Vec<Amount>>`；把它写成未聚合的 `Ref<Amount>` 在 Rust 类型层面仍可表达，因此在
//! **Definition 构建期**被拒绝（`RunError::Definition`：聚合后形状与 `O` 不符），业务 Node 零调用。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we-t05-each-output-missing-aggregate-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Item(u32);

#[derive(Data)]
struct Amount(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn double(query: Query<&Item>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    let error = block_on(runtime.execute::<_, (), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            // O 标注为未聚合的 Ref<Amount>，而 item 声明 Ref<Amount> 应聚合为 Ref<Vec<Amount>>。
            let bad: Ref<Amount> = flow.each_output::<Item, Ref<Amount>, _>(items, |sub| {
                let item = sub.ref_of::<Item>();
                let value = sub.then_with_output(&double, item);
                sub.output_with(value);
            });
            let _ = bad;
            flow.output_with(());
        },
        vec![Item(1), Item(2), Item(3)],
    ))
    .err()
    .expect("漏聚合的 each_output 应在 Definition 构建期被拒绝");

    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "被拒绝的 Definition 不执行任何业务 Node"
    );

    println!("we_t05 each_output missing aggregate rejected: {error}");
    println!("we-t05-each-output-missing-aggregate-reject-ok");
}
