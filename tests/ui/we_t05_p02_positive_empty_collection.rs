//! WE-T05 P02／P12 正例：空集合的 `()`／空 Vec 输出，且 body 业务步骤零执行。
//!
//! SPEC §8：“空集合不调用 body，正常输出相应空 Vec 或 `()`”；任务书 §3.1：“**空集合不调用
//! Body** 指不执行其业务步骤，构建闭包及统一验证仍执行”。因此本夹具观察的是**业务节点执行
//! 计数为零**（闭包本身的 Rust 代码在构建期仍会运行一次，故计数器只放在 Node `run` 内）。
//!
//! 覆盖单输出／多输出／unit 三种形状在空集合下的输出，并断言没有任何业务 Node 被调用。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-positive-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Amount(u32);

/// 业务节点执行计数：空集合下必须保持 0。
static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn touch(query: Query<&Item>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0))
}

async fn audit(query: Query<&Item>) -> Result<(), BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn main() {
    let runtime = Runtime::new();
    let empty = || Vec::<Item>::new();

    // 1) 空集合 + unit：Each 输出为 ()，业务步骤零执行。
    CALLS.store(0, Ordering::SeqCst);
    let unit = block_on(runtime.execute(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                each.then(&audit, item);
            })
        },
        empty(),
    ))
    .expect("empty unit each runs");
    assert_eq!(unit, (), "empty unit each outputs ()");
    assert_eq!(CALLS.load(Ordering::SeqCst), 0, "empty collection runs no body step");

    // 2) 空集合 + 单输出：空 Vec，业务步骤零执行。
    let amounts = block_on(runtime.execute(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| each.then(&touch, item))
        },
        empty(),
    ))
    .expect("empty single-output each runs");
    assert!(amounts.is_empty(), "empty collection yields an empty Vec");
    assert_eq!(CALLS.load(Ordering::SeqCst), 0, "empty collection runs no body step");

    // 3) 空集合 + 多输出：每槽都是空 Vec，业务步骤零执行。
    let (first, second) = block_on(runtime.execute(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let first = each.then(&touch, item);
                let second = each.then(&touch, item);
                (first, second)
            })
        },
        empty(),
    ))
    .expect("empty multi-output each runs");
    assert!(first.is_empty() && second.is_empty(), "each slot is an empty Vec");
    assert_eq!(CALLS.load(Ordering::SeqCst), 0, "empty collection runs no body step");

    println!("we_t05-positive-ok");
}
