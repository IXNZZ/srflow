//! T03 迁移自 `we_t05_p02_positive_unit_and_single.rs`（WE-T05 P02 正例）：unit 与单输出 Each 的
//! 真实运行。新契约下 Root 输出改由 [`Flow::output_with`] 声明（定义闭包固定返回 `()`），显式
//! 连接更名为 `then_with_output`；原断言与运行语义不变。
//!
//! 覆盖任务书 §1 输出映射表与 §8 的 P02／P11（公开部分）：
//!
//! 1. `()` body 输出 → `()` Each 输出：不建 collector、不产生假业务槽，但每项真实执行；
//!    以业务计数器观察“每项恰执行一次”（不靠 `is_some` 或引用非空）。
//! 2. `Ref<O>` body 输出 → `Ref<Vec<O>>`：元素按输入顺序，值可断言。
//!
//! SPEC §8：“空集合不调用 body，正常输出相应空 Vec 或 `()`”——空集合样本见
//! `v22_t03_from_we_t05_p02_positive_empty_collection.rs`。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-positive-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Amount(u32);

/// unit body 的业务步骤计数：每项成功执行一次。
static AUDITED: AtomicUsize = AtomicUsize::new(0);

/// unit 输出：只做业务副作用，不产生业务槽。
async fn audit(query: Query<&Item>) -> Result<(), BodyError> {
    let item = query.get();
    assert!(item.0 > 0, "item value stays readable");
    AUDITED.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

/// 单输出：每个 item 产生一个新的 owned 值。
async fn double(query: Query<&Item>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();

    // 1) unit：Each 输出为 ()，body 每项恰执行一次。
    AUDITED.store(0, Ordering::SeqCst);
    let unit = block_on(runtime.execute::<_, (), _>(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            let () = flow.each(items, |item, each| {
                each.then_with_output(&audit, item);
            });
            flow.output_with(());
        },
        vec![Item(1), Item(2), Item(3)],
    ))
    .expect("unit each runs");
    assert_eq!(unit, (), "unit body maps to ()");
    assert_eq!(
        AUDITED.load(Ordering::SeqCst),
        3,
        "unit body ran exactly once per item"
    );

    // 2) 单输出：Ref<O> -> Ref<Vec<O>>，元素按输入顺序。
    let amounts = block_on(runtime.execute::<_, Vec<Amount>, _>(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each(items, |item, each| each.then_with_output(&double, item));
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3)],
    ))
    .expect("single-output each runs");
    assert_eq!(
        amounts.iter().map(|amount| amount.0).collect::<Vec<_>>(),
        vec![2, 4, 6],
        "single output collects one value per item in input order"
    );

    println!("we_t05-positive-ok");
}
