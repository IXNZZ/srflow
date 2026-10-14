//! WE-T05 P02 正例：`Ref<Box<(A, B)>>` 作为**单叶**聚合。
//!
//! SPEC §3.3：“单组合业务值可用已声明业务 struct，或明确的 `Box<(A, B)>` Data 叶子表达。
//! 后者产生 `Ref<Box<(A, B)>>`，只占一个位置”；SPEC §8 输出映射据此推出
//! `Ref<Box<(A, B)>> → Ref<Vec<Box<(A, B)>>>`（任务书 §2.2 明确列为**推定**，非 SPEC 逐字明文）。
//!
//! 本夹具断言结果确实是**一个** `Vec<Box<(Score, Report)>>`（而不是两个 `Vec` 槽），且每个
//! 元素的 box 内业务值完整保留。native tuple 本身不作为默认 Data 叶子，故不经 `Box` 无法表达。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-positive-ok`。

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Score(u32);
#[derive(Data)]
struct Report(u32);

/// 单叶组合业务值：`Box<(Score, Report)>` 占一个位置。
async fn bundle(query: Query<&Item>) -> Result<Box<(Score, Report)>, BodyError> {
    let value = query.get().0;
    Ok(Box::new((Score(value * 10), Report(value * 100))))
}

fn main() {
    let runtime = Runtime::new();

    // 单槽：一个 Ref<Vec<Box<(Score, Report)>>>，不是两个 Vec 槽。
    let bundled: Vec<Box<(Score, Report)>> = block_on(runtime.execute(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| each.then(&bundle, item))
        },
        vec![Item(1), Item(2), Item(3)],
    ))
    .expect("box-leaf each runs");

    assert_eq!(bundled.len(), 3, "one collected element per item");
    assert_eq!(
        bundled
            .iter()
            .map(|leaf| (leaf.0.0, leaf.1.0))
            .collect::<Vec<_>>(),
        vec![(10, 100), (20, 200), (30, 300)],
        "boxed business values survive aggregation in input order"
    );

    println!("we_t05-positive-ok");
}
