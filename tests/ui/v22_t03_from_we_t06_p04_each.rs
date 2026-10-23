//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_p04_each.rs`）：Each（SPEC §8）。
//!
//! 原目的（X24/S04／C13／C15）：普通集合（`Ref<Vec<Item>>`）的单输出逐项聚合与输入顺序、
//! unit body 输出，以及 **item-backed** 集合（外层 Each 的 item 自身是 `Ref<Vec<..>>`，其元素
//! 再成集合）。
//!
//! 迁移手法：旧两参 `flow.then(node, refs)`（含 item body 内的 `each.then(..)`）改为
//! `then_with_output`；只有 Root 闭包改为固定返回 `()` 并经 `output_with` 声明 owned `Out`
//! （分别为 `Vec<Score>`、`()`、`Vec<Vec<Score>>`）。Each 的 child 契约、空内层语义与断言不变。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, Runtime};

#[derive(Data)]
struct Item(u32);

#[derive(Data)]
struct Score(u32);

/// unit body 的逐项执行计数。
static AUDITED: AtomicUsize = AtomicUsize::new(0);

/// 单输出：每个 item 产生一个新的 owned 值。
async fn score_of(query: Query<&Item>) -> Result<Score, BodyError> {
    Ok(Score(query.get().0 * 10))
}

async fn audit(query: Query<&Item>) -> Result<(), BodyError> {
    assert!(query.get().0 > 0, "item value stays readable inside the body");
    AUDITED.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn main() {
    let runtime = Runtime::new();

    // 1) 普通集合：Ref<O> -> Ref<Vec<O>>，元素按输入顺序。
    let scores = block_on(runtime.execute::<_, Vec<Score>, _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each(items, |item, each| each.then_with_output(&score_of, item));
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3)],
    ))
    .expect("single-output each runs");
    assert_eq!(
        scores.iter().map(|score| score.0).collect::<Vec<_>>(),
        vec![10, 20, 30],
        "单输出按输入顺序逐项聚合"
    );

    // 2) unit body 输出：不产生假业务槽，但每项真实执行一次。
    AUDITED.store(0, Ordering::SeqCst);
    let unit = block_on(runtime.execute::<_, (), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each(items, |item, each| {
                each.then_with_output(&audit, item);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3), Item(4)],
    ))
    .expect("unit each runs");
    assert_eq!(unit, (), "unit body maps to ()");
    assert_eq!(AUDITED.load(Ordering::SeqCst), 4, "每项恰执行一次");

    // 3) item-backed：外层 Each 的 item 自身是 Ref<Vec<Item>>（元素的元素再成集合）。
    let nested = block_on(runtime.execute::<_, Vec<Vec<Score>>, _>(
        |flow: &mut Flow<'_>, groups: Ref<Vec<Vec<Item>>>| {
            let out_refs = flow.each(groups, |inner, outer_each| {
                outer_each.each(inner, |item, inner_each| {
                    inner_each.then_with_output(&score_of, item)
                })
            });
            flow.output_with(out_refs);
        },
        vec![vec![Item(1), Item(2)], vec![Item(3)], vec![]],
    ))
    .expect("item-backed nested each runs");
    let flattened = nested
        .iter()
        .map(|group| group.iter().map(|score| score.0).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(
        flattened,
        vec![vec![10, 20], vec![30], vec![]],
        "item-backed 集合逐组、逐项按输入顺序聚合，空内层给出空 Vec"
    );

    println!("we_t06 p04 scores={:?}", flattened);
    println!("we_t06-positive-ok");
}
