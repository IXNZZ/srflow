//! T03 迁移自 `we_t05_p02_positive_sixteen_slots.rs`（WE-T05 P02 正例）：16 槽 flat 输出的**完整
//! arity** 边界（SPEC §3.2 的 16 位置上限）。新契约下 Root 输出改由 [`Flow::output_with`] 声明
//! （定义闭包固定返回 `()`），显式连接更名为 `then_with_output`；原逐槽断言不变。
//!
//! 覆盖任务书 §8 的 P02“完整 arity 正例”：`Ref<O1> … Ref<O16>` 全部 16 个 flat 槽各形成
//! `Ref<Vec<O>>`；本夹具用**同一 Data 类型**占据 16 个槽（同类型不同逻辑位置）并逐槽断言
//! 元素值与顺序，从而不依赖 arity 命名、也不依赖逐槽不同类型。
//!
//! 第 17 槽的拒绝见 `v22_t03_from_we_t05_p02_each_body_output_17_slots.rs` 与
//! `v22_t03_from_we_t05_p03_each_seventeen_slots_child_chain.rs`。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-positive-ok`。

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Mark(u32);

/// 16 个槽共用同一节点类型：每次调用都是独立逻辑位置。
async fn slot(query: Query<&Item>) -> Result<Mark, BodyError> {
    Ok(Mark(query.get().0))
}

/// 逐槽断言：每槽都收到按输入顺序的 2 个元素。
fn check(name: &str, values: &[Mark]) {
    assert_eq!(
        values.iter().map(|value| value.0).collect::<Vec<_>>(),
        vec![1, 2],
        "slot {name} collects per item in input order"
    );
}

fn main() {
    let runtime = Runtime::new();

    let out = block_on(runtime.execute::<
        _,
        (
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
            Vec<Mark>,
        ),
        _,
    >(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each_output::<
                Item,
                (
                    Ref<Vec<Mark>>, Ref<Vec<Mark>>, Ref<Vec<Mark>>, Ref<Vec<Mark>>,
                    Ref<Vec<Mark>>, Ref<Vec<Mark>>, Ref<Vec<Mark>>, Ref<Vec<Mark>>,
                    Ref<Vec<Mark>>, Ref<Vec<Mark>>, Ref<Vec<Mark>>, Ref<Vec<Mark>>,
                    Ref<Vec<Mark>>, Ref<Vec<Mark>>, Ref<Vec<Mark>>, Ref<Vec<Mark>>,
                ),
                _,
            >(items, |sub| {
                let item = sub.ref_of::<Item>();
                let c01 = sub.then_with_output(&slot, item);
                let c02 = sub.then_with_output(&slot, item);
                let c03 = sub.then_with_output(&slot, item);
                let c04 = sub.then_with_output(&slot, item);
                let c05 = sub.then_with_output(&slot, item);
                let c06 = sub.then_with_output(&slot, item);
                let c07 = sub.then_with_output(&slot, item);
                let c08 = sub.then_with_output(&slot, item);
                let c09 = sub.then_with_output(&slot, item);
                let c10 = sub.then_with_output(&slot, item);
                let c11 = sub.then_with_output(&slot, item);
                let c12 = sub.then_with_output(&slot, item);
                let c13 = sub.then_with_output(&slot, item);
                let c14 = sub.then_with_output(&slot, item);
                let c15 = sub.then_with_output(&slot, item);
                let c16 = sub.then_with_output(&slot, item);
                sub.output_with((
                    c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15,
                    c16,
                ));
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ))
    .expect("16-slot each runs");

    let (
        c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16,
    ) = out;
    check("01", &c01);
    check("02", &c02);
    check("03", &c03);
    check("04", &c04);
    check("05", &c05);
    check("06", &c06);
    check("07", &c07);
    check("08", &c08);
    check("09", &c09);
    check("10", &c10);
    check("11", &c11);
    check("12", &c12);
    check("13", &c13);
    check("14", &c14);
    check("15", &c15);
    check("16", &c16);

    println!("we_t05-positive-ok");
}
