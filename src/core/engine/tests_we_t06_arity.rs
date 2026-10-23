//! WE-T06 §5.2／X03：arity 覆盖缺口的补齐样本（A 阶段，legacy 仍在位但本文件不使用）。
//!
//! 依据 `docs/workflow_engine_tasks/WE_T06_PROBE/arity_matrix.md` §2／§4／§6 的两个实跑缺口：
//! 1. Each Output 的 5～15 代表 arity 此前无实跑样本（现有 0／1／2／3／4／16）：此处取
//!    **8 槽**为代表值，走真实 `Runtime::execute`，逐槽断值并断言 8 个 collector 身份互异；
//! 2. 泛型 `#[derive(Data)]` 类型此前只被构造、未作为真实数据流叶子：此处以 `Wrapper<T>`
//!    作为 Root 输入／Node 输入／Node 输出／Root 输出与 Each 集合元素真实参与 execute，
//!    并断言实际值与所有权（构造次数 = 析构次数，不泄漏、不重复析构）。
//!
//! 归属：本文件由 WE-T06 的 arity 补齐工作项独占。身份观察为只读
//! （`test_support::take_log_snapshot`），不改动任何生产语义。

#![allow(clippy::type_complexity)]

use crate::Data;
use crate::core::engine::error::{BodyError, RunError};
use crate::core::engine::flow::Flow;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, Ref};
use crate::core::identity::DataId;
use crate::core::test_support;
use futures::executor::block_on;
use std::cell::Cell;
use std::rc::Rc;

// ================================================================ 业务类型

#[derive(Data, Debug, PartialEq, Eq)]
struct Item(u32);

#[derive(Data, Debug, PartialEq, Eq)]
struct Out(u32);

/// 泛型 `#[derive(Data)]` 叶子：既作为 Root／Node 输入输出，也作为 Each 集合元素。
#[derive(Data)]
struct Wrapper<T>(T);

/// 带析构观察的载荷：证明所有权随真实数据流转移且恰好析构一次。
struct Payload {
    value: u32,
    created: Rc<Cell<usize>>,
    dropped: Rc<Cell<usize>>,
}

impl Payload {
    fn new(value: u32, created: &Rc<Cell<usize>>, dropped: &Rc<Cell<usize>>) -> Self {
        created.set(created.get() + 1);
        Self {
            value,
            created: Rc::clone(created),
            dropped: Rc::clone(dropped),
        }
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
    }
}

// ================================================================ Node 业务函数

/// 每个槽一个常数偏置节点：`item * 100 + bias`，bias 1..=8，使 8 个槽的聚合值两两不同。
macro_rules! slot_nodes {
    ($($name:ident = $bias:expr;)+) => {
        $(
            async fn $name(query: Query<&Item>) -> Result<Out, BodyError> {
                Ok(Out(query.get().0 * 100 + $bias))
            }
        )+
    };
}

slot_nodes!(
    slot1 = 1;
    slot2 = 2;
    slot3 = 3;
    slot4 = 4;
    slot5 = 5;
    slot6 = 6;
    slot7 = 7;
    slot8 = 8;
);

/// 读取借用的 `Wrapper<Payload>` 并产生一个**新的** owned `Wrapper<Payload>`：
/// 输入以借用进入，输出以所有权离开。
async fn bump_wrapper(query: Query<&Wrapper<Payload>>) -> Result<Wrapper<Payload>, BodyError> {
    let inner = &query.get().0;
    Ok(Wrapper(Payload::new(
        inner.value + 1,
        &inner.created,
        &inner.dropped,
    )))
}

/// 泛型叶子作为 Each 集合元素的 Body：每项翻倍后以新的 `Wrapper<u32>` 离开。
async fn double_wrapper(query: Query<&Wrapper<u32>>) -> Result<Wrapper<u32>, BodyError> {
    Ok(Wrapper(query.get().0 * 2))
}

/// 每样本起点：清空 take 身份记录（只读观察），使身份断言只反映本样本。
fn reset() {
    test_support::take_log_reset();
}

// ================================================================ 缺口 1：Each Output 代表 arity

/// Each Output 代表中间 arity（8 槽）：真实 `execute` 逐槽断值 + 8 个 collector 身份互异。
#[test]
fn each_eight_slot_body_output_runs_with_distinct_collector_identities() {
    reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
    ), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each(items, |item, sub| {
                (
                    sub.then_with_output(slot1, item),
                    sub.then_with_output(slot2, item),
                    sub.then_with_output(slot3, item),
                    sub.then_with_output(slot4, item),
                    sub.then_with_output(slot5, item),
                    sub.then_with_output(slot6, item),
                    sub.then_with_output(slot7, item),
                    sub.then_with_output(slot8, item),
                )
            });
            flow.output_with(out_refs);
        },
        vec![Item(3), Item(5), Item(7)],
    ));
    let (s1, s2, s3, s4, s5, s6, s7, s8) = outcome.expect("8-slot each succeeds");

    macro_rules! check_slot {
        ($values:expr, $bias:expr) => {
            assert_eq!(
                $values.iter().map(|out| out.0).collect::<Vec<u32>>(),
                vec![300 + $bias, 500 + $bias, 700 + $bias],
                "slot bias {} aggregates only its own outputs in collection order",
                $bias
            );
        };
    }
    check_slot!(s1, 1);
    check_slot!(s2, 2);
    check_slot!(s3, 3);
    check_slot!(s4, 4);
    check_slot!(s5, 5);
    check_slot!(s6, 6);
    check_slot!(s7, 7);
    check_slot!(s8, 8);

    // 身份：8 个 collector 各产生一个聚合 `Vec`，身份两两互异（逐对断言 + 序号去重）。
    let takes = test_support::take_log_snapshot();
    assert_eq!(takes.len(), 8, "one aggregated Vec per slot");
    for (i, a) in takes.iter().enumerate() {
        for (j, b) in takes.iter().enumerate() {
            if i != j {
                assert_ne!(
                    a, b,
                    "slots {i} and {j} must own distinct collector identities"
                );
            }
        }
    }
    let mut seqs: Vec<u64> = takes.iter().map(DataId::seq).collect();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs.len(), 8, "8 distinct identity sequence numbers");
}

// ================================================================ 缺口 2：泛型 derive(Data) 叶子

/// 泛型 `#[derive(Data)]` 叶子作为 Root 输入 → Node 输入 → Node 输出 → Root 输出的真实数据流，
/// 断言实际值与所有权转移（输入以借用进入，输出以所有权离开；构造次数 = 析构次数）。
#[test]
fn generic_derive_data_wrapper_is_a_real_leaf_from_root_input_to_root_output() {
    reset();
    let created = Rc::new(Cell::new(0usize));
    let dropped = Rc::new(Cell::new(0usize));
    let runtime = Runtime::new();
    let outcome: Result<Wrapper<Payload>, RunError> = block_on(runtime.execute(
        |flow, input: Ref<Wrapper<Payload>>| {
            let out_refs = flow.then_with_output(bump_wrapper, input);
            flow.output_with(out_refs);
        },
        Wrapper(Payload::new(41, &created, &dropped)),
    ));
    let output = outcome.expect("generic derive(Data) wrapper runs as a real data leaf");
    assert_eq!(
        output.0.value, 42,
        "the node borrowed the root `Wrapper<Payload>` and produced its own owned wrapper"
    );
    assert_eq!(
        created.get(),
        2,
        "root input wrapper plus node-produced wrapper"
    );
    assert_eq!(
        dropped.get(),
        1,
        "the owned root input is dropped exactly once when the execution closes"
    );
    drop(output);
    assert_eq!(
        dropped.get(),
        2,
        "the extracted root output owns and drops its payload exactly once"
    );
}

/// 泛型 `#[derive(Data)]` 叶子作为 Each 集合元素真实参与 execute：按索引顺序逐项聚合。
#[test]
fn generic_derive_data_wrapper_aggregates_as_each_collection_elements() {
    reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Vec<Wrapper<u32>>, _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Wrapper<u32>>>| {
            let out_refs = flow.each(items, |item, sub| {
                sub.then_with_output(double_wrapper, item)
            });
            flow.output_with(out_refs);
        },
        vec![Wrapper(3u32), Wrapper(5u32), Wrapper(7u32)],
    ));
    let collected: Vec<Wrapper<u32>> = outcome.expect("wrapper collection each succeeds");
    assert_eq!(
        collected
            .iter()
            .map(|wrapper| wrapper.0)
            .collect::<Vec<u32>>(),
        vec![6, 10, 14],
        "generic wrapper elements flow through each in index order"
    );
}
