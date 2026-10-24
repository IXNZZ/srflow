//! V22-T07 定向测试：自动输入桥与 Each 聚合的中间 arity 补齐（最终回归）。
//!
//! 自动桥（arity 3～15）：每个 arity 两个用例，分别覆盖函数 item 桥与具体 Struct Node 桥；
//! 每个用例内**两入口都真实执行**：① 普通形式 `then`（`&mut Flow`）＋自动下游 `then_output`
//! 读取其发布的输出；② 取输出形式 `then_output` 并把交付值作为父域输出。
//!
//! Each 聚合（arity 4～7、9～15）：逐槽各一 Node → 逐槽独立 `Vec`，断言每槽值序列、业务调用数
//! 与逐槽聚合 DataId 互不相同；0／单值／2／3／8／16 已有既有载体，不重复。
//!
//! 全部经正式 `Runtime::execute`（v2.2 签名：Root 闭包 `|flow, refs|` 返回 unit）。
//! 生成来源：`docs/implementation_tasks_v2.2/V22_T07_VALIDATION/gen/gen_t07_tests.py`（确定性输出）。

#![allow(clippy::let_unit_value, clippy::unused_unit, clippy::type_complexity)]

use crate::core::engine::data::Data;
use crate::core::engine::error::BodyError;
use crate::core::engine::flow::Flow;
use crate::core::engine::node::Node;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, Ref};
use crate::core::test_support::{take_log_reset, take_log_snapshot};
use futures::executor::block_on;
use std::cell::Cell;
use std::collections::BTreeSet;

// ---------------------------------------------------------------- 业务类型与计数

macro_rules! t07_slots {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Debug, PartialEq, Eq, Clone)]
        struct $name(u32);
        impl Data for $name {}
    )+};
}
t07_slots!(
    T07S01, T07S02, T07S03, T07S04, T07S05, T07S06, T07S07, T07S08, T07S09, T07S10, T07S11, T07S12,
    T07S13, T07S14, T07S15
);

#[derive(Debug, PartialEq, Eq, Clone)]
struct T07Sum(u32);
impl Data for T07Sum {}

#[derive(Debug, PartialEq, Eq, Clone)]
struct T07Item(u32);
impl Data for T07Item {}

thread_local! {
    static T07_RUNS: Cell<usize> = const { Cell::new(0) };
    static T07_SEEN: Cell<u32> = const { Cell::new(u32::MAX) };
}

fn reset_case() {
    T07_RUNS.with(|c| c.set(0));
    T07_SEEN.with(|c| c.set(u32::MAX));
    take_log_reset();
}

fn bump_runs() {
    T07_RUNS.with(|c| c.set(c.get() + 1));
}

fn runs() -> usize {
    T07_RUNS.with(Cell::get)
}

fn record_seen(v: u32) {
    T07_SEEN.with(|c| c.set(v));
}

fn seen() -> u32 {
    T07_SEEN.with(Cell::get)
}

/// 自动下游读取：证明普通形式 `then` 仍发布其声明输出。
async fn t07_record_sum(query: Query<&T07Sum>) -> Result<(), BodyError> {
    record_seen(query.get().0);
    Ok(())
}

/// 本次样本真实发生的 `take_verified` 完整 DataId 序列（去重后计数）。
fn distinct_takes() -> usize {
    take_log_snapshot()
        .iter()
        .map(|id| format!("{id:?}"))
        .collect::<BTreeSet<_>>()
        .len()
}

fn take_count() -> usize {
    take_log_snapshot().len()
}

// ---------------------------------------------------------------- 自动桥中间 arity（3～15）

/// 为一个 arity 生成：函数 item 桥、具体 Struct Node 桥，以及两个真实运行用例。
macro_rules! t07_auto_case {
    (
        $fn_test:ident, $struct_test:ident,
        $fn_node:ident, $struct_node:ident,
        $arity:literal, $expected:literal,
        [ $( ($ty:ident, $bind:ident, $val:expr) ),+ $(,)? ]
    ) => {
        async fn $fn_node(query: Query<($( &$ty, )+)>) -> Result<T07Sum, BodyError> {
            bump_runs();
            let ($( $bind, )+) = query.get();
            $( assert_eq!($bind.0, $val, "arity {} 槽 {} 值不符", $arity, stringify!($ty)); )+
            Ok(T07Sum(0u32 $( + $bind.0 )+))
        }

        struct $struct_node;

        impl Node for $struct_node {
            type Input = ( $( $ty, )+ );
            type Output = T07Sum;

            async fn run(&self, query: Query<&( $( $ty, )+ )>) -> Result<T07Sum, BodyError> {
                bump_runs();
                let ($( $bind, )+) = query.get();
                $( assert_eq!($bind.0, $val, "arity {} 槽 {} 值不符", $arity, stringify!($ty)); )+
                Ok(T07Sum(0u32 $( + $bind.0 )+))
            }
        }

        #[test]
        fn $fn_test() {
            reset_case();
            let input = ( $( $ty($val), )+ );
            let runtime = Runtime::new();

            // ① 普通形式 then ＋ 自动下游读取
            block_on(runtime.execute::<_, (), _>(
                |flow, _input| {
                    let _ = flow.then($fn_node);
                    let _: () = flow.then_output(t07_record_sum);
                    flow.output_with(());
                },
                input.clone(),
            ))
            .expect("普通形式应登记并执行");

            assert_eq!(runs(), 1, "arity {} 函数桥普通形式应恰好执行一次", $arity);
            assert_eq!(seen(), $expected, "arity {} 自动下游应读到普通形式发布的输出", $arity);

            // ② 取输出形式 then_output
            reset_case();
            let out = block_on(runtime.execute::<_, T07Sum, _>(
                |flow, _input| {
                    let sum = flow.then_output($fn_node);
                    flow.output_with(sum);
                },
                input.clone(),
            ))
            .expect("取输出形式应交付 T07Sum");

            assert_eq!(out.0, $expected, "arity {} 函数桥取输出形式交付值", $arity);
            assert_eq!(runs(), 1, "arity {} 函数桥取输出形式应恰好执行一次", $arity);
        }

        #[test]
        fn $struct_test() {
            reset_case();
            let input = ( $( $ty($val), )+ );
            let runtime = Runtime::new();

            // ① 普通形式 then ＋ 自动下游读取
            block_on(runtime.execute::<_, (), _>(
                |flow, _input| {
                    let _ = flow.then($struct_node);
                    let _: () = flow.then_output(t07_record_sum);
                    flow.output_with(());
                },
                input.clone(),
            ))
            .expect("普通形式应登记并执行");

            assert_eq!(runs(), 1, "arity {} Struct 桥普通形式应恰好执行一次", $arity);
            assert_eq!(seen(), $expected, "arity {} 自动下游应读到普通形式发布的输出", $arity);

            // ② 取输出形式 then_output
            reset_case();
            let out = block_on(runtime.execute::<_, T07Sum, _>(
                |flow, _input| {
                    let sum = flow.then_output($struct_node);
                    flow.output_with(sum);
                },
                input.clone(),
            ))
            .expect("取输出形式应交付 T07Sum");

            assert_eq!(out.0, $expected, "arity {} Struct 桥取输出形式交付值", $arity);
            assert_eq!(runs(), 1, "arity {} Struct 桥取输出形式应恰好执行一次", $arity);
        }
    };
}

t07_auto_case!(
    t07_auto_function_arity_3,
    t07_auto_struct_arity_3,
    t07_fn_node_arity_3,
    T07StructNodeArity3,
    3,
    6,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_4,
    t07_auto_struct_arity_4,
    t07_fn_node_arity_4,
    T07StructNodeArity4,
    4,
    10,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_5,
    t07_auto_struct_arity_5,
    t07_fn_node_arity_5,
    T07StructNodeArity5,
    5,
    15,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_6,
    t07_auto_struct_arity_6,
    t07_fn_node_arity_6,
    T07StructNodeArity6,
    6,
    21,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_7,
    t07_auto_struct_arity_7,
    t07_fn_node_arity_7,
    T07StructNodeArity7,
    7,
    28,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_8,
    t07_auto_struct_arity_8,
    t07_fn_node_arity_8,
    T07StructNodeArity8,
    8,
    36,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32),
        (T07S08, v08, 8u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_9,
    t07_auto_struct_arity_9,
    t07_fn_node_arity_9,
    T07StructNodeArity9,
    9,
    45,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32),
        (T07S08, v08, 8u32),
        (T07S09, v09, 9u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_10,
    t07_auto_struct_arity_10,
    t07_fn_node_arity_10,
    T07StructNodeArity10,
    10,
    55,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32),
        (T07S08, v08, 8u32),
        (T07S09, v09, 9u32),
        (T07S10, v10, 10u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_11,
    t07_auto_struct_arity_11,
    t07_fn_node_arity_11,
    T07StructNodeArity11,
    11,
    66,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32),
        (T07S08, v08, 8u32),
        (T07S09, v09, 9u32),
        (T07S10, v10, 10u32),
        (T07S11, v11, 11u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_12,
    t07_auto_struct_arity_12,
    t07_fn_node_arity_12,
    T07StructNodeArity12,
    12,
    78,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32),
        (T07S08, v08, 8u32),
        (T07S09, v09, 9u32),
        (T07S10, v10, 10u32),
        (T07S11, v11, 11u32),
        (T07S12, v12, 12u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_13,
    t07_auto_struct_arity_13,
    t07_fn_node_arity_13,
    T07StructNodeArity13,
    13,
    91,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32),
        (T07S08, v08, 8u32),
        (T07S09, v09, 9u32),
        (T07S10, v10, 10u32),
        (T07S11, v11, 11u32),
        (T07S12, v12, 12u32),
        (T07S13, v13, 13u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_14,
    t07_auto_struct_arity_14,
    t07_fn_node_arity_14,
    T07StructNodeArity14,
    14,
    105,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32),
        (T07S08, v08, 8u32),
        (T07S09, v09, 9u32),
        (T07S10, v10, 10u32),
        (T07S11, v11, 11u32),
        (T07S12, v12, 12u32),
        (T07S13, v13, 13u32),
        (T07S14, v14, 14u32)
    ]
);
t07_auto_case!(
    t07_auto_function_arity_15,
    t07_auto_struct_arity_15,
    t07_fn_node_arity_15,
    T07StructNodeArity15,
    15,
    120,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32),
        (T07S08, v08, 8u32),
        (T07S09, v09, 9u32),
        (T07S10, v10, 10u32),
        (T07S11, v11, 11u32),
        (T07S12, v12, 12u32),
        (T07S13, v13, 13u32),
        (T07S14, v14, 14u32),
        (T07S15, v15, 15u32)
    ]
);

// ---------------------------------------------------------------- Each 中间 arity（4～7、9～15）

/// 为一个 arity 生成：逐槽 Node、Item Body 与一个真实运行用例（三元素集合）。
macro_rules! t07_each_case {
    (
        $test:ident, $body:ident, $arity:literal,
        [ $( ($ty:ident, $node:ident, $bind:ident, $off:expr, $expect:expr) ),+ $(,)? ]
    ) => {
        $(
            async fn $node(query: Query<&T07Item>) -> Result<$ty, BodyError> {
                bump_runs();
                Ok($ty(query.get().0 + $off))
            }
        )+

        fn $body(sub: &mut Flow<'_>) {
            let item = sub.ref_of::<T07Item>();
            $( let $bind = sub.then_with_output($node, item); )+
            sub.output_with(( $( $bind, )+ ));
        }

        #[test]
        fn $test() {
            reset_case();
            let runtime = Runtime::new();
            let out = block_on(runtime.execute::<Vec<T07Item>, ( $( Vec<$ty>, )+ ), _>(
                |flow, items| {
                    let ( $( $bind, )+ ): ( $( Ref<Vec<$ty>>, )+ ) = flow.each_output(items, $body);
                    flow.output_with(( $( $bind, )+ ));
                },
                vec![T07Item(1), T07Item(2), T07Item(3)],
            ))
            .expect("Each 中间 arity 聚合必须真实执行");

            let ( $( $bind, )+ ) = out;
            $( assert_eq!($bind, $expect, "arity {} 槽 {} 聚合值", $arity, stringify!($ty)); )+
            assert_eq!(runs(), 3 * $arity, "arity {} 三项各执行每槽一个 Node", $arity);
            assert_eq!(take_count(), $arity, "arity {} 每个槽一个聚合 Vec 真实 take", $arity);
            assert_eq!(distinct_takes(), $arity, "arity {} 逐槽聚合身份互不相同", $arity);
        }
    };
}

t07_each_case!(
    t07_each_arity_4,
    t07_each_body_4,
    4,
    [
        (
            T07S01,
            t07_each_node_4_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_4_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_4_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_4_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_5,
    t07_each_body_5,
    5,
    [
        (
            T07S01,
            t07_each_node_5_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_5_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_5_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_5_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_5_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_6,
    t07_each_body_6,
    6,
    [
        (
            T07S01,
            t07_each_node_6_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_6_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_6_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_6_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_6_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        ),
        (
            T07S06,
            t07_each_node_6_06,
            s06,
            6u32,
            vec![T07S06(7), T07S06(8), T07S06(9)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_7,
    t07_each_body_7,
    7,
    [
        (
            T07S01,
            t07_each_node_7_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_7_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_7_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_7_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_7_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        ),
        (
            T07S06,
            t07_each_node_7_06,
            s06,
            6u32,
            vec![T07S06(7), T07S06(8), T07S06(9)]
        ),
        (
            T07S07,
            t07_each_node_7_07,
            s07,
            7u32,
            vec![T07S07(8), T07S07(9), T07S07(10)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_9,
    t07_each_body_9,
    9,
    [
        (
            T07S01,
            t07_each_node_9_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_9_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_9_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_9_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_9_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        ),
        (
            T07S06,
            t07_each_node_9_06,
            s06,
            6u32,
            vec![T07S06(7), T07S06(8), T07S06(9)]
        ),
        (
            T07S07,
            t07_each_node_9_07,
            s07,
            7u32,
            vec![T07S07(8), T07S07(9), T07S07(10)]
        ),
        (
            T07S08,
            t07_each_node_9_08,
            s08,
            8u32,
            vec![T07S08(9), T07S08(10), T07S08(11)]
        ),
        (
            T07S09,
            t07_each_node_9_09,
            s09,
            9u32,
            vec![T07S09(10), T07S09(11), T07S09(12)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_10,
    t07_each_body_10,
    10,
    [
        (
            T07S01,
            t07_each_node_10_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_10_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_10_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_10_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_10_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        ),
        (
            T07S06,
            t07_each_node_10_06,
            s06,
            6u32,
            vec![T07S06(7), T07S06(8), T07S06(9)]
        ),
        (
            T07S07,
            t07_each_node_10_07,
            s07,
            7u32,
            vec![T07S07(8), T07S07(9), T07S07(10)]
        ),
        (
            T07S08,
            t07_each_node_10_08,
            s08,
            8u32,
            vec![T07S08(9), T07S08(10), T07S08(11)]
        ),
        (
            T07S09,
            t07_each_node_10_09,
            s09,
            9u32,
            vec![T07S09(10), T07S09(11), T07S09(12)]
        ),
        (
            T07S10,
            t07_each_node_10_10,
            s10,
            10u32,
            vec![T07S10(11), T07S10(12), T07S10(13)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_11,
    t07_each_body_11,
    11,
    [
        (
            T07S01,
            t07_each_node_11_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_11_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_11_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_11_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_11_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        ),
        (
            T07S06,
            t07_each_node_11_06,
            s06,
            6u32,
            vec![T07S06(7), T07S06(8), T07S06(9)]
        ),
        (
            T07S07,
            t07_each_node_11_07,
            s07,
            7u32,
            vec![T07S07(8), T07S07(9), T07S07(10)]
        ),
        (
            T07S08,
            t07_each_node_11_08,
            s08,
            8u32,
            vec![T07S08(9), T07S08(10), T07S08(11)]
        ),
        (
            T07S09,
            t07_each_node_11_09,
            s09,
            9u32,
            vec![T07S09(10), T07S09(11), T07S09(12)]
        ),
        (
            T07S10,
            t07_each_node_11_10,
            s10,
            10u32,
            vec![T07S10(11), T07S10(12), T07S10(13)]
        ),
        (
            T07S11,
            t07_each_node_11_11,
            s11,
            11u32,
            vec![T07S11(12), T07S11(13), T07S11(14)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_12,
    t07_each_body_12,
    12,
    [
        (
            T07S01,
            t07_each_node_12_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_12_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_12_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_12_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_12_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        ),
        (
            T07S06,
            t07_each_node_12_06,
            s06,
            6u32,
            vec![T07S06(7), T07S06(8), T07S06(9)]
        ),
        (
            T07S07,
            t07_each_node_12_07,
            s07,
            7u32,
            vec![T07S07(8), T07S07(9), T07S07(10)]
        ),
        (
            T07S08,
            t07_each_node_12_08,
            s08,
            8u32,
            vec![T07S08(9), T07S08(10), T07S08(11)]
        ),
        (
            T07S09,
            t07_each_node_12_09,
            s09,
            9u32,
            vec![T07S09(10), T07S09(11), T07S09(12)]
        ),
        (
            T07S10,
            t07_each_node_12_10,
            s10,
            10u32,
            vec![T07S10(11), T07S10(12), T07S10(13)]
        ),
        (
            T07S11,
            t07_each_node_12_11,
            s11,
            11u32,
            vec![T07S11(12), T07S11(13), T07S11(14)]
        ),
        (
            T07S12,
            t07_each_node_12_12,
            s12,
            12u32,
            vec![T07S12(13), T07S12(14), T07S12(15)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_13,
    t07_each_body_13,
    13,
    [
        (
            T07S01,
            t07_each_node_13_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_13_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_13_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_13_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_13_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        ),
        (
            T07S06,
            t07_each_node_13_06,
            s06,
            6u32,
            vec![T07S06(7), T07S06(8), T07S06(9)]
        ),
        (
            T07S07,
            t07_each_node_13_07,
            s07,
            7u32,
            vec![T07S07(8), T07S07(9), T07S07(10)]
        ),
        (
            T07S08,
            t07_each_node_13_08,
            s08,
            8u32,
            vec![T07S08(9), T07S08(10), T07S08(11)]
        ),
        (
            T07S09,
            t07_each_node_13_09,
            s09,
            9u32,
            vec![T07S09(10), T07S09(11), T07S09(12)]
        ),
        (
            T07S10,
            t07_each_node_13_10,
            s10,
            10u32,
            vec![T07S10(11), T07S10(12), T07S10(13)]
        ),
        (
            T07S11,
            t07_each_node_13_11,
            s11,
            11u32,
            vec![T07S11(12), T07S11(13), T07S11(14)]
        ),
        (
            T07S12,
            t07_each_node_13_12,
            s12,
            12u32,
            vec![T07S12(13), T07S12(14), T07S12(15)]
        ),
        (
            T07S13,
            t07_each_node_13_13,
            s13,
            13u32,
            vec![T07S13(14), T07S13(15), T07S13(16)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_14,
    t07_each_body_14,
    14,
    [
        (
            T07S01,
            t07_each_node_14_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_14_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_14_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_14_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_14_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        ),
        (
            T07S06,
            t07_each_node_14_06,
            s06,
            6u32,
            vec![T07S06(7), T07S06(8), T07S06(9)]
        ),
        (
            T07S07,
            t07_each_node_14_07,
            s07,
            7u32,
            vec![T07S07(8), T07S07(9), T07S07(10)]
        ),
        (
            T07S08,
            t07_each_node_14_08,
            s08,
            8u32,
            vec![T07S08(9), T07S08(10), T07S08(11)]
        ),
        (
            T07S09,
            t07_each_node_14_09,
            s09,
            9u32,
            vec![T07S09(10), T07S09(11), T07S09(12)]
        ),
        (
            T07S10,
            t07_each_node_14_10,
            s10,
            10u32,
            vec![T07S10(11), T07S10(12), T07S10(13)]
        ),
        (
            T07S11,
            t07_each_node_14_11,
            s11,
            11u32,
            vec![T07S11(12), T07S11(13), T07S11(14)]
        ),
        (
            T07S12,
            t07_each_node_14_12,
            s12,
            12u32,
            vec![T07S12(13), T07S12(14), T07S12(15)]
        ),
        (
            T07S13,
            t07_each_node_14_13,
            s13,
            13u32,
            vec![T07S13(14), T07S13(15), T07S13(16)]
        ),
        (
            T07S14,
            t07_each_node_14_14,
            s14,
            14u32,
            vec![T07S14(15), T07S14(16), T07S14(17)]
        )
    ]
);
t07_each_case!(
    t07_each_arity_15,
    t07_each_body_15,
    15,
    [
        (
            T07S01,
            t07_each_node_15_01,
            s01,
            1u32,
            vec![T07S01(2), T07S01(3), T07S01(4)]
        ),
        (
            T07S02,
            t07_each_node_15_02,
            s02,
            2u32,
            vec![T07S02(3), T07S02(4), T07S02(5)]
        ),
        (
            T07S03,
            t07_each_node_15_03,
            s03,
            3u32,
            vec![T07S03(4), T07S03(5), T07S03(6)]
        ),
        (
            T07S04,
            t07_each_node_15_04,
            s04,
            4u32,
            vec![T07S04(5), T07S04(6), T07S04(7)]
        ),
        (
            T07S05,
            t07_each_node_15_05,
            s05,
            5u32,
            vec![T07S05(6), T07S05(7), T07S05(8)]
        ),
        (
            T07S06,
            t07_each_node_15_06,
            s06,
            6u32,
            vec![T07S06(7), T07S06(8), T07S06(9)]
        ),
        (
            T07S07,
            t07_each_node_15_07,
            s07,
            7u32,
            vec![T07S07(8), T07S07(9), T07S07(10)]
        ),
        (
            T07S08,
            t07_each_node_15_08,
            s08,
            8u32,
            vec![T07S08(9), T07S08(10), T07S08(11)]
        ),
        (
            T07S09,
            t07_each_node_15_09,
            s09,
            9u32,
            vec![T07S09(10), T07S09(11), T07S09(12)]
        ),
        (
            T07S10,
            t07_each_node_15_10,
            s10,
            10u32,
            vec![T07S10(11), T07S10(12), T07S10(13)]
        ),
        (
            T07S11,
            t07_each_node_15_11,
            s11,
            11u32,
            vec![T07S11(12), T07S11(13), T07S11(14)]
        ),
        (
            T07S12,
            t07_each_node_15_12,
            s12,
            12u32,
            vec![T07S12(13), T07S12(14), T07S12(15)]
        ),
        (
            T07S13,
            t07_each_node_15_13,
            s13,
            13u32,
            vec![T07S13(14), T07S13(15), T07S13(16)]
        ),
        (
            T07S14,
            t07_each_node_15_14,
            s14,
            14u32,
            vec![T07S14(15), T07S14(16), T07S14(17)]
        ),
        (
            T07S15,
            t07_each_node_15_15,
            s15,
            15u32,
            vec![T07S15(16), T07S15(17), T07S15(18)]
        )
    ]
);

// ---------------------------------------------------------------- §4.1 补缺：arity 16 两桥 + Struct 桥 then 的 0／1／2

t07_auto_case!(
    t07_auto_function_arity_16,
    t07_auto_struct_arity_16,
    t07_fn_node_arity_16,
    T07StructNodeArity16,
    16,
    136,
    [
        (T07S01, v01, 1u32),
        (T07S02, v02, 2u32),
        (T07S03, v03, 3u32),
        (T07S04, v04, 4u32),
        (T07S05, v05, 5u32),
        (T07S06, v06, 6u32),
        (T07S07, v07, 7u32),
        (T07S08, v08, 8u32),
        (T07S09, v09, 9u32),
        (T07S10, v10, 10u32),
        (T07S11, v11, 11u32),
        (T07S12, v12, 12u32),
        (T07S13, v13, 13u32),
        (T07S14, v14, 14u32),
        (T07S15, v15, 15u32),
        (T07S16, v16, 16u32)
    ]
);

t07_slots!(T07S16);

/// 零输入 Struct Node（普通形式 `then` 的 arity 0 运行载体）。
struct T07UnitSink;

impl Node for T07UnitSink {
    type Input = ();
    type Output = T07Sum;

    async fn run(&self, _query: Query<&()>) -> Result<T07Sum, BodyError> {
        bump_runs();
        Ok(T07Sum(55))
    }
}

/// 单槽输入 Struct Node（普通形式 `then` 的 arity 1 运行载体）。
struct T07OneSink;

impl Node for T07OneSink {
    type Input = T07S01;
    type Output = T07Sum;

    async fn run(&self, query: Query<&T07S01>) -> Result<T07Sum, BodyError> {
        bump_runs();
        let value = query.get().0;
        assert_eq!(value, 1, "arity 1 单槽值");
        Ok(T07Sum(value))
    }
}

/// 双槽输入 Struct Node（普通形式 `then` 的 arity 2 运行载体）。
struct T07TwoSink;

impl Node for T07TwoSink {
    type Input = (T07S01, T07S02);
    type Output = T07Sum;

    async fn run(&self, query: Query<&(T07S01, T07S02)>) -> Result<T07Sum, BodyError> {
        bump_runs();
        let (first, second) = query.get();
        assert_eq!((first.0, second.0), (1, 2), "arity 2 逐槽值");
        Ok(T07Sum(first.0 + second.0))
    }
}

#[test]
fn t07_auto_struct_then_arity_0() {
    reset_case();
    block_on(Runtime::new().execute::<(), (), _>(
        |flow, _input| {
            let _ = flow.then(T07UnitSink);
            let _: () = flow.then_output(t07_record_sum);
            flow.output_with(());
        },
        (),
    ))
    .expect("arity 0 Struct 桥普通形式应登记并执行");
    assert_eq!(runs(), 1, "arity 0 Struct 桥普通形式应恰好执行一次");
    assert_eq!(seen(), 55, "自动下游应读到普通形式发布的输出");
}

#[test]
fn t07_auto_struct_then_arity_1() {
    reset_case();
    block_on(Runtime::new().execute::<T07S01, (), _>(
        |flow, _input| {
            let _ = flow.then(T07OneSink);
            let _: () = flow.then_output(t07_record_sum);
            flow.output_with(());
        },
        T07S01(1),
    ))
    .expect("arity 1 Struct 桥普通形式应登记并执行");
    assert_eq!(runs(), 1, "arity 1 Struct 桥普通形式应恰好执行一次");
    assert_eq!(seen(), 1, "自动下游应读到普通形式发布的输出");
}

#[test]
fn t07_auto_struct_then_arity_2() {
    reset_case();
    block_on(Runtime::new().execute::<(T07S01, T07S02), (), _>(
        |flow, _input| {
            let _ = flow.then(T07TwoSink);
            let _: () = flow.then_output(t07_record_sum);
            flow.output_with(());
        },
        (T07S01(1), T07S02(2)),
    ))
    .expect("arity 2 Struct 桥普通形式应登记并执行");
    assert_eq!(runs(), 1, "arity 2 Struct 桥普通形式应恰好执行一次");
    assert_eq!(seen(), 3, "自动下游应读到普通形式发布的输出");
}
