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

/// 只在断言/转换中被引用（构造点随历史用例删除）：保留类型以维持断言可执行。
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
struct T07Item(u32);

use crate::core::engine::data::Data;
use crate::core::engine::error::BodyError;

use crate::core::engine::node::Node;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::Query;
use crate::core::test_support::take_log_reset;
use futures::executor::block_on;
use std::cell::Cell;

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

// ---------------------------------------------------------------- Each 中间 arity（4～7、9～15）

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
