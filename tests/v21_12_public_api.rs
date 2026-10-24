//! 旧 V21-12 外部消费者（`tests/v21_12_public_api.rs`，13 项 A02～A06）的 B 阶段迁移落点
//! （WE-T06 W11）。
//!
//! 旧文件整份 `use srflow::legacy::{…}`；`srflow::legacy` 随 B 阶段退出后，这 13 项在门面
//! 缺失时会退化为 `E0432`（找不到名字）——那**不是**它们原本要证明的语义。因此按 W09 ledger
//! §3.1 逐项处置：11 项由 A 阶段新外部集成等价替代，2 项按新契约改写。本文件保留旧路径与
//! “外部消费者”身份，**只使用 crate 根的新公开面**（`srflow::{…}`），不借道 crate 内部，
//! 也不引用 `srflow::legacy`。
//!
//! 13 项去向（逐项证据见 `docs/workflow_engine_tasks/WE_T06_FREEZE/W11_consumer_migration.md`）：
//!
//! | # | 旧项 | 处置 | 目的地 |
//! | ---: | --- | --- | --- |
//! | 1 | `a02_function_async_struct_and_arc_nodes_produce_owned_outputs` | 新外部集成替代 | `tests/we_t06_s01_s03.rs::s02_node_forms_query_shapes_and_call_counts` |
//! | 2 | `a02_definition_reuse_creates_independent_executions` | 新外部集成替代 | `tests/we_t06_s01_s03.rs::s01_two_executions_build_independent_definitions_and_reject_an_old_ref` |
//! | 3 | `a03_plain_unit_functions_are_rejected_at_definition_build` | 改写为新契约外部集成 | `docs/workflow_engine_tasks/WE_T06_PROBE/fixtures/we_t06_unit_shape.rs`（unit 形状类型层不可表达，E0277） |
//! | 4 | `a03_struct_node_unit_output_and_async_node_are_supported` | 新外部集成替代 | `tests/we_t06_s01_s03.rs::s02_node_forms_query_shapes_and_call_counts` |
//! | 5 | `a04_shape_matrix_unit_data_out2_and_rejections` | 新外部集成替代 | `tests/we_t06_s01_s03.rs::s01_root_linear_owned_flow_shapes_and_values`＋`tests/we_t06_s08_s09.rs::s09_root_duplicate_output_rejected_before_any_business` |
//! | 6 | `a05_each_node_body_and_shared_input` | 新外部集成替代 | `tests/we_t06_s04_s07.rs::s04_plain_collection_slots_aggregate_in_item_order` |
//! | 7 | `a05_loop_iter_multi_round_and_retry_finish` | 新外部集成替代 | `tests/we_t06_s04_s07.rs::s06_retry_restarts_full_attempt_and_cleans_before_the_next`＋`s07_break_delivers_the_entry_current_of_the_round` |
//! | 8 | `a05_match_branch_default_and_no_match_error` | 新外部集成替代 | `tests/we_t06_s04_s07.rs::s05_no_match_otherwise_and_failure_never_fall_back` |
//! | 9 | `a05_cross_controller_chain_each_then_match` | 新外部集成替代 | `tests/we_t06_s08_s09.rs::s08_six_operations_compose_in_one_execution` |
//! | 10 | `a06_unit_root_takes_no_business_output` | 新外部集成替代 | `tests/we_t06_s01_s03.rs::s01_root_linear_owned_flow_shapes_and_values` |
//! | 11 | `a06_business_error_is_observable_and_returns_no_partial_output` | 新外部集成替代 | `tests/we_t06_s08_s09.rs::s09_path_c_failure_terminates_and_stops_followups` |
//! | 12 | `a06_duplicate_root_data_id_is_rejected_before_any_take` | 新外部集成替代 | `tests/we_t06_s08_s09.rs::s09_root_duplicate_output_rejected_before_any_business` |
//! | 13 | `tuple_flow_inputs_support_three_and_sixteen_positions` | 改写为新契约外部集成 | 本文件 `tuple_root_inputs_support_three_positions`／`tuple_root_inputs_support_sixteen_positions` |
//!
//! 本文件实际承载第 13 项的改写（旧契约承诺 3／16 元 Flow 输入；新契约承诺 flat 2～16 元，但
//! 外部集成此前无 3／16 正例）。第 3 项的改写落点是编译拒绝夹具（unit 形状在类型层不可表达），
//! 不在可编译的测试文件内。
//!
//! 运行：`cargo test --offline --test v21_12_public_api`
// `let _: () = …` 是刻意的 unit 形状断言（Node 输出），按测试意图保留。
#![allow(clippy::unused_unit, clippy::let_unit_value)]

use futures::executor::block_on;
use srflow::{BodyError, Query, Runtime};

/// 3 元 Node 输入：只读借用三个位置，产生显式 `()`。
async fn consume_three(query: Query<(&u8, &u16, &u32)>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

/// 16 元 Node 输入：flat Ref tuple 的全部位置都真实可读。
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
async fn consume_sixteen(
    query: Query<(
        &u8,
        &u16,
        &u32,
        &u64,
        &u128,
        &usize,
        &i8,
        &i16,
        &i32,
        &i64,
        &i128,
        &isize,
        &bool,
        &char,
        &f32,
        &f64,
    )>,
) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

/// 旧项 13（a）：flat 3 元 Root 输入 → 3 元 Node 输入 → 3 元 owned 输出，值逐位置往返。
#[test]
fn tuple_root_inputs_support_three_positions() {
    let runtime = Runtime::new();
    let (first, second, third) = block_on(runtime.execute::<(u8, u16, u32), (u8, u16, u32), _>(
        |flow, refs| {
            let _: () = flow.then_with_output(consume_three, refs);
            let out_refs = refs;
            flow.output_with(out_refs);
        },
        (1u8, 2u16, 3u32),
    ))
    .expect("a 3-position flat Root tuple runs end to end");
    assert_eq!((first, second, third), (1u8, 2u16, 3u32));
}

/// 旧项 13（b）：flat 16 元 Root 输入 → 16 元 Node 输入 → 16 元 owned 输出，首尾位置往返。
#[test]
fn tuple_root_inputs_support_sixteen_positions() {
    let runtime = Runtime::new();
    let output = block_on(runtime.execute::<(
        u8,
        u16,
        u32,
        u64,
        u128,
        usize,
        i8,
        i16,
        i32,
        i64,
        i128,
        isize,
        bool,
        char,
        f32,
        f64,
    ), (
        u8,
        u16,
        u32,
        u64,
        u128,
        usize,
        i8,
        i16,
        i32,
        i64,
        i128,
        isize,
        bool,
        char,
        f32,
        f64,
    ), _>(
        |flow, refs| {
            let _: () = flow.then_with_output(consume_sixteen, refs);
            let out_refs = refs;
            flow.output_with(out_refs);
        },
        (
            1u8, 2u16, 3u32, 4u64, 5u128, 6usize, 7i8, 8i16, 9i32, 10i64, 11i128, 12isize, true,
            'x', 13.5f32, 14.5f64,
        ),
    ))
    .expect("a 16-position flat Root tuple runs end to end");
    assert_eq!(output.0, 1u8, "first position round-trips");
    assert_eq!(output.15, 14.5f64, "last (16th) position round-trips");
}
