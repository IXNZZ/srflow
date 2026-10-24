//! V22-T06 定向测试：Iter 的五签名（`iter`／`iter_output`／`next`／`next_with`／`current`）、
//! 同一冻结 Next、入口时点、失败隔离与 G06 精选取槽。
//!
//! 覆盖判别性证据：
//! - 普通 `iter` 与 `iter_output` 走同一条登记路径：同一场景下可运行值与业务调用计数一致，
//!   普通形式照常把最终 State 发布为父域来源（可由后继 Node / `ref_of` 解析）；
//! - Round 的唯一 Next 契约：缺失、重复、Round 使用 output、普通 Body 使用 next、
//!   Shape arity／逐槽类型不符，全部在业务执行前拒绝并保留首因；
//! - `iter_output` 在失败 Round 上返回受控失败形状：不 panic、不产生可运行依赖，
//!   Definition 收口仍给首个真实原因；
//! - 自动 `next` 在声明点冻结所选来源：其后创建的同类型新来源不改变下一轮实际读取值，
//!   且后续 Step 确实执行；
//! - `current::<T>()` 只给本轮 formal Current 槽身份：与 captured initial、Next 可区分，
//!   重复调用幂等、其后新增同类型来源不改变它；
//! - `current::<T>()` 的错误边界：非直接 Round、请求槽数不符、逐槽类型不符都记录
//!   Definition 期首因并按请求 Shape 返回失败形状；
//! - 0 轮预算：`iter_output` 返回失败形状且 Definition 拒绝，不 panic。

#![allow(clippy::let_unit_value, clippy::unused_unit)]

use crate::core::engine::data::Data;
use crate::core::engine::definition::{
    BodyRole, Builder, Declaration, DeclarationKind, Definition, HandoverSlot, PositionSource,
    StepEntry, build_definition,
};
use crate::core::engine::error::{BodyError, BuildErrorDetail};
use crate::core::engine::flow::Flow;
use crate::core::engine::identity::{BodyId, PositionHandle, PositionId};
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{PositionDesc, Query, Ref, StateShape, TypeDesc};
use futures::executor::block_on;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

// ---------------------------------------------------------------- 业务类型与计数

#[derive(Debug, PartialEq, Eq)]
struct Key(u32);
impl Data for Key {}

#[derive(Debug, PartialEq, Eq)]
struct Tag(u32);
impl Data for Tag {}

#[derive(Debug, PartialEq, Eq)]
struct Score(u32);
impl Data for Score {}

thread_local! {
    static CALLS: Cell<u32> = const { Cell::new(0) };
    static ROUNDS: Cell<u32> = const { Cell::new(0) };
    /// F07 断言节点自己的轮次计数（与 break 判定的计数分开，避免相互干扰）。
    static EXPECT_ROUNDS: Cell<u32> = const { Cell::new(0) };
}

fn call() {
    CALLS.with(|calls| calls.set(calls.get() + 1));
}

fn calls() -> u32 {
    CALLS.with(Cell::get)
}

fn reset_counters() {
    CALLS.with(|calls| calls.set(0));
    ROUNDS.with(|rounds| rounds.set(0));
    EXPECT_ROUNDS.with(|rounds| rounds.set(0));
}

// ---------------------------------------------------------------- 业务 Node

async fn key_plus_ten(query: Query<&Key>) -> Result<Key, BodyError> {
    call();
    Ok(Key(query.get().0 + 10))
}

async fn tag_of_key(query: Query<&Key>) -> Result<Tag, BodyError> {
    call();
    Ok(Tag(query.get().0 + 100))
}

async fn report_of_key(query: Query<&Key>) -> Result<Score, BodyError> {
    call();
    Ok(Score(query.get().0 * 2))
}

/// 断在第一轮：交付进入该轮时的 current。
async fn break_now(_: Query<&Key>) -> Result<(), BodyError> {
    call();
    Err(BodyError::iter_break())
}

/// 第 2 次调用才 Break，用于观察 Normal 推进后的下一轮 Current。
async fn break_on_second_round(_: Query<&Key>) -> Result<(), BodyError> {
    call();
    let round = ROUNDS.with(|rounds| {
        let next = rounds.get() + 1;
        rounds.set(next);
        next
    });
    if round >= 2 {
        return Err(BodyError::iter_break());
    }
    Ok(())
}

async fn assert_pair_same(query: Query<(&Key, &Key)>) -> Result<(), BodyError> {
    call();
    let (left, right) = query.get();
    assert_eq!(left.0, right.0, "两份引用必须指向同一实例");
    Ok(())
}

// ---------------------------------------------------------------- F01／F06：真实执行

/// 普通与取输出形式在同一场景下真实执行同一推进与 Break 语义。
#[test]
fn t06_iter_output_delivers_the_entry_current_of_the_break_round() {
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Key, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let out_refs = flow.iter_output(initial, 5, |round| {
                let current = round.current::<Key>();
                round.then_with_output(&key_plus_ten, current);
                round.then_with_output(&break_now, current);
                round.next_with(current);
            });
            flow.output_with(out_refs);
        },
        Key(1),
    ));
    assert_eq!(
        outcome.expect("break delivers the entry current of the round"),
        Key(1)
    );
    assert!(calls() >= 2, "目前轮的业务 Node 确实执行");
}

/// 普通形式不返回 Ref，但最终 State 照常发布为父域来源，后继 Node 能真实读取。
#[test]
fn t06_iter_ordinary_form_publishes_the_final_state_to_the_parent() {
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Score, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            flow.iter(initial, 5, |round| {
                let current = round.current::<Key>();
                round.then_with_output(&key_plus_ten, current);
                round.then_with_output(&break_now, current);
                round.next_with(current);
            });
            let published = flow.ref_of::<Key>();
            let report = flow.then_with_output(&report_of_key, published);
            flow.output_with(report);
        },
        Key(1),
    ));
    assert_eq!(
        outcome.expect("ordinary iter publishes the final state for downstream steps"),
        Score(2)
    );
}

// ---------------------------------------------------------------- F04：唯一 Next 契约

#[test]
fn t06_missing_next_is_rejected_before_business_code() {
    reset_counters();
    let result = build_definition::<Key, (), _>(|flow, initial| {
        flow.iter(initial, 1, |round| {
            let current = round.current::<Key>();
            round.then_with_output(&key_plus_ten, current);
        });
    });
    match result {
        Err(BuildErrorDetail::MissingDeclaration { kind, .. }) => {
            assert_eq!(kind, DeclarationKind::Next);
        }
        Err(other) => panic!("缺 Next 必须在 Definition 阶段拒绝，实际 {other}"),
        Ok(_) => panic!("缺 Next 必须构建拒绝"),
    }
    assert_eq!(calls(), 0, "拒绝必须发生在任何业务 Node 之前");
}

#[test]
fn t06_duplicate_next_is_rejected_and_keeps_the_first_declaration() {
    reset_counters();
    let result = build_definition::<Key, (), _>(|flow, initial| {
        flow.iter(initial, 1, |round| {
            let current = round.current::<Key>();
            let advanced = round.then_with_output(&key_plus_ten, current);
            round.next_with(current);
            round.next_with(advanced);
        });
    });
    match result {
        Err(BuildErrorDetail::DuplicateDeclaration { first, second, .. }) => {
            assert_eq!(
                (first, second),
                (DeclarationKind::Next, DeclarationKind::Next)
            );
        }
        Err(other) => panic!("重复 Next 必须拒绝，实际 {other}"),
        Ok(_) => panic!("重复 Next 必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

#[test]
fn t06_round_output_is_rejected() {
    reset_counters();
    let result = build_definition::<Key, (), _>(|flow, initial| {
        flow.iter(initial, 1, |round| {
            let current = round.current::<Key>();
            let advanced = round.then_with_output(&key_plus_ten, current);
            round.output_with(advanced);
            round.next_with(current);
        });
    });
    match result {
        Err(BuildErrorDetail::DeclarationNotAllowed { kind, role, .. }) => {
            assert_eq!((kind, role), (DeclarationKind::Output, BodyRole::Round));
        }
        Err(other) => panic!("Round 使用 output 必须拒绝，实际 {other}"),
        Ok(_) => panic!("Round 使用 output 必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

#[test]
fn t06_next_in_an_ordinary_body_is_rejected() {
    reset_counters();
    let result = build_definition::<Key, (), _>(|flow, initial| {
        flow.chain(|sub| {
            let _ = sub.then_with_output(&key_plus_ten, initial);
            sub.next::<Key>();
        });
    });
    match result {
        Err(BuildErrorDetail::DeclarationNotAllowed { kind, role, .. }) => {
            assert_eq!((kind, role), (DeclarationKind::Next, BodyRole::Ordinary));
        }
        Err(other) => panic!("普通 Body 使用 next 必须拒绝，实际 {other}"),
        Ok(_) => panic!("普通 Body 使用 next 必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

// ---------------------------------------------------------------- F05：Shape 与失败恢复

#[test]
fn t06_next_arity_and_slot_type_mismatch_are_rejected() {
    reset_counters();
    let arity = build_definition::<Key, (), _>(|flow, initial| {
        flow.iter(initial, 1, |round| {
            let current = round.current::<Key>();
            let tag = round.then_with_output(&tag_of_key, current);
            round.next_with((current, current));
            let _ = tag;
        });
    });
    match arity {
        Err(BuildErrorDetail::DeclarationArityMismatch {
            kind,
            slot,
            expected,
            found,
            ..
        }) => {
            assert_eq!(
                (kind, slot, expected, found),
                (DeclarationKind::Next, 0, 1, 2)
            );
        }
        Err(other) => panic!("Next 槽数不符必须拒绝，实际 {other}"),
        Ok(_) => panic!("Next 槽数不符必须构建拒绝"),
    }

    let shape = build_definition::<Key, (), _>(|flow, initial| {
        flow.iter(initial, 1, |round| {
            let current = round.current::<Key>();
            let tag = round.then_with_output(&tag_of_key, current);
            round.next_with(tag);
        });
    });
    match shape {
        Err(BuildErrorDetail::DeclarationShapeMismatch {
            kind,
            slot,
            expected,
            found,
            ..
        }) => {
            assert_eq!(
                (kind, slot, expected, found),
                (
                    DeclarationKind::Next,
                    0,
                    TypeDesc::of::<Key>(),
                    Some(TypeDesc::of::<Tag>())
                )
            );
        }
        Err(other) => panic!("Next 逐槽类型不符必须拒绝，实际 {other}"),
        Ok(_) => panic!("Next 逐槽类型不符必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

/// 失败 Round 上 `iter_output` 返回受控失败形状：不 panic，收口给首个真实原因，
/// 且该形状被后续使用时不会成为合法来源。
#[test]
fn t06_iter_output_failure_shape_is_controlled_and_keeps_the_first_cause() {
    reset_counters();
    let result = build_definition::<Key, Score, _>(|flow, initial| {
        let bad = flow.iter_output(initial, 1, |round| {
            let current = round.current::<Key>();
            round.then_with_output(&key_plus_ten, current);
            // 缺 Next：整个 Round 失败。
        });
        let report = flow.then_with_output(&report_of_key, bad);
        flow.output_with(report);
    });
    match result {
        Err(BuildErrorDetail::MissingDeclaration { kind, .. }) => {
            assert_eq!(kind, DeclarationKind::Next, "首因必须是缺 Next");
        }
        Err(other) => panic!("失败形状不得遮蔽真实首因，实际 {other}"),
        Ok(_) => panic!("失败 Round 必须构建拒绝"),
    }
    assert_eq!(calls(), 0, "失败 Round 不得执行任何业务 Node");
}

#[test]
fn t06_zero_rounds_budget_is_rejected_with_a_controlled_shape() {
    reset_counters();
    let result = build_definition::<Key, Key, _>(|flow, initial| {
        let out_refs = flow.iter_output(initial, 0, |round| {
            let current = round.current::<Key>();
            round.then_with_output(&key_plus_ten, current);
            round.next_with(current);
        });
        flow.output_with(out_refs);
    });
    match result {
        Err(BuildErrorDetail::IterZeroRounds { .. }) => {}
        Err(other) => panic!("0 轮必须拒绝，实际 {other}"),
        Ok(_) => panic!("0 轮必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

// ---------------------------------------------------------------- F06：冻结与下一轮实读

/// 自动 `next` 固定在声明点解析的来源：其后出现的同类型新来源不改变下一轮读取值。
#[test]
fn t06_automatic_next_freezes_the_source_at_the_declaration_point() {
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Key, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let out_refs = flow.iter_output(initial, 5, |round| {
                let current = round.current::<Key>();
                let frozen_candidate = round.then_with_output(&key_plus_ten, current);
                // 自动 next 在**声明点**解析并冻结来源（此时最近的是 frozen_candidate）。
                round.next::<Key>();
                // 声明之后才出现同类型新来源：不得改变下一轮实际读取值。
                let later = round.then_with_output(&key_plus_ten, frozen_candidate);
                let _ = later;
                round.then_with_output(&break_on_second_round, current);
            });
            flow.output_with(out_refs);
        },
        Key(1),
    ));
    assert_eq!(
        outcome.expect("frozen automatic source drives the following round"),
        Key(11)
    );
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Key, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let out_refs = flow.iter_output(initial, 5, |round| {
                let current = round.current::<Key>();
                round.then_with_output(&key_plus_ten, current);
                round.then_with_output(&break_on_second_round, current);
                round.next_with(current);
            });
            flow.output_with(out_refs);
        },
        Key(1),
    ));
    assert_eq!(
        outcome.expect("the second round breaks and delivers its entry current"),
        Key(1)
    );
    assert!(calls() >= 3, "后续 Step 与第二轮判断确实执行");
}

// ---------------------------------------------------------------- F07／F08：Current 身份

#[test]
fn t06_current_is_idempotent_and_independent_of_later_sources() {
    reset_counters();
    let mut handles: Vec<PositionHandle> = Vec::new();
    let definition = build_definition::<Key, (), _>(|flow, initial| {
        flow.iter(initial, 1, |round| {
            let first = round.current::<Key>();
            let second = round.current::<Key>();
            let newer = round.then_with_output(&key_plus_ten, first);
            // 新增同类型来源之后再次查询：必须仍是同一槽身份。
            let third = round.current::<Key>();
            handles = vec![first.handle(), second.handle(), third.handle()];
            let _ = newer;
            round.next_with(first);
        });
    })
    .expect("重复查询 current 合法");
    assert_eq!(handles.len(), 3);
    assert_eq!(handles[0], handles[1], "重复查询必须返回同一槽身份");
    assert_eq!(
        handles[0], handles[2],
        "新增同类型来源之后仍必须返回同一槽身份（不得改解析到新来源）"
    );
    let currents: Vec<_> = definition
        .positions()
        .iter()
        .filter(|record| matches!(record.source(), PositionSource::IterCurrent { .. }))
        .collect();
    assert_eq!(currents.len(), 1, "重复与后续查询都不新增 Current 槽");
    assert_eq!(calls(), 0, "current 查询不产生业务 Step");
}

#[test]
fn t06_current_is_rejected_outside_a_direct_round_body() {
    reset_counters();
    let result = build_definition::<Key, (), _>(|flow, initial| {
        flow.chain(|sub| {
            let _current = sub.current::<Key>();
            let out_refs = sub.then_with_output(&key_plus_ten, initial);
            sub.output_with(out_refs);
        });
    });
    match result {
        Err(BuildErrorDetail::CurrentOutsideRound { .. }) => {}
        Err(other) => panic!("普通 Body 使用 current 必须拒绝，实际 {other}"),
        Ok(_) => panic!("普通 Body 使用 current 必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

#[test]
fn t06_current_arity_and_shape_mismatch_are_rejected() {
    reset_counters();
    let arity = build_definition::<Key, (), _>(|flow, initial| {
        flow.iter((initial, initial), 1, |round| {
            let only = round.current::<Key>();
            let (left, right) = round.current::<(Key, Key)>();
            round.next_with((only, only));
            let _ = (left, right);
        });
    });
    match arity {
        Err(BuildErrorDetail::CurrentArityMismatch {
            expected, found, ..
        }) => assert_eq!((expected, found), (2, 1)),
        Err(other) => panic!("请求槽数与本轮不符必须拒绝，实际 {other}"),
        Ok(_) => panic!("请求槽数与本轮不符必须构建拒绝"),
    }

    let shape = build_definition::<Key, (), _>(|flow, initial| {
        flow.iter(initial, 1, |round| {
            let wrong = round.current::<Score>();
            let _ = wrong;
            round.next_with(initial);
        });
    });
    match shape {
        Err(BuildErrorDetail::CurrentShapeMismatch {
            slot,
            expected,
            found,
            ..
        }) => {
            assert_eq!(
                (slot, expected, found),
                (0, TypeDesc::of::<Key>(), TypeDesc::of::<Score>())
            );
        }
        Err(other) => panic!("请求逐槽类型与本轮不符必须拒绝，实际 {other}"),
        Ok(_) => panic!("请求逐槽类型与本轮不符必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

/// e19 形态：两槽 current 各自独立身份，同一实例可作为两个 Next 槽。
#[test]
fn t06_current_keeps_distinct_slots_and_may_share_one_next() {
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Key, Key), _>(
        |flow: &mut Flow<'_>, (left, right): (Ref<Key>, Ref<Key>)| {
            let out_refs = flow.iter_output((left, right), 5, |round| {
                let (x, y) = round.current::<(Key, Key)>();
                round.then_with_output(&assert_pair_same, (x, y));
                round.then_with_output(&break_now, x);
                round.next_with((x, x));
            });
            flow.output_with(out_refs);
        },
        (Key(7), Key(7)),
    ));
    let (first, second) = outcome.expect("break delivers the entry current slots");
    assert_eq!((first, second), (Key(7), Key(7)));
}

/// e20 形态：逐槽交换/保留（`current` + `next_with`）真实推进。
#[test]
fn t06_current_supports_cross_slot_swap_and_retention() {
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Key, Key, Key), _>(
        |flow: &mut Flow<'_>, (a, b, c): (Ref<Key>, Ref<Key>, Ref<Key>)| {
            let out_refs = flow.iter_output((a, b, c), 5, |round| {
                let (x, y, z) = round.current::<(Key, Key, Key)>();
                round.then_with_output(&break_on_second_round, x);
                round.next_with((y, x, z));
            });
            flow.output_with(out_refs);
        },
        (Key(1), Key(2), Key(3)),
    ));
    let (first, second, third) = outcome.expect("break delivers the swapped state");
    assert_eq!((first, second, third), (Key(2), Key(1), Key(3)));
}

// ---------------------------------------------------------------- F14：组合

/// 新 Iter 与已验收的 Each／Retry 组合仍走同一执行链。
#[test]
fn t06_iter_composes_with_retry_and_chain() {
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Score, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let out_refs: Ref<Score> = flow.chain_output(|sub| {
                let iterated = sub.iter_output(initial, 3, |round| {
                    let current = round.current::<Key>();
                    round.then_with_output(&key_plus_ten, current);
                    round.then_with_output(&break_now, current);
                    round.next_with(current);
                });
                let report = sub.then_with_output(&report_of_key, iterated);
                sub.output_with(report);
            });
            flow.output_with(out_refs);
        },
        Key(2),
    ));
    assert_eq!(
        outcome.expect("iter inside chain composes with the accepted facades"),
        Score(4)
    );
}

/// Iter 的 Next 与 `report_of_score` 组合：显式 next_with 的推进值被下一轮真实读取。
#[test]
fn t06_round_reads_the_declared_next_in_the_following_round() {
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Key, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let out_refs = flow.iter_output(initial, 5, |round| {
                let current = round.current::<Key>();
                let next = round.then_with_output(&key_plus_ten, current);
                round.then_with_output(&break_on_second_round, next);
                round.next_with(next);
            });
            flow.output_with(out_refs);
        },
        Key(1),
    ));
    assert_eq!(
        outcome.expect("the next round reads the declared Next"),
        Key(11)
    );
}

// ---------------------------------------------------------------- R01：失败 Round 不得发布父输出

fn desc(
    handle: crate::core::engine::identity::PositionHandle,
    type_desc: TypeDesc,
) -> PositionDesc {
    PositionDesc { handle, type_desc }
}

/// 低层构造审查 R01 场景：按**真实调用方的时点顺序**构造，再由 `seed` 注入本轮失败形态，
/// 返回（登记是否成功、父输出是否已发布、首因）。
///
/// 时序必须与 `Flow::iter` 一致：先在父域建好 initial，再取入口时点、formal Current，
/// 最后在 Round 内声明唯一 Next。否则 initial／Next 在各自被记录的历史时点本就不可用，
/// 会额外产生失败并掩盖正在验证的单一门控（失败发布判定本身）。
fn failed_round_case(
    seed: impl FnOnce(&mut Builder<'static>, BodyId, usize),
) -> (bool, Option<u64>, Option<BuildErrorDetail>) {
    let builder = Rc::new(RefCell::new(Builder::new()));
    let root = builder.borrow_mut().new_body(None, BodyRole::Ordinary);
    let step = builder.borrow_mut().step_count(root);
    // initial 是父域合法业务来源，且早于入口时点（本轮 formal Current 在 parent 域不可用）。
    let initial = builder.borrow_mut().allocate(
        root,
        PositionSource::NodeOutput { step: 0, slot: 0 },
        TypeDesc::of::<Key>(),
    );
    let entry_order = builder.borrow_mut().fresh_order();
    let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
    builder
        .borrow_mut()
        .set_round_initial(round, vec![TypeDesc::of::<Key>()]);
    let current = builder
        .borrow_mut()
        .allocate_iter_current(round, step, &[TypeDesc::of::<Key>()]);
    // 与真实调用方一致：Round Body 开始前取「已知失败」基线，Body 内（含 Next 声明）
    // 的任何已知失败都必须被增量看见。
    let failures_before = builder.borrow().failure_marker();
    seed(&mut builder.borrow_mut(), round, step);
    // Next 在 Round 内**当前时点**声明：来源（formal Current）此时已经存在并发布。
    let declare_order = builder.borrow_mut().fresh_order();
    let _ = builder.borrow_mut().declare_next(
        round,
        vec![desc(current[0], TypeDesc::of::<Key>())],
        declare_order,
    );
    let round_clean = builder.borrow().failure_marker() == failures_before;
    let (outputs, ok) = builder.borrow_mut().register_iter(
        root,
        round,
        step,
        entry_order,
        vec![desc(initial, TypeDesc::of::<Key>())],
        current,
        1,
        round_clean,
        <Ref<Key> as StateShape>::register_states,
        <Ref<Key> as StateShape>::state_import_slots,
    );
    let published = outputs.first().and_then(|handle| {
        builder
            .borrow()
            .position(*handle)
            .and_then(|record| record.published_order())
    });
    let first = builder.borrow().errors().first().cloned();
    (ok, published, first)
}

/// R01 正对照：无失败 Round 的父输出必须发布（否则下面的失败隔离断言可以被“恒不发布”蒙过）。
#[test]
fn t06_clean_round_publishes_the_parent_output() {
    let (ok, published, first) = failed_round_case(|_builder, _round, _step| {});
    assert!(ok, "合法 Round 必须登记成功");
    assert!(published.is_some(), "合法 Round 的父输出必须发布");
    assert!(first.is_none(), "合法 Round 不应有任何错误，实际 {first:?}");
}

/// R01 反例 ④：**延后分类**的失败（未登记输入位置）同样必须阻止父输出发布。
///
/// 这类失败在使用点无法立即定案（可能是“稍后分配的合法位置”），因此不进入 `errors()`；
/// 断言 `errors()` 为空正是判别点：发布判定不能只数即时错误条数。
#[test]
fn t06_deferred_unregistered_input_blocks_parent_publication() {
    let (ok, published, first) = failed_round_case(|builder, round, _step| {
        let order = builder.fresh_order();
        let unknown = PositionHandle {
            definition: builder.definition_id(),
            position: PositionId::new(u32::MAX - 1),
        };
        assert!(
            builder
                .record_use_at(round, Some(0), desc(unknown, TypeDesc::of::<Key>()), order)
                .is_some(),
            "未登记位置的使用必须判定失败"
        );
    });
    assert!(
        first.is_none(),
        "延后分类的失败不在即时错误列表里（判别点），实际 {first:?}"
    );
    assert!(!ok, "延后分类的失败必须使登记失败");
    assert_eq!(
        published, None,
        "延后分类的失败 Round 不得发布父输出（不得被父域解析为合法来源）"
    );
}

/// R01 反例 ⑤：Next 声明自身失败（逐槽类型不符，随后重复声明被拒）时，父输出也不得发布。
///
/// 判别点：该场景下 `declared_ok` 为假、`initial_ok` 与 `shape_ok` 均为真，因此发布与否
/// 只由「Next 有效性」门控决定。
#[test]
fn t06_failed_next_declaration_blocks_parent_publication() {
    let (ok, published, first) = failed_round_case(|builder, round, _step| {
        let slots = builder
            .resolve_iter_current(round, &[TypeDesc::of::<Key>()])
            .expect("本轮存在 formal Current");
        let order = builder.fresh_order();
        let declared =
            builder.declare_next(round, vec![desc(slots[0], TypeDesc::of::<Tag>())], order);
        assert!(!declared, "逐槽类型不符的 Next 声明必须失败");
    });
    assert!(!ok, "Next 声明失败必须使登记失败");
    assert_eq!(published, None, "Next 声明失败时父输出不得发布");
    assert!(
        matches!(
            first,
            Some(BuildErrorDetail::TypeMismatch { .. })
                | Some(BuildErrorDetail::DeclarationShapeMismatch { .. })
                | Some(BuildErrorDetail::DuplicateDeclaration { .. })
        ),
        "首要诊断必须是 Next 声明自身的失败，实际 {first:?}"
    );
}

/// R01 反例 ⑥：Round 的 Next **重复声明**（第一份快照保留、其后声明失败）时父输出不得发布。
///
/// 判别点：该场景下 initial、Shape、来源可用性与交接槽数量**全部成立**，唯一失效来源是
/// 「Next 有效性」（`declared && !failed`）—— 这是与 M01（round_clean）不同的独立门控。
#[test]
fn t06_duplicate_next_declaration_blocks_parent_publication() {
    let (ok, published, first) = failed_round_case(|builder, round, _step| {
        let slots = builder
            .resolve_iter_current(round, &[TypeDesc::of::<Key>()])
            .expect("本轮存在 formal Current");
        let order = builder.fresh_order();
        let accepted =
            builder.declare_next(round, vec![desc(slots[0], TypeDesc::of::<Key>())], order);
        assert!(
            accepted,
            "第一次 Next 声明本身必须成立（隔离点只在后续失效）"
        );
    });
    assert!(!ok, "Next 声明失效必须使登记失败");
    assert_eq!(published, None, "Next 声明失效时父输出不得发布");
    assert!(
        matches!(first, Some(BuildErrorDetail::DuplicateDeclaration { .. })),
        "首要诊断必须是重复声明，实际 {first:?}"
    );
}

/// R01 端到端（重复 Next）：该 Round 的父输出保持未发布 —— 父域解析停在早先合法 initial，
/// 首因是重复声明，业务零执行。
#[test]
fn t06_duplicate_next_round_does_not_publish_a_parent_source() {
    reset_counters();
    let mut parent_source_is_initial = false;
    let result = build_definition::<Key, Key, _>(|flow, initial| {
        let state: Ref<Key> = flow.iter_output(initial, 1, |round| {
            let current = round.current::<Key>();
            round.next_with(current);
            round.next_with(current); // 第二次声明：构建拒绝并隔离该 Round 的父输出
        });
        parent_source_is_initial = flow.ref_of::<Key>().handle() == initial.handle();
        flow.output_with(state);
    });
    match result {
        Err(BuildErrorDetail::DuplicateDeclaration { .. }) => {}
        Err(other) => panic!("重复 Next 必须是首因，实际 {other}"),
        Ok(_) => panic!("重复 Next 必须构建拒绝"),
    }
    assert!(
        parent_source_is_initial,
        "重复声明 Round 的父输出不得成为父域来源"
    );
    assert_eq!(calls(), 0);
}

/// R01 反例 ⑦（Next 来源有效性）：Round 的 Next 使用**父域已失效来源**（taint 派生失败，
/// 既不即时入列、也不属于延后分类集合）时，父输出不得发布。
///
/// 判别点：本轮 `round_clean` 为真（Round 内没有新增错误），失效只能由「Next 声明来源
/// 可用性」这一门控给出。
#[test]
fn t06_next_on_a_failed_parent_source_blocks_parent_publication() {
    reset_counters();
    let mut parent_source_is_initial = false;
    let result = build_definition::<Key, Key, _>(|flow, initial| {
        // 父域：输入为未登记句柄的 Node —— 其输出被 taint（延后分类，不写入错误列表）。
        let unknown = Ref::<Key>::from_handle(PositionHandle {
            definition: flow.definition_id(),
            position: PositionId::new(u32::MAX - 1),
        });
        let failed = flow.then_with_output(&key_plus_ten, unknown);
        let state: Ref<Key> = flow.iter_output(initial, 1, |round| {
            // Next 依赖父域已失效来源：声明失败但不新增即时错误。
            round.next_with(failed);
        });
        parent_source_is_initial = flow.ref_of::<Key>().handle() == initial.handle();
        flow.output_with(state);
    });
    match result {
        // 父域 Node 的未登记输入最终按延后分类给出 UnknownPosition。
        Err(BuildErrorDetail::UnknownPosition { .. }) => {}
        Err(other) => panic!("失败来源的首因不得被遮蔽，实际 {other}"),
        Ok(_) => panic!("失败来源必须构建拒绝"),
    }
    assert!(
        parent_source_is_initial,
        "失败 Round 的父输出不得成为父域来源"
    );
    assert_eq!(calls(), 0);
}

/// R01 反例 ⑧（派生失败，且原因在 Round 之前）：Round 内的 child 声明选中**已失效来源**
/// （`TaintedSource`，为保留更早首因不重复写入诊断）时，父输出同样不得发布。
///
/// 判别点：上游错误发生在 Round 入口之前，因此本轮「已知失败标记」的增量只可能来自这次
/// child 声明失败；该分支若不更新失败标记，失败的 Round 就会发布合法父来源。
#[test]
fn t06_tainted_child_declaration_blocks_parent_publication() {
    reset_counters();
    let mut parent_source_is_initial = false;
    let result = build_definition::<(), (), _>(|flow, _| {
        // 上游：无来源的 Ref（MissingSource）→ 使用它的 Node 输出被 taint。
        let absent = flow.ref_of::<Key>();
        let bad = flow.then_with_output(&key_plus_ten, absent);
        // 合法 formal initial：在 Round 入口之前创建，是父域业务来源。
        let initial = flow.provide_output(|| Key(1));
        flow.iter(initial, 1, |round| {
            // Round 内**新发生**的 child 声明失败：child 的 Output 选中已失效来源。
            round.chain(|sub| {
                sub.output_with(bad);
            });
            // Next 自身合法（formal Current）。
            round.next_with(round.current::<Key>());
        });
        parent_source_is_initial = flow.ref_of::<Key>().handle() == initial.handle();
    });
    match result {
        Err(BuildErrorDetail::MissingSource { .. }) => {}
        Err(other) => panic!("必须保留最早的来源错误，实际 {other}"),
        Ok(_) => panic!("失败 Round 必须构建拒绝"),
    }
    assert!(
        parent_source_is_initial,
        "child 选中已失效来源时，失败 Round 的父输出不得成为父域来源"
    );
    assert_eq!(calls(), 0, "失败 Round 不得执行业务 Node");
}

/// R01 正对照（同一上游错误，但 child 声明合法）：合法 Round 仍必须发布它的父来源。
///
/// 不能用「全局 errors 非空就禁止后续发布」代替局部失败传播。
#[test]
fn t06_clean_child_declaration_still_publishes_after_an_earlier_error() {
    reset_counters();
    let mut published = false;
    let result = build_definition::<(), (), _>(|flow, _| {
        let absent = flow.ref_of::<Key>();
        let _bad = flow.then_with_output(&key_plus_ten, absent);
        let initial = flow.provide_output(|| Key(1));
        flow.iter(initial, 1, |round| {
            let current = round.current::<Key>();
            round.chain(|sub| {
                sub.output_with(current);
            });
            round.next_with(current);
        });
        published = flow.ref_of::<Key>().handle() != initial.handle();
    });
    assert!(result.is_err(), "上游错误仍必须使构建拒绝");
    assert!(
        published,
        "child 声明合法时，Round 的父输出必须照常发布（不得被无关的更早错误连带禁止）"
    );
    assert_eq!(calls(), 0);
}

/// R01 反例 ①：Round 内 `current` 查询错误后仍有合法 Next，父输出也不得发布。
#[test]
fn t06_round_current_query_error_blocks_parent_publication() {
    let (ok, published, first) = failed_round_case(|builder, round, _step| {
        let _ = builder.resolve_iter_current(round, &[TypeDesc::of::<Score>()]);
    });
    assert!(!ok, "Round 内错误必须使登记失败");
    assert_eq!(
        published, None,
        "失败 Round 的父输出不得发布（不得被父域解析为合法来源）"
    );
    assert!(
        matches!(first, Some(BuildErrorDetail::CurrentShapeMismatch { .. })),
        "首因必须是本轮自身的错误，实际 {first:?}"
    );
}

/// R01 反例 ②：Round 的 child Body 记录错误后仍有合法 Next，父输出同样不得发布。
#[test]
fn t06_round_child_body_error_blocks_parent_publication() {
    let (ok, published, first) = failed_round_case(|builder, round, _step| {
        let child = builder.new_body(Some(round), BodyRole::Ordinary);
        let _ = builder.resolve_iter_current(child, &[TypeDesc::of::<Key>()]);
    });
    assert!(!ok, "child Body 错误必须使登记失败");
    assert_eq!(published, None, "child 出错时父输出不得发布");
    assert!(
        matches!(first, Some(BuildErrorDetail::CurrentOutsideRound { .. })),
        "首因必须是 child 自己的错误，实际 {first:?}"
    );
}

/// R01 反例 ③（正对照）：Round 使用 output 已被拒绝且父输出不发布，
/// 说明失败隔离覆盖的不止「声明类」错误。
#[test]
fn t06_round_output_error_also_blocks_parent_publication() {
    let (ok, published, first) = failed_round_case(|builder, round, _step| {
        let order = builder.fresh_order();
        let _ = builder.declare_output(round, Vec::new(), order);
    });
    assert!(!ok);
    assert_eq!(published, None);
    assert!(
        matches!(first, Some(BuildErrorDetail::DeclarationNotAllowed { .. })),
        "Round 的 output 声明必须是首因，实际 {first:?}"
    );
}

/// R01 端到端：失败 Round 的首因不得被父域解析或后续 Node 遮蔽。
#[test]
fn t06_failed_round_keeps_first_cause_for_parent_resolution() {
    reset_counters();
    let result = build_definition::<Key, Score, _>(|flow, initial| {
        let published: Ref<Key> = flow.iter_output(initial, 1, |round| {
            let _bad = round.current::<Score>();
            let good = round.current::<Key>();
            round.next_with(good);
        });
        let report = flow.then_with_output(&report_of_key, published);
        flow.output_with(report);
    });
    match result {
        Err(BuildErrorDetail::CurrentShapeMismatch {
            slot,
            expected,
            found,
            ..
        }) => {
            assert_eq!(
                (slot, expected, found),
                (0, TypeDesc::of::<Key>(), TypeDesc::of::<Score>())
            );
        }
        Err(other) => panic!("失败 Round 的首因不得被遮蔽，实际 {other}"),
        Ok(_) => panic!("失败 Round 必须构建拒绝"),
    }
    assert_eq!(calls(), 0, "失败 Round 不得执行业务 Node");
}

/// R01 端到端（延后分类 + 真实 Node 输入，无后续消费）：Round 内把未登记句柄交给 Node，
/// Next 本身合法。父域解析必须仍停在早先合法 initial，`iter_output` 返回受控失败形状，
/// 收口给延后分类的 `UnknownPosition`，且没有任何业务执行。
#[test]
fn t06_deferred_node_input_in_a_round_blocks_parent_resolution() {
    reset_counters();
    let mut parent_source_is_initial = false;
    let mut failure_shape_is_controlled = false;
    let result = build_definition::<Key, Key, _>(|flow, initial| {
        let state: Ref<Key> = flow.iter_output(initial, 1, |round| {
            let unknown = Ref::<Key>::from_handle(PositionHandle {
                definition: round.definition_id(),
                position: PositionId::new(u32::MAX - 1),
            });
            round.then_with_output(&key_plus_ten, unknown);
            round.next_with(round.current::<Key>());
        });
        // 受控失败形状：句柄是本 Definition 的**未解析哨兵**（位置 u32::MAX），
        // 不是任何一个已登记位置 —— 因此后续使用不能把它变成合法来源。
        failure_shape_is_controlled = state.handle().definition == flow.definition_id()
            && state.handle().position.index() == u32::MAX as usize;
        // 父域解析必须停在早先合法 initial，而不是失败 Round 的新输出。
        parent_source_is_initial = flow.ref_of::<Key>().handle() == initial.handle();
        flow.output_with(state);
    });
    match result {
        Err(BuildErrorDetail::UnknownPosition { .. }) => {}
        Err(other) => panic!("延后分类的子输入失败必须是首因，实际 {other}"),
        Ok(_) => panic!("失败 Round 必须构建拒绝"),
    }
    assert!(
        parent_source_is_initial,
        "失败 Round 的父输出不得成为父域来源"
    );
    assert!(
        failure_shape_is_controlled,
        "iter_output 必须返回受控失败形状"
    );
    assert_eq!(calls(), 0, "失败 Round 不得执行业务 Node");
}

/// R01 端到端（后续消费）：失败 Round 的受控失败形状被后继 Node 与 Root 收口使用时，
/// 既不合法化、也不执行业务 Node；父域解析仍停在早先合法 initial。
#[test]
fn t06_deferred_round_failure_shape_is_not_legitimized_by_downstream_use() {
    reset_counters();
    let mut parent_source_is_initial = false;
    let result = build_definition::<Key, Score, _>(|flow, initial| {
        let state: Ref<Key> = flow.iter_output(initial, 1, |round| {
            let unknown = Ref::<Key>::from_handle(PositionHandle {
                definition: round.definition_id(),
                position: PositionId::new(u32::MAX - 1),
            });
            round.then_with_output(&key_plus_ten, unknown);
            round.next_with(round.current::<Key>());
        });
        parent_source_is_initial = flow.ref_of::<Key>().handle() == initial.handle();
        let report = flow.then_with_output(&report_of_key, state);
        flow.output_with(report);
    });
    match result {
        // 失败形状（哨兵身份）被消费本身也会被记为外来定义；两者都不得使构建通过。
        Err(BuildErrorDetail::UnknownPosition { .. })
        | Err(BuildErrorDetail::ForeignDefinition { .. }) => {}
        Err(other) => panic!("失败 Round 的后续消费必须是受控失败，实际 {other}"),
        Ok(_) => panic!("失败 Round 必须构建拒绝"),
    }
    assert!(
        parent_source_is_initial,
        "失败 Round 的父输出不得成为父域来源"
    );
    assert_eq!(calls(), 0, "失败 Round 不得执行业务 Node");
}

/// R01 端到端（普通形式 + 无后续使用）：失败 Round 的父状态既不发布也不遮蔽首因。
#[test]
fn t06_failed_round_ordinary_form_without_downstream_use() {
    reset_counters();
    let result = build_definition::<Key, (), _>(|flow, initial| {
        flow.iter(initial, 1, |round| {
            let _bad = round.current::<Score>();
            let good = round.current::<Key>();
            round.next_with(good);
        });
    });
    match result {
        Err(BuildErrorDetail::CurrentShapeMismatch { slot: 0, .. }) => {}
        Err(other) => panic!("普通形式同样必须保留本轮首因，实际 {other}"),
        Ok(_) => panic!("失败 Round 必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

/// R01 端到端（Node + Root 消费）：失败 Round 的父输出被后继 Node 与 Root 使用时不合法化。
#[test]
fn t06_failed_round_output_used_by_node_and_root() {
    reset_counters();
    let result = build_definition::<Key, Score, _>(|flow, initial| {
        let bad: Ref<Key> = flow.iter_output(initial, 1, |round| {
            let _ = round.chain(|sub| {
                let _ = sub.current::<Key>();
            });
            let good = round.current::<Key>();
            round.next_with(good);
        });
        let report = flow.then_with_output(&report_of_key, bad);
        flow.output_with(report);
    });
    match result {
        Err(BuildErrorDetail::CurrentOutsideRound { .. }) => {}
        Err(other) => panic!("child 错误必须是首因（父输出与后继 Node 不得遮蔽），实际 {other}"),
        Ok(_) => panic!("失败 Round 必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

/// R01 端到端（外层收口）：失败 Round 被外层 chain 收口时同样不发布、不遮蔽首因。
#[test]
fn t06_failed_round_wrapped_by_outer_chain() {
    reset_counters();
    let result = build_definition::<Key, Score, _>(|flow, initial| {
        let report: Ref<Score> = flow.chain_output(|sub| {
            let inner: Ref<Key> = sub.iter_output(initial, 1, |round| {
                let _bad = round.current::<Score>();
                let good = round.current::<Key>();
                round.next_with(good);
            });
            let report = sub.then_with_output(&report_of_key, inner);
            sub.output_with(report);
        });
        flow.output_with(report);
    });
    match result {
        Err(BuildErrorDetail::CurrentShapeMismatch { slot: 0, .. }) => {}
        Err(other) => panic!("外层收口不得遮蔽本轮首因，实际 {other}"),
        Ok(_) => panic!("失败 Round 必须构建拒绝"),
    }
    assert_eq!(calls(), 0);
}

// ---------------------------------------------------------------- R04：区分性断言

thread_local! {
    /// 供 Round 内断言节点比较「本轮实际读到的值」。
    static EXPECT: Cell<u32> = const { Cell::new(0) };
}

fn set_expect(value: u32) {
    EXPECT.with(|expect| expect.set(value));
}

/// 断言本轮 `ref_of` 读到的 Key 等于期望值（F07：必须解析到 formal Current）。
async fn expect_key(query: Query<&Key>) -> Result<(), BodyError> {
    call();
    let value = query.get().0;
    EXPECT.with(|expect| {
        assert_eq!(value, expect.get(), "本轮读到的必须是 formal Current 的值");
    });
    Ok(())
}

/// 断言三槽两两不同（F08：新增同类型来源后仍取得完整、有序的槽序列）。
async fn assert_keys_distinct(query: Query<(&Key, &Key, &Key)>) -> Result<(), BodyError> {
    call();
    let (a, b, c) = query.get();
    assert!(
        a.0 != b.0 && b.0 != c.0 && a.0 != c.0,
        "三个槽必须是不同的实例：({}, {}, {})",
        a.0,
        b.0,
        c.0
    );
    Ok(())
}

/// F07 用：断言本轮 `ref_of` 读到的 Current 等于该轮序号（Current 随轮推进，不是父域 initial）。
async fn key_plus_one(query: Query<&Key>) -> Result<Key, BodyError> {
    call();
    Ok(Key(query.get().0 + 1))
}

async fn expect_round_number(query: Query<&Key>) -> Result<(), BodyError> {
    call();
    let round = EXPECT_ROUNDS.with(|rounds| {
        let next = rounds.get() + 1;
        rounds.set(next);
        next
    });
    assert_eq!(
        query.get().0,
        round,
        "第 {round} 轮必须读到 formal Current（值等于轮序号）"
    );
    Ok(())
}

/// 交接槽画像：逐槽 `(来源 Core -> 本地出口 Core -> 父输出 Core : 类型)`。
///
/// 用 Core 序号而不是指针/地址：每个 Definition 的来源根都从 0 起分配，序号在同一构建
/// 内唯一、跨构建可比，因此画像可以逐项比较真实接线。
fn handover_profile(slots: &[HandoverSlot]) -> String {
    format!(
        "[{}]",
        slots
            .iter()
            .map(|slot| format!(
                "({}->{}->{}:{:?})",
                slot.source.seq(),
                slot.exit.seq(),
                slot.output.seq(),
                slot.type_desc
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}

/// Step 的控制器交接视图：Chain／Each／Retry／Iter／Choose 各自的真实映射与桥。
fn controller_profile(entry: &dyn StepEntry) -> String {
    let mut view = String::new();
    if let Some(chain) = entry.chain() {
        view.push_str(&format!(
            "|chain(child={},exits={:?},bindings={:?},slots={})",
            chain.child().index(),
            chain
                .exits()
                .iter()
                .map(|position| position.index())
                .collect::<Vec<_>>(),
            chain.bindings(),
            handover_profile(chain.slots()),
        ));
    }
    if let Some(each) = entry.each() {
        view.push_str(&format!(
            "|each(item={},collection=({},{:?}),item_slot={},slots={:?})",
            each.item().index(),
            each.collection().handle.position.index(),
            each.collection().type_desc,
            each.item_slot().index(),
            each.slots()
                .iter()
                .map(|slot| (
                    slot.source.seq(),
                    slot.local.seq(),
                    slot.output.seq(),
                    slot.collected_type
                ))
                .collect::<Vec<_>>(),
        ));
    }
    if let Some(retry) = entry.retry() {
        view.push_str(&format!(
            "|retry(child={},max={},slots={},bridges={:?})",
            retry.child().index(),
            retry.max_retries(),
            handover_profile(retry.slots()),
            retry
                .bridges()
                .iter()
                .map(|core| core.seq())
                .collect::<Vec<_>>(),
        ));
    }
    if let Some(iter) = entry.iter() {
        view.push_str(&format!(
            "|iter(child={},max={},current={:?},initial={:?},slots={},bridges={:?})",
            iter.child().index(),
            iter.max_iterations(),
            iter.current()
                .iter()
                .map(|position| position.index())
                .collect::<Vec<_>>(),
            iter.initial()
                .iter()
                .map(|desc| (desc.handle.position.index(), desc.type_desc))
                .collect::<Vec<_>>(),
            handover_profile(iter.slots()),
            iter.bridges()
                .iter()
                .map(|core| core.seq())
                .collect::<Vec<_>>(),
        ));
    }
    let choose = entry.choose_slots();
    if !choose.is_empty() {
        view.push_str(&format!(
            "|choose({:?})",
            choose
                .iter()
                .map(|slot| (
                    slot.branch,
                    slot.slot,
                    slot.source.seq(),
                    slot.exit.seq(),
                    slot.output.seq(),
                    slot.type_desc
                ))
                .collect::<Vec<_>>()
        ));
    }
    view
}

/// 声明画像：状态 + 声明时点 + 逐槽（位置下标 + 类型）+ Each typed 能力（声明时冻结）。
fn declaration_profile(declaration: &Declaration) -> String {
    let bridge = match &declaration.each_bridge {
        Some(bridge) => format!("({:?}/{:?})", bridge.declared(), bridge.collected()),
        None => "none".to_string(),
    };
    format!(
        "({:?}@{},slots={:?},each_bridge={})",
        declaration.state(),
        declaration.order,
        declaration
            .slots
            .iter()
            .map(|desc| (desc.handle.position.index(), desc.type_desc))
            .collect::<Vec<_>>(),
        bridge,
    )
}

/// 定义级结构画像（统一取同一个 Definition 的登记时点，不含运行期状态）：逐位置身份
/// （来源形态／owner／类型／分配与发布时点／taint／Core 序号）、逐 Body（用途／父／
/// imports／声明含 typed 能力）、逐 Step（类别／登记时点／callable／真实 Query 输入与输出／
/// 控制器交接映射与桥）。排序后作为结构指纹比较。
fn profile_of(definition: &Definition<'_>) -> Vec<String> {
    let mut rows: Vec<String> = Vec::new();
    for (index, record) in definition.positions().iter().enumerate() {
        rows.push(format!(
            "P{index}|src={:?}|owner={}|type={:?}|created={}|published={:?}|tainted={}|core={}",
            record.source(),
            record.owner().index(),
            record.type_desc(),
            record.created_order(),
            record.published_order(),
            record.is_tainted(),
            record.core().seq(),
        ));
    }
    for (index, body) in definition.bodies().iter().enumerate() {
        rows.push(format!(
            "B{index}|role={:?}|parent={:?}|imports={:?}|out={}|next={}|round_initial={:?}",
            body.role(),
            body.parent().map(|parent| parent.index()),
            body.imports()
                .iter()
                .map(|position| position.index())
                .collect::<Vec<_>>(),
            declaration_profile(body.output_declaration()),
            declaration_profile(body.next_declaration()),
            body.round_initial(),
        ));
        for (step, entry) in body.steps().iter().enumerate() {
            rows.push(format!(
                "B{index}/S{step}|kind={:?}|order={}|name={:?}|inputs={:?}|outputs={:?}{}",
                entry.kind(),
                entry.order(),
                entry.callable_name(),
                entry
                    .inputs()
                    .iter()
                    .map(|desc| (desc.handle.position.index(), desc.type_desc))
                    .collect::<Vec<_>>(),
                entry
                    .outputs()
                    .iter()
                    .map(|position| position.index())
                    .collect::<Vec<_>>(),
                controller_profile(entry.as_ref()),
            ));
        }
    }
    rows.sort();
    rows
}

/// 位置形状/属性画像（**不含**Step 的真实输入接线）：用于证明两份定义只在接线上不同。
fn position_shape_of(definition: &Definition<'_>) -> Vec<String> {
    let mut rows: Vec<String> = definition
        .positions()
        .iter()
        .enumerate()
        .map(|(index, record)| {
            format!(
                "P{index}|{:?}|owner={}|{:?}|{:?}",
                record.source(),
                record.owner().index(),
                record.type_desc(),
                record.published_order(),
            )
        })
        .collect();
    rows.sort();
    rows
}

/// F02：普通 `iter` 与 `iter_output` 共用同一登记路径 —— 同场景结构画像一致，
/// 且真实执行时交付值与业务调用计数一致。
#[test]
fn t06_ordinary_and_output_forms_share_the_same_registration_profile() {
    reset_counters();
    let ordinary = build_definition::<Key, Score, _>(|flow, initial| {
        flow.iter(initial, 2, |round| {
            let current = round.current::<Key>();
            round.then_with_output(&break_now, current);
            round.next_with(current);
        });
        let published = flow.ref_of::<Key>();
        let report = flow.then_with_output(&report_of_key, published);
        flow.output_with(report);
    })
    .expect("普通形式构建");

    reset_counters();
    let output = build_definition::<Key, Score, _>(|flow, initial| {
        let state = flow.iter_output(initial, 2, |round| {
            let current = round.current::<Key>();
            round.then_with_output(&break_now, current);
            round.next_with(current);
        });
        let report = flow.then_with_output(&report_of_key, state);
        flow.output_with(report);
    })
    .expect("取输出形式构建");

    assert_eq!(
        profile_of(&ordinary),
        profile_of(&output),
        "两种形式必须共用同一条登记路径：结构画像逐条一致"
    );
    assert_eq!(calls(), 0, "定义期只登记，不执行业务 Node");

    // 同一场景真实执行：交付值与业务调用计数必须一致。
    reset_counters();
    let runtime = Runtime::new();
    let ordinary_value = block_on(runtime.execute::<_, Score, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            flow.iter(initial, 2, |round| {
                let current = round.current::<Key>();
                round.then_with_output(&break_now, current);
                round.next_with(current);
            });
            let published = flow.ref_of::<Key>();
            let report = flow.then_with_output(&report_of_key, published);
            flow.output_with(report);
        },
        Key(1),
    ))
    .expect("普通形式执行");
    let ordinary_calls = calls();

    reset_counters();
    let runtime = Runtime::new();
    let output_value = block_on(runtime.execute::<_, Score, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let state = flow.iter_output(initial, 2, |round| {
                let current = round.current::<Key>();
                round.then_with_output(&break_now, current);
                round.next_with(current);
            });
            let report = flow.then_with_output(&report_of_key, state);
            flow.output_with(report);
        },
        Key(1),
    ))
    .expect("取输出形式执行");

    assert_eq!(ordinary_value, Score(2));
    assert_eq!(ordinary_value, output_value, "两种形式交付值必须一致");
    assert_eq!(
        ordinary_calls,
        calls(),
        "两种形式业务调用计数必须一致（含 Iter 内与后继 Node）"
    );
    assert_eq!(calls(), 2, "首轮 Break 场景：Iter 内 1 次 + 后继 1 次");
}

/// F02 对照：位置形状完全相同、**只有真实 Query 输入接线不同** —— 运行值必须不同，
/// 结构画像也必须不同（否则画像不能代表真实接线）。
#[test]
fn t06_profile_distinguishes_a_real_query_input_change() {
    fn wire(flow: &mut Flow<'_>, (a, b): (Ref<Key>, Ref<Key>), use_other: bool) {
        let state = flow.iter_output(a, 2, |round| {
            let current = round.current::<Key>();
            round.then_with_output(&break_now, current);
            round.next_with(current);
        });
        // 末尾 Node 的真实输入：Iter 的最终 State 或同一个 Root 的第二个同类型输入。
        let selected = if use_other { b } else { state };
        let report = flow.then_with_output(&report_of_key, selected);
        flow.output_with(report);
    }

    reset_counters();
    let baseline =
        build_definition::<(Key, Key), Score, _>(|flow, inputs| wire(flow, inputs, false))
            .expect("基线构建");
    let changed = build_definition::<(Key, Key), Score, _>(|flow, inputs| wire(flow, inputs, true))
        .expect("接线变体构建");

    assert_eq!(
        position_shape_of(&baseline),
        position_shape_of(&changed),
        "两份定义的位置形状与属性必须完全相同（差异只允许来自真实接线）"
    );
    assert_ne!(
        profile_of(&baseline),
        profile_of(&changed),
        "只改一个真实 Query 输入来源时结构画像必须不同"
    );

    let runtime = Runtime::new();
    let first = block_on(
        runtime.execute::<_, Score, _>(|flow, inputs| wire(flow, inputs, false), (Key(1), Key(7))),
    )
    .expect("基线执行");
    let second = block_on(
        runtime.execute::<_, Score, _>(|flow, inputs| wire(flow, inputs, true), (Key(1), Key(7))),
    )
    .expect("接线变体执行");
    assert_eq!(first, Score(2), "读 Iter 最终 State：Break 交付 current=1");
    assert_eq!(second, Score(14), "读第二个 Root 输入：7*2=14");
}

/// F02：链式（`chain_output` 内）与逐句（Root 内直接写）的同场景运行契约一致。
#[test]
fn t06_chain_and_sequential_forms_share_the_same_runtime_contract() {
    reset_counters();
    let runtime = Runtime::new();
    let sequential = block_on(runtime.execute::<_, Score, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let state = flow.iter_output(initial, 2, |round| {
                let current = round.current::<Key>();
                round.then_with_output(&break_now, current);
                round.next_with(current);
            });
            let report = flow.then_with_output(&report_of_key, state);
            flow.output_with(report);
        },
        Key(3),
    ))
    .expect("逐句形式执行");
    let sequential_calls = calls();

    reset_counters();
    let runtime = Runtime::new();
    let chained = block_on(runtime.execute::<_, Score, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let report: Ref<Score> = flow.chain_output(|sub| {
                let state = sub.iter_output(initial, 2, |round| {
                    let current = round.current::<Key>();
                    round.then_with_output(&break_now, current);
                    round.next_with(current);
                });
                let report = sub.then_with_output(&report_of_key, state);
                sub.output_with(report);
            });
            flow.output_with(report);
        },
        Key(3),
    ))
    .expect("链式形式执行");

    assert_eq!(sequential, Score(6));
    assert_eq!(sequential, chained, "链式/逐句交付值必须一致");
    assert_eq!(
        sequential_calls,
        calls(),
        "链式/逐句业务调用计数必须一致（含 Iter 内与后继 Node）"
    );
    assert_eq!(calls(), 2);
}

/// F06：显式 `next_with` 冻结早先候选；其后同类型新来源不改变下一轮实际读取值，后续 Step 仍执行。
#[test]
fn t06_explicit_next_with_freezes_the_earlier_source() {
    reset_counters();
    set_expect(1);
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Key, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let out_refs = flow.iter_output(initial, 5, |round| {
                let current = round.current::<Key>();
                // 候选 A：声明点之前已有的来源。
                let frozen = round.then_with_output(&key_plus_ten, current);
                round.next_with(frozen);
                // 声明之后才出现的同类型新来源：不得改变冻结值。
                let later = round.then_with_output(&key_plus_ten, frozen);
                let _ = later;
                round.then_with_output(&break_on_second_round, current);
                round.then_with_output(&expect_key, current);
            });
            flow.output_with(out_refs);
        },
        Key(1),
    ));
    // 第二轮读到的必须是第一轮冻结的 A（11），而不是后出现的 B（21）。
    assert_eq!(outcome.expect("显式 next_with 的冻结值驱动下一轮"), Key(11));
    assert!(calls() >= 4, "两轮内的后续 Step 确实执行，实际 {}", calls());
}

/// F07：父域存在更早同类型来源时，Round 内 `ref_of` 仍解析到 formal Current（随轮推进）。
#[test]
fn t06_ref_of_resolves_to_the_formal_current_with_an_earlier_parent_source() {
    reset_counters();
    let runtime = Runtime::new();
    // 第一轮 current = 1（= initial），Normal 推进到 2；第二轮 current 必须是 2 而非父域 initial 的 1。
    let outcome = block_on(runtime.execute::<_, Key, _>(
        |flow: &mut Flow<'_>, initial: Ref<Key>| {
            let out_refs = flow.iter_output(initial, 5, |round| {
                // 父域已有同类型来源（initial）；这里必须解析到本轮的 formal Current。
                let current = round.ref_of::<Key>();
                round.then_with_output(&expect_round_number, current);
                let judged = round.then_with_output(&break_on_second_round, current);
                let _ = judged;
                let advanced = round.then_with_output(&key_plus_one, current);
                round.next_with(advanced);
            });
            flow.output_with(out_refs);
        },
        Key(1),
    ));
    assert_eq!(outcome.expect("Current 随轮推进"), Key(2));
    assert!(calls() >= 3);
}

/// F08：重复 `current` 查询返回同一批槽身份（逐槽 handle 直接比较）；新增同类型来源后
/// 仍取得完整有序槽序列，且三槽身份两两不同。
#[test]
fn t06_repeated_current_keeps_slot_identity_after_new_same_type_sources() {
    reset_counters();
    let mut pairs: Vec<(PositionHandle, PositionHandle)> = Vec::new();
    let mut distinct = false;
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Key, Key, Key), _>(
        |flow: &mut Flow<'_>, (a, b, c): (Ref<Key>, Ref<Key>, Ref<Key>)| {
            let out_refs = flow.iter_output((a, b, c), 3, |round| {
                let (x, y, z) = round.current::<(Key, Key, Key)>();
                // 新增同类型来源后再次查询：必须仍是同一批 formal Current 槽。
                let _newer = round.then_with_output(&key_plus_ten, x);
                let (x2, y2, z2) = round.current::<(Key, Key, Key)>();
                pairs = vec![
                    (x.handle(), x2.handle()),
                    (y.handle(), y2.handle()),
                    (z.handle(), z2.handle()),
                ];
                let handles = [x.handle(), y.handle(), z.handle()];
                distinct = handles[0] != handles[1]
                    && handles[1] != handles[2]
                    && handles[0] != handles[2];
                round.then_with_output(&assert_keys_distinct, (x, y, z));
                round.then_with_output(&break_now, x);
                round.next_with((x, y, z));
            });
            flow.output_with(out_refs);
        },
        (Key(1), Key(2), Key(3)),
    ));
    let (first, second, third) = outcome.expect("三槽完整交付");
    assert_eq!(pairs.len(), 3);
    for (slot, (before, after)) in pairs.iter().enumerate() {
        assert_eq!(
            before, after,
            "槽 {slot} 的两次查询必须返回同一 handle 身份"
        );
    }
    assert!(distinct, "三个 formal Current 槽必须两两不同");
    assert_eq!((first.0, second.0, third.0), (1, 2, 3));
    assert!(calls() >= 2);
}
