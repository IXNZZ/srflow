//! V22-T03 定向测试：Root expectation 首次进入生产构建路径（C03/C04）。
//!
//! 覆盖 `NEW::root_expectation_enforced`：期望 owned Out=B、合法 Factory 产生 A、Node 由 A 产生 B，
//! 而 Body 只声明 A；唯一不符是声明/期望类型，正式 [`Runtime::execute`] 必须以 Definition 拒绝，
//! 且 Node/Factory 均零调用。期望 Out=A 的同一 Body 是运行正对照，证明拒绝来自 expectation
//! 而不是缺来源或类型装配错误。
//!
//! 另含 Root declaration 元数据区分（未声明 / 显式 unit）与 unit／非 unit 互不匹配的定向断言，
//! 以及 unit Root 省略声明时不会从闭包 `()` 补出显式 unit 的对照组。

use crate::core::engine::data::Data;
use crate::core::engine::definition::{DeclarationKind, DeclarationState, build_definition};
use crate::core::engine::error::{BodyError, BuildErrorDetail, RunError};
use crate::core::engine::identity::BodyId;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, TypeDesc};
use futures::executor::block_on;
use std::cell::Cell;

thread_local! {
    /// 合法 Factory 的真实调用计数。
    static FACTORY_CALLS: Cell<u32> = const { Cell::new(0) };
    /// 合法 Node 的真实调用计数。
    static NODE_CALLS: Cell<u32> = const { Cell::new(0) };
}

fn reset_counters() {
    FACTORY_CALLS.with(|calls| calls.set(0));
    NODE_CALLS.with(|calls| calls.set(0));
}

fn factory_calls() -> u32 {
    FACTORY_CALLS.with(Cell::get)
}

fn node_calls() -> u32 {
    NODE_CALLS.with(Cell::get)
}

struct A(u32);
impl Data for A {}

struct B(u32);
impl Data for B {}

fn produce_a() -> A {
    FACTORY_CALLS.with(|calls| calls.set(calls.get() + 1));
    A(1)
}

async fn a_to_b(query: Query<&A>) -> Result<B, BodyError> {
    NODE_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(B(query.get().0 + 1))
}

/// 期望 Out=B，Body 声明 A：唯一不符是声明与期望的 Shape。
///
/// 反例由正式 `execute` 返回 `RunError::Definition`，内部首因为
/// `DeclarationShapeMismatch(expected=B, found=A)`；Factory 与 Node 都在业务之前被拒绝，
/// 计数必须为零（模板闭包本身一次都不会被调用）。
#[test]
fn c03_root_expectation_rejects_declared_shape_mismatch_before_any_business() {
    reset_counters();
    let runtime = Runtime::new();
    let error = block_on(runtime.execute::<_, B, _>(
        |flow, ()| {
            // 合法的静态来源链：Factory 产生 A，Node 由 A 产生 B。
            let produced = flow.provide_output(produce_a);
            let _consumed = flow.then_with_output(a_to_b, produced);
            // 唯一错误：声明 A，而调用方期望 owned Out = B。
            flow.output::<A>();
        },
        (),
    ))
    .err()
    .expect("期望 Out=B 时声明 A 必须被定义拒绝");

    match error {
        RunError::Definition(error) => match error.detail {
            BuildErrorDetail::DeclarationShapeMismatch {
                body,
                kind,
                slot,
                expected,
                found,
            } => {
                assert_eq!(body, BodyId::ROOT, "首因必须落在 Root Body");
                assert_eq!(kind, DeclarationKind::Output);
                assert_eq!(slot, 0);
                assert_eq!(expected, TypeDesc::of::<B>(), "期望来自调用方 owned Out");
                assert_eq!(found, Some(TypeDesc::of::<A>()), "实际是 Body 声明的 A");
            }
            other => panic!("期望 DeclarationShapeMismatch，实际 {other}"),
        },
        other => panic!("期望 RunError::Definition，实际 {other}"),
    }

    assert_eq!(factory_calls(), 0, "定义拒绝前 Factory 不得被调用");
    assert_eq!(node_calls(), 0, "定义拒绝前 Node 不得被调用");
}

/// 同一 Body 在期望 Out=A 时正常运行：证明上面的拒绝来自 expectation 本身。
#[test]
fn c03_root_expectation_accepts_the_same_body_when_the_declaration_matches() {
    reset_counters();
    let runtime = Runtime::new();
    let produced: A = block_on(runtime.execute::<_, A, _>(
        |flow, ()| {
            let produced = flow.provide_output(produce_a);
            let _consumed = flow.then_with_output(a_to_b, produced);
            flow.output::<A>();
        },
        (),
    ))
    .expect("期望 Out=A 时同一声明必须运行成功");

    assert_eq!(produced.0, 1, "Root 交付的是 Factory 产出的 A");
    assert_eq!(factory_calls(), 1, "正对照中 Factory 真实执行一次");
    assert_eq!(node_calls(), 1, "正对照中 Node 真实执行一次");
}

/// 期望 Out=B 且声明 B：合法 Node 输出作为根输出交付（同时读取 B 的 payload）。
#[test]
fn c03_root_expectation_accepts_a_node_output_as_the_root_output() {
    reset_counters();
    let runtime = Runtime::new();
    let produced: B = block_on(runtime.execute::<_, B, _>(
        |flow, ()| {
            let seed = flow.provide_output(produce_a);
            let consumed = flow.then_with_output(a_to_b, seed);
            flow.output::<B>();
            let _ = consumed;
        },
        (),
    ))
    .expect("期望 Out=B 时声明 B 必须运行成功");
    assert_eq!(produced.0, 2, "B 由 A 派生：1 + 1");
    assert_eq!(factory_calls(), 1);
    assert_eq!(node_calls(), 1);
}

/// unit Root 的声明元数据：未声明、`output::<()>()`、`output_with(())` 三者可区分。
#[test]
fn c03_unit_root_metadata_distinguishes_undeclared_and_explicit_unit() {
    struct UnitNode;
    impl crate::core::engine::node::Node for UnitNode {
        type Input = ();
        type Output = ();
        async fn run(&self, _query: Query<&()>) -> Result<(), BodyError> {
            Ok(())
        }
    }

    let undeclared =
        build_definition::<(), (), _>(|_flow, ()| {}).expect("未声明的 unit Root 正常接受");
    assert_eq!(
        undeclared.root().output_declaration().state(),
        DeclarationState::Undeclared,
        "闭包返回 () 不得被当作显式 unit 声明"
    );

    let explicit = build_definition::<(), (), _>(|flow, ()| {
        flow.output::<()>();
    })
    .expect("显式 unit 声明正常接受");
    assert_eq!(
        explicit.root().output_declaration().state(),
        DeclarationState::Unit
    );

    let explicit_with = build_definition::<(), (), _>(|flow, ()| {
        flow.output_with(());
    })
    .expect("output_with(()) 正常接受");
    assert_eq!(
        explicit_with.root().output_declaration().state(),
        DeclarationState::Unit
    );

    let unit_step_without_declaration = build_definition::<(), (), _>(|flow, ()| {
        flow.then(UnitNode);
    })
    .expect("unit Node 可不声明输出");
    assert_eq!(
        unit_step_without_declaration
            .root()
            .output_declaration()
            .state(),
        DeclarationState::Undeclared
    );
    assert_eq!(
        unit_step_without_declaration.root().steps().len(),
        1,
        "省略声明不等于省略步骤：unit Node 仍登记并执行"
    );
}

/// unit／非 unit 互不匹配与缺声明仍是 Definition 拒绝，且不改用更早的无关原因。
#[test]
fn c03_root_expectation_rejects_arity_and_missing_declaration() {
    let extra_slot = build_definition::<(), (), _>(|flow, ()| {
        // 先给出 A 的合法静态来源，确保首因是 arity 不符而不是缺来源。
        let _produced = flow.provide_output(produce_a);
        flow.output::<A>();
    })
    .err()
    .expect("期望 unit 时声明的 A 必须被拒绝");
    assert!(
        matches!(
            extra_slot,
            BuildErrorDetail::DeclarationArityMismatch {
                expected: 0,
                found: 1,
                ..
            }
        ),
        "{extra_slot}"
    );

    let missing_slot = build_definition::<(), B, _>(|flow, ()| {
        flow.output::<()>();
    })
    .err()
    .expect("期望 B 时声明的 unit 必须被拒绝");
    assert!(
        matches!(
            missing_slot,
            BuildErrorDetail::DeclarationArityMismatch {
                expected: 1,
                found: 0,
                ..
            }
        ),
        "{missing_slot}"
    );

    let undeclared = build_definition::<(), B, _>(|_flow, ()| {})
        .err()
        .expect("期望 B 时完全不声明必须被拒绝");
    assert!(
        matches!(
            undeclared,
            BuildErrorDetail::MissingDeclaration {
                body: BodyId::ROOT,
                kind: DeclarationKind::Output,
            }
        ),
        "{undeclared}"
    );
}
