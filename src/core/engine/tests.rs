//! WE-T01 定向测试：覆盖 A01～A15 的声明层行为与构建期拒绝。
//!
//! 测试只使用 crate 内部的登记入口；不执行任何业务 Node。伪造样本（无来源位置、
//! 前向位置、跨边界泄漏）只在测试可控路径中构造，不给生产路径增加伪造能力。

// 说明：`let _: () = …` 是刻意的类型断言（固定 Node 输出的 unit 形状），
// `&Box<T>` 用于证明 Box tuple 是单个 Data 叶子；两者都按测试意图保留。
#![allow(clippy::unused_unit, clippy::let_unit_value, clippy::borrowed_box)]

use super::data::Data;
use super::definition::{ChainBinding, PositionSource, StepKind, build_definition};
use super::error::{BodyError, BuildErrorDetail, Visibility};
use super::flow::Flow;
use super::identity::{PositionHandle, PositionId};
use super::node::Node;
use super::shape::{Query, Ref};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ---------------------------------------------------------------- 业务类型与 Node

/// 非 Clone／Copy 的业务 Data：证明接线与 Ref 复制不要求业务类型可复制。
struct Payload {
    id: u32,
    history: Vec<u32>,
}

impl Data for Payload {}

struct Amount(u32);

impl Data for Amount {}

struct Doubled(u32);

impl Data for Doubled {}

async fn double(query: Query<&Amount>) -> Result<Doubled, BodyError> {
    let amount = query.get();
    Ok(Doubled(amount.0 * 2))
}

async fn add_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

async fn sink_doubled(query: Query<&Doubled>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

async fn load(query: Query<()>) -> Result<Amount, BodyError> {
    let _ = query.get();
    Ok(Amount(0))
}

async fn pair_reader(query: Query<(&Amount, &Amount)>) -> Result<Doubled, BodyError> {
    let (left, right) = query.get();
    Ok(Doubled(left.0 + right.0))
}

async fn consume_payload_a(query: Query<&Payload>) -> Result<u32, BodyError> {
    let payload = query.get();
    Ok(payload.id + payload.history.len() as u32 + 1)
}

async fn consume_payload_b(query: Query<&Payload>) -> Result<u32, BodyError> {
    Ok(query.get().id + 2)
}

struct AddOne;

impl Node for AddOne {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

/// Fragment 首选形式：普通函数返回 closure，保留非 static Node 借用。
fn fragment<'n>(
    node: &'n AddOne,
    input: Ref<Amount>,
) -> impl FnOnce(&mut Flow<'n>) -> Ref<Amount> + 'n {
    move |sub| sub.then_with_output(node, input)
}

/// Fragment 第二种形式：接收 `&mut Flow` 的普通函数，在 chain closure 内调用。
fn call_in_closure<'n>(flow: &mut Flow<'n>, node: &'n AddOne, input: Ref<Amount>) -> Ref<Amount> {
    flow.then_with_output(node, input)
}

// ---------------------------------------------------------------- A01 Root Shape

#[test]
fn a01_unit_root_registers_no_position_and_keeps_zero_input_node() {
    let definition = build_definition::<(), (), _>(|flow, ()| {
        let produced = flow.then_with_output(load, ());
        let _: Ref<Amount> = produced;
        let () = ();
        flow.output_with(());
    })
    .expect("zero-input root should build");
    let root = definition.root();
    assert_eq!(root.steps().len(), 1);
    assert!(root.steps()[0].inputs().is_empty());
    assert_eq!(root.steps()[0].outputs().len(), 1);
    assert!(root.output().is_empty());
    assert_eq!(definition.positions().len(), 1);
}

#[test]
fn a01_root_shapes_single_tuple_and_boundaries() {
    let single = build_definition::<Amount, Amount, _>(|_flow, amount| {
        let out_refs = amount;
        _flow.output_with(out_refs);
    })
    .unwrap();
    assert_eq!(single.root().output().len(), 1);
    assert_eq!(single.positions().len(), 1);

    let pair = build_definition::<(Amount, Doubled), (Amount, Doubled), _>(|_flow, pair| {
        let out_refs = pair;
        _flow.output_with(out_refs);
    })
    .unwrap();
    assert_eq!(pair.root().output().len(), 2);
    assert_ne!(
        pair.root().output()[0].handle.position,
        pair.root().output()[1].handle.position
    );

    let wide = build_definition::<
        (
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
        ),
        (
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
        ),
        _,
    >(|_flow, refs| {
        let out_refs = refs;
        _flow.output_with(out_refs);
    })
    .unwrap();
    assert_eq!(wide.root().output().len(), 16);
    assert_eq!(wide.positions().len(), 16);
}

// ---------------------------------------------------------------- A02 arity 覆盖

macro_rules! arity_case {
    ($name:ident, $arity:literal, $($t:ty),+) => {
        #[test]
        fn $name() {
            async fn node(query: Query<($(&$t,)+)>) -> Result<(), BodyError> {
                let view: ($(&$t,)+) = query.get();
                let _ = view;
                Ok(())
            }
            let definition = build_definition::<($($t,)+), ($($t,)+), _>(|flow, refs| {
                let _: () = flow.then_with_output(node, refs);
                let out_refs = refs;
                flow.output_with(out_refs);
            })
            .expect("every promised arity should register and build");
            assert_eq!(definition.root().steps().len(), 1);
            assert_eq!(definition.root().steps()[0].inputs().len(), $arity);
            assert_eq!(definition.root().output().len(), $arity);
            assert_eq!(definition.positions().len(), $arity);
        }
    };
}

arity_case!(a02_arity_2, 2, u8, u16);
arity_case!(a02_arity_3, 3, u8, u16, u32);
arity_case!(a02_arity_4, 4, u8, u16, u32, u64);
arity_case!(a02_arity_5, 5, u8, u16, u32, u64, u128);
arity_case!(a02_arity_6, 6, u8, u16, u32, u64, u128, usize);
arity_case!(a02_arity_7, 7, u8, u16, u32, u64, u128, usize, i8);
arity_case!(a02_arity_8, 8, u8, u16, u32, u64, u128, usize, i8, i16);
arity_case!(a02_arity_9, 9, u8, u16, u32, u64, u128, usize, i8, i16, i32);
arity_case!(
    a02_arity_10,
    10,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64
);
arity_case!(
    a02_arity_11,
    11,
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
    i128
);
arity_case!(
    a02_arity_12,
    12,
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
    isize
);
arity_case!(
    a02_arity_13,
    13,
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
    bool
);
arity_case!(
    a02_arity_14,
    14,
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
    char
);
arity_case!(
    a02_arity_15,
    15,
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
    f32
);
arity_case!(
    a02_arity_16,
    16,
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
    f64
);

#[test]
fn a02_native_tuple_shape_differs_from_box_data_leaf() {
    async fn boxed_leaf(query: Query<&Box<(u8, u16)>>) -> Result<(), BodyError> {
        let boxed: &Box<(u8, u16)> = query.get();
        let _ = boxed;
        Ok(())
    }

    async fn two_inputs(query: Query<(&u8, &u16)>) -> Result<(), BodyError> {
        let (first, second) = query.get();
        let _ = (first, second);
        Ok(())
    }

    let single = build_definition::<Box<(u8, u16)>, (), _>(|flow, leaf| {
        let () = flow.then_with_output(boxed_leaf, leaf);
        flow.output_with(());
    })
    .expect("Box tuple should be a single Data leaf");
    assert_eq!(single.root().steps()[0].inputs().len(), 1);

    let tuple = build_definition::<(u8, u16), (), _>(|flow, refs| {
        let _: () = flow.then_with_output(two_inputs, refs);
        let () = ();
        flow.output_with(());
    })
    .expect("native tuple should be a two-position shape");
    assert_eq!(tuple.root().steps()[0].inputs().len(), 2);
}

// ---------------------------------------------------------------- A03 Ref 语义

#[test]
fn a03_ref_copy_keeps_identity_and_supports_non_clone_data() {
    let definition = build_definition::<Payload, (u32, u32), _>(|flow, payload| {
        let copy = payload;
        assert_eq!(payload.handle(), copy.handle());
        let first = flow.then_with_output(consume_payload_a, payload);
        let second = flow.then_with_output(consume_payload_b, copy);
        let out_refs = (first, second);
        flow.output_with(out_refs);
    })
    .expect("non-Clone data should build through Copy Refs");
    // Root 输入 1 个位置 + 2 个 Node 输出位置；Ref 复制不分配新位置。
    assert_eq!(definition.positions().len(), 3);
    let steps = definition.root().steps();
    assert_eq!(
        steps[0].inputs()[0].handle.position,
        steps[1].inputs()[0].handle.position
    );
}

#[test]
fn a03_same_type_distinct_inputs_keep_distinct_identity() {
    let definition =
        build_definition::<(Payload, Payload), (Payload, Payload), _>(|_flow, (left, right)| {
            assert_ne!(left.handle(), right.handle());
            let out_refs = (left, right);
            _flow.output_with(out_refs);
        })
        .expect("same-type distinct inputs should stay distinct");
    assert_eq!(definition.positions().len(), 2);
}

/// 由旧 `data_ref::tests::v21_05_data_ref_clone_needs_no_data_clone` 迁移：保留不变量
/// “位置句柄复制不要求业务数据 `Clone`／`Copy`，复制共享同一位置”，改用新链的
/// [`Ref`]（`Copy`，与业务类型无关）与真实 `build_definition` 入口。
#[test]
fn v21_05_data_ref_clone_needs_no_data_clone() {
    let definition = build_definition::<Payload, u32, _>(|flow, payload| {
        let copy = payload; // 复制位置句柄；`Payload` 不是 `Clone`／`Copy`
        assert_eq!(payload.handle(), copy.handle(), "复制共享同一位置");
        let out_refs = flow.then_with_output(consume_payload_a, copy);
        flow.output_with(out_refs);
    })
    .expect("a copied Ref must not require the business type to be Clone");
    // Root 输入 1 个位置 + 1 个 Node 输出位置；句柄复制不分配新位置。
    assert_eq!(definition.positions().len(), 2);
}

// ---------------------------------------------------------------- A04 Node 形式

#[test]
fn a04_all_supported_node_forms_register_without_calling() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);

    struct Counting;

    impl Node for Counting {
        type Input = Amount;
        type Output = Amount;

        async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(Amount(query.get().0))
        }
    }

    /// 非 static 配置的 Struct Node：句柄借用 `'a` 的本地配置。
    struct Configured<'a> {
        step: &'a u32,
    }

    impl Node for Configured<'_> {
        type Input = Amount;
        type Output = Amount;

        async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
            Ok(Amount(query.get().0 + *self.step))
        }
    }

    let step = 1u32;
    let configured = Configured { step: &step };
    assert_eq!(*configured.step, 1);
    let owned = Counting;
    let shared = Arc::new(Counting);
    let async_closure =
        async |query: Query<&Amount>| -> Result<Amount, BodyError> { Ok(Amount(query.get().0)) };
    let function_ref = &add_one;

    let definition = build_definition::<Amount, Amount, _>(|flow, amount| {
        let a = flow.then_with_output(&configured, amount);
        let b = flow.then_with_output(owned, a);
        let c = flow.then_with_output(Arc::clone(&shared), b);
        let d = flow.then_with_output(add_one, c);
        let e = flow.then_with_output(function_ref, d);
        let out_refs = flow.then_with_output(async_closure, e);
        flow.output_with(out_refs);
    })
    .expect("all supported node forms should register");

    assert_eq!(definition.root().steps().len(), 6);
    assert_eq!(CALLS.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------- A05 Query 映射

#[test]
fn a05_query_maps_declarations_to_leaf_borrows() {
    let amount = Amount(7);
    let doubled = Doubled(11);

    let query = Query::<&(Amount, Doubled)>::from_view((&amount, &doubled));
    let (left, right): (&Amount, &Doubled) = query.get();
    assert_eq!(left.0, 7);
    assert_eq!(right.0, 11);

    let query = Query::<&Amount>::from_view(&amount);
    let single: &Amount = query.get();
    assert_eq!(single.0, 7);

    let query = Query::<()>::from_view(());
    let _unit: () = query.get();

    let query = Query::<(&Amount, &Doubled)>::from_view((&amount, &doubled));
    let (left, right) = query.get();
    assert_eq!((left.0, right.0), (7, 11));
}

// ---------------------------------------------------------------- A06 顺序与 unit

#[test]
fn a06_order_unit_steps_and_unused_outputs_are_kept() {
    let definition = build_definition::<Amount, (), _>(|flow, amount| {
        let doubled = flow.then_with_output(double, amount);
        let _unit: () = flow.then_with_output(sink_doubled, doubled);
        let unused = flow.then_with_output(double, amount);
        let _ = unused;
        let () = ();
        flow.output_with(());
    })
    .expect("unit and unused steps should build");

    let steps = definition.root().steps();
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0].kind(), StepKind::Node);
    assert_eq!(steps[0].outputs().len(), 1);
    // Step 持有真实 callable：诊断名来自具体 Node 类型，不是空标记。
    assert!(steps[0].callable_name().is_some());
    assert_eq!(steps[1].kind(), StepKind::Node);
    assert_eq!(steps[1].outputs().len(), 0);
    assert_eq!(steps[2].outputs().len(), 1);
    // 顺序按定义登记：第 2 个 Step 的输入指向第 1 个 Step 的输出位置。
    assert_eq!(steps[1].inputs()[0].handle.position, steps[0].outputs()[0]);
    // Root 显式 unit：已完成、零输出。
    assert!(definition.root().output().is_empty());
}

#[test]
fn a06_explicit_unit_children_are_complete() {
    let definition = build_definition::<Amount, (), _>(|flow, amount| {
        let () = flow.chain(|sub| {
            let produced = sub.then_with_output(double, amount);
            let _: () = sub.then_with_output(sink_doubled, produced);
            ()
        });
        flow.output_with(());
    })
    .expect("explicit unit child should be a complete body");

    let root = definition.root();
    assert_eq!(root.steps().len(), 1);
    let child = definition.body(root.steps()[0].chain().unwrap().child());
    assert!(child.output().is_empty());
    assert_eq!(child.steps().len(), 2);
}

// ---------------------------------------------------------------- A07 Chain 与 Fragment

#[test]
fn a07_chain_builds_child_body_and_parent_refs() {
    let definition = build_definition::<Amount, Doubled, _>(|flow, amount| {
        let out_refs = flow.chain(|sub| sub.then_with_output(double, amount));
        flow.output_with(out_refs);
    })
    .expect("chain should build");

    let root = definition.root();
    assert_eq!(root.steps().len(), 1);
    assert_eq!(root.steps()[0].kind(), StepKind::Chain);
    let chain = root.steps()[0].chain().expect("chain step view");
    let child = definition.body(chain.child());
    assert_eq!(child.parent(), Some(root.id()));
    assert_eq!(child.steps().len(), 1);
    assert_eq!(child.output().len(), 1);
    assert_eq!(root.steps()[0].outputs().len(), 1);
    // parent 的 Ref 是新的逻辑位置，不是 child 内部位置。
    assert_ne!(
        child.output()[0].handle.position,
        root.steps()[0].outputs()[0]
    );
    assert_eq!(chain.exits().len(), 1);
}

#[test]
fn a07_fragment_forms_build_with_non_static_nodes() {
    let node = AddOne;
    let definition = build_definition::<Amount, Amount, _>(|flow, amount| {
        let first = flow.chain(fragment(&node, amount));
        let out_refs = flow.chain(|sub| call_in_closure(sub, &node, first));
        flow.output_with(out_refs);
    })
    .expect("both fragment forms should build");

    assert_eq!(definition.bodies().len(), 3);
    let root = definition.root();
    assert_eq!(root.steps().len(), 2);
    let first_body = definition.body(root.steps()[0].chain().unwrap().child());
    let second_body = definition.body(root.steps()[1].chain().unwrap().child());
    assert_eq!(first_body.parent(), Some(root.id()));
    assert_eq!(second_body.parent(), Some(root.id()));
    assert_eq!(first_body.steps().len(), 1);
    assert_eq!(second_body.steps().len(), 1);
    assert!(first_body.steps()[0].chain().is_none());
}

// ---------------------------------------------------------------- A08 合法来源

#[test]
fn a08_root_input_and_node_output_refs_are_accepted() {
    let definition = build_definition::<Amount, Doubled, _>(|flow, amount| {
        let produced = flow.then_with_output(AddOne, amount);
        let out_refs = flow.then_with_output(double, produced);
        flow.output_with(out_refs);
    })
    .expect("legal root input and node output refs should be accepted");

    let steps = definition.root().steps();
    assert_eq!(steps.len(), 2);
    assert_eq!(
        steps[0].inputs()[0].handle.position,
        definition.positions()[0].handle().position
    );
    assert_eq!(steps[1].inputs()[0].handle.position, steps[0].outputs()[0]);
}

// ---------------------------------------------------------------- A09 拒绝类别

#[test]
fn a09_foreign_definition_ref_is_rejected() {
    let mut leaked: Option<Ref<Amount>> = None;
    let _first = build_definition::<Amount, Amount, _>(|_flow, amount| {
        leaked = Some(amount);
        let out_refs = amount;
        _flow.output_with(out_refs);
    })
    .expect("first definition should build");

    let foreign = leaked.expect("captured root ref");
    let error = build_definition::<Amount, Amount, _>(|flow, amount| {
        let _ = flow.then_with_output(AddOne, foreign);
        let out_refs = amount;
        flow.output_with(out_refs);
    })
    .err()
    .expect("foreign definition ref must be rejected");
    assert!(
        matches!(error, BuildErrorDetail::ForeignDefinition { .. }),
        "{error}"
    );
}

#[test]
fn a09_child_local_leak_is_rejected() {
    let mut leaked: Option<Ref<Doubled>> = None;
    let error = build_definition::<Amount, (), _>(|flow, amount| {
        let _kept = flow.chain(|sub| {
            let produced = sub.then_with_output(double, amount);
            leaked = Some(produced);
            produced
        });
        let child_local = leaked.expect("leaked child ref");
        let _ = flow.then_with_output(sink_doubled, child_local);
        let () = ();
        flow.output_with(());
    })
    .err()
    .expect("child-local ref used in the parent must be rejected");
    assert!(
        matches!(
            error,
            BuildErrorDetail::OutOfScope {
                relation: Visibility::ChildLocal,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a09_sibling_internal_ref_is_rejected() {
    let mut leaked: Option<Ref<Doubled>> = None;
    let error = build_definition::<Amount, (), _>(|flow, amount| {
        let _first = flow.chain(|sub| {
            let produced = sub.then_with_output(double, amount);
            leaked = Some(produced);
            produced
        });
        let sibling_local = leaked.expect("leaked first-child ref");
        let () = flow.chain(move |sub| sub.then_with_output(sink_doubled, sibling_local));
        flow.output_with(());
    })
    .err()
    .expect("sibling internal ref must be rejected");
    assert!(
        matches!(
            error,
            BuildErrorDetail::OutOfScope {
                relation: Visibility::Sibling,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a09_chain_output_is_usable_in_parent() {
    let definition = build_definition::<Amount, Doubled, _>(|flow, amount| {
        let checked = flow.chain(|sub| sub.then_with_output(AddOne, amount));
        let out_refs = flow.then_with_output(double, checked);
        flow.output_with(out_refs);
    })
    .expect("chain output should be a legal parent-visible ref");
    assert_eq!(definition.root().steps().len(), 2);
}

// ---------------------------------------------------------------- A10／A11 来源防线

#[test]
fn a10_position_without_registered_source_is_rejected() {
    let error = build_definition::<Amount, Amount, _>(|flow, amount| {
        let definition = flow.definition_id();
        let forged = Ref::<Amount>::from_handle(PositionHandle {
            definition,
            position: PositionId::new(9_999),
        });
        let _ = flow.then_with_output(AddOne, forged);
        let out_refs = amount;
        flow.output_with(out_refs);
    })
    .err()
    .expect("unregistered position must be rejected");
    assert!(
        matches!(error, BuildErrorDetail::UnknownPosition { .. }),
        "{error}"
    );
}

#[test]
fn a11_registered_source_survives_handle_reconstruction() {
    let definition = build_definition::<Amount, Doubled, _>(|flow, amount| {
        let produced = flow.then_with_output(double, amount);
        let reconstructed = Ref::<Doubled>::from_handle(produced.handle());
        let _: () = flow.then_with_output(sink_doubled, reconstructed);
        let out_refs = produced;
        flow.output_with(out_refs);
    })
    .expect("a handle to a registered position keeps its legal source");
    assert_eq!(definition.root().steps().len(), 2);
}

// ---------------------------------------------------------------- A12 词法依赖

#[test]
fn a12_deep_capture_records_every_intermediate_import() {
    let mut root_position: Option<PositionId> = None;
    let definition = build_definition::<Amount, Doubled, _>(|flow, amount| {
        root_position = Some(amount.handle().position);
        let out_refs =
            flow.chain(|outer| outer.chain(|inner| inner.then_with_output(double, amount)));
        flow.output_with(out_refs);
    })
    .expect("deep ancestor capture should build");

    let root = definition.root();
    assert!(root.imports().is_empty());
    let outer = definition.body(root.steps()[0].chain().unwrap().child());
    let inner = definition.body(outer.steps()[0].chain().unwrap().child());
    let root_position = root_position.expect("captured root position");
    let record = definition
        .record(PositionHandle {
            definition: definition.id(),
            position: root_position,
        })
        .expect("root input record");
    assert_eq!(record.owner(), definition.root().id());
    assert!(outer.imports().contains(&root_position));
    assert!(inner.imports().contains(&root_position));
}

#[test]
fn a12_forward_position_is_rejected() {
    let error = build_definition::<Amount, Amount, _>(|flow, amount| {
        let definition = flow.definition_id();
        let future = flow.next_position_id();
        let forged = Ref::<Amount>::from_handle(PositionHandle {
            definition,
            position: future,
        });
        let _ = flow.then_with_output(AddOne, forged);
        let out_refs = amount;
        flow.output_with(out_refs);
    })
    .err()
    .expect("a position allocated after its use must be rejected");
    assert!(
        matches!(error, BuildErrorDetail::NotYetAvailable { .. }),
        "{error}"
    );
}

// ---------------------------------------------------------------- A13 输出边界

#[test]
fn a13_root_duplicate_ref_is_rejected() {
    let error = build_definition::<Amount, (Amount, Amount), _>(|_flow, amount| {
        let out_refs = (amount, amount);
        _flow.output_with(out_refs);
    })
    .err()
    .expect("root direct duplicate must be rejected");
    assert!(
        matches!(error, BuildErrorDetail::DuplicateRootOutput { .. }),
        "{error}"
    );
}

#[test]
fn a13_chain_duplicate_source_is_canonicalized_to_one_exit() {
    let definition = build_definition::<Amount, (), _>(|flow, amount| {
        let pair = flow.chain(|sub| {
            let produced = sub.then_with_output(double, amount);
            (produced, produced)
        });
        let _: (Ref<Doubled>, Ref<Doubled>) = pair;
        let () = ();
        flow.output_with(());
    })
    .expect("chain duplicate source should be accepted and canonicalized");

    let root = definition.root();
    let chain = root.steps()[0].chain().expect("chain step");
    assert_eq!(chain.exits().len(), 1);
    assert_eq!(
        chain.bindings(),
        &[ChainBinding::ChildExit(0), ChainBinding::ChildExit(0)]
    );
    let outputs = root.steps()[0].outputs();
    assert_eq!(outputs.len(), 2);
    assert_ne!(outputs[0], outputs[1]);
    assert!(root.output().is_empty());
}

#[test]
fn a13_root_returning_chain_refs_passes_static_checks() {
    let definition = build_definition::<Amount, (Doubled, Doubled), _>(|flow, amount| {
        let out_refs = flow.chain(|sub| {
            let produced = sub.then_with_output(double, amount);
            (produced, produced)
        });
        flow.output_with(out_refs);
    })
    .expect("two distinct parent refs from one chain exit are statically legal");
    let output = definition.root().output();
    assert_eq!(output.len(), 2);
    assert_ne!(output[0].handle.position, output[1].handle.position);
}

#[test]
fn a13_node_repeated_readonly_inputs_are_legal() {
    let definition = build_definition::<Amount, Doubled, _>(|flow, amount| {
        let out_refs = flow.then_with_output(pair_reader, (amount, amount));
        flow.output_with(out_refs);
    })
    .expect("repeated read-only node inputs should be legal");
    let step = &definition.root().steps()[0];
    assert_eq!(step.inputs().len(), 2);
    assert_eq!(
        step.inputs()[0].handle.position,
        step.inputs()[1].handle.position
    );
}

#[test]
fn a13_chain_alias_output_imports_ancestor_without_transfer() {
    let definition = build_definition::<Amount, Amount, _>(|flow, amount| {
        let out_refs = flow.chain(|_sub| amount);
        flow.output_with(out_refs);
    })
    .expect("chain alias output should be legal");

    let root = definition.root();
    let chain = root.steps()[0].chain().expect("chain step");
    assert!(chain.exits().is_empty());
    assert!(matches!(
        chain.bindings()[0],
        ChainBinding::AncestorAlias(_)
    ));
    let child = definition.body(chain.child());
    let imported = child.imports()[0];
    // parent 输出是新的逻辑位置，不是被 alias 的祖先位置。
    assert_eq!(
        root.output()[0].handle.position,
        root.steps()[0].outputs()[0]
    );
    assert_ne!(root.output()[0].handle.position, imported);
}

// ---------------------------------------------------------------- A14／A15 构建边界

#[test]
fn a14_failing_build_never_calls_nodes() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);

    struct Counting;

    impl Node for Counting {
        type Input = Amount;
        type Output = Doubled;

        async fn run(&self, _query: Query<&Self::Input>) -> Result<Doubled, BodyError> {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(Doubled(0))
        }
    }

    let error = build_definition::<Amount, (Amount, Amount), _>(|flow, amount| {
        let produced = flow.then_with_output(Counting, amount);
        let _ = produced;
        let out_refs = (amount, amount);
        flow.output_with(out_refs);
    })
    .err()
    .expect("duplicate root output should fail the build");
    assert!(
        matches!(error, BuildErrorDetail::DuplicateRootOutput { .. }),
        "{error}"
    );
    assert_eq!(CALLS.load(Ordering::SeqCst), 0);
}

#[test]
fn a15_definition_is_built_from_one_body_authority() {
    let mut root_position: Option<PositionId> = None;
    let definition = build_definition::<Amount, Amount, _>(|flow, amount| {
        root_position = Some(amount.handle().position);
        let first = flow.chain(|sub| sub.then_with_output(AddOne, amount));
        let out_refs = flow.chain(|sub| sub.then_with_output(AddOne, first));
        flow.output_with(out_refs);
    })
    .expect("nested chain bodies should build");

    // Body 编号即下标；父子关系由 Body 自身持有，不存在第二棵权威 Scope 树。
    assert_eq!(definition.bodies().len(), 3);
    let root = definition.root();
    let root_position = root_position.expect("captured root position");
    let first_body = definition.body(root.steps()[0].chain().unwrap().child());
    let second_body = definition.body(root.steps()[1].chain().unwrap().child());
    assert_eq!(first_body.parent(), Some(root.id()));
    assert_eq!(second_body.parent(), Some(root.id()));
    assert_eq!(first_body.imports(), &[root_position]);
    assert_eq!(second_body.imports(), &[root.steps()[0].outputs()[0]]);
}

// ================================================================ WE-T03 Choose

/// 只实现 `Data + PartialEq` 的匹配键（与公开样本一致的最小约束）。
struct Key(u8);

impl Data for Key {}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

async fn key_from_amount(query: Query<&Amount>) -> Result<Key, BodyError> {
    Ok(Key(query.get().0 as u8))
}

#[test]
fn t03_choose_is_one_step_with_shared_parent_slots_and_per_branch_exits() {
    let definition = build_definition::<Amount, Amount, _>(|flow, amount| {
        let selector = flow.then_with_output(key_from_amount, amount);
        let out_refs = flow.choose(selector, |choice| {
            choice.case(Key(1), |branch| branch.then_with_output(add_one, amount));
            choice.case(Key(2), |branch| branch.then_with_output(add_one, amount));
            choice.otherwise(|branch| branch.then_with_output(add_one, amount));
        });
        flow.output_with(out_refs);
    })
    .expect("a valid choose definition builds");

    // 唯一 Definition：selector Node + 单个 Choose Step（不是每个分支一个 Chain Step）。
    let root = definition.root();
    let kinds: Vec<StepKind> = root.steps().iter().map(|step| step.kind()).collect();
    assert_eq!(
        kinds,
        vec![StepKind::Node, StepKind::Choose],
        "branches are bodies of one ordered step, not separate ordered steps"
    );
    let choose = &root.steps()[1];
    assert_eq!(choose.inputs().len(), 1, "the selector is the choose input");
    assert_eq!(choose.outputs().len(), 1, "one shared parent output slot");

    // 分支 Body：三个直接 child，各自声明同一个输出形状。
    assert_eq!(
        definition.bodies().len(),
        4,
        "root plus three branch bodies"
    );
    for body in definition.bodies().iter().skip(1) {
        assert_eq!(body.parent(), Some(root.id()));
        assert_eq!(
            body.output().len(),
            1,
            "every branch declares the output shape"
        );
    }

    // 固定槽位：一个共享 ChooseOutput，与每分支各一个 ChooseExit。
    let outputs = definition
        .positions()
        .iter()
        .filter(|record| matches!(record.source(), PositionSource::ChooseOutput { .. }))
        .count();
    let exits = definition
        .positions()
        .iter()
        .filter(|record| matches!(record.source(), PositionSource::ChooseExit { .. }))
        .count();
    assert_eq!(
        outputs, 1,
        "one fixed logical parent slot per output position"
    );
    assert_eq!(exits, 3, "each branch keeps its own declared exit");
}

#[test]
fn t03_unselected_branch_forward_source_is_rejected() {
    let error = build_definition::<Amount, Amount, _>(|flow, amount| {
        let selector = flow.then_with_output(key_from_amount, amount);
        let out_refs = flow.choose(selector, |choice| {
            // 第一个分支合法；第二个分支（将来不会被选中）使用前向位置。
            choice.case(Key(1), |branch| branch.then_with_output(add_one, amount));
            choice.case(Key(2), |branch| {
                let definition = branch.definition_id();
                let future = branch.next_position_id();
                let forged = Ref::<Amount>::from_handle(PositionHandle {
                    definition,
                    position: future,
                });
                branch.then_with_output(add_one, forged)
            });
        });
        flow.output_with(out_refs);
    })
    .err()
    .expect("an unselected branch with a forward source must be rejected");
    assert!(
        matches!(error, BuildErrorDetail::NotYetAvailable { .. }),
        "{error}"
    );
}
