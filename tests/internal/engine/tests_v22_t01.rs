//! V22-T01 定向测试：来源解析、声明元数据、发布/失效与旧契约衔接。
//!
//! 测试只做 Definition 构建与结构检查，不执行任何业务 Node；A15 通过正式 Runtime
//! 验证构建拒绝与零 Node 执行。受控 Builder 状态用于表达尚无公开新 API 的声明组合。
#![allow(clippy::err_expect, clippy::let_unit_value, clippy::unused_unit)]

use super::definition::{
    BodyRole, Builder, DeclarationKind, DeclarationState, PositionSource, build_definition,
};
use super::error::{BodyError, BuildErrorDetail, RunError};
use super::flow::Flow;
use super::identity::{BodyId, DefinitionId, PositionHandle, PositionId};
use super::runtime::Runtime;
use super::shape::{PositionDesc, Query, Ref, StateShape, TypeDesc};
use futures::executor::block_on;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ---------------------------------------------------------------- 业务类型与 Node

#[derive(Debug, PartialEq, Eq)]
struct Amount(u32);
impl super::data::Data for Amount {}

#[derive(Debug, PartialEq, Eq)]
struct Doubled(u32);
impl super::data::Data for Doubled {}

#[derive(Debug, PartialEq, Eq)]
struct Key(u32);
impl super::data::Data for Key {}

struct Missing;
impl super::data::Data for Missing {}

async fn add_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

async fn sink_amount(query: Query<&Amount>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

async fn sink_missing(query: Query<&Missing>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

async fn load_missing(query: Query<()>) -> Result<Missing, BodyError> {
    let _ = query.get();
    Ok(Missing)
}

static EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

async fn counting_load(query: Query<()>) -> Result<Amount, BodyError> {
    let _ = query.get();
    EXECUTIONS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(0))
}

// ---------------------------------------------------------------- 受控 Builder 辅助

fn builder_with_inputs(
    types: &[TypeDesc],
) -> (Rc<RefCell<Builder<'static>>>, BodyId, Vec<PositionHandle>) {
    let builder = Rc::new(RefCell::new(Builder::new()));
    let root = builder.borrow_mut().new_body(None, BodyRole::Ordinary);
    let mut handles = Vec::with_capacity(types.len());
    for (slot, type_desc) in types.iter().enumerate() {
        let handle = builder.borrow_mut().allocate(
            root,
            PositionSource::RootInput { slot: slot as u16 },
            *type_desc,
        );
        handles.push(handle);
    }
    (builder, root, handles)
}

fn finish_builder(
    builder: Rc<RefCell<Builder<'static>>>,
    root: BodyId,
) -> Result<super::definition::Definition<'static>, BuildErrorDetail> {
    let state = Rc::try_unwrap(builder)
        .ok()
        .expect("test does not retain the internal Flow")
        .into_inner();
    state.finish(root)
}

fn desc(handle: PositionHandle, type_desc: TypeDesc) -> super::shape::PositionDesc {
    super::shape::PositionDesc { handle, type_desc }
}

fn amount(handle: PositionHandle) -> Ref<Amount> {
    Ref::from_handle(handle)
}

fn doubled(handle: PositionHandle) -> Ref<Doubled> {
    Ref::from_handle(handle)
}

// ---------------------------------------------------------------- A01 最近来源与冻结

#[test]
fn a01_nearest_root_and_node_sources_are_frozen() {
    let definition = build_definition::<(Amount, Amount), Amount, _>(|flow, (first, second)| {
        assert_ne!(first.handle(), second.handle());
        let nearest = flow.ref_of::<Amount>();
        assert_eq!(nearest.handle(), second.handle());
        let produced = flow.then_with_output(add_one, first);
        let after = flow.ref_of::<Amount>();
        assert_eq!(after.handle(), produced.handle());
        assert_ne!(nearest.handle(), after.handle());
        let out_refs = second;
        flow.output_with(out_refs);
    })
    .expect("same-type root inputs and later node source should build");
    assert_eq!(definition.root().steps().len(), 1);
}

// ---------------------------------------------------------------- A02 可见性与遮蔽

#[test]
fn a02_ancestor_shadowing_and_chain_output_visibility() {
    let definition = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        let root = flow.ref_of::<Amount>();
        flow.chain(|sub| {
            let ancestor = sub.ref_of::<Amount>();
            assert_eq!(ancestor.handle(), amount_ref.handle());
            let local = sub.then_with_output(add_one, amount_ref);
            let nearest = sub.ref_of::<Amount>();
            assert_eq!(nearest.handle(), local.handle());
            let out_refs = local;
            sub.output_with(out_refs);
        });
        let after = flow.ref_of::<Amount>();
        assert_ne!(after.handle(), root.handle());
        let out_refs = after;
        flow.output_with(out_refs);
    })
    .expect("ancestor query, local shadowing and chain output should build");
    assert_eq!(definition.root().steps().len(), 1);
}

// ---------------------------------------------------------------- A03a Choose 发布时序

#[test]
fn a03a_preallocated_choose_output_is_unpublished_in_branch() {
    let (builder, root, inputs) =
        builder_with_inputs(&[TypeDesc::of::<Key>(), TypeDesc::of::<Amount>()]);
    let key = inputs[0];
    let amount_source = inputs[1];
    let step = builder.borrow().step_count(root);
    // 正式 Flow::choose 在分配输出和构建分支之前取得 Step.order；这里必须复现同一时序。
    let step_order = builder.borrow_mut().fresh_order();
    let outputs =
        builder
            .borrow_mut()
            .allocate_choose_outputs(root, step, &[TypeDesc::of::<Amount>()]);
    let branch = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    let query_order = {
        let order = builder.borrow().current_order();
        let flow = Flow::open(Rc::clone(&builder), branch);
        let found = flow.ref_of::<Amount>();
        assert_eq!(
            found.handle(),
            amount_source,
            "only the published ancestor may be selected during branch construction"
        );
        order
    };
    assert!(query_order > step_order);
    {
        let mut flow = Flow::open(Rc::clone(&builder), branch);
        flow.output_with(amount(amount_source));
    }
    let branch = builder
        .borrow_mut()
        .register_choose_branch(branch, 0, 0, &outputs);
    let selector = desc(key, TypeDesc::of::<Key>());
    {
        let mut builder = builder.borrow_mut();
        builder.push_choose_step(
            root,
            super::definition::ChooseStep::new(
                selector,
                vec![super::definition::ChooseCase {
                    key: Key(1),
                    branch,
                }],
                None,
                outputs.iter().map(|handle| handle.position).collect(),
                step_order,
            ),
            true,
        );
    }
    let published = builder
        .borrow()
        .position(outputs[0])
        .expect("choose output record")
        .published_order()
        .expect("published after push");
    assert!(
        published > query_order && published > step_order,
        "publication must use a fresh order, not the pre-branch Step.order"
    );
    {
        let flow = Flow::open(Rc::clone(&builder), root);
        let found = flow.ref_of::<Amount>();
        assert_eq!(
            found.handle().position,
            outputs[0].position,
            "after push the parent sees the new Choose output"
        );
    }
    finish_builder(builder, root).expect("published choose output should build");
}

// ---------------------------------------------------------------- A03b 类别与遮蔽

#[test]
fn a03b_newer_retry_and_iter_bridges_are_excluded_from_queries() {
    let retry_definition = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        let retried: Ref<Amount> = flow.retry_output(1, |sub| {
            let out_refs = sub.then_with_output(add_one, amount_ref);
            sub.output_with(out_refs);
        });
        let found = flow.ref_of::<Amount>();
        assert_eq!(found.handle(), retried.handle());
        let out_refs = retried;
        flow.output_with(out_refs);
    })
    .expect("retry bridge must not shadow the retry output");
    assert_eq!(retry_definition.root().steps().len(), 1);

    let iter_definition = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        let iterated = flow.iter_output(amount_ref, 1, |round| {
            let current = round.current::<Amount>();
            let next = round.then_with_output(add_one, current);
            round.next_with(next);
        });
        let found = flow.ref_of::<Amount>();
        assert_eq!(found.handle(), iterated.handle());
        let out_refs = iterated;
        flow.output_with(out_refs);
    })
    .expect("iter bridge must not shadow the iter output");
    assert_eq!(iter_definition.root().steps().len(), 1);
}

// ---------------------------------------------------------------- A04 Item/Current

#[test]
fn a04_item_and_current_are_visible_and_shadowed() {
    let _ = build_definition::<Vec<Amount>, Vec<Amount>, _>(|flow, items| {
        let out_refs: Ref<Vec<Amount>> = flow.each_output(items, |sub| {
            let item = sub.ref_of::<Amount>();

            let found = sub.ref_of::<Amount>();
            assert_eq!(found.handle(), item.handle());
            let advanced = sub.then_with_output(add_one, item);
            let nearest = sub.ref_of::<Amount>();
            assert_eq!(nearest.handle(), advanced.handle());
            let out_refs = advanced;
            sub.output_with(out_refs);
        });
        flow.output_with(out_refs);
    })
    .expect("item query, shadowing and collected output should build");

    let _ = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        let out_refs = flow.iter_output(amount_ref, 1, |round| {
            let current = round.current::<Amount>();
            let saved = round.ref_of::<Amount>();
            assert_eq!(saved.handle(), current.handle());
            let advanced = round.then_with_output(add_one, current);
            let nearest = round.ref_of::<Amount>();
            assert_eq!(nearest.handle(), advanced.handle());
            let _ = round.then_with_output(sink_amount, saved);
            round.next_with(advanced);
        });
        flow.output_with(out_refs);
    })
    .expect("current query, saved identity and shadowing should build");
}

// ---------------------------------------------------------------- A05 Import

#[test]
fn a05_unused_ref_of_adds_nothing() {
    let definition = build_definition::<Amount, (), _>(|flow, _amount_ref| {
        let _ = flow.ref_of::<Amount>();
        let () = ();
        flow.output_with(());
    })
    .expect("unused successful query should build");
    assert_eq!(definition.root().steps().len(), 0);
    assert_eq!(definition.positions().len(), 1);
    assert!(definition.root().imports().is_empty());
}

// ---------------------------------------------------------------- A06 声明状态与重复

// ---------------------------------------------------------------- A07 固定时点与显式校验

#[test]
fn a07_output_is_frozen_at_declaration_and_preserves_slots() {
    let (builder, root, inputs) =
        builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Doubled>()]);
    let amount_source = inputs[0];
    let _doubled_source = inputs[1];
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output_with(amount(amount_source));
        let _ = flow.then_with_output(add_one, amount(amount_source));
    }
    let definition = finish_builder(builder, root).expect("frozen output");
    let declaration = definition.root().output_declaration();
    assert_eq!(declaration.slots.len(), 1);
    assert_eq!(declaration.slots[0].handle.position, amount_source.position);
    assert_eq!(definition.root().steps().len(), 1);

    let (builder, root, inputs) =
        builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Doubled>()]);
    let amount_source = inputs[0];
    let doubled_source = inputs[1];
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output_with((doubled(doubled_source), amount(amount_source)));
    }
    let definition = finish_builder(builder, root).expect("explicit slot order");
    let slots = &definition.root().output_declaration().slots;
    assert_eq!(slots[0].handle.position, doubled_source.position);
    assert_eq!(slots[1].handle.position, amount_source.position);
    assert_eq!(slots[0].type_desc, TypeDesc::of::<Doubled>());
    assert_eq!(slots[1].type_desc, TypeDesc::of::<Amount>());
}

#[test]
fn a07_explicit_output_rejects_each_invalid_source() {
    let foreign = {
        let (builder, root, _) = builder_with_inputs(&[]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            let handle = PositionHandle {
                definition: DefinitionId::fresh(),
                position: PositionId::new(0),
            };
            flow.output_with(amount(handle));
        }
        finish_builder(builder, root).err().expect("foreign source")
    };
    assert!(
        matches!(foreign, BuildErrorDetail::ForeignDefinition { .. }),
        "{foreign}"
    );

    let unknown = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let definition = builder.borrow().definition_id();
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output_with(amount(PositionHandle {
                definition,
                position: PositionId::new(9_999),
            }));
        }
        finish_builder(builder, root).err().expect("unknown source")
    };
    assert!(
        matches!(unknown, BuildErrorDetail::UnknownPosition { .. }),
        "{unknown}"
    );

    let future = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let definition = builder.borrow().definition_id();
        let position = builder.borrow().next_position_id();
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output_with(amount(PositionHandle {
                definition,
                position,
            }));
        }
        finish_builder(builder, root).err().expect("future source")
    };
    assert!(
        matches!(future, BuildErrorDetail::NotYetAvailable { .. }),
        "{future}"
    );

    let out_of_scope = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let item = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let item_position =
            builder
                .borrow_mut()
                .allocate_each_item(item, 0, TypeDesc::of::<Amount>());
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output_with(amount(item_position));
        }
        finish_builder(builder, root)
            .err()
            .expect("child-local source")
    };
    assert!(
        matches!(out_of_scope, BuildErrorDetail::OutOfScope { .. }),
        "{out_of_scope}"
    );

    let type_mismatch = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Doubled>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output_with(amount(inputs[0]));
        }
        finish_builder(builder, root).err().expect("type mismatch")
    };
    assert!(
        matches!(type_mismatch, BuildErrorDetail::TypeMismatch { .. }),
        "{type_mismatch}"
    );
}

// ---------------------------------------------------------------- A08 Shape

#[test]
fn a08_zero_single_pair_and_sixteen_slots() {
    let (builder, root, _) = builder_with_inputs(&[]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output::<()>();
    }
    let definition = finish_builder(builder, root).expect("zero slots");
    assert!(definition.root().output().is_empty());

    let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output_with(amount(inputs[0]));
    }
    let definition = finish_builder(builder, root).expect("one slot");
    assert_eq!(definition.root().output().len(), 1);

    let (builder, root, inputs) =
        builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Doubled>()]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output_with((amount(inputs[0]), doubled(inputs[1])));
    }
    let definition = finish_builder(builder, root).expect("two slots");
    assert_eq!(definition.root().output().len(), 2);

    let types = [
        TypeDesc::of::<u8>(),
        TypeDesc::of::<u16>(),
        TypeDesc::of::<u32>(),
        TypeDesc::of::<u64>(),
        TypeDesc::of::<u128>(),
        TypeDesc::of::<usize>(),
        TypeDesc::of::<i8>(),
        TypeDesc::of::<i16>(),
        TypeDesc::of::<i32>(),
        TypeDesc::of::<i64>(),
        TypeDesc::of::<i128>(),
        TypeDesc::of::<isize>(),
        TypeDesc::of::<bool>(),
        TypeDesc::of::<char>(),
        TypeDesc::of::<f32>(),
        TypeDesc::of::<f64>(),
    ];
    let (builder, root, inputs) = builder_with_inputs(&types);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output_with((
            Ref::<u8>::from_handle(inputs[0]),
            Ref::<u16>::from_handle(inputs[1]),
            Ref::<u32>::from_handle(inputs[2]),
            Ref::<u64>::from_handle(inputs[3]),
            Ref::<u128>::from_handle(inputs[4]),
            Ref::<usize>::from_handle(inputs[5]),
            Ref::<i8>::from_handle(inputs[6]),
            Ref::<i16>::from_handle(inputs[7]),
            Ref::<i32>::from_handle(inputs[8]),
            Ref::<i64>::from_handle(inputs[9]),
            Ref::<i128>::from_handle(inputs[10]),
            Ref::<isize>::from_handle(inputs[11]),
            Ref::<bool>::from_handle(inputs[12]),
            Ref::<char>::from_handle(inputs[13]),
            Ref::<f32>::from_handle(inputs[14]),
            Ref::<f64>::from_handle(inputs[15]),
        ));
    }
    let definition = finish_builder(builder, root).expect("sixteen slots");
    assert_eq!(definition.root().output().len(), 16);
}

#[test]
fn a08_same_type_different_refs_and_root_duplicate() {
    let (builder, root, inputs) =
        builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Amount>()]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output_with((amount(inputs[0]), amount(inputs[1])));
    }
    let definition = finish_builder(builder, root).expect("distinct same-type refs");
    assert_eq!(definition.root().output().len(), 2);
    assert_ne!(
        definition.root().output()[0].handle.position,
        definition.root().output()[1].handle.position
    );

    let (builder, root, _) =
        builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Amount>()]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output::<(Amount, Amount)>();
    }
    let error = finish_builder(builder, root)
        .err()
        .expect("automatic duplicate slots still duplicate the root ref");
    assert!(
        matches!(error, BuildErrorDetail::DuplicateRootOutput { .. }),
        "{error}"
    );

    let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output_with((amount(inputs[0]), amount(inputs[0])));
    }
    let error = finish_builder(builder, root)
        .err()
        .expect("explicit duplicate root ref must be rejected");
    assert!(
        matches!(error, BuildErrorDetail::DuplicateRootOutput { .. }),
        "{error}"
    );
}

// ---------------------------------------------------------------- A09 Root 期望 Shape

// ---------------------------------------------------------------- A10 Choose 契约

fn declare_ok(
    builder: &Rc<RefCell<Builder<'static>>>,
    body: BodyId,
    slots: Vec<super::shape::PositionDesc>,
) {
    let order = builder.borrow_mut().fresh_order();
    assert!(
        builder.borrow_mut().declare_output(body, slots, order),
        "test declaration should succeed"
    );
}

// ---------------------------------------------------------------- A11 Iter Next

#[test]
fn a11_next_declaration_states_and_shapes() {
    let normal = {
        let (builder, root, inputs) =
            builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Doubled>()]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder.borrow_mut().set_round_initial(
            round,
            vec![TypeDesc::of::<Amount>(), TypeDesc::of::<Doubled>()],
        );
        {
            let mut flow = Flow::open(Rc::clone(&builder), round);
            flow.next_with((amount(inputs[0]), doubled(inputs[1])));
        }
        finish_builder(builder, root).expect("two-slot next")
    };
    let round = normal
        .bodies()
        .iter()
        .find(|body| body.role() == BodyRole::Round)
        .expect("round body");
    assert_eq!(round.next_declaration().state(), DeclarationState::NonUnit);
    assert_eq!(round.next_declaration().slots.len(), 2);
    assert_eq!(
        round.output_declaration().state(),
        DeclarationState::Undeclared
    );

    let missing = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let _round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            missing,
            Some(BuildErrorDetail::MissingDeclaration {
                kind: DeclarationKind::Next,
                ..
            })
        ),
        "{missing:?}"
    );

    let duplicate = {
        let (builder, root, _) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), round);
            flow.next::<Amount>();
            flow.next::<Amount>();
        }
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            duplicate,
            Some(BuildErrorDetail::DuplicateDeclaration { .. })
        ),
        "{duplicate:?}"
    );

    let unit = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        let order = builder.borrow_mut().fresh_order();
        builder.borrow_mut().declare_next(round, Vec::new(), order);
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(unit, Some(BuildErrorDetail::NextUnitUnsupported { .. })),
        "{unit:?}"
    );

    let arity = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        let order = builder.borrow_mut().fresh_order();
        builder.borrow_mut().declare_next(
            round,
            vec![
                desc(inputs[0], TypeDesc::of::<Amount>()),
                desc(inputs[0], TypeDesc::of::<Amount>()),
            ],
            order,
        );
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            arity,
            Some(BuildErrorDetail::DeclarationArityMismatch { .. })
        ),
        "{arity:?}"
    );

    let shape = {
        let (builder, root, inputs) =
            builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Doubled>()]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        let order = builder.borrow_mut().fresh_order();
        builder.borrow_mut().declare_next(
            round,
            vec![desc(inputs[1], TypeDesc::of::<Doubled>())],
            order,
        );
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            shape,
            Some(BuildErrorDetail::DeclarationShapeMismatch { .. })
        ),
        "{shape:?}"
    );

    let round_output = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        let order = builder.borrow_mut().fresh_order();
        builder
            .borrow_mut()
            .declare_output(round, Vec::new(), order);
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            round_output,
            Some(BuildErrorDetail::DeclarationNotAllowed { .. })
        ),
        "{round_output:?}"
    );

    let ordinary_next = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let order = builder.borrow_mut().fresh_order();
        builder.borrow_mut().declare_next(
            root,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            order,
        );
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            ordinary_next,
            Some(BuildErrorDetail::DeclarationNotAllowed { .. })
        ),
        "{ordinary_next:?}"
    );

    let child_output_cannot_replace_next = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        let child = builder
            .borrow_mut()
            .new_body(Some(round), BodyRole::Ordinary);
        declare_ok(
            &builder,
            child,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
        );
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            child_output_cannot_replace_next,
            Some(BuildErrorDetail::MissingDeclaration {
                kind: DeclarationKind::Next,
                ..
            })
        ),
        "{child_output_cannot_replace_next:?}"
    );
}

// ---------------------------------------------------------------- A12 旧契约

#[test]
fn a12_old_adapters_preserve_non_unit_and_first_failure() {
    // Chain：non-unit + 首声明失败
    let chain_non_unit = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), child);
            flow.output::<Amount>();
        }
        let before = output_snapshot(&builder, child);
        assert_eq!(before.0, DeclarationState::NonUnit);
        let order = builder.borrow_mut().fresh_order();
        assert!(!builder.borrow_mut().declare_output(
            child,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            order
        ));
        let (outputs, _declared) = builder
            .borrow_mut()
            .register_chain_outputs(root, child, None);
        // 重复声明使该 Body 的声明失败：收口保留第一份声明快照与首因，
        // 但必须隔离派生的父输出（槽位对齐、来源保持失效）。
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("chain output")
                .is_tainted()
        );
        assert_eq!(output_snapshot(&builder, child), before);
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        chain_non_unit,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let chain_first_failure = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), child);
            flow.output::<Missing>();
        }
        let before = output_snapshot(&builder, child);
        assert_eq!(before.0, DeclarationState::Failed);
        let (outputs, _declared) = builder
            .borrow_mut()
            .register_chain_outputs(root, child, None);
        // 新契约：收口消费 child 的失败声明，按声明槽数发布失效句柄（槽位对齐 + taint），
        // 不再像旧适配器那样按空槽位登记；首因仍由 child 自身保留。
        assert_eq!(outputs.len(), 1, "失败声明仍保持槽位对齐");
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("chain output")
                .is_tainted()
        );
        assert_eq!(output_snapshot(&builder, child), before);
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        chain_first_failure,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    // Choose：non-unit（含 otherwise）+ 首声明失败
    let choose_non_unit = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let step = builder.borrow().step_count(root);
        let outputs =
            builder
                .borrow_mut()
                .allocate_choose_outputs(root, step, &[TypeDesc::of::<Amount>()]);
        let case = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), case);
            flow.output::<Amount>();
            // 重复声明：保留 DuplicateDeclaration 与派生失效(taint) 的原有断言。
            flow.output_with(amount(inputs[0]));
        }
        let case_before = output_snapshot(&builder, case);
        builder
            .borrow_mut()
            .register_choose_branch(case, step, 0, &outputs);
        assert_eq!(output_snapshot(&builder, case), case_before);
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("choose output")
                .is_tainted()
        );

        let otherwise = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), otherwise);
            flow.output::<Amount>();
            // 重复声明：保留 DuplicateDeclaration 与派生失效(taint) 的原有断言。
            flow.output_with(amount(inputs[0]));
        }
        let otherwise_before = output_snapshot(&builder, otherwise);
        builder
            .borrow_mut()
            .register_choose_branch(otherwise, step, 1, &outputs);
        assert_eq!(output_snapshot(&builder, otherwise), otherwise_before);
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        choose_non_unit,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let choose_first_failure = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let step = builder.borrow().step_count(root);
        let outputs =
            builder
                .borrow_mut()
                .allocate_choose_outputs(root, step, &[TypeDesc::of::<Amount>()]);
        let branch = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), branch);
            flow.output::<Missing>();
        }
        let before = output_snapshot(&builder, branch);
        assert_eq!(before.0, DeclarationState::Failed);
        builder
            .borrow_mut()
            .register_choose_branch(branch, step, 0, &outputs);
        assert_eq!(output_snapshot(&builder, branch), before);
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("choose output")
                .published_order()
                .is_none(),
            "failed declaration must not publish the parent output"
        );

        let otherwise = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), otherwise);
            flow.output::<Missing>();
        }
        let otherwise_before = output_snapshot(&builder, otherwise);
        assert_eq!(otherwise_before.0, DeclarationState::Failed);
        builder
            .borrow_mut()
            .register_choose_branch(otherwise, step, 1, &outputs);
        assert_eq!(output_snapshot(&builder, otherwise), otherwise_before);
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("choose output")
                .published_order()
                .is_none()
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        choose_first_failure,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    // Retry：non-unit + 首声明失败
    let retry_non_unit = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), child);
            flow.output::<Amount>();
        }
        let before = output_snapshot(&builder, child);
        let order = builder.borrow_mut().fresh_order();
        assert!(!builder.borrow_mut().declare_output(
            child,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            order
        ));
        let (outputs, _declared) = builder.borrow_mut().register_retry(root, child, 1, None);
        // 与 Chain 同理：重复声明后的 child 不得发布合法父域来源。
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("retry output")
                .is_tainted()
        );
        assert_eq!(output_snapshot(&builder, child), before);
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        retry_non_unit,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let retry_first_failure = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), child);
            flow.output::<Missing>();
        }
        let before = output_snapshot(&builder, child);
        let (outputs, _declared) = builder.borrow_mut().register_retry(root, child, 1, None);
        // 新契约：失败声明仍按声明槽数发布失效句柄（槽位对齐 + taint），首因保留在 child。
        assert_eq!(outputs.len(), 1, "失败声明仍保持槽位对齐");
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("retry output")
                .is_tainted()
        );
        assert_eq!(output_snapshot(&builder, child), before);
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        retry_first_failure,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    // Each：non-unit + 首声明失败
    let each_non_unit = {
        let (builder, root, inputs) =
            builder_with_inputs(&[TypeDesc::of::<Vec<Amount>>(), TypeDesc::of::<Amount>()]);
        let item = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let item_position =
            builder
                .borrow_mut()
                .allocate_each_item(item, 0, TypeDesc::of::<Amount>());
        {
            let mut flow = Flow::open(Rc::clone(&builder), item);
            flow.output::<Amount>();
            // 重复声明：保留 DuplicateDeclaration 与派生失效(taint) 的原有断言。
            flow.output_with(amount(inputs[1]));
        }
        let before = output_snapshot(&builder, item);
        let outputs = builder.borrow_mut().register_each(
            root,
            item,
            0,
            desc(inputs[0], TypeDesc::of::<Vec<Amount>>()),
            item_position,
            None,
            super::shape::each_bind_item::<Amount>,
            super::shape::each_collection_len::<Amount>,
        );
        assert!(
            builder
                .borrow()
                .position(outputs.0[0])
                .expect("each output")
                .is_tainted()
        );
        assert_eq!(output_snapshot(&builder, item), before);
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        each_non_unit,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let each_first_failure = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Vec<Amount>>()]);
        let item = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let item_position =
            builder
                .borrow_mut()
                .allocate_each_item(item, 0, TypeDesc::of::<Amount>());
        {
            let mut flow = Flow::open(Rc::clone(&builder), item);
            flow.output::<Missing>();
        }
        let before = output_snapshot(&builder, item);
        let outputs = builder.borrow_mut().register_each(
            root,
            item,
            0,
            desc(inputs[0], TypeDesc::of::<Vec<Amount>>()),
            item_position,
            None,
            super::shape::each_bind_item::<Amount>,
            super::shape::each_collection_len::<Amount>,
        );
        assert_eq!(outputs.0.len(), 1, "失败声明仍保持槽位对齐");
        assert_eq!(output_snapshot(&builder, item), before);
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        each_first_failure,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    // Iter：non-unit + 首声明失败
    let iter_non_unit = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), round);
            flow.next_with(amount(inputs[0]));
        }
        let before = next_snapshot(&builder, round);
        // 新契约：`declared` 不再经 `register_iter` 补声明；原第二次声明改为等价的显式
        // 声明路径，仍须被拒绝且不得覆盖首份声明。
        let order = builder.borrow_mut().fresh_order();
        assert!(!builder.borrow_mut().declare_next(
            round,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            order
        ));
        let entry_order = builder.borrow_mut().fresh_order();
        let (outputs, _declared) = builder.borrow_mut().register_iter(
            root,
            round,
            0,
            entry_order,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            Vec::new(),
            1,
            true, // 低层登记：本节测试不引入 Round 内新增构建错误
            <Ref<Amount> as StateShape>::register_states,
            <Ref<Amount> as StateShape>::state_import_slots,
        );
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("iter output")
                .is_tainted()
        );
        assert_eq!(next_snapshot(&builder, round), before);
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        iter_non_unit,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let iter_first_failure = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), round);
            flow.next::<Missing>();
        }
        let before = next_snapshot(&builder, round);
        assert_eq!(before.0, DeclarationState::Failed);
        // 新契约：`declared` 不再经 `register_iter` 补声明；原第二次声明改为等价的显式
        // 声明路径，仍须被拒绝且不得覆盖首因。
        let order = builder.borrow_mut().fresh_order();
        assert!(!builder.borrow_mut().declare_next(
            round,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            order
        ));
        let entry_order = builder.borrow_mut().fresh_order();
        let (outputs, _declared) = builder.borrow_mut().register_iter(
            root,
            round,
            0,
            entry_order,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            Vec::new(),
            1,
            true, // 低层登记：本节测试不引入 Round 内新增构建错误
            <Ref<Amount> as StateShape>::register_states,
            <Ref<Amount> as StateShape>::state_import_slots,
        );
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("iter output")
                .is_tainted()
        );
        assert_eq!(next_snapshot(&builder, round), before);
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        iter_first_failure,
        Some(BuildErrorDetail::MissingSource { .. })
    ));
}

// ---------------------------------------------------------------- A13 失败隔离

#[test]
fn a13a_missing_ref_of_fails_even_when_unused() {
    let unused = build_definition::<(), (), _>(|flow, ()| {
        let _ = flow.ref_of::<Missing>();
        let () = ();
        flow.output_with(());
    })
    .err()
    .expect("unused missing query must fail");
    assert!(
        matches!(unused, BuildErrorDetail::MissingSource { .. }),
        "{unused}"
    );

    let used = build_definition::<(), (), _>(|flow, ()| {
        let bad = flow.ref_of::<Missing>();
        let _ = flow.then_with_output(sink_missing, bad);
        let () = ();
        flow.output_with(());
    })
    .err()
    .expect("explicit use of a failed handle must fail");
    assert!(
        matches!(used, BuildErrorDetail::MissingSource { .. }),
        "{used}"
    );
}

fn unpublished_amount(builder: &Rc<RefCell<Builder<'static>>>, root: BodyId) -> PositionHandle {
    let step = builder.borrow().step_count(root);
    builder
        .borrow_mut()
        .allocate_choose_outputs(root, step, &[TypeDesc::of::<Amount>()])[0]
}

fn output_snapshot(
    builder: &Rc<RefCell<Builder<'static>>>,
    body: BodyId,
) -> (DeclarationState, u64, Vec<PositionDesc>) {
    let builder = builder.borrow();
    let declaration = builder.body(body).output_declaration();
    (
        declaration.state(),
        declaration.order,
        declaration.slots.clone(),
    )
}

fn next_snapshot(
    builder: &Rc<RefCell<Builder<'static>>>,
    body: BodyId,
) -> (DeclarationState, u64, Vec<PositionDesc>) {
    let builder = builder.borrow();
    let declaration = builder.body(body).next_declaration();
    (
        declaration.state(),
        declaration.order,
        declaration.slots.clone(),
    )
}

#[test]
fn a13b2_explicit_declarations_reject_unpublished_and_tainted() {
    let unpublished_output = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let source = unpublished_amount(&builder, root);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output_with(amount(source));
        }
        let declaration = builder.borrow().body(root).output_declaration().clone();
        assert!(matches!(
            declaration.first_failure,
            Some(BuildErrorDetail::NotPublished { .. })
        ));
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        unpublished_output,
        Some(BuildErrorDetail::NotPublished { .. })
    ));

    let unpublished_next = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let source = unpublished_amount(&builder, root);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), round);
            flow.next_with(amount(source));
        }
        let declaration = builder.borrow().body(round).next_declaration().clone();
        assert!(matches!(
            declaration.first_failure,
            Some(BuildErrorDetail::NotPublished { .. })
        ));
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        unpublished_next,
        Some(BuildErrorDetail::NotPublished { .. })
    ));

    let tainted_output = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        builder.borrow_mut().mark_tainted(inputs[0]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output_with(amount(inputs[0]));
        }
        let declaration = builder.borrow().body(root).output_declaration().clone();
        assert!(matches!(
            declaration.first_failure,
            Some(BuildErrorDetail::TaintedSource { .. })
        ));
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        tainted_output,
        Some(BuildErrorDetail::TaintedSource { .. })
    ));

    let tainted_next = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        builder.borrow_mut().mark_tainted(inputs[0]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), round);
            flow.next_with(amount(inputs[0]));
        }
        let declaration = builder.borrow().body(round).next_declaration().clone();
        assert!(matches!(
            declaration.first_failure,
            Some(BuildErrorDetail::TaintedSource { .. })
        ));
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        tainted_next,
        Some(BuildErrorDetail::TaintedSource { .. })
    ));
}

// ---------------------------------------------------------------- A14 声明失败

#[test]
fn a14_later_same_type_source_is_usable_and_old_handle_stays_invalid() {
    let error = build_definition::<(), (), _>(|flow, ()| {
        let bad = flow.ref_of::<Missing>();
        let produced = flow.then_with_output(load_missing, ());
        let found = flow.ref_of::<Missing>();
        assert_eq!(found.handle(), produced.handle());
        assert_ne!(found.handle(), bad.handle());
        let _ = flow.then_with_output(sink_missing, found);
        let _ = flow.then_with_output(sink_missing, bad);
        let () = ();
        flow.output_with(());
    })
    .err()
    .expect("the original failed handle must keep the build rejected");
    assert!(
        matches!(error, BuildErrorDetail::MissingSource { .. }),
        "{error}"
    );
}

// ---------------------------------------------------------------- A15 Runtime 收口

#[test]
fn a15_runtime_rejects_missing_source_before_any_node() {
    EXECUTIONS.store(0, Ordering::SeqCst);
    let runtime = Runtime::new();
    let error = block_on(runtime.execute::<(), (), _>(
        |flow, ()| {
            let _ = flow.ref_of::<Missing>();
            let _ = flow.then_with_output(counting_load, ());
            let () = ();
            flow.output_with(());
        },
        (),
    ))
    .err()
    .expect("missing source must reject the definition");
    match error {
        RunError::Definition(build) => {
            let text = format!("{build}");
            assert!(text.contains("no available business source"), "{text}");
            assert!(text.contains("Missing"), "{text}");
        }
        other => panic!("expected Definition error, got {other}"),
    }
    assert_eq!(EXECUTIONS.load(Ordering::SeqCst), 0);

    EXECUTIONS.store(0, Ordering::SeqCst);
    let used = block_on(runtime.execute::<(), (), _>(
        |flow, ()| {
            let bad = flow.ref_of::<Missing>();
            let _ = flow.then_with_output(sink_missing, bad);
            let _ = flow.then_with_output(counting_load, ());
            let () = ();
            flow.output_with(());
        },
        (),
    ))
    .err()
    .expect("explicit use of the failed handle must reject");
    match used {
        RunError::Definition(build) => {
            let text = format!("{build}");
            assert!(text.contains("no available business source"), "{text}");
            assert!(text.contains("Missing"), "{text}");
        }
        other => panic!("expected Definition error, got {other}"),
    }
    assert_eq!(EXECUTIONS.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------- A16 旧路径回归
