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

async fn double(query: Query<&Amount>) -> Result<Doubled, BodyError> {
    Ok(Doubled(query.get().0 * 2))
}

async fn sink_amount(query: Query<&Amount>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

async fn sink_doubled(query: Query<&Doubled>) -> Result<(), BodyError> {
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

async fn missing_to_amount(query: Query<&Missing>) -> Result<Amount, BodyError> {
    let _ = query.get();
    Ok(Amount(0))
}

async fn key_from_amount(query: Query<&Amount>) -> Result<Key, BodyError> {
    Ok(Key(query.get().0))
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

#[test]
fn a02_child_local_and_sibling_are_not_visible() {
    let error = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        flow.chain(|sub| {
            let _ = sub.then_with_output(double, amount_ref);
            sub.output::<()>();
        });
        let _ = flow.ref_of::<Doubled>();
        let out_refs = amount_ref;
        flow.output_with(out_refs);
    })
    .err()
    .expect("unexported child-local output must not be visible to the parent");
    assert!(
        matches!(error, BuildErrorDetail::MissingSource { .. }),
        "{error}"
    );
}

#[test]
fn a02_sibling_child_local_is_not_visible() {
    let error = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        flow.chain(|sub| {
            let _ = sub.then_with_output(double, amount_ref);
            sub.output::<()>();
        });
        flow.chain(|sub| {
            let _ = sub.ref_of::<Doubled>();
            sub.output::<()>();
        });
        let out_refs = amount_ref;
        flow.output_with(out_refs);
    })
    .err()
    .expect("a sibling's unexported local must not be visible");
    assert!(
        matches!(error, BuildErrorDetail::MissingSource { .. }),
        "{error}"
    );
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

#[test]
fn a03a_preallocated_choose_output_has_no_other_source() {
    let (builder, root, _inputs) = builder_with_inputs(&[TypeDesc::of::<Key>()]);
    let step = builder.borrow().step_count(root);
    let _outputs =
        builder
            .borrow_mut()
            .allocate_choose_outputs(root, step, &[TypeDesc::of::<Doubled>()]);
    let branch = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    {
        let flow = Flow::open(Rc::clone(&builder), branch);
        let _ = flow.ref_of::<Doubled>();
    }
    let error = finish_builder(builder, root).err().expect("missing source");
    assert!(
        matches!(error, BuildErrorDetail::MissingSource { .. }),
        "{error}"
    );
}

#[test]
fn a03a_early_explicit_use_is_not_retroactively_published() {
    let (builder, root, inputs) =
        builder_with_inputs(&[TypeDesc::of::<Key>(), TypeDesc::of::<Amount>()]);
    let key = inputs[0];
    let amount_source = inputs[1];
    let step = builder.borrow().step_count(root);
    let step_order = builder.borrow_mut().fresh_order();
    let outputs =
        builder
            .borrow_mut()
            .allocate_choose_outputs(root, step, &[TypeDesc::of::<Amount>()]);
    let branch = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    {
        let mut flow = Flow::open(Rc::clone(&builder), branch);
        let _ = flow.then_with_output(add_one, amount(outputs[0]));
    }
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
    assert!(
        builder
            .borrow()
            .position(outputs[0])
            .expect("choose output record")
            .published_order()
            .expect("published after push")
            > step_order,
        "early use must remain before the actual publication order"
    );
    let error = finish_builder(builder, root)
        .err()
        .expect("early unpublished use must remain rejected");
    assert!(
        matches!(error, BuildErrorDetail::NotPublished { .. }),
        "{error}"
    );
}

// ---------------------------------------------------------------- A03b 类别与遮蔽

fn classify(source: PositionSource, query_child: bool) -> Option<BuildErrorDetail> {
    let builder = Rc::new(RefCell::new(Builder::<'static>::new()));
    let root = builder.borrow_mut().new_body(None, BodyRole::Ordinary);
    let child = if query_child {
        Some(
            builder
                .borrow_mut()
                .new_body(Some(root), BodyRole::Ordinary),
        )
    } else {
        None
    };
    let owner = child.unwrap_or(root);
    let handle = builder
        .borrow_mut()
        .allocate(owner, source, TypeDesc::of::<Amount>());
    let order = builder.borrow_mut().fresh_order();
    builder
        .borrow()
        .availability_error(owner, None, desc(handle, TypeDesc::of::<Amount>()), order)
}

#[test]
fn a03b_business_source_table_is_explicit() {
    let allowed = [
        PositionSource::RootInput { slot: 0 },
        PositionSource::NodeOutput { step: 0, slot: 0 },
        PositionSource::ChainOutput { step: 0, slot: 0 },
        PositionSource::ChooseOutput { step: 0, slot: 0 },
        PositionSource::RetryOutput { step: 0, slot: 0 },
        PositionSource::IterCurrent { step: 0, slot: 0 },
        PositionSource::IterOutput { step: 0, slot: 0 },
        PositionSource::EachItem { step: 0, slot: 0 },
        PositionSource::EachOutput { step: 0, slot: 0 },
    ];
    for source in allowed {
        assert!(
            classify(source, true).is_none(),
            "{source:?} should be business"
        );
    }
    let excluded = [
        PositionSource::ChainExit { step: 0, slot: 0 },
        PositionSource::ChooseExit {
            step: 0,
            branch: 0,
            slot: 0,
        },
        PositionSource::RetryExit { step: 0, slot: 0 },
        PositionSource::IterExit { step: 0, slot: 0 },
        PositionSource::RetryBridge { step: 0, slot: 0 },
        PositionSource::IterBridge { step: 0, slot: 0 },
        PositionSource::EachLocal { step: 0, slot: 0 },
    ];
    for source in excluded {
        assert!(
            matches!(
                classify(source, true),
                Some(BuildErrorDetail::NotBusinessSource { .. })
            ),
            "{source:?} must be excluded"
        );
    }
}

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
        let iterated = flow.iter(amount_ref, 1, |current, sub| {
            sub.then_with_output(add_one, current)
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
        let out_refs = flow.iter(amount_ref, 1, |current, round| {
            let saved = round.ref_of::<Amount>();
            assert_eq!(saved.handle(), current.handle());
            let advanced = round.then_with_output(add_one, current);
            let nearest = round.ref_of::<Amount>();
            assert_eq!(nearest.handle(), advanced.handle());
            let _ = round.then_with_output(sink_amount, saved);
            advanced
        });
        flow.output_with(out_refs);
    })
    .expect("current query, saved identity and shadowing should build");
}

#[test]
fn a04_current_is_not_visible_to_the_parent() {
    let (builder, root, _inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
    let current =
        builder
            .borrow_mut()
            .allocate_iter_current(round, 0, &[TypeDesc::of::<Doubled>()]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        let _ = flow.then_with_output(sink_doubled, doubled(current[0]));
    }
    let error = finish_builder(builder, root)
        .err()
        .expect("parent use of formal current must be rejected");
    assert!(
        matches!(
            error,
            BuildErrorDetail::OutOfScope {
                relation: super::error::Visibility::ChildLocal,
                ..
            }
        ),
        "{error}"
    );
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

#[test]
fn a05_declaration_imports_are_recorded_and_deduplicated() {
    let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let amount_source = inputs[0];
    let child = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    let grandchild = builder
        .borrow_mut()
        .new_body(Some(child), BodyRole::Ordinary);
    {
        let mut flow = Flow::open(Rc::clone(&builder), grandchild);
        let _ = flow.then_with_output(add_one, amount(amount_source));
        flow.output_with(amount(amount_source));
    }
    let definition = finish_builder(builder, root).expect("grandchild output should build");
    for body in [child, grandchild] {
        let body = definition.body(body);
        assert_eq!(
            body.imports()
                .iter()
                .filter(|position| **position == amount_source.position)
                .count(),
            1,
            "each layer records one deduplicated import"
        );
    }
}

#[test]
fn a05_next_with_records_ancestor_imports() {
    let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let amount_source = inputs[0];
    let child = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    let round = builder.borrow_mut().new_body(Some(child), BodyRole::Round);
    builder
        .borrow_mut()
        .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), round);
        flow.next_with(amount(amount_source));
    }
    let definition = finish_builder(builder, root).expect("round next should build");
    for body in [child, round] {
        assert!(
            definition
                .body(body)
                .imports()
                .contains(&amount_source.position)
        );
    }
}

#[test]
fn a05_output_only_declaration_records_imports() {
    let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let amount_source = inputs[0];
    let child = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    let grandchild = builder
        .borrow_mut()
        .new_body(Some(child), BodyRole::Ordinary);
    {
        let mut flow = Flow::open(Rc::clone(&builder), grandchild);
        flow.output_with(amount(amount_source));
    }
    let definition = finish_builder(builder, root).expect("output-only declaration should build");
    for body in [child, grandchild] {
        assert_eq!(
            definition
                .body(body)
                .imports()
                .iter()
                .filter(|position| **position == amount_source.position)
                .count(),
            1,
            "output declaration alone must record one import per layer"
        );
    }
}

#[test]
fn a05_next_use_deduplicates_with_node_input() {
    let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let amount_source = inputs[0];
    let child = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    let round = builder.borrow_mut().new_body(Some(child), BodyRole::Round);
    builder
        .borrow_mut()
        .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), round);
        let _ = flow.then_with_output(add_one, amount(amount_source));
        flow.next_with(amount(amount_source));
    }
    let definition = finish_builder(builder, root).expect("next declaration should build");
    for body in [child, round] {
        assert_eq!(
            definition
                .body(body)
                .imports()
                .iter()
                .filter(|position| **position == amount_source.position)
                .count(),
            1,
            "node use and next declaration share one import per layer"
        );
    }
}

// ---------------------------------------------------------------- A06 声明状态与重复

#[test]
fn a06_declaration_states_are_distinct() {
    let (builder, root, _) = builder_with_inputs(&[]);
    let definition = finish_builder(builder, root).expect("undeclared empty body");
    assert_eq!(
        definition.root().output_declaration().state(),
        DeclarationState::Undeclared
    );

    let (builder, root, _) = builder_with_inputs(&[]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output::<()>();
    }
    let definition = finish_builder(builder, root).expect("unit declaration");
    assert_eq!(
        definition.root().output_declaration().state(),
        DeclarationState::Unit
    );

    let (builder, root, _) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output::<Amount>();
    }
    let definition = finish_builder(builder, root).expect("non-unit declaration");
    assert_eq!(
        definition.root().output_declaration().state(),
        DeclarationState::NonUnit
    );
}

#[test]
fn a06_duplicate_declarations_do_not_overwrite_the_first() {
    for (first_unit, second_unit) in [(false, false), (true, true), (true, false)] {
        let (builder, root, _) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            if first_unit {
                flow.output::<()>();
            } else {
                flow.output::<Amount>();
            }
            if second_unit {
                flow.output::<()>();
            } else {
                flow.output::<Amount>();
            }
        }
        let (state, order) = {
            let state = builder.borrow();
            let declaration = state.body(root).output_declaration();
            (declaration.state(), declaration.order)
        };
        assert_eq!(
            state,
            if first_unit {
                DeclarationState::Unit
            } else {
                DeclarationState::NonUnit
            }
        );
        let error = finish_builder(builder, root)
            .err()
            .expect("second declaration must be rejected");
        assert!(
            matches!(error, BuildErrorDetail::DuplicateDeclaration { .. }),
            "{error}"
        );
        assert!(order > 0);
    }
}

#[test]
fn a06_failed_declaration_is_not_cleared_by_a_later_one() {
    let (builder, root, _) = builder_with_inputs(&[]);
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        flow.output::<Missing>();
        flow.output::<()>();
    }
    let state = builder.borrow().body(root).output_declaration().state();
    assert_eq!(state, DeclarationState::Failed);
    let error = finish_builder(builder, root)
        .err()
        .expect("failed declaration must keep the build failed");
    assert!(
        matches!(error, BuildErrorDetail::MissingSource { .. }),
        "{error}"
    );
}

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

#[test]
fn a09_root_expectation_checks_unit_and_non_unit() {
    let unit_omitted = {
        let (builder, root, _) = builder_with_inputs(&[]);
        builder.borrow_mut().set_root_expectation(Vec::new());
        finish_builder(builder, root)
    };
    assert!(unit_omitted.is_ok());

    let unit_explicit = {
        let (builder, root, _) = builder_with_inputs(&[]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output::<()>();
        }
        builder.borrow_mut().set_root_expectation(Vec::new());
        finish_builder(builder, root)
    };
    assert!(unit_explicit.is_ok());

    let missing = {
        let (builder, root, _) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        builder
            .borrow_mut()
            .set_root_expectation(vec![TypeDesc::of::<Amount>()]);
        finish_builder(builder, root)
    };
    let missing = match missing {
        Err(error) => error,
        Ok(_) => panic!("non-unit root must declare output"),
    };
    assert!(
        matches!(missing, BuildErrorDetail::MissingDeclaration { .. }),
        "{missing}"
    );

    let arity = {
        let (builder, root, _) = builder_with_inputs(&[]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output::<()>();
        }
        builder
            .borrow_mut()
            .set_root_expectation(vec![TypeDesc::of::<Amount>()]);
        finish_builder(builder, root)
    };
    let arity = match arity {
        Err(error) => error,
        Ok(_) => panic!("expected arity must match"),
    };
    assert!(
        matches!(arity, BuildErrorDetail::DeclarationArityMismatch { .. }),
        "{arity}"
    );

    let shape = {
        let (builder, root, _) = builder_with_inputs(&[TypeDesc::of::<Doubled>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output::<Doubled>();
        }
        builder
            .borrow_mut()
            .set_root_expectation(vec![TypeDesc::of::<Amount>()]);
        finish_builder(builder, root)
    };
    let shape = match shape {
        Err(error) => error,
        Ok(_) => panic!("expected shape must match"),
    };
    assert!(
        matches!(shape, BuildErrorDetail::DeclarationShapeMismatch { .. }),
        "{shape}"
    );

    let matching = {
        let (builder, root, _) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output::<Amount>();
        }
        builder
            .borrow_mut()
            .set_root_expectation(vec![TypeDesc::of::<Amount>()]);
        finish_builder(builder, root)
    };
    assert!(matching.is_ok());
}

// ---------------------------------------------------------------- A10 Choose 契约

fn push_choose_key(
    builder: &Rc<RefCell<Builder<'static>>>,
    root: BodyId,
    selector: super::shape::PositionDesc,
    cases: Vec<BodyId>,
    otherwise: Option<BodyId>,
    output_types: &[TypeDesc],
) {
    let step = builder.borrow().step_count(root);
    let outputs = builder
        .borrow_mut()
        .allocate_choose_outputs(root, step, output_types);
    let order = builder.borrow_mut().fresh_order();
    let cases = cases
        .into_iter()
        .enumerate()
        .map(|(index, body)| super::definition::ChooseCase {
            key: Key(index as u32),
            branch: super::definition::ChooseBranch {
                body,
                sources: Vec::new(),
                slots: Vec::new(),
            },
        })
        .collect();
    let otherwise = otherwise.map(|body| super::definition::ChooseBranch {
        body,
        sources: Vec::new(),
        slots: Vec::new(),
    });
    let outputs = outputs.iter().map(|handle| handle.position).collect();
    builder.borrow_mut().push_choose_step(
        root,
        super::definition::ChooseStep::new(selector, cases, otherwise, outputs, order),
        true,
    );
}

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

#[test]
fn a10_choose_all_undeclared_unit_and_non_unit_are_legal() {
    for mode in 0..3 {
        let (builder, root, inputs) = builder_with_inputs(&[
            TypeDesc::of::<Key>(),
            TypeDesc::of::<Amount>(),
            TypeDesc::of::<Doubled>(),
        ]);
        let selector = desc(inputs[0], TypeDesc::of::<Key>());
        let branches: Vec<BodyId> = (0..2)
            .map(|_| {
                builder
                    .borrow_mut()
                    .new_body(Some(root), BodyRole::Ordinary)
            })
            .collect();
        let (output_types, declared) = match mode {
            0 => (Vec::new(), None),
            1 => (Vec::new(), Some(vec![])),
            _ => (
                vec![TypeDesc::of::<Amount>()],
                Some(vec![desc(inputs[1], TypeDesc::of::<Amount>())]),
            ),
        };
        if let Some(slots) = declared {
            for branch in &branches {
                declare_ok(&builder, *branch, slots.clone());
            }
        }
        push_choose_key(&builder, root, selector, branches, None, &output_types);
        finish_builder(builder, root).expect("consistent choose branches");
    }
}

#[test]
fn a10_choose_mixed_state_and_shape_mismatches_are_rejected() {
    let mixed = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Key>()]);
        let selector = desc(inputs[0], TypeDesc::of::<Key>());
        let branches: Vec<BodyId> = (0..2)
            .map(|_| {
                builder
                    .borrow_mut()
                    .new_body(Some(root), BodyRole::Ordinary)
            })
            .collect();
        declare_ok(&builder, branches[0], vec![]);
        push_choose_key(&builder, root, selector, branches, None, &[]);
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            mixed,
            Some(BuildErrorDetail::ChooseDeclarationStateMismatch { .. })
        ),
        "{mixed:?}"
    );

    let arity = {
        let (builder, root, inputs) =
            builder_with_inputs(&[TypeDesc::of::<Key>(), TypeDesc::of::<Amount>()]);
        let selector = desc(inputs[0], TypeDesc::of::<Key>());
        let branches: Vec<BodyId> = (0..2)
            .map(|_| {
                builder
                    .borrow_mut()
                    .new_body(Some(root), BodyRole::Ordinary)
            })
            .collect();
        let one = vec![desc(inputs[1], TypeDesc::of::<Amount>())];
        let two = vec![
            desc(inputs[1], TypeDesc::of::<Amount>()),
            desc(inputs[1], TypeDesc::of::<Amount>()),
        ];
        declare_ok(&builder, branches[0], one);
        declare_ok(&builder, branches[1], two);
        push_choose_key(
            &builder,
            root,
            selector,
            branches,
            None,
            &[TypeDesc::of::<Amount>(), TypeDesc::of::<Amount>()],
        );
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(arity, Some(BuildErrorDetail::ChooseArityMismatch { .. })),
        "{arity:?}"
    );

    let slot_type = {
        let (builder, root, inputs) = builder_with_inputs(&[
            TypeDesc::of::<Key>(),
            TypeDesc::of::<Amount>(),
            TypeDesc::of::<Doubled>(),
        ]);
        let selector = desc(inputs[0], TypeDesc::of::<Key>());
        let branches: Vec<BodyId> = (0..2)
            .map(|_| {
                builder
                    .borrow_mut()
                    .new_body(Some(root), BodyRole::Ordinary)
            })
            .collect();
        declare_ok(
            &builder,
            branches[0],
            vec![desc(inputs[1], TypeDesc::of::<Amount>())],
        );
        declare_ok(
            &builder,
            branches[1],
            vec![desc(inputs[2], TypeDesc::of::<Doubled>())],
        );
        push_choose_key(
            &builder,
            root,
            selector,
            branches,
            None,
            &[TypeDesc::of::<Amount>()],
        );
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            slot_type,
            Some(BuildErrorDetail::ChooseSlotTypeMismatch { .. })
        ),
        "{slot_type:?}"
    );

    let otherwise_mixed = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Key>()]);
        let selector = desc(inputs[0], TypeDesc::of::<Key>());
        let case = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let otherwise = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        declare_ok(&builder, case, vec![]);
        push_choose_key(&builder, root, selector, vec![case], Some(otherwise), &[]);
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(
            otherwise_mixed,
            Some(BuildErrorDetail::ChooseDeclarationStateMismatch { .. })
        ),
        "{otherwise_mixed:?}"
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

#[test]
fn a11_old_iter_maps_state_to_next() {
    let definition = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        let out_refs = flow.iter(amount_ref, 1, |current, sub| {
            sub.then_with_output(add_one, current)
        });
        flow.output_with(out_refs);
    })
    .expect("old iter should build");
    let round = definition
        .bodies()
        .iter()
        .find(|body| body.role() == BodyRole::Round)
        .expect("round body");
    assert_eq!(round.next_declaration().state(), DeclarationState::NonUnit);
    assert_eq!(
        round.output_declaration().state(),
        DeclarationState::Undeclared
    );
}

// ---------------------------------------------------------------- A12 旧契约

#[test]
fn a12_old_unit_and_next_mappings() {
    let root = build_definition::<(), (), _>(|_flow, ()| {
        _flow.output_with(());
    })
    .expect("old unit root");
    assert_eq!(
        root.root().output_declaration().state(),
        DeclarationState::Unit
    );

    let chain = build_definition::<(), (), _>(|flow, ()| {
        flow.chain(|_sub| {
            _sub.output::<()>();
        });
        let () = ();
        flow.output_with(());
    })
    .expect("old unit chain");
    let child = chain
        .bodies()
        .iter()
        .find(|body| body.parent() == Some(chain.root().id()))
        .expect("chain child");
    assert_eq!(child.output_declaration().state(), DeclarationState::Unit);

    let retry = build_definition::<(), (), _>(|flow, ()| {
        flow.retry(1, |_sub| {
            _sub.output::<()>();
        });
        let () = ();
        flow.output_with(());
    })
    .expect("old unit retry");
    let attempt = retry
        .bodies()
        .iter()
        .find(|body| body.parent() == Some(retry.root().id()))
        .expect("retry attempt");
    assert_eq!(attempt.output_declaration().state(), DeclarationState::Unit);

    let each = build_definition::<Vec<Amount>, (), _>(|flow, items| {
        flow.each(items, |_sub| {
            let _item = _sub.ref_of::<Amount>();

            let out_refs = ();
            _sub.output_with(out_refs);
        });
        let () = ();
        flow.output_with(());
    })
    .expect("old unit each");
    let item = each
        .bodies()
        .iter()
        .find(|body| body.parent() == Some(each.root().id()))
        .expect("each item");
    assert_eq!(item.output_declaration().state(), DeclarationState::Unit);

    let choose = build_definition::<Amount, (), _>(|flow, amount_ref| {
        let selector = flow.then_with_output(key_from_amount, amount_ref);
        flow.choose_with(selector, |choice| {
            choice.case(Key(1), |_sub| {
                let out_refs = ();
                _sub.output_with(out_refs);
            });
            choice.case(Key(2), |_sub| {
                let out_refs = ();
                _sub.output_with(out_refs);
            });
        });
        let () = ();
        flow.output_with(());
    })
    .expect("old unit choose");
    let branches: Vec<_> = choose
        .bodies()
        .iter()
        .filter(|body| body.parent() == Some(choose.root().id()))
        .collect();
    assert_eq!(branches.len(), 2);
    for branch in branches {
        assert_eq!(branch.output_declaration().state(), DeclarationState::Unit);
    }

    let iter = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        let out_refs = flow.iter(amount_ref, 1, |current, sub| {
            sub.then_with_output(add_one, current)
        });
        flow.output_with(out_refs);
    })
    .expect("old iter");
    let round = iter
        .bodies()
        .iter()
        .find(|body| body.role() == BodyRole::Round)
        .expect("round body");
    assert_eq!(round.next_declaration().state(), DeclarationState::NonUnit);
    assert_eq!(
        round.output_declaration().state(),
        DeclarationState::Undeclared
    );
}

#[test]
fn a12_old_adapters_reject_second_declarations() {
    let root = {
        let (builder, root, _) = builder_with_inputs(&[]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output::<()>();
        }
        let order = builder.borrow_mut().fresh_order();
        assert!(!builder.borrow_mut().declare_output(root, Vec::new(), order));
        assert_eq!(
            builder.borrow().body(root).output_declaration().state(),
            DeclarationState::Unit
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        root,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let chain = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), child);
            flow.output::<()>();
        }
        let order = builder.borrow_mut().fresh_order();
        assert!(
            !builder
                .borrow_mut()
                .declare_output(child, Vec::new(), order)
        );
        builder
            .borrow_mut()
            .register_chain_outputs(root, child, None);
        assert_eq!(
            builder.borrow().body(child).output_declaration().state(),
            DeclarationState::Unit
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        chain,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let choose = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let step = builder.borrow().step_count(root);
        let outputs = builder
            .borrow_mut()
            .allocate_choose_outputs(root, step, &[]);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), child);
            flow.output::<()>();
            // 第二次声明必须被拒绝：保留原有的 DuplicateDeclaration 目标。
            flow.output::<()>();
        }
        builder
            .borrow_mut()
            .register_choose_branch(child, step, 0, &outputs);
        assert_eq!(
            builder.borrow().body(child).output_declaration().state(),
            DeclarationState::Unit
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        choose,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let retry = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), child);
            flow.output::<()>();
        }
        let order = builder.borrow_mut().fresh_order();
        assert!(
            !builder
                .borrow_mut()
                .declare_output(child, Vec::new(), order)
        );
        builder.borrow_mut().register_retry(root, child, 1, None);
        assert_eq!(
            builder.borrow().body(child).output_declaration().state(),
            DeclarationState::Unit
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        retry,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let each = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Vec<Amount>>()]);
        let collection = desc(inputs[0], TypeDesc::of::<Vec<Amount>>());
        let item = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let item_position =
            builder
                .borrow_mut()
                .allocate_each_item(item, 0, TypeDesc::of::<Amount>());
        {
            let mut flow = Flow::open(Rc::clone(&builder), item);
            flow.output::<()>();
            // 第二次声明必须被拒绝：保留原有的 DuplicateDeclaration 目标。
            flow.output::<()>();
        }
        builder.borrow_mut().register_each(
            root,
            item,
            0,
            collection,
            item_position,
            None,
            super::shape::each_bind_item::<Amount>,
            super::shape::each_collection_len::<Amount>,
        );
        assert_eq!(
            builder.borrow().body(item).output_declaration().state(),
            DeclarationState::Unit
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        each,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let iter = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), round);
            flow.next_with(amount(inputs[0]));
        }
        builder.borrow_mut().register_iter(
            root,
            round,
            0,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            Vec::new(),
            1,
            <Ref<Amount> as StateShape>::register_states,
            <Ref<Amount> as StateShape>::state_import_slots,
        );
        assert_eq!(
            builder.borrow().body(round).next_declaration().state(),
            DeclarationState::NonUnit
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        iter,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));

    let first_failure = {
        let (builder, root, _) = builder_with_inputs(&[]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output::<Missing>();
        }
        let order = builder.borrow_mut().fresh_order();
        assert!(!builder.borrow_mut().declare_output(root, Vec::new(), order));
        assert_eq!(
            builder.borrow().body(root).output_declaration().state(),
            DeclarationState::Failed
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        first_failure,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    let non_unit = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output::<Amount>();
        }
        let order = builder.borrow_mut().fresh_order();
        assert!(!builder.borrow_mut().declare_output(
            root,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            order
        ));
        assert_eq!(
            builder.borrow().body(root).output_declaration().state(),
            DeclarationState::NonUnit
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        non_unit,
        Some(BuildErrorDetail::DuplicateDeclaration { .. })
    ));
}

#[test]
fn a12_real_build_definition_rejects_internal_output_then_old_return() {
    let error = build_definition::<(), (), _>(|flow, ()| {
        flow.output::<()>();
        let () = ();
        flow.output_with(());
    })
    .err()
    .expect("real root return must not silently replace the internal declaration");
    assert!(
        matches!(error, BuildErrorDetail::DuplicateDeclaration { .. }),
        "{error}"
    );
}

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
        let outputs = builder.borrow_mut().register_iter(
            root,
            round,
            0,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            Vec::new(),
            1,
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
        let outputs = builder.borrow_mut().register_iter(
            root,
            round,
            0,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            Vec::new(),
            1,
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

fn tainted_amount(builder: &Rc<RefCell<Builder<'static>>>, root: BodyId) -> PositionHandle {
    let mut flow = Flow::open(Rc::clone(builder), root);
    let bad = flow.ref_of::<Missing>();
    flow.then_with_output(missing_to_amount, bad).handle()
}

fn tainted_vec(builder: &Rc<RefCell<Builder<'static>>>, root: BodyId) -> PositionHandle {
    let mut flow = Flow::open(Rc::clone(builder), root);
    let bad = flow.ref_of::<Missing>();
    flow.then_with_output(missing_to_vec, bad).handle()
}

async fn missing_to_vec(query: Query<&Missing>) -> Result<Vec<Amount>, BodyError> {
    let _ = query.get();
    Ok(Vec::new())
}

#[test]
fn a13b_tainted_and_unpublished_sources_do_not_publish_outputs() {
    let chain = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let tainted = tainted_amount(&builder, root);
        let order = builder.borrow_mut().fresh_order();
        let _ = builder.borrow_mut().declare_output(
            child,
            vec![desc(tainted, TypeDesc::of::<Amount>())],
            order,
        );
        let (outputs, _declared) = builder
            .borrow_mut()
            .register_chain_outputs(root, child, None);
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("chain output")
                .is_tainted()
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        chain,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    let choose = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let step = builder.borrow().step_count(root);
        let outputs =
            builder
                .borrow_mut()
                .allocate_choose_outputs(root, step, &[TypeDesc::of::<Amount>()]);
        let branch = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let tainted = tainted_amount(&builder, root);
        {
            let mut flow = Flow::open(Rc::clone(&builder), branch);
            flow.output_with(amount(tainted));
        }
        builder
            .borrow_mut()
            .register_choose_branch(branch, step, 0, &outputs);
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("choose output")
                .is_tainted()
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        choose,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    let retry = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let tainted = tainted_amount(&builder, root);
        let order = builder.borrow_mut().fresh_order();
        let _ = builder.borrow_mut().declare_output(
            child,
            vec![desc(tainted, TypeDesc::of::<Amount>())],
            order,
        );
        let (outputs, _declared) = builder.borrow_mut().register_retry(root, child, 1, None);
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("retry output")
                .is_tainted()
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        retry,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    let iter = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        let current =
            builder
                .borrow_mut()
                .allocate_iter_current(round, 0, &[TypeDesc::of::<Amount>()]);
        let tainted = tainted_amount(&builder, root);
        let outputs = builder.borrow_mut().register_iter(
            root,
            round,
            0,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            vec![desc(tainted, TypeDesc::of::<Amount>())],
            current,
            1,
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
        finish_builder(builder, root).err()
    };
    assert!(matches!(iter, Some(BuildErrorDetail::MissingSource { .. })));

    let each = {
        let (builder, root, inputs) =
            builder_with_inputs(&[TypeDesc::of::<Vec<Amount>>(), TypeDesc::of::<Amount>()]);
        let item = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let item_position =
            builder
                .borrow_mut()
                .allocate_each_item(item, 0, TypeDesc::of::<Amount>());
        let tainted = tainted_vec(&builder, root);
        {
            let mut flow = Flow::open(Rc::clone(&builder), item);
            flow.output_with(amount(inputs[1]));
        }
        let outputs = builder.borrow_mut().register_each(
            root,
            item,
            0,
            desc(tainted, TypeDesc::of::<Vec<Amount>>()),
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
        finish_builder(builder, root).err()
    };
    assert!(matches!(each, Some(BuildErrorDetail::MissingSource { .. })));
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
fn a13b_first_unpublished_declaration_survives_old_return() {
    let output = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let source = unpublished_amount(&builder, root);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output_with(amount(source));
        }
        let before = output_snapshot(&builder, root);
        assert_eq!(before.0, DeclarationState::Failed);
        let old_order = builder.borrow_mut().fresh_order();
        assert!(!builder.borrow_mut().declare_output(root, vec![], old_order));
        assert_eq!(output_snapshot(&builder, root), before);
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(output, Some(BuildErrorDetail::NotPublished { .. })),
        "{output:?}"
    );

    let next = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let step = builder.borrow().step_count(root);
        let source = unpublished_amount(&builder, root);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), round);
            flow.next_with(amount(source));
        }
        let before = next_snapshot(&builder, round);
        assert_eq!(before.0, DeclarationState::Failed);
        builder.borrow_mut().register_iter(
            root,
            round,
            step,
            Vec::new(),
            vec![desc(source, TypeDesc::of::<Amount>())],
            Vec::new(),
            1,
            <Ref<Amount> as StateShape>::register_states,
            <Ref<Amount> as StateShape>::state_import_slots,
        );
        assert_eq!(next_snapshot(&builder, round), before);
        finish_builder(builder, root).err()
    };
    assert!(
        matches!(next, Some(BuildErrorDetail::NotPublished { .. })),
        "{next:?}"
    );
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

#[test]
fn a13b1_unpublished_source_is_rejected_by_all_register_paths() {
    let chain = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let source = unpublished_amount(&builder, root);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let order = builder.borrow_mut().fresh_order();
        let _ = builder.borrow_mut().declare_output(
            child,
            vec![desc(source, TypeDesc::of::<Amount>())],
            order,
        );
        let (outputs, _declared) = builder
            .borrow_mut()
            .register_chain_outputs(root, child, None);
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("chain output")
                .is_tainted()
        );
        assert!(
            builder
                .borrow()
                .body(root)
                .steps()
                .last()
                .and_then(|step| step.chain())
                .expect("chain step")
                .slots()
                .is_empty()
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(chain, Some(BuildErrorDetail::NotPublished { .. })));

    let choose = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let step = builder.borrow().step_count(root);
        let outputs =
            builder
                .borrow_mut()
                .allocate_choose_outputs(root, step, &[TypeDesc::of::<Amount>()]);
        let source = outputs[0];
        let branch = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), branch);
            // 显式声明一个尚未发布的父输出：首因 NotPublished。
            flow.output_with(amount(source));
        }
        let branch = builder
            .borrow_mut()
            .register_choose_branch(branch, step, 0, &outputs);
        assert!(branch.slots.is_empty());
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("choose output")
                .is_tainted()
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        choose,
        Some(BuildErrorDetail::NotPublished { .. })
    ));

    let retry = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let source = unpublished_amount(&builder, root);
        let child = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let order = builder.borrow_mut().fresh_order();
        let _ = builder.borrow_mut().declare_output(
            child,
            vec![desc(source, TypeDesc::of::<Amount>())],
            order,
        );
        let (outputs, _declared) = builder.borrow_mut().register_retry(root, child, 1, None);
        assert!(
            builder
                .borrow()
                .position(outputs[0])
                .expect("retry output")
                .is_tainted()
        );
        assert!(
            builder
                .borrow()
                .body(root)
                .steps()
                .last()
                .and_then(|step| step.retry())
                .expect("retry step")
                .slots()
                .is_empty()
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(retry, Some(BuildErrorDetail::NotPublished { .. })));

    let iter = {
        let (builder, root, inputs) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        let source = unpublished_amount(&builder, root);
        let round = builder.borrow_mut().new_body(Some(root), BodyRole::Round);
        builder
            .borrow_mut()
            .set_round_initial(round, vec![TypeDesc::of::<Amount>()]);
        let current =
            builder
                .borrow_mut()
                .allocate_iter_current(round, 0, &[TypeDesc::of::<Amount>()]);
        let outputs = builder.borrow_mut().register_iter(
            root,
            round,
            0,
            vec![desc(inputs[0], TypeDesc::of::<Amount>())],
            vec![desc(source, TypeDesc::of::<Amount>())],
            current,
            1,
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
        assert!(
            builder
                .borrow()
                .body(root)
                .steps()
                .last()
                .and_then(|step| step.iter())
                .expect("iter step")
                .slots()
                .is_empty()
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(iter, Some(BuildErrorDetail::NotPublished { .. })));

    let each = {
        let (builder, root, inputs) =
            builder_with_inputs(&[TypeDesc::of::<Vec<Amount>>(), TypeDesc::of::<Amount>()]);
        let source = unpublished_amount(&builder, root);
        let item = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let item_position =
            builder
                .borrow_mut()
                .allocate_each_item(item, 0, TypeDesc::of::<Amount>());
        {
            let mut flow = Flow::open(Rc::clone(&builder), item);
            // 显式声明一个尚未发布的位置：首因 NotPublished。
            flow.output_with(amount(source));
        }
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
        assert!(
            builder
                .borrow()
                .body(root)
                .steps()
                .last()
                .and_then(|step| step.each())
                .expect("each step")
                .slots()
                .is_empty()
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(each, Some(BuildErrorDetail::NotPublished { .. })));
}

#[test]
fn a13b1_tainted_real_output_reaches_explicit_paths() {
    let explicit_node = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let tainted = tainted_amount(&builder, root);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            let _ = flow.then_with_output(add_one, amount(tainted));
        }
        assert!(
            builder
                .borrow()
                .errors()
                .iter()
                .any(|error| matches!(error, BuildErrorDetail::TaintedSource { .. }))
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        explicit_node,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    let item_declaration = {
        let (builder, root, _) = builder_with_inputs(&[]);
        let tainted = tainted_amount(&builder, root);
        let item = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        {
            let mut flow = Flow::open(Rc::clone(&builder), item);
            flow.output_with(amount(tainted));
        }
        let declaration = builder.borrow().body(item).output_declaration().clone();
        assert_eq!(declaration.state(), DeclarationState::Failed);
        assert!(matches!(
            declaration.first_failure,
            Some(BuildErrorDetail::TaintedSource { .. })
        ));
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        item_declaration,
        Some(BuildErrorDetail::MissingSource { .. })
    ));
}

// ---------------------------------------------------------------- A14 声明失败

#[test]
fn a14_partial_auto_declaration_and_later_sources_do_not_repair_failures() {
    let partial = {
        let (builder, root, _) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output::<(Amount, Missing)>();
        }
        assert_eq!(
            builder.borrow().body(root).output_declaration().state(),
            DeclarationState::Failed
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        partial,
        Some(BuildErrorDetail::MissingSource { .. })
    ));

    let invalid_explicit = {
        let (builder, root, _) = builder_with_inputs(&[]);
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            flow.output_with(amount(PositionHandle {
                definition: DefinitionId::fresh(),
                position: PositionId::new(0),
            }));
            flow.output::<()>();
        }
        assert_eq!(
            builder.borrow().body(root).output_declaration().state(),
            DeclarationState::Failed
        );
        finish_builder(builder, root).err()
    };
    assert!(matches!(
        invalid_explicit,
        Some(BuildErrorDetail::ForeignDefinition { .. })
    ));

    let later_source = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        let bad = flow.ref_of::<Missing>();
        let _ = flow.ref_of::<Amount>();
        let _ = flow.then_with_output(sink_missing, bad);
        let out_refs = amount_ref;
        flow.output_with(out_refs);
    })
    .err()
    .expect("a later legal source must not revive the failed handle");
    assert!(
        matches!(later_source, BuildErrorDetail::MissingSource { .. }),
        "{later_source}"
    );
}

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

#[test]
fn a16_old_chain_path_still_builds() {
    let definition = build_definition::<Amount, Amount, _>(|flow, amount_ref| {
        let chained: Ref<Amount> = flow.chain_output(|sub| {
            let out_refs = sub.then_with_output(add_one, amount_ref);
            sub.output_with(out_refs);
        });
        flow.then_with_output(add_one, chained);
        let out_refs = chained;
        flow.output_with(out_refs);
    })
    .expect("old chain path should keep working");
    assert_eq!(definition.root().steps().len(), 2);
}
