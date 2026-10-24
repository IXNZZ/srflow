//! WE-T05 静态层定向测试：Each 的统一 Step 登记、formal item 来源与输出映射。
//!
//! 这些测试只做构建期登记与验证，不执行任何业务 Node。
use super::*;
use crate::core::engine::data::Data;
use crate::core::engine::error::{BodyError, BuildErrorDetail, Visibility};
use crate::core::engine::flow::Flow;
use crate::core::engine::shape::{Query, Ref};

struct Item(u32);
impl Data for Item {}

struct Score(u32);
impl Data for Score {}

async fn score_of(query: Query<&Item>) -> Result<Score, BodyError> {
    Ok(Score(query.get().0))
}

async fn double_score(query: Query<&Score>) -> Result<Score, BodyError> {
    Ok(Score(query.get().0 * 2))
}

#[test]
fn t03_each_is_a_unified_ordered_step_with_declared_slots() {
    let definition = build_definition::<Vec<Item>, (Vec<Score>, Vec<Score>), _>(|flow, items| {
        let out_refs: (Ref<Vec<Score>>, Ref<Vec<Score>>) = flow.each_output(items, |each| {
            let item = each.ref_of::<Item>();

            let score = each.then_with_output(score_of, item);
            let doubled = each.then_with_output(double_score, score);
            let out_refs = (score, doubled);
            each.output_with(out_refs);
        });
        flow.output_with(out_refs);
    })
    .expect("each with a flat tuple body should build");

    let root = definition.root();
    assert_eq!(root.steps().len(), 1);
    assert_eq!(root.steps()[0].kind(), StepKind::Each);
    assert_eq!(
        root.steps()[0].inputs().len(),
        1,
        "the collection is the single static input"
    );
    assert_eq!(root.steps()[0].outputs().len(), 2, "two collected slots");

    let step = root.steps()[0].each().expect("each step view");
    let item_body = definition.body(step.item());
    assert_eq!(item_body.parent(), Some(root.id()));
    assert_eq!(
        item_body.steps().len(),
        2,
        "the full item body is registered"
    );
    assert_eq!(step.slots().len(), 2);

    // formal item: one logical record, owned by the item body, typed as the element.
    let item_records: Vec<_> = definition
        .positions()
        .iter()
        .filter(|record| matches!(record.source(), PositionSource::EachItem { .. }))
        .collect();
    assert_eq!(item_records.len(), 1);
    assert_eq!(item_records[0].owner(), item_body.id());
    assert_eq!(item_records[0].type_desc(), TypeDesc::of::<Item>());
    assert_eq!(item_records[0].handle().position, step.item_slot());

    // collected slots: independent Vec<Score> positions.
    let outputs: Vec<_> = definition
        .positions()
        .iter()
        .filter(|record| matches!(record.source(), PositionSource::EachOutput { .. }))
        .collect();
    assert_eq!(outputs.len(), 2);
    assert!(
        outputs
            .iter()
            .all(|record| record.type_desc() == TypeDesc::of::<Vec<Score>>())
    );
    assert_ne!(outputs[0].core(), outputs[1].core());
    assert!(
        step.slots()
            .iter()
            .all(|slot| slot.collected_type == TypeDesc::of::<Vec<Score>>())
    );
}

#[test]
fn t11_unit_each_registers_no_collector_slots() {
    let definition = build_definition::<Vec<Item>, (), _>(|flow, items| {
        let () = flow.each_output(items, |each| {
            let item = each.ref_of::<Item>();

            let _score = each.then_with_output(score_of, item);
            each.output::<()>();
        });
        flow.output_with(());
    })
    .expect("unit each should build");

    let root = definition.root();
    assert_eq!(root.steps()[0].kind(), StepKind::Each);
    assert!(root.steps()[0].outputs().is_empty());
    let step = root.steps()[0].each().expect("each step view");
    assert!(step.slots().is_empty());
    assert!(!definition.positions().iter().any(|record| matches!(
        record.source(),
        PositionSource::EachLocal { .. } | PositionSource::EachOutput { .. }
    )));
}

#[test]
fn t04_formal_item_is_child_local_and_rejected_in_the_parent() {
    // Forge a parent use of the item position: the universal validate_use must reject it
    // with OutOfScope(ChildLocal) before any Node runs.
    let builder = Rc::new(RefCell::new(Builder::<'static>::new()));
    let root = builder.borrow_mut().new_body(None, BodyRole::Ordinary);
    let item_body = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    let item_handle = builder
        .borrow_mut()
        .allocate_each_item(item_body, 0, TypeDesc::of::<Item>());
    {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        let leaked = Ref::<Item>::from_handle(item_handle);
        let _ = flow.then_with_output(score_of, leaked);
    }
    let state = Rc::try_unwrap(builder)
        .ok()
        .expect("definition closure does not retain the internal Flow")
        .into_inner();
    let error = match state.finish(root) {
        Ok(_) => panic!("parent use of the formal item must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        BuildErrorDetail::OutOfScope {
            relation: Visibility::ChildLocal,
            owner,
            ..
        } if owner == item_body
    ));
}
