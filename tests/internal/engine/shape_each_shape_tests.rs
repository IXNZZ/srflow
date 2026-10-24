use super::*;
use crate::core::engine::identity::{DefinitionId, PositionId};

struct Alpha;
impl crate::core::engine::data::Data for Alpha {}

struct Beta;
impl crate::core::engine::data::Data for Beta {}

fn handle(index: u32) -> PositionHandle {
    PositionHandle {
        definition: DefinitionId::fresh(),
        position: PositionId::new(index),
    }
}

#[test]
fn e01_unit_maps_to_unit_with_zero_slots() {
    assert!(<<() as EachShape>::Collected as ShapeOps>::type_descs().is_empty());
    let mut none = std::iter::empty();
    <<() as EachShape>::Collected as ShapeOps>::from_positions(&mut none);
}

#[test]
fn e02_single_ref_maps_to_vec_ref() {
    let descs = <<Ref<Alpha> as EachShape>::Collected as ShapeOps>::type_descs();
    assert_eq!(descs, vec![TypeDesc::of::<Vec<Alpha>>()]);
    let mut two = [handle(0), handle(1)].into_iter();
    let collected = <<Ref<Alpha> as EachShape>::Collected as ShapeOps>::from_positions(&mut two);
    assert_eq!(collected.handle().position, PositionId::new(0));
}

#[test]
fn e03_flat_tuple_maps_slot_by_slot_without_merging() {
    type Body = (Ref<Alpha>, Ref<Beta>);
    let descs = <<Body as EachShape>::Collected as ShapeOps>::type_descs();
    assert_eq!(
        descs,
        vec![TypeDesc::of::<Vec<Alpha>>(), TypeDesc::of::<Vec<Beta>>()]
    );
    let mut handles = [handle(0), handle(1)].into_iter();
    let (first, second) =
        <<Body as EachShape>::Collected as ShapeOps>::from_positions(&mut handles);
    assert_eq!(first.handle().position, PositionId::new(0));
    assert_eq!(second.handle().position, PositionId::new(1));
}

#[test]
fn e04_tuples_preserve_position_identity() {
    // Same element type in two slots still yields two independent positions.
    type Body = (Ref<Alpha>, Ref<Alpha>);
    let mut handles = [handle(7), handle(8)].into_iter();
    let (first, second) =
        <<Body as EachShape>::Collected as ShapeOps>::from_positions(&mut handles);
    assert_ne!(first.handle().position, second.handle().position);
}
