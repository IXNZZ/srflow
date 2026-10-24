use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::DataContainer;
use crate::core::identity::{CollectorIdAllocator, ExecutionIdentity};
use crate::core::internal_error::InternalError;

/// 另一类型，用于类型身份与诊断的区分。
struct Other;

/// 非 Clone 业务值，带 Drop 观测。
struct Tracked {
    num: u32,
    drops: Arc<AtomicUsize>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 另一个类型，用于类型不符场景。
struct Plain(u32);

fn container() -> DataContainer {
    DataContainer::with_identity(ExecutionIdentity::new())
}

fn tracked(num: u32, drops: &Arc<AtomicUsize>) -> Tracked {
    Tracked {
        num,
        drops: Arc::clone(drops),
    }
}

#[test]
fn a01_entries_are_addressable_by_data_id_not_by_type() {
    let mut container = container();
    let drops = Arc::new(AtomicUsize::new(0));

    let first = container.insert_owned(tracked(1, &drops)).unwrap();
    let second = container.insert_owned(tracked(2, &drops)).unwrap();
    let text = container.insert_owned(String::from("hello")).unwrap();
    let number = container.insert_owned(7u64).unwrap();

    assert_eq!(container.borrow::<Tracked>(&first).unwrap().num, 1);
    assert_eq!(container.borrow::<Tracked>(&second).unwrap().num, 2);
    assert_eq!(container.borrow::<String>(&text).unwrap(), "hello");
    assert_eq!(*container.borrow::<u64>(&number).unwrap(), 7);

    // 同类型的两个实例身份互不相同，不能按类型猜实例。
    assert_ne!(first, second);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}

#[test]
fn a02_non_clone_values_share_read_borrows_without_copying() {
    let mut container = container();
    let drops = Arc::new(AtomicUsize::new(0));

    let first = container.insert_owned(tracked(5, &drops)).unwrap();
    let second = container.insert_owned(tracked(6, &drops)).unwrap();

    let repeated = container.borrow::<Tracked>(&first).unwrap();
    let again = container.borrow::<Tracked>(&first).unwrap();
    let other = container.borrow::<Tracked>(&second).unwrap();

    // 同一实例的重复借用指向同一地址；不同实例互不相同。
    assert!(std::ptr::eq(repeated, again));
    assert!(!std::ptr::eq(repeated, other));
    assert_eq!((repeated.num, again.num, other.num), (5, 5, 6));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}

#[test]
fn a03_foreign_ids_are_rejected_in_every_entry_point() {
    let mut first_execution = container();
    let mut second_execution = container();
    let drops = Arc::new(AtomicUsize::new(0));

    let id_in_first = first_execution.insert_owned(tracked(1, &drops)).unwrap();
    let id_in_second = second_execution.insert_owned(tracked(2, &drops)).unwrap();

    // 本地序号碰撞：两份 Execution 的首个 DataId 序号相同。
    assert_eq!(id_in_first.seq(), id_in_second.seq());
    assert_ne!(id_in_first, id_in_second);

    assert!(matches!(
        second_execution.validate(&id_in_first),
        Err(InternalError::ForeignExecution { .. })
    ));
    assert!(matches!(
        second_execution.borrow::<Tracked>(&id_in_first),
        Err(InternalError::ForeignExecution { .. })
    ));
    assert!(matches!(
        second_execution.remove_owned::<Tracked>(&id_in_first),
        Err(InternalError::ForeignExecution { .. })
    ));
    assert!(matches!(
        second_execution.destroy(&id_in_first),
        Err(InternalError::ForeignExecution { .. })
    ));

    // 两个 Execution 各自的数据都保持有效。
    assert_eq!(
        first_execution.borrow::<Tracked>(&id_in_first).unwrap().num,
        1
    );
    assert_eq!(
        second_execution
            .borrow::<Tracked>(&id_in_second)
            .unwrap()
            .num,
        2
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}

#[test]
fn a05_removed_ids_are_never_reissued_and_stay_invalid() {
    let mut container = container();
    let old = container.insert_owned(Plain(1)).unwrap();

    let value = container.remove_owned::<Plain>(&old).unwrap();
    assert_eq!(value.0, 1);

    let new = container.insert_owned(Plain(2)).unwrap();
    assert_ne!(new, old);
    assert_eq!(new.seq(), old.seq() + 1);

    assert!(matches!(
        container.borrow::<Plain>(&old),
        Err(InternalError::DataNotFound { .. })
    ));
    assert!(matches!(
        container.remove_owned::<Plain>(&old),
        Err(InternalError::DataNotFound { .. })
    ));
    assert!(matches!(
        container.destroy(&old),
        Err(InternalError::DataNotFound { .. })
    ));
    assert_eq!(container.borrow::<Plain>(&new).unwrap().0, 2);
}

#[test]
fn a07_type_mismatch_is_rejected_before_any_move() {
    let mut container = container();
    let drops = Arc::new(AtomicUsize::new(0));
    let id = container.insert_owned(tracked(9, &drops)).unwrap();

    assert!(matches!(
        container.borrow::<Plain>(&id),
        Err(InternalError::TypeMismatch { .. })
    ));
    assert!(matches!(
        container.remove_owned::<Plain>(&id),
        Err(InternalError::TypeMismatch { .. })
    ));
    // 原 entry 与 ID 保持有效，值没有被移出或销毁。
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(container.borrow::<Tracked>(&id).unwrap().num, 9);

    // 诊断带预期／实际类型。
    match container.borrow::<Plain>(&id) {
        Err(error) => {
            let text = error.to_string();
            assert!(text.contains("Tracked"), "{text}");
            assert!(text.contains("Plain"), "{text}");
        }
        Ok(_) => panic!("wrong type was accepted"),
    }
}

#[test]
fn a08_successful_removal_transfers_the_value_and_invalidates_the_id() {
    let mut container = container();
    let drops = Arc::new(AtomicUsize::new(0));
    let id = container.insert_owned(tracked(3, &drops)).unwrap();

    let taken = container.remove_owned::<Tracked>(&id).unwrap();
    assert_eq!(taken.num, 3);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    drop(taken);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        container.borrow::<Tracked>(&id),
        Err(InternalError::DataNotFound { .. })
    ));

    drop(container);
    // 已移出的值不会在容器结束时重复 drop。
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a09_destroy_and_teardown_drop_each_value_once() {
    let mut container = container();
    let drops = Arc::new(AtomicUsize::new(0));
    let destroyed = container.insert_owned(tracked(1, &drops)).unwrap();
    let _remaining = container.insert_owned(tracked(2, &drops)).unwrap();

    container.destroy(&destroyed).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    assert!(matches!(
        container.destroy(&destroyed),
        Err(InternalError::DataNotFound { .. })
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    drop(container);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn a10_a_retained_id_neither_keeps_the_value_alive_nor_revives_it() {
    let mut container = container();
    let drops = Arc::new(AtomicUsize::new(0));
    let id = container.insert_owned(tracked(1, &drops)).unwrap();
    let retained = id.clone();

    container.destroy(&id).unwrap();
    // ID 只保留身份元数据，不保活业务值。
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        container.borrow::<Tracked>(&retained),
        Err(InternalError::DataNotFound { .. })
    ));

    drop(container);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a11_data_id_exhaustion_is_checked_and_leaves_existing_values_intact() {
    let execution = ExecutionIdentity::with_starts(u64::MAX - 1, 0);
    let mut container = DataContainer::with_identity(execution);

    let last = container.insert_owned(Plain(1)).unwrap();
    assert_eq!(last.seq(), u64::MAX - 1);

    assert!(matches!(
        container.insert_owned(Plain(2)),
        Err(InternalError::IdSpaceExhausted { .. })
    ));
    // 耗尽不破坏已存值，也不重新发出旧 ID。
    assert_eq!(container.borrow::<Plain>(&last).unwrap().0, 1);
    let retained = last.clone();
    assert_eq!(container.borrow::<Plain>(&retained).unwrap().0, 1);
    match container.insert_owned(Plain(3)) {
        Err(error) => assert!(error.to_string().contains("DataId")),
        Ok(id) => panic!("exhausted container handed out {id}"),
    }
}

#[test]
fn a12_unit_outputs_are_rejected_without_consuming_an_id() {
    let mut container = container();

    let first = container.insert_owned(Plain(1)).unwrap();
    assert!(matches!(
        container.insert_owned(()),
        Err(InternalError::UnitNotStorable)
    ));
    let second = container.insert_owned(Plain(2)).unwrap();

    // unit 没有生成 DataId，也没有留下 entry。
    assert_eq!(second.seq(), first.seq() + 1);
    assert_eq!(container.borrow::<Plain>(&first).unwrap().0, 1);
    assert_eq!(container.borrow::<Plain>(&second).unwrap().0, 2);
}

#[test]
fn a13_non_send_and_non_sync_values_are_storable() {
    let mut container = container();

    let shared = container.insert_owned(Rc::new(5u32)).unwrap();
    let cell = container.insert_owned(Cell::new(7u32)).unwrap();

    assert_eq!(**container.borrow::<Rc<u32>>(&shared).unwrap(), 5);
    let borrowed_cell = container.borrow::<Cell<u32>>(&cell).unwrap();
    assert_eq!(borrowed_cell.get(), 7);
    borrowed_cell.set(9);
    assert_eq!(container.borrow::<Cell<u32>>(&cell).unwrap().get(), 9);
}

#[test]
fn a14_shortening_the_borrow_allows_mutation() {
    let mut container = container();
    let id = container.insert_owned(Plain(4)).unwrap();

    {
        let borrowed = container.borrow::<Plain>(&id).unwrap();
        assert_eq!(borrowed.0, 4);
    }

    container.destroy(&id).unwrap();
    assert!(matches!(
        container.borrow::<Plain>(&id),
        Err(InternalError::DataNotFound { .. })
    ));
}

#[test]
fn i06_collections_are_single_entries_borrowed_as_a_whole() {
    let mut container = container();
    let collection = container.insert_owned(vec![Plain(1), Plain(2)]).unwrap();

    // 集合作为整体取得一个 DataId。本任务没有 item 调用路径，因此这里只证明完整
    // 集合可按身份借用，不声明 item 的身份规则（那条由 V21-08 证明）。
    let borrowed = container.borrow::<Vec<Plain>>(&collection).unwrap();
    assert_eq!(borrowed.len(), 2);
    assert_eq!(borrowed[1].0, 2);
}

#[test]
fn a16_containers_from_one_identity_share_the_data_id_sequence() {
    let identity = ExecutionIdentity::new();
    let mut first = DataContainer::with_identity(Arc::clone(&identity));
    let from_first = first.insert_owned(Plain(1)).unwrap();

    // 先分配，再克隆身份构造第二个容器。
    let mut second = DataContainer::with_identity(Arc::clone(&identity));
    let from_second = second.insert_owned(Plain(2)).unwrap();

    // 同一身份根只有一条 DataId 序列；第二个容器不会从 0 重新开始。
    assert_eq!((from_first.seq(), from_second.seq()), (0, 1));
    assert_ne!(from_first, from_second);

    // entry 表仍按容器独立：身份相同不赋予存在性。
    assert!(matches!(
        first.borrow::<Plain>(&from_second),
        Err(InternalError::DataNotFound { .. })
    ));
}

/// C12／C14：collector 建构区与移动／完成都在容器内部完成。
#[test]
fn v21_03_collector_area_moves_and_finishes_inside_the_container() {
    let identity = ExecutionIdentity::new();
    let mut container = DataContainer::with_identity(Arc::clone(&identity));
    let ids = CollectorIdAllocator::new(Arc::clone(&identity));
    let collector = ids.allocate().unwrap();

    container.begin_collector::<Tracked>(&collector).unwrap();
    let id = container
        .insert_owned(Tracked {
            num: 7,
            drops: Arc::new(AtomicUsize::new(0)),
        })
        .unwrap();
    assert_eq!(id.seq(), 0);
    assert_eq!(container.collector_len(&collector).unwrap(), 0);

    container.move_into_collector(&collector, &id).unwrap();
    assert_eq!(container.collector_len(&collector).unwrap(), 1);
    assert_eq!(container.collector_moves(&collector).unwrap(), 1);
    assert!(
        container.borrow::<Tracked>(&id).is_err(),
        "old identity is invalid"
    );

    // 完成的 Vec 才获得新的普通 DataId。
    let finished = container.finish_collector(&collector).unwrap();
    assert_eq!(finished.seq(), 1);
    assert_eq!(
        container.borrow::<Vec<Tracked>>(&finished).unwrap().len(),
        1
    );
    assert!(matches!(
        container.collector_len(&collector),
        Err(InternalError::CollectorNotFound { .. })
    ));
}

/// C17 证据：元素类型判定使用真实 `TypeId`，不比较类型名，也不误用 `Box<dyn Any>` 的 TypeId。
#[test]
fn v21_03_element_check_uses_real_type_id_not_the_stored_name() {
    let identity = ExecutionIdentity::new();
    let mut container = DataContainer::with_identity(Arc::clone(&identity));
    let collector = CollectorIdAllocator::new(Arc::clone(&identity))
        .allocate()
        .unwrap();
    container.begin_collector::<Tracked>(&collector).unwrap();

    let id = container.insert_owned(Other).unwrap();
    // test-only 故障：把 entry 的类型名改成与 collector 元素同名，类型身份仍是 `Other`。
    container.entries.get_mut(&id.seq()).unwrap().type_name = std::any::type_name::<Tracked>();

    let error = container.move_into_collector(&collector, &id).unwrap_err();
    assert!(
        matches!(error, InternalError::TypeMismatch { .. }),
        "type identity must win over the recorded name, got {error:?}"
    );
    assert_eq!(container.collector_moves(&collector).unwrap(), 0);
    assert!(
        container.borrow::<Other>(&id).is_ok(),
        "rejected move leaves the entry untouched"
    );
}

/// C13：容器级 collector 身份诊断：`()` 元素、foreign 句柄、清理后不可用。
#[test]
fn v21_03_collector_identity_diagnostics() {
    let identity = ExecutionIdentity::new();
    let mut container = DataContainer::with_identity(Arc::clone(&identity));
    let ids = CollectorIdAllocator::new(Arc::clone(&identity));

    let unit = ids.allocate().unwrap();
    assert!(matches!(
        container.begin_collector::<()>(&unit),
        Err(InternalError::UnitNotCollectible)
    ));

    let foreign = CollectorIdAllocator::new(ExecutionIdentity::new())
        .allocate()
        .unwrap();
    assert!(matches!(
        container.begin_collector::<Tracked>(&foreign),
        Err(InternalError::CollectorForeignExecution { .. })
    ));
    assert!(matches!(
        container.collector_len(&foreign),
        Err(InternalError::CollectorForeignExecution { .. })
    ));

    let collector = ids.allocate().unwrap();
    container.begin_collector::<Tracked>(&collector).unwrap();
    container.destroy_collector(&collector).unwrap();
    assert!(matches!(
        container.collector_len(&collector),
        Err(InternalError::CollectorNotFound { .. })
    ));
    assert!(matches!(
        container.destroy_collector(&collector),
        Err(InternalError::CollectorNotFound { .. })
    ));
}
