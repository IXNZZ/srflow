use std::collections::HashSet;
use std::sync::Arc;

use super::{RefId, RefIdAllocator, RefIdSource};
use crate::core::internal_error::{IdKind, InternalError};

#[test]
fn b02_ref_ids_from_one_source_continue_the_original_sequence() {
    let source = RefIdSource::new();
    let first = RefIdAllocator::new(Arc::clone(&source));

    // 先分配，再克隆来源构造第二个句柄。
    let a = first.allocate().unwrap();
    let second = RefIdAllocator::new(Arc::clone(&source));
    let b = second.allocate().unwrap();
    let c = first.allocate().unwrap();

    assert_eq!((a.seq(), b.seq(), c.seq()), (0, 1, 2));
    assert_ne!(a, b);
    assert_ne!(b, c);
    assert_ne!(a, c);
}

#[test]
#[allow(clippy::mutable_key_type)] // Eq／Hash 只用来源地址与序号，二者不随分配变化
fn b02_ref_ids_from_different_sources_never_match() {
    let first = RefIdAllocator::new(RefIdSource::new());
    let second = RefIdAllocator::new(RefIdSource::new());
    let id_in_first = first.allocate().unwrap();
    let id_in_second = second.allocate().unwrap();

    assert_eq!(id_in_first.seq(), id_in_second.seq());
    assert_ne!(id_in_first, id_in_second);

    let mut set: HashSet<RefId> = HashSet::new();
    set.insert(id_in_first.clone());
    set.insert(id_in_second.clone());
    assert_eq!(set.len(), 2);
    set.insert(id_in_first.clone());
    assert_eq!(set.len(), 2);
}

#[test]
fn b02_ref_id_exhaustion_is_checked_and_does_not_wrap() {
    let source = RefIdSource::with_start(u64::MAX - 1);
    let allocator = RefIdAllocator::new(source);

    let last = allocator.allocate().unwrap();
    assert_eq!(last.seq(), u64::MAX - 1);

    assert!(matches!(
        allocator.allocate(),
        Err(InternalError::IdSpaceExhausted { .. })
    ));
    // 耗尽后持续拒绝，且诊断带 RefId 类别。
    match allocator.allocate() {
        Err(error) => assert!(error.to_string().contains("RefId")),
        Ok(id) => panic!("exhausted source handed out {id}"),
    }
}

/// 由旧 `signature::tests::v21_05_batch_allocation_is_atomic_on_exhaustion` 迁移：
/// 保留不变量“整组分配耗尽时原子失败——不消耗任何序号、不留部分分配”，改用
/// `RefIdAllocator::allocate_batch` 的真实入口（旧 `Out2<..>::allocate` 封套已退出）。
#[test]
fn v21_05_batch_allocation_is_atomic_on_exhaustion() {
    let source = RefIdSource::with_start(u64::MAX - 1);
    let allocator = RefIdAllocator::new(source);
    // 只剩一个序号时一次取 2 个：整组失败。
    let error = allocator.allocate_batch(2).unwrap_err();
    assert!(matches!(
        error,
        InternalError::IdSpaceExhausted { kind: IdKind::Ref }
    ));
    // 整组失败不消耗序号：仍然只有一个序号可用。
    assert_eq!(allocator.next_probe(), u64::MAX - 1);
    assert_eq!(allocator.allocate_batch(1).unwrap()[0].seq(), u64::MAX - 1);
    assert!(allocator.allocate_batch(1).is_err());
}
