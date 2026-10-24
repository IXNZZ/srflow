use std::collections::HashSet;
use std::sync::Arc;

use super::{
    CollectorId, CollectorIdAllocator, DataId, DataIdAllocator, ExecutionIdentity, ScopeIdAllocator,
};
use crate::core::internal_error::InternalError;

#[test]
// `DataId` 持有 `Arc<ExecutionIdentity>`，身份根内部有序号原子量，因此 clippy 会
// 把 `DataId` 视为含内部可变性的 key。这里 hash 与 eq 只用身份根地址与本地序号，
// 二者都不随分配变化，集合语义安全；A03 要求的正是这一验证。
#[allow(clippy::mutable_key_type)]
fn data_ids_compare_and_hash_by_execution_identity_not_by_value() {
    let first_ids = DataIdAllocator::new(ExecutionIdentity::new());
    let second_ids = DataIdAllocator::new(ExecutionIdentity::new());
    let id_in_first = first_ids.allocate().unwrap();
    let same_id = first_ids.allocate().unwrap();
    let id_in_second = second_ids.allocate().unwrap();

    assert_eq!(id_in_first.seq(), 0);
    assert_ne!(id_in_first, same_id);
    // 不同身份根、序号相同：不相等——这正是 A03 要求防止的碰撞。
    assert_ne!(id_in_first, id_in_second);
    assert_eq!(id_in_first.seq(), id_in_second.seq());

    // Hash 与 Eq 一致：不同 ID 不会因身份相等语义错误而被合并。
    let mut set: HashSet<DataId> = HashSet::new();
    set.insert(id_in_first.clone());
    set.insert(id_in_second.clone());
    assert_eq!(set.len(), 2);
    set.insert(id_in_first.clone());
    assert_eq!(set.len(), 2);
}

#[test]
fn a11_scope_id_exhaustion_is_checked_and_does_not_repeat() {
    let scopes = ScopeIdAllocator::new(ExecutionIdentity::with_starts(0, u64::MAX - 1));

    let last = scopes.allocate().unwrap();
    assert_eq!(last.seq(), u64::MAX - 1);

    assert!(matches!(
        scopes.allocate(),
        Err(InternalError::IdSpaceExhausted { .. })
    ));
    // 耗尽后仍持续拒绝，且诊断带上耗尽的身份类别。
    match scopes.allocate() {
        Err(error) => assert!(error.to_string().contains("ScopeId")),
        Ok(id) => panic!("exhausted allocator handed out {id}"),
    }
}

#[test]
fn a16_data_id_handles_share_one_sequence_per_execution() {
    let identity = ExecutionIdentity::new();
    let first = DataIdAllocator::new(Arc::clone(&identity));

    // 先由 first 取号，再克隆身份并构造第二个句柄。
    let a = first.allocate().unwrap();
    let second = DataIdAllocator::new(Arc::clone(&identity));
    let b = second.allocate().unwrap();
    let c = first.allocate().unwrap();

    // 新句柄续用原序列，没有重发初始序号。
    assert_eq!((a.seq(), b.seq(), c.seq()), (0, 1, 2));
    assert_ne!(a, b);
    assert_ne!(b, c);
    assert_ne!(a, c);
}

#[test]
fn a16_data_and_scope_sequences_are_independent() {
    let identity = ExecutionIdentity::new();
    let data = DataIdAllocator::new(Arc::clone(&identity));
    let first_data = data.allocate().unwrap();

    // 先分配后再构造 Scope 句柄：它取的是独立序列，不受 DataId 进度影响。
    let scopes = ScopeIdAllocator::new(Arc::clone(&identity));
    let first_scope = scopes.allocate().unwrap();
    let second_data = data.allocate().unwrap();
    let second_scope = scopes.allocate().unwrap();

    assert_eq!((first_data.seq(), second_data.seq()), (0, 1));
    assert_eq!((first_scope.seq(), second_scope.seq()), (0, 1));
}

#[test]
fn v21_03_collector_ids_follow_identity_and_share_one_sequence() {
    let identity = ExecutionIdentity::new();
    let first = CollectorIdAllocator::new(Arc::clone(&identity));
    let a = first.allocate().unwrap();
    // 先分配再构造第二个句柄：续用同一序列，不重发初始序号。
    let second = CollectorIdAllocator::new(Arc::clone(&identity));
    let b = second.allocate().unwrap();
    assert_eq!((a.seq(), b.seq()), (0, 1));
    assert_ne!(a, b);

    // 另一次 Execution 的同序号 ID 不相等，也不能互相寻址。
    let other = CollectorIdAllocator::new(ExecutionIdentity::new());
    let foreign = other.allocate().unwrap();
    assert_eq!(foreign.seq(), a.seq());
    assert_ne!(foreign, a);

    // Eq 与 Hash 一致：不同身份不会被合并。
    #[allow(clippy::mutable_key_type)]
    let mut set: HashSet<CollectorId> = HashSet::new();
    set.insert(a.clone());
    set.insert(foreign.clone());
    assert_eq!(set.len(), 2);
    set.insert(a.clone());
    assert_eq!(set.len(), 2);
}

#[test]
fn v21_03_collector_id_exhaustion_is_checked_and_does_not_wrap() {
    let ids = CollectorIdAllocator::new(ExecutionIdentity::with_starts_and_collector_start(
        0,
        0,
        u64::MAX - 1,
    ));
    let last = ids.allocate().unwrap();
    assert_eq!(last.seq(), u64::MAX - 1);
    match ids.allocate() {
        Err(error) => assert!(error.to_string().contains("CollectorId")),
        Ok(id) => panic!("exhausted allocator handed out {id}"),
    }
}
