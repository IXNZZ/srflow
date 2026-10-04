//! Definition 逻辑位置身份（`RefId`）。
//!
//! `RefId` 表示 Definition 中一个逻辑数据位置的固定身份，与 Execution 的 `DataId`／
//! `ScopeId` 无关：它不携带某次执行的真实数据，也不取自 Execution 的序号序列。
//!
//! **同一来源只有一条 `RefId` 序列**：来源根（[`RefIdSource`]）持有唯一计数器，
//! 分配句柄只是来源根的引用。因此克隆来源后再取得句柄、或先分配再构造新句柄，都只能
//! 续用原序列，不可能另建一条从 0 开始的计数器。完整 `RefId` 由来源身份与本地序号
//! 共同决定，两个独立来源的同序号编号不相等，不会因为"裸整数相同"而碰撞。
//!
//! 来源根只保存 Definition 身份与分配元数据，不持有业务 Data；旧 `RefId` 保留来源根，
//! 因此不会因为地址复用而误指向新来源。

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::internal_error::{IdKind, InternalError};

/// 一个 Definition 分配源的 `RefId` 身份根。
pub(crate) struct RefIdSource {
    next: AtomicU64,
}

#[allow(dead_code)] // 来源根由 Definition 驱动／V21-05 的 Builder 创建，当前只由测试使用
impl RefIdSource {
    /// 创建新的来源根，序列从 0 开始。
    pub(crate) fn new() -> Arc<Self> {
        Self::create(0)
    }

    /// 以指定起点创建**全新**来源根；仅测试构建可见，供 B02 验证 checked 耗尽。
    ///
    /// 起点只在创建新来源时生效，不能改写已使用来源的计数器。
    #[cfg(test)]
    pub(crate) fn with_start(start: u64) -> Arc<Self> {
        Self::create(start)
    }

    fn create(start: u64) -> Arc<Self> {
        Arc::new(Self {
            next: AtomicU64::new(start),
        })
    }

    /// 从本来源的唯一序列取下一个序号；溢出时拒绝，不回绕。
    fn allocate_next(&self) -> Result<u64, InternalError> {
        self.next
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |next| {
                next.checked_add(1)
            })
            .map_err(|_| InternalError::IdSpaceExhausted { kind: IdKind::Ref })
    }
}

impl fmt::Debug for RefIdSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RefIdSource({:p})", std::ptr::from_ref(self))
    }
}

impl fmt::Display for RefIdSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RefIdSource({:p})", std::ptr::from_ref(self))
    }
}

/// Definition 中一个逻辑数据位置的身份。
///
/// 相等与哈希都由来源根的分配地址与本地序号共同决定，不使用来源的字段值；相等 `RefId`
/// 必然产生相同 hash，不同 `RefId` 允许 hash 碰撞，由 `Eq` 区分。
pub(crate) struct RefId {
    source: Arc<RefIdSource>,
    seq: u64,
}

#[allow(dead_code)] // seq 仅供后续 Definition 归属诊断与测试使用
impl RefId {
    /// 仅由 [`RefIdAllocator`] 在模块内调用。
    fn new(source: Arc<RefIdSource>, seq: u64) -> Self {
        Self { source, seq }
    }

    /// 本来源内的本地序号。
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    /// 归属检查：本位置是否由给定来源根分配。
    pub(crate) fn belongs_to(&self, source: &Arc<RefIdSource>) -> bool {
        Arc::ptr_eq(&self.source, source)
    }

    /// 归属检查：两个位置是否来自同一条来源序列（同一次 Definition 构建）。
    ///
    /// 与 [`Self::eq`] 相同地只比较来源分配地址与本地序号，不依赖来源字段值；它只
    /// 说明"同源"，不说明位置已在当前 Definition 登记（后者由构建器声明表回答）。
    pub(crate) fn same_source(&self, other: &Self) -> bool {
        self.seq == other.seq && Arc::ptr_eq(&self.source, &other.source)
    }
}

impl Clone for RefId {
    fn clone(&self) -> Self {
        Self {
            source: Arc::clone(&self.source),
            seq: self.seq,
        }
    }
}

impl PartialEq for RefId {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq && Arc::ptr_eq(&self.source, &other.source)
    }
}

impl Eq for RefId {}

impl Hash for RefId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.seq.hash(state);
        Arc::as_ptr(&self.source).addr().hash(state);
    }
}

impl fmt::Debug for RefId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for RefId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RefId({}, seq = {})", self.source, self.seq)
    }
}

/// `RefId` 分配句柄。
///
/// 句柄只是来源根引用：序号位于来源根内部，因此同一来源派生出的任意多个句柄都取同一条
/// 序列，重复取得不会重启编号。
pub(crate) struct RefIdAllocator {
    source: Arc<RefIdSource>,
}

#[allow(dead_code)] // RefId 由 Definition 驱动／V21-05 的 Builder 创建，当前只由测试使用
impl RefIdAllocator {
    /// 以给定来源根建立分配句柄。
    pub(crate) fn new(source: Arc<RefIdSource>) -> Self {
        Self { source }
    }

    /// 从本来源的唯一序列取下一个 `RefId`。
    pub(crate) fn allocate(&self) -> Result<RefId, InternalError> {
        let seq = self.source.allocate_next()?;
        Ok(RefId::new(Arc::clone(&self.source), seq))
    }

    /// 从同一来源序列一次取 `count` 个连续 `RefId`（checked 整组分配）。
    ///
    /// 序列在同一次原子更新中推进 `count` 位：任何一个序号会溢出时整组失败，且**不
    /// 消耗任何序号、不留下部分分配**。`count == 0` 不接触序列。调用方据此先完成全部
    /// 可恢复预检、再整组提交（V21-05 的 0／1／2 输出位置分配）。
    pub(crate) fn allocate_batch(&self, count: u64) -> Result<Vec<RefId>, InternalError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let start = self
            .source
            .next
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |next| {
                next.checked_add(count)
            })
            .map_err(|_| InternalError::IdSpaceExhausted { kind: IdKind::Ref })?;
        let mut batch = Vec::with_capacity(count as usize);
        for offset in 0..count {
            batch.push(RefId::new(Arc::clone(&self.source), start + offset));
        }
        Ok(batch)
    }

    /// 测试观测：本来源序列尚未分配的下一个序号（不改动序列）。
    #[cfg(test)]
    pub(crate) fn next_probe(&self) -> u64 {
        self.source.next.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::{RefId, RefIdAllocator, RefIdSource};
    use crate::core::internal_error::InternalError;

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

    #[test]
    fn ref_id_carries_no_business_value() {
        // 来源根只保存分配元数据；RefId 的大小与业务类型无关。
        let source = RefIdSource::new();
        let allocator = RefIdAllocator::new(source);
        let id = allocator.allocate().unwrap();
        assert_eq!(id.seq(), 0);
        assert!(std::mem::size_of::<RefId>() <= 4 * std::mem::size_of::<usize>());
    }
}
