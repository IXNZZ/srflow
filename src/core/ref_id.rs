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

impl RefIdSource {
    /// 创建新的来源根，序列从 0 开始。
    pub(crate) fn new() -> Arc<Self> {
        Self::create(0)
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

impl RefId {
    /// 仅由 [`RefIdAllocator`] 在模块内调用。
    fn new(source: Arc<RefIdSource>, seq: u64) -> Self {
        Self { source, seq }
    }

    /// 本来源内的本地序号。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    /// 归属检查：两个位置是否来自同一条来源序列（同一次 Definition 构建）。
    ///
    /// 与 [`Self::eq`] 相同地只比较来源分配地址与本地序号，不依赖来源字段值；它只
    /// 说明"同源"，不说明位置已在当前 Definition 登记（后者由构建器声明表回答）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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
}

#[cfg(test)]
#[path = "../../tests/internal/core/ref_id_observe.rs"]
pub(crate) mod observe;
#[cfg(test)]
#[path = "../../tests/internal/core/ref_id_tests.rs"]
mod tests;
