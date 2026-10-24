//! Execution 身份、`DataId` 与 `ScopeId`。
//!
//! 一次 Execution 的身份由**身份根**（[`ExecutionIdentity`]）与本地单调序号共同决定。
//! 身份根是一次独立分配，身份判据是 `Arc` 指向的地址而非字段值：两个不同 Execution
//! 里序号相同的 ID 既不相等，也不能互相寻址。身份根是引用计数的标记，不持有业务
//! Data；旧 ID 保留身份根，因而不会因为 Execution 结束、内存复用而误配新 Execution。
//!
//! **每个身份根对每种 ID 恰好持有一条序号序列。** 计数器位于身份根内部，因此从同一
//! 身份根克隆、再次构造出的任意多个分配句柄都从同一条序列取号；克隆身份句柄只复制
//! 身份，不能据此重新建立从初始值开始的计数器。ID 只能由绑定到身份根的分配器发出，
//! 其构造函数在模块内私有。
//!
//! `DataId`、`ScopeId` 与 `CollectorId` 是三个不同的内部类型，各有独立的序列：序号只增、
//! 不复用、耗尽时返回 [`InternalError::IdSpaceExhausted`]，不 wrap、不覆盖旧 entry、不重新发出
//! 旧序号。本模块只提供分配与身份比较，不管理 Scope 状态、collector 建构值或生命周期。

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::internal_error::{IdKind, InternalError};

/// 一次 Execution 的身份根，同时持有该 Execution 的 ID 序号序列。
///
/// ExecutionContext 接入后（V21-04）应只创建一次，并把它克隆给 DataContainer 与
/// `ScopeId` 分配器共享；内部 child 调用不得另建身份根。复制身份根句柄只增加引用
/// 计数，不延长任何业务 Data 的生命周期。
pub(crate) struct ExecutionIdentity {
    next_data_id: AtomicU64,
    next_scope_id: AtomicU64,
    next_collector_id: AtomicU64,
}

impl ExecutionIdentity {
    /// 创建一个新的身份根，三条序列都从 0 开始。
    pub(crate) fn new() -> Arc<Self> {
        Self::create(0, 0, 0)
    }

    /// 唯一创建路径：身份根与其三条序列同时建立。
    fn create(next_data_id: u64, next_scope_id: u64, next_collector_id: u64) -> Arc<Self> {
        Arc::new(Self {
            next_data_id: AtomicU64::new(next_data_id),
            next_scope_id: AtomicU64::new(next_scope_id),
            next_collector_id: AtomicU64::new(next_collector_id),
        })
    }

    /// 从 `DataId` 序列取下一个序号。
    fn next_data_seq(&self) -> Result<u64, InternalError> {
        self.bump(&self.next_data_id, IdKind::Data)
    }

    /// 从 `ScopeId` 序列取下一个序号。
    fn next_scope_seq(&self) -> Result<u64, InternalError> {
        self.bump(&self.next_scope_id, IdKind::Scope)
    }

    /// 从 `CollectorId` 序列取下一个序号。
    fn next_collector_seq(&self) -> Result<u64, InternalError> {
        self.bump(&self.next_collector_id, IdKind::Collector)
    }

    fn bump(&self, counter: &AtomicU64, kind: IdKind) -> Result<u64, InternalError> {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |next| {
                next.checked_add(1)
            })
            .map_err(|_| InternalError::IdSpaceExhausted { kind })
    }
}

impl fmt::Debug for ExecutionIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExecutionIdentity({:p})", std::ptr::from_ref(self))
    }
}

impl fmt::Display for ExecutionIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExecutionIdentity({:p})", std::ptr::from_ref(self))
    }
}

/// 本次 Execution 中一个 owned Data 实例的运行时身份。
///
/// 相等与哈希都由身份根的分配地址与本地序号共同决定，不使用身份根的字段值；
/// 因此两个 Execution 里序号相同的 ID 不相等。相等 ID 必然产生相同 hash；不同 ID
/// 允许 hash 碰撞，容器始终用 `Eq` 区分实例，不把 hash 值或桶位置当作身份。
pub(crate) struct DataId {
    execution: Arc<ExecutionIdentity>,
    seq: u64,
}

impl DataId {
    /// 仅由 [`DataIdAllocator`] 在模块内调用；其他模块无法按整数构造 ID。
    fn new(execution: Arc<ExecutionIdentity>, seq: u64) -> Self {
        Self { execution, seq }
    }

    /// 本次 Execution 内的本地序号。
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    /// 分配该 ID 的身份根。
    pub(crate) fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl Clone for DataId {
    fn clone(&self) -> Self {
        Self {
            execution: Arc::clone(&self.execution),
            seq: self.seq,
        }
    }
}

impl PartialEq for DataId {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq && Arc::ptr_eq(&self.execution, &other.execution)
    }
}

impl Eq for DataId {}

impl Hash for DataId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.seq.hash(state);
        Arc::as_ptr(&self.execution).addr().hash(state);
    }
}

impl fmt::Debug for DataId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for DataId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataId({}, seq = {})", self.execution, self.seq)
    }
}

/// 单次 Execution 内一个 Scope 的运行时身份。
///
/// 身份语义与 [`DataId`] 相同，但序列独立。本任务只交付分配与身份比较：Scope 的
/// 父子关系、Closed 状态与 CollectionItem cap 由 V21-02／V21-08 接入。
pub(crate) struct ScopeId {
    execution: Arc<ExecutionIdentity>,
    seq: u64,
}

impl ScopeId {
    /// 仅由 [`ScopeIdAllocator`] 在模块内调用。
    fn new(execution: Arc<ExecutionIdentity>, seq: u64) -> Self {
        Self { execution, seq }
    }

    /// 本次 Execution 内的本地序号。
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    /// 分配该 ID 的身份根。
    ///
    /// Scope registry 在查表前用它校验来源 Execution，使另一 Execution 的同序号
    /// `ScopeId` 得到"来源不符"而不是"不存在"。
    pub(crate) fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl Clone for ScopeId {
    fn clone(&self) -> Self {
        Self {
            execution: Arc::clone(&self.execution),
            seq: self.seq,
        }
    }
}

impl PartialEq for ScopeId {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq && Arc::ptr_eq(&self.execution, &other.execution)
    }
}

impl Eq for ScopeId {}

impl Hash for ScopeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.seq.hash(state);
        Arc::as_ptr(&self.execution).addr().hash(state);
    }
}

impl fmt::Debug for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScopeId({}, seq = {})", self.execution, self.seq)
    }
}

/// `DataId` 分配句柄。
///
/// 句柄只是身份根的引用：因为序号位于身份根内部，同一身份根派生出的任意多个句柄都
/// 从同一序列取号，重复取得不会重新开始编号。
pub(crate) struct DataIdAllocator {
    identity: Arc<ExecutionIdentity>,
}

impl DataIdAllocator {
    /// 以给定身份根建立分配句柄。
    pub(crate) fn new(identity: Arc<ExecutionIdentity>) -> Self {
        Self { identity }
    }

    /// 从本 Execution 的 `DataId` 序列取下一个 ID。
    pub(crate) fn allocate(&self) -> Result<DataId, InternalError> {
        let seq = self.identity.next_data_seq()?;
        Ok(DataId::new(Arc::clone(&self.identity), seq))
    }
}

/// `ScopeId` 分配句柄。
///
/// 与 [`DataIdAllocator`] 共享同一个 [`ExecutionIdentity`]，但取的是独立的 `ScopeId`
/// 序列。分配 `ScopeId` 不意味着 DataContainer 管理 Scope 生命周期。
pub(crate) struct ScopeIdAllocator {
    identity: Arc<ExecutionIdentity>,
}

impl ScopeIdAllocator {
    /// 以给定身份根建立分配句柄；身份根应与同次 Execution 的 DataContainer 共享。
    pub(crate) fn new(identity: Arc<ExecutionIdentity>) -> Self {
        Self { identity }
    }

    /// 从本 Execution 的 `ScopeId` 序列取下一个 ID。
    pub(crate) fn allocate(&self) -> Result<ScopeId, InternalError> {
        let seq = self.identity.next_scope_seq()?;
        Ok(ScopeId::new(Arc::clone(&self.identity), seq))
    }
}

/// 本次 Execution 中一个未完成／已完成 collector 的内部身份。
///
/// 不参与普通 Data borrow，也不与 `DataId`／`ScopeId`／`RefId` 互换：collector 的
/// 业务值位于 DataContainer 的私有建构区，只有该身份能定位它。相等与哈希语义与
/// [`DataId`] 相同（身份根地址 + 本地序号），因此两个 Execution 里序号相同的
/// CollectorId 既不相等也不能互相寻址；清理后该序号不再复用，旧句柄不会指向新值。
pub(crate) struct CollectorId {
    execution: Arc<ExecutionIdentity>,
    seq: u64,
}

impl CollectorId {
    /// 仅由 [`CollectorIdAllocator`] 在模块内调用。
    fn new(execution: Arc<ExecutionIdentity>, seq: u64) -> Self {
        Self { execution, seq }
    }

    /// 本次 Execution 内的本地序号。
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    /// 分配该 ID 的身份根。
    pub(crate) fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl Clone for CollectorId {
    fn clone(&self) -> Self {
        Self {
            execution: Arc::clone(&self.execution),
            seq: self.seq,
        }
    }
}

impl PartialEq for CollectorId {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq && Arc::ptr_eq(&self.execution, &other.execution)
    }
}

impl Eq for CollectorId {}

impl Hash for CollectorId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.seq.hash(state);
        Arc::as_ptr(&self.execution).addr().hash(state);
    }
}

impl fmt::Debug for CollectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for CollectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CollectorId({}, seq = {})", self.execution, self.seq)
    }
}

/// `CollectorId` 分配句柄。
///
/// 与 [`DataIdAllocator`]／[`ScopeIdAllocator`] 共享同一个 [`ExecutionIdentity`]，
/// 取独立的 `CollectorId` 序列；多个句柄续用同一序列。
pub(crate) struct CollectorIdAllocator {
    identity: Arc<ExecutionIdentity>,
}

impl CollectorIdAllocator {
    /// 以给定身份根建立分配句柄。
    pub(crate) fn new(identity: Arc<ExecutionIdentity>) -> Self {
        Self { identity }
    }

    /// 从本 Execution 的 `CollectorId` 序列取下一个 ID。
    pub(crate) fn allocate(&self) -> Result<CollectorId, InternalError> {
        let seq = self.identity.next_collector_seq()?;
        Ok(CollectorId::new(Arc::clone(&self.identity), seq))
    }
}

#[cfg(test)]
#[path = "../../tests/internal/core/identity_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/internal/core/identity_observe.rs"]
pub(crate) mod observe;
