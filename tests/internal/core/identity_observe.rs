//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::identity::*;

impl ExecutionIdentity {
    /// 以指定起点创建**全新**身份根；仅测试构建可见，供 A11 使用。
    ///
    /// 起点只在创建新身份根时生效，不能用于改写已有身份的序列，也不提供重置、
    /// 回退或空闲列表能力。`CollectorId` 序列保持从 0 开始，需要近上限起点时用
    /// [`Self::with_starts_and_collector_start`]。
    #[cfg(test)]
    pub(crate) fn with_starts(next_data_id: u64, next_scope_id: u64) -> Arc<Self> {
        Self::create(next_data_id, next_scope_id, 0)
    }

    /// 以三条序列的指定起点创建全新身份根；仅测试构建可见。
    ///
    /// 与 [`Self::with_starts`] 分开，避免改动 V21-01／V21-02 已登记的 test-only
    /// 入口签名。供 C22（完成时 DataId 耗尽）与 C27（CollectorId 耗尽）使用。
    #[cfg(test)]
    pub(crate) fn with_starts_and_collector_start(
        next_data_id: u64,
        next_scope_id: u64,
        next_collector_id: u64,
    ) -> Arc<Self> {
        Self::create(next_data_id, next_scope_id, next_collector_id)
    }

    /// 从给定序列取下一个序号。
    ///
    /// 计数是身份根内部唯一的序列来源，因此多个分配句柄交替取号不会重复。增量在
    /// 赋值前完成溢出检查：`next + 1` 会溢出时返回 [`InternalError::IdSpaceExhausted`]，
    /// 不 wrap；`u64::MAX` 本身不会被发出。
    /// cfg(test) 只读：读取 `DataId` 计数器当前值（不推进）。
    #[cfg(test)]
    fn peek_data_seq(&self) -> Result<u64, InternalError> {
        Ok(self.next_data_id.load(Ordering::SeqCst))
    }
}

impl DataIdAllocator {
    #[cfg(test)]
    pub(crate) fn next_seq_probe(&self) -> Result<u64, InternalError> {
        self.identity.peek_data_seq()
    }
}
