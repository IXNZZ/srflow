//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::ref_id::*;

#[cfg(test)]
impl RefIdAllocator {
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
}

impl RefIdSource {
    /// 以指定起点创建**全新**来源根；仅测试构建可见，供 B02 验证 checked 耗尽。
    ///
    /// 起点只在创建新来源时生效，不能改写已使用来源的计数器。
    #[cfg(test)]
    pub(crate) fn with_start(start: u64) -> Arc<Self> {
        Self::create(start)
    }
}

impl RefIdAllocator {
    /// 从同一来源序列一次取 `count` 个连续 `RefId`（checked 整组分配）。
    ///
    /// 序列在同一次原子更新中推进 `count` 位：任何一个序号会溢出时整组失败，且**不
    /// 消耗任何序号、不留下部分分配**。`count == 0` 不接触序列。调用方据此先完成全部
    /// 可恢复预检、再整组提交。
    ///
    /// 当前生产路径逐位分配（`allocate`）；整组 epsilon 语义只在验收中作为保留设施
    /// 断言（见 `tests` 的 `v21_05_batch_allocation_is_atomic_on_exhaustion` 与
    /// `e12_checked_group_allocation_fails_as_a_whole_on_exhaustion`），故仅在 test 构建存在。
    /// 测试观测：本来源序列尚未分配的下一个序号（不改动序列）。
    #[cfg(test)]
    pub(crate) fn next_probe(&self) -> u64 {
        self.source.next.load(Ordering::SeqCst)
    }
}
