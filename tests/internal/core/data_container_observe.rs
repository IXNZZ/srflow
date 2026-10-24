//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::data_container::*;

impl DataContainer {
    /// 测试观测：真实物理移动路径上的追加次数。
    #[cfg(test)]
    pub(crate) fn collector_moves(&self, collector: &CollectorId) -> Result<usize, InternalError> {
        self.collector_checked_seq(collector)?;
        Ok(self
            .collector_moves
            .get(&collector.seq())
            .copied()
            .unwrap_or_default())
    }

    /// cfg(test) 只读观察：下一个将被分配的 `DataId` 序号（序列耗尽返回 None）。
    ///
    /// 只读计数器，不分配、不改状态；用于证明失败路径不消耗 `DataId` 序号。
    #[cfg(test)]
    pub(crate) fn next_data_id_probe(&self) -> Option<u64> {
        self.ids.next_seq_probe().map_err(|_| ()).ok()
    }
}

/// 测试观测：容器最终析构事件（用于验证存储先于/后于清理的先后关系）。
#[cfg(test)]
impl Drop for DataContainer {
    fn drop(&mut self) {
        crate::core::context::creation_counts::record_event("container-drop");
    }
}
