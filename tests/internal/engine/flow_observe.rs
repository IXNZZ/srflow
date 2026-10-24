//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::engine::flow::*;

impl<'n> Flow<'n> {
    /// 测试可控入口：当前 Definition 身份，用于构造“无来源位置”反例。
    #[cfg(test)]
    pub(crate) fn definition_id(&self) -> crate::core::engine::identity::DefinitionId {
        self.builder.borrow().definition_id()
    }

    /// 测试可控入口：下一个将分配的 Definition 内位置编号，用于构造“前向来源”反例。
    #[cfg(test)]
    pub(crate) fn next_position_id(&self) -> crate::core::engine::identity::PositionId {
        self.builder.borrow().next_position_id()
    }
}
