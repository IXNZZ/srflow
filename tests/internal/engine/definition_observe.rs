//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::engine::definition::*;

impl<'n> Builder<'n> {
    /// 测试可控入口：当前 Definition 身份。
    #[cfg(test)]
    pub(crate) fn definition_id(&self) -> DefinitionId {
        self.id
    }

    /// 测试可控入口：下一个将分配的位置编号。
    #[cfg(test)]
    pub(crate) fn next_position_id(&self) -> PositionId {
        PositionId::new(self.next_position)
    }
}
