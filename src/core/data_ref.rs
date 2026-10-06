//! Definition 逻辑数据引用（`DataRef<T>`）。
//!
//! `DataRef<T>` 只包装一个 Definition 逻辑位置的 [`RefId`] 与类型标记：它不保存
//! `DataId`／`ScopeId`、真实 `&T`、业务 `T` 或本次 Execution 的绑定。因此复制句柄
//! 既不要求 `T: Clone`，也不复制任何业务 Data；同一次 Execution 的两份 `DataRef`
//! 只在被接线到 Invocation 时才对应运行时的真实值。
//!
//! 构造受控：业务侧无法凭空造出位置，只能在 Definition 构建期通过声明输入或接线
//! 输出位置取得（见 [`super::builder`]）。本模块不提供 `DataRef::new(value)` 或任何
//! owned Data 注入入口。

use std::fmt;
use std::marker::PhantomData;

use super::ref_id::RefId;

/// Definition 中一个逻辑数据位置的强类型引用。
///
/// `DataRef<T>` 不保存本次 Execution 的真实值、`DataId`／`ScopeId` 绑定或业务 `T`：
/// 复制句柄（`Clone`）既不要求 `T: Clone`，也不复制业务 Data。构造受控：业务侧无法
/// 凭空造出位置，只能在 Definition 构建期通过声明输入或接线输出位置取得（见
/// [`FlowBuilder`](crate::FlowBuilder) 等构建器）。
pub struct DataRef<T> {
    position: RefId,
    marker: PhantomData<fn() -> T>,
}

impl<T> DataRef<T> {
    /// 由 Definition 构建器分配位置时使用：不从序号序列直接暴露给业务。
    pub(crate) fn from_position(position: RefId) -> Self {
        Self {
            position,
            marker: PhantomData,
        }
    }

    /// 本引用对应的逻辑位置。
    pub(crate) fn position(&self) -> &RefId {
        &self.position
    }

    /// 在给定 Definition 来源上声明一个新的输入位置。
    ///
    /// 生产接线走 [`super::builder::Definition::declare_input`]；本入口只由 identity／
    /// data_ref 单元样本使用，用于验证位置句柄与来源序列的关系。
    #[cfg(test)]
    pub(crate) fn declare(
        allocator: &super::ref_id::RefIdAllocator,
    ) -> Result<Self, super::internal_error::InternalError> {
        Ok(Self::from_position(allocator.allocate()?))
    }
}

impl<T> Clone for DataRef<T> {
    /// 复制只复制位置句柄；不要求 `T: Clone`，也不复制业务 Data。
    fn clone(&self) -> Self {
        Self {
            position: self.position.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> fmt::Debug for DataRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DataRef<{}>({})",
            std::any::type_name::<T>(),
            self.position
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::ref_id::{RefIdAllocator, RefIdSource};
    use super::*;
    use std::sync::Arc;

    #[test]
    fn v21_05_data_ref_clone_needs_no_data_clone() {
        struct NotClone(#[allow(dead_code)] u32);
        let allocator = RefIdAllocator::new(RefIdSource::new());
        let original = DataRef::<NotClone>::declare(&allocator).unwrap();
        let copy = original.clone();
        assert_eq!(copy.position(), original.position());
        assert!(original.position().same_source(copy.position()));
    }

    #[test]
    fn v21_05_data_ref_does_not_claim_a_position_from_another_source() {
        let left = RefIdAllocator::new(RefIdSource::new());
        let right = RefIdAllocator::new(RefIdSource::new());
        let mine = DataRef::<u32>::declare(&left).unwrap();
        let other = DataRef::<u32>::declare(&right).unwrap();
        assert!(!mine.position().same_source(other.position()));
        assert_eq!(mine.position().seq(), other.position().seq());
    }

    #[test]
    fn v21_05_declared_inputs_advance_the_single_source_sequence() {
        let source = RefIdSource::new();
        let handle = RefIdAllocator::new(Arc::clone(&source));
        let first = DataRef::<u32>::declare(&handle).unwrap();
        let second = DataRef::<u32>::declare(&handle).unwrap();
        assert_eq!((first.position().seq(), second.position().seq()), (0, 1));
    }
}
