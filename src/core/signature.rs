//! 声明端口元数据：一个逻辑位置的声明类型与诊断用类型名。
//!
//! B 阶段清理后本模块只保留 [`DeclaredPort`]：它是新 Engine 的 Definition 与
//! Core `ScopeCoordinator` 共同需要的**共用设施**（Root 输出端口的声明类型信息），
//! 不持有业务值、不接触 Execution，也不建立公开 API。旧 V21-05 的接线 Signature
//! 封套（`Data`／`Unit`／`Out2`／`NodeSig`／`SyncFnSig`／`BuildError` 等）已随旧
//! 上层协议退出。

use std::any::TypeId;

use super::ref_id::RefId;

/// 一个已声明位置的元数据：位置、声明类型与诊断用类型名。
#[derive(Debug, Clone)]
pub(crate) struct DeclaredPort {
    position: RefId,
    expected: TypeId,
    expected_name: &'static str,
}

impl DeclaredPort {
    /// 以运行时类型声明位置（child-local 端口由内部 Definition 决定）。
    pub(crate) fn with_type(
        position: RefId,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Self {
        Self {
            position,
            expected,
            expected_name,
        }
    }

    /// 逻辑位置。
    pub(crate) fn position(&self) -> &RefId {
        &self.position
    }

    /// 声明类型。
    pub(crate) fn expected(&self) -> TypeId {
        self.expected
    }

    /// 声明类型的名字。
    pub(crate) fn expected_name(&self) -> &'static str {
        self.expected_name
    }
}
