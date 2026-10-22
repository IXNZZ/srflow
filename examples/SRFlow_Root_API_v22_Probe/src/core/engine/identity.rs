//! Definition 级身份：定义、Body、逻辑位置与 Core RefId 承担者。
//!
//! 这些编号只在 Definition 构建期分配与比较；它们不是运行期 DataId／ScopeId，
//! 也不进入业务数据流。

use std::sync::atomic::{AtomicU64, Ordering};

/// 一次 Definition 构建的身份；用于拒绝其他 Definition 的 Ref。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct DefinitionId(u64);

impl DefinitionId {
    /// 分配一个新的定义身份。
    pub(crate) fn fresh() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Definition 内的逻辑位置编号；与 [`DefinitionId`] 组合构成 Ref 身份。
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) struct PositionId(u32);

impl PositionId {
    /// 以 Definition 内下标构造位置编号。
    pub(crate) fn new(index: u32) -> Self {
        Self(index)
    }

    /// Definition 内的下标。
    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

/// Definition 内的 Body 编号；`BodyId(0)` 恒为 Root Body。
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) struct BodyId(u32);

impl BodyId {
    /// Root Body 的编号。
    pub(crate) const ROOT: Self = Self(0);

    /// 以 Definition 内下标构造 Body 编号。
    pub(crate) fn new(index: u32) -> Self {
        Self(index)
    }

    /// Definition 内的下标。
    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

/// Ref／RefShape 的原始位置句柄。
///
/// 句柄只表达“哪个 Definition 的哪个位置”，本身不是来源证明：任何句柄的使用
/// 仍须通过 Definition 的来源登记与验证。字段不对外公开，公开可见性只用于
/// shape helper 的签名。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PositionHandle {
    pub(crate) definition: DefinitionId,
    pub(crate) position: PositionId,
}
