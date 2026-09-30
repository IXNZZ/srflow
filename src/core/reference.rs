use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

/// 当前 Flow 中某个数据位置的强类型只读引用。
///
/// `Ref<T>` 是**数据位置的句柄**，不是业务值：它没有公开字段，也不提供可变访问。它只会由
/// [`FlowBuilder::input`](crate::core::FlowBuilder::input) 与
/// [`then`](crate::core::FlowBuilder::then)／[`then_move`](crate::core::FlowBuilder::then_move)
/// 产生，因此使用者无法伪造一个指向非法位置的 `Ref`。
///
/// # 归属
///
/// 每个 `Ref` 在内部记住自己属于哪个 Flow。把 Flow A 的 `Ref` 用在 Flow B 的
/// [`then`](crate::core::FlowBuilder::then)／[`then_move`](crate::core::FlowBuilder::then_move)／
/// [`output`](crate::core::FlowBuilder::output) 中会在**构建阶段**被拒绝，即使两边内部位置编号
/// 相同、Rust 类型也相同。把外来 `Ref` 藏在字段投影、tuple 或 [`bind!`](crate::bind) 装配中
/// 同样会被拒绝。
///
/// # 复用
///
/// `Ref` 本身可以自由复制（复制的是句柄，不是业务数据）。同一个位置能否被多个步骤使用，
/// 取决于建立连接时选择的读取方式：裸 `Ref` 作为 Binding 表示复用，之后仍可继续读取；
/// [`consume`](crate::consume) 表示把值交给这一步，之后不能再读取；[`field!`](crate::field)
/// 只借用根、复制字段，也不消费该位置。
pub struct Ref<T> {
    flow: FlowId,
    slot: SlotId,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Ref<T> {
    /// 只由 Flow 构建过程调用；不对外开放，避免业务侧绕过归属检查。
    pub(crate) fn new(flow: FlowId, slot: SlotId) -> Self {
        Self {
            flow,
            slot,
            _marker: PhantomData,
        }
    }

    pub(crate) fn flow(&self) -> FlowId {
        self.flow
    }

    pub(crate) fn slot(&self) -> SlotId {
        self.slot
    }
}

// 手工实现：派生的 Clone/Copy 会给 T 加上同样的约束，而复制句柄与 T 无关。
impl<T> Clone for Ref<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Ref<T> {}

impl<T> std::fmt::Debug for Ref<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ref")
            .field("flow", &self.flow)
            .field("slot", &self.slot)
            .field("type", &std::any::type_name::<T>())
            .finish()
    }
}

/// Flow 身份：只用于判断某个 `Ref` 是否属于当前构建中的 Flow。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlowId(u64);

impl FlowId {
    /// 取一个进程内唯一的身份。
    ///
    /// 这是身份分配器，不是业务数据存储；它不参与执行，也不跨 Flow 共享业务值。
    pub(crate) fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Flow 内部的数据位置编号。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlotId(pub(crate) usize);
