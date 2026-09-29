//! 执行期值存储（框架内部）。
//!
//! 每个 Flow 每次执行都创建一个独立的 `ValueStore`：它把各个数据位置的值按类型擦除保存，
//! 并在 Flow 内部按声明顺序读取。存储不参与业务语义，也不跨调用共享。

use std::any::Any;

use crate::core::ExecutionError;
use crate::core::reference::SlotId;

/// 一次 Flow 执行的值存储。
///
/// 业务侧看不到这个类型：Flow 只通过它把已声明数据的值交给各个 child，并在最后取出 Output。
pub(crate) struct ValueStore {
    slots: Vec<Slot>,
}

struct Slot {
    /// 当前保存的值；被取走后为 `None`。
    value: Option<Box<dyn Any + Send>>,
    /// 本次执行还允许读取该位置的次数，由 Flow 构建期的读取登记初始化。
    reads_left: u32,
}

impl ValueStore {
    /// 按每个数据位置的读取次数创建空存储。
    pub(crate) fn new(read_counts: &[u32]) -> Self {
        Self {
            slots: read_counts
                .iter()
                .map(|&reads_left| Slot {
                    value: None,
                    reads_left,
                })
                .collect(),
        }
    }

    /// 写入某个位置的值。
    pub(crate) fn insert<T: Send + 'static>(&mut self, slot: SlotId, value: T) {
        let state = self
            .slots
            .get_mut(slot.0)
            .expect("value store must cover every declared slot");
        state.value = Some(Box::new(value));
    }

    /// 消费读取：把该位置的值移动出来。
    pub(crate) fn read_consuming<T: Send + 'static>(
        &mut self,
        slot: SlotId,
    ) -> Result<T, ExecutionError> {
        let (state, _) = self.begin_read(slot)?;
        let value = state
            .value
            .take()
            .ok_or_else(|| ExecutionError::invariant("已声明位置没有值，但仍有读取操作"))?;
        downcast(value)
    }

    /// 复用读取：最后一次读取直接移动，其余读取克隆副本。
    ///
    /// “最后一次读取”由构建期登记的读取次数决定，因此同一个位置被复用时只会在真正需要
    /// 复制的那几次发生克隆。
    pub(crate) fn read_shared<T: Send + Clone + 'static>(
        &mut self,
        slot: SlotId,
    ) -> Result<T, ExecutionError> {
        let (state, is_last_read) = self.begin_read(slot)?;
        if is_last_read {
            let value = state
                .value
                .take()
                .ok_or_else(|| ExecutionError::invariant("已声明位置没有值，但仍有读取操作"))?;
            downcast(value)
        } else {
            let value = state
                .value
                .as_ref()
                .ok_or_else(|| ExecutionError::invariant("已声明位置没有值，但仍有读取操作"))?;
            let value = value
                .downcast_ref::<T>()
                .ok_or_else(|| ExecutionError::invariant("存储的值与强类型连接不一致"))?;
            Ok(value.clone())
        }
    }

    /// 记一次读取，并返回读取后的状态。
    fn begin_read(&mut self, slot: SlotId) -> Result<(&mut Slot, bool), ExecutionError> {
        let state = self
            .slots
            .get_mut(slot.0)
            .ok_or_else(|| ExecutionError::invariant("读取了未声明的数据位置"))?;
        if state.reads_left == 0 {
            return Err(ExecutionError::invariant(
                "数据位置被读取的次数多于登记次数",
            ));
        }
        state.reads_left -= 1;
        let is_last_read = state.reads_left == 0;
        Ok((state, is_last_read))
    }
}

fn downcast<T: Send + 'static>(value: Box<dyn Any + Send>) -> Result<T, ExecutionError> {
    value
        .downcast::<T>()
        .map(|value| *value)
        .map_err(|_| ExecutionError::invariant("存储的值与强类型连接不一致"))
}

#[cfg(test)]
mod tests {
    use super::ValueStore;
    use crate::core::ExecutionError;
    use crate::core::reference::SlotId;

    const SLOT: SlotId = SlotId(0);

    fn assert_invariant(error: ExecutionError) {
        assert!(
            matches!(error, ExecutionError::Invariant(_)),
            "内部存储的不变量破坏必须报告为框架错误，而不是业务结果"
        );
    }

    #[test]
    fn missing_value_is_an_invariant_error() {
        let mut store = ValueStore::new(&[1]);
        assert_invariant(store.read_consuming::<u32>(SLOT).unwrap_err());
    }

    #[test]
    fn type_mismatch_is_an_invariant_error() {
        let mut store = ValueStore::new(&[1]);
        store.insert(SLOT, String::from("text"));
        assert_invariant(store.read_consuming::<u32>(SLOT).unwrap_err());
    }

    #[test]
    fn reading_more_than_registered_is_an_invariant_error() {
        let mut store = ValueStore::new(&[1]);
        store.insert(SLOT, 7_u32);
        assert_eq!(store.read_consuming::<u32>(SLOT).unwrap(), 7);
        assert_invariant(store.read_consuming::<u32>(SLOT).unwrap_err());
    }

    #[test]
    fn undeclared_slot_is_an_invariant_error() {
        let mut store = ValueStore::new(&[1]);
        assert_invariant(store.read_consuming::<u32>(SlotId(9)).unwrap_err());
    }

    #[test]
    fn reuse_copies_until_the_last_read_takes_the_value() {
        let mut store = ValueStore::new(&[2]);
        store.insert(SLOT, vec![1_u32, 2, 3]);
        // 非最后一次读取：给出副本，原值仍在。
        assert_eq!(store.read_shared::<Vec<u32>>(SLOT).unwrap(), vec![1, 2, 3]);
        // 最后一次读取：直接移动原值。
        assert_eq!(store.read_shared::<Vec<u32>>(SLOT).unwrap(), vec![1, 2, 3]);
        assert_invariant(store.read_shared::<Vec<u32>>(SLOT).unwrap_err());
    }
}
