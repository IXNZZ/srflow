//! 一次 Execution 内异构 owned Data 的物理存储。
//!
//! [`DataContainer`] 是业务 Data 的唯一物理持有者：`insert_owned` 接管 owned 值并
//! 返回新的 [`DataId`](super::identity::DataId)；`borrow` 在校验通过后提供与容器借用期
//! 关联的短期 `&T`；`remove_owned`、`destroy` 与 collector 原语是内部的移除／移动原语。
//! 容器不导出给业务侧，也不登记 Scope 的可见性或生命周期责任——那些属于 Scope 层。
//!
//! 未完成 collector 的建构值（`Vec<O>`）同样只存在于容器内部：它在成为普通 entry 前
//! 没有 `DataId`，也不能被 `borrow` 定位；`move_into_collector` 在容器内部完成
//! "普通 entry → 建构值"的移动，`finish_collector` 才把它变成新的普通 `Vec<O>`。
//!
//! 检查顺序固定为**归属 → 存活 → 类型**，因而"来源 Execution 不符"与"数据不存在／
//! 已失效"是两类可区分的诊断。类型擦除为 `Box<dyn Any>`（要求 `'static`），当前不附加
//! `Send`／`Sync`：容器本身因此是 `!Send`／`!Sync`，多线程与异步边界由后续任务决定。

use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::sync::Arc;

use super::identity::{CollectorId, DataId, DataIdAllocator, ExecutionIdentity};
use super::internal_error::InternalError;

/// 未完成 collector 的建构接口。
///
/// 每个 collector 由 `Vec<O>` 实现本 trait 并擦除为 `Box<dyn ErasedVecBuilder>`：
/// 具体 `O` 只在创建 collector 的那一次编译期调用中出现，此后 Consume 只需按真实
/// `TypeId` 校验元素。元素的追加在 trait 内部按真实 `O` 执行 downcast，`Box<dyn Any>`
/// 不交给协调组件。
trait ErasedVecBuilder {
    /// 追加一个已按元素 `TypeId` 校验过的 owned 元素。
    ///
    /// 调用方必须先用本 trait 的 [`ErasedVecBuilder::element_type`] 校验过类型；
    /// 类型不符属于不变量破坏，因此这里直接断言而不是返回可恢复错误。
    fn push_erased(&mut self, value: Box<dyn Any>);

    /// 取出建构值，作为新的普通 `Vec<O>` entry 的内容。
    fn into_erased(self: Box<Self>) -> Box<dyn Any>;

    /// 元素类型身份。
    fn element_type(&self) -> TypeId;

    /// 元素类型名，仅用于诊断。
    fn element_name(&self) -> &'static str;

    /// 建构值的类型名（`Vec<O>`），登记普通 entry 时使用。
    fn vec_type_name(&self) -> &'static str;

    /// 已收集元素数量。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    fn element_count(&self) -> usize;
}

impl<O: Any> ErasedVecBuilder for Vec<O> {
    fn push_erased(&mut self, value: Box<dyn Any>) {
        self.push(
            *value
                .downcast::<O>()
                .expect("element type verified before push"),
        );
    }

    fn into_erased(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn element_type(&self) -> TypeId {
        TypeId::of::<O>()
    }

    fn element_name(&self) -> &'static str {
        type_name::<O>()
    }

    fn vec_type_name(&self) -> &'static str {
        type_name::<Vec<O>>()
    }

    fn element_count(&self) -> usize {
        Vec::len(self)
    }
}

/// 一个已登记 entry 的类型擦除存储。
struct DataEntry {
    /// 登记时记录的类型名，仅用于类型不符时的诊断。
    type_name: &'static str,
    /// owned 业务值；`Box<dyn Any>` 携带类型身份。
    value: Box<dyn Any>,
}

/// 一次 Execution 中业务 Data 的物理持有者。
///
/// 本阶段由测试夹具代表一个 Execution；ExecutionContext（V21-04）接入后，一次 Root
/// Execution 只会有一个 Container，并与 `ScopeId` 分配器共享同一个
/// [`ExecutionIdentity`]。生命周期结束时容器内的值正常销毁，已经移出的值不会被重复处理。
pub(crate) struct DataContainer {
    execution: Arc<ExecutionIdentity>,
    ids: DataIdAllocator,
    entries: HashMap<u64, DataEntry>,
    /// 未完成 collector 的建构区：业务 `Vec<O>` 只在这里，直到完成为普通 entry。
    collectors: HashMap<u64, Box<dyn ErasedVecBuilder>>,
    /// 每个 collector 在真实物理移动路径上的追加次数（仅测试观测）。
    #[cfg(test)]
    collector_moves: HashMap<u64, usize>,
}

impl DataContainer {
    /// 以给定身份根建立容器。
    ///
    /// ExecutionContext 接入后由上下文创建一次身份根，并把它同时交给容器与 Scope 分配器；
    /// 容器不自行创建第二个身份根。本任务不提供不带身份的 `new()` 自建入口。
    pub(crate) fn with_identity(execution: Arc<ExecutionIdentity>) -> Self {
        #[cfg(test)]
        crate::core::context::creation_counts::count_container();
        Self {
            ids: DataIdAllocator::new(Arc::clone(&execution)),
            execution,
            entries: HashMap::new(),
            collectors: HashMap::new(),
            #[cfg(test)]
            collector_moves: HashMap::new(),
        }
    }

    /// 校验 ID 属于本容器且对应 entry 仍然存活；不检查类型。
    ///
    /// 这是 `borrow`／`remove_owned`／`destroy` 共用的归属与存活检查，也是 A03 的
    /// "校验"入口。
    pub(crate) fn validate(&self, id: &DataId) -> Result<(), InternalError> {
        self.checked_seq(id).map(|_| ())
    }

    /// 登记一个 owned 业务值，成功后返回新的 `DataId`。
    ///
    /// `()` 不是业务 Data 输出，会在分配序号之前被拒绝，因此既不会生成 `DataId`，
    /// 也不会留下 entry。序号耗尽时同样不写入 entry。失败时输入值随本次调用一同
    /// drop，不会被登记，也不会处于无法追踪的状态。
    pub(crate) fn insert_owned<T: Any>(&mut self, value: T) -> Result<DataId, InternalError> {
        if TypeId::of::<T>() == TypeId::of::<()>() {
            return Err(InternalError::UnitNotStorable);
        }

        let id = self.ids.allocate()?;
        self.entries.insert(
            id.seq(),
            DataEntry {
                type_name: type_name::<T>(),
                value: Box::new(value),
            },
        );
        Ok(id)
    }

    /// 登记一个已擦除的 owned 业务值：与 [`Self::insert_owned`] 同一分配与拒绝规则。
    ///
    /// 供 Root 输入登记使用：输入的静态类型已知（`RootInputs` 的映射），但在登记边界
    /// 只保留擦除值。`type_name` 必须是该值真实类型的名字，`()` 与序号耗尽同样在写入
    /// entry 之前被拒绝。
    pub(crate) fn insert_owned_erased(
        &mut self,
        type_name: &'static str,
        value: Box<dyn Any>,
    ) -> Result<DataId, InternalError> {
        // 注意：`Box<dyn Any>` 自身的 `Any` impl 会给出 `Box<dyn Any>` 的 `TypeId`；
        // 必须先取到底层 `dyn Any` 再比较，与 `validate_type` 同一注意事项。
        if value.as_ref().type_id() == TypeId::of::<()>() {
            return Err(InternalError::UnitNotStorable);
        }

        let id = self.ids.allocate()?;
        self.entries
            .insert(id.seq(), DataEntry { type_name, value });
        Ok(id)
    }

    /// 提交段专用：已验证身份与存活后移出单值，返回擦除的 owned 值。
    ///
    /// 只供 Root 提取提交使用：调用方必须已在同一不可观察提交边界之前证明该 `DataId`
    /// 属于本 Execution 且 entry 存活（`prepare_root_extraction`）。这个入口不做可恢复
    /// 校验，`expect` 表达"预检已覆盖"的不变量而不是普通错误路径；旧 `DataId` 在移动后
    /// 失效。
    pub(crate) fn take_verified(&mut self, id: &DataId) -> Box<dyn Any> {
        debug_assert!(
            Arc::ptr_eq(&self.execution, id.execution()),
            "root take requires an id from this execution"
        );
        #[cfg(test)]
        super::test_support::record_take(id);
        self.entries
            .remove(&id.seq())
            .expect("root take requires an entry proven alive by the preflight")
            .value
    }

    /// 返回与容器借用期关联的只读借用。
    ///
    /// 校验顺序为归属、存活、类型；任何一步失败都不改变 entry。允许同时存在多个借用，
    /// 包括对同一份 Data 的重复借用，且不复制业务值。借用未结束时无法可变借用容器，
    /// 因而无法移动或销毁相应存储。
    pub(crate) fn borrow<T: Any>(&self, id: &DataId) -> Result<&T, InternalError> {
        let (_, entry) = self.entry(id)?;
        entry
            .value
            .downcast_ref::<T>()
            .ok_or_else(|| InternalError::TypeMismatch {
                requested: id.clone(),
                expected: type_name::<T>(),
                actual: entry.type_name,
            })
    }

    /// cfg(test) 只读观察：下一个将被分配的 `DataId` 序号（序列耗尽返回 None）。
    ///
    /// 只读计数器，不分配、不改状态；用于证明失败路径不消耗 `DataId` 序号。
    #[cfg(test)]
    pub(crate) fn next_data_id_probe(&self) -> Option<u64> {
        self.ids.next_seq_probe().map_err(|_| ()).ok()
    }

    /// 擦除借用：只做归属与存活检查，返回真实存储值的 `&dyn Any`。
    ///
    /// 供 CollectionItem 的元素投影使用：实际 `Vec<T>` 类型由元素的 typed 访问描述
    /// 在投影时 downcast 复核，因此这里**不**做类型断言，也不接受业务侧类型参数。
    /// 借用期与容器只读借用相同；借用未结束时无法可变借用容器。
    pub(crate) fn borrow_any(&self, id: &DataId) -> Result<&dyn Any, InternalError> {
        let (_, entry) = self.entry(id)?;
        Ok(entry.value.as_ref())
    }

    /// 只校验归属、存活与类型，不借用也不移出值。
    ///
    /// 供以运行时 `TypeId` 驱动整组校验的 Scope 层复用：它走与 `borrow` 相同的
    /// 归属 → 存活 → 类型检查路径，不建立第二条可能漂移的检查顺序。`expected_name`
    /// 只在类型不符时用于诊断（`TypeId` 本身不携带类型名）。
    pub(crate) fn validate_type(
        &self,
        id: &DataId,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Result<(), InternalError> {
        let (_, entry) = self.entry(id)?;
        // 注意：`Box<dyn Any>` 自身的 `Any` impl 也提供 `type_id`，直接调用会得到
        // `TypeId::of::<Box<dyn Any>>()`；必须先取到 `dyn Any` 再比较。
        if entry.value.as_ref().type_id() != expected {
            return Err(InternalError::TypeMismatch {
                requested: id.clone(),
                expected: expected_name,
                actual: entry.type_name,
            });
        }
        Ok(())
    }

    // V21-10 Root take 接入前只由测试驱动；V21-03 的 Consume 走 collector 内部移动原语
    /// 移除 entry 并返回原 owned 值；成功后旧 `DataId` 失效。
    ///
    /// 类型与身份在移动之前验证：类型不符时 entry 保持原状，值不会被误删。这是后续
    /// Consume（V21-03）与 Root take（V21-10）的实现原语——它们必须先完成 Scope、
    /// owner 或 Root 预检再调用本方法；本方法不授予 Orchestrator 按值取数权。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn remove_owned<T: Any>(&mut self, id: &DataId) -> Result<T, InternalError> {
        let (seq, entry) = self.entry(id)?;
        if !entry.value.is::<T>() {
            return Err(InternalError::TypeMismatch {
                requested: id.clone(),
                expected: type_name::<T>(),
                actual: entry.type_name,
            });
        }

        let entry = self
            .entries
            .remove(&seq)
            .expect("entry verified present above");
        Ok(*entry
            .value
            .downcast::<T>()
            .expect("entry type verified above"))
    }

    /// 在容器内销毁指定 entry；旧 `DataId` 再次使用时被拒绝。
    pub(crate) fn destroy(&mut self, id: &DataId) -> Result<(), InternalError> {
        let seq = self.checked_seq(id)?;
        self.entries
            .remove(&seq)
            .expect("checked_seq verified entry presence");
        Ok(())
    }

    /// 归属与存活检查；返回本地序号供后续查表。
    ///
    /// 归属预检在任何查表之前执行，因此另一 Execution 的同序号 ID 会得到
    /// `ForeignExecution`，而不会因为序号恰好命中而误读数据。
    fn checked_seq(&self, id: &DataId) -> Result<u64, InternalError> {
        if !Arc::ptr_eq(&self.execution, id.execution()) {
            return Err(InternalError::ForeignExecution {
                requested: id.clone(),
                container_execution: Arc::clone(&self.execution),
            });
        }
        if !self.entries.contains_key(&id.seq()) {
            return Err(InternalError::DataNotFound {
                requested: id.clone(),
            });
        }
        Ok(id.seq())
    }

    /// 归属、存活检查后的 entry 查找；`borrow`、`remove_owned` 与 `validate_type`
    /// 共用这一条检查路径。
    fn entry(&self, id: &DataId) -> Result<(u64, &DataEntry), InternalError> {
        let seq = self.checked_seq(id)?;
        let entry = self
            .entries
            .get(&seq)
            .expect("checked_seq verified entry presence");
        Ok((seq, entry))
    }

    /// 建立一个空 collector 的建构值，登记元素真实 `TypeId`。
    ///
    /// 只有容器持有建构值；协调组件登记责任 Scope 与元素类型元数据。`()` 不是可用的
    /// 元素类型：空 collector 仍需一个真实元素类型，否则无法与"没有集合元素"区分。
    /// `CollectorId` 由本 Execution 的身份序列分配，容器只按身份登记。
    pub(crate) fn begin_collector<O: Any>(
        &mut self,
        collector: &CollectorId,
    ) -> Result<(), InternalError> {
        if !Arc::ptr_eq(&self.execution, collector.execution()) {
            return Err(InternalError::CollectorForeignExecution {
                requested: collector.clone(),
                container_execution: Arc::clone(&self.execution),
            });
        }
        if TypeId::of::<O>() == TypeId::of::<()>() {
            return Err(InternalError::UnitNotCollectible);
        }
        let builder: Box<dyn ErasedVecBuilder> = Box::new(Vec::<O>::new());
        let fresh = self.collectors.insert(collector.seq(), builder).is_none();
        assert!(
            fresh,
            "collector identities are monotonic within one execution and never reused"
        );
        #[cfg(test)]
        self.collector_moves.insert(collector.seq(), 0);
        Ok(())
    }

    /// 未完成 collector 已收集的元素数量。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn collector_len(&self, collector: &CollectorId) -> Result<usize, InternalError> {
        Ok(self.collector_builder(collector)?.element_count())
    }

    /// collector 登记的元素类型身份与类型名。
    pub(crate) fn collector_element_type(
        &self,
        collector: &CollectorId,
    ) -> Result<(TypeId, &'static str), InternalError> {
        let builder = self.collector_builder(collector)?;
        Ok((builder.element_type(), builder.element_name()))
    }

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

    /// 把一个普通 entry 的业务值在容器内部移入 collector。
    ///
    /// 顺序固定为归属 → 存活 → 类型：任一步失败都不移除 entry、不改变建构值长度。
    /// 移动完成后旧 `DataId` 立即失效，`Box<dyn Any>` 不离开容器。
    pub(crate) fn move_into_collector(
        &mut self,
        collector: &CollectorId,
        item: &DataId,
    ) -> Result<(), InternalError> {
        let seq = self.checked_seq(item)?;
        let collector_seq = self.collector_checked_seq(collector)?;
        let builder = self
            .collectors
            .get_mut(&collector_seq)
            .expect("collector_checked_seq verified presence");
        {
            let entry = self
                .entries
                .get(&seq)
                .expect("checked_seq verified entry presence");
            // 与 validate_type 相同：先取 `dyn Any` 再比较，避免拿到 Box 自身的 TypeId。
            if entry.value.as_ref().type_id() != builder.element_type() {
                return Err(InternalError::TypeMismatch {
                    requested: item.clone(),
                    expected: builder.element_name(),
                    actual: entry.type_name,
                });
            }
        }

        let entry = self
            .entries
            .remove(&seq)
            .expect("entry verified present above");
        builder.push_erased(entry.value);
        #[cfg(test)]
        self.collector_moves
            .entry(collector_seq)
            .and_modify(|moves| *moves += 1);
        Ok(())
    }

    /// 完成 collector：建构值成为新的普通 `Vec<O>` entry，并返回其新 `DataId`。
    ///
    /// 新 `DataId` 在任何移除或写入之前分配，因此序号耗尽时 collector 仍保持未完成，
    /// 内容不变，也不留下无元素类型的 entry。完成后旧 `CollectorId` 不再存在。
    pub(crate) fn finish_collector(
        &mut self,
        collector: &CollectorId,
    ) -> Result<DataId, InternalError> {
        let collector_seq = self.collector_checked_seq(collector)?;
        let type_name = self
            .collectors
            .get(&collector_seq)
            .expect("collector_checked_seq verified presence")
            .vec_type_name();

        let id = self.ids.allocate()?;

        let builder = self
            .collectors
            .remove(&collector_seq)
            .expect("collector_checked_seq verified presence");
        #[cfg(test)]
        self.collector_moves.remove(&collector_seq);
        self.entries.insert(
            id.seq(),
            DataEntry {
                type_name,
                value: builder.into_erased(),
            },
        );
        Ok(id)
    }

    /// 销毁未完成 collector；部分结果随建构值一起析构。
    pub(crate) fn destroy_collector(
        &mut self,
        collector: &CollectorId,
    ) -> Result<(), InternalError> {
        let collector_seq = self.collector_checked_seq(collector)?;
        self.collectors
            .remove(&collector_seq)
            .expect("collector_checked_seq verified presence");
        #[cfg(test)]
        self.collector_moves.remove(&collector_seq);
        Ok(())
    }

    /// 归属与存活检查后的 collector 查找。
    fn collector_builder(
        &self,
        collector: &CollectorId,
    ) -> Result<&dyn ErasedVecBuilder, InternalError> {
        let seq = self.collector_checked_seq(collector)?;
        Ok(self
            .collectors
            .get(&seq)
            .map(Box::as_ref)
            .expect("collector_checked_seq verified presence"))
    }

    /// collector 的归属与存活检查；返回本地序号供后续查表。
    ///
    /// 已完成或已清理的 collector 记录已被移除，因此旧句柄得到"不存在／不可用"诊断，
    /// 不会重新定位到新值。
    fn collector_checked_seq(&self, collector: &CollectorId) -> Result<u64, InternalError> {
        if !Arc::ptr_eq(&self.execution, collector.execution()) {
            return Err(InternalError::CollectorForeignExecution {
                requested: collector.clone(),
                container_execution: Arc::clone(&self.execution),
            });
        }
        if !self.collectors.contains_key(&collector.seq()) {
            return Err(InternalError::CollectorNotFound {
                requested: collector.clone(),
            });
        }
        Ok(collector.seq())
    }
}

/// 测试观测：容器最终析构事件（用于验证存储先于/后于清理的先后关系）。
#[cfg(test)]
impl Drop for DataContainer {
    fn drop(&mut self) {
        crate::core::context::creation_counts::record_event("container-drop");
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::DataContainer;
    use crate::core::identity::{
        CollectorIdAllocator, DataId, ExecutionIdentity, ScopeIdAllocator,
    };
    use crate::core::internal_error::InternalError;

    /// 另一类型，用于类型身份与诊断的区分。
    struct Other;

    /// 非 Clone 业务值，带 Drop 观测。
    struct Tracked {
        num: u32,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// 另一个类型，用于类型不符场景。
    struct Plain(u32);

    fn container() -> DataContainer {
        DataContainer::with_identity(ExecutionIdentity::new())
    }

    fn tracked(num: u32, drops: &Arc<AtomicUsize>) -> Tracked {
        Tracked {
            num,
            drops: Arc::clone(drops),
        }
    }

    #[test]
    fn a01_entries_are_addressable_by_data_id_not_by_type() {
        let mut container = container();
        let drops = Arc::new(AtomicUsize::new(0));

        let first = container.insert_owned(tracked(1, &drops)).unwrap();
        let second = container.insert_owned(tracked(2, &drops)).unwrap();
        let text = container.insert_owned(String::from("hello")).unwrap();
        let number = container.insert_owned(7u64).unwrap();

        assert_eq!(container.borrow::<Tracked>(&first).unwrap().num, 1);
        assert_eq!(container.borrow::<Tracked>(&second).unwrap().num, 2);
        assert_eq!(container.borrow::<String>(&text).unwrap(), "hello");
        assert_eq!(*container.borrow::<u64>(&number).unwrap(), 7);

        // 同类型的两个实例身份互不相同，不能按类型猜实例。
        assert_ne!(first, second);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a02_non_clone_values_share_read_borrows_without_copying() {
        let mut container = container();
        let drops = Arc::new(AtomicUsize::new(0));

        let first = container.insert_owned(tracked(5, &drops)).unwrap();
        let second = container.insert_owned(tracked(6, &drops)).unwrap();

        let repeated = container.borrow::<Tracked>(&first).unwrap();
        let again = container.borrow::<Tracked>(&first).unwrap();
        let other = container.borrow::<Tracked>(&second).unwrap();

        // 同一实例的重复借用指向同一地址；不同实例互不相同。
        assert!(std::ptr::eq(repeated, again));
        assert!(!std::ptr::eq(repeated, other));
        assert_eq!((repeated.num, again.num, other.num), (5, 5, 6));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a03_foreign_ids_are_rejected_in_every_entry_point() {
        let mut first_execution = container();
        let mut second_execution = container();
        let drops = Arc::new(AtomicUsize::new(0));

        let id_in_first = first_execution.insert_owned(tracked(1, &drops)).unwrap();
        let id_in_second = second_execution.insert_owned(tracked(2, &drops)).unwrap();

        // 本地序号碰撞：两份 Execution 的首个 DataId 序号相同。
        assert_eq!(id_in_first.seq(), id_in_second.seq());
        assert_ne!(id_in_first, id_in_second);

        assert!(matches!(
            second_execution.validate(&id_in_first),
            Err(InternalError::ForeignExecution { .. })
        ));
        assert!(matches!(
            second_execution.borrow::<Tracked>(&id_in_first),
            Err(InternalError::ForeignExecution { .. })
        ));
        assert!(matches!(
            second_execution.remove_owned::<Tracked>(&id_in_first),
            Err(InternalError::ForeignExecution { .. })
        ));
        assert!(matches!(
            second_execution.destroy(&id_in_first),
            Err(InternalError::ForeignExecution { .. })
        ));

        // 两个 Execution 各自的数据都保持有效。
        assert_eq!(
            first_execution.borrow::<Tracked>(&id_in_first).unwrap().num,
            1
        );
        assert_eq!(
            second_execution
                .borrow::<Tracked>(&id_in_second)
                .unwrap()
                .num,
            2
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a04_runtime_part_sequences_advance_and_stay_independent() {
        let execution = ExecutionIdentity::new();
        let mut container = DataContainer::with_identity(Arc::clone(&execution));
        let scopes = ScopeIdAllocator::new(Arc::clone(&execution));
        let drops = Arc::new(AtomicUsize::new(0));

        let first = container.insert_owned(tracked(1, &drops)).unwrap();
        let second = container.insert_owned(tracked(2, &drops)).unwrap();
        let scope = scopes.allocate().unwrap();

        assert_eq!((first.seq(), second.seq()), (0, 1));
        // ScopeId 有独立序号空间，不从 DataId 序列取号。
        assert_eq!(scope.seq(), 0);
    }

    #[test]
    fn a05_removed_ids_are_never_reissued_and_stay_invalid() {
        let mut container = container();
        let old = container.insert_owned(Plain(1)).unwrap();

        let value = container.remove_owned::<Plain>(&old).unwrap();
        assert_eq!(value.0, 1);

        let new = container.insert_owned(Plain(2)).unwrap();
        assert_ne!(new, old);
        assert_eq!(new.seq(), old.seq() + 1);

        assert!(matches!(
            container.borrow::<Plain>(&old),
            Err(InternalError::DataNotFound { .. })
        ));
        assert!(matches!(
            container.remove_owned::<Plain>(&old),
            Err(InternalError::DataNotFound { .. })
        ));
        assert!(matches!(
            container.destroy(&old),
            Err(InternalError::DataNotFound { .. })
        ));
        assert_eq!(container.borrow::<Plain>(&new).unwrap().0, 2);
    }

    #[test]
    fn a07_type_mismatch_is_rejected_before_any_move() {
        let mut container = container();
        let drops = Arc::new(AtomicUsize::new(0));
        let id = container.insert_owned(tracked(9, &drops)).unwrap();

        assert!(matches!(
            container.borrow::<Plain>(&id),
            Err(InternalError::TypeMismatch { .. })
        ));
        assert!(matches!(
            container.remove_owned::<Plain>(&id),
            Err(InternalError::TypeMismatch { .. })
        ));
        // 原 entry 与 ID 保持有效，值没有被移出或销毁。
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(container.borrow::<Tracked>(&id).unwrap().num, 9);

        // 诊断带预期／实际类型。
        match container.borrow::<Plain>(&id) {
            Err(error) => {
                let text = error.to_string();
                assert!(text.contains("Tracked"), "{text}");
                assert!(text.contains("Plain"), "{text}");
            }
            Ok(_) => panic!("wrong type was accepted"),
        }
    }

    #[test]
    fn a08_successful_removal_transfers_the_value_and_invalidates_the_id() {
        let mut container = container();
        let drops = Arc::new(AtomicUsize::new(0));
        let id = container.insert_owned(tracked(3, &drops)).unwrap();

        let taken = container.remove_owned::<Tracked>(&id).unwrap();
        assert_eq!(taken.num, 3);
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        drop(taken);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(matches!(
            container.borrow::<Tracked>(&id),
            Err(InternalError::DataNotFound { .. })
        ));

        drop(container);
        // 已移出的值不会在容器结束时重复 drop。
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a09_destroy_and_teardown_drop_each_value_once() {
        let mut container = container();
        let drops = Arc::new(AtomicUsize::new(0));
        let destroyed = container.insert_owned(tracked(1, &drops)).unwrap();
        let _remaining = container.insert_owned(tracked(2, &drops)).unwrap();

        container.destroy(&destroyed).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        assert!(matches!(
            container.destroy(&destroyed),
            Err(InternalError::DataNotFound { .. })
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        drop(container);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn a10_a_retained_id_neither_keeps_the_value_alive_nor_revives_it() {
        let mut container = container();
        let drops = Arc::new(AtomicUsize::new(0));
        let id = container.insert_owned(tracked(1, &drops)).unwrap();
        let retained = id.clone();

        container.destroy(&id).unwrap();
        // ID 只保留身份元数据，不保活业务值。
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(matches!(
            container.borrow::<Tracked>(&retained),
            Err(InternalError::DataNotFound { .. })
        ));

        drop(container);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a11_data_id_exhaustion_is_checked_and_leaves_existing_values_intact() {
        let execution = ExecutionIdentity::with_starts(u64::MAX - 1, 0);
        let mut container = DataContainer::with_identity(execution);

        let last = container.insert_owned(Plain(1)).unwrap();
        assert_eq!(last.seq(), u64::MAX - 1);

        assert!(matches!(
            container.insert_owned(Plain(2)),
            Err(InternalError::IdSpaceExhausted { .. })
        ));
        // 耗尽不破坏已存值，也不重新发出旧 ID。
        assert_eq!(container.borrow::<Plain>(&last).unwrap().0, 1);
        let retained = last.clone();
        assert_eq!(container.borrow::<Plain>(&retained).unwrap().0, 1);
        match container.insert_owned(Plain(3)) {
            Err(error) => assert!(error.to_string().contains("DataId")),
            Ok(id) => panic!("exhausted container handed out {id}"),
        }
    }

    #[test]
    fn a12_unit_outputs_are_rejected_without_consuming_an_id() {
        let mut container = container();

        let first = container.insert_owned(Plain(1)).unwrap();
        assert!(matches!(
            container.insert_owned(()),
            Err(InternalError::UnitNotStorable)
        ));
        let second = container.insert_owned(Plain(2)).unwrap();

        // unit 没有生成 DataId，也没有留下 entry。
        assert_eq!(second.seq(), first.seq() + 1);
        assert_eq!(container.borrow::<Plain>(&first).unwrap().0, 1);
        assert_eq!(container.borrow::<Plain>(&second).unwrap().0, 2);
    }

    #[test]
    fn a13_non_send_and_non_sync_values_are_storable() {
        let mut container = container();

        let shared = container.insert_owned(Rc::new(5u32)).unwrap();
        let cell = container.insert_owned(Cell::new(7u32)).unwrap();

        assert_eq!(**container.borrow::<Rc<u32>>(&shared).unwrap(), 5);
        let borrowed_cell = container.borrow::<Cell<u32>>(&cell).unwrap();
        assert_eq!(borrowed_cell.get(), 7);
        borrowed_cell.set(9);
        assert_eq!(container.borrow::<Cell<u32>>(&cell).unwrap().get(), 9);
    }

    #[test]
    fn a14_shortening_the_borrow_allows_mutation() {
        let mut container = container();
        let id = container.insert_owned(Plain(4)).unwrap();

        {
            let borrowed = container.borrow::<Plain>(&id).unwrap();
            assert_eq!(borrowed.0, 4);
        }

        container.destroy(&id).unwrap();
        assert!(matches!(
            container.borrow::<Plain>(&id),
            Err(InternalError::DataNotFound { .. })
        ));
    }

    #[test]
    fn a14_shortening_the_borrow_allows_removal() {
        let mut container = container();
        let drops = Arc::new(AtomicUsize::new(0));
        let id = container.insert_owned(tracked(8, &drops)).unwrap();

        let observed = {
            let borrowed = container.borrow::<Tracked>(&id).unwrap();
            borrowed.num
        };
        assert_eq!(observed, 8);

        let taken = container.remove_owned::<Tracked>(&id).unwrap();
        assert_eq!(taken.num, 8);
        drop(taken);
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        // 旧 ID 失效：读取、再次移除与销毁都被拒绝。
        assert!(matches!(
            container.borrow::<Tracked>(&id),
            Err(InternalError::DataNotFound { .. })
        ));
        assert!(matches!(
            container.remove_owned::<Tracked>(&id),
            Err(InternalError::DataNotFound { .. })
        ));
        assert!(matches!(
            container.destroy(&id),
            Err(InternalError::DataNotFound { .. })
        ));
    }

    #[test]
    fn a14_shortening_the_borrow_allows_insertion() {
        let mut container = container();
        let original = container.insert_owned(Plain(1)).unwrap();

        {
            let borrowed = container.borrow::<Plain>(&original).unwrap();
            assert_eq!(borrowed.0, 1);
        }

        let added = container.insert_owned(Plain(2)).unwrap();

        // 原值与新值都有效，新 ID 不复用旧 ID。
        assert_eq!(container.borrow::<Plain>(&original).unwrap().0, 1);
        assert_eq!(container.borrow::<Plain>(&added).unwrap().0, 2);
        assert_ne!(original, added);
        assert_eq!(added.seq(), original.seq() + 1);
    }

    #[test]
    fn a15_container_never_looks_values_up_by_type() {
        // 只按 DataId 寻址：容器没有按类型取值的入口，因此"按类型猜实例"不可表达。
        let mut container = container();
        let id: DataId = container.insert_owned(Plain(1)).unwrap();
        assert_eq!(container.borrow::<Plain>(&id).unwrap().0, 1);
    }

    #[test]
    fn i06_collections_are_single_entries_borrowed_as_a_whole() {
        let mut container = container();
        let collection = container.insert_owned(vec![Plain(1), Plain(2)]).unwrap();

        // 集合作为整体取得一个 DataId。本任务没有 item 调用路径，因此这里只证明完整
        // 集合可按身份借用，不声明 item 的身份规则（那条由 V21-08 证明）。
        let borrowed = container.borrow::<Vec<Plain>>(&collection).unwrap();
        assert_eq!(borrowed.len(), 2);
        assert_eq!(borrowed[1].0, 2);
    }

    #[test]
    fn a16_containers_from_one_identity_share_the_data_id_sequence() {
        let identity = ExecutionIdentity::new();
        let mut first = DataContainer::with_identity(Arc::clone(&identity));
        let from_first = first.insert_owned(Plain(1)).unwrap();

        // 先分配，再克隆身份构造第二个容器。
        let mut second = DataContainer::with_identity(Arc::clone(&identity));
        let from_second = second.insert_owned(Plain(2)).unwrap();

        // 同一身份根只有一条 DataId 序列；第二个容器不会从 0 重新开始。
        assert_eq!((from_first.seq(), from_second.seq()), (0, 1));
        assert_ne!(from_first, from_second);

        // entry 表仍按容器独立：身份相同不赋予存在性。
        assert!(matches!(
            first.borrow::<Plain>(&from_second),
            Err(InternalError::DataNotFound { .. })
        ));
    }

    /// C12／C14：collector 建构区与移动／完成都在容器内部完成。
    #[test]
    fn v21_03_collector_area_moves_and_finishes_inside_the_container() {
        let identity = ExecutionIdentity::new();
        let mut container = DataContainer::with_identity(Arc::clone(&identity));
        let ids = CollectorIdAllocator::new(Arc::clone(&identity));
        let collector = ids.allocate().unwrap();

        container.begin_collector::<Tracked>(&collector).unwrap();
        let id = container
            .insert_owned(Tracked {
                num: 7,
                drops: Arc::new(AtomicUsize::new(0)),
            })
            .unwrap();
        assert_eq!(id.seq(), 0);
        assert_eq!(container.collector_len(&collector).unwrap(), 0);

        container.move_into_collector(&collector, &id).unwrap();
        assert_eq!(container.collector_len(&collector).unwrap(), 1);
        assert_eq!(container.collector_moves(&collector).unwrap(), 1);
        assert!(
            container.borrow::<Tracked>(&id).is_err(),
            "old identity is invalid"
        );

        // 完成的 Vec 才获得新的普通 DataId。
        let finished = container.finish_collector(&collector).unwrap();
        assert_eq!(finished.seq(), 1);
        assert_eq!(
            container.borrow::<Vec<Tracked>>(&finished).unwrap().len(),
            1
        );
        assert!(matches!(
            container.collector_len(&collector),
            Err(InternalError::CollectorNotFound { .. })
        ));
    }

    /// C17 证据：元素类型判定使用真实 `TypeId`，不比较类型名，也不误用 `Box<dyn Any>` 的 TypeId。
    #[test]
    fn v21_03_element_check_uses_real_type_id_not_the_stored_name() {
        let identity = ExecutionIdentity::new();
        let mut container = DataContainer::with_identity(Arc::clone(&identity));
        let collector = CollectorIdAllocator::new(Arc::clone(&identity))
            .allocate()
            .unwrap();
        container.begin_collector::<Tracked>(&collector).unwrap();

        let id = container.insert_owned(Other).unwrap();
        // test-only 故障：把 entry 的类型名改成与 collector 元素同名，类型身份仍是 `Other`。
        container.entries.get_mut(&id.seq()).unwrap().type_name = std::any::type_name::<Tracked>();

        let error = container.move_into_collector(&collector, &id).unwrap_err();
        assert!(
            matches!(error, InternalError::TypeMismatch { .. }),
            "type identity must win over the recorded name, got {error:?}"
        );
        assert_eq!(container.collector_moves(&collector).unwrap(), 0);
        assert!(
            container.borrow::<Other>(&id).is_ok(),
            "rejected move leaves the entry untouched"
        );
    }

    /// C13：容器级 collector 身份诊断：`()` 元素、foreign 句柄、清理后不可用。
    #[test]
    fn v21_03_collector_identity_diagnostics() {
        let identity = ExecutionIdentity::new();
        let mut container = DataContainer::with_identity(Arc::clone(&identity));
        let ids = CollectorIdAllocator::new(Arc::clone(&identity));

        let unit = ids.allocate().unwrap();
        assert!(matches!(
            container.begin_collector::<()>(&unit),
            Err(InternalError::UnitNotCollectible)
        ));

        let foreign = CollectorIdAllocator::new(ExecutionIdentity::new())
            .allocate()
            .unwrap();
        assert!(matches!(
            container.begin_collector::<Tracked>(&foreign),
            Err(InternalError::CollectorForeignExecution { .. })
        ));
        assert!(matches!(
            container.collector_len(&foreign),
            Err(InternalError::CollectorForeignExecution { .. })
        ));

        let collector = ids.allocate().unwrap();
        container.begin_collector::<Tracked>(&collector).unwrap();
        container.destroy_collector(&collector).unwrap();
        assert!(matches!(
            container.collector_len(&collector),
            Err(InternalError::CollectorNotFound { .. })
        ));
        assert!(matches!(
            container.destroy_collector(&collector),
            Err(InternalError::CollectorNotFound { .. })
        ));
    }
}
