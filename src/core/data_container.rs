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

#[cfg(test)]
#[path = "../../tests/internal/core/data_container_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/internal/core/data_container_observe.rs"]
pub(crate) mod observe;
