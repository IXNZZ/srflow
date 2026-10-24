//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::context::*;

/// 可失败叶子调用驱动：叶子以 `Result<O, BodyError>` 表达执行错误。
///
/// 成功时与 [`invoke_leaf`] 相同地登记 owned 输出；失败时叶子 guard 只标记执行错误并
/// 传播，**不清理 caller 的 Scope**——沿用 caller Scope 的叶子不承担其退出责任。
pub(crate) async fn invoke_fallible_leaf<T, X, O, F>(
    context: &mut ExecutionContext,
    scope: &ScopeId,
    input: &RefId,
    output: &RefId,
    config: X,
    leaf: F,
) -> Result<DataId, BodyError>
where
    T: Any,
    X: 'static,
    O: Any,
    F: for<'a> AsyncFnOnce(&'a T, X) -> Result<O, BodyError>,
{
    let mut guard = context.enter(InvocationKind::Leaf, scope, false)?;
    match crate::core::context::observe::fallible_leaf_step(
        &mut guard, scope, input, output, config, leaf,
    )
    .await
    {
        Ok(id) => {
            guard.complete();
            Ok(id)
        }
        Err(error) => {
            guard.failed_with(&error);
            Err(error)
        }
    }
}
pub(crate) async fn fallible_leaf_step<T, X, O, F>(
    guard: &mut InvocationGuard<'_>,
    scope: &ScopeId,
    input: &RefId,
    output: &RefId,
    config: X,
    leaf: F,
) -> Result<DataId, BodyError>
where
    T: Any,
    X: 'static,
    O: Any,
    F: for<'a> AsyncFnOnce(&'a T, X) -> Result<O, BodyError>,
{
    let produced = {
        let shared: &ExecutionContext = guard;
        let borrowed = shared.resolve::<T>(scope, input)?;
        leaf(borrowed, config).await?
    };
    Ok(guard.register_owned(scope, output, produced)?)
}
/// 无输出叶子调用驱动：`()` 输出不登记业务 Data，也不产生 `DataId`。
///
/// 顺序与 [`invoke_leaf`] 相同，但成功后不调用 `register_owned`（`()` 不是业务 Data）。
pub(crate) async fn invoke_leaf_unit<T, X, F>(
    context: &mut ExecutionContext,
    scope: &ScopeId,
    input: &RefId,
    config: X,
    leaf: F,
) -> Result<(), BodyError>
where
    T: Any,
    X: 'static,
    F: for<'a> AsyncFnOnce(&'a T, X) -> (),
{
    let guard = context.enter(InvocationKind::Leaf, scope, false)?;
    let outcome = {
        let shared: &ExecutionContext = &guard;
        let borrowed = shared.resolve::<T>(scope, input);
        match borrowed {
            Ok(borrowed) => {
                leaf(borrowed, config).await;
                Ok(())
            }
            Err(error) => Err(error),
        }
    };
    match outcome {
        Ok(()) => {
            guard.complete();
            Ok(())
        }
        Err(error) => {
            let body_error = BodyError::from(error);
            guard.failed_with(&body_error);
            Err(body_error)
        }
    }
}
/// 边界调用驱动：建立 child Scope、导入输入、运行 body、由边界整组 Export 并关闭。
///
/// body 使用同一个 Context；它可以通过 `create_child` 建立 Item／Round 等 descendant
/// 作用域，这些作用域由本边界的 Scope 退出清理覆盖。body 失败时 guard 走执行错误
/// 退出：清理本边界仍负责的 Scope 后把失败交给调用方。`config` 与叶子驱动同样原样透传。
pub(crate) async fn invoke_boundary<X, F>(
    context: &mut ExecutionContext,
    imports: &[ImportSlot],
    declared: &[RefId],
    outputs: &mut Vec<ExportSlot>,
    config: X,
    body: F,
) -> Result<ScopeId, BodyError>
where
    X: 'static,
    F: for<'a> AsyncFnOnce(&'a mut ExecutionContext, &'a ScopeId, X) -> Result<(), BodyError>,
{
    let caller = context.current_scope().ok_or(ScopeError::Invariant {
        violated: "a boundary invocation requires an active caller frame",
    })?;
    // 先做全部可失败检查（终止、可见范围、caller Active），再建立 child：
    // 这样"child 已建立但无法进入 guard"不可能出现在可返回路径上。
    context.require_boundary_entry(&caller)?;
    let child = context.create_child(&caller)?;
    // Import 在 guard 生效前完成显式输入装配；失败时立刻显式清理刚建立的 child，
    // 不留孤立的、不受 guard 保护的 Scope，也不留下部分输入绑定。
    if let Err(error) = context.import_batch(&child, &caller, imports) {
        // 建立阶段失败属于首次执行失败：记录终止与定位，并保存原始诊断；清理失败
        // 另行记录（`record_cleanup_failure`），不覆盖原错误。
        context.terminate(
            TerminationKind::BodyError,
            Some(child.clone()),
            "boundary input assembly failed",
            Some(error.clone()),
        );
        if let Err(cleanup_error) = context.abort(&child) {
            context.record_cleanup_failure(child.clone(), cleanup_error);
        }
        return Err(error.into());
    }
    // 预检已保证这条进入是不可失败的内部不变量：此后 body／finalize 的失败都在 guard
    // 生效后发生，由受控退出清理 child 与未完成责任。
    let mut guard = context
        .enter(InvocationKind::Boundary, &child, true)
        .expect("boundary entry after its pre-checks cannot fail");
    match crate::core::context::observe::boundary_step(
        &mut guard, &child, declared, outputs, config, body,
    )
    .await
    {
        Ok(()) => {
            guard.complete();
            Ok(child)
        }
        Err(error) => {
            // 边界自身与尚未关闭的 descendant 由 guard 的执行错误退出清理。
            guard.failed_with(&error);
            Err(error)
        }
    }
}
pub(crate) async fn boundary_step<X, F>(
    guard: &mut InvocationGuard<'_>,
    child: &ScopeId,
    declared: &[RefId],
    outputs: &mut Vec<ExportSlot>,
    config: X,
    body: F,
) -> Result<(), BodyError>
where
    X: 'static,
    F: for<'a> AsyncFnOnce(&'a mut ExecutionContext, &'a ScopeId, X) -> Result<(), BodyError>,
{
    body(&mut *guard, child, config).await?;
    guard.finalize(child, declared, outputs)?;
    guard.release_responsibility(child);
    Ok(())
}

impl ExecutionContext {
    /// 窄只读观察：target-aware 的 refs／owned 快照（区分完整 Data 与 CollectionItem）。
    #[cfg(test)]
    pub(crate) fn snapshot_targets_probe(
        &self,
        scope: &ScopeId,
    ) -> Result<crate::core::scope::TargetSnapshotPair, ScopeError> {
        self.coordinator.snapshot_targets_probe(scope)
    }

    /// cfg(test) 只读观察：下一个将被分配的 `DataId` 序号。
    #[cfg(test)]
    pub(crate) fn next_data_id_probe(&self) -> Option<u64> {
        self.coordinator.next_data_id_probe()
    }

    /// 测试故障注入：把一个 target 直接写入指定 Scope 的一个本地位置（只改元数据）。
    ///
    /// 用于构造 Root 输出（类型不符／alias／CollectionItem／foreign Execution）与
    /// Item 输入（陈旧 cap／imported 目标）等防御反例；不提供业务旁路，也不改变判断路径。
    #[cfg(test)]
    pub(crate) fn inject_scope_target_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        target: crate::core::scope::RefTarget,
    ) {
        self.coordinator
            .inject_target_probe(scope, position, target);
    }

    /// 测试故障注入：把某个位置上的 CollectionItem 的 lifetime cap 换成指定 Scope。
    ///
    /// 只用于构造"上一 Item 已关闭"的 cap 失效反例；不改变其它元数据，也不成为生产入口。
    #[cfg(test)]
    pub(crate) fn replace_item_cap_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        cap: &ScopeId,
    ) {
        self.coordinator
            .replace_item_cap_probe(scope, position, cap);
    }

    /// 窄只读观察：某位置当前绑定的完整 Data 身份（item 目标返回空）。
    #[cfg(test)]
    pub(crate) fn target_data_id_probe(
        &self,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<Option<crate::core::identity::DataId>, ScopeError> {
        self.coordinator.target_data_id_probe(scope, position)
    }

    /// 测试故障注入：直接销毁存储中的某个 Data entry（构造"清理前提失败"）。
    ///
    /// 只用于在最底层的真实收口入口制造后置前提故障；不提供业务数据旁路，也不修改
    /// 前提检查本体。
    #[cfg(test)]
    pub(crate) fn destroy_probe(&mut self, id: &crate::core::identity::DataId) {
        self.coordinator.destroy_probe(id);
    }

    /// 窄只读观察：collector 已移动进建构区的元素个数（cfg(test) 证据用）。
    ///
    /// 不改变 collector 或计数；finish／清理会移除计数，证据须在移除前取得。
    #[cfg(test)]
    pub(crate) fn collector_moves_probe(
        &self,
        collector: &CollectorId,
    ) -> Result<usize, ScopeError> {
        self.coordinator.collector_moves_probe(collector)
    }

    /// 测试观测：本 Execution 身份根的稳定地址。
    #[cfg(test)]
    pub(crate) fn identity_probe(&self) -> *const () {
        self.coordinator.identity_probe()
    }

    /// 测试观测：内部协调组件的稳定地址。
    #[cfg(test)]
    pub(crate) fn coordinator_probe(&self) -> *const () {
        self.coordinator.coordinator_probe()
    }

    /// cfg(test) 只读：控制器当前登记的待回收旧值。
    #[cfg(test)]
    pub(crate) fn pending_probe(
        &self,
        controller: &ScopeId,
    ) -> Option<Vec<crate::core::identity::DataId>> {
        self.coordinator.pending_probe(controller)
    }

    /// cfg(test) 窄故障：写入坏 state metadata（声明类型）。
    #[cfg(test)]
    pub(crate) fn corrupt_state_type_probe(
        &mut self,
        state: &ControlStateId,
        expected: std::any::TypeId,
        expected_name: &'static str,
    ) -> Result<(), ScopeError> {
        self.coordinator
            .corrupt_state_type_probe(state, expected, expected_name)
    }

    /// cfg(test) 窄故障：替换某个本地位置上 item target 的 cap Scope。
    #[cfg(test)]
    pub(crate) fn corrupt_item_cap_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        cap: ScopeId,
    ) -> Result<(), ScopeError> {
        self.coordinator
            .corrupt_item_cap_probe(scope, position, cap)
    }

    /// 测试观测：某 DataId 是否仍在本 Execution 内存活（只读诊断，不授予读取权）。
    #[cfg(test)]
    pub(crate) fn alive_probe(&self, id: &crate::core::identity::DataId) -> bool {
        self.coordinator.alive_probe(id)
    }

    /// 测试观测：某 DataId 当前的责任 Scope（只读诊断，不授予读取权）。
    #[cfg(test)]
    pub(crate) fn owner_probe(
        &self,
        id: &crate::core::identity::DataId,
    ) -> Result<ScopeId, ScopeError> {
        self.coordinator.owner_probe(id)
    }

    /// 测试观测：唯一 DataContainer 的稳定地址。
    #[cfg(test)]
    pub(crate) fn container_probe(&self) -> *const () {
        self.coordinator.container_probe()
    }

    /// 测试观测：guard 在 Drop 中执行清理的次数。
    #[cfg(test)]
    pub(crate) fn cleanup_events(&self) -> usize {
        self.cleanup_events
    }
}

/// 测试观测：Context 最终析构事件。
#[cfg(test)]
impl Drop for ExecutionContext {
    fn drop(&mut self) {
        creation_counts::record_event("context-drop");
    }
}

#[cfg(test)]
impl InvocationGuard<'_> {
    /// 测试观测：立即触发本 guard 的 Drop 校验（等价于在当前位置丢弃 guard）。
    #[cfg(test)]
    pub(crate) fn exit_for_probe(self) {}
}

/// cfg(test) 观测：清理开始（责任集合首个 Scope 标签）。
pub(super) fn record_cleanup_start(scopes: &[ScopeId]) {
    let label = scopes
        .first()
        .map(|scope| scope.seq().to_string())
        .unwrap_or_else(|| String::from("none"));
    crate::core::context::creation_counts::record_event(&format!("cleanup-start:{label}"));
}

/// cfg(test) 观测：清理结束（计数 + 标签），需可变 Context 以累加清理事件。
pub(super) fn note_cleanup_end(context: &mut ExecutionContext, scopes: &[ScopeId]) {
    context.cleanup_events += 1;
    crate::core::context::creation_counts::count_guard_cleanup();
    let label = scopes
        .first()
        .map(|scope| scope.seq().to_string())
        .unwrap_or_else(|| String::from("none"));
    crate::core::context::creation_counts::record_event(&format!("cleanup-end:{label}"));
}

/// cfg(test) 观测：即便发生 Root 内失败清理，也按快照阶段记录 Root 现场。
pub(super) fn record_root_after_failure_cleanup(
    context: &ExecutionContext,
    frame_scope: &Option<ScopeId>,
    frame_index: usize,
) {
    if let Some(scope) = frame_scope.clone()
        && context.frames[frame_index].kind() == InvocationKind::Root
    {
        crate::core::test_support::record_strict_root_snapshot(
            context,
            &scope,
            crate::core::test_support::RootSnapshotPhase::AfterFailureCleanup,
            0,
            &[],
        );
    }
}

/// cfg(test) 观测：frame 退出事件并按当前栈顶截断 frame 栈。
pub(super) fn observe_frame_exit(context: &mut ExecutionContext, frame_index: usize) {
    let kind = match context.frames[frame_index].kind() {
        InvocationKind::Root => "root",
        InvocationKind::Boundary => "boundary",
        InvocationKind::Leaf => "leaf",
    };
    let scope = context.frames[frame_index].scope().cloned();
    context.frames.truncate(frame_index);
    match &scope {
        Some(scope) => crate::core::context::creation_counts::record_event(&format!(
            "frame-exit:{kind}:{}",
            scope.seq()
        )),
        None => crate::core::context::creation_counts::record_event(&format!("frame-exit:{kind}")),
    }
}

/// cfg(test) 故障注入：第二条清理诊断（只登记、不覆盖首诊断）。
pub(super) fn apply_later_cleanup_diagnostic(context: &mut ExecutionContext) {
    let Some(later) = crate::core::test_support::take_later_cleanup_diagnostic() else {
        return;
    };
    let scope = context
        .cleanup_failure
        .as_ref()
        .expect("just stored the first diagnostic")
        .scope()
        .clone();
    context.record_cleanup_failure(scope, later);
}
