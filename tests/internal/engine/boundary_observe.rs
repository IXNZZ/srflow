//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::engine::boundary::*;

pub(crate) fn probe_foreign_identity(ctx: &ExecutionContext, caller: &ScopeId) {
    let Some((foreign_scope, foreign_data)) = take_foreign_access_fault() else {
        return;
    };
    let local_snapshot = |ctx: &ExecutionContext| {
        ctx.snapshot_targets_probe(caller)
            .map(|(refs, owned)| (refs.len(), owned.len()))
            .unwrap_or((0, 0))
    };
    let (refs_before, owned_before) = local_snapshot(ctx);
    let state_detail = match ctx.state(&foreign_scope) {
        Ok(_) => String::from("unexpectedly accepted"),
        Err(error) => format!("{error}"),
    };
    crate::core::engine::observe::record_foreign_access(ForeignAccessRecord {
        operation: "state(foreign-scope)",
        rejected: ctx.state(&foreign_scope).is_err(),
        detail: state_detail,
        local_refs_before: refs_before,
        local_refs_after: refs_before,
        local_owned_before: owned_before,
        local_owned_after: owned_before,
    });
    let alive = ctx.alive_probe(&foreign_data);
    let owner_detail = match ctx.owner_probe(&foreign_data) {
        Ok(owner) => format!("unexpectedly owned by {owner:?}"),
        Err(error) => format!("{error}"),
    };
    let (refs_after, owned_after) = local_snapshot(ctx);
    crate::core::engine::observe::record_foreign_access(ForeignAccessRecord {
        operation: "alive/owner(foreign-data)",
        rejected: !alive && ctx.owner_probe(&foreign_data).is_err(),
        detail: format!("alive={alive}; owner: {owner_detail}"),
        local_refs_before: refs_before,
        local_refs_after: refs_after,
        local_owned_before: owned_before,
        local_owned_after: owned_after,
    });
}
pub(crate) fn record_export_snapshot(ctx: &ExecutionContext, phase: &'static str, scope: &ScopeId) {
    if let Ok((refs, owned)) = ctx.snapshot_targets_probe(scope) {
        let owned_alive = owned
            .iter()
            .map(|id| (id.clone(), ctx.alive_probe(id), ctx.owner_probe(id).ok()))
            .collect();
        crate::core::engine::observe::record_export_snapshot(ExportSnapshotRecord {
            phase,
            scope: scope.clone(),
            refs,
            owned,
            owned_alive,
            next_data_id: ctx.next_data_id_probe(),
        });
    }
}
pub(crate) fn record_controller_scope_pair(
    guard: &InvocationGuard<'_>,
    phase: &'static str,
    round: &ScopeId,
    controller: &ScopeId,
) {
    crate::core::test_support::record_controller_scope_snapshot(
        crate::core::test_support::capture_controller_scope_snapshot(
            guard, phase, "round", round, None,
        ),
    );
    crate::core::test_support::record_controller_scope_snapshot(
        crate::core::test_support::capture_controller_scope_snapshot(
            guard,
            phase,
            "controller",
            controller,
            Some(controller),
        ),
    );
}
pub(crate) fn inject_export_occupy_fault(
    ctx: &mut ExecutionContext,
    caller: &ScopeId,
    slots: &[HandoverSlot],
) {
    let Some(index) = take_export_occupy_fault() else {
        return;
    };
    let Some(slot) = slots.get(index) else {
        panic!("export occupy fault requires an existing slot index");
    };
    ctx.register_owned(caller, &slot.output, 0u8)
        .expect("occupied caller position must be free before the fault");
}
pub(crate) fn inject_each_finish_occupy_fault(
    guard: &mut InvocationGuard<'_>,
    each: &ScopeId,
    slots: &[EachSlot],
) {
    let Some(index) = take_each_finish_occupy_fault() else {
        return;
    };
    let Some(slot) = slots.get(index) else {
        panic!("each finish occupy fault requires an existing slot index");
    };
    guard
        .register_owned(each, &slot.local, 0u8)
        .expect("occupied each local position must be free before the fault");
}
pub(crate) fn inject_control_delivery_occupy_fault(
    ctx: &mut ExecutionContext,
    scope: &ScopeId,
    positions: &[RefId],
    hop: ControlDeliveryOccupyHop,
) {
    let Some(index) = take_control_delivery_occupy_fault(hop) else {
        return;
    };
    let Some(position) = positions.get(index) else {
        panic!("control delivery occupy fault requires an existing slot index");
    };
    ctx.register_owned(scope, position, 0u8)
        .expect("occupied control delivery position must be free before the fault");
}
pub(crate) fn record_each_finish_reject_scene(
    ctx: &ExecutionContext,
    each: &ScopeId,
    caller: &ScopeId,
    slots: &[EachSlot],
    collectors: &[CollectorId],
    error: &ScopeError,
) {
    let collector_moves: Vec<Option<usize>> = collectors
        .iter()
        .map(|collector| ctx.collector_moves_probe(collector).ok())
        .collect();
    let collector_present: Vec<bool> = collector_moves.iter().map(Option::is_some).collect();
    let (each_refs, each_owned) = ctx.snapshot_targets_probe(each).unwrap_or_default();
    let (caller_refs, caller_owned) = ctx.snapshot_targets_probe(caller).unwrap_or_default();
    crate::core::test_support::record_each_finish_reject(
        crate::core::test_support::EachFinishRejectScene {
            each: each.clone(),
            caller: caller.clone(),
            slot_locals: slots.iter().map(|slot| slot.local.clone()).collect(),
            slot_outputs: slots.iter().map(|slot| slot.output.clone()).collect(),
            collectors: collectors.to_vec(),
            collector_present,
            collector_moves,
            each_refs,
            each_owned,
            caller_refs,
            caller_owned,
            rejected: error.clone(),
        },
    );
}
pub(crate) fn record_controller(
    kind: &'static str,
    at: &'static str,
    caller: &ScopeId,
    controller: &ScopeId,
    round: Option<&ScopeId>,
    index: usize,
) {
    crate::core::engine::observe::record_controller_round(
        crate::core::engine::observe::ControllerRoundRecord {
            kind,
            at,
            caller: caller.clone(),
            controller: controller.clone(),
            round: round.cloned(),
            index,
        },
    );
}
pub(crate) fn record_delivery(
    kind: &'static str,
    hop: &'static str,
    from: &ScopeId,
    to: &ScopeId,
    pairs: Vec<(RefId, RefId)>,
) {
    crate::core::engine::observe::record_controller_delivery(
        crate::core::engine::observe::ControllerDeliveryRecord {
            kind,
            hop,
            from: from.clone(),
            to: to.clone(),
            pairs,
        },
    );
}
pub(crate) fn probe_closed_child(
    guard: &mut InvocationGuard<'_>,
    child: &ScopeId,
    slots: &[HandoverSlot],
) {
    match guard.enter(InvocationKind::Boundary, child, true) {
        Ok(entered) => entered.complete(),
        Err(error) => {
            crate::core::engine::observe::record_scope_rejection(ScopeRejectionRecord {
                scope: child.clone(),
                operation: "enter-closed-child",
                error,
            });
        }
    }
    // 旧位置的受控读取入口：位置取本 Step 的真实声明出口（必然曾绑定在 child 内）。
    if let Some(slot) = slots.first()
        && let Err(error) = guard.resolve::<u8>(child, &slot.exit)
    {
        crate::core::engine::observe::record_scope_rejection(ScopeRejectionRecord {
            scope: child.clone(),
            operation: "resolve-closed-child",
            error,
        });
    }
}
pub(crate) fn inject_import_occupy_fault(
    ctx: &mut ExecutionContext,
    definition: &Definition<'_>,
    child_body: BodyId,
    child: &ScopeId,
) {
    let Some(index) = take_import_occupy_fault() else {
        return;
    };
    let position = definition
        .body(child_body)
        .imports()
        .get(index)
        .copied()
        .expect("import occupy fault requires an existing import slot");
    let handle = crate::core::engine::identity::PositionHandle {
        definition: definition.id(),
        position,
    };
    let record = definition
        .record(handle)
        .expect("import occupy fault targets a registered position");
    ctx.register_owned(child, record.core(), 0u8)
        .expect("import occupy fault expects a free child position");
}

/// cfg(test) 故障注入：按 slots 的声明输出位置安装控制投递占用故障。
pub(crate) fn inject_control_delivery_occupy_fault_from_slots<'a, I>(
    ctx: &mut ExecutionContext,
    caller: &ScopeId,
    slots: I,
    hop: ControlDeliveryOccupyHop,
) where
    I: IntoIterator<Item = &'a RefId>,
{
    let destinations: Vec<RefId> = slots.into_iter().cloned().collect();
    inject_control_delivery_occupy_fault(ctx, caller, &destinations, hop);
}

/// cfg(test) 观测 + 故障注入：Each 控制 Scope 退出前的现场与清理故障。
pub(crate) fn observe_each_control_exit(
    guard: &mut InvocationGuard<'_>,
    caller: &ScopeId,
    each: &ScopeId,
    item: &ScopeId,
    index: usize,
) {
    record_scope_state(guard, "each-item-control-closed", item);
    record_controller("each", "each-control-exit", caller, each, None, index);
    if take_each_control_cleanup_fault() {
        break_owned_entry(guard, each);
    }
}

/// cfg(test)：Retry／Iter 两跳整组交付的目的位置预占跳点。
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlDeliveryOccupyHop {
    RoundToController,
    /// 第二跳：controller → caller（预占 caller 的 `slot.output`）。
    ControllerToCaller,
}

#[cfg(test)]
thread_local! {
    static CONTROL_CLEANUP_FAULT: Cell<bool> = const { Cell::new(false) };
    static EXPORT_OCCUPY_FAULT: Cell<Option<usize>> = const { Cell::new(None) };
    static IMPORT_OCCUPY_FAULT: Cell<Option<usize>> = const { Cell::new(None) };
    static RETRY_ROUND_CLEANUP_FAULT: Cell<bool> = const { Cell::new(false) };
    static ITER_DELIVERY_CLEANUP_FAULT: Cell<bool> = const { Cell::new(false) };
    static BOUNDARY_FAILURE_CLEANUP_FAULT: Cell<bool> = const { Cell::new(false) };
    static EACH_CONTROL_CLEANUP_FAULT: Cell<bool> = const { Cell::new(false) };
    static EACH_FINISH_OCCUPY_FAULT: Cell<Option<usize>> = const { Cell::new(None) };
    static CONTROL_DELIVERY_OCCUPY_FAULT: Cell<Option<(ControlDeliveryOccupyHop, usize)>> =
        const { Cell::new(None) };
    /// cfg(test)：在 caller frame 中尝试访问另一个 Execution 的 Scope／Data 身份。
    static FOREIGN_ACCESS_FAULT: std::cell::RefCell<Option<(ScopeId, crate::core::identity::DataId)>> =
        const { std::cell::RefCell::new(None) };
}

/// cfg(test)：安装一次跨执行身份访问样例（在第二次执行的 caller frame 中触发）。
#[cfg(test)]
pub(crate) fn install_foreign_access_fault(
    foreign_scope: ScopeId,
    foreign_data: crate::core::identity::DataId,
) {
    FOREIGN_ACCESS_FAULT.with(|fault| *fault.borrow_mut() = Some((foreign_scope, foreign_data)));
}

#[cfg(test)]
pub(crate) fn take_foreign_access_fault() -> Option<(ScopeId, crate::core::identity::DataId)> {
    FOREIGN_ACCESS_FAULT.with(|fault| fault.borrow_mut().take())
}

/// cfg(test)：在真实 caller frame 中尝试访问第一域身份，记录原框架拒绝与本地快照。
/// cfg(test)：安装一次 Chain Control 清理故障。
#[cfg(test)]
pub(crate) fn install_control_cleanup_fault() {
    CONTROL_CLEANUP_FAULT.with(|fault| fault.set(true));
}

/// cfg(test)：安装一次 caller 输出位置预占故障（整组 Export 预检拒绝样本）。
#[cfg(test)]
pub(crate) fn install_export_occupy_fault(index: usize) {
    EXPORT_OCCUPY_FAULT.with(|fault| fault.set(Some(index)));
}

/// cfg(test)：安装一次 child 目的位置预占故障（整组 Import 预检拒绝样本）。
#[cfg(test)]
pub(crate) fn install_import_occupy_fault(index: usize) {
    IMPORT_OCCUPY_FAULT.with(|fault| fault.set(Some(index)));
}

/// cfg(test)：安装一次 Retry 匹配轮次的 round 收口清理故障（close_control_scope 失败样本）。
#[cfg(test)]
pub(crate) fn install_retry_round_cleanup_fault() {
    RETRY_ROUND_CLEANUP_FAULT.with(|fault| fault.set(true));
}

/// cfg(test)：安装一次 IterBreak 交付前的 round 收口清理故障（deliver_round 失败样本）。
#[cfg(test)]
pub(crate) fn install_iter_delivery_cleanup_fault() {
    ITER_DELIVERY_CLEANUP_FAULT.with(|fault| fault.set(true));
}

/// cfg(test)：安装一次 Chain／Choose 边界执行错误退出的 child 清理故障（子 Scope 清理失败样本）。
#[cfg(test)]
pub(crate) fn install_boundary_cleanup_fault() {
    BOUNDARY_FAILURE_CLEANUP_FAULT.with(|fault| fault.set(true));
}

/// cfg(test)：安装一次 Each 层 Control 退出清理故障（破坏 Each 的一个 owned entry）。
#[cfg(test)]
#[allow(dead_code)] // 窄故障钩子：由 Each 定向验收样本安装（当前尚未接入消费者）
pub(crate) fn install_each_control_cleanup_fault() {
    EACH_CONTROL_CLEANUP_FAULT.with(|fault| fault.set(true));
}

/// cfg(test)：安装一次 Each 整组 finish 后项目标预占故障。样本以正式
/// `register_owned` 在 `finish_collectors` 之前占用第 `index` 个槽的 Each 本地最终
/// 输出位置，使该槽 finish 的真实前提（“目标位置未绑定”）在整组收口的中途失败。
/// 只破坏合法前提：不伪造 `Err`、不绕过 `finish_collectors`、不放宽任何判断。
#[cfg(test)]
pub(crate) fn install_each_finish_occupy_fault(index: usize) {
    EACH_FINISH_OCCUPY_FAULT.with(|fault| fault.set(Some(index)));
}

/// cfg(test)：安装一次 Retry／Iter 两跳整组交付的目的位置预占故障（由下一次匹配的跳点
/// 消费一次）。`ControllerToCaller` 预占 caller 的第 `index` 个输出位置；`RoundToController`
/// 预占 controller 的第 `index` 个桥位置。二者都让后续真实整组交付的预检拒绝。
#[cfg(test)]
#[allow(dead_code)] // 窄故障钩子：由 W06 见证正式化样本安装（消费者随后接入）
pub(crate) fn install_control_delivery_occupy_fault(hop: ControlDeliveryOccupyHop, index: usize) {
    CONTROL_DELIVERY_OCCUPY_FAULT.with(|fault| fault.set(Some((hop, index))));
}

#[cfg(test)]
pub(crate) fn take_retry_round_cleanup_fault() -> bool {
    RETRY_ROUND_CLEANUP_FAULT.with(|fault| fault.replace(false))
}

#[cfg(test)]
pub(crate) fn take_iter_delivery_cleanup_fault() -> bool {
    ITER_DELIVERY_CLEANUP_FAULT.with(|fault| fault.replace(false))
}

#[cfg(test)]
pub(crate) fn take_boundary_cleanup_fault() -> bool {
    BOUNDARY_FAILURE_CLEANUP_FAULT.with(|fault| fault.replace(false))
}

#[cfg(test)]
pub(crate) fn take_each_control_cleanup_fault() -> bool {
    EACH_CONTROL_CLEANUP_FAULT.with(|fault| fault.replace(false))
}

#[cfg(test)]
pub(crate) fn take_each_finish_occupy_fault() -> Option<usize> {
    EACH_FINISH_OCCUPY_FAULT.with(|fault| fault.take())
}

#[cfg(test)]
pub(crate) fn take_control_delivery_occupy_fault(hop: ControlDeliveryOccupyHop) -> Option<usize> {
    CONTROL_DELIVERY_OCCUPY_FAULT.with(|fault| {
        let matches = fault.get().is_some_and(|(installed, _)| installed == hop);
        if matches {
            fault.take().map(|(_, index)| index)
        } else {
            None
        }
    })
}

/// cfg(test)：在给定 Scope 登记一个 owned 临时值后立即销毁其 entry，使后续收口
/// （abort／finalize 的 cleanup_targets）真实失败——责任登记仍指向已销毁的值。
/// 这是既有 T02／T03 的清理故障套路；只在 test 构建下调用，不改变非 test 行为。
#[cfg(test)]
pub(crate) fn break_owned_entry(guard: &mut InvocationGuard<'_>, scope: &ScopeId) {
    let position =
        crate::core::ref_id::RefIdAllocator::new(crate::core::ref_id::RefIdSource::new())
            .allocate()
            .expect("fault position");
    let id = guard
        .register_owned(scope, &position, 0u8)
        .expect("fault value registration");
    guard.destroy_probe(&id);
}

#[cfg(test)]
pub(crate) fn take_control_cleanup_fault() -> bool {
    CONTROL_CLEANUP_FAULT.with(|fault| fault.replace(false))
}

#[cfg(test)]
pub(crate) fn take_export_occupy_fault() -> Option<usize> {
    EXPORT_OCCUPY_FAULT.with(|fault| fault.take())
}

#[cfg(test)]
pub(crate) fn take_import_occupy_fault() -> Option<usize> {
    IMPORT_OCCUPY_FAULT.with(|fault| fault.take())
}

/// cfg(test)：只读记录一个 Scope 的当前状态。
#[cfg(test)]
pub(crate) fn record_scope_state(ctx: &ExecutionContext, at: &'static str, scope: &ScopeId) {
    if let Ok(state) = ctx.state(scope) {
        crate::core::engine::observe::record_scope_state(ScopeStateRecord {
            at,
            scope: scope.clone(),
            state,
        });
    }
}

/// cfg(test)：只读记录一个 Scope 的本地绑定／owned／存活／owner／下一个 DataId 序号。
/// cfg(test)：在 Control 退出前登记一个 child 临时值并销毁其 entry，
/// 使 `abort` 的清理前提真实失败（责任记录保留）。
#[cfg(test)]
pub(crate) fn inject_control_cleanup_fault(guard: &mut InvocationGuard<'_>, child: &ScopeId) {
    if !take_control_cleanup_fault() {
        return;
    }
    break_owned_entry(guard, child);
}

/// cfg(test)：在边界以执行错误退出前销毁 child 已真实拥有的一个 entry，
/// 使 child 的退出清理真实失败（父层接管样本；不新增登记，终止后登记会被拒绝）。
#[cfg(test)]
pub(crate) fn inject_boundary_cleanup_fault(guard: &mut InvocationGuard<'_>, child: &ScopeId) {
    if !take_boundary_cleanup_fault() {
        return;
    }
    let owned = guard
        .snapshot_targets_probe(child)
        .map(|(_, owned)| owned)
        .unwrap_or_default();
    if let Some(id) = owned.first() {
        guard.destroy_probe(id);
    }
}
