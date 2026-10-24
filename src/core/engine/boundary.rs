//! Chain 边界与 Body 顺序执行（Engine §4、§6.5、§7）。
//!
//! 顺序固定为：caller frame 校验 → 建立直接 child → 整组 Import → 进入 Boundary Guard
//! → 顺序执行 child Body → child 本地出口 alias 装配 → 整组 Export → 关闭 child →
//! 解除责任 → complete。Control 退出复用 D04-01 已验证的
//! abort → Closed 确认 → 解除责任 → complete 组合。

use crate::core::context::{
    BodyError as CoreBodyError, ExecutionContext, InvocationGuard, InvocationKind, ItemGroupPermit,
    TerminationKind,
};
use crate::core::engine::data::Data;
#[cfg(test)]
use crate::core::engine::definition::EachSlot;
use crate::core::engine::definition::{
    ChainStep, ChooseStep, Definition, EachStep, HandoverSlot, IterStep, RetryStep,
};
use crate::core::engine::error::{BudgetTermination, ExecFailure};
use crate::core::engine::identity::BodyId;
use crate::core::engine::identity::{PositionHandle, PositionId};
#[cfg(test)]
use crate::core::engine::observe::{
    ChainCreationRecord, ChainImportRecord, ChooseSelectionRecord, ControlCompletionRecord,
    ControlReleaseRecord, ExportSnapshotRecord, ForeignAccessRecord, FrameRecord,
    ScopeRejectionRecord, ScopeStateRecord,
};
use crate::core::engine::shape::TypeDesc;
#[cfg(test)]
use crate::core::identity::CollectorId;
use crate::core::identity::ScopeId;
use crate::core::internal_error::ScopeError;
use crate::core::ref_id::RefId;
use crate::core::scope::{
    AliasSlot, ConsumeGroupOutcome, ExportSlot, ImportSlot, PromoteGroupOutcome, ScopeState,
    StateImportSlot,
};
use std::any::TypeId;
#[cfg(test)]
use std::cell::Cell;

/// 顺序执行一个 Body 的全部 Step；前一步结束后才开始后一步。
pub(crate) async fn run_body(
    ctx: &mut ExecutionContext,
    definition: &Definition<'_>,
    body: BodyId,
    scope: &ScopeId,
) -> Result<(), ExecFailure> {
    let count = definition.body(body).steps().len();
    for index in 0..count {
        definition.body(body).steps()[index]
            .execute(definition, ctx, scope)
            .await?;
    }
    Ok(())
}

/// 一个调用边界的 child Import 需求 → 整组 ImportSlot
/// （同一静态 RefId 作为 source 与 target）。
struct BoundaryImports {
    slots: Vec<ImportSlot>,
    #[cfg(test)]
    pairs: Vec<(RefId, RefId)>,
}

/// 执行一个 Chain Step：建立真实 child 边界并完成整组数据交接。
pub(crate) async fn invoke_chain(
    ctx: &mut ExecutionContext,
    caller: &ScopeId,
    step: &ChainStep,
    definition: &Definition<'_>,
) -> Result<(), ExecFailure> {
    // 1) 建立直接 child（终止／可见范围检查在 create_child 内）。
    let child = ctx
        .create_child(caller)
        .map_err(|error| ExecFailure::runtime("chain child creation rejected", &error))?;
    #[cfg(test)]
    {
        crate::core::test_support::boundary_creation_record(
            child.clone(),
            caller.clone(),
            crate::core::orchestrator::observe::ScopeRole::Flow,
            ctx.identity_probe(),
            ctx.coordinator_probe(),
            ctx.container_probe(),
        );
        crate::core::engine::observe::record_chain_creation(ChainCreationRecord {
            child: child.clone(),
            caller: caller.clone(),
            closed_before: crate::core::test_support::closed_scope_snapshot(),
        });
        observe::probe_foreign_identity(ctx, caller);
    }

    // 2) 整组 Import：同一静态 RefId 在 caller 与 child 各绑定一次（Scope-local）。
    let imports = build_imports(
        definition,
        step.child(),
        "chain import references an unregistered position",
    )?;
    #[cfg(test)]
    {
        crate::core::engine::observe::record_chain_import(ChainImportRecord {
            child: child.clone(),
            caller: caller.clone(),
            pairs: imports.pairs.clone(),
        });
        observe::inject_import_occupy_fault(ctx, definition, step.child(), &child);
    }
    if let Err(error) = ctx.import_batch(&child, caller, &imports.slots) {
        #[cfg(test)]
        {
            // 绑定快照必须在 abort 撤销引用之前取得。
            observe::record_export_snapshot(ctx, "import-rejected", &child);
        }
        ctx.terminate(
            TerminationKind::BodyError,
            Some(child.clone()),
            "chain input assembly failed",
            Some(error.clone()),
        );
        if let Err(cleanup) = ctx.abort(&child) {
            ctx.record_cleanup_failure(child.clone(), cleanup);
        }
        #[cfg(test)]
        observe::record_scope_state(ctx, "import-rejected", &child);
        return Err(ExecFailure::runtime("chain input assembly failed", &error));
    }

    #[cfg(test)]
    observe::inject_export_occupy_fault(ctx, caller, step.slots());

    // 3) 进入 Boundary Guard（预检已保证该进入是不可失败的内部不变量）。
    let mut guard = ctx
        .enter(InvocationKind::Boundary, &child, true)
        .expect("boundary entry after its pre-checks cannot fail");
    #[cfg(test)]
    crate::core::engine::observe::record_frame(FrameRecord {
        kind: "boundary",
        phase: "enter",
        scope: child.clone(),
    });

    let outcome = run_body(&mut guard, definition, step.child(), &child).await;
    #[cfg(test)]
    observe::record_scope_state(&guard, "after-body", &child);

    let result = match outcome {
        Ok(()) => {
            #[cfg(test)]
            observe::record_export_snapshot(&guard, "before", caller);
            match finish_boundary(&mut guard, &child, step.slots(), &CHAIN_LABELS) {
                Ok(()) => {
                    #[cfg(test)]
                    {
                        observe::record_export_snapshot(&guard, "committed", caller);
                        observe::record_scope_state(&guard, "after-export", &child);
                    }
                    guard.release_responsibility(&child);
                    guard.complete();
                    Ok(())
                }
                Err(error) => {
                    #[cfg(test)]
                    observe::record_export_snapshot(&guard, "rejected", caller);
                    guard.failed_with(&CoreBodyError::new("chain output handover rejected"));
                    Err(error)
                }
            }
        }
        Err(ExecFailure::Control(signal)) => {
            #[cfg(test)]
            {
                observe::record_scope_state(&guard, "control-pending", &child);
                observe::inject_control_cleanup_fault(&mut guard, &child);
            }
            let result = exit_control_boundary(guard, &child, signal);
            #[cfg(test)]
            crate::core::engine::observe::record_control_completion(ControlCompletionRecord {
                child: child.clone(),
                terminated: ctx.is_terminated(),
                termination_scope: ctx
                    .termination()
                    .and_then(|termination| termination.scope().cloned()),
                child_state: ctx.state(&child).ok(),
            });
            result
        }
        Err(error) => {
            #[cfg(test)]
            observe::inject_boundary_cleanup_fault(&mut guard, &child);
            guard.failed_with(&CoreBodyError::new("chain body failed"));
            Err(error)
        }
    };

    #[cfg(test)]
    crate::core::engine::observe::record_frame(FrameRecord {
        kind: "boundary",
        phase: "exit",
        scope: child,
    });
    result
}

/// 执行一个 Choose Step：在直接 caller frame 内匹配 selector，只为选中分支建立真实 child。
///
/// 布局（WE-T03 §5.2 公布的映射）：选择与所选分支共用一次边界建立——选择本身不建立
/// 独立 Scope，比较在 caller frame 内完成后借用立即终结（不跨可变 Context 操作）；
/// 只有选中分支的 Body 获得真实 child、Import 与 Export。无匹配且无 otherwise 时
/// 不建立任何 running Scope，直接返回不可恢复的 [`ExecFailure::NoMatchingCase`]。
pub(crate) async fn invoke_choose<K>(
    ctx: &mut ExecutionContext,
    caller: &ScopeId,
    step: &ChooseStep<K>,
    definition: &Definition<'_>,
) -> Result<(), ExecFailure>
where
    K: Data + PartialEq,
{
    // 1) 选择：只在 caller frame 内借用 selector，并用 PartialEq 与配置键比较。
    let selector = definition
        .record(step.selector().handle)
        .ok_or_else(|| {
            ExecFailure::runtime_opt("choose selector has no registered position", None)
        })?
        .core()
        .clone();
    let selected = {
        let value = ctx
            .resolve::<K>(caller, &selector)
            .map_err(|error| ExecFailure::runtime("choose selector borrow rejected", &error))?;
        step.cases().iter().position(|case| case.key == *value)
    };

    // 2) 分支决定：命中 case、改走 otherwise，或不可恢复的无匹配终止。
    let (branch, otherwise_used) = match selected {
        Some(index) => (&step.cases()[index].branch, false),
        None => match step.otherwise() {
            Some(branch) => (branch, true),
            None => {
                #[cfg(test)]
                crate::core::engine::observe::record_choose_selection(ChooseSelectionRecord {
                    caller: caller.clone(),
                    selected: None,
                    otherwise_used: false,
                    child: None,
                });
                return Err(ExecFailure::NoMatchingCase);
            }
        },
    };

    #[cfg(not(test))]
    let _ = otherwise_used;

    // 3) 建立直接 child（终止／可见范围检查在 create_child 内）。
    let child = ctx
        .create_child(caller)
        .map_err(|error| ExecFailure::runtime("choose branch child creation rejected", &error))?;
    #[cfg(test)]
    {
        crate::core::test_support::boundary_creation_record(
            child.clone(),
            caller.clone(),
            crate::core::orchestrator::observe::ScopeRole::Flow,
            ctx.identity_probe(),
            ctx.coordinator_probe(),
            ctx.container_probe(),
        );
        crate::core::engine::observe::record_chain_creation(ChainCreationRecord {
            child: child.clone(),
            caller: caller.clone(),
            closed_before: crate::core::test_support::closed_scope_snapshot(),
        });
        crate::core::engine::observe::record_choose_selection(ChooseSelectionRecord {
            caller: caller.clone(),
            selected: selected.map(|index| index as u16),
            otherwise_used,
            child: Some(child.clone()),
        });
        observe::record_export_snapshot(ctx, "child-created", &child);
        observe::record_export_snapshot(ctx, "caller-before", caller);
        observe::probe_foreign_identity(ctx, caller);
    }

    // 4) 整组 Import：所选分支的捕获需求（同一静态 RefId 在 caller 与 child 各绑定一次）。
    let imports = build_imports(
        definition,
        branch.body,
        "choose branch import references an unregistered position",
    )?;
    #[cfg(test)]
    {
        crate::core::engine::observe::record_chain_import(ChainImportRecord {
            child: child.clone(),
            caller: caller.clone(),
            pairs: imports.pairs.clone(),
        });
        observe::inject_import_occupy_fault(ctx, definition, branch.body, &child);
    }
    if let Err(error) = ctx.import_batch(&child, caller, &imports.slots) {
        #[cfg(test)]
        observe::record_export_snapshot(ctx, "import-rejected", &child);
        ctx.terminate(
            TerminationKind::BodyError,
            Some(child.clone()),
            "choose branch input assembly failed",
            Some(error.clone()),
        );
        if let Err(cleanup) = ctx.abort(&child) {
            ctx.record_cleanup_failure(child.clone(), cleanup);
        }
        #[cfg(test)]
        {
            observe::record_scope_state(ctx, "import-rejected", &child);
            observe::record_export_snapshot(ctx, "import-rejected-caller", caller);
        }
        return Err(ExecFailure::runtime(
            "choose branch input assembly failed",
            &error,
        ));
    }

    #[cfg(test)]
    observe::inject_export_occupy_fault(ctx, caller, &branch.slots);

    // 5) 进入 Boundary Guard（预检已保证该进入是不可失败的内部不变量）。
    let mut guard = ctx
        .enter(InvocationKind::Boundary, &child, true)
        .expect("boundary entry after its pre-checks cannot fail");
    #[cfg(test)]
    crate::core::engine::observe::record_frame(FrameRecord {
        kind: "boundary",
        phase: "enter",
        scope: child.clone(),
    });

    let outcome = run_body(&mut guard, definition, branch.body, &child).await;
    #[cfg(test)]
    observe::record_scope_state(&guard, "after-body", &child);

    let result = match outcome {
        Ok(()) => {
            #[cfg(test)]
            observe::record_export_snapshot(&guard, "before", caller);
            match finish_boundary(&mut guard, &child, &branch.slots, &CHOOSE_LABELS) {
                Ok(()) => {
                    #[cfg(test)]
                    {
                        observe::record_export_snapshot(&guard, "committed", caller);
                        observe::record_scope_state(&guard, "after-export", &child);
                    }
                    guard.release_responsibility(&child);
                    guard.complete();
                    Ok(())
                }
                Err(error) => {
                    #[cfg(test)]
                    observe::record_export_snapshot(&guard, "rejected", caller);
                    guard.failed_with(&CoreBodyError::new("choose output handover rejected"));
                    Err(error)
                }
            }
        }
        Err(ExecFailure::Control(signal)) => {
            #[cfg(test)]
            {
                observe::record_scope_state(&guard, "control-pending", &child);
                observe::inject_control_cleanup_fault(&mut guard, &child);
            }
            let result = exit_control_boundary(guard, &child, signal);
            #[cfg(test)]
            crate::core::engine::observe::record_control_completion(ControlCompletionRecord {
                child: child.clone(),
                terminated: ctx.is_terminated(),
                termination_scope: ctx
                    .termination()
                    .and_then(|termination| termination.scope().cloned()),
                child_state: ctx.state(&child).ok(),
            });
            result
        }
        Err(error) => {
            #[cfg(test)]
            observe::inject_boundary_cleanup_fault(&mut guard, &child);
            guard.failed_with(&CoreBodyError::new("choose branch body failed"));
            Err(error)
        }
    };

    #[cfg(test)]
    crate::core::engine::observe::record_frame(FrameRecord {
        kind: "boundary",
        phase: "exit",
        scope: child,
    });
    result
}

/// 执行一个 Iter Step：真实 controller Scope 保存受控状态，每轮建立直接 child Round。
///
/// 布局：caller 内预检 → 建立 controller → 整组导入原始依赖与初始状态（同静态 RefId 各绑定
/// 一次）→ 进入 controller Boundary → 逐槽登记 ControlState → 每轮建立 Round、从 controller
/// 装配 ordinary 捕获与 StateImport（formal current）→ 进入 Round Boundary → `run_body`。
/// Normal 整组 Promote 接受 next 并只关闭 Round 一次；IterBreak 关闭 Round 后两跳交付进入
/// 本轮时的 current；第 N 轮仍 Continue 时按预算返回终止错误。
pub(crate) async fn invoke_iter(
    ctx: &mut ExecutionContext,
    caller: &ScopeId,
    step: &IterStep,
    definition: &Definition<'_>,
) -> Result<(), ExecFailure> {
    let slots = step.slots();
    let current_positions: Vec<PositionId> = step.current().to_vec();
    let current = resolve_positions(
        definition,
        &current_positions,
        "iter references an unregistered formal current",
    )?;
    let current_cores: Vec<RefId> = current.iter().map(|(core, _)| core.clone()).collect();
    let initial_positions: Vec<PositionId> = step
        .initial()
        .iter()
        .map(|desc| desc.handle.position)
        .collect();
    let initial = resolve_positions(
        definition,
        &initial_positions,
        "iter references an unregistered initial state",
    )?;
    let initial_cores: Vec<RefId> = initial.iter().map(|(core, _)| core.clone()).collect();
    let round_body = step.child();

    #[cfg(test)]
    observe::inject_control_delivery_occupy_fault_from_slots(
        ctx,
        caller,
        slots.iter().map(|slot| &slot.output),
        observe::ControlDeliveryOccupyHop::ControllerToCaller,
    );

    let controller = ctx
        .create_child(caller)
        .map_err(|error| ExecFailure::runtime("iter controller creation rejected", &error))?;
    // 控制器整组装配：round Body 的普通词法捕获 + initial 状态来源。两者可能指向同一
    // 逻辑位置（普通捕获与 initial 重叠），或 initial 内部自带重复（如 iter((x, x), ..)）。
    // 按 (source, destination, 类型) 规范化去重——每个 Scope 本地位置只绑定一次；初始状态
    // 列表本身仍保留逐槽身份（`register_states` 对每槽独立登记）。目的位置相同但来源／类型
    // 不同不在此处合并，交由 import_batch 判为冲突。
    let mut descriptors: Vec<(RefId, RefId, TypeDesc)> = Vec::new();
    for position in definition.body(round_body).imports() {
        let handle = PositionHandle {
            definition: definition.id(),
            position: *position,
        };
        let record = definition.record(handle).ok_or_else(|| {
            ExecFailure::runtime_opt("iter import references an unregistered position", None)
        })?;
        if current_cores.iter().any(|core| core == record.core()) {
            continue;
        }
        descriptors.push((
            record.core().clone(),
            record.core().clone(),
            record.type_desc(),
        ));
    }
    for (core, type_desc) in &initial {
        descriptors.push((core.clone(), core.clone(), *type_desc));
    }
    let mut seen: Vec<(RefId, RefId, TypeId)> = Vec::new();
    let mut assembly: Vec<ImportSlot> = Vec::new();
    for (source, target, type_desc) in descriptors {
        let key = (source.clone(), target.clone(), type_desc.type_id());
        if seen.contains(&key) {
            continue;
        }
        seen.push(key);
        assembly.push(ImportSlot::with_type(
            &source,
            target,
            type_desc.type_id(),
            type_desc.type_name(),
        ));
    }
    if let Err(error) = ctx.import_batch(&controller, caller, &assembly) {
        ctx.terminate(
            TerminationKind::BodyError,
            Some(controller.clone()),
            "iter input assembly failed",
            Some(error.clone()),
        );
        if let Err(cleanup) = ctx.abort(&controller) {
            ctx.record_cleanup_failure(controller.clone(), cleanup);
        }
        return Err(ExecFailure::runtime("iter input assembly failed", &error));
    }

    #[cfg(test)]
    observe::record_controller("iter", "controller-created", caller, &controller, None, 0);
    let mut controller_guard = ctx
        .enter(InvocationKind::Boundary, &controller, true)
        .expect("iter controller entry after its pre-checks cannot fail");
    let states = match step.register_states(&mut controller_guard, &controller, &initial_cores) {
        Ok(states) => states,
        Err(error) => {
            return Err(fail_with_cleanup(
                controller_guard,
                &controller,
                "iter state registration rejected",
                &error,
                None,
            ));
        }
    };

    let ordinary = body_import_slots(
        definition,
        round_body,
        &current_cores,
        "iter import references an unregistered position",
    )?;
    let group: Vec<RefId> = slots.iter().map(|slot| slot.source.clone()).collect();
    let bridges: Vec<RefId> = step.bridges().to_vec();
    let bridge_types: Vec<TypeDesc> = slots.iter().map(|slot| slot.type_desc).collect();
    let destinations: Vec<RefId> = slots.iter().map(|slot| slot.output.clone()).collect();
    let mut completed = 0usize;

    loop {
        completed += 1;
        #[cfg(test)]
        observe::inject_control_delivery_occupy_fault(
            &mut controller_guard,
            &controller,
            &bridges,
            observe::ControlDeliveryOccupyHop::RoundToController,
        );
        let round = controller_guard
            .create_child(&controller)
            .map_err(|error| ExecFailure::runtime("iter round creation rejected", &error))?;
        #[cfg(test)]
        observe::record_controller(
            "iter",
            "round-created",
            caller,
            &controller,
            Some(&round),
            completed,
        );
        let state_slots: Vec<StateImportSlot> = step.state_import_slots(&states, &current_cores);
        if let Err(error) =
            controller_guard.import_batch_with_states(&round, &controller, &ordinary, &state_slots)
        {
            return Err(fail_with_cleanup(
                controller_guard,
                &controller,
                "iter round assembly failed",
                &error,
                None,
            ));
        }
        let permit = crate::core::context::RoundGroupPermit {
            controller: controller.clone(),
            round: round.clone(),
            group: group.clone(),
            states: states.clone(),
        };
        let mut round_guard = controller_guard
            .enter(InvocationKind::Boundary, &round, true)
            .expect("round entry after its pre-checks cannot fail");
        let outcome = run_body(&mut round_guard, definition, round_body, &round).await;

        match outcome {
            Ok(()) => {
                match round_guard.promote_group_in_round_boundary(&round, &group, &permit) {
                    PromoteGroupOutcome::Promoted => {
                        #[cfg(test)]
                        observe::record_controller(
                            "iter",
                            "round-promoted",
                            caller,
                            &controller,
                            Some(&round),
                            completed,
                        );
                        round_guard.release_responsibility(&round);
                        round_guard.complete();
                    }
                    PromoteGroupOutcome::Rejected {
                        primary,
                        cleanup_failure,
                    } => {
                        let failure = fail_with_cleanup(
                            round_guard,
                            &round,
                            "iter promote rejected",
                            &primary,
                            cleanup_failure,
                        );
                        controller_guard.failed_with(&CoreBodyError::new("iter promote rejected"));
                        return Err(failure);
                    }
                }
                if let Err(error) = controller_guard.recycle_pending(&controller) {
                    controller_guard
                        .failed_with(&CoreBodyError::new("iter pending recycle rejected"));
                    return Err(ExecFailure::runtime(
                        "iter pending recycle rejected",
                        &error,
                    ));
                }
                if completed >= step.max_iterations() {
                    #[cfg(test)]
                    observe::record_controller(
                        "iter",
                        "budget-exhausted",
                        caller,
                        &controller,
                        Some(&round),
                        completed,
                    );
                    controller_guard.failed_with(&CoreBodyError::new("iter round limit reached"));
                    return Err(ExecFailure::Budget(
                        BudgetTermination::IterationLimitReached {
                            max_iterations: step.max_iterations(),
                            completed_iterations: completed,
                        },
                    ));
                }
            }
            Err(ExecFailure::Control(signal)) => {
                let matched = matches!(
                    signal,
                    crate::core::engine::error::ControlSignal::IterBreak(_)
                );
                if !matched {
                    #[cfg(test)]
                    observe::record_controller(
                        "iter",
                        "round-propagated",
                        caller,
                        &controller,
                        Some(&round),
                        completed,
                    );
                    // Retry 等不匹配信号：退出 Round 与 controller 后原样传播。
                    if let Err(error) =
                        close_control_scope(round_guard, "iter round exit cleanup failed")
                    {
                        controller_guard.failed_with(&CoreBodyError::new("iter round exit failed"));
                        return Err(error);
                    }
                    close_control_scope(controller_guard, "iter boundary exit cleanup failed")?;
                    return Err(ExecFailure::Control(signal));
                }
                #[cfg(test)]
                observe::record_controller(
                    "iter",
                    "round-caught",
                    caller,
                    &controller,
                    Some(&round),
                    completed,
                );
                // 本轮已产生但未接受的 next 随 Round 关闭清理；随后两跳交付 current。
                #[cfg(test)]
                if observe::take_iter_delivery_cleanup_fault() {
                    observe::break_owned_entry(&mut round_guard, &round);
                }
                let delivery: Vec<Delivery<'_>> = slots
                    .iter()
                    .zip(&bridges)
                    .zip(&current_cores)
                    .map(|((slot, bridge), source)| Delivery {
                        exit: &slot.exit,
                        source,
                        destination: bridge,
                        type_desc: slot.type_desc,
                    })
                    .collect();
                #[cfg(test)]
                observe::record_controller_scope_pair(
                    &round_guard,
                    "delivery-before",
                    &round,
                    &controller,
                );
                match deliver_round(&mut round_guard, &round, &delivery, &ITER_LABELS) {
                    Ok(()) => {
                        #[cfg(test)]
                        observe::record_delivery(
                            "iter",
                            "round-to-controller",
                            &round,
                            &controller,
                            slots
                                .iter()
                                .zip(&bridges)
                                .map(|(slot, bridge)| (slot.exit.clone(), bridge.clone()))
                                .collect(),
                        );
                        round_guard.release_responsibility(&round);
                        round_guard.complete();
                    }
                    Err(error) => {
                        #[cfg(test)]
                        observe::record_controller_scope_pair(
                            &round_guard,
                            "delivery-failed",
                            &round,
                            &controller,
                        );
                        round_guard
                            .failed_with(&CoreBodyError::new("iter break delivery rejected"));
                        controller_guard
                            .failed_with(&CoreBodyError::new("iter break delivery failed"));
                        return Err(error);
                    }
                }
                match export_group(
                    &mut controller_guard,
                    &controller,
                    &bridges,
                    &bridge_types,
                    destinations.iter(),
                    &ITER_LABELS,
                ) {
                    Ok(()) => {
                        #[cfg(test)]
                        observe::record_delivery(
                            "iter",
                            "controller-to-caller",
                            &controller,
                            caller,
                            bridges
                                .iter()
                                .cloned()
                                .zip(destinations.iter().cloned())
                                .collect(),
                        );
                        controller_guard.release_responsibility(&controller);
                        controller_guard.complete();
                        return Ok(());
                    }
                    Err(error) => {
                        controller_guard.failed_with(&CoreBodyError::new("iter export rejected"));
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                #[cfg(test)]
                observe::record_controller(
                    "iter",
                    "round-failed",
                    caller,
                    &controller,
                    Some(&round),
                    completed,
                );
                round_guard.failed_with(&CoreBodyError::new("iter round body failed"));
                controller_guard.failed_with(&CoreBodyError::new("iter round failed"));
                return Err(error);
            }
        }
    }
}

/// 执行一个 Each Step：真实 EachScope（caller 的直接 child）与其逐项直接 child ItemScope。
///
/// 布局（WE-T05 §4）：caller 建立 Each → 整组 Import（集合输入 + item Body 普通祖先捕获，
/// 按 (source, destination, 类型) 规范化去重）→ 进入 Each Boundary → 逐槽登记 typed
/// collector（unit 为零个）→ 借用集合取 len 后立即结束借用 → 逐 index：建 Item child →
/// 当前 Each frame 内 `bind_item_input`（原集合元素受限借用，无独立 DataId、不 Clone／不
/// 移走）→ 装 ordinary Imports（不重复装配 formal item）→ 进入 Item Boundary → `run_body`
/// 同一静态 item Body → Normal 整组 Consume（unit 只正常关闭）→ 已 Closed 后解除责任并
/// complete。全部项成功后逐槽 finish 成 Each-owned 普通 Vec、一次绑定 Each-local 最终输出，
/// 再整组 Export 到 caller 声明位置，关闭 Each。Control 不捕获 Retry／IterBreak：当前 Item
/// 不 Consume，先退 Item 再退 Each（销毁未完成 collectors），停止后续 index，向上返回原信号。
pub(crate) async fn invoke_each(
    ctx: &mut ExecutionContext,
    caller: &ScopeId,
    step: &EachStep,
    definition: &Definition<'_>,
) -> Result<(), ExecFailure> {
    // 静态解析：集合输入位置与形式 item 位置（构建期已登记；缺失即不可达的框架故障）。
    let collection_desc = step.collection();
    let collection_core = definition
        .record(collection_desc.handle)
        .map(|record| record.core().clone())
        .ok_or_else(|| {
            ExecFailure::runtime_opt("each collection references an unregistered position", None)
        })?;
    let collection_type = collection_desc.type_desc;
    let item_body = step.item();
    let item_core = definition
        .record(PositionHandle {
            definition: definition.id(),
            position: step.item_slot(),
        })
        .map(|record| record.core().clone())
        .ok_or_else(|| {
            ExecFailure::runtime_opt("each formal item references an unregistered position", None)
        })?;

    // item Body 的普通词法捕获；形式 item 不在此重复装配（由 `bind_item_input` 单独绑定）。
    let ordinary = body_import_slots(
        definition,
        item_body,
        std::slice::from_ref(&item_core),
        "each import references an unregistered position",
    )?;

    // 1) 建立直接 child EachScope。
    let each = ctx
        .create_child(caller)
        .map_err(|error| ExecFailure::runtime("each scope creation rejected", &error))?;
    #[cfg(test)]
    {
        crate::core::test_support::boundary_creation_record(
            each.clone(),
            caller.clone(),
            crate::core::orchestrator::observe::ScopeRole::Each,
            ctx.identity_probe(),
            ctx.coordinator_probe(),
            ctx.container_probe(),
        );
        observe::record_controller("each", "scope-created", caller, &each, None, 0);
    }

    // 2) 整组 Import：集合输入与 item Body 祖先捕获在同一次规范化装配中各绑定一次；同一
    //    逻辑位置（source／destination／类型）只装一次，不按 DataId 合并不同逻辑位置，也不
    //    重复装配 formal item。
    let mut descriptors: Vec<(RefId, RefId, TypeDesc)> = vec![(
        collection_core.clone(),
        collection_core.clone(),
        collection_type,
    )];
    for position in definition.body(item_body).imports() {
        let handle = PositionHandle {
            definition: definition.id(),
            position: *position,
        };
        let record = definition.record(handle).ok_or_else(|| {
            ExecFailure::runtime_opt("each import references an unregistered position", None)
        })?;
        if record.core() == &item_core {
            continue;
        }
        descriptors.push((
            record.core().clone(),
            record.core().clone(),
            record.type_desc(),
        ));
    }
    let mut seen: Vec<(RefId, RefId, TypeId)> = Vec::new();
    let mut assembly: Vec<ImportSlot> = Vec::new();
    for (source, target, type_desc) in descriptors {
        let key = (source.clone(), target.clone(), type_desc.type_id());
        if seen.contains(&key) {
            continue;
        }
        seen.push(key);
        assembly.push(ImportSlot::with_type(
            &source,
            target,
            type_desc.type_id(),
            type_desc.type_name(),
        ));
    }
    if let Err(error) = ctx.import_batch(&each, caller, &assembly) {
        ctx.terminate(
            TerminationKind::BodyError,
            Some(each.clone()),
            "each input assembly failed",
            Some(error.clone()),
        );
        if let Err(cleanup) = ctx.abort(&each) {
            ctx.record_cleanup_failure(each.clone(), cleanup);
        }
        return Err(ExecFailure::runtime("each input assembly failed", &error));
    }

    // 3) 进入 Each Boundary Guard（预检已保证该进入是不可失败的内部不变量）。
    let mut each_guard = ctx
        .enter(InvocationKind::Boundary, &each, true)
        .expect("each boundary entry after its pre-checks cannot fail");

    // 4) 按输出槽登记类型化 collectors（unit 为零个）。
    let collectors = match step.begin_collectors(&mut each_guard, &each) {
        Ok(collectors) => collectors,
        Err(error) => {
            return Err(fail_with_cleanup(
                each_guard,
                &each,
                "each collector registration rejected",
                &error,
                None,
            ));
        }
    };

    let slots = step.slots();
    let group: Vec<RefId> = slots.iter().map(|slot| slot.source.clone()).collect();
    // 每次收口复用同一许可，仅逐项替换 `item`：声明组与 collector 组只装配一次，避免
    // 在逐项循环里重复克隆这两个组。
    let mut permit = ItemGroupPermit {
        item: each.clone(),
        each: each.clone(),
        group: group.clone(),
        collectors: collectors.clone(),
    };

    // 5) 借用集合取得 len，借用立即结束（Engine 只保存 index／len 纯元数据）。
    let len = match step.collection_len(&each_guard, &each, &collection_core) {
        Ok(len) => len,
        Err(error) => {
            return Err(fail_with_cleanup(
                each_guard,
                &each,
                "each collection length read rejected",
                &error,
                None,
            ));
        }
    };

    // 6) 逐 index：建 Item、绑定原元素、装 ordinary Imports、执行同一静态 Body、收口。
    for index in 0..len {
        let item = match each_guard.create_child(&each) {
            Ok(item) => item,
            Err(error) => {
                return Err(fail_with_cleanup(
                    each_guard,
                    &each,
                    "each item creation rejected",
                    &error,
                    None,
                ));
            }
        };
        #[cfg(test)]
        {
            crate::core::test_support::boundary_creation_record(
                item.clone(),
                each.clone(),
                crate::core::orchestrator::observe::ScopeRole::Item,
                each_guard.identity_probe(),
                each_guard.coordinator_probe(),
                each_guard.container_probe(),
            );
            observe::record_controller("each", "item-created", caller, &each, Some(&item), index);
        }

        // 形式 item：原集合元素的受限借用（地址对应原 Vec 元素；无独立 DataId、不移走）。
        if let Err(error) = step.bind_item(
            &mut each_guard,
            &item,
            &each,
            &collection_core,
            &item_core,
            index,
        ) {
            return Err(fail_with_cleanup(
                each_guard,
                &each,
                "each item binding rejected",
                &error,
                None,
            ));
        }
        if let Err(error) = each_guard.import_batch(&item, &each, &ordinary) {
            return Err(fail_with_cleanup(
                each_guard,
                &each,
                "each item import rejected",
                &error,
                None,
            ));
        }

        let mut item_guard = each_guard
            .enter(InvocationKind::Boundary, &item, true)
            .expect("item boundary entry after its pre-checks cannot fail");
        let outcome = run_body(&mut item_guard, definition, item_body, &item).await;

        match outcome {
            Ok(()) => {
                if group.is_empty() {
                    // unit：不建 collector、不制造 unit Data／Ref，也不调用空 Consume 伪造
                    // 成功；真实 Body 已执行，这里只正常关闭 Item。
                    if let Err(error) = item_guard.finalize(&item, &[], &mut Vec::new()) {
                        item_guard
                            .failed_with(&CoreBodyError::new("each unit item close rejected"));
                        each_guard.failed_with(&CoreBodyError::new("each unit item failed"));
                        return Err(ExecFailure::runtime(
                            "each unit item close rejected",
                            &error,
                        ));
                    }
                } else {
                    permit.item = item.clone();
                    match item_guard.consume_group_in_item_boundary(&item, &group, &permit) {
                        ConsumeGroupOutcome::Consumed => {}
                        ConsumeGroupOutcome::Rejected {
                            primary,
                            cleanup_failure,
                        } => {
                            let failure = fail_with_cleanup(
                                item_guard,
                                &item,
                                "each item consume rejected",
                                &primary,
                                cleanup_failure,
                            );
                            each_guard.failed_with(&CoreBodyError::new("each item consume failed"));
                            return Err(failure);
                        }
                    }
                }
                #[cfg(test)]
                observe::record_controller(
                    "each",
                    "item-consumed",
                    caller,
                    &each,
                    Some(&item),
                    index,
                );
                // 已 Closed 后解除责任并完成本 Item frame；消费后才创建下一 Item。
                item_guard.release_responsibility(&item);
                item_guard.complete();
            }
            Err(ExecFailure::Control(signal)) => {
                // §6.1 两阶段退出：当前 Item 不 Consume，先退 Item（已成功项仍在 collectors），
                // 再退 Each（销毁未完成 collectors），停止后续 index，向上返回原信号。
                #[cfg(test)]
                {
                    observe::record_controller(
                        "each",
                        "item-control-exit",
                        caller,
                        &each,
                        Some(&item),
                        index,
                    );
                    observe::inject_control_cleanup_fault(&mut item_guard, &item);
                }
                if let Err(error) = close_control_scope(item_guard, "each item exit cleanup failed")
                {
                    each_guard.failed_with(&CoreBodyError::new("each item exit failed"));
                    return Err(error);
                }
                #[cfg(test)]
                observe::observe_each_control_exit(&mut each_guard, caller, &each, &item, index);

                close_control_scope(each_guard, "each boundary exit cleanup failed")?;
                #[cfg(test)]
                crate::core::engine::observe::record_control_completion(ControlCompletionRecord {
                    child: each.clone(),
                    terminated: ctx.is_terminated(),
                    termination_scope: ctx.termination().and_then(|t| t.scope().cloned()),
                    child_state: ctx.state(&each).ok(),
                });
                return Err(ExecFailure::Control(signal));
            }
            Err(error) => {
                #[cfg(test)]
                observe::record_controller(
                    "each",
                    "item-failed",
                    caller,
                    &each,
                    Some(&item),
                    index,
                );
                item_guard.failed_with(&CoreBodyError::new("each item body failed"));
                each_guard.failed_with(&CoreBodyError::new("each item failed"));
                return Err(error);
            }
        }
    }

    // 7) 全部项成功：逐槽 finish 为 Each-owned 普通 Vec 并一次绑定 Each 本地最终输出；若后续
    //    finish／Export 拒绝，已形成的 Each-local Vec 仍属 Each 并按失败路径清理，不向 caller
    //    返回部分正常输出。
    let locals: Vec<RefId> = slots.iter().map(|slot| slot.local.clone()).collect();
    #[cfg(test)]
    observe::inject_each_finish_occupy_fault(&mut each_guard, &each, slots);
    if let Err(error) = step.finish_collectors(&mut each_guard, &each, &collectors, &locals) {
        #[cfg(test)]
        observe::record_each_finish_reject_scene(
            &each_guard,
            &each,
            caller,
            slots,
            &collectors,
            &error,
        );
        each_guard.failed_with(&CoreBodyError::new("each collector finish rejected"));
        return Err(ExecFailure::runtime(
            "each collector finish rejected",
            &error,
        ));
    }
    let collected_types: Vec<TypeDesc> = slots.iter().map(|slot| slot.collected_type).collect();
    #[cfg(test)]
    observe::record_export_snapshot(&each_guard, "before", caller);

    // 8) 整组 Export：Each 声明输出（Each-local Vec）→ caller 声明位置，随后关闭 Each。
    match export_group(
        &mut each_guard,
        &each,
        &locals,
        &collected_types,
        slots.iter().map(|slot| &slot.output),
        &EACH_LABELS,
    ) {
        Ok(()) => {
            #[cfg(test)]
            {
                observe::record_export_snapshot(&each_guard, "committed", caller);
                observe::record_controller("each", "exported", caller, &each, None, 0);
            }
            each_guard.release_responsibility(&each);
            each_guard.complete();
            Ok(())
        }
        Err(error) => {
            #[cfg(test)]
            observe::record_export_snapshot(&each_guard, "rejected", caller);
            each_guard.failed_with(&CoreBodyError::new("each output handover rejected"));
            Err(error)
        }
    }
}

/// 边界收口各步的诊断措辞（Chain 与 Choose 各自保持与自身边界一致的说法）。
struct BoundaryLabels {
    close: &'static str,
    alias: &'static str,
    export: &'static str,
}

const CHAIN_LABELS: BoundaryLabels = BoundaryLabels {
    close: "chain close rejected",
    alias: "chain exit alias rejected",
    export: "chain export rejected",
};

const CHOOSE_LABELS: BoundaryLabels = BoundaryLabels {
    close: "choose branch close rejected",
    alias: "choose branch exit alias rejected",
    export: "choose branch export rejected",
};

const RETRY_LABELS: BoundaryLabels = BoundaryLabels {
    close: "retry controller close rejected",
    alias: "retry round exit alias rejected",
    export: "retry output handover rejected",
};

const ITER_LABELS: BoundaryLabels = BoundaryLabels {
    close: "iter controller close rejected",
    alias: "iter round exit alias rejected",
    export: "iter output handover rejected",
};

const EACH_LABELS: BoundaryLabels = BoundaryLabels {
    close: "each local close rejected",
    alias: "each local alias rejected",
    export: "each output handover rejected",
};

/// 一个 Round 的整组交付映射：Round 本地出口 ← 来源，再整组导出到目的位置。
struct Delivery<'a> {
    exit: &'a RefId,
    source: &'a RefId,
    destination: &'a RefId,
    type_desc: TypeDesc,
}

/// Round 本地出口 alias 装配 + 整组 Export（直接父子必须成立；空组只关闭来源）。
fn deliver_round(
    guard: &mut InvocationGuard<'_>,
    round: &ScopeId,
    delivery: &[Delivery<'_>],
    labels: &BoundaryLabels,
) -> Result<(), ExecFailure> {
    if delivery.is_empty() {
        return guard
            .finalize(round, &[], &mut Vec::new())
            .map_err(|error| ExecFailure::runtime(labels.close, &error));
    }
    let aliases: Vec<AliasSlot> = delivery
        .iter()
        .map(|slot| {
            AliasSlot::with_type(
                slot.source,
                slot.exit.clone(),
                slot.type_desc.type_id(),
                slot.type_desc.type_name(),
            )
        })
        .collect();
    guard
        .bind_local_aliases(round, &aliases)
        .map_err(|error| ExecFailure::runtime(labels.alias, &error))?;
    let declared: Vec<RefId> = delivery.iter().map(|slot| slot.exit.clone()).collect();
    let types: Vec<TypeDesc> = delivery.iter().map(|slot| slot.type_desc).collect();
    export_group(
        guard,
        round,
        &declared,
        &types,
        delivery.iter().map(|slot| slot.destination),
        labels,
    )
}

/// 整组 Export（来源已在 child 绑定；空组只关闭来源）。
fn export_group<'s>(
    guard: &mut InvocationGuard<'_>,
    child: &ScopeId,
    declared: &[RefId],
    types: &[TypeDesc],
    destinations: impl Iterator<Item = &'s RefId>,
    labels: &BoundaryLabels,
) -> Result<(), ExecFailure> {
    let mut outputs: Vec<ExportSlot> = Vec::new();
    for ((source, type_desc), destination) in declared.iter().zip(types).zip(destinations) {
        outputs.push(ExportSlot::with_type(
            source,
            destination.clone(),
            type_desc.type_id(),
            type_desc.type_name(),
        ));
    }
    guard
        .finalize(child, declared, &mut outputs)
        .map_err(|error| ExecFailure::runtime(labels.export, &error))
}

/// 受控控制退出：abort → Closed 确认 → 解除责任 → complete，Context 保持有效。
fn close_control_scope(
    mut guard: InvocationGuard<'_>,
    note: &'static str,
) -> Result<(), ExecFailure> {
    let responsible = guard.responsible_scopes().to_vec();
    for scope in responsible {
        let cleanup = guard
            .abort(&scope)
            .and_then(|()| match guard.state(&scope)? {
                ScopeState::Closed => Ok(()),
                _ => Err(ScopeError::Invariant {
                    violated: "control exit needs a Closed responsibility scope",
                }),
            });
        if let Err(error) = cleanup {
            #[cfg(test)]
            observe::record_scope_state(&guard, "control-cleanup-failed", &scope);
            guard.failed_with(&CoreBodyError::from(error.clone()));
            return Err(ExecFailure::runtime(note, &error));
        }
        guard.release_responsibility(&scope);
    }
    guard.complete();
    Ok(())
}

/// Body 的整组 ImportSlot（同一静态 RefId 作为 source 与 target），可排除给定来源。
fn body_import_slots(
    definition: &Definition<'_>,
    body: BodyId,
    exclude: &[RefId],
    note: &'static str,
) -> Result<Vec<ImportSlot>, ExecFailure> {
    let mut slots = Vec::new();
    for position in definition.body(body).imports() {
        let handle = PositionHandle {
            definition: definition.id(),
            position: *position,
        };
        let record = definition
            .record(handle)
            .ok_or_else(|| ExecFailure::runtime_opt(note, None))?;
        if exclude.iter().any(|core| core == record.core()) {
            continue;
        }
        slots.push(ImportSlot::with_type(
            record.core(),
            record.core().clone(),
            record.type_desc().type_id(),
            record.type_desc().type_name(),
        ));
    }
    Ok(slots)
}

/// 静态解析一组逻辑位置：Core RefId 与声明类型（构建期已保证登记存在）。
fn resolve_positions(
    definition: &Definition<'_>,
    positions: &[PositionId],
    note: &'static str,
) -> Result<Vec<(RefId, TypeDesc)>, ExecFailure> {
    positions
        .iter()
        .map(|position| {
            let handle = PositionHandle {
                definition: definition.id(),
                position: *position,
            };
            definition
                .record(handle)
                .map(|record| (record.core().clone(), record.type_desc()))
                .ok_or_else(|| ExecFailure::runtime_opt(note, None))
        })
        .collect()
}

/// 预检失败或装配失败后的统一收口：登记首清理诊断 → 标记失败退出 → 返回框架故障。
fn fail_with_cleanup(
    mut guard: InvocationGuard<'_>,
    scope: &ScopeId,
    note: &'static str,
    primary: &ScopeError,
    cleanup: Option<ScopeError>,
) -> ExecFailure {
    if let Some(cleanup) = cleanup {
        guard.record_cleanup_failure(scope.clone(), cleanup);
    }
    guard.failed_with(&CoreBodyError::new(note));
    ExecFailure::runtime(note, primary)
}

/// 执行一个 Retry Step：真实 controller Scope + 每次 attempt 的真实 RoundScope。
///
/// 布局：caller 内预检 → 建立 controller（caller 的直接 child）→ 整组导入原始依赖 →
/// 进入 controller Boundary → 每 attempt 建立直接 child Round、从 controller 装配本地捕获 →
/// 进入 Round Boundary → `run_body`。attempt 正常退出时两跳整组交付
/// （Round 出口 → controller 桥 → caller 声明输出），随后关闭 Round 与 controller。
pub(crate) async fn invoke_retry(
    ctx: &mut ExecutionContext,
    caller: &ScopeId,
    step: &RetryStep,
    definition: &Definition<'_>,
) -> Result<(), ExecFailure> {
    #[cfg(test)]
    observe::inject_control_delivery_occupy_fault_from_slots(
        ctx,
        caller,
        step.slots().iter().map(|slot| &slot.output),
        observe::ControlDeliveryOccupyHop::ControllerToCaller,
    );

    let controller = ctx
        .create_child(caller)
        .map_err(|error| ExecFailure::runtime("retry controller creation rejected", &error))?;
    // 原始依赖：attempt Body 的静态捕获在 caller → controller 各绑定一次。
    let imports = body_import_slots(
        definition,
        step.child(),
        &[],
        "retry import references an unregistered position",
    )?;
    if let Err(error) = ctx.import_batch(&controller, caller, &imports) {
        ctx.terminate(
            TerminationKind::BodyError,
            Some(controller.clone()),
            "retry input assembly failed",
            Some(error.clone()),
        );
        if let Err(cleanup) = ctx.abort(&controller) {
            ctx.record_cleanup_failure(controller.clone(), cleanup);
        }
        return Err(ExecFailure::runtime("retry input assembly failed", &error));
    }

    #[cfg(test)]
    observe::record_controller("retry", "controller-created", caller, &controller, None, 0);
    let mut controller_guard = ctx
        .enter(InvocationKind::Boundary, &controller, true)
        .expect("retry controller entry after its pre-checks cannot fail");

    let slots = step.slots();
    let bridges: Vec<RefId> = step.bridges().to_vec();
    let destinations: Vec<RefId> = slots.iter().map(|slot| slot.output.clone()).collect();
    let mut attempts = 0usize;

    loop {
        attempts += 1;
        #[cfg(test)]
        observe::inject_control_delivery_occupy_fault(
            &mut controller_guard,
            &controller,
            &bridges,
            observe::ControlDeliveryOccupyHop::RoundToController,
        );
        let round = controller_guard
            .create_child(&controller)
            .map_err(|error| ExecFailure::runtime("retry round creation rejected", &error))?;
        #[cfg(test)]
        observe::record_controller(
            "retry",
            "round-created",
            caller,
            &controller,
            Some(&round),
            attempts,
        );
        if let Err(error) = controller_guard.import_batch(&round, &controller, &imports) {
            let failure = fail_with_cleanup(
                controller_guard,
                &controller,
                "retry attempt input assembly failed",
                &error,
                None,
            );
            return Err(failure);
        }

        let mut round_guard = controller_guard
            .enter(InvocationKind::Boundary, &round, true)
            .expect("round entry after its pre-checks cannot fail");
        let outcome = run_body(&mut round_guard, definition, step.child(), &round).await;

        match outcome {
            Ok(()) => {
                // 成功：两跳整组交付。第一跳关闭 Round 并把输出交给 controller 本地桥。
                let delivery: Vec<Delivery<'_>> = slots
                    .iter()
                    .zip(&bridges)
                    .map(|(slot, bridge)| Delivery {
                        exit: &slot.exit,
                        source: &slot.source,
                        destination: bridge,
                        type_desc: slot.type_desc,
                    })
                    .collect();
                match deliver_round(&mut round_guard, &round, &delivery, &RETRY_LABELS) {
                    Ok(()) => {
                        #[cfg(test)]
                        {
                            observe::record_controller(
                                "retry",
                                "round-normal",
                                caller,
                                &controller,
                                Some(&round),
                                attempts,
                            );
                            observe::record_delivery(
                                "retry",
                                "round-to-controller",
                                &round,
                                &controller,
                                slots
                                    .iter()
                                    .zip(&bridges)
                                    .map(|(slot, bridge)| (slot.exit.clone(), bridge.clone()))
                                    .collect(),
                            );
                        }
                        round_guard.release_responsibility(&round);
                        round_guard.complete();
                    }
                    Err(error) => {
                        round_guard.failed_with(&CoreBodyError::new("retry delivery rejected"));
                        controller_guard.failed_with(&CoreBodyError::new("retry delivery failed"));
                        return Err(error);
                    }
                }
                let bridge_types: Vec<TypeDesc> = slots.iter().map(|slot| slot.type_desc).collect();
                match export_group(
                    &mut controller_guard,
                    &controller,
                    &bridges,
                    &bridge_types,
                    destinations.iter(),
                    &RETRY_LABELS,
                ) {
                    Ok(()) => {
                        #[cfg(test)]
                        observe::record_delivery(
                            "retry",
                            "controller-to-caller",
                            &controller,
                            caller,
                            bridges
                                .iter()
                                .cloned()
                                .zip(destinations.iter().cloned())
                                .collect(),
                        );
                        controller_guard.release_responsibility(&controller);
                        controller_guard.complete();
                        return Ok(());
                    }
                    Err(error) => {
                        controller_guard.failed_with(&CoreBodyError::new("retry export rejected"));
                        return Err(error);
                    }
                }
            }
            Err(ExecFailure::Control(signal)) => match signal {
                crate::core::engine::error::ControlSignal::Retry(last_error) => {
                    #[cfg(test)]
                    observe::record_controller(
                        "retry",
                        "round-caught",
                        caller,
                        &controller,
                        Some(&round),
                        attempts,
                    );
                    // 受控清理当前 attempt；清理失败按 SPEC §12 转 Runtime 且不继续轮次。
                    #[cfg(test)]
                    if observe::take_retry_round_cleanup_fault() {
                        observe::break_owned_entry(&mut round_guard, &round);
                    }
                    #[cfg(test)]
                    observe::record_controller_scope_pair(
                        &round_guard,
                        "retry-cleanup-before",
                        &round,
                        &controller,
                    );
                    if let Err(error) =
                        close_control_scope(round_guard, "retry attempt cleanup failed")
                    {
                        controller_guard
                            .failed_with(&CoreBodyError::new("retry attempt cleanup failed"));
                        return Err(error);
                    }
                    if attempts <= step.max_retries() {
                        continue;
                    }
                    #[cfg(test)]
                    observe::record_controller(
                        "retry",
                        "budget-exhausted",
                        caller,
                        &controller,
                        Some(&round),
                        attempts,
                    );
                    controller_guard.failed_with(&CoreBodyError::new("retry budget exhausted"));
                    return Err(ExecFailure::Budget(BudgetTermination::RetryExhausted {
                        max_retries: step.max_retries(),
                        attempts,
                        last_error,
                    }));
                }
                other => {
                    #[cfg(test)]
                    observe::record_controller(
                        "retry",
                        "round-propagated",
                        caller,
                        &controller,
                        Some(&round),
                        attempts,
                    );
                    // 不捕获的信号（IterBreak 等）：退出 controller 并原样传播；Context 保持有效。
                    if let Err(error) =
                        close_control_scope(round_guard, "retry attempt cleanup failed")
                    {
                        controller_guard
                            .failed_with(&CoreBodyError::new("retry attempt cleanup failed"));
                        return Err(error);
                    }
                    close_control_scope(controller_guard, "retry boundary exit cleanup failed")?;
                    return Err(ExecFailure::Control(other));
                }
            },
            Err(error) => {
                #[cfg(test)]
                observe::record_controller(
                    "retry",
                    "round-failed",
                    caller,
                    &controller,
                    Some(&round),
                    attempts,
                );
                round_guard.failed_with(&CoreBodyError::new("retry attempt body failed"));
                controller_guard.failed_with(&CoreBodyError::new("retry attempt failed"));
                return Err(error);
            }
        }
    }
}

/// child 本地出口 alias 装配与整组 Export。
fn finish_boundary(
    guard: &mut InvocationGuard<'_>,
    child: &ScopeId,
    slots: &[HandoverSlot],
    labels: &BoundaryLabels,
) -> Result<(), ExecFailure> {
    if slots.is_empty() {
        guard
            .finalize(child, &[], &mut Vec::new())
            .map_err(|error| ExecFailure::runtime(labels.close, &error))?;
        #[cfg(test)]
        observe::probe_closed_child(guard, child, slots);
        return Ok(());
    }
    // 每个输出槽的 child 本地唯一声明出口 alias 到该槽来源的实际 target；
    // 不移动业务值、不改变 owner。
    let aliases: Vec<AliasSlot> = slots
        .iter()
        .map(|slot| {
            AliasSlot::with_type(
                &slot.source,
                slot.exit.clone(),
                slot.type_desc.type_id(),
                slot.type_desc.type_name(),
            )
        })
        .collect();
    guard
        .bind_local_aliases(child, &aliases)
        .map_err(|error| ExecFailure::runtime(labels.alias, &error))?;
    let declared: Vec<RefId> = slots.iter().map(|slot| slot.exit.clone()).collect();
    let mut outputs: Vec<ExportSlot> = slots
        .iter()
        .map(|slot| {
            ExportSlot::with_type(
                &slot.exit,
                slot.output.clone(),
                slot.type_desc.type_id(),
                slot.type_desc.type_name(),
            )
        })
        .collect();
    guard
        .finalize(child, &declared, &mut outputs)
        .map_err(|error| ExecFailure::runtime(labels.export, &error))?;
    #[cfg(test)]
    observe::probe_closed_child(guard, child, slots);
    Ok(())
}

/// Control 退出：abort → Closed 确认 → 解除责任 → complete → 原样返回信号。
///
/// 清理失败不再返回可恢复 Control，转为 Runtime Failure 并保留未完成责任供 Guard 收拢。
fn exit_control_boundary(
    mut guard: InvocationGuard<'_>,
    child: &ScopeId,
    signal: crate::core::engine::error::ControlSignal,
) -> Result<(), ExecFailure> {
    #[cfg(not(test))]
    let _ = child;
    let responsible = guard.responsible_scopes().to_vec();
    #[cfg(test)]
    let mut released = 0usize;
    for scope in responsible {
        let cleanup = guard
            .abort(&scope)
            .and_then(|()| match guard.state(&scope)? {
                ScopeState::Closed => Ok(()),
                _ => Err(ScopeError::Invariant {
                    violated: "control exit needs a Closed responsibility scope",
                }),
            });
        if let Err(error) = cleanup {
            #[cfg(test)]
            observe::record_scope_state(&guard, "control-cleanup-failed", &scope);
            guard.failed_with(&CoreBodyError::from(error.clone()));
            return Err(ExecFailure::runtime("control exit cleanup failed", &error));
        }
        #[cfg(test)]
        observe::record_scope_state(&guard, "control-closed", &scope);
        guard.release_responsibility(&scope);
        #[cfg(test)]
        {
            released += 1;
        }
    }
    #[cfg(test)]
    crate::core::engine::observe::record_control_release(ControlReleaseRecord {
        child: child.clone(),
        responsible_empty: guard.responsible_scopes().is_empty(),
        released,
    });
    guard.complete();
    Err(ExecFailure::Control(signal))
}

/// child 的 Import 需求 → 整组 ImportSlot（同一静态 RefId 作为 source 与 target）。
fn build_imports(
    definition: &Definition<'_>,
    child_body: BodyId,
    note: &'static str,
) -> Result<BoundaryImports, ExecFailure> {
    let child = definition.body(child_body);
    let mut slots = Vec::with_capacity(child.imports().len());
    #[cfg(test)]
    let mut pairs = Vec::with_capacity(child.imports().len());
    for position in child.imports() {
        let handle = crate::core::engine::identity::PositionHandle {
            definition: definition.id(),
            position: *position,
        };
        let record = definition
            .record(handle)
            .ok_or_else(|| ExecFailure::runtime_opt(note, None))?;
        slots.push(ImportSlot::with_type(
            record.core(),
            record.core().clone(),
            record.type_desc().type_id(),
            record.type_desc().type_name(),
        ));
        #[cfg(test)]
        pairs.push((record.core().clone(), record.core().clone()));
    }
    Ok(BoundaryImports {
        slots,
        #[cfg(test)]
        pairs,
    })
}

/// cfg(test)：预占 child 的第 N 个 Import 目的位置，使整组 Import 预检在任何绑定前拒绝。
#[cfg(test)]
#[path = "../../../tests/internal/engine/boundary_observe.rs"]
pub(crate) mod observe;
