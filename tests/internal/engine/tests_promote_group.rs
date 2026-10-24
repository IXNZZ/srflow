//! WE-T04 定向样本（成组 Promote 的 Core 内部证据，E21～E23）。
//!
//! 本模块直接构造**真实** `ExecutionContext`／`ScopeCoordinator` 登记（Root → controller →
//! Round 的真实 frame 链与真实 `ControlStateId`），并驱动成组收口的唯一受控入口
//! [`ExecutionContext::promote_group_in_round_boundary`]；不模拟执行器、不绕过登记、不用
//! 睡眠／mock 代替判断路径。
//!
//! 观察使用既有 cfg(test) 独立通道（未被本模块空置）：
//! `take_round_collect_group`（成组收口 Before／AfterReject 完整前后快照）、
//! `take_promote_group_states`（成组提交现场）、`closed_scope_snapshot`（来源关闭计数）。
//! E23 的公开主链（`Runtime::execute` + `Flow::iter`）证据由 `tests_retry_iter` 承担，
//! 本模块只补许可矩阵与"后项拒绝无半提交"的精确内部快照。
use crate::Data;
use crate::core::context::{ExecutionContext, InvocationKind, RoundGroupPermit};
use crate::core::identity::{DataId, ExecutionIdentity};
use crate::core::internal_error::ScopeError;
use crate::core::ref_id::{RefIdAllocator, RefIdSource};
use crate::core::scope::{PromoteGroupOutcome, RefTarget, ScopeState, TargetSnapshot};
use crate::core::test_support::{
    RoundCollectGroupSnapshot, RoundCollectSnapshotPhase, closed_scope_reset,
    closed_scope_snapshot, promote_group_states_reset, round_collect_group_reset,
    take_promote_group_states, take_round_collect_group,
};
use std::any::TypeId;

#[derive(Data)]
#[allow(dead_code)] // 样本只验证状态 target 身份，不读取业务字段。
struct Amount(u32);

/// 每个样本独立复位三条观察通道（thread-local，隔离于其它测试线程）。
fn reset_channels() {
    round_collect_group_reset();
    promote_group_states_reset();
    closed_scope_reset();
}

/// 本地位置分配器：与真实构建同源语义（单调、不复用）。
fn positions() -> RefIdAllocator {
    RefIdAllocator::new(RefIdSource::new())
}

/// 许可／结构层的 typed 拒绝：必须发生在冻结与任何提交之前，因而没有 cleanup 段。
fn rejection(outcome: PromoteGroupOutcome) -> ScopeError {
    match outcome {
        PromoteGroupOutcome::Rejected {
            primary,
            cleanup_failure,
        } => {
            assert!(
                cleanup_failure.is_none(),
                "shape/permit rejection must not run cleanup: {cleanup_failure:?}"
            );
            primary
        }
        PromoteGroupOutcome::Promoted => panic!("expected a typed rejection, got Promoted"),
    }
}

/// 前后快照比较：除观察阶段外逐字段相等（证明冻结之后到后项拒绝之间零副作用）。
fn assert_same_pre_commit(a: &RoundCollectGroupSnapshot, b: &RoundCollectGroupSnapshot) {
    assert_eq!(a.phase, RoundCollectSnapshotPhase::Before);
    assert_eq!(b.phase, RoundCollectSnapshotPhase::AfterReject);
    assert_eq!(a.source, b.source);
    assert_eq!(a.selected, b.selected);
    assert_eq!(a.states, b.states);
    assert_eq!(a.source_refs, b.source_refs);
    assert_eq!(a.source_owned, b.source_owned);
    assert_eq!(a.controller_refs, b.controller_refs);
    assert_eq!(a.controller_owned, b.controller_owned);
    assert_eq!(a.next_data_id, b.next_data_id);
    assert_eq!(a.observation_error, b.observation_error);
}

/// 真实 frame 链：Root → controller（Boundary）→ Round（Boundary）。
///
/// `setup` 在 **controller frame** 内执行（此时可登记 controller 本地引用／控制状态，也可
/// 对已是其直接 child 的 Round 登记输入 refs）；`body` 在 **Round frame** 内执行，驱动成组
/// 收口并读取观察通道。受制于 `enter` 的直接父子门禁，Round 必是 controller 的直接 child。
/// 收尾按 LIFO `complete`（正常退出，不触发 abort 清理）。
macro_rules! group_scenario {
    (
        $ctx:ident, $root:ident, $controller:ident, $cguard:ident, $round:ident, $guard:ident;
        setup { $($setup:tt)* }
        body { $($body:tt)* }
    ) => {{
        let identity = $crate::core::identity::ExecutionIdentity::new();
        let mut $ctx = $crate::core::context::ExecutionContext::new(identity);
        let $root = $ctx.root_scope();
        let mut root_guard = $ctx
            .enter($crate::core::context::InvocationKind::Root, &$root, true)
            .expect("root frame");
        let $controller = root_guard.create_child(&$root).expect("controller scope");
        let mut $cguard = root_guard
            .enter(
                $crate::core::context::InvocationKind::Boundary,
                &$controller,
                true,
            )
            .expect("controller frame");
        let $round = $cguard.create_child(&$controller).expect("round scope");
        $($setup)*
        let mut $guard = $cguard
            .enter(
                $crate::core::context::InvocationKind::Boundary,
                &$round,
                true,
            )
            .expect("round frame");
        $($body)*
        $guard.complete();
        $cguard.complete();
        root_guard.complete();
    }};
}

// ================================================================ E21 许可矩阵

// E21-a：许可的 round 必须是本次收口的 Round（错 Round）。
#[test]
fn e21_wrong_round_is_rejected_before_freeze() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap();
            let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
        }
        body {
            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: root.clone(),
                group: vec![p0.clone()],
                states: vec![s0]};
            let primary = rejection(guard.promote_group_in_round_boundary(&round, &[p0], &permit));
            assert!(matches!(
                primary,
                ScopeError::Invariant { violated } if violated == "group round collect requires the permitted round scope"
            ), "actual: {primary}");
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
            assert!(take_round_collect_group().is_empty(), "no freeze -> no observation");
            assert!(take_promote_group_states().is_empty());
        }
    );
}

// E21-b：frame 的 caller 必须是许可登记的 controller（错父 frame）。
#[test]
fn e21_wrong_controller_frame_is_rejected_before_freeze() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap();
            let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
        }
        body {
            let permit = RoundGroupPermit {
                controller: root.clone(),
                round: round.clone(),
                group: vec![p0.clone()],
                states: vec![s0]};
            let primary = rejection(guard.promote_group_in_round_boundary(&round, &[p0], &permit));
            assert!(matches!(
                primary,
                ScopeError::Invariant { violated } if violated == "group round frame caller is not the registered controller scope"
            ), "actual: {primary}");
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
            assert!(take_round_collect_group().is_empty());
        }
    );
}

// E21-c：目标 state 必须由许可登记的 controller 负责（**owner** 判据）。
//
// 本样本的 state 与 Round 同属**本次 Execution**，owner 是 Round（controller 的直接 child），
// 因此它登记的是 owner 不符，而不是“外域 state”。跨 Execution 的 state 由
// `e21_foreign_execution_state_is_rejected_before_freeze`（StateForeignExecution）单独覆盖；
// 两者判据与诊断都不同，不能在索引里合并。
#[test]
fn e21_state_not_owned_by_registered_controller_is_rejected_before_freeze() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap();
            // 归属 Round（controller 的直接 child）而非 controller：owner 不符（同一 Execution）。
            let round_owned = cguard.register_uninitialized_state::<Amount>(&round).unwrap();
        }
        body {
            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: round.clone(),
                group: vec![p0.clone()],
                states: vec![round_owned]};
            let primary = rejection(guard.promote_group_in_round_boundary(&round, &[p0], &permit));
            assert!(matches!(
                primary,
                ScopeError::Invariant { violated } if violated == "group round permit state is not owned by the registered controller"
            ), "actual: {primary}");
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
            assert!(take_round_collect_group().is_empty());
        }
    );
}

// E21-d：目的 state 逐槽唯一；重复目的 state 是 typed 拒绝（DuplicateStateDestination）。
#[test]
fn e21_duplicate_state_destination_is_rejected_before_freeze() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
            setup {
                let alloc = positions();
                let p0 = alloc.allocate().unwrap();
                let p1 = alloc.allocate().unwrap();
                let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
            }
            body {
                let permit = RoundGroupPermit {
                    controller: controller.clone(),
                    round: round.clone(),
                    group: vec![p0.clone(), p1.clone()],
                    states: vec![s0.clone(), s0.clone()]};
                let primary = rejection(
                    guard.promote_group_in_round_boundary(&round, &[p0, p1], &permit)
    );
                assert!(matches!(
                    primary,
                    ScopeError::DuplicateStateDestination { ref state } if state == &s0
                ), "actual: {primary}");
                assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
                assert!(take_round_collect_group().is_empty());
                assert!(take_promote_group_states().is_empty());
            }
        );
}

// E21-e：许可的 state 数与声明组长度必须一致。
#[test]
fn e21_state_count_mismatch_is_rejected_before_freeze() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
            setup {
                let alloc = positions();
                let p0 = alloc.allocate().unwrap();
                let p1 = alloc.allocate().unwrap();
                let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
            }
            body {
                let permit = RoundGroupPermit {
                    controller: controller.clone(),
                    round: round.clone(),
                    group: vec![p0.clone(), p1.clone()],
                    states: vec![s0]};
                let primary = rejection(
                    guard.promote_group_in_round_boundary(&round, &[p0, p1], &permit)
    );
                assert!(matches!(
                    primary,
                    ScopeError::Invariant { violated } if violated == "group round permit state count does not match the declared group"
                ), "actual: {primary}");
                assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
                assert!(take_round_collect_group().is_empty());
            }
        );
}

// E21-f：空组不得读成 unit Iter 合法。
#[test]
fn e21_empty_group_is_rejected_before_freeze() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {}
        body {
            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: round.clone(),
                group: Vec::new(),
                states: Vec::new()};
            let primary = rejection(guard.promote_group_in_round_boundary(&round, &[], &permit));
            assert!(matches!(
                primary,
                ScopeError::Invariant { violated } if violated == "group round collection requires a non-empty declared group"
            ), "actual: {primary}");
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
            assert!(take_round_collect_group().is_empty());
        }
    );
}

// E21-g：本次 selected 必须与许可的声明组逐槽一致（声明槽错配）。
#[test]
fn e21_selected_slot_mismatch_is_rejected_before_freeze() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap();
            let p1 = alloc.allocate().unwrap();
            let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
        }
        body {
            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: round.clone(),
                group: vec![p0.clone()],
                states: vec![s0]};
            let primary = rejection(guard.promote_group_in_round_boundary(&round, &[p1], &permit));
            assert!(matches!(
                primary,
                ScopeError::Invariant { violated } if violated == "group round collection slot does not match the declared group position"
            ), "actual: {primary}");
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
            assert!(take_round_collect_group().is_empty());
        }
    );
}

// E21-h：selected 长度必须等于许可声明组长度。
#[test]
fn e21_selected_length_mismatch_is_rejected_before_freeze() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap();
            let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
        }
        body {
            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: round.clone(),
                group: vec![p0],
                states: vec![s0]};
            let primary = rejection(guard.promote_group_in_round_boundary(&round, &[], &permit));
            assert!(matches!(
                primary,
                ScopeError::Invariant { violated } if violated == "group round collection length does not match the declared group"
            ), "actual: {primary}");
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
            assert!(take_round_collect_group().is_empty());
        }
    );
}

// E21-i：许可的目标 state 必须属于**本 Execution**；另一 Execution 的 state 是 typed 拒绝，
// 且在任何冻结与任何观察之前（外域判据；与 owner 判据分属不同诊断）。
#[test]
fn e21_foreign_execution_state_is_rejected_before_freeze() {
    reset_channels();
    // 第二个真实 Execution：真实 Root → controller frame，登记一个真实 state。
    let foreign_state = {
        let mut foreign_ctx = ExecutionContext::new(ExecutionIdentity::new());
        let foreign_root = foreign_ctx.root_scope();
        let mut foreign_root_guard = foreign_ctx
            .enter(InvocationKind::Root, &foreign_root, true)
            .expect("foreign root frame");
        let foreign_controller = foreign_root_guard
            .create_child(&foreign_root)
            .expect("foreign controller scope");
        let mut foreign_guard = foreign_root_guard
            .enter(InvocationKind::Boundary, &foreign_controller, true)
            .expect("foreign controller frame");
        let state = foreign_guard
            .register_uninitialized_state::<Amount>(&foreign_controller)
            .expect("foreign state");
        foreign_guard.complete();
        foreign_root_guard.complete();
        state
    };
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap();
        }
        body {
            let before = guard.snapshot_targets_probe(&round).unwrap();
            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: round.clone(),
                group: vec![p0.clone()],
                states: vec![foreign_state.clone()]};
            let primary = rejection(guard.promote_group_in_round_boundary(&round, &[p0], &permit));
            assert!(matches!(
                primary,
                ScopeError::StateForeignExecution { ref state } if state == &foreign_state
            ), "actual: {primary}");
            // 外域判据先于冻结：来源状态与两侧快照不变、没有观察、没有清理分支。
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
            assert_eq!(guard.snapshot_targets_probe(&round).unwrap(), before);
            assert!(
                take_round_collect_group().is_empty(),
                "no freeze -> no Before observation"
            );
            assert!(take_promote_group_states().is_empty());
            assert_eq!(
                closed_scope_snapshot().iter().filter(|scope| *scope == &round).count(),
                0,
                "the rejected group closes nothing"
            );
        }
    );
}

// E21-j：来源 Round 存在真实活跃 descendant 时不得收口（成组专属：来源必须可收口）。
#[test]
fn e21_active_descendant_is_rejected_before_freeze() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap();
            let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
        }
        body {
            // 真实活跃 descendant：Round 的直接 child（真实 Scope，未收口、非伪造状态）。
            let live = guard.create_child(&round).expect("live descendant scope");
            let before = guard.snapshot_targets_probe(&round).unwrap();
            let selected = vec![p0];
            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: round.clone(),
                group: selected.clone(),
                states: vec![s0]};
            let primary =
                rejection(guard.promote_group_in_round_boundary(&round, &selected, &permit));
            assert!(matches!(
                primary,
                ScopeError::ActiveDescendants { ref scope } if scope == &round
            ), "actual: {primary}");
            // 早检先于冻结：来源快照不变、没有观察、没有提交、来源未关闭。
            assert_eq!(guard.snapshot_targets_probe(&round).unwrap(), before);
            assert!(take_round_collect_group().is_empty());
            assert!(take_promote_group_states().is_empty());
            assert_eq!(
                closed_scope_snapshot().iter().filter(|scope| *scope == &round).count(),
                0
            );
            // 收尾：关闭该真实 descendant，Round 本身仍 Active（未被收口）。
            guard.abort(&live).expect("the live descendant is closed");
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Active);
        }
    );
}

// E21-k：目的 state 的声明类型与来源实际类型不符 → 成组预检的类型判据拒绝（逐槽复用同一个
// `prepare_promote`；同一判据的单项实跑证据见既有 `r15_...` 样本）。
#[test]
fn e21_state_type_mismatch_is_rejected_by_group_preflight() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap();
            let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
            cguard
                .register_owned::<Amount>(&round, &p0, Amount(11))
                .unwrap();
            // 窄故障：只改该 state 的声明类型元数据（与来源实际类型不同）。
            cguard
                .corrupt_state_type_probe(&s0, TypeId::of::<u32>(), "u32")
                .unwrap();
        }
        body {
            let selected = vec![p0];
            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: round.clone(),
                group: selected.clone(),
                states: vec![s0]};
            let primary =
                rejection(guard.promote_group_in_round_boundary(&round, &selected, &permit));
            assert!(matches!(primary, ScopeError::TypeMismatch { .. }), "actual: {primary}");
            // 冻结之后到类型拒绝之间零副作用；cleanup 成功后来源真实关闭一次、没有提交现场。
            let observed = take_round_collect_group();
            assert_eq!(observed.len(), 2, "before + after-reject: {observed:?}");
            assert_same_pre_commit(&observed[0], &observed[1]);
            assert!(take_promote_group_states().is_empty(), "no commit site");
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Closed);
            assert_eq!(
                closed_scope_snapshot().iter().filter(|scope| *scope == &round).count(),
                1
            );
        }
    );
}

// ================================================================ E22 后项拒绝无半提交

// E22：合法首项 + 非法后项（后项目标已被销毁）→ 整组拒绝；冻结之后到后项拒绝之间
// 逐字段零副作用，首项未提前提交、owned 未转移；primary 与 cleanup_failure 分开报告。
#[test]
fn e22_late_slot_rejection_leaves_no_partial_commit() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap(); // 合法首项位置（Round-owned）
            let p1 = alloc.allocate().unwrap(); // 非法后项位置（指向已销毁的 controller-owned Data）
            let cpos = alloc.allocate().unwrap(); // controller 本地来源位置
            let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
            let s1 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();

            let first: DataId = cguard
                .register_owned::<Amount>(&round, &p0, Amount(11))
                .unwrap();
            // 后项目标：先由 controller 拥有并合法导入 Round，再销毁底层 entry（保留悬空 ref）。
            cguard
                .register_owned::<Amount>(&controller, &cpos, Amount(22))
                .unwrap();
            cguard
                .import_batch(
                    &round,
                    &controller,
                    &[crate::core::scope::ImportSlot::new::<Amount>(&cpos, &p1)],
                )
                .unwrap();
            let doomed = cguard
                .target_data_id_probe(&controller, &cpos)
                .unwrap()
                .expect("controller-owned import source");
            cguard.destroy_probe(&doomed);
        }
        body {
            let before = guard.snapshot_targets_probe(&round).unwrap();
            assert!(before.0.iter().any(|(position, _)| position == &p0));
            assert!(before.0.iter().any(|(position, _)| position == &p1));
            assert!(before.1.contains(&first), "首项仍是 Round-owned");
            let before_next = guard.next_data_id_probe();

            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: round.clone(),
                group: vec![p0.clone(), p1.clone()],
                states: vec![s0.clone(), s1.clone()]};
            let outcome =
                guard.promote_group_in_round_boundary(&round, &[p0.clone(), p1.clone()], &permit);

            let (primary, cleanup_failure) = match outcome {
                PromoteGroupOutcome::Rejected { primary, cleanup_failure } => (primary, cleanup_failure),
                PromoteGroupOutcome::Promoted => {
                    panic!("a dead late target must reject the whole group")
                }
            };
            // primary 是后项的真实 typed 拒绝；cleanup 成功，因而 cleanup_failure 分开为 None。
            assert!(matches!(
                primary,
                ScopeError::TargetNotAlive { ref position } if position == &p1
            ), "actual primary: {primary}");
            assert!(cleanup_failure.is_none(), "cleanup of the source succeeded: {cleanup_failure:?}");

            // 真实前后快照：Before（冻结后、任何 prepare 前）与 AfterReject（后项拒绝后、cleanup 前）。
            let observed = take_round_collect_group();
            assert_eq!(observed.len(), 2, "before + after-reject observations: {observed:?}");
            assert_eq!(observed[0].observation_error, None);
            assert_eq!(observed[1].observation_error, None);
            assert_same_pre_commit(&observed[0], &observed[1]);

            // 全部 state target 仍为空、pending 未变（首项未提前提交）。
            for snapshot in [&observed[0], &observed[1]] {
                assert!(snapshot.states.iter().all(|state| state.target.is_none()));
                assert!(snapshot.states.iter().all(|state| state.pending.is_empty()));
                // 首项仍由 Round 持有且存活；未转移给 controller。
                assert_eq!(snapshot.selected[0].target, Some(TargetSnapshot::Data(first.clone())));
                assert_eq!(snapshot.selected[0].owner.as_ref(), Some(&round));
                assert!(snapshot.selected[0].alive);
                assert!(snapshot.source_owned.contains(&first));
                assert!(!snapshot.controller_owned.contains(&first));
            }
            // 两侧 owned 在拒绝前后逐字段相等（含首项仍归 Round）。
            assert_eq!(observed[0].source_owned, observed[1].source_owned);
            assert_eq!(observed[0].controller_owned, observed[1].controller_owned);
            // 没有提交现场被记录：整组没有半提交。
            assert!(take_promote_group_states().is_empty(), "no commit site for a rejected group");

            // 冻结之后的拒绝按既有纪律清理来源：Round 关闭一次，下一 DataId 序号不前进。
            assert_eq!(guard.next_data_id_probe(), before_next);
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Closed);
            assert_eq!(
                closed_scope_snapshot().iter().filter(|scope| *scope == &round).count(),
                1,
                "the source Round closes exactly once"
            );
        }
    );
}

// ================================================================ E23 成组成功

// E23-a：共同更新全部 state target；重复 target 共享同一 DataId 时责任只转移一次；
// 来源 Round 只关闭一次；父 RefId 未绑定／未重绑；pending 正确登记。
#[test]
fn e23_group_promote_updates_all_states_and_transfers_a_shared_id_once() {
    reset_channels();
    group_scenario!(ctx, root, controller, cguard, round, guard;
        setup {
            let alloc = positions();
            let p0 = alloc.allocate().unwrap();
            let p1 = alloc.allocate().unwrap();
            let s0 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();
            let s1 = cguard.register_uninitialized_state::<Amount>(&controller).unwrap();

            // 两个声明槽共享同一物理实例（合法 alias 样本）。
            let shared = cguard
                .register_owned::<Amount>(&round, &p0, Amount(7))
                .unwrap();
            cguard.inject_scope_target_probe(&round, &p1, RefTarget::Data(shared.clone()));
        }
        body {
            let controller_refs_before = guard.snapshot_targets_probe(&controller).unwrap().0;
            let next_before = guard.next_data_id_probe();

            let permit = RoundGroupPermit {
                controller: controller.clone(),
                round: round.clone(),
                group: vec![p0.clone(), p1.clone()],
                states: vec![s0.clone(), s1.clone()]};
            let outcome =
                guard.promote_group_in_round_boundary(&round, &[p0.clone(), p1.clone()], &permit);
            assert!(matches!(outcome, PromoteGroupOutcome::Promoted), "actual: {outcome:?}");

            // 观察通道被真实消费：一个 Before、无 AfterReject。
            let observed = take_round_collect_group();
            assert_eq!(observed.len(), 1);
            assert_eq!(observed[0].phase, RoundCollectSnapshotPhase::Before);
            assert_eq!(observed[0].observation_error, None);

            // 逐槽提交现场。
            let records = take_promote_group_states();
            assert_eq!(records.len(), 1, "one commit site per successful group");
            let record = &records[0];
            assert_eq!(&record.controller, &controller);
            assert_eq!(&record.source, &round);
            assert_eq!(record.states.len(), 2);
            for (owner, _seq, target, pending) in &record.states {
                assert_eq!(owner, &controller);
                assert_eq!(target, &Some(TargetSnapshot::Data(shared.clone())));
                assert!(pending.is_empty(), "no replaced value is pending on first init");
            }
            // 责任按 DataId 去重：共享实例只出现一次，且已进入 controller.owned。
            assert_eq!(record.transferred, vec![shared.clone()]);
            assert_eq!(
                record.controller_owned.iter().filter(|id| **id == shared).count(),
                1,
                "owned 集合幂等：共享实例只保留一份责任"
            );

            // 来源 Round 只关闭一次。
            assert_eq!(guard.state(&round).unwrap(), ScopeState::Closed);
            assert_eq!(
                closed_scope_snapshot().iter().filter(|scope| *scope == &round).count(),
                1
            );
            // 父 RefId 未绑定／未重绑：controller 本地位置集合与分配序号都不变。
            assert_eq!(
                guard.snapshot_targets_probe(&controller).unwrap().0,
                controller_refs_before
            );
            assert!(controller_refs_before.is_empty());
            assert_eq!(guard.next_data_id_probe(), next_before, "promote allocates no new DataId");
        }
    );
}
