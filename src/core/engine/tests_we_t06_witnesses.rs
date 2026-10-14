//! WE-T06 §5.3／X15／X16／X19／X20：T04 R2 九条补充见证与 T05 R1 三条上游 cap／类型防御反例的正式等价化。
//!
//! 目标：让这些判据成为普通 `cargo test` 可驱动、**无需临时改生产源**即可接入的正式证据；
//! 原补丁、源码与日志作为历史出处保留（见 `docs/workflow_engine_tasks/WE_T06_PROBE/W06_witness_map.md`）。
//!
//! 覆盖矩阵（逐条对应任务书 X 行）：
//! - X15：Retry／Iter 两跳 Runtime Export 后项拒绝 + 实际 Root 零 take（4 条，`Runtime::execute` 全链驱动）；
//! - X16：成组 Promote 的 item cap 成功／后项 cap 拒绝／剩余 owned 清理前提（3 条，Core 受控入口）；
//! - X19：Retry／Iter 预算终止下"后续不同清理诊断不覆盖首诊断"（2 条，含公开两通道）；
//! - X20：定位链上游 cap 越界／已关闭／实际集合类型防御（3 条，本层合法也不能绕过回溯）。
//!
//! 故障来源纪律：凡由 cfg(test) 窄钩子或窄 probe 构造的内部前提，样本内均显式写明
//! "合成内部前提，自然业务不会产生"；断言取真实值／身份／计数／事件顺序，不做壳断言。
//!
//! 归属：本文件由 WE-T06 的见证正式化工作项独占（W06）。

use crate::Data;
use crate::core::context::{ExecutionContext, InvocationKind, RoundGroupPermit};
use crate::core::engine::boundary::{
    ControlDeliveryOccupyHop, install_control_delivery_occupy_fault,
};
use crate::core::engine::error::{BodyError, FrameworkDiagnostic, RunError};
use crate::core::engine::observe;
use crate::core::engine::runtime::{Runtime, install_root_cleanup_faults};
use crate::core::engine::shape::Query;
use crate::core::identity::{DataId, ExecutionIdentity, ScopeId};
use crate::core::internal_error::ScopeError;
use crate::core::ref_id::{RefId, RefIdAllocator, RefIdSource};
use crate::core::scope::{
    CollectionRoot, ImportSlot, ItemAccess, PromoteGroupOutcome, RefTarget, ScopeCoordinator,
    ScopeState, TargetSnapshot,
};
use crate::core::test_support as ts;
use futures::executor::block_on;
use std::cell::Cell;
use std::error::Error;

// ================================================================ X16：成组 Promote（Core 受控入口）

/// 集合元素类型（非内建 Data；与 T04 R2 见证同型，样本只核对身份与状态现场）。
#[derive(Data, Debug, PartialEq, Eq)]
struct Amount(u32);

/// 每个样本独立复位本文件使用的观察通道（thread-local，隔离其它测试线程）。
fn reset_channels() {
    ts::reset_observations();
    ts::round_collect_group_reset();
    ts::promote_group_states_reset();
    ts::closed_scope_reset();
}

/// 与真实构建同源的本地位置分配器（单调、不复用）。
fn positions() -> RefIdAllocator {
    RefIdAllocator::new(RefIdSource::new())
}

/// 成组 Promote 的 item-backed 收口样本主体。
///
/// 真实 frame 链：Root（拥有 `Vec<Amount>`）→ cap（ItemScope，item 目标的 lifetime cap）→
/// controller（两个控制状态的 owner）→ Round（两个声明槽）。
///
/// `reject == false`：两个槽都是**合法 item 别名**（同一 cap 内），成组 Promote 成功，
/// 状态只换成 item 目标、不转移任何 owned 责任（item 没有独立 owner）。
/// `reject == true`：后项槽的 item cap 被 `corrupt_item_cap_probe` 改成 Round 自身——
/// **合成内部前提**：公开 `Flow` 构造无法把 item 目标的 cap 改成非祖先，自然业务不会产生；
/// 用于核对"后项目的位置拒绝时整组零提交、零重绑"。
fn group_item_cap(reject: bool) {
    reset_channels();
    let mut ctx = ExecutionContext::new(ExecutionIdentity::new());
    let root = ctx.root_scope();
    let mut root_guard = ctx.enter(InvocationKind::Root, &root, true).unwrap();
    let alloc = positions();
    let collection_position = alloc.allocate().unwrap();
    let item_position = alloc.allocate().unwrap();

    // Root 真实拥有集合 `vec![Amount(9)]`；cap 是它的直接 child（ItemScope）。
    let collection_id = root_guard
        .register_owned(&root, &collection_position, vec![Amount(9)])
        .unwrap();
    let cap = root_guard.create_child(&root).unwrap();
    root_guard
        .bind_item_input::<Amount>(&cap, &root, &collection_position, &item_position, 0)
        .unwrap();

    let mut cap_guard = root_guard
        .enter(InvocationKind::Boundary, &cap, true)
        .unwrap();
    let controller = cap_guard.create_child(&cap).unwrap();
    cap_guard
        .import_batch(
            &controller,
            &cap,
            &[ImportSlot::new::<Amount>(&item_position, &item_position)],
        )
        .unwrap();
    let mut controller_guard = cap_guard
        .enter(InvocationKind::Boundary, &controller, true)
        .unwrap();

    // 两个控制状态都以同一 item 位置初始化：Promote 后应各自指向该 item 目标。
    let state0 = controller_guard
        .register_state::<Amount>(&controller, &item_position)
        .unwrap();
    let state1 = controller_guard
        .register_state::<Amount>(&controller, &item_position)
        .unwrap();

    let round = controller_guard.create_child(&controller).unwrap();
    let slot0 = alloc.allocate().unwrap();
    let slot1 = alloc.allocate().unwrap();
    // 后项槽先合法绑定为同一 item 的别名（拒绝路径随后把它的 cap 破坏）。
    controller_guard
        .import_batch(
            &round,
            &controller,
            &[ImportSlot::new::<Amount>(&item_position, &slot1)],
        )
        .unwrap();

    if reject {
        // 首项槽绑定本轮新产生的 owned 值；后项槽的 item cap 被改成 Round（合成前提）。
        controller_guard
            .register_owned(&round, &slot0, Amount(12))
            .unwrap();
        controller_guard
            .corrupt_item_cap_probe(&round, &slot1, round.clone())
            .unwrap();
    } else {
        controller_guard
            .import_batch(
                &round,
                &controller,
                &[ImportSlot::new::<Amount>(&item_position, &slot0)],
            )
            .unwrap();
    }

    // Round 的收口调用必须在登记完成之后进入（guard 独占借用 controller frame）。
    let mut guard = controller_guard
        .enter(InvocationKind::Boundary, &round, true)
        .unwrap();

    let selected = vec![slot0.clone(), slot1.clone()];
    let permit = RoundGroupPermit {
        controller: controller.clone(),
        round: round.clone(),
        group: selected.clone(),
        states: vec![state0.clone(), state1.clone()],
    };
    let outcome = guard.promote_group_in_round_boundary(&round, &selected, &permit);
    let snapshots = ts::take_round_collect_group();

    if reject {
        let PromoteGroupOutcome::Rejected {
            primary,
            cleanup_failure,
        } = outcome
        else {
            panic!("expected the late item-cap rejection, got {outcome:?}");
        };
        // 精确诊断：后项槽位置、被破坏的 cap（Round 自身）、真实请求方（controller）。
        assert!(
            matches!(
                &primary,
                ScopeError::ItemOutsideCap { position, cap: rejected_cap, requester }
                    if position == &slot1 && rejected_cap == &round && requester == &controller
            ),
            "late item cap rejection: {primary:?}"
        );
        // 拒绝后来源清理前提仍完好，因此没有第二诊断。
        assert!(cleanup_failure.is_none(), "{cleanup_failure:?}");

        // 前后两次完整快照（冻结之后 / 后项拒绝之后、清理之前）：除阶段外逐字段相等。
        assert_eq!(snapshots.len(), 2, "{snapshots:?}");
        let before = &snapshots[0];
        let after = &snapshots[1];
        assert_eq!(before.phase, ts::RoundCollectSnapshotPhase::Before);
        assert_eq!(after.phase, ts::RoundCollectSnapshotPhase::AfterReject);
        assert_eq!(before.observation_error, None);
        assert_eq!(after.observation_error, None);
        assert_eq!(before.source, round);
        assert_eq!(before.selected, after.selected);
        assert_eq!(before.states, after.states);
        assert_eq!(before.source_refs, after.source_refs);
        assert_eq!(before.source_owned, after.source_owned);
        assert_eq!(before.controller_refs, after.controller_refs);
        assert_eq!(before.controller_owned, after.controller_owned);
        assert_eq!(before.next_data_id, after.next_data_id);
        // 两个声明的槽仍与 Promote 前逐字节相同：没有半提交、没有被重绑。
        assert_eq!(before.selected.len(), 2, "{before:?}");
        // 首项槽是本轮新产生的 owned 值，仍由 Round 拥有（未被移动）。
        assert!(
            matches!(&before.selected[0].target, Some(TargetSnapshot::Data(_))),
            "the first slot stays the round's own produced value: {:?}",
            before.selected[0]
        );
        assert_eq!(before.selected[0].owner.as_ref(), Some(&round));
        assert!(before.selected[0].alive);
        // 后项槽是被破坏 cap 的 item 目标：item 没有独立 owner，也不单独存活。
        assert!(
            matches!(
                &before.selected[1].target,
                Some(TargetSnapshot::CollectionItem { collection, lifetime_cap, .. })
                    if collection == &collection_id && lifetime_cap == &round
            ),
            "the late slot keeps its corrupted item target: {:?}",
            before.selected[1]
        );
        assert_eq!(before.selected[1].owner, None);
        assert!(!before.selected[1].alive);
        for state in &before.states {
            assert!(state.pending.is_empty());
            assert!(matches!(
                &state.target,
                Some(TargetSnapshot::CollectionItem { collection, lifetime_cap, .. })
                    if collection == &collection_id && lifetime_cap == &cap
            ));
        }
        // 零提交现场：整组在任何 effects 之前拒绝。
        assert!(
            ts::take_promote_group_states().is_empty(),
            "no commit site for a rejected group"
        );
        // 拒绝路径真实清理来源：Round 只关闭一次。
        assert_eq!(guard.state(&round).unwrap(), ScopeState::Closed);
        assert_eq!(
            ts::closed_scope_snapshot()
                .iter()
                .filter(|scope| *scope == &round)
                .count(),
            1
        );
    } else {
        assert!(
            matches!(outcome, PromoteGroupOutcome::Promoted),
            "legal item aliases promote as a group: {outcome:?}"
        );
        // 成功路径只记录冻结前快照（没有 AfterReject）。
        assert_eq!(snapshots.len(), 1, "{snapshots:?}");
        assert_eq!(snapshots[0].phase, ts::RoundCollectSnapshotPhase::Before);
        assert_eq!(snapshots[0].observation_error, None);

        let commits = ts::take_promote_group_states();
        assert_eq!(commits.len(), 1, "one commit site for a promoted group");
        let commit = &commits[0];
        assert_eq!(commit.controller, controller);
        assert_eq!(commit.source, round);
        assert_eq!(commit.states.len(), 2);
        for (owner, _seq, target, pending) in &commit.states {
            assert_eq!(owner, &controller);
            assert!(pending.is_empty(), "item targets record no replaced value");
            assert!(
                matches!(
                    target,
                    Some(TargetSnapshot::CollectionItem { collection, lifetime_cap, .. })
                        if collection == &collection_id && lifetime_cap == &cap
                ),
                "each state points at the item target: {target:?}"
            );
        }
        // item 没有独立 owner：责任不转移，controller 的 owned 集合不变。
        assert!(commit.transferred.is_empty(), "{:?}", commit.transferred);
        assert!(
            commit.controller_owned.is_empty(),
            "{:?}",
            commit.controller_owned
        );
        assert_eq!(guard.state(&round).unwrap(), ScopeState::Closed);
        assert_eq!(
            ts::closed_scope_snapshot()
                .iter()
                .filter(|scope| *scope == &round)
                .count(),
            1
        );
    }

    // 收尾：Round 收口后 controller 仍能读到同一 item 元素，集合责任仍在 Root。
    guard.release_responsibility(&round);
    guard.complete();
    assert_eq!(
        controller_guard
            .resolve::<Amount>(&controller, &item_position)
            .unwrap(),
        &Amount(9)
    );
    assert!(controller_guard.alive_probe(&collection_id));
    assert_eq!(controller_guard.owner_probe(&collection_id).unwrap(), root);
    controller_guard.complete();
    cap_guard.complete();
    root_guard.failed("W06 item-promote sample teardown");
    reset_channels();
}

/// X16-a：item-backed 成组 Promote 成功——两个状态共同指向真实 item 目标，且不把 item 变成
/// controller 的 owned（来源权保持、无第二次 owner）。
#[test]
fn w06_promote_group_item_cap_success_without_reowning() {
    group_item_cap(false);
}

/// X16-b：后项 item cap 越界 → 整组在任何提交之前拒绝，前后快照逐字段相等（无半提交）。
#[test]
fn w06_promote_group_late_item_cap_rejection_without_partial_commit() {
    group_item_cap(true);
}

/// X16-c：剩余 owned 的清理前提（来源仍有一个已被销毁的 owned entry）必须在提交前校验：
/// 拒绝带真实 `primary` 与其后的清理诊断，两槽状态与来源 owned 全部未变，Round 未关闭、
/// 责任仍在 guard 上（没有 caller 半成功）。
#[test]
fn w06_promote_group_remaining_owned_cleanup_precondition_rejects_before_effects() {
    reset_channels();
    let mut ctx = ExecutionContext::new(ExecutionIdentity::new());
    let root = ctx.root_scope();
    let mut root_guard = ctx.enter(InvocationKind::Root, &root, true).unwrap();
    let controller = root_guard.create_child(&root).unwrap();
    let mut controller_guard = root_guard
        .enter(InvocationKind::Boundary, &controller, true)
        .unwrap();
    let states = vec![
        controller_guard
            .register_uninitialized_state::<Amount>(&controller)
            .unwrap(),
        controller_guard
            .register_uninitialized_state::<Amount>(&controller)
            .unwrap(),
    ];
    let round = controller_guard.create_child(&controller).unwrap();
    let alloc = positions();
    let slot0 = alloc.allocate().unwrap();
    let slot1 = alloc.allocate().unwrap();
    let junk_position = alloc.allocate().unwrap();

    // 两个候选槽都是合法 owned 值；另有一个 owned entry 被销毁（合成内部前提：只由
    // `destroy_probe` 构造，公开 Flow 不会产生"责任记录指向已销毁实例"）。
    let first = controller_guard
        .register_owned(&round, &slot0, Amount(1))
        .unwrap();
    let second = controller_guard
        .register_owned(&round, &slot1, Amount(2))
        .unwrap();
    let broken = controller_guard
        .register_owned(&round, &junk_position, 0u8)
        .unwrap();
    controller_guard.destroy_probe(&broken);

    let mut guard = controller_guard
        .enter(InvocationKind::Boundary, &round, true)
        .unwrap();
    let selected = vec![slot0, slot1];
    let permit = RoundGroupPermit {
        controller: controller.clone(),
        round: round.clone(),
        group: selected.clone(),
        states,
    };
    let outcome = guard.promote_group_in_round_boundary(&round, &selected, &permit);
    let PromoteGroupOutcome::Rejected {
        primary,
        cleanup_failure,
    } = outcome
    else {
        panic!("a broken remaining owned entry must reject the group: {outcome:?}");
    };
    // 提交前校验的真实诊断：清理前提（首因）。
    assert!(
        matches!(
            &primary,
            ScopeError::Invariant {
                violated: "owned entry must exist until its scope closes"
            }
        ),
        "{primary:?}"
    );
    // 拒绝后对来源的真实清理也失败，作为**第二诊断**单独保留（不是首因）。
    assert!(
        matches!(
            cleanup_failure,
            Some(ScopeError::Invariant {
                violated: "owned entry must exist until its scope closes"
            })
        ),
        "{cleanup_failure:?}"
    );

    let snapshots = ts::take_round_collect_group();
    assert_eq!(snapshots.len(), 2, "{snapshots:?}");
    let before = &snapshots[0];
    let after = &snapshots[1];
    assert_eq!(before.phase, ts::RoundCollectSnapshotPhase::Before);
    assert_eq!(after.phase, ts::RoundCollectSnapshotPhase::AfterReject);
    assert_eq!(before.observation_error, None);
    assert_eq!(after.observation_error, None);
    assert_eq!(before.selected, after.selected);
    assert_eq!(before.states, after.states);
    assert_eq!(before.source_refs, after.source_refs);
    assert_eq!(before.source_owned, after.source_owned);
    assert_eq!(before.controller_refs, after.controller_refs);
    assert_eq!(before.controller_owned, after.controller_owned);
    assert_eq!(before.next_data_id, after.next_data_id);
    // 零 effects：状态仍未初始化；两个候选实例仍由 Round 拥有（未移动）。
    for state in &before.states {
        assert_eq!(state.target, None);
        assert!(state.pending.is_empty());
    }
    assert!(before.source_owned.contains(&first));
    assert!(before.source_owned.contains(&second));
    assert!(before.controller_owned.is_empty());
    assert!(ts::take_promote_group_states().is_empty(), "no commit site");

    // 来源未关闭，责任仍在本 guard 上：没有 caller 半成功。
    assert_ne!(guard.state(&round).unwrap(), ScopeState::Closed);
    assert_eq!(guard.responsible_scopes(), &[round]);

    guard.failed("W06 broken-cleanup-promote sample round teardown");
    controller_guard.failed("W06 broken-cleanup-promote sample controller teardown");
    root_guard.failed("W06 broken-cleanup-promote sample root teardown");
    reset_channels();
}

// ================================================================ X20：定位链上游 cap／类型回溯防御

/// item 元素类型（非内建 Data；与 T05 R1 见证同型）。
#[derive(Data, Debug, PartialEq, Eq)]
struct Nest(u32);

/// 以真实协调组件入口驱动的夹具：不使用 Context frame，因此可直接观测 `TargetSnapshot`
/// 并注入窄 probe 目标（与 `tests_each_nested.rs` 的 Core 级样本同法）。
struct Fixture {
    coordinator: ScopeCoordinator,
    ids: RefIdAllocator,
}

impl Fixture {
    fn new() -> Self {
        Self {
            coordinator: ScopeCoordinator::new(ExecutionIdentity::new()),
            ids: RefIdAllocator::new(RefIdSource::new()),
        }
    }

    fn ref_id(&self) -> RefId {
        self.ids.allocate().unwrap()
    }

    fn root(&self) -> ScopeId {
        self.coordinator.root()
    }

    fn child(&mut self, parent: &ScopeId) -> ScopeId {
        self.coordinator.create_child(parent).unwrap()
    }
}

/// 两层 item-backed 集合的真实结构：Root 拥有 `Vec<Vec<Nest>>` → 外层 Each 导入 →
/// 外层 item（下标 0）→ 内层 Each 导入该 item → 内层 item（下标 1）。
struct NestedStack {
    outer_item: ScopeId,
    inner_item: ScopeId,
    inner_item_position: RefId,
    collection: DataId,
}

fn nested_stack(f: &mut Fixture) -> NestedStack {
    let root = f.root();
    let collection_position = f.ref_id();
    let collection = f
        .coordinator
        .register_owned(
            &root,
            &collection_position,
            vec![vec![Nest(1), Nest(2)], vec![Nest(3)]],
        )
        .unwrap();

    let outer_each = f.child(&root);
    let outer_each_collection = f.ref_id();
    f.coordinator
        .import_batch(
            &outer_each,
            &root,
            &[ImportSlot::new::<Vec<Vec<Nest>>>(
                &collection_position,
                &outer_each_collection,
            )],
        )
        .unwrap();
    let outer_item = f.child(&outer_each);
    let outer_item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<Vec<Nest>>(
            &outer_item,
            &outer_each,
            &outer_each_collection,
            &outer_item_position,
            0,
        )
        .unwrap();

    let inner_each = f.child(&outer_item);
    let inner_each_collection = f.ref_id();
    f.coordinator
        .import_batch(
            &inner_each,
            &outer_item,
            &[ImportSlot::new::<Vec<Nest>>(
                &outer_item_position,
                &inner_each_collection,
            )],
        )
        .unwrap();
    let inner_item = f.child(&inner_each);
    let inner_item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<Nest>(
            &inner_item,
            &inner_each,
            &inner_each_collection,
            &inner_item_position,
            1,
        )
        .expect("a legal item-backed collection binds its element");

    NestedStack {
        outer_item,
        inner_item,
        inner_item_position,
        collection,
    }
}

/// 复现嵌套 item 目标：外层 item 的第 0 个元素作为内层集合、内层元素下标 1；两层 cap 分别
/// 是外层／内层 ItemScope。只用于构造 cap／类型反例，不改判断路径。
fn nested_target(stack: &NestedStack) -> RefTarget {
    RefTarget::CollectionItem {
        collection: CollectionRoot::Item(Box::new(RefTarget::CollectionItem {
            collection: CollectionRoot::Data(stack.collection.clone()),
            index: 0,
            lifetime_cap: stack.outer_item.clone(),
            access: ItemAccess::for_collection::<Vec<Nest>>(),
        })),
        index: 1,
        lifetime_cap: stack.inner_item.clone(),
        access: ItemAccess::for_collection::<Nest>(),
    }
}

/// X20-a：上游 cap 变成**存活 sibling**（不是内层 item 的祖先）→ 回溯拒绝，即使本层 cap
/// 自身合法；责任链上不产生任何 owned。
#[test]
fn w06_upstream_cap_cannot_be_a_live_sibling() {
    reset_channels();
    let mut f = Fixture::new();
    let stack = nested_stack(&mut f);
    let root = f.root();
    let wrong_cap = f.child(&root);

    // 合成内部前提：只替换外层 item 目标的 cap 元数据（自然业务不会产生非祖先 cap）。
    let mut target = nested_target(&stack);
    if let RefTarget::CollectionItem {
        collection: CollectionRoot::Item(outer),
        ..
    } = &mut target
        && let RefTarget::CollectionItem { lifetime_cap, .. } = outer.as_mut()
    {
        *lifetime_cap = wrong_cap.clone();
    }
    f.coordinator
        .inject_target_probe(&stack.inner_item, &stack.inner_item_position, target);

    let error = f
        .coordinator
        .resolve::<Nest>(&stack.inner_item, &stack.inner_item_position)
        .map(|_| ())
        .expect_err("a live sibling upstream cap must be rejected by backtracking");
    assert!(
        matches!(
            &error,
            ScopeError::ItemOutsideCap { position, cap, requester }
                if position == &stack.inner_item_position
                    && cap == &wrong_cap
                    && requester == &stack.inner_item
        ),
        "got {error:?}"
    );
    assert_eq!(
        f.coordinator.owned_len_probe(&stack.inner_item).unwrap(),
        0,
        "a rejected nested read registers no owned data"
    );
    assert_eq!(f.coordinator.owned_len_probe(&root).unwrap(), 1);
    f.coordinator.abort(&root).unwrap();
    reset_channels();
}

/// X20-b：上游 cap 已关闭 → 即使本层 cap 与本地状态都合法（内层 item 仍 Active），也必须
/// 在回溯处拒绝，且不复活旧身份。
#[test]
fn w06_closed_upstream_cap_is_checked_even_when_local_cap_is_active() {
    reset_channels();
    let mut f = Fixture::new();
    let stack = nested_stack(&mut f);
    let root = f.root();
    let closed_cap = f.child(&root);
    f.coordinator.abort(&closed_cap).unwrap();

    // 合成内部前提：只替换外层 item 目标的 cap 元数据为已关闭 Scope。
    let mut target = nested_target(&stack);
    if let RefTarget::CollectionItem {
        collection: CollectionRoot::Item(outer),
        ..
    } = &mut target
        && let RefTarget::CollectionItem { lifetime_cap, .. } = outer.as_mut()
    {
        *lifetime_cap = closed_cap.clone();
    }
    f.coordinator
        .inject_target_probe(&stack.inner_item, &stack.inner_item_position, target);

    // 本层依然合法（Active 且 cap 是自己的 item scope）——不能绕过上游拒绝。
    assert_eq!(
        f.coordinator.state(&stack.inner_item).unwrap(),
        ScopeState::Active
    );
    let error = f
        .coordinator
        .resolve::<Nest>(&stack.inner_item, &stack.inner_item_position)
        .map(|_| ())
        .expect_err("a closed upstream cap must be rejected by backtracking");
    assert!(
        matches!(&error, ScopeError::ScopeClosed { scope } if scope == &closed_cap),
        "got {error:?}"
    );
    assert_eq!(f.coordinator.owned_len_probe(&stack.inner_item).unwrap(), 0);
    f.coordinator.abort(&root).unwrap();
    reset_channels();
}

/// X20-c：本层 metadata 声称的集合类型与上游元素的**真实**类型不符 → 拒绝，`actual` 是真实
/// 集合元素类型名（不靠元数据背书）。
#[test]
fn w06_upstream_actual_value_type_is_not_replaced_by_local_metadata() {
    reset_channels();
    let mut f = Fixture::new();
    let stack = nested_stack(&mut f);
    let root = f.root();

    // 合成内部前提：只替换内层 item 目标的访问描述元数据（声明集合为 `Vec<u32>`）。
    let mut target = nested_target(&stack);
    if let RefTarget::CollectionItem { access, .. } = &mut target {
        *access = ItemAccess::for_collection::<u32>();
    }
    f.coordinator
        .inject_target_probe(&stack.inner_item, &stack.inner_item_position, target);

    let error = f
        .coordinator
        .resolve::<u32>(&stack.inner_item, &stack.inner_item_position)
        .map(|_| ())
        .expect_err("the real upstream element type must be re-checked");
    assert!(
        matches!(
            &error,
            ScopeError::TypeMismatch { position, expected, .. }
                if position == &stack.inner_item_position
                    && *expected == std::any::type_name::<Vec<u32>>()
        ),
        "got {error:?}"
    );
    // 如实登记当前诊断的 `actual` 字段：production 在回溯处投影出的是 `&dyn Any`，因此
    // `actual` 是 `"dyn core::any::Any"`（真实拒绝不变；字段信息量不足，见本波次 defect 登记）。
    let ScopeError::TypeMismatch { actual, .. } = &error else {
        unreachable!("the match above proved the variant")
    };
    assert_eq!(
        *actual, "dyn core::any::Any",
        "current diagnostic field value"
    );
    assert_eq!(f.coordinator.owned_len_probe(&stack.inner_item).unwrap(), 0);
    assert_eq!(f.coordinator.owned_len_probe(&root).unwrap(), 1);
    f.coordinator.abort(&root).unwrap();
    reset_channels();
}

// ================================================================ X15：Retry／Iter 两跳 Runtime Export 后项拒绝

thread_local! {
    /// 交付拒绝后不应再被调用的下游节点（`later`）的真实调用次数。
    static LATER_CALLS: Cell<u32> = const { Cell::new(0) };
}

/// 运行期样本的完整复位（含 take 记录、严格观察错误与观察通道）。
fn reset_runtime_channels() {
    ts::strict_reset_observations();
    ts::round_collect_group_reset();
    ts::promote_group_states_reset();
    ts::closed_scope_reset();
    observe::reset();
    LATER_CALLS.with(|calls| calls.set(0));
}

async fn plus(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(*query.get() + 1)
}

async fn break_now(_: Query<&u32>) -> Result<(), BodyError> {
    Err(BodyError::iter_break())
}

async fn later(_: Query<(&u32, &u32)>) -> Result<(), BodyError> {
    LATER_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(())
}

/// 4 条 X15 见证的公共主体：`hop == 1` 是**第一跳**（round → controller）的后项目的位置被
/// 预占，`hop == 2` 是**第二跳**（controller → caller）的后项目的位置被预占。
///
/// 故障来源：`install_control_delivery_occupy_fault` 是 cfg(test) 窄钩子，只以正式
/// `register_owned` 在交付前预占一个目的位置（**合成内部前提**：公开 `Flow` 构造无法把交付
/// 目的位置预先绑定，故本判据没有生产可达样本）；驱动与拒绝全部走真实主链
/// `Runtime::execute → invoke_iter／invoke_retry → deliver_round／export_group → finalize`，
/// 不伪造 `Err`、不绕过判断路径。
fn runtime_export_rejection(iter: bool, hop: u8) {
    reset_runtime_channels();
    install_control_delivery_occupy_fault(
        if hop == 1 {
            ControlDeliveryOccupyHop::RoundToController
        } else {
            ControlDeliveryOccupyHop::ControllerToCaller
        },
        1, // 后项槽
    );

    let result = if iter {
        block_on(Runtime::new().execute(
            |flow, input| {
                let output = flow.iter(input, 3, |(a, b), round| {
                    let x = round.then(plus, a);
                    let y = round.then(plus, b);
                    round.then(break_now, a);
                    (x, y)
                });
                flow.then(later, output);
                output
            },
            (7u32, 8u32),
        ))
    } else {
        block_on(Runtime::new().execute(
            |flow, input| {
                let output = flow.retry(0, |attempt| {
                    (attempt.then(plus, input), attempt.then(plus, input))
                });
                flow.then(later, output);
                output
            },
            7u32,
        ))
    };

    let error = result.expect_err("an occupied late destination must reject the whole delivery");
    let RunError::Runtime(runtime_error) = error else {
        panic!("a delivery rejection is a framework runtime error: {error}");
    };
    let kind = if iter { "iter" } else { "retry" };
    assert_eq!(
        runtime_error.note(),
        if iter {
            "iter output handover rejected"
        } else {
            "retry output handover rejected"
        }
    );

    // 两跳两侧的真实身份均取自生产路径的观察通道。
    let rounds = observe::take_controller_rounds();
    let created = rounds
        .iter()
        .find(|record| record.kind == kind && record.at == "controller-created")
        .expect("the controller creation is observed");
    let controller = created.controller.clone();
    let caller = created.caller.clone();
    let round = rounds
        .iter()
        .find(|record| record.kind == kind && record.at == "round-created")
        .and_then(|record| record.round.clone())
        .expect("the round creation is observed");
    // 被预占的目的位置属于哪一侧，以及该次失败交付的来源侧。
    let (occupied, source_scope) = if hop == 1 {
        (controller.clone(), round.clone())
    } else {
        (caller.clone(), controller.clone())
    };

    let exports = ts::take_export_pre_cleanup();
    let rejected: Vec<&ts::ExportPreCleanupSnapshot> = exports
        .iter()
        .filter(|snapshot| snapshot.caller.as_ref() == Some(&occupied))
        .collect();
    assert_eq!(
        rejected.len(),
        2,
        "before + after-reject of the failing hop: {exports:?}"
    );
    let before = rejected[0];
    let after = rejected[1];
    assert_eq!(before.phase, ts::ExportSnapshotPhase::Before);
    assert_eq!(after.phase, ts::ExportSnapshotPhase::AfterReject);

    let Some(TargetSnapshot::Data(occupant)) = after.conflict_target.clone() else {
        panic!("the occupied destination carries the injected owned value: {after:?}");
    };
    for snapshot in [before, after] {
        assert_eq!(snapshot.observation_error, None);
        assert_eq!(snapshot.child.as_ref(), Some(&source_scope));
        assert_eq!(snapshot.caller.as_ref(), Some(&occupied));
        assert_eq!(snapshot.slots.len(), 2, "{snapshot:?}");
        assert_eq!(snapshot.caller_state, Some(ScopeState::Active));
        assert_eq!(
            snapshot.conflict_target,
            Some(TargetSnapshot::Data(occupant.clone()))
        );
        assert_eq!(snapshot.conflict_owner.as_ref(), Some(&occupied));
        assert!(
            snapshot.conflict_alive,
            "the occupant stays alive: {snapshot:?}"
        );
    }

    // 拒绝点的位置身份 + 整组原子性：除被占位的后项外，没有任何槽在 caller 侧被绑定。
    let caller_refs = after.caller_refs.as_ref().expect("caller refs observed");
    for (index, slot) in after.slots.iter().enumerate() {
        let bound = caller_refs.iter().any(|(position, _)| position == &slot.1);
        if index == 1 {
            assert!(bound, "the occupied late destination is the rejected slot");
        } else {
            assert!(!bound, "the legal slot {index} is not half-exported");
        }
    }
    assert!(
        after
            .caller_owned
            .as_ref()
            .expect("caller owned observed")
            .contains(&occupant)
    );

    // 精确诊断：公开 source 是框架原始 ScopeError，命名"精确目的位置 + 占用它的 Scope"。
    let source = Error::source(&runtime_error).expect("the original Scope diagnostic is preserved");
    assert_eq!(
        source.to_string(),
        ScopeError::RefAlreadyBound {
            scope: occupied.clone(),
            position: after.slots[1].1.clone(),
        }
        .to_string(),
        "the framework text names the exact occupied destination and scope"
    );
    let framework = source
        .downcast_ref::<FrameworkDiagnostic>()
        .expect("the delivery diagnostic is the framework's original Scope diagnostic");
    let rendered = format!("{framework:?}");
    assert!(
        rendered.starts_with("FrameworkDiagnostic { scope_error: RefAlreadyBound {"),
        "{rendered}"
    );
    assert!(
        rendered.contains(&format!("scope: {occupied}")),
        "{rendered}"
    );
    assert!(
        rendered.contains(&format!("position: {}", after.slots[1].1)),
        "{rendered}"
    );

    // 两端完整快照逐字段相等：拒绝不产生任何绑定／责任／取号副作用。
    assert_eq!(before.child, after.child);
    assert_eq!(before.caller, after.caller);
    assert_eq!(before.child_refs, after.child_refs);
    assert_eq!(before.child_owned, after.child_owned);
    assert_eq!(before.caller_refs, after.caller_refs);
    assert_eq!(before.caller_owned, after.caller_owned);
    assert_eq!(before.slots, after.slots);
    assert_eq!(before.next_data_id, after.next_data_id);

    // 两跳时序：第一跳拒绝时没有任何交付记录；第二跳拒绝时第一跳已合法完成并被记录。
    let deliveries = observe::take_controller_deliveries();
    if hop == 1 {
        assert!(
            deliveries.is_empty(),
            "the first hop rejects before any delivery: {deliveries:?}"
        );
    } else {
        assert_eq!(deliveries.len(), 1, "{deliveries:?}");
        assert_eq!(deliveries[0].kind, kind);
        assert_eq!(deliveries[0].hop, "round-to-controller");
        assert_eq!(deliveries[0].from, round);
        assert_eq!(deliveries[0].to, controller);
    }

    // 实际 Root 零 take：失败清理真实发生、Root 关闭、没有任何 AfterCommit 或 take。
    let roots = ts::take_root_snapshots();
    assert!(
        !roots.is_empty(),
        "the Runtime failure cleanup is really observed"
    );
    assert!(
        roots.iter().all(|snapshot| snapshot.planned_takes == 0),
        "{roots:?}"
    );
    assert!(
        !roots
            .iter()
            .any(|snapshot| snapshot.phase == ts::RootSnapshotPhase::AfterCommit),
        "nothing was ever committed"
    );
    let cleaned = roots
        .iter()
        .find(|snapshot| snapshot.phase == ts::RootSnapshotPhase::AfterFailureCleanup)
        .expect("the Root failure cleanup snapshot exists");
    assert_eq!(cleaned.root_state, ScopeState::Closed);
    assert!(
        cleaned.root_owned.is_empty(),
        "the failed execution hands nothing to the caller"
    );
    assert!(ts::take_log_snapshot().is_empty(), "zero take");
    // 下游节点从未被调用：拒绝发生在交付出口，而不是业务继续之后。
    assert_eq!(LATER_CALLS.with(Cell::get), 0);
    ts::assert_no_observation_errors();
    reset_runtime_channels();
}

/// X15-a：Retry 第一跳（round → controller）后项拒绝。
#[test]
fn w06_retry_first_hop_runtime_rejection_zero_take() {
    runtime_export_rejection(false, 1);
}

/// X15-b：Retry 第二跳（controller → caller）后项拒绝。
#[test]
fn w06_retry_second_hop_runtime_rejection_zero_take() {
    runtime_export_rejection(false, 2);
}

/// X15-c：Iter（IterBreak 交付）第一跳后项拒绝。
#[test]
fn w06_iter_first_hop_runtime_rejection_zero_take() {
    runtime_export_rejection(true, 1);
}

/// X15-d：Iter（IterBreak 交付）第二跳后项拒绝。
#[test]
fn w06_iter_second_hop_runtime_rejection_zero_take() {
    runtime_export_rejection(true, 2);
}

// ================================================================ X19：后续不同清理诊断不覆盖首诊断

/// 真实 Root 清理故障的诊断文本（`destroy_probe` 破坏一个 owned entry 的自然结果）。
const NATURAL_ROOT_CLEANUP: &str = "owned entry must exist until its scope closes";
/// 合成的"后续不同清理诊断"文本（`install_later_cleanup_diagnostic`；自然业务不会产生）。
const SYNTHETIC_LATER_CLEANUP: &str =
    "W06 synthetic later cleanup diagnosis (test-only audit path)";

#[derive(Debug)]
struct Requested;

impl std::fmt::Display for Requested {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("retry requested")
    }
}

impl Error for Requested {}

async fn retry_now(_: Query<&u32>) -> Result<(), BodyError> {
    Err(BodyError::retry(Requested))
}

/// 预算终止（Retry 耗尽／Iter 轮次上限）叠加真实 Root 清理故障，再叠加一条**不同**的后续
/// 清理诊断：首诊断、首 Scope、stored-first 与公开两通道都不得被后续诊断改写。
fn distinct_budget_diagnostics(iter: bool) {
    reset_runtime_channels();
    // 真实故障：Root 退出清理破坏一个 owned entry（业务不读取的输入）。
    install_root_cleanup_faults(1);
    // 合成内部前提：后续不同诊断（T04 R2 audit 路径正式化；自然业务不会产生）。
    ts::install_later_cleanup_diagnostic(ScopeError::Invariant {
        violated: SYNTHETIC_LATER_CLEANUP,
    });

    let result = if iter {
        block_on(Runtime::new().execute(
            |flow, (_unused, seed)| flow.iter(seed, 1, |current, round| round.then(plus, current)),
            (0u8, 7u32),
        ))
    } else {
        block_on(Runtime::new().execute(
            |flow, (_unused, seed)| {
                flow.retry(1, |attempt| {
                    attempt.then(retry_now, seed);
                    attempt.then(plus, seed)
                })
            },
            (0u8, 7u32),
        ))
    };

    let error = result.expect_err("budget exhaustion with a cleanup fault is terminal");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("budget exhaustion plus a cleanup failure is Runtime: {error}");
    };
    assert_eq!(
        runtime_error.note(),
        "budget exhaustion ended with a cleanup failure"
    );
    // 公开通道一：Error::source 保留冻结的预算分类（全部字段与动态原因）。
    let frozen = Error::source(runtime_error)
        .expect("the frozen budget classification survives")
        .downcast_ref::<RunError>()
        .expect("source is the frozen RunError");
    if iter {
        assert!(
            matches!(
                frozen,
                RunError::IterationLimitReached {
                    max_iterations: 1,
                    completed_iterations: 1
                }
            ),
            "{frozen:?}"
        );
    } else {
        let RunError::RetryExhausted {
            max_retries,
            attempts,
            last_error,
        } = frozen
        else {
            panic!("expected the frozen RetryExhausted, got {frozen:?}");
        };
        assert_eq!((*max_retries, *attempts), (1, 2));
        assert!(
            Error::source(last_error)
                .expect("the last dynamic reason is reachable")
                .downcast_ref::<Requested>()
                .is_some()
        );
    }

    // 两次清理失败登记在同一真实 Scope 上：首条被保存，后续不同诊断只登记、不覆盖。
    let records = ts::take_cleanup_failures();
    assert_eq!(records.len(), 2, "{records:?}");
    assert!(records[0].stored_first, "{records:?}");
    assert!(
        !records[1].stored_first,
        "the later diagnosis only registers: {records:?}"
    );
    assert_eq!(
        records[0].scope, records[1].scope,
        "both diagnoses belong to one real scope: {records:?}"
    );
    assert_eq!(
        records[0].error.to_string(),
        ScopeError::Invariant {
            violated: NATURAL_ROOT_CLEANUP
        }
        .to_string()
    );
    assert_eq!(
        records[1].error.to_string(),
        ScopeError::Invariant {
            violated: SYNTHETIC_LATER_CLEANUP
        }
        .to_string()
    );
    assert_ne!(
        records[0].error.to_string(),
        records[1].error.to_string(),
        "the later diagnosis is really a different one"
    );

    // 公开通道二：cleanup_source 恰是首条诊断（不是后项、不是分类）。
    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    assert_eq!(cleanup.to_string(), records[0].error.to_string());
    assert_ne!(cleanup.to_string(), records[1].error.to_string());

    // 首 Scope 是真实 RootScope（不是控制器帧或轮次）。
    let root_scope = observe::take_frames()
        .iter()
        .find(|frame| frame.kind == "root" && frame.phase == "enter")
        .map(|frame| frame.scope.clone())
        .expect("the Root frame is observed");
    assert_eq!(records[0].scope, root_scope);
    ts::assert_no_observation_errors();
    reset_runtime_channels();
}

/// X19-a：Retry 预算耗尽 + 清理故障 + 后续不同诊断。
#[test]
fn w06_retry_budget_distinct_later_cleanup_cannot_overwrite_first() {
    distinct_budget_diagnostics(false);
}

/// X19-b：Iter 轮次上限 + 清理故障 + 后续不同诊断。
#[test]
fn w06_iter_budget_distinct_later_cleanup_cannot_overwrite_first() {
    distinct_budget_diagnostics(true);
}
