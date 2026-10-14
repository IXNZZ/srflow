//! WE-T04 定向样本（状态生命周期：E13／E16／E17／E19／E20）。
//!
//! 全部用例只通过公开入口 `Runtime::new().execute(...)` 运行；观察点只用本任务的
//! cfg(test) 通道（控制器／Round 记录、成组收口前后快照、成组 Promote 现场、已关闭
//! Scope、Root 提取快照）与逐实例 Drop 事件，不直接触碰内部结构、不重绑任何 Ref。
//!
//! 每个样本都按真实物理身份锚定：控制器／Round 用 `ScopeId`（含单调序号），状态与
//! 输出用 `ControlStateId`（owner＋seq）、`RefId` 与 `DataId`，回收用 pending 集合与
//! 逐实例 Drop 顺序；不使用"标签总数"替代逐槽证据。

use crate::Data;
use crate::core::engine::error::BodyError;
use crate::core::engine::observe;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, Ref};
use crate::core::scope::TargetSnapshot;
use crate::core::test_support::{
    PromoteGroupStateRecord, RootSnapshotPhase, closed_scope_snapshot, promote_group_states_reset,
    reset_observations, root_snapshots_reset, round_collect_group_reset, take_promote_group_states,
    take_root_snapshots, take_round_collect_group,
};
use futures::executor::block_on;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

// ---------------------------------------------------------------- 业务类型与 Node

/// 携带逐实例 Drop 与产生事件的业务状态值；`value` 决定确定性推进，`tag` 唯一标识实例。
#[derive(Data)]
struct Parcel {
    value: u32,
    tag: &'static str,
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for Parcel {
    fn drop(&mut self) {
        self.log.borrow_mut().push(format!("drop:{}", self.tag));
    }
}

fn parcel(tag: &'static str, value: u32, log: &Rc<RefCell<Vec<String>>>) -> Parcel {
    Parcel {
        value,
        tag,
        log: Rc::clone(log),
    }
}

fn tag_for(value: u32) -> &'static str {
    match value {
        0 => "A",
        1 => "B",
        2 => "C",
        3 => "D",
        4 => "E",
        _ => "F",
    }
}

/// 唯一自产 Node：读取 current，登记"产生"事件并推进一格（value+1，tag 随 value）。
async fn advance(query: Query<&Parcel>) -> Result<Parcel, BodyError> {
    let current = query.get();
    current
        .log
        .borrow_mut()
        .push(format!("produce:{}", current.tag));
    Ok(parcel(
        tag_for(current.value + 1),
        current.value + 1,
        &current.log,
    ))
}

/// 产生共享值：tag 固定为 `S`，读入 current 并推进一格（E19 多槽共享目标）。
async fn produce_shared(query: Query<&Parcel>) -> Result<Parcel, BodyError> {
    let current = query.get();
    current.log.borrow_mut().push(String::from("produce:S"));
    Ok(parcel("S", current.value + 1, &current.log))
}

thread_local! {
    /// 值阈值：current.value ≥ 阈值时发出 IterBreak（每样本设置一次）。
    static BREAK_WHEN: Cell<u32> = const { Cell::new(u32::MAX) };
    /// Judge 调用次数阈值：第 N 次调用发出 IterBreak（E17 same-target 使用）。
    static JUDGE_CALLS: Cell<u32> = const { Cell::new(0) };
}

async fn judge_break_at(query: Query<&Parcel>) -> Result<(), BodyError> {
    if query.get().value >= BREAK_WHEN.with(Cell::get) {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}

async fn judge_break_on_call(query: Query<&Parcel>) -> Result<(), BodyError> {
    let call = JUDGE_CALLS.with(|calls| {
        let next = calls.get() + 1;
        calls.set(next);
        next
    });
    let _ = query.get();
    if call >= 2 {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------- 观察工具

fn reset_all() {
    observe::reset();
    reset_observations();
    round_collect_group_reset();
    promote_group_states_reset();
    take_root_snapshots();
    root_snapshots_reset();
    closed_scope_snapshot();
    BREAK_WHEN.with(|slot| slot.set(u32::MAX));
    JUDGE_CALLS.with(|slot| slot.set(0));
}

/// 从 Root 提取快照里取 `phase` 的记录。
fn root_phase(
    snapshots: &[crate::core::test_support::RootSnapshot],
    phase: RootSnapshotPhase,
) -> &crate::core::test_support::RootSnapshot {
    snapshots
        .iter()
        .find(|snapshot| snapshot.phase == phase)
        .expect("root snapshot phase present")
}

fn data_id(target: &TargetSnapshot) -> DataIdView<'_> {
    match target {
        TargetSnapshot::Data(id) => DataIdView(id),
        TargetSnapshot::CollectionItem { .. } => panic!("state target must be a complete Data"),
    }
}

/// 只读 DataId 包装：便于在断言消息里显示，又不需要复制内部身份。
struct DataIdView<'a>(&'a crate::core::identity::DataId);

impl std::fmt::Debug for DataIdView<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl PartialEq<crate::core::identity::DataId> for DataIdView<'_> {
    fn eq(&self, other: &crate::core::identity::DataId) -> bool {
        self.0 == other
    }
}

impl PartialEq for DataIdView<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl DataIdView<'_> {
    fn id(&self) -> &crate::core::identity::DataId {
        self.0
    }
}

fn promote_of(records: &[PromoteGroupStateRecord], index: usize) -> &PromoteGroupStateRecord {
    records.get(index).expect("promote group record present")
}

// ---------------------------------------------------------------- E13

/// E13：Normal 接受 next；第二轮已产生未接受的 C 后 Break 交付本轮进入时的 current B。
///
/// A(0)→B(1)（第 1 轮 Normal 整组 Promote 接受 B）；第 2 轮先产生 C(2)，随后 Break：
/// C 未 Promote、随 Round 关闭释放；交付进入本轮时的 current B。
#[test]
fn e13_produced_next_is_discarded_and_entry_current_is_delivered() {
    reset_all();
    BREAK_WHEN.with(|slot| slot.set(1));
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute(
        |flow: &mut crate::core::engine::flow::Flow<'_>, state: Ref<Parcel>| {
            flow.iter(state, 5, |current, round| {
                let next = round.then(&advance, current);
                round.then(&judge_break_at, current);
                next
            })
        },
        parcel("A", 0, &log),
    ))
    .expect("break delivers the entry current");
    assert_eq!((delivered.value, delivered.tag), (1, "B"));
    // 逐实例 Drop：C 在第二轮 Round 关闭时释放；B 作为交付值尚未释放。
    let events = log.borrow().clone();
    assert!(
        events.contains(&String::from("drop:C")),
        "C discarded: {events:?}"
    );
    assert!(
        !events.contains(&String::from("drop:B")),
        "B is delivered, not discarded: {events:?}"
    );

    // 帧／Scope：controller 唯一，两轮 Round 的 ScopeId 单调且互不相同，caller 恒为 Root。
    let frames = observe::take_frames();
    let root = frames
        .iter()
        .find(|record| record.kind == "root" && record.phase == "enter")
        .expect("root frame entered")
        .scope
        .clone();
    let rounds = observe::take_controller_rounds();
    assert_eq!(
        rounds.iter().map(|record| record.at).collect::<Vec<_>>(),
        [
            "controller-created",
            "round-created",
            "round-promoted",
            "round-created",
            "round-caught"
        ]
    );
    assert!(rounds.iter().all(|record| record.caller == root));
    let controller = rounds[0].controller.clone();
    assert!(rounds.iter().all(|record| record.controller == controller));
    let round_one = rounds[1].round.clone().expect("round 1 scope");
    let round_two = rounds[3].round.clone().expect("round 2 scope");
    assert_ne!(round_one, round_two);
    assert!(
        round_one.seq() < round_two.seq(),
        "later round gets a larger ScopeId: {round_one:?} < {round_two:?}"
    );
    assert_eq!(
        rounds
            .iter()
            .filter(|record| record.at == "round-promoted")
            .count(),
        1,
        "only the first round promotes"
    );

    // 成组收口 Before（第 1 轮）：state 当时的 target = initial A；来源槽 target = 自产 B。
    let collects = take_round_collect_group();
    assert_eq!(collects.len(), 1, "one promote collect snapshot");
    let before = &collects[0];
    assert_eq!(before.source, round_one);
    let a_id = data_id(before.states[0].target.as_ref().expect("state initialized"));
    assert!(before.states[0].pending.is_empty());
    let b_id = data_id(
        before.selected[0]
            .target
            .as_ref()
            .expect("round output bound"),
    );
    assert_ne!(a_id, b_id, "A and B are distinct DataIds");
    assert_eq!(before.states[0].owner, controller);

    // 成组 Promote 现场：唯一一次提交把 B 的责任交给 controller；A 从未进入 owned／pending。
    let promotes = take_promote_group_states();
    assert_eq!(promotes.len(), 1, "only one promote commit");
    let promoted = promote_of(&promotes, 0);
    assert_eq!(promoted.controller, controller);
    assert_eq!(promoted.source, round_one);
    assert_eq!(promoted.states.len(), 1);
    assert_eq!(data_id(promoted.states[0].2.as_ref().unwrap()), b_id);
    assert!(promoted.states[0].3.is_empty(), "nothing pending for A");
    assert_eq!(promoted.transferred, vec![b_id.id().clone()]);
    assert_eq!(promoted.controller_owned, vec![b_id.id().clone()]);
    assert!(!promoted.controller_owned.contains(a_id.id()));
    assert!(!promoted.states[0].3.contains(a_id.id()));

    // 最终交付后 controller Scope 关闭、状态回收、无残留责任。
    let closed = closed_scope_snapshot();
    assert!(closed.contains(&controller));
    assert!(closed.contains(&round_one));
    assert!(closed.contains(&round_two));
    let snapshots = take_root_snapshots();
    let after_close = root_phase(&snapshots, RootSnapshotPhase::AfterClose);
    assert!(
        after_close.root_owned.is_empty(),
        "controller state recycled"
    );
    let after_commit = root_phase(&snapshots, RootSnapshotPhase::AfterCommit);
    assert_eq!(
        after_commit.planned_takes, 1,
        "exactly one owned output taken"
    );
    reset_all();
}

// ---------------------------------------------------------------- E16

/// E16：imported 初始 A 不被 Loop 销毁；无保留的 Loop-owned 旧值由 pending 及时回收。
///
/// A(0)→B(1)→C(2) 再 Break：第 1／2 轮 Normal 各 Promote 一次；第 2 轮把上一轮
/// Loop-owned 的 B 置为 pending，并在第 3 轮 Node 之前回收（Drop）；交付进入第 3 轮时的
/// current C。imported 的 A 始终归 Root（owner 不变），不进入 controller owned／pending。
#[test]
fn e16_imported_initial_survives_and_loop_owned_old_value_is_recycled() {
    reset_all();
    BREAK_WHEN.with(|slot| slot.set(2));
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute(
        |flow: &mut crate::core::engine::flow::Flow<'_>, state: Ref<Parcel>| {
            flow.iter(state, 6, |current, round| {
                let next = round.then(&advance, current);
                round.then(&judge_break_at, current);
                next
            })
        },
        parcel("A", 0, &log),
    ))
    .expect("break delivers the entry current");
    assert_eq!((delivered.value, delivered.tag), (2, "C"));

    // 事件顺序：produce:A, produce:B, drop:B, produce:C, drop:D；B 的回收早于第 3 轮 Node。
    let events = log.borrow().clone();
    let position = |needle: &str| events.iter().position(|event| event == needle);
    let drop_b = position("drop:B").expect("old loop state B recycled");
    let produce_c = position("produce:C").expect("round 3 runs its node");
    assert!(
        drop_b < produce_c,
        "B is recycled before round 3 node: {events:?}"
    );
    assert!(
        position("drop:D").is_some(),
        "unaccepted D discarded: {events:?}"
    );
    assert!(position("drop:C").is_none(), "C is delivered: {events:?}");

    // 成组收口：两轮 Promote；第 1 轮旧 target 是 imported A，第 2 轮旧 target 是 loop-owned B。
    let collects = take_round_collect_group();
    assert_eq!(collects.len(), 2, "two normal promote collects");
    let a_id = data_id(collects[0].states[0].target.as_ref().unwrap())
        .id()
        .clone();
    let b_id = data_id(collects[1].states[0].target.as_ref().unwrap())
        .id()
        .clone();
    assert!(collects[0].states[0].pending.is_empty());
    assert!(
        collects[1].states[0].pending.is_empty(),
        "pending is settled before prepare"
    );

    let promotes = take_promote_group_states();
    assert_eq!(promotes.len(), 2);
    // 第 1 轮：A → B，A（imported）不产生 pending／转移。
    assert_eq!(data_id(promotes[0].states[0].2.as_ref().unwrap()), b_id);
    assert!(
        promotes[0].states[0].3.is_empty(),
        "imported A is not pending"
    );
    assert_eq!(promotes[0].transferred, vec![b_id.clone()]);
    assert!(!promotes[0].controller_owned.contains(&a_id));
    // 第 2 轮：B → C，B 成为 pending（待回收）；提交现场 B 仍在 owned，回收发生在提交之后。
    let c_id = data_id(promotes[1].states[0].2.as_ref().unwrap())
        .id()
        .clone();
    assert_eq!(promotes[1].states[0].3, vec![b_id.clone()], "B is pending");
    assert!(promotes[1].controller_owned.contains(&b_id));
    assert!(promotes[1].controller_owned.contains(&c_id));
    assert!(!promotes[1].states[0].3.contains(&a_id));
    assert!(!promotes[1].controller_owned.contains(&a_id));

    // imported A 的 owner 保持 Root：Root 提取前 owned 集合仍含 A，且不含 B／C。
    let snapshots = take_root_snapshots();
    let after_commit = root_phase(&snapshots, RootSnapshotPhase::AfterCommit);
    assert!(
        after_commit.root_owned.contains(&a_id),
        "A stays Root-owned"
    );
    assert!(!after_commit.root_owned.contains(&b_id));
    assert!(!after_commit.root_owned.contains(&c_id));
    reset_all();
}

// ---------------------------------------------------------------- E17

/// E17：same-target next 合法（先一轮 Normal Promote 再 Break），不复制、不销毁、不增 owner。
///
/// 每轮返回 current 本身作为 next：第 1 轮 Normal 整组 Promote 接受"同一 target"，
/// 第 2 轮 Break 交付 current。DataId／state target 不变、pending 为空、无新分配、
/// parent initial Ref 未重绑。
#[test]
fn e17_same_target_next_survives_a_normal_promote_without_copying_or_reowning() {
    reset_all();
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute(
        |flow: &mut crate::core::engine::flow::Flow<'_>, state: Ref<Parcel>| {
            flow.iter(state, 5, |current, round| {
                round.then(&judge_break_on_call, current);
                current
            })
        },
        parcel("A", 0, &log),
    ))
    .expect("break delivers the unchanged current");
    assert_eq!((delivered.value, delivered.tag), (0, "A"));
    assert!(
        log.borrow().is_empty(),
        "no produce and no drop: {:?}",
        log.borrow()
    );

    let collects = take_round_collect_group();
    assert_eq!(collects.len(), 1, "one normal promote");
    let a_id = data_id(collects[0].states[0].target.as_ref().unwrap())
        .id()
        .clone();
    let before_next_data_id = collects[0].next_data_id;
    assert!(collects[0].states[0].alive);

    let promotes = take_promote_group_states();
    assert_eq!(promotes.len(), 1);
    assert_eq!(data_id(promotes[0].states[0].2.as_ref().unwrap()), a_id);
    assert!(promotes[0].states[0].3.is_empty(), "no pending old value");
    assert!(promotes[0].transferred.is_empty(), "no new owner");
    assert!(promotes[0].controller_owned.is_empty(), "no owned state");
    assert_eq!(promotes[0].controller_owned.len(), 0);

    // 分配未推进：同一 DataId，没有复制。
    let snapshots = take_root_snapshots();
    let after_commit = root_phase(&snapshots, RootSnapshotPhase::AfterCommit);
    assert_eq!(
        after_commit.next_data_id, before_next_data_id,
        "no allocation on same-target"
    );
    // parent initial Ref 未重绑：Root 输入位置仍指向同一 target。
    assert!(
        after_commit
            .root_refs
            .iter()
            .any(|(_, target)| data_id(target) == a_id),
        "initial Ref still targets A: {:?}",
        after_commit.root_refs
    );
    reset_all();
}

// ---------------------------------------------------------------- E19

/// E19：多槽共享目标保持独立 state／输出身份与唯一责任。
///
/// 两槽 initial 为不同 Root 输入；第 1 轮两槽 next 指向同一自产 target S（重复 next
/// target）。两个 ControlState 的 owner＋seq 各自独立；责任只按 DataId 转移一次；
/// 最终只选择 slot 0 作为合法 Root 输出（整组重复 owned tuple 的 Root 拒绝见 E28）。
#[test]
fn e19_shared_target_keeps_independent_states_and_single_responsibility() {
    reset_all();
    BREAK_WHEN.with(|slot| slot.set(1));
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute(
        |flow: &mut crate::core::engine::flow::Flow<'_>, refs: (Ref<Parcel>, Ref<Parcel>)| {
            flow.iter(refs, 5, |(left, _right), round| {
                let shared = round.then(&produce_shared, left);
                round.then(&judge_break_at, left);
                (shared, shared)
            })
            .0
        },
        (parcel("A0", 0, &log), parcel("A1", 0, &log)),
    ))
    .expect("break delivers the selected shared slot");
    assert_eq!((delivered.value, delivered.tag), (1, "S"));

    let collects = take_round_collect_group();
    assert_eq!(collects.len(), 1);
    let before = &collects[0];
    let state_seq = [before.states[0].seq, before.states[1].seq];
    assert_ne!(
        state_seq[0], state_seq[1],
        "states keep independent identities"
    );
    assert!(
        before
            .states
            .iter()
            .all(|state| state.owner == before.states[0].owner)
    );

    let promotes = take_promote_group_states();
    assert_eq!(promotes.len(), 1);
    let promoted = promote_of(&promotes, 0);
    assert_eq!(promoted.states.len(), 2);
    let shared_id = data_id(promoted.states[0].2.as_ref().unwrap()).id().clone();
    assert_eq!(
        data_id(promoted.states[1].2.as_ref().unwrap()),
        shared_id,
        "both states target the same DataId"
    );
    // 唯一责任：同一实例只转移一次。
    assert_eq!(promoted.transferred, vec![shared_id.clone()]);
    assert_eq!(promoted.controller_owned, vec![shared_id.clone()]);
    assert!(
        promoted.states.iter().all(|state| state.3.is_empty()),
        "imported initials are not pending"
    );
    reset_all();
}

// ---------------------------------------------------------------- E20

/// E20：跨槽交换、一槽仍保留旧 DataId、共享旧值不会提前回收。
///
/// 三槽 (x,y,z)=(A,B,C)；一轮 Normal 把 next 设为 (y,x,z)：slot 0／1 互换（跨槽），
/// slot 2 保留旧 DataId。交换后 A／B 仍分别被另一槽引用，不得被当作无主旧值回收；
/// 交付 (B,A,C) 三槽完整。
#[test]
fn e20_cross_slot_swap_and_retention_never_recycle_shared_values() {
    reset_all();
    BREAK_WHEN.with(|slot| slot.set(1));
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute(
        |flow: &mut crate::core::engine::flow::Flow<'_>,
         refs: (Ref<Parcel>, Ref<Parcel>, Ref<Parcel>)| {
            flow.iter(refs, 5, |(x, y, z), round| {
                round.then(&judge_break_at, x);
                (y, x, z)
            })
        },
        (
            parcel("A", 0, &log),
            parcel("B", 1, &log),
            parcel("C", 2, &log),
        ),
    ))
    .expect("break delivers the swapped state");
    assert_eq!(
        (delivered.0.tag, delivered.1.tag, delivered.2.tag),
        ("B", "A", "C")
    );
    assert!(
        log.borrow().is_empty(),
        "no shared old value is prematurely dropped: {:?}",
        log.borrow()
    );

    let collects = take_round_collect_group();
    assert_eq!(collects.len(), 1);
    let before = &collects[0];
    let a_id = data_id(before.states[0].target.as_ref().unwrap())
        .id()
        .clone();
    let b_id = data_id(before.states[1].target.as_ref().unwrap())
        .id()
        .clone();
    let c_id = data_id(before.states[2].target.as_ref().unwrap())
        .id()
        .clone();
    assert!(before.states.iter().all(|state| state.alive));
    assert!(before.states.iter().all(|state| state.pending.is_empty()));

    let promotes = take_promote_group_states();
    assert_eq!(promotes.len(), 1);
    let promoted = promote_of(&promotes, 0);
    // slot 0 取 y（B），slot 1 取 x（A）：跨槽交换；slot 2 仍为 z（C）。
    assert_eq!(data_id(promoted.states[0].2.as_ref().unwrap()), b_id);
    assert_eq!(data_id(promoted.states[1].2.as_ref().unwrap()), a_id);
    assert_eq!(data_id(promoted.states[2].2.as_ref().unwrap()), c_id);
    // 交换后 A／B 各自仍被另一槽引用，均不得成为 pending。
    assert!(promoted.states.iter().all(|state| state.3.is_empty()));
    assert!(
        promoted.controller_owned.is_empty(),
        "imported values are not re-owned"
    );
    assert!(promoted.transferred.is_empty());
    reset_all();
}
