//! WE-T05 R01 修复回归：**item-backed 嵌套集合**——外层 Each 的 item（`Ref<Vec<T>>`）作为
//! 内层 Each 的集合（SPEC §8 接受任意 `Ref<Vec<T>>` 来源）。
//!
//! 公开用法与独立复审见证 `docs/workflow_engine_tasks/WE_T05_REVIEW/witnesses/nested_collection_each.rs`
//! 相同：只经公开入口 `Runtime::new().execute(...)` 组合 Flow／Query／Data，元素无
//! Clone／Copy／Debug。Core 级 cap／责任链样本另用真实协调组件入口（与既有 v21-08 目标
//! 元数据样本同法），只读观察与窄故障注入不改判断路径。
//!
//! 覆盖复审 §2.4 的最低闭环要求：
//! - 非空与空内层集合**对照**；元素非 Clone／Copy；内层只产生新 owned 输出；
//! - 外层原集合在内层 Each 之后仍可读取；
//! - 多级嵌套（两层以上 item-as-collection）与经合法子 Body（chain）后 cap 仍保持；
//! - Core 侧真实根数据／类型化访问关系／cap 链的元数据与拒绝判据；
//! - Control／Failure／取消下不留下悬空引用，各 owned 实例恰清理一次；
//! - item 本身仍不可 Consume、不取得 owned 责任。

use super::error::{BodyError, ControlSignal, RunError};
use super::runtime::Runtime;
use super::shape::{Query, Ref};
use crate::Data;
use crate::core::identity::{DataId, ExecutionIdentity, ScopeId};
use crate::core::internal_error::ScopeError;
use crate::core::ref_id::{RefId, RefIdAllocator, RefIdSource};
use crate::core::scope::{
    CollectionRoot, ExportSlot, ImportSlot, ItemAccess, RefTarget, ScopeCoordinator, ScopeState,
    TargetSnapshot,
};
use crate::core::test_support;
use futures::executor::block_on;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

// ---------------------------------------------------------------- 观察与复位

thread_local! {
    /// 内层 item Body 的业务调用次数（线程局部隔离平行用例）。
    static BODY_CALLS: Cell<usize> = const { Cell::new(0) };
    /// Core 样本的逐实例析构日志。
    static CORE_DROPS: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    /// 公开入口样本的逐实例析构日志句柄（由输入构造时安装）。
    static DROP_SINK: RefCell<Option<Rc<RefCell<Vec<String>>>>> = const { RefCell::new(None) };
}

fn reset_observations() {
    BODY_CALLS.with(|calls| calls.set(0));
    DROP_SINK.with(|sink| *sink.borrow_mut() = None);
    test_support::reset_observations();
}

fn body_calls() -> usize {
    BODY_CALLS.with(Cell::get)
}

// ---------------------------------------------------------------- 业务类型

/// 集合元素：刻意**不实现 Clone／Copy／Debug**——item 借用必须是原元素受限借用。
#[derive(Data)]
struct Item(u32);

/// item Body 新产生的 owned 输出（内层 Each 收集它，外层收集其 `Vec`）。
#[derive(Data, Debug, PartialEq, Eq)]
struct Total(u32);

/// 带析构日志的集合元素（Control／Failure／取消的逐实例清理证据）。
#[derive(Data)]
struct TrackItem {
    id: u32,
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for TrackItem {
    fn drop(&mut self) {
        self.log.borrow_mut().push(format!("drop:item:{}", self.id));
    }
}

/// 只读共享的析构日志（pending 样本的节点输入）。
#[derive(Data)]
struct SharedLog {
    log: Rc<RefCell<Vec<String>>>,
}

/// 内层 item Body 产生的 owned 输出（带析构事件，用于取消样本的部分成果证据）。
#[derive(Data)]
struct TrackedTotal {
    value: u32,
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for TrackedTotal {
    fn drop(&mut self) {
        self.log
            .borrow_mut()
            .push(format!("drop:total:{}", self.value));
    }
}

/// 内层 item Body 的 pending 节点：第一次 poll 起持续挂起（真 Pending，不模拟 Ready）。
struct Pending {
    log: Rc<RefCell<Vec<String>>>,
    polled: bool,
}

impl Future for Pending {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        if !self.polled {
            self.polled = true;
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.log
            .borrow_mut()
            .push(String::from("drop:pending-node"));
    }
}

// ---------------------------------------------------------------- 业务节点

/// 每个 item 产生一个新的 owned `Total`（原元素只被读取）。
async fn increment(query: Query<&Item>) -> Result<Total, BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(Total(query.get().0 + 1))
}

/// 在内层 Each 之后读取**原外层集合**：证明集合未被移动／复制，仍可读取。
async fn count_items(query: Query<&Vec<Vec<Item>>>) -> Result<u64, BodyError> {
    Ok(query.get().iter().map(|group| group.len() as u64).sum())
}

/// 内层 item Body 首次进入即发出 IterBreak（Control 退出样本）。
async fn break_inside_nested(query: Query<&TrackItem>) -> Result<Total, BodyError> {
    let _ = query.get();
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Err(BodyError::iter_break())
}

/// 不可恢复业务失败（Failure 样本）。
#[derive(Debug)]
struct Rejected(&'static str);

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rejected: {}", self.0)
    }
}

impl std::error::Error for Rejected {}

/// 内层 item Body 首次进入即失败（Failure 退出样本）。
async fn fail_inside_nested(query: Query<&TrackItem>) -> Result<Total, BodyError> {
    let _ = query.get();
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    Err(BodyError::fail(Rejected("nested item refused")))
}

/// 内层 item Body 产生当前产物；最后一格（id 3）进入真 Pending（取消样本）。
async fn produce_then_pend(
    query: Query<(&TrackItem, &SharedLog)>,
) -> Result<TrackedTotal, BodyError> {
    let (item, shared) = query.get();
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    let product = TrackedTotal {
        value: item.id + 1,
        log: Rc::clone(&shared.log),
    };
    if item.id == 3 {
        Pending {
            log: Rc::clone(&shared.log),
            polled: false,
        }
        .await;
    }
    Ok(product)
}

// ---------------------------------------------------------------- 值投影

fn values(collected: &[Vec<Total>]) -> Vec<Vec<u32>> {
    collected
        .iter()
        .map(|group| group.iter().map(|total| total.0).collect())
        .collect()
}

// ---------------------------------------------------------------- 公开入口样本

/// R01（见证形态）：外层 Each 的 item 作为内层 Each 的集合，非空输入必须执行 item Body 并
/// 逐槽聚合，空的内层集合必须成功且零调用——两者对照，且元素无 Clone／Copy／Debug。
#[test]
fn r01_witness_shape_executes_non_empty_and_stays_empty_for_empty_inputs() {
    reset_observations();
    let runtime = Runtime::new();
    let non_empty = block_on(runtime.execute::<_, Vec<Vec<Total>>, _>(
        |flow, groups: Ref<Vec<Vec<Item>>>| {
            let out_refs = flow.each(groups, |group, outer| {
                outer.each(group, |item, inner| inner.then_with_output(increment, item))
            });
            flow.output_with(out_refs);
        },
        vec![vec![Item(1), Item(2)], vec![Item(3)]],
    ))
    .expect("a nested Each over a Ref<Vec<T>> item must execute");
    assert_eq!(
        values(&non_empty),
        vec![vec![2u32, 3], vec![4]],
        "witness [[2,3],[4]]"
    );
    assert_eq!(body_calls(), 3, "one item Body call per inner element");

    reset_observations();
    let empty = block_on(runtime.execute::<_, Vec<Vec<Total>>, _>(
        |flow, groups: Ref<Vec<Vec<Item>>>| {
            let out_refs = flow.each(groups, |group, outer| {
                outer.each(group, |item, inner| inner.then_with_output(increment, item))
            });
            flow.output_with(out_refs);
        },
        vec![vec![], vec![]],
    ))
    .expect("empty item-backed collections succeed");
    assert_eq!(empty, vec![Vec::<Total>::new(), Vec::new()]);
    assert_eq!(body_calls(), 0, "an empty inner collection runs no body");
}

/// R01：内层 Each 完成后，**原外层集合**仍可读取（没有被 item 借用取走或复制），且内层
/// 只产生新的 owned 输出。
#[test]
fn r01_the_outer_collection_stays_readable_after_the_nested_each() {
    reset_observations();
    let (nested, total) = block_on(Runtime::new().execute::<_, (Vec<Vec<Total>>, u64), _>(
        |flow, groups: Ref<Vec<Vec<Item>>>| {
            let collected = flow.each(groups, |group, outer| {
                outer.each(group, |item, inner| inner.then_with_output(increment, item))
            });
            // 外层 Each 之后仍读取同一外层集合位置：来源没有被消费或搬走。
            let total = flow.then_with_output(&count_items, groups);
            let out_refs = (collected, total);
            flow.output_with(out_refs);
        },
        vec![vec![Item(1), Item(2)], vec![Item(3)]],
    ))
    .expect("the outer collection stays readable after the nested Each");
    assert_eq!(values(&nested), vec![vec![2u32, 3], vec![4]]);
    assert_eq!(total, 3, "all three original elements are still readable");
    assert_eq!(body_calls(), 3);
}

/// R01：三阶嵌套（外层集合 → item 作为集合 → item 再作为集合）逐层执行且聚合正确，
/// 即至少两层 item-as-collection 的 cap 链保持。
#[test]
fn r01_three_levels_of_item_backed_collections_keep_values_and_caps() {
    reset_observations();
    let collected = block_on(Runtime::new().execute::<_, Vec<Vec<Vec<Total>>>, _>(
        |flow, cube: Ref<Vec<Vec<Vec<Item>>>>| {
            let out_refs = flow.each(cube, |plane, level1| {
                level1.each(plane, |row, level2| {
                    level2.each(row, |cell, level3| level3.then_with_output(increment, cell))
                })
            });
            flow.output_with(out_refs);
        },
        vec![
            vec![vec![Item(1), Item(2)], vec![Item(3)]],
            vec![vec![Item(4)]],
        ],
    ))
    .expect("three levels of item-backed collections execute");
    let levels: Vec<Vec<Vec<u32>>> = collected
        .iter()
        .map(|plane| {
            plane
                .iter()
                .map(|row| row.iter().map(|total| total.0).collect())
                .collect()
        })
        .collect();
    assert_eq!(levels, vec![vec![vec![2u32, 3], vec![4]], vec![vec![5]]]);
    assert_eq!(body_calls(), 4, "one body call per innermost element");
}

/// R01：经**合法子 Body**（chain 子 Body 深捕获外层 item 位置）使用 item-backed 集合，
/// cap 与来源权限保持，值仍然正确。
#[test]
fn r01_a_chain_child_body_can_use_the_item_backed_collection() {
    reset_observations();
    let collected = block_on(Runtime::new().execute::<_, Vec<Vec<Total>>, _>(
        |flow, groups: Ref<Vec<Vec<Item>>>| {
            let out_refs = flow.each(groups, |group, outer| {
                outer.chain(move |sub| {
                    sub.each(group, |item, inner| inner.then_with_output(increment, item))
                })
            });
            flow.output_with(out_refs);
        },
        vec![vec![Item(1), Item(2)], vec![Item(3)]],
    ))
    .expect("a chain child body may use the outer item as its collection");
    assert_eq!(values(&collected), vec![vec![2u32, 3], vec![4]]);
    assert_eq!(body_calls(), 3);
}

// ---------------------------------------------------------------- Core：真实根／cap 链

/// 以真实协调组件入口驱动的夹具：不使用 Context frame，因此可直接观测 `TargetSnapshot`。
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

    fn targets(&self, scope: &ScopeId) -> (Vec<(RefId, TargetSnapshot)>, Vec<DataId>) {
        self.coordinator.snapshot_targets_probe(scope).unwrap()
    }

    fn target(&self, scope: &ScopeId, position: &RefId) -> TargetSnapshot {
        self.targets(scope)
            .0
            .into_iter()
            .find(|(bound, _)| bound == position)
            .map(|(_, target)| target)
            .expect("the position is bound")
    }
}

fn core_drop_log() -> Rc<RefCell<Vec<String>>> {
    CORE_DROPS.with(Rc::clone)
}

fn core_drop_events() -> Vec<String> {
    CORE_DROPS.with(|log| log.borrow().clone())
}

fn count_core_drops(needle: &str) -> usize {
    core_drop_events()
        .iter()
        .filter(|event| event.as_str() == needle)
        .count()
}

/// 两层 item-backed 集合的真实结构：Root 拥有 `Vec<Vec<TrackItem>>` → 外层 Each 导入 →
/// 外层 item（index 0）→ 内层 Each 导入该 item 位置 → 内层 item（index 1）。
struct NestedStack {
    outer_item: ScopeId,
    outer_item_position: RefId,
    inner_each: ScopeId,
    inner_item: ScopeId,
    inner_item_position: RefId,
    collection: DataId,
    /// Root 的集合位置（供整树关闭证据）。
    collection_position: RefId,
}

fn nested_stack(f: &mut Fixture) -> NestedStack {
    let root = f.root();
    let collection_position = f.ref_id();
    let collection = f
        .coordinator
        .register_owned(
            &root,
            &collection_position,
            vec![
                vec![
                    TrackItem {
                        id: 1,
                        log: core_drop_log(),
                    },
                    TrackItem {
                        id: 2,
                        log: core_drop_log(),
                    },
                ],
                vec![TrackItem {
                    id: 3,
                    log: core_drop_log(),
                }],
            ],
        )
        .unwrap();

    let outer_each = f.child(&root);
    let outer_each_collection = f.ref_id();
    f.coordinator
        .import_batch(
            &outer_each,
            &root,
            &[ImportSlot::new::<Vec<Vec<TrackItem>>>(
                &collection_position,
                &outer_each_collection,
            )],
        )
        .unwrap();
    let outer_item = f.child(&outer_each);
    let outer_item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<Vec<TrackItem>>(
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
            &[ImportSlot::new::<Vec<TrackItem>>(
                &outer_item_position,
                &inner_each_collection,
            )],
        )
        .unwrap();
    let inner_item = f.child(&inner_each);
    let inner_item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<TrackItem>(
            &inner_item,
            &inner_each,
            &inner_each_collection,
            &inner_item_position,
            1,
        )
        .expect("a legal item-backed collection binds its element");

    NestedStack {
        outer_item,
        outer_item_position,
        inner_each,
        inner_item,
        inner_item_position,
        collection,
        collection_position,
    }
}

/// 复现一个**嵌套** item 目标：外层 item 的第 0 个元素作为内层集合、内层元素下标 1，
/// 两层 cap 分别是外层／内层 ItemScope。只用于构造 cap 反例，不改判断路径。
fn nested_target(stack: &NestedStack) -> RefTarget {
    RefTarget::CollectionItem {
        collection: CollectionRoot::Item(Box::new(RefTarget::CollectionItem {
            collection: CollectionRoot::Data(stack.collection.clone()),
            index: 0,
            lifetime_cap: stack.outer_item.clone(),
            access: ItemAccess::for_collection::<Vec<TrackItem>>(),
        })),
        index: 1,
        lifetime_cap: stack.inner_item.clone(),
        access: ItemAccess::for_collection::<TrackItem>(),
    }
}

/// R01（Core）：嵌套 item 目标记录**真实根数据**与类型化访问关系，cap 链在读取、别名
/// 与跨 cap 转移三处都保持原责任链，关闭后旧目标不可复活，整树关闭后逐实例析构一次。
#[test]
fn r01_core_nested_target_records_the_real_root_and_enforces_the_cap_chain() {
    let _ = core_drop_log();
    core_drop_events();
    CORE_DROPS.with(|log| log.borrow_mut().clear());
    let mut f = Fixture::new();
    let stack = nested_stack(&mut f);

    // (1) cap 内：真实借用内层元素（root[0][1]），值来自原集合元素。
    let borrowed = f
        .coordinator
        .resolve::<TrackItem>(&stack.inner_item, &stack.inner_item_position)
        .expect("the nested item borrow is readable inside its cap");
    assert_eq!(borrowed.id, 2, "root[0][1] 的真实元素");

    // (2) 记录的元数据：真实根数据（不是内层 Vec 的伪造身份）、内层下标、内层 cap、两层类型。
    match f.target(&stack.inner_item, &stack.inner_item_position) {
        TargetSnapshot::CollectionItem {
            collection,
            index,
            lifetime_cap,
            collection_type,
            element_type,
        } => {
            assert_eq!(collection, stack.collection, "记录真实根数据");
            assert_eq!(index, 1, "内层元素下标");
            assert_eq!(lifetime_cap, stack.inner_item, "cap 是内层 ItemScope");
            assert_eq!(collection_type, std::any::TypeId::of::<Vec<TrackItem>>());
            assert_eq!(element_type, std::any::TypeId::of::<TrackItem>());
        }
        other => panic!("expected a nested item target, got {other:?}"),
    }
    // 外层 item 自身的 cap 仍是外层 ItemScope（链上每跳 cap 都保留）。
    match f.target(&stack.outer_item, &stack.outer_item_position) {
        TargetSnapshot::CollectionItem {
            collection,
            index,
            lifetime_cap,
            ..
        } => {
            assert_eq!(collection, stack.collection, "外层 item 的集合是同一真实根");
            assert_eq!(index, 0);
            assert_eq!(lifetime_cap, stack.outer_item, "外层 cap 是外层 ItemScope");
        }
        other => panic!("expected the outer item target, got {other:?}"),
    }

    // (3) 内层 item 的后代仍在 cap 内：别名合法且读到同一元素。
    let descendant = f.child(&stack.inner_item);
    let descendant_slot = f.ref_id();
    f.coordinator
        .import_batch(
            &descendant,
            &stack.inner_item,
            &[ImportSlot::new::<TrackItem>(
                &stack.inner_item_position,
                &descendant_slot,
            )],
        )
        .expect("a descendant inside the nested cap may alias the item");
    assert_eq!(
        f.coordinator
            .resolve::<TrackItem>(&descendant, &descendant_slot)
            .expect("the alias stays readable inside the cap")
            .id,
        2
    );

    // (4) cap 外：内层 Each 的兄弟 Scope 指向同一嵌套目标被拒绝（判据携带真实 cap／requester）。
    let sibling = f.child(&stack.inner_each);
    let escaped = f.ref_id();
    f.coordinator
        .inject_target_probe(&sibling, &escaped, nested_target(&stack));
    let error = f
        .coordinator
        .resolve::<TrackItem>(&sibling, &escaped)
        .map(|_| ())
        .expect_err("a sibling is outside the nested item cap");
    assert!(
        matches!(&error, ScopeError::ItemOutsideCap { cap, requester, .. }
            if cap == &stack.inner_item && requester == &sibling),
        "got {error:?}"
    );

    // (5) 责任链保持：链上没有任何新 owner（只有 Root 拥有那一个真实集合）。
    for scope in [
        &stack.outer_item,
        &stack.inner_each,
        &stack.inner_item,
        &descendant,
        &sibling,
    ] {
        assert_eq!(
            f.coordinator.owned_len_probe(scope).unwrap(),
            0,
            "a nested item borrow never registers owned data"
        );
    }
    assert_eq!(f.coordinator.owned_len_probe(&f.root()).unwrap(), 1);

    // (6) cap 外目的：内层 item 从其自身 Scope 导给 caller（内层 Each）在真实 Export 的
    // 目的端被拒绝（cap 是实际 ItemScope，destination 是实际 caller）。
    f.coordinator.abort(&descendant).unwrap();
    f.coordinator.abort(&sibling).unwrap();
    let exported = f.ref_id();
    let mut slots = vec![ExportSlot::new::<TrackItem>(
        &stack.inner_item_position,
        &exported,
    )];
    let error = f
        .coordinator
        .finalize(
            &stack.inner_item,
            std::slice::from_ref(&stack.inner_item_position),
            &mut slots,
        )
        .map(|_| ())
        .expect_err("the nested item may not escape its cap");
    assert!(
        matches!(&error, ScopeError::ItemCapEscape { cap, destination, .. }
            if cap == &stack.inner_item && destination == &stack.inner_each),
        "got {error:?}"
    );

    // (7) 关闭：旧目标立即不可读，且真实根集合本身不动（不带悬空引用）。
    assert_eq!(
        f.coordinator.state(&stack.inner_item).unwrap(),
        ScopeState::Closed
    );
    let error = f
        .coordinator
        .resolve::<TrackItem>(&stack.inner_item, &stack.inner_item_position)
        .map(|_| ())
        .expect_err("a closed nested item cannot be revived");
    assert!(
        matches!(
            error,
            ScopeError::ScopeClosed { .. } | ScopeError::ItemOutsideCap { .. }
        ),
        "got {error:?}"
    );
    assert!(
        f.coordinator.alive_probe(&stack.collection),
        "the real root collection is untouched while its owners are alive"
    );

    // (8) 整树关闭后真实集合逐实例析构恰好一次，且旧位置不再有绑定。
    let root = f.root();
    f.coordinator.abort(&root).unwrap();
    assert!(
        !f.coordinator.alive_probe(&stack.collection),
        "the destroyed real root collection is gone"
    );
    assert!(
        f.targets(&root)
            .0
            .iter()
            .all(|(position, _)| position != &stack.collection_position),
        "a closed scope revokes its references"
    );
    for id in 1..=3 {
        assert_eq!(
            count_core_drops(&format!("drop:item:{id}")),
            1,
            "{:?}",
            core_drop_events()
        );
    }
    assert_eq!(
        core_drop_events().len(),
        3,
        "no duplicated or missing instance"
    );
}

/// R01（Core）：嵌套 item 仍不是完整 owned Data——Consume 在任何取值之前拒绝，且不产生
/// 责任转移；这保持复审“不得放宽 ownership”的要求。
#[test]
fn r01_core_nested_item_is_never_consumable_and_owns_nothing() {
    CORE_DROPS.with(|log| log.borrow_mut().clear());
    let mut f = Fixture::new();
    let stack = nested_stack(&mut f);
    let collector = f
        .coordinator
        .begin_collector::<Total>(&stack.inner_each)
        .unwrap();

    let error = f
        .coordinator
        .consume_item(&stack.inner_item, &stack.inner_item_position, &collector)
        .expect_err("a nested item borrow is not a complete owned Data");
    assert!(
        matches!(error, ScopeError::NonCompleteTarget { .. }),
        "got {error:?}"
    );
    assert_eq!(
        f.coordinator.collector_moves(&collector).unwrap(),
        0,
        "no physical move happens"
    );
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
    assert_eq!(f.coordinator.owned_len_probe(&stack.inner_item).unwrap(), 0);

    let root = f.root();
    f.coordinator.abort(&root).unwrap();
    assert_eq!(core_drop_events().len(), 3);
}

// ---------------------------------------------------------------- Control／Failure／取消

/// 嵌套 item-backed 集合的最小公开形态（带析构日志的元素 + 共享日志句柄）。
fn tracked_input() -> (Vec<Vec<TrackItem>>, Rc<RefCell<Vec<String>>>) {
    let log = Rc::new(RefCell::new(Vec::new()));
    DROP_SINK.with(|sink| *sink.borrow_mut() = Some(Rc::clone(&log)));
    let input = vec![
        vec![
            TrackItem {
                id: 1,
                log: Rc::clone(&log),
            },
            TrackItem {
                id: 2,
                log: Rc::clone(&log),
            },
        ],
        vec![TrackItem {
            id: 3,
            log: Rc::clone(&log),
        }],
    ];
    (input, log)
}

fn tracked_events() -> Vec<String> {
    DROP_SINK.with(|sink| {
        sink.borrow()
            .as_ref()
            .expect("the tracked input installs its log")
            .borrow()
            .clone()
    })
}

fn assert_each_instance_drops_once(expected: &[&str]) {
    let events = tracked_events();
    for needle in expected {
        assert_eq!(
            events
                .iter()
                .filter(|event| event.as_str() == *needle)
                .count(),
            1,
            "{needle} drops exactly once: {events:?}"
        );
    }
    assert_eq!(
        events.len(),
        expected.len(),
        "no extra instance: {events:?}"
    );
}

/// 创建过的 Scope 必须全部关闭（取消／Control／Failure 后不留 detached child）。
fn assert_no_detached_scope() {
    let closed = test_support::closed_scope_snapshot();
    let created: Vec<ScopeId> = test_support::boundary_creation_snapshot()
        .into_iter()
        .map(|(child, _, _)| child)
        .collect();
    assert!(!created.is_empty(), "the run creates real scopes");
    assert!(
        created.iter().all(|scope| closed.contains(scope)),
        "every created scope is closed: {closed:?}"
    );
}

/// R01：内层 item Body 发出未处理 Control（IterBreak）时，Item→Each 逐层退出、后续元素
/// 不再执行，root 输入逐实例析构恰好一次，且不留下悬空引用。
#[test]
fn r01_control_inside_the_nested_body_drops_each_instance_once() {
    reset_observations();
    let outcome = block_on(Runtime::new().execute::<_, Vec<Vec<Total>>, _>(
        |flow, groups: Ref<Vec<Vec<TrackItem>>>| {
            let out_refs = flow.each(groups, |group, outer| {
                outer.each(group, |item, inner| {
                    inner.then_with_output(break_inside_nested, item)
                })
            });
            flow.output_with(out_refs);
        },
        tracked_input().0,
    ));
    match outcome {
        Err(RunError::UnhandledControl(ControlSignal::IterBreak(_))) => {}
        other => panic!("expected an unhandled IterBreak, got {other:?}"),
    }
    assert_eq!(body_calls(), 1, "control stops the remaining elements");
    assert_each_instance_drops_once(&["drop:item:1", "drop:item:2", "drop:item:3"]);
    assert_no_detached_scope();
}

/// R01：内层 item Body 发生不可恢复 Failure 时，同一个收口路径逐实例析构一次，输入不被
/// 重复销毁，也没有 detached Scope。
#[test]
fn r01_failure_inside_the_nested_body_drops_each_instance_once() {
    reset_observations();
    let outcome = block_on(Runtime::new().execute::<_, Vec<Vec<Total>>, _>(
        |flow, groups: Ref<Vec<Vec<TrackItem>>>| {
            let out_refs = flow.each(groups, |group, outer| {
                outer.each(group, |item, inner| {
                    inner.then_with_output(fail_inside_nested, item)
                })
            });
            flow.output_with(out_refs);
        },
        tracked_input().0,
    ));
    match outcome {
        Err(RunError::Body(_)) => {}
        other => panic!("expected a business Failure, got {other:?}"),
    }
    assert_eq!(body_calls(), 1);
    assert_each_instance_drops_once(&["drop:item:1", "drop:item:2", "drop:item:3"]);
    assert_no_detached_scope();
}

/// R01：内层 item Body 真 Pending 后取消——借用 Future 先结束，部分成果与 root 输入逐实例
/// 析构恰好一次，全部真实建立的 Scope 关闭（无悬空引用、无 detached child）。
#[test]
fn r01_pending_cancel_inside_the_nested_body_leaves_no_dangling_borrow() {
    reset_observations();
    let (input, log) = tracked_input();
    let runtime = Runtime::new();
    let mut future = Box::pin(runtime.execute::<_, Vec<Vec<TrackedTotal>>, _>(
        |flow, (groups, shared): (Ref<Vec<Vec<TrackItem>>>, Ref<SharedLog>)| {
            let out_refs = flow.each(groups, |group, outer| {
                outer.each(group, |item, inner| {
                    inner.then_with_output(&produce_then_pend, (item, shared))
                })
            });
            flow.output_with(out_refs);
        },
        (input, SharedLog { log }),
    ));

    let waker = futures::task::noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    assert!(
        matches!(future.as_mut().poll(&mut cx), Poll::Pending),
        "the nested body really suspends"
    );
    assert_eq!(body_calls(), 3, "the first inner collection completed");
    assert!(
        tracked_events().is_empty(),
        "nothing is dropped while suspended: {:?}",
        tracked_events()
    );

    drop(future);

    let events = tracked_events();
    // 借用 Future 先结束，然后当前产物、已收成果、root 输入逐实例刚好一次。
    let pending = events
        .iter()
        .position(|event| event == "drop:pending-node")
        .unwrap_or_else(|| panic!("the borrowed body future ends: {events:?}"));
    for needle in [
        "drop:total:2",
        "drop:total:3",
        "drop:total:4",
        "drop:item:1",
    ] {
        let at = events
            .iter()
            .position(|event| event == needle)
            .unwrap_or_else(|| panic!("missing {needle}: {events:?}"));
        assert!(
            pending < at,
            "the borrowed future ends before {needle}: {events:?}"
        );
    }
    for needle in [
        "drop:pending-node",
        "drop:total:2",
        "drop:total:3",
        "drop:total:4",
        "drop:item:1",
        "drop:item:2",
        "drop:item:3",
    ] {
        assert_eq!(
            events
                .iter()
                .filter(|event| event.as_str() == needle)
                .count(),
            1,
            "{needle} drops exactly once: {events:?}"
        );
    }
    assert_eq!(events.len(), 7, "no extra instance: {events:?}");
    assert_no_detached_scope();
}
