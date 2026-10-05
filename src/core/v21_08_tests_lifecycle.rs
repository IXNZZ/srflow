//! V21-08 顺序／失败／取消切片（H06、H19～H21、H23、H24）。
//!
//! 这些样本只使用 crate 内可见的 Each／Flow／Context 设施，不构成公开 API 承诺。
//! 全部次序断言都在**同一条共享序列**（[`take_shared_events`]）内比较位置：`record`
//! 的业务见证与 guard 清理／frame 退出事件写进同一日志，因此 `at`/`nth` 的位置可跨
//! 业务与清理事件直接比较；绝不在两条独立日志之间比较序号。
//!
//! H20／H21 的故障注入：当前生产代码只在协调组件内部提供 `destroy_probe` /
//! `inject_target_probe` / `set_finalizing_probe`，且它们是 context.rs／scope.rs 私有
//! 字段的方法，测试模块无法触达；样本因此经**真实收口入口**
//! （`consume_in_item_boundary`／`finish_collector`／真实 Each 的最终 Export）核对可达
//! 的拒绝与不变量，并在结果里报告缺失的注入钩子签名。

use super::builder::{CallSite, TypedCallBuilder};
use super::context::{
    BodyError, ExecutionContext, InvocationKind, ItemConsumePermit, TerminationKind,
};
use super::data_ref::DataRef;
use super::each::{Each, EachBuilder, EachOnly, EachShared};
use super::flow::{Flow, FlowBuilder};
use super::identity::ScopeId;
use super::internal_error::ScopeError;
use super::orchestrator::{OrchCall, ScopeRole};
use super::ref_id::{RefIdAllocator, RefIdSource};
use super::runtime::RootExecution;
use super::scope::{ConsumeOutcome, ScopeState};
use super::signature::{AsyncFnSig, Data, OrchSig, SyncFnSig};
use super::test_support::{
    RootView, advance_to_pending, at, boundary_address_snapshot, boundary_creation_snapshot, count,
    definition_in_root, drive, export_attempt_snapshot, gate_wait, install_gate, nth, record,
    release_gate, reset_observations, root_input, run_definition_plain, saw, saw_prefix,
    take_events, take_shared_events,
};

// ---------------------------------------------------------------- 业务夹具

/// 非 Clone 业务集合元素。
#[derive(Debug, PartialEq, Eq)]
struct Item {
    id: u32,
}

/// 非 Clone shared Data。
#[derive(Debug, PartialEq, Eq)]
struct Rules {
    weight: u32,
}

/// body 前置临时值：Drop 属于本项临时数据清理。
#[derive(Debug)]
struct Temp(#[allow(dead_code)] u32);

impl Drop for Temp {
    fn drop(&mut self) {
        record("temp-dropped");
    }
}

/// 每项 body 输出：Drop 见证"全部输出恰好清理一次"。
#[derive(Debug)]
struct Out {
    key: u32,
}

impl Drop for Out {
    fn drop(&mut self) {
        record("out-dropped");
    }
}

/// 跨 await 持有真实借用的见证：Drop 时借用才结束。
struct Borrow<'a>(&'a Item);

impl Drop for Borrow<'_> {
    fn drop(&mut self) {
        record("borrow-end");
    }
}

fn make_temp(item: &Item) -> Result<Temp, BodyError> {
    record("temp-made");
    Ok(Temp(item.id))
}

/// 第一项 Pending、释放后到 Ready 的异步 body（H06）。
async fn body_gated_shared(item: &Item, rules: &Rules) -> Result<Out, BodyError> {
    record("body-enter");
    gate_wait().await;
    record("body-resumed");
    Ok(Out {
        key: item.id + rules.weight,
    })
}

/// 第二项（id==20）才 Pending 的异步 body，并在 await 前后持有真实借用（H23／H24）。
async fn body_second_gated(item: &Item) -> Result<Out, BodyError> {
    record("body-enter");
    if item.id == 20 {
        let held = Borrow(item);
        gate_wait().await;
        record("body-resumed");
        return Ok(Out { key: held.0.id });
    }
    record("body-resumed");
    Ok(Out { key: item.id })
}

/// 第二项 body 业务失败（H19）。
fn probe_or_fail(item: &Item) -> Result<Out, BodyError> {
    record("body-enter");
    if item.id == 20 {
        return Err(BodyError::new("item body failure"));
    }
    Ok(Out { key: item.id })
}

/// 无挂起的普通 body（H24 父后步失败样本）。
fn body_plain(item: &Item) -> Result<Out, BodyError> {
    record("body-enter");
    Ok(Out { key: item.id })
}

#[allow(clippy::ptr_arg)] // Node 输入类型就是 `Vec<Out>`
fn after_each(results: &Vec<Out>) -> Result<u32, BodyError> {
    record("after-each");
    Ok(results.iter().map(|out| out.key).sum())
}

/// 父后步失败：证明最终 `Vec<O>` 已交给 parent 并由它清理（H24）。
fn later_failing(results: &Vec<Out>) -> Result<u32, BodyError> {
    record("later-failing");
    let _ = results;
    Err(BodyError::new("later step failure"))
}

/// 内嵌一层失败 Flow：`(Item,) -> Data<Out>`，内部 Node 在 id==20 失败（H19）。
fn failing_inner_flow() -> Flow<(Item,), Data<Out>> {
    let (mut inner, item_ref) = FlowBuilder::<(Item,)>::start().expect("inner");
    let _temp = inner
        .then::<_, SyncFnSig<(Item,), Data<Temp>>, _>(
            make_temp as fn(&Item) -> Result<Temp, BodyError>,
            item_ref.clone(),
        )
        .expect("inner temp");
    let out = inner
        .then::<_, SyncFnSig<(Item,), Data<Out>>, _>(
            probe_or_fail as fn(&Item) -> Result<Out, BodyError>,
            item_ref.clone(),
        )
        .expect("inner probe");
    inner.finish::<Data<Out>, _>(out).expect("inner finish")
}

// ---------------------------------------------------------------- 构建夹具

/// EachOnly + 无挂起 body：`(Vec<Item>,) -> Data<Vec<Out>>`。
fn plain_each_only() -> Each<EachOnly<Item>, Out> {
    let (mut body, item_ref) = FlowBuilder::<(Item,)>::start().expect("body");
    let _temp = body
        .then::<_, SyncFnSig<(Item,), Data<Temp>>, _>(
            make_temp as fn(&Item) -> Result<Temp, BodyError>,
            item_ref.clone(),
        )
        .expect("temp");
    let out = body
        .then::<_, SyncFnSig<(Item,), Data<Out>>, _>(
            body_plain as fn(&Item) -> Result<Out, BodyError>,
            item_ref.clone(),
        )
        .expect("probe");
    let body_flow = body.finish::<Data<Out>, _>(out).expect("body finish");
    let mut each_builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, OrchSig<Item, Data<Out>>>(body_flow)
        .expect("each body");
    each_builder.finish().expect("each finish")
}

/// EachOnly + 第二项挂起 body：`(Vec<Item>,) -> Data<Vec<Out>>`（H23／H24）。
fn gated_each_only() -> Each<EachOnly<Item>, Out> {
    let (mut body, item_ref) = FlowBuilder::<(Item,)>::start().expect("body");
    let _temp = body
        .then::<_, SyncFnSig<(Item,), Data<Temp>>, _>(
            make_temp as fn(&Item) -> Result<Temp, BodyError>,
            item_ref.clone(),
        )
        .expect("temp");
    let out = body
        .then::<_, AsyncFnSig<(Item,), Data<Out>>, _>(body_second_gated, item_ref.clone())
        .expect("probe");
    let body_flow = body.finish::<Data<Out>, _>(out).expect("body finish");
    let mut each_builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, OrchSig<Item, Data<Out>>>(body_flow)
        .expect("each body");
    each_builder.finish().expect("each finish")
}

/// 父 Flow 夹具：完成态 Flow 与两侧位置句柄。
type FlowFixture = (
    Flow<(Vec<Item>,), Data<Vec<Out>>>,
    DataRef<Vec<Item>>,
    DataRef<Vec<Out>>,
);

/// 父 Flow：`(Vec<Item>,) -> Data<Vec<Out>>`，只含 Each 一步。
fn each_only_parent(each: Each<EachOnly<Item>, Out>) -> FlowFixture {
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let collected: DataRef<Vec<Out>> = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
        .expect("each step");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");
    (flow, collection, collected)
}

/// 取一个 Scope 的 `(refs 数, owned 数)`（target-aware 只读快照）。
fn refs_owned(ctx: &ExecutionContext, scope: &ScopeId) -> (usize, usize) {
    let (refs, owned) = ctx.snapshot_targets_probe(scope).expect("target snapshot");
    (refs.len(), owned.len())
}

/// 触发一次真实 Each 的失败：注册集合／shared，直接在 Root frame 中驱动 Each 调用点，
/// 核对首个完整诊断、不可恢复性与两侧不变量。
fn observe_failing_each(each: Each<EachShared<Item, Rules>, Out>, items: Vec<Item>) {
    reset_observations();
    let (mut parent, (collection, rules)) =
        FlowBuilder::<(Vec<Item>, Rules)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<(Vec<Item>, Rules), Data<Vec<Out>>>, _>(
            each,
            (collection.clone(), rules.clone()),
        )
        .expect("each step");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");
    let collection_position = collection.position().clone();
    let rules_position = rules.position().clone();
    let collected_position = collected.position().clone();

    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned(&root, &collection_position, items)
        .expect("collection");
    execution
        .context_mut()
        .register_owned(&root, &rules_position, Rules { weight: 7 })
        .expect("rules");
    let mut guard = execution
        .context_mut()
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let site = flow.definition().steps()[0].site();
    let first = drive(match site {
        CallSite::Orchestrator(site) => site.invoke(&mut guard, &root),
        CallSite::Node(_) => panic!("Each step must be an orchestrator call site"),
    });
    let error = first.expect_err("each must fail");
    assert_eq!(
        error.note(),
        "item body failure",
        "首次原因即第二项 body 错误"
    );

    // 首个完整诊断（四字段）：类别／原因／实际 Scope／Scope 诊断。
    {
        let termination = guard.termination().expect("首次终止诊断");
        assert_eq!(termination.kind(), TerminationKind::BodyError);
        assert_eq!(termination.note(), "item body failure");
        let recorded = termination.scope().cloned().expect("诊断带实际 Scope");
        let role = boundary_creation_snapshot()
            .into_iter()
            .find(|(scope, _, _)| *scope == recorded)
            .map(|(_, _, role)| role)
            .expect("诊断 Scope 来自真实创建点");
        assert_eq!(role, ScopeRole::Flow, "第二项 body Flow 是最深失败 Scope");
        assert!(termination.scope_error().is_none());
    }
    let kind_before = TerminationKind::BodyError;
    let note_before = guard.termination().expect("诊断").note();
    let scope_before = guard.termination().expect("诊断").scope().cloned();
    let scope_error_before = guard
        .termination()
        .expect("诊断")
        .scope_error()
        .map(|error| format!("{error}"));

    // 不可恢复：终止后普通调用被拒、commit 被拒、四字段不被覆盖。
    let again = drive(match site {
        CallSite::Orchestrator(site) => site.invoke(&mut guard, &root),
        CallSite::Node(_) => panic!("Each step must be an orchestrator call site"),
    });
    assert!(again.is_err(), "终止后不能再执行业务调用");
    assert!(
        guard.finalize(&root, &[], &mut Vec::new()).is_err(),
        "终止后普通 commit 被拒"
    );
    {
        let after = guard.termination().expect("诊断保留");
        assert_eq!(after.kind(), kind_before);
        assert_eq!(after.note(), note_before);
        assert_eq!(after.scope(), scope_before.as_ref());
        assert_eq!(
            after.scope_error().map(|error| format!("{error}")),
            scope_error_before
        );
    }

    // 集合／shared 保留原 owner；没有部分集合输出绑定到 Root。
    let root_refs = guard.snapshot_probe(&root).expect("root snapshot").0;
    let collection_id = root_refs
        .iter()
        .find(|(position, _)| position == &collection_position)
        .map(|(_, id)| id.clone())
        .expect("collection still bound");
    let rules_id = root_refs
        .iter()
        .find(|(position, _)| position == &rules_position)
        .map(|(_, id)| id.clone())
        .expect("rules still bound");
    assert_eq!(
        guard.owner_probe(&collection_id).expect("owner"),
        root,
        "集合保留原 owner"
    );
    assert_eq!(
        guard.owner_probe(&rules_id).expect("owner"),
        root,
        "shared 保留原 owner"
    );
    assert!(
        !root_refs
            .iter()
            .any(|(position, _)| position == &collected_position),
        "失败不返回部分集合"
    );
    assert!(guard.alive_probe(&collection_id));
    drop(guard);
    drop(execution);

    // 事件证据（同一共享序列内比较）。
    let shared = take_shared_events();
    let creations = boundary_creation_snapshot();
    assert_eq!(
        creations
            .iter()
            .filter(|(_, _, role)| *role == ScopeRole::Item)
            .count(),
        2,
        "只建立前两项，后项不执行: {creations:?}"
    );
    assert_eq!(
        count(&shared, "collector-after:1"),
        1,
        "前项已 Consume: {shared:?}"
    );
    assert_eq!(
        count(&shared, "collector-after:2"),
        0,
        "第二项未 Consume: {shared:?}"
    );
    assert_eq!(
        count(&shared, "body-enter"),
        2,
        "第二项后不再执行: {shared:?}"
    );
    assert!(!saw(&shared, "after-each"), "父后步未执行: {shared:?}");
    assert_eq!(
        count(&shared, "out-dropped"),
        1,
        "collector 前项 Drop 一次: {shared:?}"
    );
    assert_eq!(
        count(&shared, "temp-dropped"),
        2,
        "成功项与当前临时值各 Drop 一次: {shared:?}"
    );
    assert!(
        at(&shared, "collector-after:1") < nth(&shared, "body-enter", 1),
        "前项消费先于第二项 body: {shared:?}"
    );
    assert!(
        nth(&shared, "body-enter", 1) < nth(&shared, "temp-dropped", 1),
        "第二项 body 后其临时值清理: {shared:?}"
    );
}

// ---------------------------------------------------------------- H06

#[test]
fn h06_multi_item_serial_no_parallel() {
    reset_observations();
    let (mut parent, (collection, rules)) =
        FlowBuilder::<(Vec<Item>, Rules)>::start().expect("parent");
    let (mut body, (item_ref, rules_ref)) = FlowBuilder::<(Item, Rules)>::start().expect("body");
    let _temp = body
        .then::<_, SyncFnSig<(Item,), Data<Temp>>, _>(
            make_temp as fn(&Item) -> Result<Temp, BodyError>,
            item_ref.clone(),
        )
        .expect("temp");
    let body_out = body
        .then::<_, AsyncFnSig<(Item, Rules), Data<Out>>, _>(
            body_gated_shared,
            (item_ref.clone(), rules_ref.clone()),
        )
        .expect("probe");
    let body_flow: Flow<(Item, Rules), Data<Out>> = body.finish(body_out).expect("body finish");
    let mut each_builder: EachBuilder<EachShared<Item, Rules>, Out> =
        EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, OrchSig<(Item, Rules), Data<Out>>>(body_flow)
        .expect("each body");
    let each: Each<EachShared<Item, Rules>, Out> = each_builder.finish().expect("each finish");
    let collected = parent
        .then::<_, OrchSig<(Vec<Item>, Rules), Data<Vec<Out>>>, _>(
            each,
            (collection.clone(), rules.clone()),
        )
        .expect("wire each");
    let after = parent
        .then::<_, SyncFnSig<(Vec<Out>,), Data<u32>>, _>(
            after_each as fn(&Vec<Out>) -> Result<u32, BodyError>,
            collected.clone(),
        )
        .expect("after step");
    let flow = parent.finish::<Data<u32>, _>(after).expect("parent finish");

    install_gate();
    let boxed = advance_to_pending(
        definition_in_root::<_, fn(&mut RootView<'_, '_>) -> Result<(), BodyError>>(
            flow.definition(),
            vec![
                root_input(
                    &collection,
                    vec![Item { id: 10 }, Item { id: 20 }, Item { id: 30 }],
                ),
                root_input(&rules, Rules { weight: 7 }),
            ],
            |_: &mut ExecutionContext, _: &ScopeId| {},
            None,
        ),
        1,
    );
    // 前项 Pending：后项未建立／执行，父后步未执行，无清理。
    {
        let creations = boundary_creation_snapshot();
        let items = creations
            .iter()
            .filter(|(_, _, role)| *role == ScopeRole::Item)
            .count();
        let shared = take_shared_events();
        assert_eq!(
            items, 1,
            "前项 Pending 时只建立一个 ItemScope: {creations:?}"
        );
        assert_eq!(
            count(&shared, "body-enter"),
            1,
            "只进入第一项 body: {shared:?}"
        );
        assert_eq!(
            count(&shared, "body-resumed"),
            0,
            "第一项尚未恢复: {shared:?}"
        );
        assert!(!saw(&shared, "after-each"), "父后步未执行: {shared:?}");
        assert_eq!(count(&shared, "cleanup-start"), 0, "无失败清理: {shared:?}");
    }
    // 同一 Future：release 后到 Ready。
    release_gate();
    drive(boxed).expect("each run");
    let shared = take_shared_events();
    let creations = boundary_creation_snapshot();
    let roles: Vec<ScopeRole> = creations.iter().map(|(_, _, role)| *role).collect();
    assert_eq!(
        roles,
        vec![
            ScopeRole::Each,
            ScopeRole::Item,
            ScopeRole::Flow,
            ScopeRole::Item,
            ScopeRole::Flow,
            ScopeRole::Item,
            ScopeRole::Flow
        ],
        "Each 一次、每项 Item 一次、每项 body child 一次: {roles:?}"
    );
    let item1_seq = creations[1].0.seq();
    assert!(
        at(&shared, &format!("frame-exit:boundary:{item1_seq}")) < nth(&shared, "body-enter", 1),
        "Item Closed 后才 next: {shared:?}"
    );
    assert!(
        at(&shared, "collector-after:1") < nth(&shared, "body-enter", 1),
        "第一项消费后才执行第二项: {shared:?}"
    );
    assert!(
        nth(&shared, "body-resumed", 2) < at(&shared, "after-each"),
        "父后步最后执行: {shared:?}"
    );
    assert!(at(&shared, "after-each") < at(&shared, "frame-exit:root"));
    assert_eq!(
        count(&shared, "temp-dropped"),
        3,
        "每项临时值清理一次: {shared:?}"
    );
    assert_eq!(
        count(&shared, "out-dropped"),
        3,
        "最终集合在 Root 收口时析构一次: {shared:?}"
    );
}

// ---------------------------------------------------------------- H19

#[test]
fn h19_second_item_failure_stops_and_preserves_first_cause() {
    // 场景 A：body Flow 内部的 Node 在第二项失败。
    {
        let (mut body, (item_ref, _rules_ref)) =
            FlowBuilder::<(Item, Rules)>::start().expect("body");
        let _temp = body
            .then::<_, SyncFnSig<(Item,), Data<Temp>>, _>(
                make_temp as fn(&Item) -> Result<Temp, BodyError>,
                item_ref.clone(),
            )
            .expect("temp");
        let out = body
            .then::<_, SyncFnSig<(Item,), Data<Out>>, _>(
                probe_or_fail as fn(&Item) -> Result<Out, BodyError>,
                item_ref.clone(),
            )
            .expect("probe");
        let body_flow = body.finish::<Data<Out>, _>(out).expect("body finish");
        let mut each_builder: EachBuilder<EachShared<Item, Rules>, Out> =
            EachBuilder::start().expect("each");
        each_builder
            .then_body::<_, OrchSig<(Item, Rules), Data<Out>>>(body_flow)
            .expect("each body");
        let each = each_builder.finish().expect("each finish");
        observe_failing_each(
            each,
            vec![Item { id: 10 }, Item { id: 20 }, Item { id: 30 }],
        );
    }
    // 场景 B：body 是完成态 Flow，其内部再嵌一层失败 Flow（body Flow 执行错误）。
    {
        let (mut outer, (item_ref, _rules_ref)) =
            FlowBuilder::<(Item, Rules)>::start().expect("outer body");
        let out = outer
            .then::<_, OrchSig<Item, Data<Out>>, _>(failing_inner_flow(), item_ref.clone())
            .expect("inner flow step");
        let body_flow = outer.finish::<Data<Out>, _>(out).expect("body finish");
        let mut each_builder: EachBuilder<EachShared<Item, Rules>, Out> =
            EachBuilder::start().expect("each");
        each_builder
            .then_body::<_, OrchSig<(Item, Rules), Data<Out>>>(body_flow)
            .expect("each body");
        let each = each_builder.finish().expect("each finish");
        observe_failing_each(each, vec![Item { id: 10 }, Item { id: 20 }]);
    }
}

// ---------------------------------------------------------------- H20

#[test]
fn h20_item_consume_rejections_preserve_state_and_primary() {
    // 场景 A：真实收口成功 → Item Closed、旧输出移入 collector，EachScope／其他 collector 不变。
    {
        reset_observations();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let allocator = RefIdAllocator::new(RefIdSource::new());
        let selected = allocator.allocate().expect("selected");
        let temp = allocator.allocate().expect("temp");
        let other_pos = allocator.allocate().expect("other");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let each = guard.create_child(&root).expect("each");
        let collector = guard.begin_collector::<Out>(&each).expect("collector");
        let other = guard.create_child(&root).expect("other");
        let other_collector = guard
            .begin_collector::<Out>(&other)
            .expect("other collector");
        let _other_id = guard
            .register_owned(&other, &other_pos, Out { key: 99 })
            .expect("other value");
        let item = guard.create_child(&each).expect("item");
        let selected_id = guard
            .register_owned(&item, &selected, Out { key: 1 })
            .expect("selected output");
        let _temp_id = guard.register_owned(&item, &temp, Temp(9)).expect("temp");
        let each_before = refs_owned(&guard, &each);
        let other_before = refs_owned(&guard, &other);
        let permit = ItemConsumePermit {
            each: each.clone(),
            collector: collector.clone(),
        };
        let mut each_guard = guard
            .enter(InvocationKind::Boundary, &each, true)
            .expect("each frame");
        let mut item_guard = each_guard
            .enter(InvocationKind::Boundary, &item, true)
            .expect("item frame");
        let outcome = item_guard.consume_in_item_boundary(&item, &selected, &permit);
        assert!(
            matches!(outcome, ConsumeOutcome::Consumed),
            "真实收口成功: {outcome:?}"
        );
        assert_eq!(
            item_guard.state(&item).expect("state"),
            ScopeState::Closed,
            "Consumed 后 Item Closed"
        );
        assert!(
            !item_guard.alive_probe(&selected_id),
            "旧输出身份已失效（已移入 collector）"
        );
        assert_eq!(
            item_guard.collector_moves_probe(&collector).expect("moves"),
            1,
            "collector 恰好移动一次"
        );
        assert_eq!(
            refs_owned(&item_guard, &each),
            each_before,
            "EachScope 控制元数据不变"
        );
        assert_eq!(
            refs_owned(&item_guard, &other),
            other_before,
            "其他 collector 的 Scope 不变"
        );
        assert_eq!(
            item_guard
                .collector_moves_probe(&other_collector)
                .expect("moves"),
            0,
            "其他 collector 不受影响"
        );
        // Closed 责任分支：Consumed 后 guard 仍负责 Item，解除责任后为空。
        assert_eq!(
            item_guard.responsible_scopes(),
            std::slice::from_ref(&item),
            "Consumed 分支先保留责任"
        );
        // 与生产 run_item 一致：Closed 分支解除责任后完成。
        item_guard.release_responsibility(&item);
        assert!(item_guard.responsible_scopes().is_empty(), "解除后不再负责");
        item_guard.complete();
        each_guard.release_responsibility(&each);
        each_guard.complete();
        drop(guard);
        drop(execution);
    }

    // 场景 B：早期预检拒绝（有活 descendant）——未 Closed 分支；两侧状态不变、原错保留。
    {
        reset_observations();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let allocator = RefIdAllocator::new(RefIdSource::new());
        let selected = allocator.allocate().expect("selected");
        let other_pos = allocator.allocate().expect("other");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let each = guard.create_child(&root).expect("each");
        let collector = guard.begin_collector::<Out>(&each).expect("collector");
        let other = guard.create_child(&root).expect("other");
        let other_collector = guard
            .begin_collector::<Out>(&other)
            .expect("other collector");
        let _other_id = guard
            .register_owned(&other, &other_pos, Out { key: 99 })
            .expect("other value");
        let item = guard.create_child(&each).expect("item");
        let selected_id = guard
            .register_owned(&item, &selected, Out { key: 1 })
            .expect("selected output");
        let each_before = refs_owned(&guard, &each);
        let item_before = refs_owned(&guard, &item);
        let other_before = refs_owned(&guard, &other);
        let permit = ItemConsumePermit {
            each: each.clone(),
            collector: collector.clone(),
        };
        let mut each_guard = guard
            .enter(InvocationKind::Boundary, &each, true)
            .expect("each frame");
        let mut item_guard = each_guard
            .enter(InvocationKind::Boundary, &item, true)
            .expect("item frame");
        let _grand = item_guard.create_child(&item).expect("live descendant");
        let outcome = item_guard.consume_in_item_boundary(&item, &selected, &permit);
        match outcome {
            ConsumeOutcome::Rejected {
                primary,
                cleanup_failure,
            } => {
                assert!(
                    matches!(primary, ScopeError::ActiveDescendants { .. }),
                    "原错是活 descendant 预检: {primary:?}"
                );
                assert!(
                    cleanup_failure.is_none(),
                    "早期拒绝不假装已清理: {cleanup_failure:?}"
                );
            }
            other => panic!("expected rejection, got {other:?}"),
        }
        assert_eq!(
            item_guard.state(&item).expect("state"),
            ScopeState::Active,
            "早期拒绝后 Item 仍未 Closed（未 Closed 责任分支）"
        );
        assert!(item_guard.alive_probe(&selected_id), "旧输出仍存活");
        assert_eq!(
            item_guard.owner_probe(&selected_id).expect("owner"),
            item,
            "旧输出仍 Item-owned"
        );
        assert_eq!(
            refs_owned(&item_guard, &item),
            item_before,
            "Item 本地 refs／owned 不变"
        );
        assert_eq!(
            refs_owned(&item_guard, &each),
            each_before,
            "EachScope 控制元数据不变"
        );
        assert_eq!(
            refs_owned(&item_guard, &other),
            other_before,
            "兄弟不受影响"
        );
        assert_eq!(
            item_guard.collector_moves_probe(&collector).expect("moves"),
            0,
            "collector 未变"
        );
        assert_eq!(
            item_guard
                .collector_moves_probe(&other_collector)
                .expect("moves"),
            0,
            "其他 collector 不受影响"
        );
        // 未 Closed 责任分支：拒绝后 Item 仍 Active，guard 继续负责（不解除、不冒充已清理）。
        assert_eq!(
            item_guard.responsible_scopes(),
            std::slice::from_ref(&item),
            "未 Closed 分支保留责任"
        );
        // 释放：丢弃 item guard 清理子树。
        drop(item_guard);
        drop(each_guard);
        drop(guard);
        drop(execution);
    }

    // 场景 C：prepare 阶段拒绝（选中目标是 CollectionItem，非完整 Data）——Closed 分支；
    // 原错保留、内部清理成功；EachScope／兄弟／Root 不受影响。
    {
        reset_observations();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let allocator = RefIdAllocator::new(RefIdSource::new());
        let collection_pos = allocator.allocate().expect("collection");
        let item_port = allocator.allocate().expect("item port");
        let other_pos = allocator.allocate().expect("other");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let each = guard.create_child(&root).expect("each");
        let _collection_id = guard
            .register_owned(&each, &collection_pos, vec![Item { id: 1 }])
            .expect("collection");
        let collector = guard.begin_collector::<Out>(&each).expect("collector");
        let other = guard.create_child(&root).expect("other");
        let other_collector = guard
            .begin_collector::<Out>(&other)
            .expect("other collector");
        let _other_id = guard
            .register_owned(&other, &other_pos, Out { key: 99 })
            .expect("other value");
        let item = guard.create_child(&each).expect("item");
        let each_before = refs_owned(&guard, &each);
        let other_before = refs_owned(&guard, &other);
        let root_before = refs_owned(&guard, &root);
        let permit = ItemConsumePermit {
            each: each.clone(),
            collector: collector.clone(),
        };
        let mut each_guard = guard
            .enter(InvocationKind::Boundary, &each, true)
            .expect("each frame");
        each_guard
            .bind_item_input::<Item>(&item, &each, &collection_pos, &item_port, 0)
            .expect("bind item input");
        let mut item_guard = each_guard
            .enter(InvocationKind::Boundary, &item, true)
            .expect("item frame");
        let outcome = item_guard.consume_in_item_boundary(&item, &item_port, &permit);
        match outcome {
            ConsumeOutcome::Rejected {
                primary,
                cleanup_failure,
            } => {
                assert!(
                    matches!(primary, ScopeError::NonCompleteTarget { .. }),
                    "原错是 prepare 的完整 Data 前提: {primary:?}"
                );
                assert!(
                    cleanup_failure.is_none(),
                    "内部清理成功，无清理失败: {cleanup_failure:?}"
                );
            }
            other => panic!("expected rejection, got {other:?}"),
        }
        // Closed 空状态不能替代前述断言：另核对两侧 refs／owned 与其它 collector 不变。
        assert_eq!(
            item_guard.state(&item).expect("state"),
            ScopeState::Closed,
            "prepare 拒绝后内部清理成功 → Closed"
        );
        assert_eq!(
            refs_owned(&item_guard, &each),
            each_before,
            "EachScope 控制元数据不变"
        );
        assert_eq!(
            refs_owned(&item_guard, &other),
            other_before,
            "兄弟不受影响"
        );
        assert_eq!(
            refs_owned(&item_guard, &root),
            root_before,
            "ancestor 不受影响"
        );
        assert_eq!(
            item_guard.collector_moves_probe(&collector).expect("moves"),
            0,
            "collector 未变"
        );
        assert_eq!(
            item_guard
                .collector_moves_probe(&other_collector)
                .expect("moves"),
            0,
            "其他 collector 不受影响"
        );
        drop(item_guard);
        drop(each_guard);
        drop(guard);
        drop(execution);
    }
}

// ---------------------------------------------------------------- H21

#[test]
fn h21_finish_preconditions_and_caller_export_conflict() {
    // 真实 Each 的最终输出位置（故障为 test-only 的坐标构造）。
    let final_position = {
        let each = plain_each_only();
        each.final_position().clone()
    };

    // 1) controller 非 Active：真实 abort 关闭控制器后 finish 拒绝。
    {
        reset_observations();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let allocator = RefIdAllocator::new(RefIdSource::new());
        let probe0 = allocator.allocate().expect("probe0");
        let probe1 = allocator.allocate().expect("probe1");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let each = guard.create_child(&root).expect("each");
        let collector = guard.begin_collector::<Out>(&each).expect("collector");
        let probe = guard.create_child(&root).expect("probe");
        guard.abort(&each).expect("close controller");
        let p0 = guard.register_owned(&probe, &probe0, 0u32).expect("p0");
        let result = guard.finish_collector(&each, &collector, &final_position);
        assert!(
            matches!(result, Err(ScopeError::ScopeClosed { .. })),
            "非 Active 控制器拒绝 finish: {result:?}"
        );
        let p1 = guard.register_owned(&probe, &probe1, 0u32).expect("p1");
        assert_eq!(p1.seq(), p0.seq() + 1, "finish 前置失败不消耗 DataId");
        assert_eq!(refs_owned(&guard, &each), (0, 0), "无新绑定");
        drop(guard);
        drop(execution);
    }

    // 2) 输出已绑定：控制器最终位置已有绑定 → RefAlreadyBound，绑定与 collector 不变。
    {
        reset_observations();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let allocator = RefIdAllocator::new(RefIdSource::new());
        let probe0 = allocator.allocate().expect("probe0");
        let probe1 = allocator.allocate().expect("probe1");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let each = guard.create_child(&root).expect("each");
        let collector = guard.begin_collector::<Out>(&each).expect("collector");
        let probe = guard.create_child(&root).expect("probe");
        let _bound = guard
            .register_owned(&each, &final_position, vec![Out { key: 7 }])
            .expect("prebound output");
        let before = refs_owned(&guard, &each);
        let p0 = guard.register_owned(&probe, &probe0, 0u32).expect("p0");
        let result = guard.finish_collector(&each, &collector, &final_position);
        match result {
            Err(ScopeError::RefAlreadyBound { position, .. }) => {
                assert_eq!(position, final_position, "冲突位置即最终输出位置");
            }
            other => panic!("expected RefAlreadyBound, got {other:?}"),
        }
        let p1 = guard.register_owned(&probe, &probe1, 0u32).expect("p1");
        assert_eq!(p1.seq(), p0.seq() + 1, "finish 前置失败不消耗 DataId");
        assert_eq!(refs_owned(&guard, &each), before, "无新绑定");
        assert_eq!(
            guard.collector_moves_probe(&collector).expect("moves"),
            0,
            "collector 未消耗"
        );
        drop(guard);
        drop(execution);
    }

    // 3) 有活 descendant：控制器仍有未关闭 child → ActiveDescendants，无新绑定。
    {
        reset_observations();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let allocator = RefIdAllocator::new(RefIdSource::new());
        let probe0 = allocator.allocate().expect("probe0");
        let probe1 = allocator.allocate().expect("probe1");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let each = guard.create_child(&root).expect("each");
        let collector = guard.begin_collector::<Out>(&each).expect("collector");
        let _descendant = guard.create_child(&each).expect("live descendant");
        let probe = guard.create_child(&root).expect("probe");
        let p0 = guard.register_owned(&probe, &probe0, 0u32).expect("p0");
        let result = guard.finish_collector(&each, &collector, &final_position);
        match result {
            Err(ScopeError::ActiveDescendants { scope }) => assert_eq!(scope, each),
            other => panic!("expected ActiveDescendants, got {other:?}"),
        }
        let p1 = guard.register_owned(&probe, &probe1, 0u32).expect("p1");
        assert_eq!(p1.seq(), p0.seq() + 1, "finish 前置失败不消耗 DataId");
        assert_eq!(refs_owned(&guard, &each), (0, 0), "无新绑定");
        assert_eq!(
            guard.collector_moves_probe(&collector).expect("moves"),
            0,
            "collector 未消耗"
        );
        drop(guard);
        drop(execution);
    }

    // 4) collector owner 不匹配：collector 属于另一控制器 → CollectorNotOwnedBy。
    {
        reset_observations();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let allocator = RefIdAllocator::new(RefIdSource::new());
        let probe0 = allocator.allocate().expect("probe0");
        let probe1 = allocator.allocate().expect("probe1");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let each = guard.create_child(&root).expect("each");
        let other = guard.create_child(&root).expect("other");
        let other_collector = guard.begin_collector::<Out>(&other).expect("collector");
        let probe = guard.create_child(&root).expect("probe");
        let p0 = guard.register_owned(&probe, &probe0, 0u32).expect("p0");
        let result = guard.finish_collector(&each, &other_collector, &final_position);
        match result {
            Err(ScopeError::CollectorNotOwnedBy { expected_owner, .. }) => {
                assert_eq!(expected_owner, each, "诊断保留实际控制器");
            }
            other => panic!("expected CollectorNotOwnedBy, got {other:?}"),
        }
        let p1 = guard.register_owned(&probe, &probe1, 0u32).expect("p1");
        assert_eq!(p1.seq(), p0.seq() + 1, "finish 前置失败不消耗 DataId");
        assert_eq!(refs_owned(&guard, &each), (0, 0), "控制器无新绑定");
        assert_eq!(
            guard
                .collector_moves_probe(&other_collector)
                .expect("moves"),
            0,
            "其他 owner 的 collector 未被消耗"
        );
        drop(guard);
        drop(execution);
    }

    // 5) caller 最终端口冲突：真实 Each 收口成功但 Export 到已被预占的 caller 位置失败；
    //    新 Vec 在提交前快照里仍 Each-owned，caller 无半成品，全部 O 清理一次。
    {
        reset_observations();
        let each = plain_each_only();
        let final_position = each.final_position().clone();
        let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
        let collected = parent
            .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
            .expect("each step");
        let flow = parent
            .finish::<Data<Vec<Out>>, _>(collected.clone())
            .expect("parent finish");
        let collection_position = collection.position().clone();
        let collected_position = collected.position().clone();

        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(
                &root,
                &collection_position,
                vec![Item { id: 1 }, Item { id: 2 }],
            )
            .expect("collection");
        let prebound_id = execution
            .context_mut()
            .register_owned(&root, &collected_position, Vec::<Out>::new())
            .expect("prebound caller port");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let site = flow.definition().steps()[0].site();
        let outcome = drive(match site {
            CallSite::Orchestrator(site) => site.invoke(&mut guard, &root),
            CallSite::Node(_) => panic!("Each step must be an orchestrator call site"),
        });
        let error = outcome.expect_err("caller 端口冲突必须拒绝整组 Export");
        assert_eq!(error.note(), "scope operation failed");
        {
            let termination = guard.termination().expect("终止诊断");
            assert_eq!(termination.kind(), TerminationKind::BodyError);
            match termination.scope_error() {
                Some(ScopeError::RefAlreadyBound { position, scope }) => {
                    assert_eq!(position, &collected_position, "冲突位置是 caller 最终端口");
                    assert_eq!(scope, &root, "冲突位置所在 Scope 是 caller");
                }
                other => panic!("expected RefAlreadyBound, got {other:?}"),
            }
        }
        // 提交前快照：最终 Vec 已绑定在 Each child 上（Export 失败前仍 Each-owned）。
        let exports = export_attempt_snapshot();
        assert_eq!(exports.len(), 1, "只有一次 Export 尝试: {exports:?}");
        let (each_child, refs, owned) = &exports[0];
        assert_eq!(owned.len(), 1, "Each child 恰好持有一份完整 Vec: {owned:?}");
        let vec_id = owned[0].clone();
        assert!(
            refs.iter()
                .any(|(position, id)| position == &final_position && id == &vec_id),
            "最终 Vec 绑定在 Each 的声明输出位置: {refs:?}"
        );
        // Each child 确实是导出到 caller 的那个 child（来自同一次真实调用边界）。
        assert!(
            boundary_creation_snapshot()
                .iter()
                .any(|(scope, _, role)| scope == each_child && *role == ScopeRole::Each),
            "导出来源是真实 Each child: {each_child:?}"
        );
        // 失败清理后：新 Vec 已析构一次；caller 仍是最初预占值。
        assert!(
            !guard.alive_probe(&vec_id),
            "失败的 Each child 已清理新 Vec"
        );
        let root_refs = guard.snapshot_probe(&root).expect("root snapshot").0;
        let bound = root_refs
            .iter()
            .find(|(position, _)| position == &collected_position)
            .map(|(_, id)| id.clone())
            .expect("caller port still bound");
        assert_eq!(bound, prebound_id, "caller 端口仍是最初值，无半成品");
        assert!(guard.alive_probe(&prebound_id));
        assert_eq!(guard.owner_probe(&prebound_id).expect("owner"), root);
        drop(guard);
        drop(execution);
        let shared = take_shared_events();
        assert_eq!(
            count(&shared, "collector-after:2"),
            1,
            "Each 收口成功: {shared:?}"
        );
        assert_eq!(
            count(&shared, "out-dropped"),
            2,
            "全部 O 清理一次: {shared:?}"
        );
    }
}

// ---------------------------------------------------------------- H23

#[test]
fn h23_erased_each_body_cancellation() {
    let (flow, collection, collected) = each_only_parent(gated_each_only());
    let collection_position = collection.position().clone();
    let collected_position = collected.position().clone();
    let items = || vec![Item { id: 10 }, Item { id: 20 }];

    // 未 poll 对照：同一入口的 Future 从不推进，直接丢弃。
    {
        reset_observations();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(&root, &collection_position, items())
            .expect("collection");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let site = flow.definition().steps()[0].site();
        let unpolled = match site {
            CallSite::Orchestrator(site) => Box::pin(site.invoke(&mut guard, &root)),
            CallSite::Node(_) => panic!("Each step must be an orchestrator call site"),
        };
        drop(unpolled);
        assert_eq!(take_events(), Vec::<String>::new(), "未 poll 无业务事件");
        assert!(take_shared_events().is_empty(), "未 poll 不进入任何边界");
        drop(guard);
        drop(execution);
    }

    // Pending 后丢弃真实 Future 本体：此前一项已收集，下一项持借用 Pending。
    {
        reset_observations();
        install_gate();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(&root, &collection_position, items())
            .expect("collection");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let site = flow.definition().steps()[0].site();
        let boxed = match site {
            CallSite::Orchestrator(site) => advance_to_pending(site.invoke(&mut guard, &root), 1),
            CallSite::Node(_) => panic!("Each step must be an orchestrator call site"),
        };
        let creations = boundary_creation_snapshot();
        assert_eq!(
            creations
                .iter()
                .map(|(_, _, role)| *role)
                .collect::<Vec<_>>(),
            vec![
                ScopeRole::Each,
                ScopeRole::Item,
                ScopeRole::Flow,
                ScopeRole::Item,
                ScopeRole::Flow
            ],
            "Each／两项 Item 与各自 body child: {creations:?}"
        );
        let each_seq = creations[0].0.seq();
        let item2_seq = creations[3].0.seq();
        let flow2_seq = creations[4].0.seq();
        drop(boxed);

        let shared = take_shared_events();
        let events = take_events();
        assert_eq!(
            count(&shared, "collector-after:1"),
            1,
            "此前一项已收集: {shared:?}"
        );
        assert_eq!(
            count(&shared, "collector-after:2"),
            0,
            "下一项未收集: {shared:?}"
        );
        assert_eq!(
            count(&events, "out-dropped"),
            1,
            "collector 恰好析构一次: {events:?}"
        );
        assert_eq!(count(&shared, "borrow-end"), 1, "借用结束一次: {shared:?}");
        // 借用先结束，随后被取消的最深 Leaf 清理，再由内到外逐层相邻。
        assert!(
            at(&shared, "borrow-end") < at(&shared, "cleanup-start:none"),
            "{shared:?}"
        );
        assert!(
            at(&shared, "cleanup-start:none") < at(&shared, "cleanup-end:none"),
            "{shared:?}"
        );
        assert!(
            at(&shared, "cleanup-end:none")
                < nth(&shared, &format!("frame-exit:leaf:{flow2_seq}"), 1),
            "{shared:?}"
        );
        assert!(
            nth(&shared, &format!("frame-exit:leaf:{flow2_seq}"), 1)
                < at(&shared, &format!("cleanup-start:{flow2_seq}")),
            "{shared:?}"
        );
        for seq in [flow2_seq, item2_seq, each_seq] {
            assert!(
                at(&shared, &format!("cleanup-start:{seq}"))
                    < at(&shared, &format!("cleanup-end:{seq}")),
                "Scope {seq} 清理 start 先于 end: {shared:?}"
            );
            assert!(
                at(&shared, &format!("cleanup-end:{seq}"))
                    < at(&shared, &format!("frame-exit:boundary:{seq}")),
                "Scope {seq} 清理 end 先于 frame 退出: {shared:?}"
            );
        }
        assert!(
            at(&shared, &format!("frame-exit:boundary:{flow2_seq}"))
                < at(&shared, &format!("cleanup-start:{item2_seq}")),
            "Flow 退出后才清理 Item: {shared:?}"
        );
        assert!(
            at(&shared, &format!("frame-exit:boundary:{item2_seq}"))
                < at(&shared, &format!("cleanup-start:{each_seq}")),
            "Item 退出后才清理 Each: {shared:?}"
        );
        assert!(
            !shared.iter().any(|event| event.contains("frame-exit:root")),
            "ancestor Root 存活"
        );
        assert!(!shared.iter().any(|event| event.contains("context-drop")));
        // collector 的析构发生在 Each 清理段内（同一共享序列内比较）。
        assert!(
            at(&shared, &format!("cleanup-start:{each_seq}")) < at(&shared, "out-dropped"),
            "collector 在 Each 清理开始后析构: {shared:?}"
        );
        assert!(
            at(&shared, "out-dropped") < at(&shared, &format!("cleanup-end:{each_seq}")),
            "collector 在 Each 清理结束前析构: {shared:?}"
        );
        // 逐 Scope parent／Closed，最深取消定位。
        for (scope, parent, _) in &creations {
            assert_eq!(
                guard.parent_of(scope).expect("parent known"),
                Some(parent.clone()),
                "parent 关系与实际一致"
            );
            assert!(
                matches!(guard.state(scope), Ok(ScopeState::Closed)),
                "取消后 child Scope 已关闭"
            );
        }
        assert!(
            matches!(guard.state(&root), Ok(ScopeState::Active)),
            "Root 仍 Active"
        );
        let termination = guard.termination().expect("取消记录");
        assert_eq!(termination.kind(), TerminationKind::Cancelled);
        assert_eq!(
            termination.scope(),
            Some(&creations[4].0),
            "取消定位是实际最深 Scope"
        );
        drop(guard);
        drop(execution);
    }

    // Ready 对照：同一 erased 入口不挂起时正常完成并把结果交给 caller。
    {
        reset_observations();
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        execution
            .context_mut()
            .register_owned(&root, &collection_position, items())
            .expect("collection");
        let mut guard = execution
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .expect("root frame");
        let site = flow.definition().steps()[0].site();
        let outcome = drive(match site {
            CallSite::Orchestrator(site) => site.invoke(&mut guard, &root),
            CallSite::Node(_) => panic!("Each step must be an orchestrator call site"),
        });
        assert!(outcome.is_ok(), "{outcome:?}");
        let shared = take_shared_events();
        assert_eq!(count(&shared, "collector-after:2"), 1, "{shared:?}");
        assert_eq!(count(&shared, "borrow-end"), 1, "{shared:?}");
        assert_eq!(
            count(&shared, "out-dropped"),
            0,
            "Ready 后输出已交给 caller: {shared:?}"
        );
        let root_refs = guard.snapshot_probe(&root).expect("root snapshot").0;
        assert!(
            root_refs
                .iter()
                .any(|(position, _)| position == &collected_position),
            "Ready 后输出绑定在 caller: {root_refs:?}"
        );
        drop(guard);
        drop(execution);
    }
}

// ---------------------------------------------------------------- H24

#[test]
fn h24_root_owning_future_cancellation() {
    let (flow, collection, _collected) = each_only_parent(gated_each_only());
    let items = || vec![Item { id: 10 }, Item { id: 20 }];

    // 未 poll 对照：真正拥有 Context 的 Root Future 从不推进。
    {
        reset_observations();
        let unpolled = Box::pin(definition_in_root::<
            fn(&mut ExecutionContext, &ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            flow.definition(),
            vec![root_input(&collection, items())],
            |_, _| {},
            None,
        ));
        drop(unpolled);
        let untouched = take_shared_events();
        assert!(
            !untouched.iter().any(|event| event.starts_with("frame-exit")
                || event.starts_with("cleanup")
                || event.contains("context-drop")
                || event.contains("container-drop")),
            "未 poll 的执行不创建 Context／Scope: {untouched:?}"
        );
        assert_eq!(count(&take_events(), "body-enter"), 0);
    }

    // Pending 后直接 drop 真正拥有 Context 的 Root Future。
    {
        reset_observations();
        install_gate();
        let boxed = advance_to_pending(
            definition_in_root::<
                fn(&mut ExecutionContext, &ScopeId),
                fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
            >(
                flow.definition(),
                vec![root_input(&collection, items())],
                |_, _| {},
                None,
            ),
            1,
        );
        let creations = boundary_creation_snapshot();
        let addresses = boundary_address_snapshot();
        assert_eq!(
            creations
                .iter()
                .map(|(_, _, role)| *role)
                .collect::<Vec<_>>(),
            vec![
                ScopeRole::Each,
                ScopeRole::Item,
                ScopeRole::Flow,
                ScopeRole::Item,
                ScopeRole::Flow
            ],
            "{creations:?}"
        );
        let each_seq = creations[0].0.seq();
        let item2_seq = creations[3].0.seq();
        let flow2_seq = creations[4].0.seq();
        let root_seq = creations[0].1.seq();
        drop(boxed);

        let shared = take_shared_events();
        assert_eq!(
            count(&shared, "collector-after:1"),
            1,
            "此前一项已收集: {shared:?}"
        );
        let exit = |seq: u64| format!("frame-exit:boundary:{seq}");
        for seq in [flow2_seq, item2_seq, each_seq] {
            assert_eq!(
                count(&shared, &exit(seq)),
                1,
                "每层恰好退出一次: {shared:?}"
            );
        }
        // Each／Item／descendant 清理先于 Root／Context／Container。
        assert!(
            at(&shared, &exit(flow2_seq)) < at(&shared, &exit(item2_seq)),
            "{shared:?}"
        );
        assert!(
            at(&shared, &exit(item2_seq)) < at(&shared, &exit(each_seq)),
            "{shared:?}"
        );
        // 每一层：cleanup-start → cleanup-end → frame-exit，再由内到外进入上一层。
        for seq in [flow2_seq, item2_seq, each_seq] {
            assert!(
                at(&shared, &format!("cleanup-start:{seq}"))
                    < at(&shared, &format!("cleanup-end:{seq}")),
                "Scope {seq} 清理 start 先于 end: {shared:?}"
            );
            assert!(
                at(&shared, &format!("cleanup-end:{seq}")) < at(&shared, &exit(seq)),
                "Scope {seq} 清理 end 先于 frame 退出: {shared:?}"
            );
        }
        assert!(
            nth(&shared, &format!("frame-exit:leaf:{flow2_seq}"), 1)
                < at(&shared, &format!("cleanup-start:{flow2_seq}")),
            "被取消 Leaf 退出后才清理其 Scope: {shared:?}"
        );
        assert!(
            at(&shared, &exit(flow2_seq)) < at(&shared, &format!("cleanup-start:{item2_seq}")),
            "Flow 退出后才清理 Item: {shared:?}"
        );
        assert!(
            at(&shared, &exit(item2_seq)) < at(&shared, &format!("cleanup-start:{each_seq}")),
            "Item 退出后才清理 Each: {shared:?}"
        );
        assert!(
            at(&shared, &exit(each_seq)) < at(&shared, &format!("cleanup-start:{root_seq}")),
            "Each 退出后才开始 Root 清理: {shared:?}"
        );
        assert!(
            at(&shared, &format!("cleanup-start:{root_seq}"))
                < at(&shared, &format!("cleanup-end:{root_seq}")),
            "{shared:?}"
        );
        assert!(
            at(&shared, &format!("cleanup-end:{root_seq}")) < at(&shared, "frame-exit:root"),
            "{shared:?}"
        );
        assert!(
            at(&shared, "frame-exit:root") < at(&shared, "context-drop"),
            "{shared:?}"
        );
        assert!(
            at(&shared, "frame-exit:root") < at(&shared, "container-drop"),
            "{shared:?}"
        );
        // 借用结束 → 最深 Leaf 清理 → 逐层相邻。
        assert_eq!(count(&shared, "borrow-end"), 1, "{shared:?}");
        assert!(
            at(&shared, "borrow-end") < at(&shared, "cleanup-start:none"),
            "{shared:?}"
        );
        assert!(
            at(&shared, "cleanup-start:none") < at(&shared, "cleanup-end:none"),
            "{shared:?}"
        );
        assert!(
            at(&shared, "cleanup-end:none")
                < nth(&shared, &format!("frame-exit:leaf:{flow2_seq}"), 1),
            "{shared:?}"
        );

        // Ready 对照 + 另一 Execution 对照：地址与身份空间互不相同。
        reset_observations();
        let outcome =
            run_definition_plain(flow.definition(), vec![root_input(&collection, items())]);
        assert!(outcome.is_ok(), "{outcome:?}");
        let after_addresses = boundary_address_snapshot();
        assert_ne!(
            addresses[0].1, after_addresses[0].1,
            "另一 Execution 身份不同"
        );
        assert_ne!(
            addresses[0].2, after_addresses[0].2,
            "另一 Execution coordinator 不同"
        );
        assert_ne!(
            addresses[0].3, after_addresses[0].3,
            "另一 Execution container 不同"
        );
        let ready = take_shared_events();
        assert_eq!(count(&ready, "collector-after:2"), 1, "{ready:?}");
        assert_eq!(
            count(&ready, "out-dropped"),
            2,
            "成功运行最终由 Root 清理一次: {ready:?}"
        );
    }
}

#[test]
fn h24_each_success_then_parent_later_failure_cleans_final_vec_once() {
    reset_observations();
    let each = plain_each_only();
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
        .expect("each step");
    let after = parent
        .then::<_, SyncFnSig<(Vec<Out>,), Data<u32>>, _>(
            later_failing as fn(&Vec<Out>) -> Result<u32, BodyError>,
            collected.clone(),
        )
        .expect("later step");
    let flow = parent.finish::<Data<u32>, _>(after).expect("parent finish");

    let outcome = run_definition_plain(
        flow.definition(),
        vec![root_input(
            &collection,
            vec![Item { id: 10 }, Item { id: 20 }, Item { id: 30 }],
        )],
    );
    let error = outcome.expect_err("later step fails");
    assert_eq!(error.note(), "later step failure");

    let shared = take_shared_events();
    let each_seq = boundary_creation_snapshot()
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Each)
        .map(|(scope, _, _)| scope.seq())
        .expect("Each created");
    assert_eq!(
        count(&shared, "collector-after:3"),
        1,
        "Each 已成功收口: {shared:?}"
    );
    assert_eq!(count(&shared, "later-failing"), 1, "{shared:?}");
    assert_eq!(
        count(&shared, "temp-dropped"),
        3,
        "每项临时值清理一次: {shared:?}"
    );
    assert_eq!(
        count(&shared, "out-dropped"),
        3,
        "最终 Vec 由 parent 清理一次: {shared:?}"
    );
    assert!(
        at(&shared, "collector-after:3") < at(&shared, "later-failing"),
        "Each 成功先于父后步失败: {shared:?}"
    );
    assert!(
        at(&shared, "later-failing") < nth(&shared, "out-dropped", 0),
        "parent 在失败后清理最终 Vec: {shared:?}"
    );
    assert!(
        saw(&shared, &format!("frame-exit:boundary:{each_seq}")),
        "Each child 已正常退出: {shared:?}"
    );
    assert_eq!(
        count(&shared, &format!("cleanup-start:{each_seq}")),
        0,
        "Each child 不重复处理 collector: {shared:?}"
    );
}

#[test]
fn h20_real_runner_prepare_rejection_keeps_primary_and_cleanup_failure() {
    // R3：经**真实 Each Item runner**注入后置前提故障（在真实 Consume 入口前销毁被选输出），
    // 验证：实际保存的首错、清理错误及其 Scope 精确可核；prepare 拒绝到内部清理之间的完整
    // 快照不变；两条 guard 责任分支、后续项停止与 ancestor 不受影响。
    reset_observations();
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let mut each_builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
            only_out as fn(&Item) -> Result<Out, BodyError>,
        )
        .expect("body");
    let each: Each<EachOnly<Item>, Out> = each_builder.finish().expect("each finish");
    let wrapper_output = each.wrapper_definition().output_ports()[0]
        .position()
        .clone();
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected)
        .expect("finish");

    super::each::install_pre_consume_fault(super::each::PreConsumeFault::Selected);
    let outcome = drive(super::test_support::definition_in_root::<
        fn(&mut ExecutionContext, &ScopeId),
        fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
    >(
        flow.definition(),
        vec![root_input(
            &collection,
            vec![Item { id: 1 }, Item { id: 2 }, Item { id: 3 }],
        )],
        |_, _| {},
        None,
    ));
    let primary = outcome.expect_err("后置前提故障必须让 Each 失败");

    // 首错：精确变体与字段（被销毁的输出位置就是包装声明输出），并定位到实际失败的 ItemScope。
    let item_scope = boundary_creation_snapshot()
        .into_iter()
        .find(|(_, _, role)| *role == ScopeRole::Item)
        .map(|(child, _, _)| child)
        .expect("ItemScope recorded");
    let expected_primary = format!(
        "item-primary:{:?}",
        ScopeError::TargetNotAlive {
            position: wrapper_output.clone()
        }
    );
    let shared = take_shared_events();
    assert!(saw(&shared, &expected_primary), "首错精确匹配: {shared:?}");
    assert!(
        matches!(
            primary.scope_error(),
            Some(ScopeError::TargetNotAlive { position }) if position == &wrapper_output
        ),
        "返回的首错: {primary:?}"
    );
    assert!(
        saw(&shared, &format!("item-failure-scope:{}", item_scope.seq())),
        "首错定位到实际失败的 ItemScope: {shared:?}"
    );
    // Context **实际保存**的首次终止（独立只读通道）：类别、定位 Scope、说明与原始
    // ScopeError 精确匹配；改错 `failed_with` 保存的内容会被此断言拒绝。
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "首次终止只保存一次: {saved:?}");
    let saved = &saved[0];
    assert_eq!(saved.kind, TerminationKind::BodyError, "{saved:?}");
    assert_eq!(
        saved.scope.as_ref(),
        Some(&item_scope),
        "定位到该 ItemScope: {saved:?}"
    );
    assert_eq!(saved.note, "scope operation failed", "{saved:?}");
    assert!(
        matches!(
            &saved.scope_error,
            Some(ScopeError::TargetNotAlive { position }) if position == &wrapper_output
        ),
        "实际保存的首错（Selected）: {saved:?}"
    );
    // 清理错误：从 Context 实际保存的 `cleanup_report()` 回读；Scope 必须是该 ItemScope，
    // 内容必须精确等于清理失败本身，且不覆盖首错。
    let expected_cleanup = format!(
        "item-cleanup-report:CleanupDiagnostic {{ scope: {item_scope:?}, error: Invariant {{ violated: \"owned entry must exist until its scope closes\" }} }}"
    );
    assert!(
        saw(&shared, &expected_cleanup),
        "清理错误与其 Scope 精确匹配: {shared:?}"
    );
    assert_eq!(count(&shared, "item-guard:open"), 1, "{shared:?}");
    assert!(saw(&shared, "consume-cleanup-failure"), "{shared:?}");
    // 首个拒绝即停止：只建立一项、父后步不执行、collector 未移动。
    assert_eq!(count(&shared, "collector-before:0"), 1, "{shared:?}");
    assert_eq!(count(&shared, "body:only-out"), 1, "{shared:?}");
    assert!(!saw(&shared, "after-each"), "{shared:?}");

    // 成对完整快照：Before 与 AfterReject 逐项一致（身份级），观察不吞错误。
    let snapshots = super::test_support::take_consume_pre_cleanup();
    assert_eq!(snapshots.len(), 2, "{snapshots:?}");
    let (before, reject) = (&snapshots[0], &snapshots[1]);
    for snapshot in [before, reject] {
        assert!(snapshot.observation_error.is_none(), "{snapshot:?}");
        assert!(snapshot.selected_owner.is_some(), "{snapshot:?}");
        assert_eq!(snapshot.collector_moves, Some(0), "{snapshot:?}");
    }
    assert_eq!(before.item_refs, reject.item_refs);
    assert_eq!(before.item_owned, reject.item_owned);
    assert_eq!(before.parent_refs, reject.parent_refs);
    assert_eq!(before.parent_owned, reject.parent_owned);
    assert_eq!(before.collector_owner, reject.collector_owner);
    assert_eq!(before.collector_element, reject.collector_element);
    assert_eq!(before.selected_data, reject.selected_data);
    assert_eq!(
        before.selected_owner, reject.selected_owner,
        "旧输出仍由 Item 负责"
    );
    assert!(
        before
            .item_owned
            .as_ref()
            .is_some_and(|owned| !owned.is_empty()),
        "Item 仍拥有被选输出: {before:?}"
    );

    // 对照：恒等 body Flow 的 prepare 拒绝会被内部清理关闭 Item，guard 走"已 Closed"分支。
    reset_observations();
    let (body, input) = FlowBuilder::<(Item,)>::start().expect("body");
    let identity: Flow<(Item,), Data<Item>> = body.finish(input).expect("identity finish");
    let (mut parent2, collection2) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let mut each_builder2: EachBuilder<EachOnly<Item>, Item> = EachBuilder::start().expect("each");
    each_builder2
        .then_body::<_, OrchSig<Item, Data<Item>>>(identity)
        .expect("identity body");
    let each2: Each<EachOnly<Item>, Item> = each_builder2.finish().expect("each finish");
    let collected2 = parent2
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Item>>>, _>(each2, collection2.clone())
        .expect("wire each");
    let flow2 = parent2
        .finish::<Data<Vec<Item>>, _>(collected2)
        .expect("finish");
    let failed = drive(super::test_support::definition_in_root::<
        fn(&mut ExecutionContext, &ScopeId),
        fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
    >(
        flow2.definition(),
        vec![root_input(&collection2, vec![Item { id: 9 }])],
        |_, _| {},
        None,
    ));
    assert!(
        failed.is_err(),
        "恒等 body 暴露 imported alias，Item 收口必须拒绝"
    );
    let shared = take_shared_events();
    assert_eq!(count(&shared, "item-guard:closed"), 1, "{shared:?}");
    assert_eq!(count(&shared, "consume-cleanup-failure"), 0, "{shared:?}");
}

/// 供 H20 使用的 body：产生新 owned 输出。
fn only_out(item: &Item) -> Result<Out, BodyError> {
    record("body:only-out");
    Ok(Out { key: item.id })
}

#[test]
fn h20_cleanup_precondition_failure_keeps_selected_alive_and_saves_dual_diagnostics() {
    // R3 第二场景：被选输出仍有效，但 Item 的 owned 集合中出现一个已不存在的 entry →
    // prepare 在"剩余 owned 的清理前提"处拒绝、随后 cleanup 同因失败：两份诊断都保存。
    reset_observations();
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let mut each_builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
            only_out as fn(&Item) -> Result<Out, BodyError>,
        )
        .expect("body");
    let each: Each<EachOnly<Item>, Out> = each_builder.finish().expect("each finish");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected)
        .expect("finish");

    super::each::install_pre_consume_fault(super::each::PreConsumeFault::OtherOwned);
    let outcome = drive(super::test_support::definition_in_root::<
        fn(&mut ExecutionContext, &ScopeId),
        fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
    >(
        flow.definition(),
        vec![root_input(
            &collection,
            vec![Item { id: 3 }, Item { id: 4 }],
        )],
        |_, _| {},
        None,
    ));
    let primary = outcome.expect_err("清理前提失败必须让 Each 失败");

    let item_scope = boundary_creation_snapshot()
        .into_iter()
        .find(|(_, _, role)| *role == ScopeRole::Item)
        .map(|(child, _, _)| child)
        .expect("ItemScope recorded");
    let expected_primary = format!(
        "item-primary:{:?}",
        ScopeError::Invariant {
            violated: "owned entry must exist until its scope closes"
        }
    );
    let shared = take_shared_events();
    assert!(saw(&shared, &expected_primary), "首错精确匹配: {shared:?}");
    assert!(
        matches!(
            primary.scope_error(),
            Some(ScopeError::Invariant {
                violated: "owned entry must exist until its scope closes"
            })
        ),
        "返回的首错也必须精确等于同一变体: {primary:?}"
    );
    assert!(
        saw(&shared, &format!("item-failure-scope:{}", item_scope.seq())),
        "首错定位到实际失败的 ItemScope: {shared:?}"
    );
    // Context **实际保存**的首次终止（独立只读通道）：类别、定位 Scope、说明与原始
    // ScopeError 精确匹配；改错 `failed_with` 保存的内容会被此断言拒绝。
    let saved = super::test_support::take_termination_saved();
    assert_eq!(saved.len(), 1, "首次终止只保存一次: {saved:?}");
    let saved = &saved[0];
    assert_eq!(saved.kind, TerminationKind::BodyError, "{saved:?}");
    assert_eq!(
        saved.scope.as_ref(),
        Some(&item_scope),
        "定位到该 ItemScope: {saved:?}"
    );
    assert_eq!(saved.note, "scope operation failed", "{saved:?}");
    assert!(
        matches!(
            &saved.scope_error,
            Some(ScopeError::Invariant {
                violated: "owned entry must exist until its scope closes"
            })
        ),
        "实际保存的首错（OtherOwned）: {saved:?}"
    );
    let expected_cleanup = format!(
        "item-cleanup-report:CleanupDiagnostic {{ scope: {item_scope:?}, error: Invariant {{ violated: \"owned entry must exist until its scope closes\" }} }}"
    );
    assert!(
        saw(&shared, &expected_cleanup),
        "清理错误与其 Scope 精确匹配: {shared:?}"
    );
    assert_eq!(count(&shared, "item-guard:open"), 1, "{shared:?}");
    assert!(saw(&shared, "consume-cleanup-failure"), "{shared:?}");
    assert_eq!(count(&shared, "body:only-out"), 1, "{shared:?}");

    // 成对完整快照：被选输出在拒绝前后都存活且仍由 Item 负责，两侧状态逐项不变。
    let snapshots = super::test_support::take_consume_pre_cleanup();
    assert_eq!(snapshots.len(), 2, "{snapshots:?}");
    let (before, reject) = (&snapshots[0], &snapshots[1]);
    for snapshot in [before, reject] {
        assert!(snapshot.observation_error.is_none(), "{snapshot:?}");
        assert!(snapshot.selected_alive, "被选输出仍有效: {snapshot:?}");
        assert!(snapshot.selected_owner.is_some(), "{snapshot:?}");
        assert_eq!(snapshot.collector_moves, Some(0), "{snapshot:?}");
    }
    assert_eq!(before.item_refs, reject.item_refs);
    assert_eq!(before.item_owned, reject.item_owned);
    assert_eq!(before.parent_refs, reject.parent_refs);
    assert_eq!(before.parent_owned, reject.parent_owned);
    assert_eq!(before.selected_data, reject.selected_data);
    assert!(
        before
            .item_owned
            .as_ref()
            .is_some_and(|owned| owned.len() >= 2),
        "Item 拥有被选输出与另一份 owned: {before:?}"
    );
}

#[test]
fn h21_finish_preconditions_are_rejected_through_the_real_each_path() {
    // R5：四类 finish 前置故障经**真实 Each finish 路径**触发；每例断言：无任何最终绑定、
    // finish 操作前后完整状态（身份级）与下一个 DataId 序号不变（故障自身的登记单独比较）、
    // 已收集值仍被清理一次。
    use super::each::FinishFault;

    let cases = [
        (FinishFault::NonActive, "non-active"),
        (FinishFault::AlreadyBound, "already-bound"),
        (FinishFault::LiveDescendant, "live-descendant"),
        (FinishFault::OwnerMismatch, "owner-mismatch"),
    ];
    for (fault, label) in cases {
        reset_observations();
        let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
        let mut each_builder: EachBuilder<EachOnly<Item>, Out> =
            EachBuilder::start().expect("each");
        each_builder
            .then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
                only_out as fn(&Item) -> Result<Out, BodyError>,
            )
            .expect("body");
        let each: Each<EachOnly<Item>, Out> = each_builder.finish().expect("each finish");
        let collected = parent
            .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
            .expect("wire each");
        let flow = parent
            .finish::<Data<Vec<Out>>, _>(collected)
            .expect("finish");

        super::each::install_finish_fault(fault);
        let outcome = drive(super::test_support::definition_in_root::<
            fn(&mut ExecutionContext, &ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            flow.definition(),
            vec![root_input(&collection, vec![Item { id: 5 }])],
            |_, _| {},
            None,
        ));
        let error = outcome.expect_err("finish 前置故障必须让 Each 失败");
        let expected = match fault {
            FinishFault::NonActive => matches!(
                error.scope_error(),
                Some(ScopeError::ScopeNotActive { .. }) | Some(ScopeError::ScopeClosed { .. })
            ),
            FinishFault::AlreadyBound => {
                matches!(
                    error.scope_error(),
                    Some(ScopeError::RefAlreadyBound { .. })
                )
            }
            FinishFault::LiveDescendant => {
                matches!(
                    error.scope_error(),
                    Some(ScopeError::ActiveDescendants { .. })
                )
            }
            FinishFault::OwnerMismatch => matches!(
                error.scope_error(),
                Some(ScopeError::CollectorNotOwnedBy { .. })
            ),
        };
        assert!(expected, "{label}: 真实 finish 路径拒绝: {error:?}");

        let shared = take_shared_events();
        assert_eq!(
            count(&shared, "collector-after:1"),
            1,
            "{label}: 项本身已消费: {shared:?}"
        );
        // 拒绝前无部分提交：不出现任何最终绑定（EachOnly 成功时真实最终 refs 为 2）。
        assert!(
            !saw_prefix(&shared, "each-finish-refs:"),
            "{label}: finish 失败不产生最终绑定: {shared:?}"
        );
        let pick = |prefix: &str| -> Option<String> {
            shared
                .iter()
                .find(|event| event.starts_with(prefix))
                .map(|event| event[prefix.len()..].to_string())
        };
        let before_state = pick("finish-before-state:").expect("finish 前状态已记录");
        let reject_state = pick("finish-reject-state:").expect("拒绝后状态已记录");
        let before_next = pick("finish-before-next-id:").expect("finish 前序号已记录");
        let reject_next = pick("finish-reject-next-id:").expect("拒绝后序号已记录");
        let (expected_refs, expected_owned) = match fault {
            // `AlreadyBound` 的故障自身在基线与前置检查之间登记一份值；基线已取在注入之后。
            FinishFault::AlreadyBound => ("finish-reject-refs:2", "finish-reject-owned:1"),
            _ => ("finish-reject-refs:1", "finish-reject-owned:0"),
        };
        assert!(
            saw(&shared, expected_refs),
            "{label}: {expected_refs}: {shared:?}"
        );
        assert!(
            saw(&shared, expected_owned),
            "{label}: {expected_owned}: {shared:?}"
        );
        assert!(
            saw(&shared, "finish-reject-moves:1"),
            "{label}: collector 未被提交: {shared:?}"
        );
        // 完整前后快照（身份级）与 DataId 序号：四类故障统一比较**相等**（基线已取在注入之后），
        // 因此 finish 操作自身的任何状态改动或额外 DataId 消耗都会被检出。
        assert_eq!(before_state, reject_state, "{label}: finish 操作不改状态");
        assert_eq!(
            before_next, reject_next,
            "{label}: finish 操作不消耗 DataId"
        );
        assert_eq!(
            count(&shared, "out-dropped"),
            1,
            "{label}: 已收集的值清理一次: {shared:?}"
        );
    }
}
