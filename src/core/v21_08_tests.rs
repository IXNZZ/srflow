//! V21-08 验收样本：Each 的 item cap、直接 Consume 与错误退出。

use super::builder::{Definition, TypedCallBuilder};
use super::context::BodyError;
use super::each::{Each, EachBuilder, EachOnly, EachShared};
use super::flow::{Flow, FlowBuilder};
use super::orchestrator::{OrchCall, ScopeRole};
use super::signature::{AsyncFnSig, Data, OrchSig, SyncFnSig};
use super::test_support::{
    RootView, advance_to_pending, at, boundary_address_snapshot, boundary_creation_snapshot, count,
    drive, gate_wait, install_gate, nth, record, release_gate, reset_observations, root_input,
    run_definition_in_root, run_definition_plain, saw, saw_prefix, take_events, take_shared_events,
};

/// 非 Clone 业务集合元素（Drop 见证用于 H01 的输入清理一次）。
#[derive(Debug, PartialEq, Eq)]
struct Item {
    id: u32,
}

impl Drop for Item {
    fn drop(&mut self) {
        record("h01-item-dropped");
    }
}

/// 非 Clone shared Data。
#[derive(Debug, PartialEq, Eq)]
struct Rules {
    weight: u32,
}

/// body 前置临时值：Drop 见证属于本项的临时数据清理。
#[derive(Debug)]
struct Temp(#[allow(dead_code)] u32);

impl Drop for Temp {
    fn drop(&mut self) {
        record("temp-dropped");
    }
}

/// 每项 body 输出（Drop 见证用于 H01 的结果清理一次）。
#[derive(Debug, PartialEq, Eq)]
struct Out {
    /// 结果值。
    key: u32,
    /// 在 body 内观察到的 item 实际地址（证明借用来自原集合元素本身）。
    address: usize,
    /// body 内观察到的 shared 值。
    weight: u32,
}

impl Drop for Out {
    fn drop(&mut self) {
        record("h01-out-dropped");
    }
}

fn make_temp(item: &Item) -> Result<Temp, BodyError> {
    record("temp-made");
    Ok(Temp(item.id))
}

async fn probe_item(item: &Item, rules: &Rules) -> Result<Out, BodyError> {
    record("body-enter");
    let address = item as *const Item as usize;
    gate_wait().await;
    record("body-resumed");
    Ok(Out {
        key: item.id + rules.weight,
        address,
        weight: rules.weight,
    })
}

#[allow(clippy::ptr_arg)] // Node 输入类型就是 `Vec<Item>`，必须保持该声明形态
fn count_items(items: &Vec<Item>) -> Result<u32, BodyError> {
    Ok(items.len() as u32)
}

#[allow(clippy::ptr_arg)] // 同上：Node 输入类型是 `Vec<Out>`
fn after_each(results: &Vec<Out>) -> Result<u32, BodyError> {
    record("after-each");
    Ok(results.iter().map(|out| out.key).sum())
}

#[test]
fn h01_each_collects_owned_outputs_in_order() {
    reset_observations();
    // Root 声明输入：非 Clone 集合与 shared；父 Flow 先经 Node 产生中间 Data，再调用 Each。
    let (mut parent, (collection, rules)) =
        FlowBuilder::<(Vec<Item>, Rules)>::start().expect("parent");
    let _intermediate = parent
        .then::<_, SyncFnSig<(Vec<Item>,), Data<u32>>, _>(
            count_items as fn(&Vec<Item>) -> Result<u32, BodyError>,
            collection.clone(),
        )
        .expect("intermediate");

    // body：完成态 Flow（前置临时值 + 异步 Node）。
    let (mut body, (item_ref, rules_ref)) = FlowBuilder::<(Item, Rules)>::start().expect("body");
    let _temp = body
        .then::<_, SyncFnSig<(Item,), Data<Temp>>, _>(
            make_temp as fn(&Item) -> Result<Temp, BodyError>,
            item_ref.clone(),
        )
        .expect("temp");
    let body_out = body
        .then::<_, AsyncFnSig<(Item, Rules), Data<Out>>, _>(
            probe_item,
            (item_ref.clone(), rules_ref.clone()),
        )
        .expect("probe");
    let body_flow: Flow<(Item, Rules), Data<Out>> = body.finish(body_out).expect("body finish");

    // Each：集合 + shared；body 是完成态 Flow。
    let mut each_builder: EachBuilder<EachShared<Item, Rules>, Out> =
        EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, OrchSig<(Item, Rules), Data<Out>>>(body_flow)
        .expect("each body");
    let each: Each<EachShared<Item, Rules>, Out> = each_builder.finish().expect("each finish");

    let each_collection_port = each.definition().inputs()[0].position().clone();
    let item_input_port = each.wrapper_definition().inputs()[0].position().clone();
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

    let collection_position = collection.position().clone();
    let collected_position = collected.position().clone();
    let originals = vec![Item { id: 10 }, Item { id: 20 }, Item { id: 30 }];
    let expected_addresses: Vec<usize> = originals
        .iter()
        .map(|item| item as *const Item as usize)
        .collect();

    let counts_before = super::context::creation_counts::snapshot();
    install_gate();
    let each_collection_port = each_collection_port.clone();
    let item_input_port = item_input_port.clone();
    let boxed = advance_to_pending(
        super::test_support::definition_in_root(
            flow.definition(),
            vec![
                root_input(&collection, originals),
                root_input(&rules, Rules { weight: 7 }),
            ],
            |_: &mut super::context::ExecutionContext, _: &super::identity::ScopeId| {},
            Some(move |view: &mut RootView<'_, '_>| {
                let before_snapshot = view.snapshot_full()?;
                // 子 Scope 已关闭：本地引用随之失效，不能再 resolve。
                let records = boundary_creation_snapshot();
                let each_scope = records
                    .iter()
                    .find(|(_, _, role)| *role == ScopeRole::Each)
                    .map(|(child, _, _)| child.clone())
                    .expect("EachScope recorded");
                assert_eq!(
                    view.probe().state(&each_scope)?,
                    super::scope::ScopeState::Closed,
                    "EachScope 已关闭"
                );
                assert!(
                    matches!(
                        view.probe()
                            .resolve::<Vec<Item>>(&each_scope, &each_collection_port),
                        Err(super::internal_error::ScopeError::ScopeClosed { .. })
                    ),
                    "关闭后本地引用不再可 resolve（EachScope）"
                );
                for (child, _, role) in &records {
                    if *role != ScopeRole::Item {
                        continue;
                    }
                    assert_eq!(
                        view.probe().state(child)?,
                        super::scope::ScopeState::Closed,
                        "ItemScope 已关闭"
                    );
                    assert!(
                        matches!(
                            view.probe().resolve::<Item>(child, &item_input_port),
                            Err(super::internal_error::ScopeError::ScopeClosed { .. })
                        ),
                        "关闭后本地引用不再可 resolve（ItemScope）"
                    );
                }
                let values = view.resolve::<Vec<Out>>(&collected_position)?;
                assert_eq!(values.len(), 3, "三项都已完成");
                assert_eq!(
                    values.iter().map(|out| out.key).collect::<Vec<_>>(),
                    vec![17, 27, 37],
                    "结果按输入顺序"
                );
                assert_eq!(
                    values.iter().map(|out| out.address).collect::<Vec<_>>(),
                    expected_addresses,
                    "每项借用都来自原集合元素本身"
                );
                assert_eq!(
                    values.iter().map(|out| out.weight).collect::<Vec<_>>(),
                    vec![7, 7, 7]
                );
                let items = view.resolve::<Vec<Item>>(&collection_position)?;
                assert_eq!(items.len(), 3, "原集合未被移走");
                assert_eq!(
                    view.snapshot_full()?,
                    before_snapshot,
                    "Root 观察前后完整 refs／owned 不变"
                );
                assert_eq!(
                    count(&take_events(), "temp-dropped"),
                    3,
                    "每项临时值清理一次"
                );
                Ok(())
            }),
        ),
        1,
    );
    // 第一项停在真实 body 借用处：后续项与父后步都还没有发生。
    {
        let shared = take_shared_events();
        let creations = boundary_creation_snapshot();
        let item_creations = creations
            .iter()
            .filter(|(_, _, role)| *role == ScopeRole::Item)
            .count();
        assert!(
            !saw(&shared, "after-each") && !saw_prefix(&shared, "cleanup-start"),
            "第一项 Pending 时父后步与清理都未发生: {shared:?}"
        );
        assert!(take_events().iter().all(|event| event != "after-each"));
        assert_eq!(item_creations, 1, "第一项 Pending 时只建立了一个 ItemScope");
    }
    release_gate();
    drive(boxed).expect("each run");
    let shared = take_shared_events();
    let creation_roles: Vec<ScopeRole> = boundary_creation_snapshot()
        .into_iter()
        .map(|(_, _, role)| role)
        .collect();
    assert_eq!(
        creation_roles,
        vec![
            ScopeRole::Each,
            ScopeRole::Item,
            ScopeRole::Flow,
            ScopeRole::Item,
            ScopeRole::Flow,
            ScopeRole::Item,
            ScopeRole::Flow
        ],
        "EachScope 一次、每项 ItemScope 一次、每项 body child 一次: {creation_roles:?}"
    );
    // 真实 parent 链与唯一执行域证据。
    let records = boundary_creation_snapshot();
    let each_scope = records
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Each)
        .map(|(child, _, _)| child.clone())
        .expect("EachScope recorded");
    let item_records: Vec<(super::identity::ScopeId, super::identity::ScopeId)> = records
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Item)
        .map(|(child, parent, _)| (child.clone(), parent.clone()))
        .collect();
    assert_eq!(item_records.len(), 3);
    assert!(
        item_records.iter().all(|(_, parent)| parent == &each_scope),
        "ItemScope parent 必须是本次 EachScope: {item_records:?}"
    );
    let flow_records: Vec<(super::identity::ScopeId, super::identity::ScopeId)> = records
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Flow)
        .map(|(child, parent, _)| (child.clone(), parent.clone()))
        .collect();
    assert_eq!(flow_records.len(), 3, "每项一个 body child");
    for (child, parent) in &flow_records {
        assert!(
            item_records.iter().any(|(item, _)| item == parent),
            "body child 的 parent 必须是某个 ItemScope: {parent}"
        );
        assert!(
            item_records.iter().all(|(item, _)| item != child),
            "body child 不是 ItemScope 本身: {child}"
        );
    }
    let counts_after = super::context::creation_counts::snapshot();
    assert_eq!(
        counts_after,
        (
            counts_before.0 + 1,
            counts_before.1 + 1,
            counts_before.2 + 1
        ),
        "同一 Execution 只新增一份 Context／Coordinator／Container"
    );
    let addresses = boundary_address_snapshot();
    assert!(!addresses.is_empty(), "真实调用点地址已记录");
    let (identity, coordinator, container) = {
        let (_, identity, coordinator, container) = addresses[0];
        (identity, coordinator, container)
    };
    assert!(
        addresses
            .iter()
            .all(|(_, i, c, k)| { *i == identity && *c == coordinator && *k == container }),
        "唯一 Execution 身份／Coordinator／Container: {addresses:?}"
    );
    for (child, _, _) in &records {
        assert!(
            shared
                .iter()
                .any(|event| event.starts_with(&format!("frame-exit:boundary:{}", child.seq()))),
            "子 Scope 退出后本地引用失效: {child}"
        );
    }
    let events = take_events();
    assert_eq!(
        count(&events, "h01-item-dropped"),
        3,
        "集合元素清理一次: {events:?}"
    );
    assert_eq!(
        count(&events, "h01-out-dropped"),
        3,
        "结果清理一次: {events:?}"
    );
    assert_eq!(
        count(&shared, "collector-after:1"),
        1,
        "第一项消费后 collector 长度为一: {shared:?}"
    );
    assert_eq!(
        count(&shared, "collector-after:2"),
        1,
        "第二项消费后长度递增: {shared:?}"
    );
    assert_eq!(
        count(&shared, "collector-after:3"),
        1,
        "第三项消费后长度递增: {shared:?}"
    );
    assert!(
        nth(&shared, "body-resumed", 2) < at(&shared, "after-each"),
        "全部项完成后父后步才执行: {shared:?}"
    );
    assert!(at(&shared, "after-each") < at(&shared, "frame-exit:root"));
}

#[test]
fn h08_definition_is_stable_and_registration_tamper_is_rejected() {
    reset_observations();
    let (mut parent, (collection, rules)) =
        FlowBuilder::<(Vec<Item>, Rules)>::start().expect("parent");
    let (mut body, (item_ref, rules_ref)) = FlowBuilder::<(Item, Rules)>::start().expect("body");
    let body_out = body
        .then::<_, AsyncFnSig<(Item, Rules), Data<Out>>, _>(
            probe_item,
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

    // 运行前后：不分配 Definition RefId（在包装内以事件观察，见 each.rs 的 wrapper-allocated）。
    let collected = parent
        .then::<_, OrchSig<(Vec<Item>, Rules), Data<Vec<Out>>>, _>(
            each,
            (collection.clone(), rules.clone()),
        )
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("finish");

    let outcome = run_definition_in_root(
        flow.definition(),
        vec![
            root_input(&collection, vec![Item { id: 1 }, Item { id: 2 }]),
            root_input(&rules, Rules { weight: 0 }),
        ],
        |view| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            assert_eq!(values.len(), 2);
            Ok(())
        },
    );
    outcome.expect("run");
    let shared = take_shared_events();
    // 同一完成态 Definition 可以在第二个 Execution 上复用并重新计算。
    let second = run_definition_in_root(
        flow.definition(),
        vec![
            root_input(
                &collection,
                vec![Item { id: 7 }, Item { id: 8 }, Item { id: 9 }],
            ),
            root_input(&rules, Rules { weight: 1 }),
        ],
        |view| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            assert_eq!(values.len(), 3, "复用 Definition 重新计算");
            Ok(())
        },
    );
    second.expect("second run");
    let shared = {
        let mut all = shared;
        all.extend(take_shared_events());
        all
    };
    // 执行期不分配 Definition RefId：包装 Definition 的分配计数在每次运行内前后一致。
    let allocated: Vec<u64> = shared
        .iter()
        .filter_map(|event| event.strip_prefix("wrapper-allocated:"))
        .filter_map(|value| value.parse().ok())
        .collect();
    let allocated_end: Vec<u64> = shared
        .iter()
        .filter_map(|event| event.strip_prefix("wrapper-allocated-end:"))
        .filter_map(|value| value.parse().ok())
        .collect();
    assert_eq!(allocated.len(), 2, "两次运行各观察一次: {shared:?}");
    assert_eq!(
        allocated, allocated_end,
        "运行期不分配包装 RefId: {shared:?}"
    );
    // EachScope 的控制元数据不随 item 增长：refs 恒为「集合 + shared + 最终输出」= 3，
    // ordinary owned 在 finish 之前始终为 0。
    assert_eq!(
        count(&shared, "each-refs:2"),
        5,
        "每项后 EachScope refs 不变: {shared:?}"
    );
    assert!(
        !saw(&shared, "each-refs:3"),
        "item 结果不新增 ref: {shared:?}"
    );
    assert_eq!(
        count(&shared, "each-owned:0"),
        5,
        "finish 前 ordinary owned 为空: {shared:?}"
    );
    // 每次运行在全部项完成后只发生一次最终绑定：refs 2→3，owned 0→1。
    assert_eq!(
        count(&shared, "each-finish-refs:3"),
        2,
        "最终输出位置每次运行只绑定一次: {shared:?}"
    );
    assert_eq!(
        count(&shared, "each-finish-owned:1"),
        2,
        "最终 Vec 每次运行只登记一次: {shared:?}"
    );
}

#[test]
fn h08_tampered_wrapper_ports_are_rejected_at_run() {
    reset_observations();
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let mut each_builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, SyncFnSig<(Item,), Data<Out>>>(|item: &Item| {
            Ok(Out {
                key: item.id,
                address: 0,
                weight: 0,
            })
        })
        .expect("body");
    // 测试注入：登记端口快照被替换为空表，运行期必须按真实登记身份拒绝。
    each_builder.tamper_wrapper_outputs_probe(Vec::new());
    let each: Each<EachOnly<Item>, Out> = each_builder.finish().expect("each finish");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected)
        .expect("finish");

    let result = run_definition_plain(
        flow.definition(),
        vec![root_input(&collection, vec![Item { id: 1 }])],
    );
    let error = result.expect_err("tampered registration must be rejected");
    assert!(
        format!("{error:?}")
            .contains("registered each body outputs do not match the registered wrapper"),
        "actual: {error:?}"
    );
    let shared = take_shared_events();
    assert!(
        !saw(&shared, "collector-after:1"),
        "被拒绝的运行不产生消费: {shared:?}"
    );
}

/// H15 正例用 body：Node 持真实 item 借用跨 await 读取（借用结束后由 Item 收口 Consume）。
async fn borrow_then_read(item: &Item) -> Result<Out, BodyError> {
    let address = item as *const Item as usize;
    record("borrow-held");
    gate_wait().await;
    record("borrow-read");
    let out = Out {
        key: item.id,
        address,
        weight: 0,
    };
    // 借用到此结束；Item 收口的 Consume 必须发生在其后（同一条共享序列内比较）。
    record("borrow-end");
    Ok(out)
}

#[test]
fn h15_item_borrow_is_read_across_await_and_mutation_happens_after_it_ends() {
    reset_observations();
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let mut each_builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, AsyncFnSig<(Item,), Data<Out>>>(borrow_then_read)
        .expect("body");
    let each: Each<EachOnly<Item>, Out> = each_builder.finish().expect("each finish");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("finish");

    let originals = vec![Item { id: 41 }, Item { id: 42 }];
    let expected_addresses: Vec<usize> = originals
        .iter()
        .map(|item| item as *const Item as usize)
        .collect();
    // 停在真实借用处，然后在同一 Future 上继续：借用结束后 Item 收口才做 Consume。
    install_gate();
    let boxed = advance_to_pending(
        super::test_support::definition_in_root::<
            fn(&mut super::context::ExecutionContext, &super::identity::ScopeId),
            fn(&mut RootView<'_, '_>) -> Result<(), BodyError>,
        >(
            flow.definition(),
            vec![root_input(&collection, originals)],
            |_, _| {},
            None,
        ),
        1,
    );
    release_gate();
    drive(boxed).expect("run");
    let shared = take_shared_events();
    let events = take_events();
    assert_eq!(count(&events, "borrow-held"), 2, "两项都持借用: {events:?}");
    assert_eq!(
        count(&events, "borrow-read"),
        2,
        "借用跨 await 后仍可读取: {events:?}"
    );
    assert_eq!(
        count(&shared, "borrow-end"),
        2,
        "两项借用各自结束: {shared:?}"
    );
    assert!(
        at(&shared, "borrow-end") < at(&shared, "collector-after:1"),
        "借用结束先于 Item 收口的 Consume: {shared:?}"
    );
    assert_eq!(count(&shared, "collector-after:1"), 1, "{shared:?}");
    assert_eq!(count(&shared, "collector-after:2"), 1, "{shared:?}");
    let observed = run_definition_in_root(
        flow.definition(),
        vec![root_input(
            &collection,
            vec![Item { id: 41 }, Item { id: 42 }],
        )],
        |view| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            assert_eq!(values.len(), 2);
            assert!(
                values.iter().all(|out| out.address != 0),
                "地址来自真实元素"
            );
            Ok(())
        },
    );
    observed.expect("replay");
    let _ = expected_addresses;
}

#[test]
fn h14_broken_item_input_is_rejected_in_the_real_pack_path_before_the_body() {
    reset_observations();
    // body 是完成态 Flow：其 Step 是真实 Orchestrator 调用点（带 erased pack）。
    let (mut body, item_ref) = FlowBuilder::<(Item,)>::start().expect("body");
    let out = body
        .then::<_, SyncFnSig<(Item,), Data<Out>>, _>(
            mark_item as fn(&Item) -> Result<Out, BodyError>,
            item_ref,
        )
        .expect("body step");
    let body_flow: Flow<(Item,), Data<Out>> = body.finish(out).expect("body finish");

    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let mut each_builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, OrchSig<Item, Data<Out>>>(body_flow)
        .expect("each body");
    let each: Each<EachOnly<Item>, Out> = each_builder.finish().expect("each finish");

    // 坏 item 输入：把 body 调用点的输入 pack 换成指向不存在位置的同类 pack；真实 pack
    // 校验必须在 body 之前拒绝（body=0）。
    let bogus = super::ref_id::RefIdAllocator::new(super::ref_id::RefIdSource::new())
        .allocate()
        .expect("bogus position");
    match each.wrapper_definition().steps()[0].site() {
        super::builder::CallSite::Orchestrator(site) => {
            site.inject_pack_probe(Box::new(super::orchestrator::probe_targets1::<Item>(bogus)));
        }
        super::builder::CallSite::Node(_) => panic!("Flow body must be an orchestrator call site"),
    }
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected)
        .expect("finish");
    let outcome = run_definition_plain(
        flow.definition(),
        vec![root_input(&collection, vec![Item { id: 1 }])],
    );
    assert!(outcome.is_err(), "坏 item 输入必须在 body 前拒绝");
    let shared = take_shared_events();
    assert_eq!(
        count(&shared, "body:mark-item"),
        0,
        "body 未执行: {shared:?}"
    );
    assert!(!saw(&shared, "collector-after:1"), "未发生消费: {shared:?}");
}

fn mark_item(item: &Item) -> Result<Out, BodyError> {
    record("body:mark-item");
    Ok(Out {
        key: item.id,
        address: 0,
        weight: 0,
    })
}

/// R1 负例用 body：普通 Orchestrator 声明自己的输入／输出，却试图把收到的 Scope 交给
/// 另一个 Each 的会话。
struct ForeignEachBody {
    definition: Definition,
    foreign: Each<EachOnly<Item>, Out>,
}

impl OrchCall<(Vec<Item>,), Data<Vec<Out>>> for ForeignEachBody {
    type Pack = super::orchestrator::Targets1<Vec<Item>>;

    fn definition(&self) -> &Definition {
        &self.definition
    }

    fn run<'a>(
        &'a self,
        scope: super::orchestrator::OrchScope<'a, Self::Pack, Data<Vec<Out>>>,
    ) -> super::signature::NodeFut<'a, ()> {
        Box::pin(async move {
            // 形状匹配允许调用转交，但本次实际 Definition 不是该 foreign Each：
            // 必须在 collector／Item／body 之前拒绝。
            let session: super::each::EachSession<'_, EachOnly<Item>, Out> =
                super::orchestrator::EachScopeTransfer::begin_each_session(scope, &self.foreign)?;
            let _ = session;
            Ok(())
        })
    }
}

#[test]
fn h26_foreign_each_scope_is_rejected_before_collector_item_or_body() {
    reset_observations();
    // foreign Each：body 会记录事件；若被误用就会执行。
    let mut foreign_builder: EachBuilder<EachOnly<Item>, Out> =
        EachBuilder::start().expect("foreign each");
    foreign_builder
        .then_body::<_, SyncFnSig<(Item,), Data<Out>>>(|item: &Item| {
            record("foreign-body");
            Ok(Out {
                key: item.id,
                address: 0,
                weight: 0,
            })
        })
        .expect("foreign body");
    let foreign: Each<EachOnly<Item>, Out> = foreign_builder.finish().expect("foreign finish");

    // 本地普通 Orchestrator：自身的输入／输出 Declaration 与 Each 形状相同。
    let mut definition = Definition::new();
    definition
        .declare_input::<Vec<Item>>("collection")
        .expect("input");
    definition
        .declare_output_port::<Vec<Out>>("out")
        .expect("output");
    let body = ForeignEachBody {
        definition,
        foreign,
    };

    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let produced = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(body, collection.clone())
        .expect("wire foreign body");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(produced)
        .expect("finish");

    let outcome = run_definition_plain(
        flow.definition(),
        vec![root_input(&collection, vec![Item { id: 1 }])],
    );
    let error = outcome.expect_err("foreign Each 会话必须被拒绝");
    assert!(
        format!("{error:?}")
            .contains("each session requires the running definition to be the each orchestrator"),
        "actual: {error:?}"
    );
    let shared = take_shared_events();
    assert_eq!(
        count(&shared, "foreign-body"),
        0,
        "foreign Node 未执行: {shared:?}"
    );
    assert_eq!(
        boundary_creation_snapshot()
            .iter()
            .filter(|(_, _, role)| *role == ScopeRole::Item)
            .count(),
        0,
        "不建立 ItemScope: {shared:?}"
    );
    assert!(
        !saw(&shared, "collector-before:0"),
        "不建立 collector／不消费: {shared:?}"
    );
}

#[test]
fn h14_broken_item_metadata_is_rejected_on_the_real_node_input_path() {
    // R5/R14 补充：保持 ItemScope 的 item 输入位置已绑定，只损坏 CollectionItem 元数据
    // （元素声明类型／下标），经真实 Node 输入检查在 body 之前拒绝。
    for (fault, expect_invariant) in [
        (super::each::ItemMetadataFault::ElementType, false),
        (super::each::ItemMetadataFault::Index, true),
    ] {
        reset_observations();
        let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
        let mut each_builder: EachBuilder<EachOnly<Item>, Out> =
            EachBuilder::start().expect("each");
        each_builder
            .then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
                mark_item as fn(&Item) -> Result<Out, BodyError>,
            )
            .expect("body");
        let each: Each<EachOnly<Item>, Out> = each_builder.finish().expect("each finish");
        let collected = parent
            .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
            .expect("wire each");
        let flow = parent
            .finish::<Data<Vec<Out>>, _>(collected)
            .expect("finish");

        super::each::install_item_metadata_fault(fault);
        let outcome = run_definition_plain(
            flow.definition(),
            vec![root_input(&collection, vec![Item { id: 1 }])],
        );
        let error = outcome.expect_err("坏 item 元数据必须在 body 前拒绝");
        let mismatch = matches!(
            error.scope_error(),
            Some(super::internal_error::ScopeError::TypeMismatch { .. })
                | Some(super::internal_error::ScopeError::ItemIndexOutOfRange { .. })
        );
        assert!(
            mismatch || (expect_invariant && format!("{error:?}").contains("Invariant")),
            "具体诊断: {error:?}"
        );
        let shared = take_shared_events();
        assert_eq!(
            count(&shared, "body:mark-item"),
            0,
            "body 未执行（body=0）: {shared:?}"
        );
        assert!(
            !saw(&shared, "collector-before:0"),
            "未进入收口: {shared:?}"
        );
    }
}
