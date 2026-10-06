//! V21-08 形状／复用切片（H02～H05、H07）。
//!
//! 覆盖：两种输入形状（无 shared／一个 shared）× 四种 body 种类（同步函数、异步函数、
//! 具体结构体 Node、`Arc<具体 Node>`）；完成态 Flow body 的真实 child Export／直接
//! Consume 与底层 Step 执行；空集合／单项的建立与绑定计数；tuple 业务 Data 与 `()` 禁止；
//! 同一 Each 的多调用位置／多 Execution 复用与 Arc 配置共享。

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use super::builder::TypedCallBuilder;
use super::context::BodyError;
use super::each::{Each, EachBuilder, EachOnly, EachShared};
use super::flow::{Flow, FlowBuilder};
use super::identity::{DataId, ScopeId};
use super::node::{NodeCall1, NodeCall2};
use super::orchestrator::{OrchCall, ScopeRole};
use super::ref_id::RefId;
use super::signature::{
    ArcNodeSig, AsyncFnSig, BuildError, Data, NodeFut, NodeSig, OrchSig, Out2, SyncFnSig,
};
use super::test_support::{
    RootView, at, boundary_creation_snapshot, count, record, reset_observations, root_input,
    run_definition_in_root, saw, take_events, take_shared_events,
};

/// 非 Clone 业务集合元素：R7 下只按引用消费，不得隐式 Clone。
#[derive(Debug, PartialEq, Eq)]
struct Item {
    id: u32,
}

/// 非 Clone shared Data：同样只按引用 Import。
#[derive(Debug, PartialEq, Eq)]
struct Rules {
    weight: u32,
}

/// 每项输出：`key` 是业务结果，`address` 见证借用来自原集合元素本身。
#[derive(Debug, PartialEq, Eq, Clone)]
struct Out {
    key: u32,
    address: usize,
}

fn item_address(item: &Item) -> usize {
    item as *const Item as usize
}

// ---------------------------------------------------------------- body 夹具

fn only_sync(item: &Item) -> Result<Out, BodyError> {
    record("body:only-sync");
    Ok(Out {
        key: item.id,
        address: item_address(item),
    })
}

async fn only_async(item: &Item) -> Result<Out, BodyError> {
    record("body:only-async");
    Ok(Out {
        key: item.id,
        address: item_address(item),
    })
}

struct OnlyStruct;

impl NodeCall1<Item, Data<Out>> for OnlyStruct {
    fn call<'a>(&'a self, item: &'a Item) -> NodeFut<'a, Out> {
        Box::pin(async move {
            record("body:only-struct");
            Ok(Out {
                key: item.id,
                address: item_address(item),
            })
        })
    }
}

/// Arc 共享 Node：配置（调用计数）跨 item／跨 Execution 共享，输出 Data 每项新建。
struct OnlyArcNode {
    calls: Cell<u32>,
}

impl NodeCall1<Item, Data<Out>> for OnlyArcNode {
    fn call<'a>(&'a self, item: &'a Item) -> NodeFut<'a, Out> {
        Box::pin(async move {
            let call = self.calls.get() + 1;
            self.calls.set(call);
            record("body:only-arc");
            Ok(Out {
                key: item.id + call * 10,
                address: item_address(item),
            })
        })
    }
}

fn shared_sync(item: &Item, rules: &Rules) -> Result<Out, BodyError> {
    record("body:shared-sync");
    Ok(Out {
        key: item.id + rules.weight,
        address: item_address(item),
    })
}

async fn shared_async(item: &Item, rules: &Rules) -> Result<Out, BodyError> {
    record("body:shared-async");
    Ok(Out {
        key: item.id + rules.weight,
        address: item_address(item),
    })
}

struct SharedStruct;

impl NodeCall2<Item, Rules, Data<Out>> for SharedStruct {
    fn call<'a>(&'a self, item: &'a Item, rules: &'a Rules) -> NodeFut<'a, Out> {
        Box::pin(async move {
            record("body:shared-struct");
            Ok(Out {
                key: item.id + rules.weight,
                address: item_address(item),
            })
        })
    }
}

struct SharedArcNode;

impl NodeCall2<Item, Rules, Data<Out>> for SharedArcNode {
    fn call<'a>(&'a self, item: &'a Item, rules: &'a Rules) -> NodeFut<'a, Out> {
        Box::pin(async move {
            record("body:shared-arc");
            Ok(Out {
                key: item.id + rules.weight,
                address: item_address(item),
            })
        })
    }
}

/// 完成态 Flow body 的中间 Step：产出 tuple Data，证明 Each 驱动底层 Flow 的全部 Step。
fn tag_of(item: &Item) -> Result<(u32, usize), BodyError> {
    record("body:flow-step-1");
    Ok((item.id, item_address(item)))
}

fn wrap_tag(tag: &(u32, usize)) -> Result<Out, BodyError> {
    record("body:flow-step-2");
    Ok(Out {
        key: tag.0 * 2,
        address: tag.1,
    })
}

/// tuple 业务 Data 的 body。
fn pair_body(item: &Item) -> Result<(u32, u32), BodyError> {
    record("body:pair");
    Ok((item.id, item.id * 10))
}

// ---------------------------------------------------------------- 驱动辅助

/// 用无 shared 形状的 Each 跑一遍真实 Root，返回收集到的每项输出。
fn drive_only(each: Each<EachOnly<Item>, Out>, originals: Vec<Item>) -> Vec<Out> {
    reset_observations();
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");
    let observed: RefCell<Option<Vec<Out>>> = RefCell::new(None);
    run_definition_in_root(
        flow.definition(),
        vec![root_input(&collection, originals)],
        |view: &mut RootView<'_, '_>| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            let items = view.resolve::<Vec<Item>>(collection.position())?;
            assert_eq!(
                items.len(),
                values.len(),
                "原集合在 Each 后完整：元素按引用借用"
            );
            *observed.borrow_mut() = Some(values.clone());
            Ok(())
        },
    )
    .expect("each run");
    observed.into_inner().expect("observed")
}

/// 用带一个 shared 的形状跑一遍真实 Root，返回收集到的每项输出。
fn drive_shared(
    each: Each<EachShared<Item, Rules>, Out>,
    originals: Vec<Item>,
    weight: u32,
) -> Vec<Out> {
    reset_observations();
    let (mut parent, (collection, rules)) =
        FlowBuilder::<(Vec<Item>, Rules)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<(Vec<Item>, Rules), Data<Vec<Out>>>, _>(
            each,
            (collection.clone(), rules.clone()),
        )
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");
    let observed: RefCell<Option<Vec<Out>>> = RefCell::new(None);
    run_definition_in_root(
        flow.definition(),
        vec![
            root_input(&collection, originals),
            root_input(&rules, Rules { weight }),
        ],
        |view: &mut RootView<'_, '_>| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            let items = view.resolve::<Vec<Item>>(collection.position())?;
            assert_eq!(items.len(), values.len(), "原集合在 Each 后完整");
            let rules = view.resolve::<Rules>(rules.position())?;
            assert_eq!(rules.weight, weight, "shared 只按引用 Import");
            *observed.borrow_mut() = Some(values.clone());
            Ok(())
        },
    )
    .expect("each run");
    observed.into_inner().expect("observed")
}

/// 单 Each 父 Flow 夹具：完成态 Flow 与最终输出位置。
type OnlyEachFlow = (Flow<(Vec<Item>,), Data<Vec<Out>>>, RefId);

/// 构造一个含单个 Each 的完成态父 Flow（`Clone` 时共享同一 `Arc<Definition>`）。
fn parent_flow_with_only_each(each: Each<EachOnly<Item>, Out>) -> OnlyEachFlow {
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(each, collection.clone())
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");
    (flow, collected.position().clone())
}

fn keys_of(values: &[Out]) -> Vec<u32> {
    values.iter().map(|out| out.key).collect()
}

fn addresses_of(values: &[Out]) -> Vec<usize> {
    values.iter().map(|out| out.address).collect()
}

// ---------------------------------------------------------------- H02

#[test]
fn h02_each_only_supports_all_body_kinds() {
    // 同步函数
    let originals = vec![Item { id: 1 }, Item { id: 2 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    let mut sync: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    sync.then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
        only_sync as fn(&Item) -> Result<Out, BodyError>,
    )
    .expect("sync fn body");
    let values = drive_only(sync.finish().expect("finish"), originals);
    assert_eq!(keys_of(&values), vec![1, 2], "同步函数 body 真实执行");
    assert_eq!(
        addresses_of(&values),
        expected,
        "同步函数 body 借用原元素（无 implicit Clone）"
    );

    // 异步函数
    let originals = vec![Item { id: 3 }, Item { id: 4 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    let mut asynchronous: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    asynchronous
        .then_body::<_, AsyncFnSig<(Item,), Data<Out>>>(only_async)
        .expect("async fn body");
    let values = drive_only(asynchronous.finish().expect("finish"), originals);
    assert_eq!(keys_of(&values), vec![3, 4], "异步函数 body 真实执行");
    assert_eq!(addresses_of(&values), expected, "异步函数 body 借用原元素");

    // 具体结构体 Node
    let originals = vec![Item { id: 5 }, Item { id: 6 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    let mut structure: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    structure
        .then_body::<_, NodeSig<(Item,), Data<Out>>>(OnlyStruct)
        .expect("struct node body");
    let values = drive_only(structure.finish().expect("finish"), originals);
    assert_eq!(keys_of(&values), vec![5, 6], "结构体 Node body 真实执行");
    assert_eq!(
        addresses_of(&values),
        expected,
        "结构体 Node body 借用原元素"
    );

    // Arc<具体 Node>
    let originals = vec![Item { id: 7 }, Item { id: 8 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    let mut shared: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    #[allow(clippy::arc_with_non_send_sync)] // 单线程、非 Send 执行模型：测试夹具共享计数
    let arc_node = Arc::new(OnlyArcNode {
        calls: Cell::new(0),
    });
    shared
        .then_body::<_, ArcNodeSig<(Item,), Data<Out>>>(arc_node)
        .expect("arc node body");
    let values = drive_only(shared.finish().expect("finish"), originals);
    assert_eq!(
        keys_of(&values),
        vec![17, 28],
        "Arc Node body 真实执行（配置跨 item 共享计数）"
    );
    assert_eq!(addresses_of(&values), expected, "Arc Node body 借用原元素");
}

#[test]
fn h02_each_shared_supports_all_body_kinds() {
    // 同步函数
    let originals = vec![Item { id: 1 }, Item { id: 2 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    let mut sync: EachBuilder<EachShared<Item, Rules>, Out> = EachBuilder::start().expect("each");
    sync.then_body::<_, SyncFnSig<(Item, Rules), Data<Out>>>(
        shared_sync as fn(&Item, &Rules) -> Result<Out, BodyError>,
    )
    .expect("sync fn body");
    let values = drive_shared(sync.finish().expect("finish"), originals, 7);
    assert_eq!(
        keys_of(&values),
        vec![8, 9],
        "同步函数 body 真实执行且读到 shared"
    );
    assert_eq!(
        addresses_of(&values),
        expected,
        "共享形状按引用借用集合元素"
    );

    // 异步函数
    let originals = vec![Item { id: 1 }, Item { id: 2 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    let mut asynchronous: EachBuilder<EachShared<Item, Rules>, Out> =
        EachBuilder::start().expect("each");
    asynchronous
        .then_body::<_, AsyncFnSig<(Item, Rules), Data<Out>>>(shared_async)
        .expect("async fn body");
    let values = drive_shared(asynchronous.finish().expect("finish"), originals, 7);
    assert_eq!(
        keys_of(&values),
        vec![8, 9],
        "异步函数 body 真实执行且读到 shared"
    );
    assert_eq!(addresses_of(&values), expected, "异步函数 body 借用原元素");

    // 具体结构体 Node
    let originals = vec![Item { id: 1 }, Item { id: 2 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    let mut structure: EachBuilder<EachShared<Item, Rules>, Out> =
        EachBuilder::start().expect("each");
    structure
        .then_body::<_, NodeSig<(Item, Rules), Data<Out>>>(SharedStruct)
        .expect("shared struct node body");
    let values = drive_shared(structure.finish().expect("finish"), originals, 7);
    assert_eq!(
        keys_of(&values),
        vec![8, 9],
        "双输入结构体 Node body 真实执行"
    );
    assert_eq!(
        addresses_of(&values),
        expected,
        "双输入结构体 Node body 借用原元素"
    );

    // Arc<具体 Node>
    let originals = vec![Item { id: 1 }, Item { id: 2 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    let mut shared: EachBuilder<EachShared<Item, Rules>, Out> = EachBuilder::start().expect("each");
    shared
        .then_body::<_, ArcNodeSig<(Item, Rules), Data<Out>>>({
            #[allow(clippy::arc_with_non_send_sync)] // 单线程、非 Send 执行模型
            let node = Arc::new(SharedArcNode);
            node
        })
        .expect("shared arc node body");
    let values = drive_shared(shared.finish().expect("finish"), originals, 7);
    assert_eq!(
        keys_of(&values),
        vec![8, 9],
        "双输入 Arc Node body 真实执行"
    );
    assert_eq!(
        addresses_of(&values),
        expected,
        "双输入 Arc Node body 借用原元素"
    );
}

// ---------------------------------------------------------------- H03

#[test]
fn h03_finished_flow_body_exports_to_item_and_is_consumed_directly() {
    reset_observations();
    // 单输入完成态 Flow：两个真实 Step，证明 Each 驱动底层 Flow 的 Step 序列，
    // 而不是复制／重实现 Flow 的连接算法。
    let (mut body, item_ref) = FlowBuilder::<(Item,)>::start().expect("body");
    let tag = body
        .then::<_, SyncFnSig<(Item,), Data<(u32, usize)>>, _>(
            tag_of as fn(&Item) -> Result<(u32, usize), BodyError>,
            item_ref.clone(),
        )
        .expect("tag step");
    let out = body
        .then::<_, SyncFnSig<((u32, usize),), Data<Out>>, _>(
            wrap_tag as fn(&(u32, usize)) -> Result<Out, BodyError>,
            tag.clone(),
        )
        .expect("wrap step");
    let body_flow: Flow<(Item,), Data<Out>> = body.finish(out).expect("body finish");

    // 完成态 Flow 已是可独立使用的 OrchCall：类型接口直接接受它登记为 Each body。
    let mut builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    builder
        .then_body::<_, OrchSig<Item, Data<Out>>>(body_flow)
        .expect("finished flow body");

    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(
            builder.finish().expect("each finish"),
            collection.clone(),
        )
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");

    let originals = vec![Item { id: 3 }, Item { id: 5 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    run_definition_in_root(
        flow.definition(),
        vec![root_input(&collection, originals)],
        |view: &mut RootView<'_, '_>| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            assert_eq!(keys_of(values), vec![6, 10], "body Flow 的两步真实执行");
            assert_eq!(addresses_of(values), expected, "Flow body 仍借用原集合元素");
            Ok(())
        },
    )
    .expect("each run");

    let shared = take_shared_events();
    let creations: Vec<ScopeRole> = boundary_creation_snapshot()
        .into_iter()
        .map(|(_, _, role)| role)
        .collect();
    assert_eq!(
        creations,
        vec![
            ScopeRole::Each,
            ScopeRole::Item,
            ScopeRole::Flow,
            ScopeRole::Item,
            ScopeRole::Flow
        ],
        "每项一个 ItemScope，且完成态 Flow 真实建立 child：{creations:?}"
    );
    assert_eq!(
        count(&shared, "body:flow-step-1"),
        2,
        "底层 Flow 第一步执行两次: {shared:?}"
    );
    assert_eq!(
        count(&shared, "body:flow-step-2"),
        2,
        "底层 Flow 第二步执行两次"
    );
    assert_eq!(
        count(&shared, "collector-after:1"),
        1,
        "第一项直接 Consume: {shared:?}"
    );
    assert_eq!(count(&shared, "collector-after:2"), 1, "第二项直接 Consume");
    assert!(
        at(&shared, "body:flow-step-2") < at(&shared, "collector-after:1"),
        "child Export 到 Item 之后才直接 Consume: {shared:?}"
    );
}

#[test]
fn h03_finished_flow_body_with_shared_input() {
    reset_observations();
    let (mut body, (item_ref, rules_ref)) = FlowBuilder::<(Item, Rules)>::start().expect("body");
    let out = body
        .then::<_, AsyncFnSig<(Item, Rules), Data<Out>>, _>(
            shared_async,
            (item_ref.clone(), rules_ref.clone()),
        )
        .expect("flow step");
    let body_flow: Flow<(Item, Rules), Data<Out>> = body.finish(out).expect("body finish");

    let mut builder: EachBuilder<EachShared<Item, Rules>, Out> =
        EachBuilder::start().expect("each");
    builder
        .then_body::<_, OrchSig<(Item, Rules), Data<Out>>>(body_flow)
        .expect("finished flow body");

    let (mut parent, (collection, rules)) =
        FlowBuilder::<(Vec<Item>, Rules)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<(Vec<Item>, Rules), Data<Vec<Out>>>, _>(
            builder.finish().expect("each finish"),
            (collection.clone(), rules.clone()),
        )
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");

    let originals = vec![Item { id: 1 }, Item { id: 2 }];
    let expected: Vec<usize> = originals.iter().map(item_address).collect();
    run_definition_in_root(
        flow.definition(),
        vec![
            root_input(&collection, originals),
            root_input(&rules, Rules { weight: 4 }),
        ],
        |view: &mut RootView<'_, '_>| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            assert_eq!(
                keys_of(values),
                vec![5, 6],
                "双输入完成态 Flow body 真实执行一次 per item"
            );
            assert_eq!(addresses_of(values), expected);
            Ok(())
        },
    )
    .expect("each run");

    let shared = take_shared_events();
    assert_eq!(
        count(&shared, "body:shared-async"),
        2,
        "双输入 Flow body 每项真实执行: {shared:?}"
    );
    assert_eq!(
        count(&shared, "collector-after:2"),
        1,
        "两项各 Consume 一次"
    );
}

// ---------------------------------------------------------------- H04

#[test]
fn h04_empty_collection_runs_no_body_and_binds_empty_vec() {
    reset_observations();
    let mut builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    builder
        .then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
            only_sync as fn(&Item) -> Result<Out, BodyError>,
        )
        .expect("body");
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(
            builder.finish().expect("finish"),
            collection.clone(),
        )
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");

    run_definition_in_root(
        flow.definition(),
        vec![root_input(&collection, Vec::<Item>::new())],
        |view: &mut RootView<'_, '_>| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            assert!(values.is_empty(), "空集合结果为整份空 Vec");
            view.data_id_of(collected.position())?;
            Ok(())
        },
    )
    .expect("empty run");

    assert_eq!(
        count(&take_events(), "body:only-sync"),
        0,
        "空集合 body 执行 0 次"
    );
    let creations = boundary_creation_snapshot();
    assert_eq!(
        creations
            .iter()
            .filter(|(_, _, role)| *role == ScopeRole::Item)
            .count(),
        0,
        "空集合不建立任何 ItemScope: {creations:?}"
    );
    assert_eq!(
        creations
            .iter()
            .filter(|(_, _, role)| *role == ScopeRole::Each)
            .count(),
        1,
        "空集合仍建立唯一 EachScope: {creations:?}"
    );
    let shared = take_shared_events();
    assert_eq!(
        count(&shared, "each-finish-refs:2"),
        1,
        "空集合只做一次最终绑定: {shared:?}"
    );
    assert_eq!(
        count(&shared, "each-finish-owned:1"),
        1,
        "空集合登记一份空 Vec Data"
    );
    assert!(
        !saw(&shared, "collector-before:0") && !saw(&shared, "collector-after:1"),
        "空集合不产生 item 消费: {shared:?}"
    );
}

#[test]
fn h04_single_item_builds_one_item_one_consume_one_final_binding() {
    reset_observations();
    let mut builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    builder
        .then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
            only_sync as fn(&Item) -> Result<Out, BodyError>,
        )
        .expect("body");
    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(
            builder.finish().expect("finish"),
            collection.clone(),
        )
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");

    let originals = vec![Item { id: 9 }];
    let expected = item_address(&originals[0]);
    run_definition_in_root(
        flow.definition(),
        vec![root_input(&collection, originals)],
        |view: &mut RootView<'_, '_>| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            assert_eq!(values.len(), 1, "单项只产生一个元素");
            assert_eq!(values[0].key, 9);
            assert_eq!(
                values[0].address, expected,
                "借用来自唯一存在的元素，无不存在 item 借用"
            );
            view.data_id_of(collected.position())?;
            Ok(())
        },
    )
    .expect("single run");

    assert_eq!(
        count(&take_events(), "body:only-sync"),
        1,
        "body 恰好执行一次"
    );
    let creations = boundary_creation_snapshot();
    assert_eq!(
        creations
            .iter()
            .filter(|(_, _, role)| *role == ScopeRole::Item)
            .count(),
        1,
        "单项只建立一个 ItemScope: {creations:?}"
    );
    let shared = take_shared_events();
    assert_eq!(
        count(&shared, "collector-before:0"),
        1,
        "只发生一次消费前观测: {shared:?}"
    );
    assert_eq!(count(&shared, "collector-after:1"), 1, "只发生一次 Consume");
    assert!(
        !saw(&shared, "collector-before:1") && !saw(&shared, "collector-after:2"),
        "不存在第二项: {shared:?}"
    );
    assert_eq!(
        count(&shared, "each-finish-refs:2"),
        1,
        "最终输出位置只绑定一次: {shared:?}"
    );
    assert_eq!(
        count(&shared, "each-finish-owned:1"),
        1,
        "最终 Vec 只登记一次"
    );
}

// ---------------------------------------------------------------- H05

#[test]
fn h05_tuple_output_is_one_data_element_per_item() {
    reset_observations();
    let mut builder: EachBuilder<EachOnly<Item>, (u32, u32)> = EachBuilder::start().expect("each");
    builder
        .then_body::<_, SyncFnSig<(Item,), Data<(u32, u32)>>>(
            pair_body as fn(&Item) -> Result<(u32, u32), BodyError>,
        )
        .expect("tuple body");
    let each = builder.finish().expect("finish");
    // 结构证据：O=(X,Y) 仍只声明一份 Data 输出端口（不被拆成 Out2，也不混成两份）。
    assert_eq!(
        each.wrapper_definition().output_ports().len(),
        1,
        "tuple body 只有一份 Data 输出"
    );
    assert_eq!(
        each.definition().output_ports().len(),
        1,
        "Each 只声明最终 Vec 输出端口"
    );

    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<(u32, u32)>>>, _>(each, collection.clone())
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<(u32, u32)>>, _>(collected.clone())
        .expect("parent finish");

    let originals = vec![Item { id: 1 }, Item { id: 2 }];
    run_definition_in_root(
        flow.definition(),
        vec![root_input(&collection, originals)],
        |view: &mut RootView<'_, '_>| {
            let values = view.resolve::<Vec<(u32, u32)>>(collected.position())?;
            assert_eq!(values.len(), 2, "每项收为一份元素（不是每项两个元素）");
            assert_eq!(
                values.as_slice(),
                &[(1, 10), (2, 20)],
                "tuple 按项成组且顺序一致"
            );
            Ok(())
        },
    )
    .expect("tuple run");

    let shared = take_shared_events();
    assert_eq!(
        count(&shared, "collector-after:1"),
        1,
        "第一项恰好消费 1 个 Data: {shared:?}"
    );
    assert_eq!(
        count(&shared, "collector-after:2"),
        1,
        "第二项恰好消费 1 个 Data"
    );
    assert!(
        !saw(&shared, "collector-after:3"),
        "两项共消费 2 个 Data: {shared:?}"
    );
    assert_eq!(
        count(&shared, "each-finish-owned:1"),
        1,
        "最终只登记一份 Vec 数据"
    );
}

// `Out2`／`Unit` body 的拒绝由 `M::BuildOutput == DataRef<O>` 在**编译期**强制
// （`FlowBuilder::<(T,)>::finish::<Unit, _>(())` 得到的 `Flow<(T,), Unit>` 无法满足
// `Wiring<BuildOutput = DataRef<O>>`），属于 `tests/ui` 负例，不在本运行期样本内。
// 下面只证明 `O=()` 的**构建前置拒绝**：不消耗 Ref 序号、不留下残留 Step。
fn unit_body(_item: &Item) -> Result<(), BodyError> {
    Ok(())
}

#[test]
fn h05_unit_output_is_rejected_before_any_ref_or_step() {
    reset_observations();
    let (mut parent, item_ref) = FlowBuilder::<(Item,)>::start().expect("parent");
    let refs_before = parent.allocated_probe();
    let steps_before = parent.step_count_probe();
    let rejected = parent.then::<_, SyncFnSig<(Item,), Data<()>>, _>(
        unit_body as fn(&Item) -> Result<(), BodyError>,
        item_ref.clone(),
    );
    assert!(
        matches!(rejected, Err(BuildError::UnsupportedFunctionUnitOutput)),
        "普通函数 unit 输出构建期拒绝: {rejected:?}"
    );
    assert_eq!(parent.allocated_probe(), refs_before, "拒绝不消耗 Ref 序号");
    assert_eq!(parent.step_count_probe(), steps_before, "拒绝不留残留 Step");

    // Each body：同一禁止条件在登记前生效，包装不会留下任何 Step。
    let mut each: EachBuilder<EachOnly<Item>, ()> = EachBuilder::start().expect("each");
    assert_eq!(
        each.wrapper_step_count_probe(),
        Some(0),
        "登记前包装无 Step"
    );
    let rejected = each.then_body::<_, SyncFnSig<(Item,), Data<()>>>(
        unit_body as fn(&Item) -> Result<(), BodyError>,
    );
    assert!(
        matches!(rejected, Err(BuildError::UnsupportedFunctionUnitOutput)),
        "Each body 也构建期拒绝: {rejected:?}"
    );
    // 可恢复拒绝必须保留合法构建态：包装仍开放、Step 仍为 0，随后可继续登记合法 body。
    assert_eq!(
        each.wrapper_step_count_probe(),
        Some(0),
        "拒绝不关闭包装、不留残留 Step"
    );
    // 可恢复拒绝必须保留合法构建态（不丢 wrapper、不留 Step）：`O = ()` 形状本就没有
    // 合法 body，因此后续以"包装仍开放 + finish 返回 EachBodyMissing 而非 panic"为证据。
    let recovered = each.finish();
    assert!(
        matches!(recovered, Err(BuildError::EachBodyMissing)),
        "拒绝后可安全 finish（返回明确错误而非 panic）"
    );
}

#[test]
fn h05_each_requires_exactly_one_body() {
    reset_observations();
    // 未登记 body 的 finish 必须返回错误而不是 panic，且不产生完成态。
    let each: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    let missing = each.finish().err();
    assert!(
        matches!(missing, Some(BuildError::EachBodyMissing)),
        "无 body 的 finish 返回明确构建错误: {missing:?}"
    );

    // 第二个 body 必须在追加 Step／分配 Ref 之前拒绝，第一登记保留。
    let mut each: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    each.then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
        only_sync as fn(&Item) -> Result<Out, BodyError>,
    )
    .expect("first body");
    let steps_after_first = each.wrapper_step_count_probe();
    let refs_after_first = each.allocated_probe();
    let second = each.then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
        only_sync as fn(&Item) -> Result<Out, BodyError>,
    );
    assert!(
        matches!(second, Err(BuildError::SecondEachBody)),
        "第二个 body 构建期拒绝: {second:?}"
    );
    assert_eq!(
        each.wrapper_step_count_probe(),
        steps_after_first,
        "拒绝不追加 Step"
    );
    assert_eq!(each.allocated_probe(), refs_after_first, "拒绝不分配 Ref");
    assert!(each.finish().is_ok(), "第一登记保留，仍可完成");
}

// ---------------------------------------------------------------- H07

#[test]
fn h07_same_each_recomputes_at_multiple_positions_and_executions() {
    reset_observations();
    let mut builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    builder
        .then_body::<_, SyncFnSig<(Item,), Data<Out>>>(
            only_sync as fn(&Item) -> Result<Out, BodyError>,
        )
        .expect("body");
    let (parent, _collected) = parent_flow_with_only_each(builder.finish().expect("finish"));

    // 同一 Each 定义被两个调用位置复用：Flow::clone 共享同一 Arc<Definition>。
    let left = parent.clone();
    let right = parent.clone();
    assert!(
        std::ptr::eq(left.definition(), right.definition()),
        "clone 共享同一不可变定义（同一 Each）"
    );

    let (mut grand, (left_in, right_in)) =
        FlowBuilder::<(Vec<Item>, Vec<Item>)>::start().expect("grand");
    let left_pos = grand
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(left, left_in.clone())
        .expect("left call");
    let right_pos = grand
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(right, right_in.clone())
        .expect("right call");
    let grand_flow = grand
        .finish::<Out2<Vec<Out>, Vec<Out>>, _>((left_pos.clone(), right_pos.clone()))
        .expect("grand finish");

    let left_ref = left_pos.position().clone();
    let right_ref = right_pos.position().clone();

    // 第一次 Execution：两个调用位置都重算，各自 Scope 身份不同。
    let first: RefCell<Option<(RefId, RefId, DataId, DataId)>> = RefCell::new(None);
    run_definition_in_root(
        grand_flow.definition(),
        vec![
            root_input(&left_in, vec![Item { id: 1 }, Item { id: 2 }]),
            root_input(&right_in, vec![Item { id: 3 }]),
        ],
        |view: &mut RootView<'_, '_>| {
            let l = view.resolve::<Vec<Out>>(&left_ref)?;
            let r = view.resolve::<Vec<Out>>(&right_ref)?;
            assert_eq!(keys_of(l), vec![1, 2], "左调用位置重算");
            assert_eq!(keys_of(r), vec![3], "右调用位置重算");
            *first.borrow_mut() = Some((
                left_ref.clone(),
                right_ref.clone(),
                view.data_id_of(&left_ref)?,
                view.data_id_of(&right_ref)?,
            ));
            Ok(())
        },
    )
    .expect("first run");
    let (ref_l1, ref_r1, data_l1, data_r1) = first.into_inner().expect("observed");
    assert_eq!(ref_l1, left_ref, "RefId 复用于每次 Execution");
    assert_eq!(ref_r1, right_ref, "RefId 复用于每次 Execution");
    assert_ne!(data_l1, data_r1, "两个调用位置的 Data 身份不同");

    let calls_first: Vec<ScopeId> = boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, role)| *role == ScopeRole::Each)
        .map(|(child, _, _)| child)
        .collect();
    assert_eq!(
        calls_first.len(),
        2,
        "同一 Each 在两个调用位置各建立一次 EachScope: {calls_first:?}"
    );
    assert_ne!(
        calls_first[0], calls_first[1],
        "两个调用位置的 EachScope 身份不同"
    );

    // 第二次 Execution：RefId 相同，Scope／Data 身份全新，结果按新输入重算。
    reset_observations();
    let second: RefCell<Option<(DataId, DataId)>> = RefCell::new(None);
    run_definition_in_root(
        grand_flow.definition(),
        vec![
            root_input(&left_in, vec![Item { id: 7 }]),
            root_input(&right_in, vec![Item { id: 8 }, Item { id: 9 }]),
        ],
        |view: &mut RootView<'_, '_>| {
            let l = view.resolve::<Vec<Out>>(&left_ref)?;
            let r = view.resolve::<Vec<Out>>(&right_ref)?;
            assert_eq!(keys_of(l), vec![7], "第二次 Execution 重算而非复用旧数据");
            assert_eq!(
                keys_of(r),
                vec![8, 9],
                "第二次 Execution 重算而非复用旧数据"
            );
            *second.borrow_mut() =
                Some((view.data_id_of(&left_ref)?, view.data_id_of(&right_ref)?));
            Ok(())
        },
    )
    .expect("second run");
    let (data_l2, data_r2) = second.into_inner().expect("observed");
    assert_ne!(data_l1, data_l2, "不同 Execution 的 Data 身份不混");
    assert_ne!(data_r1, data_r2, "不同 Execution 的 Data 身份不混");

    let calls_second: Vec<ScopeId> = boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, role)| *role == ScopeRole::Each)
        .map(|(child, _, _)| child)
        .collect();
    assert_eq!(
        calls_second.len(),
        2,
        "第二次 Execution 仍为两个调用位置各建一次"
    );
    assert!(
        calls_first
            .iter()
            .all(|a| calls_second.iter().all(|b| a != b)),
        "不同 Execution 的 Scope 身份不混: {calls_first:?} vs {calls_second:?}"
    );
}

#[test]
fn h07_arc_node_config_is_shared_but_execution_data_is_not() {
    reset_observations();
    #[allow(clippy::arc_with_non_send_sync)] // 单线程、非 Send 执行模型
    let node = Arc::new(OnlyArcNode {
        calls: Cell::new(0),
    });
    let counter = Arc::clone(&node);
    let mut builder: EachBuilder<EachOnly<Item>, Out> = EachBuilder::start().expect("each");
    builder
        .then_body::<_, ArcNodeSig<(Item,), Data<Out>>>(node)
        .expect("arc node body");

    let (mut parent, collection) = FlowBuilder::<(Vec<Item>,)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<Vec<Item>, Data<Vec<Out>>>, _>(
            builder.finish().expect("finish"),
            collection.clone(),
        )
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<Out>>, _>(collected.clone())
        .expect("parent finish");

    let first_id: RefCell<Option<DataId>> = RefCell::new(None);
    run_definition_in_root(
        flow.definition(),
        vec![root_input(
            &collection,
            vec![Item { id: 1 }, Item { id: 2 }, Item { id: 3 }],
        )],
        |view: &mut RootView<'_, '_>| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            assert_eq!(
                keys_of(values),
                vec![11, 22, 33],
                "every item 由共享配置产生结果"
            );
            *first_id.borrow_mut() = Some(view.data_id_of(collected.position())?);
            Ok(())
        },
    )
    .expect("first run");
    assert_eq!(counter.calls.get(), 3, "Arc Node 配置在 3 个 item 间共享");
    let first_id = first_id.into_inner().expect("observed");

    let second_id: RefCell<Option<DataId>> = RefCell::new(None);
    run_definition_in_root(
        flow.definition(),
        vec![root_input(
            &collection,
            vec![Item { id: 4 }, Item { id: 5 }],
        )],
        |view: &mut RootView<'_, '_>| {
            let values = view.resolve::<Vec<Out>>(collected.position())?;
            assert_eq!(
                keys_of(values),
                vec![44, 55],
                "第二次 Execution 按新输入重算"
            );
            *second_id.borrow_mut() = Some(view.data_id_of(collected.position())?);
            Ok(())
        },
    )
    .expect("second run");
    assert_eq!(counter.calls.get(), 5, "Arc Node 配置跨 Execution 共享");
    let second_id = second_id.into_inner().expect("observed");

    assert_ne!(
        first_id, second_id,
        "各 Execution 的 owned Data 身份不混（配置不承载执行数据）"
    );
}
