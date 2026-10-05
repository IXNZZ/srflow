//! V21-08 item cap／target 切片（H09～H14、H16～H18、H22）。
//!
//! 本文件只做验收测试：不修改生产代码。H16／H17 必须来自**真实 body Flow**（把自身
//! 输入原样声明为输出），其余条目直接使用协调组件的真实入口（`bind_item_input`、
//! `import_batch`、`finalize`、`consume_item`、`begin_collector`、`finish_collector`），
//! 以便在拒绝点前后取得 `snapshot_targets_probe` 的完整 refs／owned 证据。

use std::any::TypeId;

use super::builder::TypedCallBuilder;
use super::context::BodyError;
use super::each::{Each, EachBuilder, EachOnly, EachShared};
use super::flow::{Flow, FlowBuilder};
use super::identity::{DataId, ExecutionIdentity, ScopeId};
use super::internal_error::ScopeError;
use super::orchestrator::OrchCall;
use super::ref_id::{RefId, RefIdAllocator, RefIdSource};
use super::scope::{
    ExportSlot, ImportSlot, ItemAccess, RefTarget, ScopeCoordinator, ScopeState, TargetSnapshot,
};
use super::signature::{AsyncFnSig, Data, OrchSig};
use super::test_support::{
    count, reset_observations, root_input, run_definition_in_root, run_definition_plain, saw,
    take_shared_events,
};

// ---------------------------------------------------------------- 业务夹具类型

/// 非 Clone 集合元素（item 目标的本体）。
#[derive(Debug, PartialEq, Eq)]
struct CapItem {
    /// 元素身份。
    id: u32,
}

/// body 每项新产生的 owned 输出。
#[derive(Debug, PartialEq, Eq)]
struct CapOut {
    /// 输出值。
    key: u32,
}

/// 非 Clone shared Data。
#[derive(Debug, PartialEq, Eq)]
struct CapShared {
    /// shared 值。
    weight: u32,
}

/// 另一个业务类型（元素 metadata／collector 类型不符的对照）。
#[derive(Debug, PartialEq, Eq)]
struct OtherData(u32);

/// H09 的 body 输出：带元素实际地址，证明借用来自原集合元素本身。
#[derive(Debug, PartialEq, Eq)]
struct AddressedOut {
    /// 结果值。
    key: u32,
    /// body 内观察到的 item 地址。
    address: usize,
}

async fn probe_cap_item(item: &CapItem, shared: &CapShared) -> Result<AddressedOut, BodyError> {
    Ok(AddressedOut {
        key: item.id + shared.weight,
        address: item as *const CapItem as usize,
    })
}

// ---------------------------------------------------------------- 协调组件夹具

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

    fn register<T: 'static>(&mut self, scope: &ScopeId, position: &RefId, value: T) -> DataId {
        self.coordinator
            .register_owned(scope, position, value)
            .unwrap()
    }

    fn import<T: 'static>(
        &mut self,
        child: &ScopeId,
        caller: &ScopeId,
        source: &RefId,
        target: &RefId,
    ) {
        self.coordinator
            .import_batch(child, caller, &[ImportSlot::new::<T>(source, target)])
            .unwrap();
    }

    fn targets(&self, scope: &ScopeId) -> (Vec<(RefId, TargetSnapshot)>, Vec<DataId>) {
        self.coordinator.snapshot_targets_probe(scope).unwrap()
    }

    fn state(&self, scope: &ScopeId) -> ScopeState {
        self.coordinator.state(scope).unwrap()
    }

    fn owned_len(&self, scope: &ScopeId) -> usize {
        self.coordinator.owned_len_probe(scope).unwrap()
    }
}

/// 标准结构：Root 登记 `Vec<CapItem>` 集合 → EachScope 导入 → ItemScope 绑定第 `index` 项。
///
/// 返回 `(each, item, collection_data, each_collection_position, item_position)`。
fn cap_stack(f: &mut Fixture, index: usize) -> (ScopeId, ScopeId, DataId, RefId, RefId) {
    let root = f.root();
    let collection_position = f.ref_id();
    let collection = f.register(
        &root,
        &collection_position,
        vec![CapItem { id: 1 }, CapItem { id: 2 }],
    );
    let each = f.child(&root);
    let each_collection = f.ref_id();
    f.import::<Vec<CapItem>>(&each, &root, &collection_position, &each_collection);
    let item = f.child(&each);
    let item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&item, &each, &each_collection, &item_position, index)
        .unwrap();
    (each, item, collection, each_collection, item_position)
}

/// 复现一个真实 CollectionItem 目标（同 collection／index／cap／access）。
fn item_target(collection: &DataId, index: usize, cap: &ScopeId) -> RefTarget {
    RefTarget::CollectionItem {
        collection: collection.clone(),
        index,
        lifetime_cap: cap.clone(),
        access: ItemAccess::for_collection::<CapItem>(),
    }
}

// ---------------------------------------------------------------- H09

/// H09：非 Clone item／shared／O；地址相同、无 move／Clone、owner 不增、旧 O 身份失效、
/// 物理移动次数匹配项数。
#[test]
fn h09_non_clone_items_are_borrowed_in_place_and_moved_once_each() {
    reset_observations();
    // 真实 Each：body 完成态 Flow（异步 Node 读取 item 与 shared）。
    let (mut body, (item_ref, shared_ref)) =
        FlowBuilder::<(CapItem, CapShared)>::start().expect("body");
    let body_out = body
        .then::<_, AsyncFnSig<(CapItem, CapShared), Data<AddressedOut>>, _>(
            probe_cap_item,
            (item_ref.clone(), shared_ref.clone()),
        )
        .expect("probe");
    let body_flow: Flow<(CapItem, CapShared), Data<AddressedOut>> =
        body.finish(body_out).expect("body finish");

    let mut each_builder: EachBuilder<EachShared<CapItem, CapShared>, AddressedOut> =
        EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, OrchSig<(CapItem, CapShared), Data<AddressedOut>>>(body_flow)
        .expect("each body");
    let each: Each<EachShared<CapItem, CapShared>, AddressedOut> =
        each_builder.finish().expect("each finish");

    let (mut parent, (collection, shared)) =
        FlowBuilder::<(Vec<CapItem>, CapShared)>::start().expect("parent");
    let collected = parent
        .then::<_, OrchSig<(Vec<CapItem>, CapShared), Data<Vec<AddressedOut>>>, _>(
            each,
            (collection.clone(), shared.clone()),
        )
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<AddressedOut>>, _>(collected.clone())
        .expect("parent finish");

    let collection_position = collection.position().clone();
    let collected_position = collected.position().clone();
    run_definition_in_root(
        flow.definition(),
        vec![
            root_input(
                &collection,
                vec![CapItem { id: 10 }, CapItem { id: 20 }, CapItem { id: 30 }],
            ),
            root_input(&shared, CapShared { weight: 7 }),
        ],
        |view| {
            // 元素地址来自 Root 登记的原集合本身：解析同一 Vec 取得期望地址。
            let expected: Vec<usize> = view
                .resolve::<Vec<CapItem>>(&collection_position)?
                .iter()
                .map(|item| item as *const CapItem as usize)
                .collect();
            let outputs = view.resolve::<Vec<AddressedOut>>(&collected_position)?;
            assert_eq!(
                outputs.iter().map(|out| out.address).collect::<Vec<_>>(),
                expected,
                "每项借用都来自原集合元素本身（无 move／Clone）"
            );
            assert_eq!(
                outputs.iter().map(|out| out.key).collect::<Vec<_>>(),
                vec![17, 27, 37],
                "结果按输入顺序且 shared 被逐项读取"
            );
            // owner 不增：集合仍由 Root 责任。
            let collection_id = view.data_id_of(&collection_position)?;
            let root_scope = view.root().clone();
            assert_eq!(
                view.probe().owner_probe(&collection_id).unwrap(),
                root_scope,
                "集合 owner 不增"
            );
            assert_eq!(
                view.resolve::<Vec<CapItem>>(&collection_position)?.len(),
                3,
                "原集合内容原样保留"
            );
            Ok(())
        },
    )
    .expect("each run");

    // 物理移动次数匹配项数：每一项 Consume 恰好追加一次。
    let shared_events = take_shared_events();
    for index in 1..=3 {
        assert_eq!(
            count(&shared_events, &format!("collector-after:{index}")),
            1,
            "第 {index} 项移动一次: {shared_events:?}"
        );
    }

    // 消费新 O 后旧标识失效：每项 owned 输出移入 collector 后不再存活。
    let mut f = Fixture::new();
    let (each_scope, item, _collection, _each_collection, item_position) = cap_stack(&mut f, 0);
    let collector = f
        .coordinator
        .begin_collector::<CapOut>(&each_scope)
        .unwrap();
    let out_position = f.ref_id();
    let out_id = f.register(&item, &out_position, CapOut { key: 1 });
    assert!(f.coordinator.alive_probe(&out_id));
    f.coordinator
        .consume_item(&item, &out_position, &collector)
        .unwrap();
    assert!(!f.coordinator.alive_probe(&out_id), "消费后旧 O 身份失效");
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 1);
    let _ = item_position;
}

// ---------------------------------------------------------------- H10

/// H10：collector 唯一物理 owner——控制器只持元数据；Consume 不增 Each 的 refs／owned；
/// finish 前不能业务 borrow；最终 Vec 一次登记／Export。
#[test]
fn h10_collector_is_the_only_physical_owner_of_items() {
    let mut f = Fixture::new();
    let root = f.root();
    let collection_position = f.ref_id();
    let collection = f.register(
        &root,
        &collection_position,
        vec![CapItem { id: 1 }, CapItem { id: 2 }],
    );
    let each = f.child(&root);
    let each_collection = f.ref_id();
    f.import::<Vec<CapItem>>(&each, &root, &collection_position, &each_collection);
    let collector = f.coordinator.begin_collector::<CapOut>(&each).unwrap();

    // 类型／字段断言：未完成 collector 只登记元素类型与责任 Scope，不产生普通 DataId。
    let (element_name, owner) = f.coordinator.collector_metadata(&collector).unwrap();
    assert_eq!(element_name, std::any::type_name::<CapOut>());
    assert_eq!(owner, each, "collector 的责任方就是固定控制器");
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
    assert_eq!(f.owned_len(&each), 0, "控制器只持元数据，无 ordinary owned");
    let before = f.targets(&each);

    // Item Consume：Each 的完整 refs／ordinary owned 不增，collector len+1。
    let item = f.child(&each);
    let item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&item, &each, &each_collection, &item_position, 0)
        .unwrap();
    let out_position = f.ref_id();
    f.register(&item, &out_position, CapOut { key: 7 });
    f.coordinator
        .consume_item(&item, &out_position, &collector)
        .unwrap();
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 1);
    assert_eq!(
        f.targets(&each),
        before,
        "Consume 前后 Each refs／owned 不增"
    );

    // finish 前不能业务 borrow 最终输出位置。
    let final_position = f.ref_id();
    assert!(
        matches!(
            f.coordinator.resolve::<Vec<CapOut>>(&each, &final_position),
            Err(ScopeError::RefNotBound { .. })
        ),
        "未 finish 时最终输出位置尚未绑定"
    );

    // finish：最终 Vec 一次登记（owned +1、refs +1）。
    let vec_id = f
        .coordinator
        .finish_collector(&each, &collector, &final_position)
        .unwrap();
    assert_eq!(
        f.coordinator
            .resolve::<Vec<CapOut>>(&each, &final_position)
            .unwrap(),
        &vec![CapOut { key: 7 }]
    );
    let after_finish = f.targets(&each);
    assert_eq!(
        after_finish.1,
        vec![vec_id.clone()],
        "最终 Vec 只登记一份普通 owned Data"
    );
    assert_eq!(
        after_finish.0.len(),
        before.0.len() + 1,
        "只新增一个输出位置"
    );

    // Export：最终 Vec 一次性移交给 Root。
    let root_out = f.ref_id();
    let before_root = f.targets(&root);
    let mut slots = vec![ExportSlot::new::<Vec<CapOut>>(&final_position, &root_out)];
    f.coordinator
        .finalize(&each, std::slice::from_ref(&final_position), &mut slots)
        .unwrap();
    let after_root = f.targets(&root);
    assert_eq!(
        after_root.1.len(),
        before_root.1.len() + 1,
        "Root 只新增一份 Vec"
    );
    assert_eq!(
        f.coordinator
            .resolve::<Vec<CapOut>>(&root, &root_out)
            .unwrap(),
        &vec![CapOut { key: 7 }]
    );
    assert_eq!(f.owned_len(&each), 0, "导出后控制器不再持有 Vec");
    let _ = collection;
}

// ---------------------------------------------------------------- H11

/// H11：cap 内多层 Import；Node 实际读取；descendant→Item alias Export 合法；目的 Item
/// 即 cap 本身；真实 ancestry 在 cap 内；Item owner 不增加。
#[test]
fn h11_descendant_item_aliases_stay_inside_cap() {
    let mut f = Fixture::new();
    let root = f.root();
    let collection_position = f.ref_id();
    let collection = f.register(
        &root,
        &collection_position,
        vec![CapItem { id: 1 }, CapItem { id: 2 }],
    );
    let each = f.child(&root);
    let each_collection = f.ref_id();
    f.import::<Vec<CapItem>>(&each, &root, &collection_position, &each_collection);
    let item = f.child(&each);
    let item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&item, &each, &each_collection, &item_position, 0)
        .unwrap();

    // cap 内多层 Import：item → d1 → d2，目的均为 cap 的后代。
    let d1 = f.child(&item);
    let d1_position = f.ref_id();
    f.import::<CapItem>(&d1, &item, &item_position, &d1_position);
    let d2 = f.child(&d1);
    let d2_position = f.ref_id();
    f.import::<CapItem>(&d2, &d1, &d1_position, &d2_position);

    // 真实 resolver 读取：地址与原集合元素相同，元素类型经真实投影取得。
    let expected = {
        let stored = f
            .coordinator
            .resolve::<Vec<CapItem>>(&root, &collection_position)
            .unwrap();
        &stored[0] as *const CapItem as usize
    };
    assert_eq!(
        f.coordinator.resolve::<CapItem>(&d2, &d2_position).unwrap() as *const CapItem as usize,
        expected,
        "descendant 的 item 读取来自原集合元素本身"
    );
    assert_eq!(
        f.coordinator
            .resolve::<CapItem>(&d2, &d2_position)
            .unwrap()
            .id,
        1
    );

    let item_owned_before = f.owned_len(&item);
    // descendant→Item alias Export 合法：d2 → d1 → item。
    let d1_out = f.ref_id();
    let mut first = vec![ExportSlot::new::<CapItem>(&d2_position, &d1_out)];
    f.coordinator
        .finalize(&d2, std::slice::from_ref(&d2_position), &mut first)
        .unwrap();
    let d1_targets = f.targets(&d1);
    assert!(
        d1_targets
            .0
            .iter()
            .any(|(position, target)| position == &d1_out
                && matches!(target, TargetSnapshot::CollectionItem { .. })),
        "d1 获得 item alias"
    );
    assert!(d1_targets.1.is_empty(), "item alias 不产生 owned");

    let item_out = f.ref_id();
    let mut second = vec![ExportSlot::new::<CapItem>(&d1_out, &item_out)];
    f.coordinator
        .finalize(&d1, std::slice::from_ref(&d1_out), &mut second)
        .unwrap();
    let item_targets = f.targets(&item);
    assert!(
        item_targets.0.iter().any(|(position, target)| {
            position == &item_out
                && matches!(target, TargetSnapshot::CollectionItem { lifetime_cap, .. } if *lifetime_cap == item)
        }),
        "目的 Item == cap 本身"
    );
    assert_eq!(f.owned_len(&item), item_owned_before, "Item owner 不增加");
    // 真实 ancestry 在 cap 内：d1 与 d2 的 parent 链都经过 item。
    assert_eq!(f.coordinator.parent_of(&d1).unwrap(), Some(item.clone()));
    assert_eq!(f.coordinator.parent_of(&d2).unwrap(), Some(d1.clone()));
    let _ = each;
    let _ = collection;
}

// ---------------------------------------------------------------- H12

/// H12：cap 外转移拒绝——Item→Each／Root Export 与导入兄弟 Scope 都在前置拒绝；目的
/// refs／owned 无变化，合法 cap 内对照通过。
#[test]
fn h12_escaping_item_cap_is_rejected_before_any_binding() {
    let mut f = Fixture::new();
    let (each, item, collection, _each_collection, item_position) = cap_stack(&mut f, 0);
    let root = f.root();

    // 真实 body 把 item 输出到 Item：d1 在 cap 内导出 alias，Item 合法持有 item alias。
    let d1 = f.child(&item);
    let d1_position = f.ref_id();
    f.import::<CapItem>(&d1, &item, &item_position, &d1_position);
    let item_alias = f.ref_id();
    let mut inner = vec![ExportSlot::new::<CapItem>(&d1_position, &item_alias)];
    f.coordinator
        .finalize(&d1, std::slice::from_ref(&d1_position), &mut inner)
        .unwrap();

    // 目的 Each：Item→Each Export 前置拒绝（cap 外）。
    let before_each = f.targets(&each);
    let each_out = f.ref_id();
    let mut to_each = vec![ExportSlot::new::<CapItem>(&item_alias, &each_out)];
    let escape_each = f
        .coordinator
        .finalize(&item, std::slice::from_ref(&item_alias), &mut to_each)
        .unwrap_err();
    assert!(
        matches!(escape_each, ScopeError::ItemCapEscape { .. }),
        "目的 Each 在 cap 外: {escape_each:?}"
    );
    assert_eq!(f.targets(&each), before_each, "目的 Each refs／owned 不变");

    // 目的 Root：以 test-only 目标放置把同一 item target 放到 Root 的 child 上，触达
    // Root 目的检查（Item 自身 parent 是 Each，无法直接 finalize 到 Root）。
    let item2 = f.child(&each);
    let item2_position = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&item2, &each, &_each_collection, &item2_position, 1)
        .unwrap();
    let holder = f.child(&root);
    let holder_position = f.ref_id();
    f.coordinator.inject_target_probe(
        &holder,
        &holder_position,
        item_target(&collection, 1, &item2),
    );
    let before_root = f.targets(&root);
    let root_out = f.ref_id();
    let mut to_root = vec![ExportSlot::new::<CapItem>(&holder_position, &root_out)];
    let escape_root = f
        .coordinator
        .finalize(
            &holder,
            std::slice::from_ref(&holder_position),
            &mut to_root,
        )
        .unwrap_err();
    // 来源 holder 本身在 cap 外：按统一管线先以来源资格拒绝（`ItemOutsideCap`）；
    // 目的 Root 同样在 cap 外，两者都不能让 item 逃逸。
    assert!(
        matches!(escape_root, ScopeError::ItemOutsideCap { .. }),
        "来源 holder 在 cap 外: {escape_root:?}"
    );
    assert_eq!(f.targets(&root), before_root, "目的 Root refs／owned 不变");

    // 目的兄弟：把 item target 放到 Each 的本地位置后尝试导入兄弟 Scope。
    let item3 = f.child(&each);
    let item3_position = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&item3, &each, &_each_collection, &item3_position, 0)
        .unwrap();
    let exposed = f.ref_id();
    f.coordinator
        .inject_target_probe(&each, &exposed, item_target(&collection, 0, &item3));
    let sibling = f.child(&each);
    let sibling_position = f.ref_id();
    let before_sibling = f.targets(&sibling);
    let escape_sibling = f
        .coordinator
        .import_batch(
            &sibling,
            &each,
            &[ImportSlot::new::<CapItem>(&exposed, &sibling_position)],
        )
        .unwrap_err();
    assert!(
        matches!(escape_sibling, ScopeError::ItemOutsideCap { .. }),
        "来源 Each 在 cap 外（统一管线先查来源资格）: {escape_sibling:?}"
    );
    assert_eq!(
        f.targets(&sibling),
        before_sibling,
        "目的兄弟 refs／owned 不变"
    );

    // 合法 cap 内对照：import 到 cap 的后代通过。
    let inside = f.child(&item3);
    let inside_position = f.ref_id();
    f.import::<CapItem>(&inside, &item3, &item3_position, &inside_position);
    assert_eq!(
        f.coordinator
            .resolve::<CapItem>(&inside, &inside_position)
            .unwrap()
            .id,
        1
    );
}

// ---------------------------------------------------------------- H13

/// H13：Closed／stale cap——记录真实 target 元数据；Item 关闭后旧 target 不能再次
/// resolve／Import；后来 Item 新 ScopeId 不复活旧 cap；有效 sibling／新 descendant 也
/// 不得绕过；无失效 Rust borrow。
#[test]
fn h13_stale_closed_cap_cannot_be_revived_or_bypassed() {
    let mut f = Fixture::new();
    let (each, item, collection, each_collection, item_position) = cap_stack(&mut f, 0);

    // 记录真实 target 元数据。
    let recorded = f
        .targets(&item)
        .0
        .into_iter()
        .find(|(position, _)| position == &item_position)
        .map(|(_, target)| target)
        .expect("item target recorded");
    let TargetSnapshot::CollectionItem {
        collection: recorded_collection,
        index,
        lifetime_cap,
        collection_type,
        element_type,
    } = recorded
    else {
        panic!("expected a CollectionItem target");
    };
    assert_eq!(recorded_collection, collection);
    assert_eq!(index, 0);
    assert_eq!(lifetime_cap, item);
    assert_eq!(collection_type, TypeId::of::<Vec<CapItem>>());
    assert_eq!(element_type, TypeId::of::<CapItem>());

    // 关闭 item：先在一个 live sibling 上复现同一真实 target（同 collection／index／cap／access）。
    let sibling = f.child(&each);
    let stale_position = f.ref_id();
    f.coordinator.inject_target_probe(
        &sibling,
        &stale_position,
        item_target(&recorded_collection, index, &lifetime_cap),
    );
    let mut no_slots: Vec<ExportSlot> = Vec::new();
    f.coordinator.finalize(&item, &[], &mut no_slots).unwrap();
    assert_eq!(f.state(&item), ScopeState::Closed);
    assert_eq!(f.state(&lifetime_cap), ScopeState::Closed, "旧 cap 已关闭");

    // 旧 target 不能再次 resolve（重复调用证明无失效 Rust borrow）。
    for _ in 0..2 {
        assert!(
            matches!(
                f.coordinator.resolve::<CapItem>(&sibling, &stale_position),
                Err(ScopeError::ItemOutsideCap { .. })
            ),
            "关闭的 cap 不再授予借用"
        );
    }
    // 关闭后的 Item 自身也不再接受业务 resolve。
    let closed_resolve = f.ref_id();
    f.coordinator.inject_target_probe(
        &item,
        &closed_resolve,
        item_target(&recorded_collection, index, &lifetime_cap),
    );
    assert!(
        matches!(
            f.coordinator.resolve::<CapItem>(&item, &closed_resolve),
            Err(ScopeError::ScopeClosed { .. })
        ),
        "关闭的 Item 不再接受 resolve"
    );

    // 旧 target 不能被 Import 到新 descendant：cap 已关闭，任何转移在来源资格检查处即拒绝。
    let new_descendant = f.child(&sibling);
    let new_position = f.ref_id();
    let before_new = f.targets(&new_descendant);
    let stale_import = f.coordinator.import_batch(
        &new_descendant,
        &sibling,
        &[ImportSlot::new::<CapItem>(&stale_position, &new_position)],
    );
    assert!(
        matches!(
            stale_import,
            Err(ScopeError::ScopeClosed { .. }) | Err(ScopeError::ItemOutsideCap { .. })
        ),
        "stale cap 的转移被拒绝: {stale_import:?}"
    );
    assert_eq!(
        f.targets(&new_descendant),
        before_new,
        "拒绝不留绑定（来源资格先于目的）"
    );

    // 后来的 Item 取得新 ScopeId，旧 cap 不复活。
    let new_item = f.child(&each);
    let new_item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&new_item, &each, &each_collection, &new_item_position, 1)
        .unwrap();
    let new_cap = f
        .targets(&new_item)
        .0
        .into_iter()
        .find(|(position, _)| position == &new_item_position)
        .and_then(|(_, target)| match target {
            TargetSnapshot::CollectionItem { lifetime_cap, .. } => Some(lifetime_cap),
            TargetSnapshot::Data(_) => None,
        })
        .expect("new item target");
    assert_eq!(new_cap, new_item);
    assert_ne!(new_cap, lifetime_cap);
    assert_eq!(
        f.state(&lifetime_cap),
        ScopeState::Closed,
        "旧 cap 仍是 tombstone"
    );

    // 有效 sibling 与新 descendant 都不得绕过旧 cap。
    assert!(matches!(
        f.coordinator.resolve::<CapItem>(&sibling, &stale_position),
        Err(ScopeError::ItemOutsideCap { .. })
    ));
    let deep = f.child(&new_item);
    let deep_position = f.ref_id();
    f.coordinator.inject_target_probe(
        &deep,
        &deep_position,
        item_target(&recorded_collection, index, &lifetime_cap),
    );
    assert!(matches!(
        f.coordinator.resolve::<CapItem>(&deep, &deep_position),
        Err(ScopeError::ItemOutsideCap { .. })
    ));
    let _ = each_collection;
}

// ---------------------------------------------------------------- H14

/// H14：target 防御校验——foreign collection／cap、缺失／Closed Scope、越界 index、失效
/// 集合、错 Vec 类型／元素 metadata 逐项拒绝；入口（erased pack／Node body 之前）失败，
/// 合法输入对照通过。
#[test]
fn h14_target_validation_rejects_every_broken_input() {
    let mut f = Fixture::new();
    let root = f.root();
    let collection_position = f.ref_id();
    let collection = f.register(
        &root,
        &collection_position,
        vec![CapItem { id: 1 }, CapItem { id: 2 }],
    );
    let each = f.child(&root);
    let each_collection = f.ref_id();
    f.import::<Vec<CapItem>>(&each, &root, &collection_position, &each_collection);
    let item = f.child(&each);
    let item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&item, &each, &each_collection, &item_position, 0)
        .unwrap();

    // (1) 越界 index：真实绑定入口在写入 target 之前拒绝（erased pack／Node body 之前）。
    let raw_item = f.child(&each);
    let raw_position = f.ref_id();
    let before_raw = f.targets(&raw_item);
    assert!(matches!(
        f.coordinator.bind_item_input::<CapItem>(
            &raw_item,
            &each,
            &each_collection,
            &raw_position,
            2
        ),
        Err(ScopeError::ItemIndexOutOfRange { .. })
    ));
    assert_eq!(f.targets(&raw_item), before_raw, "越界绑定不写入 target");

    // (2) 已绑定 target 的越界 index。
    let bad_index = f.ref_id();
    f.coordinator
        .inject_target_probe(&item, &bad_index, item_target(&collection, 9, &item));
    assert!(matches!(
        f.coordinator.resolve::<CapItem>(&item, &bad_index),
        Err(ScopeError::ItemIndexOutOfRange { .. })
    ));

    // (3) 元素 metadata 不符：按错误元素类型读取。
    assert!(matches!(
        f.coordinator.resolve::<OtherData>(&item, &item_position),
        Err(ScopeError::TypeMismatch { .. })
    ));

    // (4) 错 Vec 类型：collection 实际是 Vec<OtherData>。
    let other_position = f.ref_id();
    let other_collection = f.register(&root, &other_position, vec![OtherData(1)]);
    let wrong_vec = f.ref_id();
    f.coordinator.inject_target_probe(
        &item,
        &wrong_vec,
        RefTarget::CollectionItem {
            collection: other_collection,
            index: 0,
            lifetime_cap: item.clone(),
            access: ItemAccess::for_collection::<CapItem>(),
        },
    );
    assert!(matches!(
        f.coordinator.resolve::<CapItem>(&item, &wrong_vec),
        Err(ScopeError::TypeMismatch { .. })
    ));

    // (5) foreign collection：属于另一次 Execution；集合存活前检把 foreign 与失效集合
    // 一起归入 `ItemCollectionNotAlive`（实际诊断，见 check_item 的存活前置）。
    let mut foreign = Fixture::new();
    let foreign_position = foreign.ref_id();
    let foreign_collection =
        foreign.register(&foreign.root(), &foreign_position, vec![CapItem { id: 9 }]);
    let foreign_source = f.ref_id();
    f.coordinator.inject_target_probe(
        &item,
        &foreign_source,
        RefTarget::CollectionItem {
            collection: foreign_collection,
            index: 0,
            lifetime_cap: item.clone(),
            access: ItemAccess::for_collection::<CapItem>(),
        },
    );
    assert!(matches!(
        f.coordinator.resolve::<CapItem>(&item, &foreign_source),
        Err(ScopeError::ItemCollectionNotAlive { .. })
    ));

    // (6) foreign／缺失 cap Scope：不属于本 Execution，无法成为有效 cap。
    let foreign_cap_source = f.ref_id();
    f.coordinator.inject_target_probe(
        &item,
        &foreign_cap_source,
        RefTarget::CollectionItem {
            collection: collection.clone(),
            index: 0,
            lifetime_cap: foreign.root(),
            access: ItemAccess::for_collection::<CapItem>(),
        },
    );
    assert!(matches!(
        f.coordinator.resolve::<CapItem>(&item, &foreign_cap_source),
        Err(ScopeError::ItemOutsideCap { .. })
    ));

    // (7) 已失效集合：entry 被销毁。
    let dead_position = f.ref_id();
    let dead_collection = f.register(&root, &dead_position, vec![CapItem { id: 3 }]);
    let dead_source = f.ref_id();
    f.coordinator
        .inject_target_probe(&item, &dead_source, item_target(&dead_collection, 0, &item));
    f.coordinator.destroy_probe(&dead_collection);
    assert!(matches!(
        f.coordinator.resolve::<CapItem>(&item, &dead_source),
        Err(ScopeError::ItemCollectionNotAlive { .. })
    ));

    // (8) 合法输入对照：真实 target 正常读取。
    assert_eq!(
        f.coordinator
            .resolve::<CapItem>(&item, &item_position)
            .unwrap()
            .id,
        1
    );
    let _ = each;
}

// ---------------------------------------------------------------- H16

/// H16：imported 完整 Data 不能收集——真实 body Flow 原样输出 shared ancestor Data；
/// 真实 Item Consume 拒绝，移动次数 0，原 owner／内容不变，collector 未增，不返回部分 Vec。
#[test]
fn h16_re_exposed_shared_data_cannot_be_collected() {
    reset_observations();
    // 真实 body Flow：把自身 shared 输入（来自 ItemScope 的 imported alias）原样声明为输出。
    let (body, (item_ref, shared_ref)) =
        FlowBuilder::<(CapItem, CapShared)>::start().expect("body");
    let body_flow: Flow<(CapItem, CapShared), Data<CapShared>> = body
        .finish::<Data<CapShared>, _>(shared_ref.clone())
        .expect("identity finish");
    let _ = item_ref;

    let mut each_builder: EachBuilder<EachShared<CapItem, CapShared>, CapShared> =
        EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, OrchSig<(CapItem, CapShared), Data<CapShared>>>(body_flow)
        .expect("each body");
    let each: Each<EachShared<CapItem, CapShared>, CapShared> =
        each_builder.finish().expect("each finish");

    let (mut parent, (collection, shared)) =
        FlowBuilder::<(Vec<CapItem>, CapShared)>::start().expect("parent");
    let produced = parent
        .then::<_, OrchSig<(Vec<CapItem>, CapShared), Data<Vec<CapShared>>>, _>(
            each,
            (collection.clone(), shared.clone()),
        )
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<CapShared>>, _>(produced)
        .expect("parent finish");

    let error = run_definition_plain(
        flow.definition(),
        vec![
            root_input(&collection, vec![CapItem { id: 1 }]),
            root_input(&shared, CapShared { weight: 5 }),
        ],
    )
    .expect_err("imported shared 不能被 Consume");
    assert!(
        matches!(error.scope_error(), Some(ScopeError::IllegalOwner { .. })),
        "期望 IllegalOwner，实际: {:?}",
        error.scope_error()
    );
    let shared_events = take_shared_events();
    assert_eq!(
        count(&shared_events, "collector-before:0"),
        1,
        "拒绝点移动次数仍为 0: {shared_events:?}"
    );
    assert!(
        !saw(&shared_events, "collector-after:1"),
        "拒绝后 collector 未增: {shared_events:?}"
    );
    assert!(
        !saw(&shared_events, "each-finish-refs:1"),
        "拒绝后未 finish，不返回部分 Vec: {shared_events:?}"
    );

    // 协调组件证据：原 owner／内容不变、collector 未增、Each 两侧 refs／owned 不变。
    let mut f = Fixture::new();
    let root = f.root();
    let shared_position = f.ref_id();
    let shared_id = f.register(&root, &shared_position, CapShared { weight: 5 });
    let each = f.child(&root);
    let shared_alias = f.ref_id();
    f.import::<CapShared>(&each, &root, &shared_position, &shared_alias);
    let collector = f.coordinator.begin_collector::<CapShared>(&each).unwrap();
    let item = f.child(&each);
    let item_shared = f.ref_id();
    f.import::<CapShared>(&item, &each, &shared_alias, &item_shared);
    let before_each = f.targets(&each);
    let rejected = f
        .coordinator
        .consume_item(&item, &item_shared, &collector)
        .unwrap_err();
    assert!(
        matches!(rejected, ScopeError::IllegalOwner { .. }),
        "imported ancestor Data 被拒绝: {rejected:?}"
    );
    assert_eq!(f.targets(&each), before_each, "Each refs／owned 不变");
    assert_eq!(
        f.coordinator.collector_len(&collector).unwrap(),
        0,
        "collector 未增"
    );
    assert_eq!(
        f.coordinator.collector_moves(&collector).unwrap(),
        0,
        "移动次数 0"
    );
    assert!(f.coordinator.alive_probe(&shared_id), "原 Data 存活");
    assert_eq!(
        f.coordinator.owner_probe(&shared_id).unwrap(),
        root,
        "原 owner 不变"
    );
    assert_eq!(
        f.coordinator
            .resolve::<CapShared>(&root, &shared_position)
            .unwrap(),
        &CapShared { weight: 5 },
        "原内容不变"
    );
}

// ---------------------------------------------------------------- H17

/// H17：CollectionItem 不能收集——真实 body Flow 原样输出 item；cap 内输出合法，但 Item
/// Consume 因非完整 owned Data 拒绝；不 take 原集合／item；显式 Node `&T -> new O` 正例可收集。
#[test]
fn h17_item_alias_cannot_be_collected_but_new_owned_output_can() {
    reset_observations();
    // 真实 body Flow：把自身 item 输入原样声明为输出（cap 内 Export 合法）。
    let (body, item_ref) = FlowBuilder::<(CapItem,)>::start().expect("body");
    let body_flow: Flow<(CapItem,), Data<CapItem>> = body
        .finish::<Data<CapItem>, _>(item_ref.clone())
        .expect("identity finish");

    let mut each_builder: EachBuilder<EachOnly<CapItem>, CapItem> =
        EachBuilder::start().expect("each");
    each_builder
        .then_body::<_, OrchSig<CapItem, Data<CapItem>>>(body_flow)
        .expect("each body");
    let each: Each<EachOnly<CapItem>, CapItem> = each_builder.finish().expect("each finish");

    let (mut parent, collection) = FlowBuilder::<(Vec<CapItem>,)>::start().expect("parent");
    let produced = parent
        .then::<_, OrchSig<Vec<CapItem>, Data<Vec<CapItem>>>, _>(each, collection.clone())
        .expect("wire each");
    let flow = parent
        .finish::<Data<Vec<CapItem>>, _>(produced)
        .expect("parent finish");

    let collection_position = collection.position().clone();
    let error = run_definition_plain(
        flow.definition(),
        vec![root_input(
            &collection,
            vec![CapItem { id: 1 }, CapItem { id: 2 }],
        )],
    )
    .expect_err("item alias 不能被 Consume");
    assert!(
        matches!(
            error.scope_error(),
            Some(ScopeError::NonCompleteTarget { .. })
        ),
        "期望 NonCompleteTarget，实际: {:?}",
        error.scope_error()
    );
    let shared_events = take_shared_events();
    assert_eq!(
        count(&shared_events, "collector-before:0"),
        1,
        "拒绝点移动次数仍为 0: {shared_events:?}"
    );
    assert!(
        !saw(&shared_events, "collector-after:1"),
        "拒绝后 collector 未增: {shared_events:?}"
    );

    // 协调组件证据 + 显式 Node `&T -> new O` 正例可收集。
    let mut f = Fixture::new();
    let root = f.root();
    let collection_position_local = f.ref_id();
    let collection_local = f.register(
        &root,
        &collection_position_local,
        vec![CapItem { id: 1 }, CapItem { id: 2 }],
    );
    let each = f.child(&root);
    let each_collection = f.ref_id();
    f.import::<Vec<CapItem>>(&each, &root, &collection_position_local, &each_collection);
    let collector = f.coordinator.begin_collector::<CapOut>(&each).unwrap();

    // alias 版：ItemScope 的位置被绑定为 CollectionItem。
    let item = f.child(&each);
    let item_position = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&item, &each, &each_collection, &item_position, 0)
        .unwrap();
    let before_collection = f.targets(&root);
    let rejected = f
        .coordinator
        .consume_item(&item, &item_position, &collector)
        .unwrap_err();
    assert!(
        matches!(rejected, ScopeError::NonCompleteTarget { .. }),
        "CollectionItem 被拒绝: {rejected:?}"
    );
    assert_eq!(
        f.coordinator.collector_moves(&collector).unwrap(),
        0,
        "不被 take"
    );
    assert_eq!(f.targets(&root), before_collection, "原集合未被 take");
    assert!(
        f.coordinator
            .resolve::<Vec<CapItem>>(&root, &collection_position_local)
            .unwrap()
            .len()
            == 2
    );

    // 正例：显式 Node 产生新的 owned O（`&T -> new O`），可被收集。
    let item_ok = f.child(&each);
    let alias_ok = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&item_ok, &each, &each_collection, &alias_ok, 1)
        .unwrap();
    let new_out = f.ref_id();
    f.register(&item_ok, &new_out, CapOut { key: 42 });
    f.coordinator
        .consume_item(&item_ok, &new_out, &collector)
        .unwrap();
    assert_eq!(
        f.coordinator.collector_len(&collector).unwrap(),
        1,
        "新 owned O 可收集"
    );
    let _ = collection_local;
    let _ = collection_position;
}

// ---------------------------------------------------------------- H18

/// H18：不属于当前 Item 的其他 Data——Each-owned、兄弟 Item 输出、错误 parent／collector
/// 类型／句柄来源都在移动前拒绝；两侧完整 refs／owned、Data 存活与 collector 内容不变。
#[test]
fn h18_foreign_owned_data_is_rejected_before_moving() {
    let mut f = Fixture::new();
    let root = f.root();
    let each = f.child(&root);
    let collector = f.coordinator.begin_collector::<CapOut>(&each).unwrap();

    // (a) Each-owned 完整 Data。
    let each_owned_position = f.ref_id();
    let each_owned = f.register(&each, &each_owned_position, CapOut { key: 99 });
    let item_a = f.child(&each);
    let alias_a = f.ref_id();
    f.import::<CapOut>(&item_a, &each, &each_owned_position, &alias_a);
    let before_each = f.targets(&each);
    let rejected = f
        .coordinator
        .consume_item(&item_a, &alias_a, &collector)
        .unwrap_err();
    assert!(
        matches!(rejected, ScopeError::IllegalOwner { .. }),
        "Each-owned 被拒绝: {rejected:?}"
    );
    assert_eq!(f.targets(&each), before_each, "Each refs／owned 不变");
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert!(f.coordinator.alive_probe(&each_owned));
    assert_eq!(
        f.coordinator.owner_probe(&each_owned).unwrap(),
        each,
        "owner 不变"
    );

    // (b) 兄弟 Item 输出：仅目标存活不授予消费权。
    let sibling_item = f.child(&each);
    let sibling_position = f.ref_id();
    let sibling_out = f.register(&sibling_item, &sibling_position, CapOut { key: 5 });
    let item_b = f.child(&each);
    let alias_b = f.ref_id();
    f.coordinator
        .inject_target_probe(&item_b, &alias_b, RefTarget::Data(sibling_out.clone()));
    let before_again = f.targets(&each);
    let rejected = f
        .coordinator
        .consume_item(&item_b, &alias_b, &collector)
        .unwrap_err();
    assert!(
        matches!(rejected, ScopeError::IllegalOwner { .. }),
        "兄弟 Item 输出被拒绝: {rejected:?}"
    );
    assert_eq!(f.targets(&each), before_again);
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert!(f.coordinator.alive_probe(&sibling_out));
    assert_eq!(
        f.coordinator.owner_probe(&sibling_out).unwrap(),
        sibling_item
    );

    // (c) 错误 parent：Item 的 parent 不是 collector 的责任方。
    let orphan = f.child(&root);
    let orphan_position = f.ref_id();
    f.register(&orphan, &orphan_position, CapOut { key: 1 });
    let wrong_parent = f
        .coordinator
        .consume_item(&orphan, &orphan_position, &collector)
        .unwrap_err();
    assert!(
        matches!(wrong_parent, ScopeError::CollectorNotOwnedBy { .. }),
        "错误 parent 被拒绝: {wrong_parent:?}"
    );

    // (d) 错误 collector 类型：owned 值与 collector 元素类型不符。
    let item_d = f.child(&each);
    let other_position = f.ref_id();
    f.register(&item_d, &other_position, OtherData(1));
    let wrong_type = f
        .coordinator
        .consume_item(&item_d, &other_position, &collector)
        .unwrap_err();
    assert!(
        matches!(wrong_type, ScopeError::TypeMismatch { .. }),
        "错类型被拒绝: {wrong_type:?}"
    );
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);

    // (e) 错误句柄来源：foreign collector。
    let mut foreign = Fixture::new();
    let foreign_each = foreign.child(&foreign.root());
    let foreign_collector = foreign
        .coordinator
        .begin_collector::<CapOut>(&foreign_each)
        .unwrap();
    let item_e = f.child(&each);
    let position_e = f.ref_id();
    let value_e = f.register(&item_e, &position_e, CapOut { key: 2 });
    let foreign_handle = f
        .coordinator
        .consume_item(&item_e, &position_e, &foreign_collector)
        .unwrap_err();
    assert!(
        matches!(foreign_handle, ScopeError::CollectorForeignExecution { .. }),
        "外来句柄被拒绝: {foreign_handle:?}"
    );
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
    assert!(
        !f.coordinator.alive_probe(&value_e),
        "拒绝后 Value 随 Item 清理"
    );
}

// ---------------------------------------------------------------- H22

/// H22：collector 与输出身份——未完成 collector 无普通 DataId；每项新 O 有独立 DataId、
/// 消费后旧 target／本地 Ref 失效；最终 Vec 新身份不与 item／旧 O 混淆；Root 观察后完整
/// 快照不变。
#[test]
fn h22_collector_and_final_vec_identities_are_distinct() {
    let mut f = Fixture::new();
    let root = f.root();
    let collection_position = f.ref_id();
    let collection = f.register(&root, &collection_position, vec![CapItem { id: 1 }]);
    let each = f.child(&root);
    let collector = f.coordinator.begin_collector::<CapOut>(&each).unwrap();

    // 未完成 collector 无普通 DataId：控制器 refs／owned 为空，最终输出位置尚未绑定。
    assert_eq!(f.coordinator.refs_len_probe(&each).unwrap(), 0);
    assert_eq!(f.owned_len(&each), 0);
    let final_position = f.ref_id();
    assert!(matches!(
        f.coordinator.resolve::<Vec<CapOut>>(&each, &final_position),
        Err(ScopeError::RefNotBound { .. })
    ));
    assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);

    // 每项新 O 独立 DataId。
    let item1 = f.child(&each);
    let out1_position = f.ref_id();
    let out1 = f.register(&item1, &out1_position, CapOut { key: 11 });
    let item2 = f.child(&each);
    let out2_position = f.ref_id();
    let out2 = f.register(&item2, &out2_position, CapOut { key: 22 });
    assert_ne!(out1, out2, "每项 O 的 DataId 独立");

    f.coordinator
        .consume_item(&item1, &out1_position, &collector)
        .unwrap();
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
    assert!(!f.coordinator.alive_probe(&out1), "消费后旧 O 身份失效");
    assert_eq!(f.state(&item1), ScopeState::Closed);
    assert!(f.targets(&item1).0.is_empty(), "消费后旧本地 Ref 失效");

    f.coordinator
        .consume_item(&item2, &out2_position, &collector)
        .unwrap();
    assert!(!f.coordinator.alive_probe(&out2));
    assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 2);

    // 最终 Vec 新身份：不与 item／旧 O／来源集合混淆。
    let vec_id = f
        .coordinator
        .finish_collector(&each, &collector, &final_position)
        .unwrap();
    assert_ne!(vec_id, out1);
    assert_ne!(vec_id, out2);
    assert_ne!(vec_id, collection);
    let resolved = f
        .coordinator
        .resolve::<Vec<CapOut>>(&each, &final_position)
        .unwrap();
    assert_eq!(
        resolved,
        &vec![CapOut { key: 11 }, CapOut { key: 22 }],
        "最终 Vec 按项顺序"
    );

    // Root 观察（只读 resolve）后完整快照不变：一次登记、无二次绑定。
    let before = f.targets(&each);
    let _ = f
        .coordinator
        .resolve::<Vec<CapOut>>(&each, &final_position)
        .unwrap()
        .len();
    assert_eq!(f.targets(&each), before, "只读观察不改快照");
    assert_eq!(f.owned_len(&each), 1, "最终 Vec 一次登记");
    assert!(f.targets(&each).1.contains(&vec_id));
}

#[test]
fn h12_item_import_checks_the_source_side_not_only_the_destination() {
    // R4 反例：把 cap == Item 的 item target 放到 parent Each（非法来源），再把它导入
    // Item（合法目的：目的就是 cap 本身）。统一管线必须拒绝非法来源，不能被合法目的背书。
    let mut f = Fixture::new();
    let (each, item, collection, _each_collection, _item_position) = cap_stack(&mut f, 0);

    let illegal_position = f.ref_id();
    f.coordinator
        .inject_target_probe(&each, &illegal_position, item_target(&collection, 1, &item));
    let each_before = f.targets(&each);
    let item_before = f.targets(&item);

    let target_position = f.ref_id();
    let rejected = f.coordinator.import_batch(
        &item,
        &each,
        &[ImportSlot::new::<CapItem>(
            &illegal_position,
            &target_position,
        )],
    );
    assert!(
        matches!(rejected, Err(ScopeError::ItemOutsideCap { .. })),
        "来源 Each 在 cap 外必须拒绝: {rejected:?}"
    );
    assert_eq!(f.targets(&each), each_before, "来源侧重不变");
    assert_eq!(f.targets(&item), item_before, "目的侧不留绑定");

    // 合法来源对照：item target 由 cap 自身（或其后代）持有时可以导入 cap 内位置。
    let descendant = f.child(&item);
    let source_position = f.ref_id();
    f.coordinator
        .bind_item_input::<CapItem>(&item, &each, &_each_collection, &source_position, 0)
        .expect("cap 自身绑定 item");
    let inside = f.ref_id();
    f.coordinator
        .import_batch(
            &descendant,
            &item,
            &[ImportSlot::new::<CapItem>(&source_position, &inside)],
        )
        .expect("cap 内来源可以导入 cap 内目的");
    let resolved = f.coordinator.resolve::<CapItem>(&descendant, &inside);
    assert_eq!(resolved.expect("合法 alias 可读取").id, 1);
}
