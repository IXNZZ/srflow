//! V22-T02 定向测试：Node 自动输入桥、四种调用形式、同步 Factory 与失败隔离。
//!
//! B01～B09、B13～B14 在本文件；Each／Retry 重复执行与取消证据见
//! `tests_v22_t02_repeat.rs`；编译夹具与外部可见性证据见
//! `docs/implementation_tasks_v2.2/V22_T02_VALIDATION/`。
//!
//! 运行正例一律经正式 `Runtime::execute`（旧签名：Root 闭包返回 RefShape）与真实
//! `Context`／`Container`；受控 Builder 只用于表达构建期结构断言与失败隔离样本。
#![allow(clippy::err_expect, clippy::let_unit_value, clippy::unused_unit)]

use super::data::Data;
use super::definition::{BodyRole, Builder, Definition, PositionSource};
use super::error::{BodyError, BuildErrorDetail, RunError};
use super::identity::{BodyId, PositionHandle, PositionId};
use super::node::Node;
use super::runtime::Runtime;
use super::shape::{Query, Ref, TypeDesc};
use futures::executor::block_on;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ---------------------------------------------------------------- 业务类型与 Node

#[derive(Debug, PartialEq, Eq, Clone)]
struct Amount(u32);
impl Data for Amount {}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Doubled(u32);
impl Data for Doubled {}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Key(u32);
impl Data for Key {}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Plain(u32);
impl Data for Plain {}

/// 非 Clone 的 owned Data：Drop 顺序/次数用于验证独立构造与清理。
#[derive(Debug, PartialEq, Eq)]
struct Counted(u32);
impl Data for Counted {}

impl Drop for Counted {
    fn drop(&mut self) {
        DROP_LOG.with(|log| log.borrow_mut().push(self.0));
    }
}

thread_local! {
    static DROP_LOG: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
}

fn take_drops() -> Vec<u32> {
    DROP_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
}

async fn load_amount(query: Query<()>) -> Result<Amount, BodyError> {
    let _ = query.get();
    Ok(Amount(1))
}

async fn add_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

async fn double(query: Query<&Amount>) -> Result<Doubled, BodyError> {
    Ok(Doubled(query.get().0 * 2))
}

async fn key_from_amount(query: Query<&Amount>) -> Result<Key, BodyError> {
    Ok(Key(query.get().0))
}

async fn combine(query: Query<(&Amount, &Doubled)>) -> Result<Key, BodyError> {
    let (amount, doubled) = query.get();
    Ok(Key(amount.0 + doubled.0))
}

async fn record_unit(_query: Query<()>) -> Result<(), BodyError> {
    UNIT_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

static UNIT_RUNS: AtomicUsize = AtomicUsize::new(0);

/// b01 专用计数：与 b02 的样本隔离，避免并行测试互相干扰。
static UNIT_RUNS_B01: AtomicUsize = AtomicUsize::new(0);

async fn record_unit_b01(_query: Query<()>) -> Result<(), BodyError> {
    UNIT_RUNS_B01.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

/// 零槽输入的 Struct Node。
struct UnitSource;

impl Node for UnitSource {
    type Input = ();
    type Output = Key;

    async fn run(&self, _query: Query<&()>) -> Result<Key, BodyError> {
        Ok(Key(7))
    }
}

/// 单槽输入的 Struct Node。
struct Bump;

impl Node for Bump {
    type Input = Amount;
    type Output = Doubled;

    async fn run(&self, query: Query<&Amount>) -> Result<Doubled, BodyError> {
        Ok(Doubled(query.get().0 + 10))
    }
}

/// 双槽输入的 Struct Node。
struct PairSink;

impl Node for PairSink {
    type Input = (Amount, Doubled);
    type Output = Key;

    async fn run(&self, query: Query<&(Amount, Doubled)>) -> Result<Key, BodyError> {
        let (amount, doubled) = query.get();
        Ok(Key(amount.0 * doubled.0))
    }
}

// ---------------------------------------------------------------- 受控 Builder 辅助

fn builder_with_inputs(
    types: &[TypeDesc],
) -> (Rc<RefCell<Builder<'static>>>, BodyId, Vec<PositionHandle>) {
    let builder = Rc::new(RefCell::new(Builder::new()));
    let root = builder.borrow_mut().new_body(None, BodyRole::Ordinary);
    let mut handles = Vec::with_capacity(types.len());
    for (slot, type_desc) in types.iter().enumerate() {
        let handle = builder.borrow_mut().allocate(
            root,
            PositionSource::RootInput { slot: slot as u16 },
            *type_desc,
        );
        handles.push(handle);
    }
    (builder, root, handles)
}

fn finish_builder(
    builder: Rc<RefCell<Builder<'static>>>,
    root: BodyId,
) -> Result<Definition<'static>, BuildErrorDetail> {
    let state = Rc::try_unwrap(builder)
        .ok()
        .expect("test does not retain the internal Flow")
        .into_inner();
    state.finish(root)
}

fn open_flow<'n>(builder: &Rc<RefCell<Builder<'n>>>, body: BodyId) -> super::flow::Flow<'n> {
    super::flow::Flow::open(Rc::clone(builder), body)
}

// ---------------------------------------------------------------- B01 自动推导

#[test]
#[allow(clippy::needless_borrows_for_generic_args)] // 刻意覆盖函数引用形态
fn b01_function_item_reference_and_async_closure() {
    let runtime = Runtime;
    let closure = async |query: Query<&Amount>| -> Result<Doubled, BodyError> {
        Ok(Doubled(query.get().0 * 3))
    };
    let (item, reference, closed) = block_on(runtime.execute(
        |flow, _input| {
            // 零槽函数：普通形式仍执行（unit Step 不省略）。
            flow.then_auto(load_amount);
            let _unit: () = flow.then_auto_output(record_unit_b01);
            // 单槽函数：item、引用、async closure 三种形态自然推导同一 Shape。
            let by_item = flow.then_auto_output(&double);
            let by_reference: Ref<Doubled> = flow.then_auto_output(double);
            let by_closure = flow.then_auto_output(closure);
            (by_item, by_reference, by_closure)
        },
        (),
    ))
    .expect("自动推导的函数形态应构建并运行");
    assert_eq!(item, Doubled(2));
    assert_eq!(reference, Doubled(2));
    assert_eq!(closed, Doubled(3));
    assert_eq!(UNIT_RUNS_B01.load(Ordering::SeqCst), 1);
}

#[test]
#[allow(clippy::needless_borrows_for_generic_args)] // 刻意覆盖 &Node／Arc<Node> 形态
fn b01_struct_owned_reference_and_arc() {
    let runtime = Runtime;
    let (owned, borrowed, shared) = block_on(runtime.execute(
        |flow, _input| {
            let owned: Ref<Key> = flow.then_auto_output(UnitSource);
            let borrowed = flow.then_auto_output(&UnitSource);
            let shared = flow.then_auto_output(Arc::new(UnitSource));
            (owned, borrowed, shared)
        },
        (),
    ))
    .expect("自动推导的 Struct 形态应构建并运行");
    assert_eq!(owned, Key(7));
    assert_eq!(borrowed, Key(7));
    assert_eq!(shared, Key(7));
}

#[test]
fn b01_two_slot_function_and_struct() {
    let runtime = Runtime;
    let (by_function, by_struct) = block_on(runtime.execute(
        |flow, _input| {
            let combined = flow.then_auto_output(combine);
            let paired = flow.then_auto_output(PairSink);
            (combined, paired)
        },
        (Amount(2), Doubled(5)),
    ))
    .expect("双槽自动推导应构建并运行");
    assert_eq!(by_function, Key(7));
    assert_eq!(by_struct, Key(2 * 5));
}

#[test]
fn b01_single_slot_struct_receives_nearest_source() {
    let runtime = Runtime;
    let out = block_on(runtime.execute(
        |flow, _input| {
            let bumped: Ref<Doubled> = flow.then_auto_output(Bump);
            let key = flow.then_with_output(key_from_amount, flow.ref_of::<Amount>());
            (bumped, key)
        },
        Amount(3),
    ))
    .expect("单槽 Struct 自动推导应按 Query 选择输入");
    assert_eq!(out.0, Doubled(13));
    assert_eq!(out.1, Key(3));
}

/// 16 个独立槽类型与求和输出。
macro_rules! slot_types {
    ($($name:ident),+) => {
        $(
            #[derive(Debug, PartialEq, Eq, Clone)]
            struct $name(u32);
            impl Data for $name {}
        )+
    };
}

slot_types!(
    Slot01, Slot02, Slot03, Slot04, Slot05, Slot06, Slot07, Slot08, Slot09, Slot10, Slot11, Slot12,
    Slot13, Slot14, Slot15, Slot16
);

#[derive(Debug, PartialEq, Eq, Clone)]
struct Sum(u32);
impl Data for Sum {}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
async fn sixteen_slot_sum(
    query: Query<(
        &Slot01,
        &Slot02,
        &Slot03,
        &Slot04,
        &Slot05,
        &Slot06,
        &Slot07,
        &Slot08,
        &Slot09,
        &Slot10,
        &Slot11,
        &Slot12,
        &Slot13,
        &Slot14,
        &Slot15,
        &Slot16,
    )>,
) -> Result<Sum, BodyError> {
    let (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16) = query.get();
    Ok(Sum(a1.0
        + a2.0
        + a3.0
        + a4.0
        + a5.0
        + a6.0
        + a7.0
        + a8.0
        + a9.0
        + a10.0
        + a11.0
        + a12.0
        + a13.0
        + a14.0
        + a15.0
        + a16.0))
}

/// 16 槽输入的 Struct Node：验证映射完整而非只支持代表 arity。
struct SixteenSink;

#[allow(clippy::type_complexity)]
impl Node for SixteenSink {
    type Input = (
        Slot01,
        Slot02,
        Slot03,
        Slot04,
        Slot05,
        Slot06,
        Slot07,
        Slot08,
        Slot09,
        Slot10,
        Slot11,
        Slot12,
        Slot13,
        Slot14,
        Slot15,
        Slot16,
    );
    type Output = Sum;

    async fn run(
        &self,
        query: Query<&(
            Slot01,
            Slot02,
            Slot03,
            Slot04,
            Slot05,
            Slot06,
            Slot07,
            Slot08,
            Slot09,
            Slot10,
            Slot11,
            Slot12,
            Slot13,
            Slot14,
            Slot15,
            Slot16,
        )>,
    ) -> Result<Sum, BodyError> {
        let v = query.get();
        Ok(Sum(v.0.0
            + v.1.0
            + v.2.0
            + v.3.0
            + v.4.0
            + v.5.0
            + v.6.0
            + v.7.0
            + v.8.0
            + v.9.0
            + v.10.0
            + v.11.0
            + v.12.0
            + v.13.0
            + v.14.0
            + v.15.0))
    }
}

#[test]
fn b01_sixteen_slot_function_and_struct() {
    let runtime = Runtime;
    let (by_function, by_struct) = block_on(runtime.execute(
        |flow, _input| {
            let function = flow.then_auto_output(sixteen_slot_sum);
            let structural = flow.then_auto_output(SixteenSink);
            (function, structural)
        },
        (
            Slot01(1),
            Slot02(2),
            Slot03(3),
            Slot04(4),
            Slot05(5),
            Slot06(6),
            Slot07(7),
            Slot08(8),
            Slot09(9),
            Slot10(10),
            Slot11(11),
            Slot12(12),
            Slot13(13),
            Slot14(14),
            Slot15(15),
            Slot16(16),
        ),
    ))
    .expect("16 槽自动推导应完整映射全部位置");
    assert_eq!(by_function, Sum(136));
    assert_eq!(by_struct, Sum(136));
}

// ---------------------------------------------------------------- B02 四种形式等价

/// 统计调用次数的双槽函数 Node。
static COMBINE_RUNS: AtomicUsize = AtomicUsize::new(0);

async fn counting_combine(query: Query<(&Amount, &Doubled)>) -> Result<Key, BodyError> {
    COMBINE_RUNS.fetch_add(1, Ordering::SeqCst);
    combine(query).await
}

#[test]
fn b02_four_forms_register_and_execute_equivalently() {
    COMBINE_RUNS.store(0, Ordering::SeqCst);
    let runtime = Runtime;
    let (auto_output, explicit_output) = block_on(runtime.execute(
        |flow, _input| {
            let amount = flow.ref_of::<Amount>();
            let doubled = flow.ref_of::<Doubled>();
            // 自动普通：丢弃句柄但登记并执行同一步骤。
            flow.then_auto(counting_combine);
            // 自动取输出。
            let auto_out = flow.then_auto_output(&counting_combine);
            // 显式普通。
            flow.then_with(counting_combine, (amount, doubled));
            // 显式取输出。
            let explicit_out = flow.then_with_output(&counting_combine, (amount, doubled));
            (auto_out, explicit_out)
        },
        (Amount(2), Doubled(5)),
    ))
    .expect("四种形式应构建并运行");
    assert_eq!(COMBINE_RUNS.load(Ordering::SeqCst), 4);
    assert_eq!(auto_output, Key(7));
    assert_eq!(explicit_output, Key(7));
}

/// 四种形式的结构快照：Step 数、输入槽位置与类型、输出槽数、定义顺序。
#[allow(clippy::type_complexity)]
fn four_form_snapshot() -> Vec<(usize, Vec<(u32, TypeDesc)>, usize, Vec<u64>)> {
    let mut snapshots = Vec::new();
    for form in 0..4 {
        let (builder, root, handles) =
            builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Doubled>()]);
        let mut flow = open_flow(&builder, root);
        let amount = Ref::<Amount>::from_handle(handles[0]);
        let doubled = Ref::<Doubled>::from_handle(handles[1]);
        match form {
            0 => {
                flow.then_auto(combine);
            }
            1 => {
                let _ = flow.then_auto_output(&combine);
            }
            2 => {
                let _ = flow.then_with(combine, (amount, doubled));
            }
            _ => {
                let _ = flow.then_with_output(&combine, (amount, doubled));
            }
        }
        drop(flow);
        let definition = finish_builder(builder, root).expect("each form should build");
        let steps = definition.root().steps();
        let order = steps.iter().map(|step| step.order()).collect::<Vec<_>>();
        let inputs = steps[0]
            .inputs()
            .iter()
            .map(|desc| {
                (
                    desc.handle.position.index() as u32,
                    definition
                        .record(desc.handle)
                        .map(|record| record.type_desc())
                        .unwrap_or_else(TypeDesc::of::<u8>),
                )
            })
            .collect::<Vec<_>>();
        snapshots.push((steps.len(), inputs, steps[0].outputs().len(), order));
    }
    snapshots
}

#[test]
fn b02_four_forms_have_identical_definition_structure() {
    let snapshots = four_form_snapshot();
    assert_eq!(snapshots.len(), 4);
    assert_eq!(snapshots[0].0, 1, "one Node Step per form");
    assert_eq!(
        snapshots[0].1,
        vec![
            (0, TypeDesc::of::<Amount>()),
            (1, TypeDesc::of::<Doubled>())
        ]
    );
    assert_eq!(
        snapshots[0].3.len(),
        1,
        "one definition-order value per Step"
    );
    for snapshot in &snapshots[1..] {
        assert_eq!(
            snapshot, &snapshots[0],
            "all four forms share one registration"
        );
    }
}

#[test]
fn b02_legacy_public_then_matches_internal_then_with_output() {
    // 旧公开 `then` 是内部唯一登记实现的薄委托：同一场景下两条入口的确定性结构完全一致。
    // 比较项：Body/Step 数、逐 Step order()、输入位置与类型、输出槽与类型、published/taint
    // 状态、完整 Import 集合。执行身份（DefinitionId）不参与比较。
    #[allow(clippy::type_complexity)]
    fn snapshot(
        use_legacy: bool,
    ) -> (
        usize,
        Vec<(u32, TypeDesc)>,
        Vec<(u32, Option<TypeDesc>, bool, bool)>,
        Vec<u64>,
        Vec<PositionId>,
    ) {
        let (builder, root, handles) =
            builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Doubled>()]);
        let amount = Ref::<Amount>::from_handle(handles[0]);
        let doubled = Ref::<Doubled>::from_handle(handles[1]);
        let mut flow = open_flow(&builder, root);
        if use_legacy {
            // 旧公开入口（T03 前保持原签名）。
            let _ = flow.then(&combine, (amount, doubled));
        } else {
            let _ = flow.then_with_output(&combine, (amount, doubled));
        }
        drop(flow);
        let definition =
            finish_builder(builder, root).expect("legacy and internal entries should both build");
        let steps = definition.root().steps();
        let inputs = steps[0]
            .inputs()
            .iter()
            .map(|desc| {
                (
                    desc.handle.position.index() as u32,
                    definition
                        .record(desc.handle)
                        .map(|record| record.type_desc())
                        .unwrap_or_else(TypeDesc::of::<u8>),
                )
            })
            .collect::<Vec<_>>();
        let outputs = steps[0]
            .outputs()
            .iter()
            .map(|position| {
                let record = definition.record(PositionHandle {
                    definition: definition.id(),
                    position: *position,
                });
                (
                    position.index() as u32,
                    record.map(|record| record.type_desc()),
                    record.and_then(|record| record.published_order()).is_some(),
                    record.map(|record| record.is_tainted()).unwrap_or(false),
                )
            })
            .collect::<Vec<_>>();
        let orders = steps.iter().map(|step| step.order()).collect::<Vec<_>>();
        (
            steps.len(),
            inputs,
            outputs,
            orders,
            definition.root().imports().to_vec(),
        )
    }

    let legacy = snapshot(true);
    let internal = snapshot(false);
    assert_eq!(legacy.0, 1, "one Node Step per entry");
    assert_eq!(
        legacy, internal,
        "旧公开 then 与内部 then_with_output 必须共用唯一登记实现"
    );
}

#[test]
fn b02_ordinary_form_publishes_source_and_unit_step_executes() {
    UNIT_RUNS.store(0, Ordering::SeqCst);
    let (builder, root, _handles) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let mut flow = open_flow(&builder, root);
    // unit 输出：普通形式仍登记并执行一步，不产生任何输出槽。
    flow.then_auto(record_unit);
    // 单 Data 输出：普通形式仍发布为后续最近来源。
    flow.then_auto(add_one);
    let produced = flow.ref_of::<Amount>();
    drop(flow);
    let definition = finish_builder(builder, root).expect("ordinary forms should build");
    let steps = definition.root().steps();
    assert_eq!(steps.len(), 2);
    assert!(steps[0].outputs().is_empty(), "unit Step publishes no slot");
    assert_eq!(steps[1].outputs().len(), 1);
    assert_eq!(
        produced.handle().position,
        steps[1].outputs()[0],
        "ordinary form still publishes its source"
    );
    let runtime = Runtime;
    let value = block_on(runtime.execute(
        |flow, _input| {
            flow.then_auto(record_unit);
            flow.then_auto(add_one);
            // 普通形式登记的步骤同样发布来源：下一次自动解析取到它的输出。

            flow.then_auto_output(&add_one)
        },
        Amount(1),
    ))
    .expect("unit and single-data ordinary forms should run");
    // 最近来源语义：第二次 add_one 读到第一次的输出（2），得到 3。
    assert_eq!(
        value,
        Amount(3),
        "published source feeds the next auto step"
    );
    assert_eq!(UNIT_RUNS.load(Ordering::SeqCst), 1);
}

#[test]
fn b02_chain_and_sequence_stay_in_the_same_body() {
    let (builder, root, handles) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let amount = Ref::<Amount>::from_handle(handles[0]);
    let mut flow = open_flow(&builder, root);
    // 链式与逐句登记产生相同 Body/Step 结构：链式不创建 child。
    flow.then_auto(record_unit).then_with(add_one, amount);
    assert_eq!(builder.borrow().body(root).steps().len(), 2);
    // chain 的 child 独立；chain 之后继续登记 parent。
    flow.chain(|sub| {
        sub.then_auto(record_unit);
    });
    flow.then_auto(double);
    let root_steps = builder.borrow().body(root).steps().len();
    drop(flow);
    let definition = finish_builder(builder, root).expect("chain plus sequence should build");
    assert_eq!(
        root_steps, 4,
        "two nodes, one chain, one later node in Root"
    );
    let child = definition
        .bodies()
        .iter()
        .find(|body| body.id() != root)
        .expect("chain child body");
    assert_eq!(child.steps().len(), 1, "child holds only its own Node");
}

// ---------------------------------------------------------------- B03 最近来源与冻结

async fn amount_from_plain(query: Query<&Plain>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 100))
}

#[test]
fn b03_nearest_source_selection_and_frozen_connection() {
    let (builder, root, handles) =
        builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Plain>()]);
    let mut flow = open_flow(&builder, root);
    // 源 A（Root 输入）之后调用 Node1：绑定 A。
    flow.then_auto(add_one);
    // 出现源 B（amount_from_plain 的输出，同类型 Amount）。
    flow.then_auto(amount_from_plain);
    // Node2 在 B 之后调用：绑定 B。
    flow.then_auto(add_one);
    drop(flow);
    let definition = finish_builder(builder, root).expect("nearest-source scenario should build");
    let steps = definition.root().steps();
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0].inputs()[0].handle.position, handles[0].position);
    assert_eq!(
        steps[2].inputs()[0].handle.position,
        steps[1].outputs()[0],
        "later same-type source is the nearest for the later call"
    );
    // 连接冻结：Node1 仍指源 A，later 源不会反向改写它。
    assert_eq!(steps[0].inputs()[0].handle.position, handles[0].position);
    let first_source = definition
        .record(steps[0].inputs()[0].handle)
        .expect("registered input")
        .source();
    assert_eq!(first_source, PositionSource::RootInput { slot: 0 });
    let third_source = definition
        .record(steps[2].inputs()[0].handle)
        .expect("registered input")
        .source();
    assert_eq!(
        third_source,
        PositionSource::NodeOutput { step: 1, slot: 0 }
    );

    // 运行读取也对应：Node1 读 A(=1) → 2；B = Plain(4)+100 = 104；Node2 读 B → 105。
    let runtime = Runtime;
    let read = block_on(runtime.execute(
        |flow, _input| {
            let first = flow.then_auto_output(&add_one);
            let second_source = flow.then_auto_output(&amount_from_plain);
            let third = flow.then_auto_output(&add_one);
            (first, second_source, third)
        },
        (Amount(1), Plain(4)),
    ))
    .expect("nearest-source scenario should run");
    assert_eq!(read.0, Amount(2));
    assert_eq!(read.1, Amount(104));
    assert_eq!(read.2, Amount(105));
}

// ---------------------------------------------------------------- B04 Import 路径

#[test]
fn b04_multi_level_auto_imports_are_recorded_and_deduplicated() {
    let (builder, root, handles) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let source = handles[0].position;
    let mut flow = open_flow(&builder, root);
    flow.chain(|sub| {
        sub.chain(|leaf| {
            // 同一祖先来源在 leaf 中使用两次：每层只保留去重后的 Import。
            leaf.then_auto(add_one);
            leaf.then_auto(add_one);
            ()
        });
        ()
    });
    drop(flow);
    let definition = finish_builder(builder, root).expect("nested auto imports should build");
    let root_body = definition.root();
    let child = definition.body(root_body.steps()[0].chain().unwrap().child());
    let leaf = definition.body(child.steps()[0].chain().unwrap().child());
    assert_eq!(
        child
            .imports()
            .iter()
            .filter(|slot| **slot == source)
            .count(),
        1,
        "intermediate body records one deduplicated import per layer"
    );
    assert_eq!(
        leaf.imports()
            .iter()
            .filter(|slot| **slot == source)
            .count(),
        1,
        "deep capture deduplicates even with two uses in the leaf"
    );

    // 显式路径回到同一判定：同样的嵌套与来源，Import 集合一致。
    let (builder, root, handles) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let source = handles[0].position;
    let explicit = Ref::<Amount>::from_handle(handles[0]);
    let mut flow = open_flow(&builder, root);
    flow.chain(|sub| {
        sub.chain(|leaf| {
            leaf.then_with(add_one, explicit);
            leaf.then_with(add_one, explicit);
            ()
        });
        ()
    });
    drop(flow);
    let definition = finish_builder(builder, root).expect("explicit imports should build");
    let root_body = definition.root();
    let child = definition.body(root_body.steps()[0].chain().unwrap().child());
    let leaf = definition.body(child.steps()[0].chain().unwrap().child());
    assert_eq!(
        child
            .imports()
            .iter()
            .filter(|slot| **slot == source)
            .count(),
        1
    );
    assert_eq!(
        leaf.imports()
            .iter()
            .filter(|slot| **slot == source)
            .count(),
        1
    );
}

#[test]
fn b04_unexported_child_and_tainted_sources_are_not_candidates() {
    // 未 Export 的 child 输出：child 内部产生 Amount，但 chain 只声明 unit 输出。
    let (builder, root, handles) = builder_with_inputs(&[TypeDesc::of::<Plain>()]);
    let mut flow = open_flow(&builder, root);
    flow.chain(|sub| {
        sub.then_auto(amount_from_plain);
        ()
    });
    flow.then_auto(add_one);
    drop(flow);
    let errors = builder.borrow().errors().to_vec();
    let _ = handles;
    assert!(
        matches!(
            errors.first(),
            Some(BuildErrorDetail::MissingSource { type_desc, slot, .. })
                if *type_desc == TypeDesc::of::<Amount>() && *slot == Some(0)
        ),
        "child-local Amount is not visible to the parent auto query: {errors:?}"
    );

    // tainted 来源：显式登记的失败 Node 输出不得成为自动输入。
    let (builder, root, handles) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let legal_position = handles[0].position;
    let mut flow = open_flow(&builder, root);
    let missing = Ref::<Plain>::from_handle(builder.borrow().missing_handle());
    // 显式连接失败 → 该 Node 的 Amount 输出被标记为失效。
    let tainted = flow.then_with_output(&amount_from_plain, missing);
    let tainted_position = tainted.handle().position;
    assert!(
        builder
            .borrow()
            .errors()
            .iter()
            .any(|error| matches!(error, BuildErrorDetail::ForeignDefinition { .. })),
        "failed explicit input is recorded"
    );
    // 自动查询必须跳过 tainted 来源：Root 的合法 Amount 仍是最近可用来源。
    flow.then_auto(add_one);
    let errors = builder.borrow().errors().to_vec();
    assert!(
        !errors
            .iter()
            .any(|error| matches!(error, BuildErrorDetail::MissingSource { .. })),
        "legal root source still satisfies the auto query: {errors:?}"
    );
    let (step_count, auto_input, uses_tainted) = {
        let state = builder.borrow();
        let steps = state.body(root).steps();
        (
            steps.len(),
            steps[1].inputs()[0].handle.position,
            steps[1]
                .inputs()
                .iter()
                .any(|desc| desc.handle.position == tainted_position),
        )
    };
    assert_eq!(
        step_count, 2,
        "failing explicit step and auto step are registered"
    );
    assert_eq!(
        auto_input, legal_position,
        "auto query resolves the legal root source"
    );
    assert!(!uses_tainted, "tainted handle never becomes an auto input");
}

// ---------------------------------------------------------------- B05 重复类型拒绝

async fn same_pair(query: Query<(&Amount, &Amount)>) -> Result<Key, BodyError> {
    let (left, right) = query.get();
    Ok(Key(left.0 * 100 + right.0))
}

async fn spread_duplicate(query: Query<(&Amount, &Doubled, &Amount)>) -> Result<Key, BodyError> {
    let (first, doubled, last) = query.get();
    Ok(Key(first.0 * 1000 + doubled.0 * 10 + last.0))
}

/// 双槽同类型输入的 Struct Node。
struct DuplicatePair;

impl Node for DuplicatePair {
    type Input = (Amount, Amount);
    type Output = Key;

    async fn run(&self, query: Query<&(Amount, Amount)>) -> Result<Key, BodyError> {
        let pair = query.get();
        Ok(Key(pair.0.0 * 100 + pair.1.0))
    }
}

/// 非相邻重复槽位的 Struct Node。
struct SpreadDuplicate;

impl Node for SpreadDuplicate {
    type Input = (Amount, Doubled, Amount);
    type Output = Key;

    async fn run(&self, query: Query<&(Amount, Doubled, Amount)>) -> Result<Key, BodyError> {
        let triple = query.get();
        Ok(Key(triple.0.0 * 1000 + triple.1.0 * 10 + triple.2.0))
    }
}

#[test]
fn b05_duplicate_auto_query_is_rejected_by_every_auto_entry() {
    let sources = [TypeDesc::of::<Amount>(), TypeDesc::of::<Doubled>()];
    for entry in ["ordinary", "output"] {
        for shape in [
            "function_adjacent",
            "function_spread",
            "struct_adjacent",
            "struct_spread",
        ] {
            let (builder, root, _handles) = builder_with_inputs(&sources);
            let definition_id = builder.borrow().definition_id();
            let before = builder.borrow().next_position_id();
            let mut flow = open_flow(&builder, root);
            let returned = match (entry, shape) {
                ("ordinary", "function_adjacent") => {
                    flow.then_auto(same_pair);
                    None
                }
                ("ordinary", "function_spread") => {
                    flow.then_auto(spread_duplicate);
                    None
                }
                ("ordinary", "struct_adjacent") => {
                    flow.then_auto(DuplicatePair);
                    None
                }
                ("ordinary", "struct_spread") => {
                    flow.then_auto(SpreadDuplicate);
                    None
                }
                ("output", "function_adjacent") => Some(flow.then_auto_output(&same_pair).handle()),
                ("output", "function_spread") => {
                    Some(flow.then_auto_output(&spread_duplicate).handle())
                }
                ("output", "struct_adjacent") => {
                    Some(flow.then_auto_output(DuplicatePair).handle())
                }
                _ => Some(flow.then_auto_output(SpreadDuplicate).handle()),
            };
            let after = builder.borrow().next_position_id();
            let steps = builder.borrow().body(root).steps().len();
            let errors = builder.borrow().errors().to_vec();
            drop(flow);
            let expected_second_slot = if shape.ends_with("spread") { 2 } else { 1 };
            assert_eq!(
                errors.first(),
                Some(&BuildErrorDetail::DuplicateQuery {
                    body: root,
                    type_desc: TypeDesc::of::<Amount>(),
                    first_slot: 0,
                    second_slot: expected_second_slot,
                }),
                "{entry}/{shape}: duplicate type must be the identifiable first cause"
            );
            assert_eq!(
                steps, 0,
                "{entry}/{shape}: failed auto step is not registered"
            );
            assert_eq!(
                after, before,
                "{entry}/{shape}: no output position is allocated for a failed auto step"
            );
            if let Some(handle) = returned {
                assert_ne!(
                    handle.definition, definition_id,
                    "{entry}/{shape}: failed output handle has no current-Definition identity"
                );
            }
            let finish = finish_builder(builder, root);
            assert!(
                matches!(finish, Err(BuildErrorDetail::DuplicateQuery { .. })),
                "{entry}/{shape}: build must reject"
            );
        }
    }
}

#[test]
fn b05_duplicate_query_without_sources_still_reports_duplicate_first() {
    let (builder, root, _handles) = builder_with_inputs(&[]);
    let mut flow = open_flow(&builder, root);
    flow.then_auto(same_pair);
    drop(flow);
    let errors = builder.borrow().errors().to_vec();
    assert!(
        matches!(
            errors.first(),
            Some(BuildErrorDetail::DuplicateQuery { .. })
        ),
        "duplicate check precedes per-slot source lookup: {errors:?}"
    );
    assert!(
        !errors
            .iter()
            .any(|error| matches!(error, BuildErrorDetail::MissingSource { .. })),
        "duplicate failure does not additionally look up sources: {errors:?}"
    );
}

#[test]
fn b05_duplicate_query_rejects_before_any_business_execution() {
    DUP_RUNTIME_RUNS.store(0, Ordering::SeqCst);
    let runtime = Runtime;
    let error = block_on(runtime.execute(
        |flow, _input| flow.then_auto_output(&counting_same_pair),
        (Amount(1), Amount(2)),
    ))
    .expect_err("duplicate automatic query must fail the Definition");
    match error {
        RunError::Definition(build) => {
            let text = format!("{build}");
            assert!(
                text.contains("more than one input slot"),
                "diagnostic text keeps the duplicate reason: {text}"
            );
        }
        other => panic!("expected Definition rejection, got {other:?}"),
    }
    assert_eq!(DUP_RUNTIME_RUNS.load(Ordering::SeqCst), 0);
}

static DUP_RUNTIME_RUNS: AtomicUsize = AtomicUsize::new(0);

async fn counting_same_pair(query: Query<(&Amount, &Amount)>) -> Result<Key, BodyError> {
    DUP_RUNTIME_RUNS.fetch_add(1, Ordering::SeqCst);
    same_pair(query).await
}

// ---------------------------------------------------------------- B06 显式同类型合法

#[test]
fn b06_explicit_same_type_inputs_stay_legal() {
    let (builder, root, handles) =
        builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Amount>()]);
    let first = Ref::<Amount>::from_handle(handles[0]);
    let second = Ref::<Amount>::from_handle(handles[1]);
    let mut flow = open_flow(&builder, root);
    // 显式 (a,b)：分别读两份值。
    let _ = flow.then_with_output(&same_pair, (first, second));
    // 显式 (a,a)：同一 Ref 重复只读借用。
    let _ = flow.then_with_output(&same_pair, (first, first));
    drop(flow);
    let definition = finish_builder(builder, root).expect("explicit same-type inputs stay legal");
    let steps = definition.root().steps();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].inputs()[0].handle.position, handles[0].position);
    assert_eq!(steps[0].inputs()[1].handle.position, handles[1].position);
    assert_eq!(steps[1].inputs()[0].handle.position, handles[0].position);
    assert_eq!(steps[1].inputs()[1].handle.position, handles[0].position);

    // 普通形式与取输出形式都运行；两份不同来源取自 Root 双参数（同一 Ref 重复只读借用）。
    let runtime = Runtime;
    let (pair_ab, pair_aa) = block_on(runtime.execute(
        |flow, (first, second)| {
            let distinct = flow.then_with_output(&same_pair, (first, second));
            let repeated = flow.then_with_output(&same_pair, (first, first));
            flow.then_with(same_pair, (second, first));
            (distinct, repeated)
        },
        (Amount(1), Amount(2)),
    ))
    .expect("explicit same-type inputs should run");
    assert_eq!(pair_ab, Key(100 + 2));
    assert_eq!(pair_aa, Key(100 + 1));
}

// ---------------------------------------------------------------- B07 失败隔离

#[test]
fn b07_duplicate_failure_is_isolated_and_not_repaired_later() {
    let (builder, root, handles) =
        builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Amount>()]);
    let definition_id = builder.borrow().definition_id();
    let before = builder.borrow().next_position_id();
    let mut flow = open_flow(&builder, root);
    let failed = flow.then_auto_output(&same_pair);
    let failed_handle = failed.handle();
    assert_eq!(
        builder.borrow().next_position_id(),
        before,
        "failed auto step publishes no position"
    );
    assert!(builder.borrow().body(root).steps().is_empty());
    // 失败结果不能被查成合法来源：查询同类型只得到哨兵，且不产生新位置。
    let looked = flow.ref_of::<Key>();
    assert_eq!(looked.handle().definition, failed_handle.definition);
    // 后续出现合法同类型来源：新来源可用，旧失败句柄依旧无效。
    flow.then_with_output(&key_from_amount, Ref::<Amount>::from_handle(handles[0]));
    let repaired = flow.ref_of::<Key>();
    assert_ne!(repaired.handle(), failed_handle);
    assert_ne!(failed_handle.definition, definition_id);
    let new_source = {
        let state = builder.borrow();
        state.body(root).steps()[0].outputs()[0]
    };
    assert_eq!(repaired.handle().position, new_source);
    drop(flow);
    assert!(
        matches!(
            finish_builder(builder, root),
            Err(BuildErrorDetail::DuplicateQuery { .. })
        ),
        "the earlier duplicate failure keeps the build rejected"
    );
}

#[test]
fn b07_partial_missing_source_publishes_no_half_result() {
    let (builder, root, _handles) = builder_with_inputs(&[TypeDesc::of::<Amount>()]);
    let before = builder.borrow().next_position_id();
    let mut flow = open_flow(&builder, root);
    // 双槽函数：Amount 槽可解析、Doubled 槽无来源。
    let failed = flow.then_auto_output(&combine);
    let failed_handle = failed.handle();
    assert!(builder.borrow().body(root).steps().is_empty());
    assert_eq!(
        builder.borrow().next_position_id(),
        before,
        "partial resolution publishes no output slot"
    );
    let errors = builder.borrow().errors().to_vec();
    assert_eq!(
        errors.first(),
        Some(&BuildErrorDetail::MissingSource {
            body: root,
            slot: Some(1),
            type_desc: TypeDesc::of::<Doubled>(),
        }),
        "missing slot keeps its target reason: {errors:?}"
    );
    assert_ne!(failed_handle.definition, builder.borrow().definition_id());
    drop(flow);
    assert!(finish_builder(builder, root).is_err());
}

#[test]
fn b07_old_root_finish_stays_panic_free_on_failed_output() {
    let runtime = Runtime;
    let error = block_on(runtime.execute(
        |flow, _input| {
            // 旧收尾：失败句柄仍作为 Root 闭包返回值声明输出。
            flow.then_auto_output(&counting_same_pair)
        },
        (Amount(1), Amount(1)),
    ))
    .expect_err("failed auto output must not build a Definition");
    match error {
        RunError::Definition(build) => {
            let text = format!("{build}");
            assert!(
                text.contains("more than one input slot"),
                "target reason survives the old Root finish: {text}"
            );
        }
        other => panic!("expected Definition rejection, got {other:?}"),
    }
}

// ---------------------------------------------------------------- B08 借用与非 Send

/// 短借用 + 非 Send 依赖的 Struct Node：持有调用方局部的 `&Cell` 与 `Rc`。
struct BorrowingNode<'a> {
    counter: &'a std::cell::Cell<u32>,
    shared: Rc<std::cell::Cell<u32>>,
}

impl Node for BorrowingNode<'_> {
    type Input = Amount;
    type Output = Counted;

    async fn run(&self, query: Query<&Amount>) -> Result<Counted, BodyError> {
        // 借用输入跨过真实 await 点，且不使用任何 Send 约束。
        let value = query.get().0;
        yield_once().await;
        self.counter.set(self.counter.get() + 1);
        let shared = self.shared.get() + 1;
        self.shared.set(shared);
        Ok(Counted(value + self.counter.get() + shared))
    }
}

/// 自唤醒的一次挂起：验证借用跨真实 `await`（非自动完成的就绪 Future）。
async fn yield_once() {
    struct Gate(bool);
    impl std::future::Future for Gate {
        type Output = ();
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if self.0 {
                std::task::Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
    Gate(false).await
}

#[test]
fn b08_short_borrow_non_send_and_await_borrow_stay_usable() {
    DROP_LOG.with(|log| log.borrow_mut().clear());
    let local = std::cell::Cell::new(0u32);
    let shared = Rc::new(std::cell::Cell::new(0u32));
    let runtime = Runtime;
    let value = block_on(runtime.execute(
        |flow, _input| {
            let node = BorrowingNode {
                counter: &local,
                shared: Rc::clone(&shared),
            };
            flow.then_auto_output(node)
        },
        Amount(5),
    ))
    .expect("short borrows and non-Send captures stay usable");
    assert_eq!(value, Counted(5 + 1 + 1));
    assert_eq!(local.get(), 1);
    assert_eq!(shared.get(), 1);
}

// ---------------------------------------------------------------- B08 补充正例（审查 R03）

/// 含 `Rc` 的非 Send Data：作为自动输入与自动输出真实运行。
#[derive(Debug, PartialEq, Eq)]
struct RcCell(Rc<std::cell::Cell<u32>>);
impl Data for RcCell {}

async fn bump_rc_cell(query: Query<&RcCell>) -> Result<RcCell, BodyError> {
    let cell = &query.get().0;
    cell.set(cell.get() + 1);
    Ok(RcCell(Rc::clone(cell)))
}

async fn read_rc_cell(query: Query<&RcCell>) -> Result<Key, BodyError> {
    Ok(Key(query.get().0.get()))
}

#[test]
fn b08_non_send_data_runs_through_auto_entries() {
    let cell = Rc::new(std::cell::Cell::new(1u32));
    let runtime = Runtime;
    let (bumped, read) = block_on(runtime.execute(
        |flow, _input| {
            // 非 Send Data（含 Rc）作为自动输入与输出；后一步按最近来源读同一份数据。
            let bumped = flow.then_auto_output(&bump_rc_cell);
            let read = flow.then_auto_output(&read_rc_cell);
            (bumped, read)
        },
        RcCell(Rc::clone(&cell)),
    ))
    .expect("non-Send data should flow through the auto entries");
    assert_eq!(read, Key(2));
    assert_eq!(
        bumped.0.get(),
        2,
        "returned Data shares the captured Rc target"
    );
    assert_eq!(cell.get(), 2);
}

/// 借用跨真实 Pending 保存：恢复后仍从同一输入引用读取。
async fn read_after_pending(query: Query<&Counted>) -> Result<Key, BodyError> {
    let held = query.get();
    yield_once().await;
    Ok(Key(held.0 + 1))
}

#[test]
fn b08_input_borrow_survives_real_pending_and_is_used_after_resume() {
    let runtime = Runtime;
    let value = block_on(runtime.execute(
        |flow, _input| flow.then_auto_output(&read_after_pending),
        Counted(41),
    ))
    .expect("borrow across a real Pending should be resumed and used");
    assert_eq!(value, Key(42));
}

#[test]
fn b08_capturing_async_closure_derives_and_runs() {
    let offset = Rc::new(std::cell::Cell::new(7u32));
    let captured = Rc::clone(&offset);
    // 带捕获（Rc/Cell，非 Send）的 async closure：调用侧自然推导，无手填 marker。
    let node = async move |query: Query<&Amount>| -> Result<Key, BodyError> {
        Ok(Key(query.get().0 + captured.get()))
    };
    let runtime = Runtime;
    let keyed = block_on(runtime.execute(|flow, _input| flow.then_auto_output(node), Amount(5)))
        .expect("capturing async closure should derive and run");
    assert_eq!(keyed, Key(12));
    assert_eq!(offset.get(), 7, "capture is read, not consumed");
}

// ---------------------------------------------------------------- B09 同步 Factory
#[test]
fn b09_factory_is_not_called_by_building_or_by_unpolled_execute() {
    BUILD_CALLS.store(0, Ordering::SeqCst);
    FACTORY_CALLS.store(0, Ordering::SeqCst);
    // 纯 Definition 构建：定义闭包执行一次，Factory 零次。
    let (builder, root, _handles) = builder_with_inputs(&[]);
    let mut flow = open_flow(&builder, root);
    flow.provide(counting_factory);
    let _ = flow.provide_output(counting_factory);
    drop(flow);
    let definition = finish_builder(builder, root).expect("provide should build");
    assert_eq!(definition.root().steps().len(), 2);
    assert_eq!(
        FACTORY_CALLS.load(Ordering::SeqCst),
        0,
        "build calls no factory"
    );

    // 未 poll 时既不构建也不调用。
    let runtime = Runtime;
    let future = runtime.execute(
        |flow, _input| {
            BUILD_CALLS.fetch_add(1, Ordering::SeqCst);
            flow.provide(counting_factory);

            flow.provide_output(counting_factory)
        },
        (),
    );
    drop(future);
    assert_eq!(
        BUILD_CALLS.load(Ordering::SeqCst),
        0,
        "unpolled execute builds nothing"
    );
    assert_eq!(
        FACTORY_CALLS.load(Ordering::SeqCst),
        0,
        "unpolled execute calls no factory"
    );
}

static BUILD_CALLS: AtomicUsize = AtomicUsize::new(0);
static FACTORY_CALLS: AtomicUsize = AtomicUsize::new(0);

fn counting_factory() -> Counted {
    FACTORY_CALLS.fetch_add(1, Ordering::SeqCst);
    Counted(FACTORY_CALLS.load(Ordering::SeqCst) as u32)
}

#[test]
fn b09_factory_runs_at_its_step_only_and_returns_fresh_owned_data() {
    DROP_LOG.with(|log| log.borrow_mut().clear());
    FACTORY_CALLS.store(0, Ordering::SeqCst);
    let runtime = Runtime;
    block_on(runtime.execute(
        |flow, _input| {
            // 普通形式与取 Ref 形式各执行一步，各自创建新的 owned Data。
            flow.provide(counting_factory);
            let _ = flow.provide_output(counting_factory);
            ()
        },
        (),
    ))
    .expect("provide steps should run");
    assert_eq!(
        FACTORY_CALLS.load(Ordering::SeqCst),
        2,
        "one call per Step execution"
    );
    let drops = take_drops();
    assert_eq!(drops.len(), 2, "both owned values are cleaned: {drops:?}");
    let mut sorted = drops.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, vec![1, 2], "each Step owns its own non-Clone value");
}

#[test]
fn b09_provide_and_provide_output_register_the_same_step() {
    fn snapshot(use_output: bool) -> (usize, usize, Vec<TypeDesc>, Vec<u64>) {
        let (builder, root, _handles) = builder_with_inputs(&[]);
        let mut flow = open_flow(&builder, root);
        if use_output {
            let _ = flow.provide_output(|| Key(3));
        } else {
            flow.provide(|| Key(3));
        }
        drop(flow);
        let definition = finish_builder(builder, root).expect("provide should build");
        let steps = definition.root().steps();
        let types = steps[0]
            .outputs()
            .iter()
            .map(|position| {
                definition
                    .record(PositionHandle {
                        definition: definition.id(),
                        position: *position,
                    })
                    .expect("registered output")
                    .type_desc()
            })
            .collect::<Vec<_>>();
        (
            steps.len(),
            steps[0].inputs().len(),
            types,
            steps.iter().map(|step| step.order()).collect(),
        )
    }
    let ordinary = snapshot(false);
    let output = snapshot(true);
    assert_eq!(
        ordinary, output,
        "both Provide forms share one registration"
    );
    assert_eq!(ordinary.0, 1);
    assert_eq!(ordinary.1, 0, "Factory Node has zero inputs");
    assert_eq!(ordinary.2, vec![TypeDesc::of::<Key>()]);
}

#[test]
fn b09_factory_order_is_before_and_after_other_steps() {
    let events: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    let log = Rc::clone(&events);
    let factory = move || {
        log.borrow_mut().push("factory");
        Counted(1)
    };
    let log_before = Rc::clone(&events);
    let before = async move |_query: Query<()>| -> Result<(), BodyError> {
        log_before.borrow_mut().push("before");
        Ok(())
    };
    let runtime = Runtime;
    let _ = block_on(runtime.execute(
        |flow, _input| {
            let _ = flow.then_auto_output(before);
            let _ = flow.provide_output(factory);
            ()
        },
        (),
    ))
    .expect("factory ordering scenario should run");
    assert_eq!(*events.borrow(), vec!["before", "factory"]);
}

// ---------------------------------------------------------------- B13 合法 Fn 形式

#[test]
fn b13_legal_fn_forms_capture_and_repeat() {
    DROP_LOG.with(|log| log.borrow_mut().clear());
    // 借用捕获 + 内含 Cell 共享计数的 Fn 闭包不应被误判为 FnMut。
    let counter = std::cell::Cell::new(0u32);
    let borrowed = || {
        counter.set(counter.get() + 1);
        Counted(counter.get())
    };
    // Rc 与显式构造新值的合法业务代码。
    let shared = Rc::new(std::cell::Cell::new(10u32));
    let cloned = Rc::clone(&shared);
    let shared_factory = move || {
        cloned.set(cloned.get() + 1);
        Counted(cloned.get())
    };
    let runtime = Runtime;
    block_on(runtime.execute(
        |flow, _input| {
            // 借用捕获、Rc/Cell 非 Send 依赖与显式构造新值的合法业务代码。
            flow.provide(borrowed);
            flow.provide(borrowed);
            flow.provide(shared_factory);
            ()
        },
        (),
    ))
    .expect("legal Fn factories should run");
    assert_eq!(counter.get(), 2, "borrowing Fn closure repeats");
    assert_eq!(shared.get(), 11);
    let drops = take_drops();
    assert_eq!(drops.len(), 3, "one owned value per Step: {drops:?}");
    let mut sorted = drops.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, vec![1, 2, 11]);
}

#[test]
fn b13_capture_and_returned_data_lifetimes_are_separate() {
    DROP_LOG.with(|log| log.borrow_mut().clear());
    CAPTURE_DROPS.store(0, Ordering::SeqCst);
    let runtime = Runtime;
    {
        let capture = CaptureGuard;
        let factory = move || {
            let _ = &capture;
            Counted(7)
        };
        block_on(runtime.execute(
            |flow, _input| {
                flow.provide(&factory);
                flow.provide(&factory);
                ()
            },
            (),
        ))
        .expect("capture lifetime scenario should run");
    }
    // 捕获对象的寿命与各次返回 Data 分开：捕获只 Drop 一次，返回 Data 每次各一次。
    assert_eq!(
        CAPTURE_DROPS.load(Ordering::SeqCst),
        1,
        "capture drops once"
    );
    assert_eq!(
        take_drops(),
        vec![7, 7],
        "returned data drops per execution"
    );
}

/// 捕获对象的 Drop 计数：与每次返回的 Data 分开统计。
struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        CAPTURE_DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

static CAPTURE_DROPS: AtomicUsize = AtomicUsize::new(0);

// ---------------------------------------------------------------- B14 链式与借用规则

#[test]
fn b14_chained_auto_provide_and_explicit_steps_run_in_order() {
    let runtime = Runtime;
    let (chained, from_factory, explicit_again) = block_on(runtime.execute(
        |flow, (amount, _plain)| {
            // 连续链式登记：自动（普通形式）→ 显式；两步各自按自身 Query 选择输入。
            let chained = flow
                .then_auto(add_one)
                .then_with_output(&key_from_amount, amount);
            let chained_again = flow.provide_output(|| Key(1));
            let explicit = flow.then_with_output(&key_from_amount, amount);
            (chained, chained_again, explicit)
        },
        (Amount(1), Plain(2)),
    ))
    .expect("chained auto/provide/explicit steps should run");
    assert_eq!(chained, Key(1), "explicit step reads the connected source");
    assert_eq!(from_factory, Key(1));
    assert_eq!(explicit_again, Key(1));

    // 结构：三步登记在同一 Body，定义顺序严格递增；自动步的输入是自身 Query 的来源。
    let (builder, root, handles) =
        builder_with_inputs(&[TypeDesc::of::<Amount>(), TypeDesc::of::<Plain>()]);
    let amount = Ref::<Amount>::from_handle(handles[0]);
    let mut flow = open_flow(&builder, root);
    let _ = flow.then_auto_output(&add_one);
    let _ = flow.provide_output(|| Key(1));
    let _ = flow.then_with_output(&key_from_amount, amount);
    drop(flow);
    let definition = finish_builder(builder, root).expect("chained registration should build");
    let steps = definition.root().steps();
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0].inputs()[0].handle.position, handles[0].position);
    assert!(
        steps[0].order() < steps[1].order() && steps[1].order() < steps[2].order(),
        "chain keeps definition order"
    );
}

#[test]
fn b14_saved_ref_and_output_with_are_the_equivalent_form() {
    // 嵌套写法（同一表达式内两次可变借用）由编译夹具 v22_t02_e0499_nested.rs 拒绝；
    // 分开保存 Ref 的等价写法合法。
    let (builder, root, _handles) = builder_with_inputs(&[]);
    let mut flow = open_flow(&builder, root);
    let saved = flow.provide_output(|| Key(5));
    flow.output_with(saved);
    drop(flow);
    let definition =
        finish_builder(builder, root).expect("saved ref plus output_with should build");
    let declaration = definition.root().output_declaration();
    assert_eq!(declaration.slots.len(), 1);
    assert_eq!(
        declaration.slots[0].handle.position,
        definition.root().steps()[0].outputs()[0]
    );
}
