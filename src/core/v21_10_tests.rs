//! V21-10 验收样本（一）：`Runtime::execute` 的正常路径。
//!
//! 覆盖 K01～K03、K06～K11、K18、K19、K21、K22 的主要路径；DEFENCE 样本见
//! `v21_10_tests_defence.rs`。所有正常样本都经真实 `Runtime::execute`，不使用手工
//! Root observer 替代输入／输出边界。

use super::builder::TypedCallBuilder;
use super::context::{BodyError, TerminationKind};
use super::data_ref::DataRef;
use super::each::{Each, EachBuilder, EachOnly};
use super::flow::{Flow, FlowBuilder};
use super::internal_error::ScopeError;
use super::loop_orchestrator::{Loop, LoopBuilder, LoopControl, LoopDecision, Retry1};
use super::match_orchestrator::{Match, MatchBuilder};
use super::node::NodeCall1;
use super::runtime::{RootErrorStage, Runtime};
use super::scope::{ScopeState, TargetSnapshot};
use super::signature::{Data, NodeFut, NodeSig, OrchSig, Out2, SyncFnSig, Unit};
use super::test_support::{
    RootSnapshot, RootSnapshotPhase, advance_to_pending, boundary_address_reset,
    boundary_address_snapshot, closed_scope_snapshot, drive, drive_pinned, install_gate, record,
    release_gate, reset_observations, take_events, take_root_snapshots,
};

// ---------------------------------------------------------------- 业务类型（非 Clone，逐实例 Drop 见证）

/// 第一份 Root 输入。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Left {
    pub(crate) id: u32,
}

impl Drop for Left {
    fn drop(&mut self) {
        record(&format!("left-dropped:{}", self.id));
    }
}

/// 第二份 Root 输入（类型与 `Left` 不同）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Right {
    pub(crate) id: u32,
}

impl Drop for Right {
    fn drop(&mut self) {
        record(&format!("right-dropped:{}", self.id));
    }
}

/// 第一类 Root 输出。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Product {
    pub(crate) value: u32,
}

impl Drop for Product {
    fn drop(&mut self) {
        record(&format!("product-dropped:{}", self.value));
    }
}

/// 第二类 Root 输出（与 `Product` 异构）。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Other {
    pub(crate) value: u64,
}

impl Drop for Other {
    fn drop(&mut self) {
        record(&format!("other-dropped:{}", self.value));
    }
}

/// 只用于临时值／Loop 状态的中间业务值。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Temp {
    pub(crate) value: u32,
}

impl Drop for Temp {
    fn drop(&mut self) {
        record(&format!("temp-dropped:{}", self.value));
    }
}

/// Loop（Retry）的最终状态：业务字段直接表达 Finish。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Finished {
    pub(crate) value: u32,
}

impl Drop for Finished {
    fn drop(&mut self) {
        record(&format!("finished-dropped:{}", self.value));
    }
}

impl LoopControl for Finished {
    fn loop_decision(&self) -> LoopDecision {
        LoopDecision::Finish
    }
}

// ---------------------------------------------------------------- Node

fn left_to_product(left: &Left) -> Result<Product, BodyError> {
    Ok(Product { value: left.id * 2 })
}

fn left_to_temp(left: &Left) -> Result<Temp, BodyError> {
    Ok(Temp { value: left.id })
}

async fn gated_left_to_product(left: &Left) -> Result<Product, BodyError> {
    super::test_support::gate_wait().await;
    Ok(Product { value: left.id * 2 })
}

/// 内部在真实输入借用处挂起的结构体 Node（K01／K09／K21／K22）。
struct GatedLeftNode;

impl NodeCall1<Left, Data<Product>> for GatedLeftNode {
    fn call<'a>(&'a self, left: &'a Left) -> NodeFut<'a, Product> {
        Box::pin(gated_left_to_product(left))
    }
}

fn right_to_other(right: &Right) -> Result<Other, BodyError> {
    record("other-step-ran");
    Ok(Other {
        value: u64::from(right.id) + 100,
    })
}

fn left_id(left: &Left) -> Result<u32, BodyError> {
    Ok(left.id)
}

fn left_as_u64(left: &Left) -> Result<u64, BodyError> {
    Ok(u64::from(left.id) + 1000)
}

fn pair_sum(left: &Left, right: &Right) -> Result<u32, BodyError> {
    Ok(left.id + right.id)
}

fn pair_deep(value: &(Left, Right)) -> Result<u32, BodyError> {
    Ok(value.0.id * 10 + value.1.id)
}

/// 结构体 unit Node：成功只表示完成动作，不产生 unit Data。
struct UnitNode;

impl NodeCall1<Left, Unit> for UnitNode {
    fn call<'a>(&'a self, left: &'a Left) -> NodeFut<'a, ()> {
        Box::pin(async move {
            let _ = left.id;
            Ok(())
        })
    }
}

fn failing_node(left: &Left) -> Result<u32, BodyError> {
    let _ = left;
    Err(BodyError::new("boom"))
}

fn later_step(left: &Left) -> Result<u32, BodyError> {
    record("later-step-ran");
    Ok(left.id + 1)
}

// ---------------------------------------------------------------- Root Flow 形状

/// 单输入 → 单 `Data<Product>`。
pub(crate) fn single_data_flow() -> Flow<(Left,), Data<Product>> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let out: DataRef<Product> = flow
        .then::<_, SyncFnSig<(Left,), Data<Product>>, _>(
            left_to_product as fn(&Left) -> Result<Product, BodyError>,
            left,
        )
        .expect("then");
    flow.finish::<Data<Product>, _>(out).expect("finish")
}

/// 单输入 → `Unit`，中间有一个临时值（K07）。
fn single_unit_with_temp_flow() -> Flow<(Left,), Unit> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let _temp: DataRef<Temp> = flow
        .then::<_, SyncFnSig<(Left,), Data<Temp>>, _>(
            left_to_temp as fn(&Left) -> Result<Temp, BodyError>,
            left,
        )
        .expect("then");
    flow.finish::<Unit, _>(()).expect("finish unit")
}

/// 结构体 unit Node 作为 Step 的 Unit Root（K07 对照）。
pub(crate) fn unit_node_flow() -> Flow<(Left,), Unit> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    flow.then::<_, NodeSig<(Left,), Unit>, _>(UnitNode, left)
        .expect("then unit node");
    flow.finish::<Unit, _>(()).expect("finish unit")
}

/// 零 Step、非空输入的 `Unit` Root（K07）。
fn zero_step_unit_flow() -> Flow<(Left,), Unit> {
    let (flow, _left) = FlowBuilder::<(Left,)>::start().expect("start");
    flow.finish::<Unit, _>(()).expect("finish unit")
}

/// 单输入 → `Out2<u32, u64>`（两个位置都来自同一声明输入）。
fn single_out2_flow() -> Flow<(Left,), Out2<u32, u64>> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let first: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left,), Data<u32>>, _>(
            left_id as fn(&Left) -> Result<u32, BodyError>,
            left.clone(),
        )
        .expect("then first");
    let second: DataRef<u64> = flow
        .then::<_, SyncFnSig<(Left,), Data<u64>>, _>(
            left_as_u64 as fn(&Left) -> Result<u64, BodyError>,
            left,
        )
        .expect("then second");
    flow.finish::<Out2<u32, u64>, _>((first, second))
        .expect("finish out2")
}

/// 单输入 → `Out2<u32, u32>`（同类型两个独立位置，验证顺序与独立实例）。
fn same_type_out2_flow() -> Flow<(Left,), Out2<u32, u32>> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let first: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left,), Data<u32>>, _>(
            left_id as fn(&Left) -> Result<u32, BodyError>,
            left.clone(),
        )
        .expect("then first");
    let second: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left,), Data<u32>>, _>(
            (|left: &Left| Ok(left.id + 1000)) as fn(&Left) -> Result<u32, BodyError>,
            left,
        )
        .expect("then second");
    flow.finish::<Out2<u32, u32>, _>((first, second))
        .expect("finish out2")
}

/// 双输入 → `Out2<Product, Other>`（异构输出）。
fn double_out2_flow() -> Flow<(Left, Right), Out2<Product, Other>> {
    let (mut flow, (left, right)) = FlowBuilder::<(Left, Right)>::start().expect("start");
    let first: DataRef<Product> = flow
        .then::<_, SyncFnSig<(Left,), Data<Product>>, _>(
            left_to_product as fn(&Left) -> Result<Product, BodyError>,
            left,
        )
        .expect("then product");
    let second: DataRef<Other> = flow
        .then::<_, SyncFnSig<(Right,), Data<Other>>, _>(
            right_to_other as fn(&Right) -> Result<Other, BodyError>,
            right,
        )
        .expect("then other");
    flow.finish::<Out2<Product, Other>, _>((first, second))
        .expect("finish out2")
}

/// 双输入 → 单 `Data<u32>`。
fn double_data_flow() -> Flow<(Left, Right), Data<u32>> {
    let (mut flow, (left, right)) = FlowBuilder::<(Left, Right)>::start().expect("start");
    let out: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left, Right), Data<u32>>, _>(
            pair_sum as fn(&Left, &Right) -> Result<u32, BodyError>,
            (left, right),
        )
        .expect("then");
    flow.finish::<Data<u32>, _>(out).expect("finish")
}

/// 双输入 → `Unit`，中间有一个被丢弃的临时值。
fn double_unit_with_temp_flow() -> Flow<(Left, Right), Unit> {
    let (mut flow, (left, right)) = FlowBuilder::<(Left, Right)>::start().expect("start");
    let _sum: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left, Right), Data<u32>>, _>(
            pair_sum as fn(&Left, &Right) -> Result<u32, BodyError>,
            (left, right),
        )
        .expect("then");
    flow.finish::<Unit, _>(()).expect("finish unit")
}

/// 单份 tuple 输入 → 单份 `Data<u32>`（K03）。
fn tuple_flow() -> Flow<((Left, Right),), Data<u32>> {
    let (mut flow, pair) = FlowBuilder::<((Left, Right),)>::start().expect("start");
    let out: DataRef<u32> = flow
        .then::<_, SyncFnSig<((Left, Right),), Data<u32>>, _>(
            pair_deep as fn(&(Left, Right)) -> Result<u32, BodyError>,
            pair,
        )
        .expect("then");
    flow.finish::<Data<u32>, _>(out).expect("finish")
}

/// 一个把输入原样重新暴露为输出的子 Flow（合法 alias 来源）。
fn identity_flow() -> Flow<(Left,), Data<Left>> {
    let (flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    flow.finish::<Data<Left>, _>(left).expect("finish")
}

/// 双输入，输出直接选第一份 Root 输入（K06）。
fn select_first_input_flow() -> Flow<(Left, Right), Data<Left>> {
    let (flow, (left, _right)) = FlowBuilder::<(Left, Right)>::start().expect("start");
    flow.finish::<Data<Left>, _>(left).expect("finish")
}

/// 输出由一个子 Flow 的 imported alias 绑定（K08：同一 DataId 两个本地 Ref）。
fn aliased_input_flow() -> Flow<(Left,), Data<Left>> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let aliased: DataRef<Left> = flow
        .then::<_, OrchSig<Left, Data<Left>>, _>(identity_flow(), left)
        .expect("then alias");
    flow.finish::<Data<Left>, _>(aliased).expect("finish")
}

/// 两个不同声明输出位置都 alias 同一 Root 输入实例（K11）。
pub(crate) fn two_alias_flow() -> Flow<(Left,), Out2<Left, Left>> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let first: DataRef<Left> = flow
        .then::<_, OrchSig<Left, Data<Left>>, _>(identity_flow(), left.clone())
        .expect("then first alias");
    let second: DataRef<Left> = flow
        .then::<_, OrchSig<Left, Data<Left>>, _>(identity_flow(), left)
        .expect("then second alias");
    flow.finish::<Out2<Left, Left>, _>((first, second))
        .expect("finish out2")
}

/// 叶子 Flow：单个在输入借用处挂起的 Node（挂起发生在 child 调用内）。
pub(crate) fn gated_leaf_flow() -> Flow<(Left,), Data<Product>> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let out: DataRef<Product> = flow
        .then::<_, NodeSig<(Left,), Data<Product>>, _>(GatedLeftNode, left)
        .expect("then gated");
    flow.finish::<Data<Product>, _>(out).expect("finish")
}

/// 一层真实 SubFlow（内部是挂起的叶子 Flow）：K09 地址证据、K21、K22。
fn subflow_gated_flow() -> Flow<(Left,), Data<Product>> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let out: DataRef<Product> = flow
        .then::<_, OrchSig<Left, Data<Product>>, _>(gated_leaf_flow(), left)
        .expect("then subflow");
    flow.finish::<Data<Product>, _>(out).expect("finish")
}

/// 主场景 Root Flow：两份异构输入 → 一层 SubFlow → 两个异构新输出（K01）。
fn main_scenario_flow() -> Flow<(Left, Right), Out2<Product, Other>> {
    let (mut flow, (left, right)) = FlowBuilder::<(Left, Right)>::start().expect("start");
    let product: DataRef<Product> = flow
        .then::<_, OrchSig<Left, Data<Product>>, _>(gated_leaf_flow(), left)
        .expect("then subflow");
    let other: DataRef<Other> = flow
        .then::<_, SyncFnSig<(Right,), Data<Other>>, _>(
            right_to_other as fn(&Right) -> Result<Other, BodyError>,
            right,
        )
        .expect("then other");
    flow.finish::<Out2<Product, Other>, _>((product, other))
        .expect("finish out2")
}

/// 第一个 Step 失败、第二个 Step 有副作用（K19）。
fn failing_then_later_flow() -> Flow<(Left,), Data<u32>> {
    let (mut flow, left) = FlowBuilder::<(Left,)>::start().expect("start");
    let _failing: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left,), Data<u32>>, _>(
            failing_node as fn(&Left) -> Result<u32, BodyError>,
            left.clone(),
        )
        .expect("then failing");
    let later: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Left,), Data<u32>>, _>(
            later_step as fn(&Left) -> Result<u32, BodyError>,
            left,
        )
        .expect("then later");
    flow.finish::<Data<u32>, _>(later).expect("finish")
}

/// 最后一个观察点。
fn last_snapshot(snapshots: &[RootSnapshot], phase: RootSnapshotPhase) -> &RootSnapshot {
    snapshots
        .iter()
        .rev()
        .find(|snapshot| snapshot.phase == phase)
        .unwrap_or_else(|| panic!("missing snapshot {phase:?}: {snapshots:?}"))
}

fn count_events(events: &[String], prefix: &str) -> usize {
    events
        .iter()
        .filter(|event| event.starts_with(prefix))
        .count()
}

// ---------------------------------------------------------------- K01／K02／K03

#[test]
fn k01_main_scenario_pending_then_ready_with_two_heterogeneous_outputs() {
    reset_observations();
    boundary_address_reset();
    install_gate();
    let root = main_scenario_flow();

    let (product, other) = {
        let mut future = Box::pin(Runtime::execute::<_, _, Out2<Product, Other>>(
            &root,
            (Left { id: 3 }, Right { id: 4 }),
        ));
        let mut pending = 0usize;
        loop {
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            match future.as_mut().poll(&mut cx) {
                std::task::Poll::Pending => {
                    pending += 1;
                    assert!(pending < 8, "只有被挂起的 Node 会让出执行");
                    let events = take_events();
                    assert!(
                        !events.iter().any(|event| event == "other-step-ran"),
                        "Pending 时后续 Step 未执行: {events:?}"
                    );
                    assert_eq!(
                        boundary_address_snapshot().len(),
                        1,
                        "SubFlow 在挂起前已建立"
                    );
                    release_gate();
                }
                std::task::Poll::Ready(result) => {
                    break result.expect("main scenario succeeds");
                }
            }
        }
    };

    assert_eq!(product.value, 6, "第一个 owned 输出");
    assert_eq!(other.value, 104, "第二个 owned 输出");

    // 唯一执行域：一层 SubFlow 建立并关闭，Root 最后关闭。
    let closed = closed_scope_snapshot();
    assert_eq!(closed.len(), 2, "一层 child 与 Root 各关闭一次: {closed:?}");
    assert!(closed[0].seq() > closed[1].seq(), "最深 descendant 先关闭");

    // 两份返回值在 Application 丢弃前都不析构；两份输入各析构一次。
    let events = take_events();
    assert_eq!(count_events(&events, "product-dropped"), 0);
    assert_eq!(count_events(&events, "other-dropped"), 0);
    assert_eq!(count_events(&events, "left-dropped"), 1, "{events:?}");
    assert_eq!(count_events(&events, "right-dropped"), 1, "{events:?}");
    drop(product);
    drop(other);
    let events = take_events();
    assert_eq!(count_events(&events, "product-dropped"), 1);
    assert_eq!(count_events(&events, "other-dropped"), 1);

    // 每个声明输出的 responsibility 移除与 take 顺序一致。
    let snapshots = take_root_snapshots();
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(after_close.planned_takes, 2);
    assert_eq!(
        after_close.taken_alive,
        vec![false, false],
        "{after_close:?}"
    );
    assert_eq!(
        after_close.taken_owned_by,
        vec![None, None],
        "Root 不再负责已移交实例: {after_close:?}"
    );
    assert_eq!(after_close.root_owned.len(), 0);
    assert_eq!(after_close.root_state, ScopeState::Closed);
}

#[test]
fn k02_six_combinations_of_inputs_and_outputs() {
    reset_observations();
    let product = drive(Runtime::execute::<_, _, Data<Product>>(
        &single_data_flow(),
        (Left { id: 5 },),
    ))
    .expect("single data");
    assert_eq!(product.value, 10);

    drive(Runtime::execute::<_, _, Unit>(
        &single_unit_with_temp_flow(),
        (Left { id: 6 },),
    ))
    .expect("single unit");

    let (first, second) = drive(Runtime::execute::<_, _, Out2<u32, u64>>(
        &single_out2_flow(),
        (Left { id: 7 },),
    ))
    .expect("single out2");
    assert_eq!((first, second), (7, 1007), "按声明顺序");

    let sum = drive(Runtime::execute::<_, _, Data<u32>>(
        &double_data_flow(),
        (Left { id: 8 }, Right { id: 9 }),
    ))
    .expect("double data");
    assert_eq!(sum, 17);

    drive(Runtime::execute::<_, _, Unit>(
        &double_unit_with_temp_flow(),
        (Left { id: 10 }, Right { id: 11 }),
    ))
    .expect("double unit");

    let (product, other) = drive(Runtime::execute::<_, _, Out2<Product, Other>>(
        &double_out2_flow(),
        (Left { id: 12 }, Right { id: 13 }),
    ))
    .expect("double out2");
    assert_eq!(product.value, 24);
    assert_eq!(other.value, 113);

    let (first, second) = drive(Runtime::execute::<_, _, Out2<u32, u32>>(
        &same_type_out2_flow(),
        (Left { id: 14 },),
    ))
    .expect("same type out2");
    assert_eq!((first, second), (14, 1014), "同类型也按声明顺序");
    let snapshots = take_root_snapshots();
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(after_close.planned_takes, 2, "两个位置分别提取");
    assert_eq!(
        after_close.taken_alive,
        vec![false, false],
        "两个独立实例都已移出 Container: {after_close:?}"
    );
}

#[test]
fn k03_single_tuple_input_and_output_are_one_data() {
    reset_observations();
    let value = drive(Runtime::execute::<_, _, Data<u32>>(
        &tuple_flow(),
        ((Left { id: 2 }, Right { id: 3 }),),
    ))
    .expect("tuple root");
    assert_eq!(value, 23, "tuple 是一份业务 Data，不拆成两个输入");
    let snapshots = take_root_snapshots();
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(after_close.planned_takes, 1, "只提取一次");
    let events = take_events();
    assert_eq!(count_events(&events, "left-dropped"), 1);
    assert_eq!(count_events(&events, "right-dropped"), 1);
}

// ---------------------------------------------------------------- K06～K09、K11

#[test]
fn k06_output_selects_root_input_without_cloning_and_cleans_the_rest() {
    reset_observations();
    let left = drive(Runtime::execute::<_, _, Data<Left>>(
        &select_first_input_flow(),
        (Left { id: 21 }, Right { id: 22 }),
    ))
    .expect("select input");
    assert_eq!(left.id, 21, "原实例（无 Clone、无重建）");

    let snapshots = take_root_snapshots();
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(
        after_close.taken_alive,
        vec![false],
        "选中的输入已移出 Container: {after_close:?}"
    );

    let events = take_events();
    assert!(
        events.iter().any(|event| event == "right-dropped:22"),
        "{events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "left-dropped:21"),
        "选中输入随返回值移交，不由 Root cleanup 销毁: {events:?}"
    );
    drop(left);
    let events = take_events();
    assert_eq!(count_events(&events, "left-dropped"), 1);
}

#[test]
fn k07_zero_step_unit_root_with_non_empty_input() {
    reset_observations();
    drive(Runtime::execute::<_, _, Unit>(
        &zero_step_unit_flow(),
        (Left { id: 30 },),
    ))
    .expect("zero step unit");
    let snapshots = take_root_snapshots();
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(after_close.planned_takes, 0, "unit 零 take");
    assert_eq!(after_close.root_state, ScopeState::Closed, "Root 已关闭");
    let events = take_events();
    assert!(
        events.iter().any(|event| event == "left-dropped:30"),
        "非空输入仍分配 DataId 并在退出时清理: {events:?}"
    );

    // 有 Step 的 Unit Root：中间临时值被清理，没有 unit Data 被提取。
    reset_observations();
    drive(Runtime::execute::<_, _, Unit>(
        &single_unit_with_temp_flow(),
        (Left { id: 31 },),
    ))
    .expect("unit with temp");
    let events = take_events();
    assert!(
        events.iter().any(|event| event == "temp-dropped:31"),
        "{events:?}"
    );
    let snapshots = take_root_snapshots();
    assert_eq!(
        last_snapshot(&snapshots, RootSnapshotPhase::AfterClose).planned_takes,
        0
    );

    // 结构体 unit Node 对照：同样不产生 unit Data。
    reset_observations();
    drive(Runtime::execute::<_, _, Unit>(
        &unit_node_flow(),
        (Left { id: 32 },),
    ))
    .expect("unit node");
    let snapshots = take_root_snapshots();
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(after_close.planned_takes, 0);
    assert_eq!(after_close.taken_alive.len(), 0);
    let events = take_events();
    assert!(
        events.iter().any(|event| event == "left-dropped:32"),
        "{events:?}"
    );
}

#[test]
fn k08_alias_refs_share_one_input_and_only_one_is_selected() {
    reset_observations();
    let left = drive(Runtime::execute::<_, _, Data<Left>>(
        &aliased_input_flow(),
        (Left { id: 40 },),
    ))
    .expect("alias root");
    assert_eq!(left.id, 40, "imported alias 不复制实例");
    let snapshots = take_root_snapshots();
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(after_close.root_refs.len(), 0, "全部 Root 本地 Ref 失效");
    assert_eq!(after_close.root_owned.len(), 0, "责任只移除一次");
    assert_eq!(after_close.taken_owned_by, vec![None], "无悬空责任");
    drop(left);
    let events = take_events();
    assert_eq!(count_events(&events, "left-dropped"), 1, "恰好析构一次");
}

#[test]
fn k09_one_definition_runs_twice_with_independent_executions() {
    reset_observations();
    boundary_address_reset();
    install_gate();
    let root = subflow_gated_flow();
    let first = advance_to_pending(
        Runtime::execute::<_, _, Data<Product>>(&root, (Left { id: 50 },)),
        1,
    );
    let second = advance_to_pending(
        Runtime::execute::<_, _, Data<Product>>(&root, (Left { id: 51 },)),
        1,
    );
    let addresses = boundary_address_snapshot();
    assert_eq!(addresses.len(), 2, "两次 nested 执行域: {addresses:?}");
    assert!(
        !std::ptr::eq(addresses[0].1, addresses[1].1),
        "两次 Execution 身份互不相同: {addresses:?}"
    );
    assert!(
        !std::ptr::eq(addresses[0].3, addresses[1].3),
        "两次 Container 互不相同: {addresses:?}"
    );
    release_gate();
    let first = drive_pinned(first).expect("first execution");
    let second = drive_pinned(second).expect("second execution");
    assert_eq!(first.value, 100);
    assert_eq!(second.value, 102, "结果按各自输入重算");
    let events = take_events();
    assert_eq!(
        count_events(&events, "left-dropped"),
        2,
        "每次执行各自清理输入"
    );
}

#[test]
fn k11_two_distinct_declarations_of_one_data_id_are_rejected_before_take() {
    reset_observations();
    let error = drive(Runtime::execute::<_, _, Out2<Left, Left>>(
        &two_alias_flow(),
        (Left { id: 60 },),
    ))
    .expect_err("duplicate physical instance must be rejected");
    assert_eq!(error.stage(), RootErrorStage::Preflight, "{error:?}");
    let (first_ref, duplicate_ref, data_id) = match error.scope_error() {
        Some(ScopeError::DuplicateRootDataId {
            first_ref,
            duplicate_ref,
            data_id,
        }) => (first_ref.clone(), duplicate_ref.clone(), data_id.clone()),
        other => panic!("expected DuplicateRootDataId, got {other:?}"),
    };
    assert_ne!(first_ref, duplicate_ref, "两个不同声明位置");

    let snapshots = take_root_snapshots();
    let rejected = last_snapshot(&snapshots, RootSnapshotPhase::PreflightRejected);
    assert_eq!(rejected.planned_takes, 0, "拒绝前没有 take: {rejected:?}");
    // 两个位置的本地绑定都解析到同一物理实例，owner 仍是 Root。
    for position in [&first_ref, &duplicate_ref] {
        let target = rejected
            .root_refs
            .iter()
            .find(|(candidate, _)| candidate == position)
            .map(|(_, target)| target.clone())
            .expect("declared output position is bound");
        assert_eq!(
            target,
            TargetSnapshot::Data(data_id.clone()),
            "同一物理实例"
        );
    }
    assert_eq!(
        rejected.root_owned,
        vec![data_id.clone()],
        "owner 不重复转移"
    );
    assert_eq!(
        rejected.taken_alive,
        Vec::<bool>::new(),
        "拒绝时没有任何实例被移出"
    );
    let after_cleanup = last_snapshot(&snapshots, RootSnapshotPhase::AfterFailureCleanup);
    assert_eq!(
        after_cleanup.next_data_id, rejected.next_data_id,
        "拒绝与失败清理都不取新的 DataId 序号: {rejected:?}"
    );
    let events = take_events();
    assert_eq!(
        count_events(&events, "left-dropped"),
        1,
        "失败清理销毁输入一次"
    );
}

// ---------------------------------------------------------------- K18～K22

#[test]
fn k18_after_commit_container_and_root_state_are_settled() {
    reset_observations();
    let product = drive(Runtime::execute::<_, _, Data<Product>>(
        &single_data_flow(),
        (Left { id: 70 },),
    ))
    .expect("root data");
    assert_eq!(product.value, 140);
    let snapshots = take_root_snapshots();
    let after_commit = last_snapshot(&snapshots, RootSnapshotPhase::AfterCommit);
    assert_eq!(after_commit.planned_takes, 1);
    assert_eq!(
        after_commit.taken_alive,
        vec![false],
        "提交后 Container 已无该 DataId: {after_commit:?}"
    );
    assert_eq!(
        after_commit.taken_owned_by,
        vec![None],
        "Root 责任已解除: {after_commit:?}"
    );
    let after_close = last_snapshot(&snapshots, RootSnapshotPhase::AfterClose);
    assert_eq!(after_close.root_refs.len(), 0, "Root refs 失效");
    assert_eq!(after_close.root_owned.len(), 0, "Root owned 无对应责任");
    assert_eq!(after_close.root_state, ScopeState::Closed);
    assert_eq!(
        after_close.next_data_id, after_commit.next_data_id,
        "提交与关闭都不取新的 DataId 序号"
    );

    // Application 持值在 Context 析构后仍可用，再 drop 恰好一次。
    let events = take_events();
    assert_eq!(count_events(&events, "product-dropped"), 0);
    assert_eq!(product.value, 140);
    drop(product);
    let events = take_events();
    assert_eq!(count_events(&events, "product-dropped"), 1);
}

#[test]
fn k19_body_error_stops_later_steps_and_keeps_the_first_diagnostic() {
    reset_observations();
    let error = drive(Runtime::execute::<_, _, Data<u32>>(
        &failing_then_later_flow(),
        (Left { id: 80 },),
    ))
    .expect_err("body error");
    assert_eq!(error.stage(), RootErrorStage::Body, "{error:?}");
    assert_eq!(error.note(), "boom", "原始说明保留");
    assert_eq!(
        error.termination(),
        Some(TerminationKind::BodyError),
        "首次失败即终止"
    );
    assert!(
        error.termination_scope().is_some(),
        "保留最深实际定位: {error:?}"
    );
    let events = take_events();
    assert!(
        !events.iter().any(|event| event == "later-step-ran"),
        "错误后不执行后续 Step: {events:?}"
    );
    assert_eq!(count_events(&events, "left-dropped"), 1, "失败清理一次");
    let snapshots = take_root_snapshots();
    assert!(
        !snapshots
            .iter()
            .any(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit),
        "失败路径不发生提取提交"
    );
    let after_cleanup = last_snapshot(&snapshots, RootSnapshotPhase::AfterFailureCleanup);
    assert_eq!(
        after_cleanup.terminated,
        Some(TerminationKind::BodyError),
        "退出后仍记录首次终止: {after_cleanup:?}"
    );
}

#[test]
fn k21_pending_future_dropped_leaves_no_owned_output() {
    reset_observations();
    install_gate();
    let root = subflow_gated_flow();
    let boxed = advance_to_pending(
        Runtime::execute::<_, _, Data<Product>>(&root, (Left { id: 90 },)),
        1,
    );
    drop(boxed);
    let events = take_events();
    assert_eq!(
        count_events(&events, "product-dropped"),
        0,
        "丢弃未完成 Future 不产生 owned 输出: {events:?}"
    );
    assert_eq!(
        count_events(&events, "left-dropped"),
        1,
        "输入随取消清理一次"
    );
    assert!(
        !closed_scope_snapshot().is_empty(),
        "取消路径逐层关闭: {:?}",
        closed_scope_snapshot()
    );
    let snapshots = take_root_snapshots();
    let after_cleanup = last_snapshot(&snapshots, RootSnapshotPhase::AfterFailureCleanup);
    assert_eq!(
        after_cleanup.terminated,
        Some(TerminationKind::Cancelled),
        "未标记退出记录为取消: {after_cleanup:?}"
    );

    // 另一 Execution 不受影响。
    release_gate();
    let ok = drive(Runtime::execute::<_, _, Data<Product>>(
        &single_data_flow(),
        (Left { id: 91 },),
    ))
    .expect("independent execution succeeds");
    assert_eq!(ok.value, 182);
}

#[test]
fn k22_subflow_pending_cancellation_closes_each_layer() {
    reset_observations();
    boundary_address_reset();
    install_gate();
    let root = subflow_gated_flow();
    let boxed = advance_to_pending(
        Runtime::execute::<_, _, Data<Product>>(&root, (Left { id: 100 },)),
        1,
    );
    assert_eq!(boundary_address_snapshot().len(), 1, "SubFlow 已建立");
    drop(boxed);
    let closed = closed_scope_snapshot();
    assert_eq!(closed.len(), 2, "SubFlow 与 Root 逐层关闭: {closed:?}");
    assert!(closed[0].seq() > closed[1].seq(), "最深 descendant 先关闭");
    let events = take_events();
    assert_eq!(
        count_events(&events, "left-dropped"),
        1,
        "每实例恰好析构一次"
    );
    assert_eq!(count_events(&events, "product-dropped"), 0);
    release_gate();
}

// ---------------------------------------------------------------- K10：控制器直接作为 Root

#[test]
fn k10_flow_match_each_and_loop_as_root_share_one_boundary() {
    reset_observations();
    // Flow
    let product = drive(Runtime::execute::<_, _, Data<Product>>(
        &single_data_flow(),
        (Left { id: 110 },),
    ))
    .expect("flow root");
    assert_eq!(product.value, 220);

    // Each：真实 Vec 输出
    let mut each: EachBuilder<EachOnly<Left>, u32> = EachBuilder::start().expect("each");
    each.then_body::<_, SyncFnSig<(Left,), Data<u32>>>(
        left_id as fn(&Left) -> Result<u32, BodyError>,
    )
    .expect("each body");
    let each: Each<EachOnly<Left>, u32> = each.finish().expect("each finish");
    let values = drive(Runtime::execute::<_, _, Data<Vec<u32>>>(
        &each,
        (vec![Left { id: 1 }, Left { id: 2 }],),
    ))
    .expect("each root");
    assert_eq!(values, vec![1, 2], "Each 返回真实 Vec");

    // Match：命中 branch 的共同输出
    let mut matched: MatchBuilder<u32, Left, Data<u32>> = MatchBuilder::start().expect("match");
    matched
        .branch::<_, SyncFnSig<(Left,), Data<u32>>>(
            1,
            left_id as fn(&Left) -> Result<u32, BodyError>,
        )
        .expect("branch");
    let matched: Match<u32, Left, Data<u32>> = matched.finish().expect("match finish");
    let routed = drive(Runtime::execute::<_, _, Data<u32>>(
        &matched,
        (1, Left { id: 111 }),
    ))
    .expect("match root");
    assert_eq!(routed, 111);

    // Loop（Retry）：返回最终状态
    let mut loop_builder: LoopBuilder<Retry1<Temp, Finished>> = LoopBuilder::start().expect("loop");
    loop_builder
        .then_body::<_, SyncFnSig<(Temp,), Data<Finished>>>(
            finish_temp as fn(&Temp) -> Result<Finished, BodyError>,
        )
        .expect("loop body");
    let loop_root: Loop<Retry1<Temp, Finished>> = loop_builder.finish().expect("loop finish");
    let final_state = drive(Runtime::execute::<_, _, Data<Finished>>(
        &loop_root,
        (Temp { value: 120 },),
    ))
    .expect("loop root");
    assert_eq!(final_state.value, 120, "Loop 返回最终状态");

    // 四种 Root 共用同一 Root 边界：每次执行都关闭自己的 RootScope（seq 0），
    // 控制器只在自身语义需要时建立 child Scope。
    let closed = closed_scope_snapshot();
    assert_eq!(
        closed.iter().filter(|scope| scope.seq() == 0).count(),
        4,
        "每次执行的 RootScope 各关闭一次: {closed:?}"
    );
    assert!(closed.len() >= 4, "控制器 Scope 一并关闭: {closed:?}");
}

fn finish_temp(state: &Temp) -> Result<Finished, BodyError> {
    Ok(Finished { value: state.value })
}
