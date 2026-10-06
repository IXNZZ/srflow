//! V21-12 外部消费者验证：只通过 `srflow` 的公开导入使用 API。
//!
//! 覆盖 A02 基本使用、A03 适配边界、A04 Flow 接线与拒绝机制、A05 控制器、
//! A06 Root 所有权与错误。内部单元测试、`#[path]` 装配与类型存在性检查不在此替代。

use std::sync::Arc;

use futures::executor::block_on;
use srflow::{
    ArcNodeSig, AsyncFnSig, BodyError, BuildError, Data, DataRef, Each, EachBuilder, EachOnly,
    EachShared, Flow, FlowBuilder, FlowBuilder2, Iter1, Loop, LoopBuilder, LoopControl,
    LoopDecision, Match, MatchBuilder, NodeCall0, NodeCall1, NodeCall2, NodeFut, OrchSig, Out2,
    Retry1, RunErrorKind, RunErrorStage, Runtime, SyncFnSig, Unit,
};

// ---------------------------------------------------------------- 业务类型

/// 无 `Clone` 的业务 Data：证明接线与执行不要求业务类型可复制。
#[derive(Debug, PartialEq, Eq)]
struct Payload {
    id: u32,
    history: Vec<u32>,
}

#[derive(Debug, PartialEq, Eq)]
struct Doubled(u32);

#[derive(Debug, PartialEq, Eq)]
struct ItemResult(u32);

#[derive(Debug, PartialEq, Eq)]
struct Shared(u32);

#[derive(Debug, PartialEq, Eq)]
struct Counter {
    base: u32,
    rounds: u32,
}
impl LoopControl for Counter {
    fn loop_decision(&self) -> LoopDecision {
        if self.rounds >= 2 {
            LoopDecision::Finish
        } else {
            LoopDecision::Continue
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct AlreadyDone;
impl LoopControl for AlreadyDone {
    fn loop_decision(&self) -> LoopDecision {
        LoopDecision::Finish
    }
}

// ---------------------------------------------------------------- A02：基本使用

#[test]
fn a02_function_async_struct_and_arc_nodes_produce_owned_outputs() {
    fn bump(payload: &Payload) -> Result<Payload, BodyError> {
        let mut history = payload.history.clone();
        history.push(payload.id);
        Ok(Payload {
            id: payload.id + 1,
            history,
        })
    }

    async fn double(payload: &Payload) -> Result<Doubled, BodyError> {
        Ok(Doubled(payload.id * 2))
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Frozen(u32);
    impl NodeCall0<Data<Frozen>> for Frozen {
        fn call<'a>(&'a self) -> NodeFut<'a, Frozen> {
            Box::pin(async move { Ok(Frozen(self.0)) })
        }
    }

    struct Echo;
    impl NodeCall1<Doubled, Data<u32>> for Echo {
        fn call<'a>(&'a self, doubled: &'a Doubled) -> NodeFut<'a, u32> {
            Box::pin(async move { Ok(doubled.0) })
        }
    }

    let (mut flow, payload) = FlowBuilder::<Payload>::start().expect("start");
    let bumped: DataRef<Payload> = flow
        .then(bump as fn(&Payload) -> Result<Payload, BodyError>, payload)
        .expect("bump");
    let doubled: DataRef<Doubled> = flow
        .then::<_, AsyncFnSig<(Payload,), Data<Doubled>>, _>(double, bumped)
        .expect("double");
    let echoed: DataRef<u32> = flow
        .then::<_, ArcNodeSig<(Doubled,), Data<u32>>, _>(Arc::new(Echo), doubled.clone())
        .expect("echo");
    // 同一句柄可被多个 Step 重复读取（不要求业务 Clone），例如再读一次 doubled。
    let echoed_again: DataRef<u32> = flow
        .then::<_, ArcNodeSig<(Doubled,), Data<u32>>, _>(Arc::new(Echo), doubled)
        .expect("echo again");
    let frozen: DataRef<Frozen> = flow
        .then::<_, ArcNodeSig<(), Data<Frozen>>, _>(Arc::new(Frozen(9)), ())
        .expect("zero-input arc node");
    let sum: DataRef<u32> = flow
        .then::<_, SyncFnSig<(u32, u32), Data<u32>>, _>(
            (|a: &u32, b: &u32| Ok(a + b)) as fn(&u32, &u32) -> Result<u32, BodyError>,
            (echoed, echoed_again),
        )
        .expect("sum");
    let flow: Flow<(Payload,), Out2<u32, Frozen>> = flow
        .finish::<Out2<u32, Frozen>, _>((sum, frozen))
        .expect("finish");

    let (sum, frozen) = block_on(Runtime::execute(
        &flow,
        Payload {
            id: 3,
            history: Vec::new(),
        },
    ))
    .expect("execute");
    assert_eq!(sum, 16);
    assert_eq!(frozen, Frozen(9));
}

#[test]
fn a02_definition_reuse_creates_independent_executions() {
    let (mut flow, payload) = FlowBuilder::<Payload>::start().expect("start");
    let bumped: DataRef<u32> = flow
        .then::<_, SyncFnSig<(Payload,), Data<u32>>, _>(
            (|payload: &Payload| Ok(payload.id + 1)) as fn(&Payload) -> Result<u32, BodyError>,
            payload,
        )
        .expect("bump");
    let flow: Flow<(Payload,), Data<u32>> = flow.finish::<Data<u32>, _>(bumped).expect("finish");

    for id in [1u32, 7, 42] {
        let out = block_on(Runtime::execute(
            &flow,
            Payload {
                id,
                history: Vec::new(),
            },
        ))
        .expect("execute");
        assert_eq!(out, id + 1);
    }
}

// ---------------------------------------------------------------- A03：适配边界

#[test]
fn a03_plain_unit_functions_are_rejected_at_definition_build() {
    fn unit_one(_a: &u32) -> Result<(), BodyError> {
        Ok(())
    }
    async fn unit_zero() -> Result<(), BodyError> {
        Ok(())
    }

    let (mut flow, input) = FlowBuilder::<u32>::start().expect("start");
    let rejected = flow.then(unit_one as fn(&u32) -> Result<(), BodyError>, input);
    assert_eq!(
        rejected.unwrap_err(),
        BuildError::UnsupportedFunctionUnitOutput
    );
    // 零参数异步 unit 函数同样在定义构建期拒绝（类型层实例化成立、运行前拒绝）。
    let rejected_async = flow.then(unit_zero, ());
    assert_eq!(
        rejected_async.unwrap_err(),
        BuildError::UnsupportedFunctionUnitOutput
    );
}

#[test]
fn a03_struct_node_unit_output_and_async_node_are_supported() {
    struct Log;
    impl NodeCall1<u32, Unit> for Log {
        fn call<'a>(&'a self, _a: &'a u32) -> NodeFut<'a, ()> {
            Box::pin(async move { Ok(()) })
        }
    }

    struct AddUp;
    impl NodeCall2<u32, u32, Data<u32>> for AddUp {
        fn call<'a>(&'a self, a: &'a u32, b: &'a u32) -> NodeFut<'a, u32> {
            Box::pin(async move { Ok(a + b) })
        }
    }

    let (mut flow, (a, b)) = FlowBuilder2::<u32, u32>::start().expect("start");
    let sum: DataRef<u32> = flow
        .then::<_, ArcNodeSig<(u32, u32), Data<u32>>, _>(Arc::new(AddUp), (a, b))
        .expect("add up");
    flow.then::<_, srflow::NodeSig<(u32,), Unit>, _>(Log, sum)
        .expect("unit node step");
    let flow: Flow<(u32, u32), Unit> = flow.finish::<Unit, _>(()).expect("finish");
    block_on(Runtime::execute(&flow, (2u32, 5u32))).expect("execute");
}

// ---------------------------------------------------------------- A04：Flow 接线

#[test]
fn a04_shape_matrix_unit_data_out2_and_rejections() {
    // Data 输出
    let (mut data_flow, input) = FlowBuilder::<u32>::start().expect("start");
    let data: DataRef<u32> = data_flow
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(
            (|v: &u32| Ok(v + 1)) as fn(&u32) -> Result<u32, BodyError>,
            input,
        )
        .expect("data step");
    let data_flow: Flow<(u32,), Data<u32>> =
        data_flow.finish::<Data<u32>, _>(data).expect("finish");
    assert_eq!(block_on(Runtime::execute(&data_flow, 1u32)).unwrap(), 2);

    // 显式 Unit 输出
    let (mut unit_flow, input) = FlowBuilder::<u32>::start().expect("start");
    unit_flow
        .then::<_, srflow::NodeSig<(u32,), Unit>, _>(UnitNode, input)
        .expect("unit step");
    let unit_flow: Flow<(u32,), Unit> = unit_flow.finish::<Unit, _>(()).expect("finish");
    assert_eq!(block_on(Runtime::execute(&unit_flow, 1u32)).unwrap(), ());

    // 重复选择同一 RefId 为多个输出：Definition 构建期拒绝
    let (mut dup_flow, input) = FlowBuilder::<u32>::start().expect("start");
    let duplicated: DataRef<u32> = dup_flow
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(
            (|v: &u32| Ok(v + 1)) as fn(&u32) -> Result<u32, BodyError>,
            input,
        )
        .expect("step");
    let duplicate = dup_flow.finish::<Out2<u32, u32>, _>((duplicated.clone(), duplicated));
    assert_eq!(duplicate.unwrap_err(), BuildError::DuplicateOutputPosition);

    // 输出类型与声明位置不一致在类型层不可表达（编译期拒绝，见 UI 夹具 l04 一族的“错类型接线”）。
    // 删除输出位置检查后，下面的完成调用会以 DuplicateOutputPosition 暴露（注入证据）。
}

struct UnitNode;
impl NodeCall1<u32, Unit> for UnitNode {
    fn call<'a>(&'a self, _a: &'a u32) -> NodeFut<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

// ---------------------------------------------------------------- A05：控制器

#[test]
fn a05_each_node_body_and_shared_input() {
    // Node body
    let mut each = EachBuilder::<EachOnly<ItemResult>, ItemResult>::start().expect("each");
    each.then_body::<_, SyncFnSig<(ItemResult,), Data<ItemResult>>>(
        (|item: &ItemResult| Ok(ItemResult(item.0 + 1)))
            as fn(&ItemResult) -> Result<ItemResult, BodyError>,
    )
    .expect("each body");
    let each: Each<EachOnly<ItemResult>, ItemResult> = each.finish().expect("finish");
    let (mut root, items) = FlowBuilder::<Vec<ItemResult>>::start().expect("root");
    let collected: DataRef<Vec<ItemResult>> = root
        .then::<_, OrchSig<Vec<ItemResult>, Data<Vec<ItemResult>>>, _>(each, items)
        .expect("each step");
    let root: Flow<(Vec<ItemResult>,), Data<Vec<ItemResult>>> = root
        .finish::<Data<Vec<ItemResult>>, _>(collected)
        .expect("root finish");
    assert_eq!(
        block_on(Runtime::execute(&root, vec![ItemResult(1), ItemResult(2)])).unwrap(),
        vec![ItemResult(2), ItemResult(3)]
    );

    // 带一个 shared 的 Each：body 是双参数函数
    let mut shared_each =
        EachBuilder::<EachShared<ItemResult, Shared>, ItemResult>::start().expect("shared each");
    shared_each
        .then_body::<_, SyncFnSig<(ItemResult, Shared), Data<ItemResult>>>(
            (|item: &ItemResult, shared: &Shared| Ok(ItemResult(item.0 + shared.0)))
                as fn(&ItemResult, &Shared) -> Result<ItemResult, BodyError>,
        )
        .expect("shared body");
    let shared_each: Each<EachShared<ItemResult, Shared>, ItemResult> =
        shared_each.finish().expect("finish");
    let (mut shared_root, (items, shared)) =
        FlowBuilder2::<Vec<ItemResult>, Shared>::start().expect("root");
    let collected: DataRef<Vec<ItemResult>> = shared_root
        .then::<_, OrchSig<(Vec<ItemResult>, Shared), Data<Vec<ItemResult>>>, _>(
            shared_each,
            (items, shared),
        )
        .expect("each step");
    let shared_root: Flow<(Vec<ItemResult>, Shared), Data<Vec<ItemResult>>> = shared_root
        .finish::<Data<Vec<ItemResult>>, _>(collected)
        .expect("root finish");
    assert_eq!(
        block_on(Runtime::execute(
            &shared_root,
            (vec![ItemResult(1)], Shared(10))
        ))
        .unwrap(),
        vec![ItemResult(11)]
    );
}

#[test]
fn a05_loop_iter_multi_round_and_retry_finish() {
    // Iter：current-state 每轮推进，两轮后 Finish
    let mut iter_builder = LoopBuilder::<Iter1<Counter>>::start().expect("iter loop");
    iter_builder
        .then_body::<_, SyncFnSig<(Counter,), Data<Counter>>>(
            (|state: &Counter| {
                Ok(Counter {
                    base: state.base + 1,
                    rounds: state.rounds + 1,
                })
            }) as fn(&Counter) -> Result<Counter, BodyError>,
        )
        .expect("iter body");
    let iter_loop: Loop<Iter1<Counter>> = iter_builder.finish().expect("loop finish");
    let (mut root, input) = FlowBuilder::<Counter>::start().expect("root");
    let out: DataRef<Counter> = root
        .then::<_, OrchSig<Counter, Data<Counter>>, _>(iter_loop, input)
        .expect("loop step");
    let root: Flow<(Counter,), Data<Counter>> =
        root.finish::<Data<Counter>, _>(out).expect("root finish");
    let final_state = block_on(Runtime::execute(&root, Counter { base: 0, rounds: 0 })).unwrap();
    assert_eq!(final_state, Counter { base: 2, rounds: 2 });

    // Retry：第一轮输出即 Finish（每轮复用原始输入）
    let mut retry_builder =
        LoopBuilder::<Retry1<Counter, AlreadyDone>>::start().expect("retry loop");
    retry_builder
        .then_body::<_, SyncFnSig<(Counter,), Data<AlreadyDone>>>(
            (|_state: &Counter| Ok(AlreadyDone)) as fn(&Counter) -> Result<AlreadyDone, BodyError>,
        )
        .expect("retry body");
    let retry_loop: Loop<Retry1<Counter, AlreadyDone>> =
        retry_builder.finish().expect("loop finish");
    let (mut root, input) = FlowBuilder::<Counter>::start().expect("root");
    let out: DataRef<AlreadyDone> = root
        .then::<_, OrchSig<Counter, Data<AlreadyDone>>, _>(retry_loop, input)
        .expect("loop step");
    let root: Flow<(Counter,), Data<AlreadyDone>> = root
        .finish::<Data<AlreadyDone>, _>(out)
        .expect("root finish");
    assert_eq!(
        block_on(Runtime::execute(&root, Counter { base: 0, rounds: 0 })).unwrap(),
        AlreadyDone
    );
}

#[test]
fn a05_match_branch_default_and_no_match_error() {
    fn build_match() -> Match<u32, u32, Data<u32>> {
        let mut matcher = MatchBuilder::<u32, u32, Data<u32>>::start().expect("match");
        matcher
            .branch::<_, SyncFnSig<(u32,), Data<u32>>>(
                7,
                (|a: &u32| Ok(*a + 1)) as fn(&u32) -> Result<u32, BodyError>,
            )
            .expect("branch");
        matcher
            .default::<_, SyncFnSig<(u32,), Data<u32>>>(
                (|a: &u32| Ok(a * 10)) as fn(&u32) -> Result<u32, BodyError>,
            )
            .expect("default");
        matcher.finish().expect("finish")
    }

    let (mut root, (route, input)) = FlowBuilder2::<u32, u32>::start().expect("root");
    let routed: DataRef<u32> = root
        .then::<_, OrchSig<(u32, u32), Data<u32>>, _>(build_match(), (route, input))
        .expect("match step");
    let root: Flow<(u32, u32), Data<u32>> =
        root.finish::<Data<u32>, _>(routed).expect("root finish");
    assert_eq!(block_on(Runtime::execute(&root, (7u32, 1u32))).unwrap(), 2);
    assert_eq!(block_on(Runtime::execute(&root, (9u32, 3u32))).unwrap(), 30);

    // 无命中且无 default：业务错误可观察
    let mut matcher = MatchBuilder::<u32, u32, Data<u32>>::start().expect("match");
    matcher
        .branch::<_, SyncFnSig<(u32,), Data<u32>>>(
            1,
            (|a: &u32| Ok(*a)) as fn(&u32) -> Result<u32, BodyError>,
        )
        .expect("branch");
    let matcher = matcher.finish().expect("finish");
    let (mut root, (route, input)) = FlowBuilder2::<u32, u32>::start().expect("root");
    let routed: DataRef<u32> = root
        .then::<_, OrchSig<(u32, u32), Data<u32>>, _>(matcher, (route, input))
        .expect("match step");
    let root: Flow<(u32, u32), Data<u32>> =
        root.finish::<Data<u32>, _>(routed).expect("root finish");
    let error = block_on(Runtime::execute(&root, (5u32, 1u32))).expect_err("no match");
    assert_eq!(error.kind(), RunErrorKind::BusinessTerminated);
}

#[test]
fn a05_cross_controller_chain_each_then_match() {
    // 组合链：Each 产生 Vec<ItemResult>，Match 以其为业务输入（跨两类控制器）。
    let mut each = EachBuilder::<EachOnly<Payload>, ItemResult>::start().expect("each");
    each.then_body::<_, SyncFnSig<(Payload,), Data<ItemResult>>>(
        (|payload: &Payload| Ok(ItemResult(payload.id)))
            as fn(&Payload) -> Result<ItemResult, BodyError>,
    )
    .expect("each body");
    let each: Each<EachOnly<Payload>, ItemResult> = each.finish().expect("finish");

    let mut matcher = MatchBuilder::<u32, Vec<ItemResult>, Data<u32>>::start().expect("match");
    matcher
        .branch::<_, SyncFnSig<(Vec<ItemResult>,), Data<u32>>>(
            1,
            (|items: &Vec<ItemResult>| Ok(items.iter().map(|item| item.0).sum()))
                as fn(&Vec<ItemResult>) -> Result<u32, BodyError>,
        )
        .expect("branch");
    matcher
        .default::<_, SyncFnSig<(Vec<ItemResult>,), Data<u32>>>(
            (|items: &Vec<ItemResult>| Ok(items.len() as u32))
                as fn(&Vec<ItemResult>) -> Result<u32, BodyError>,
        )
        .expect("default");
    let matcher: Match<u32, Vec<ItemResult>, Data<u32>> = matcher.finish().expect("finish");

    let (mut root, (payloads, route)) = FlowBuilder2::<Vec<Payload>, u32>::start().expect("root");
    let collected: DataRef<Vec<ItemResult>> = root
        .then::<_, OrchSig<Vec<Payload>, Data<Vec<ItemResult>>>, _>(each, payloads)
        .expect("each step");
    let routed: DataRef<u32> = root
        .then::<_, OrchSig<(u32, Vec<ItemResult>), Data<u32>>, _>(matcher, (route, collected))
        .expect("match step");
    let root: Flow<(Vec<Payload>, u32), Data<u32>> =
        root.finish::<Data<u32>, _>(routed).expect("root finish");

    let sum = block_on(Runtime::execute(
        &root,
        (
            vec![
                Payload {
                    id: 2,
                    history: vec![],
                },
                Payload {
                    id: 5,
                    history: vec![],
                },
            ],
            1u32,
        ),
    ))
    .unwrap();
    assert_eq!(sum, 7);
    let count = block_on(Runtime::execute(
        &root,
        (
            vec![
                Payload {
                    id: 2,
                    history: vec![],
                },
                Payload {
                    id: 5,
                    history: vec![],
                },
            ],
            0u32,
        ),
    ))
    .unwrap();
    assert_eq!(count, 2);
}

// ---------------------------------------------------------------- A06：Root 所有权与错误

#[test]
fn a06_unit_root_takes_no_business_output() {
    let (mut root, input) = FlowBuilder::<u32>::start().expect("root");
    root.then::<_, srflow::NodeSig<(u32,), Unit>, _>(UnitNode, input)
        .expect("unit step");
    let root: Flow<(u32,), Unit> = root.finish::<Unit, _>(()).expect("finish");
    assert_eq!(block_on(Runtime::execute(&root, 3u32)).unwrap(), ());
}

#[test]
fn a06_business_error_is_observable_and_returns_no_partial_output() {
    fn fail(_a: &u32) -> Result<u32, BodyError> {
        Err(BodyError::new("business failure"))
    }

    let (mut root, input) = FlowBuilder::<u32>::start().expect("root");
    let failed: DataRef<u32> = root
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(
            fail as fn(&u32) -> Result<u32, BodyError>,
            input,
        )
        .expect("fail step");
    let root: Flow<(u32,), Data<u32>> = root.finish::<Data<u32>, _>(failed).expect("finish");

    let error = block_on(Runtime::execute(&root, 1u32)).expect_err("business failure");
    assert_eq!(error.stage(), RunErrorStage::Body);
    assert_eq!(error.kind(), RunErrorKind::BusinessTerminated);
    assert_eq!(error.business_note(), Some("business failure"));
    assert_eq!(error.message(), "business failure");
    assert!(!error.cleanup_failed());
    assert!(!error.close_failed());
    let debug = format!("{error:?}");
    assert!(!debug.contains("ScopeId"), "{debug}");
    assert!(!debug.contains("RefId"), "{debug}");
    assert!(!debug.contains("DataId"), "{debug}");
}

#[test]
fn a06_duplicate_root_data_id_is_rejected_before_any_take() {
    /// identity 子 Flow：只再导出 imported 输入（同一物理 DataId）。
    fn identity_flow() -> Flow<(u32,), Data<u32>> {
        let (flow, input) = FlowBuilder::<u32>::start().expect("identity");
        flow.finish::<Data<u32>, _>(input).expect("identity finish")
    }
    fn alias_subflow() -> Flow<(u32,), Out2<u32, u32>> {
        let (mut sub, input) = FlowBuilder::<u32>::start().expect("alias");
        let first: DataRef<u32> = sub
            .then::<_, OrchSig<u32, Data<u32>>, _>(identity_flow(), input.clone())
            .expect("alias a");
        let second: DataRef<u32> = sub
            .then::<_, OrchSig<u32, Data<u32>>, _>(identity_flow(), input)
            .expect("alias b");
        sub.finish::<Out2<u32, u32>, _>((first, second))
            .expect("alias finish")
    }

    let (mut root, input) = FlowBuilder::<u32>::start().expect("root");
    let (first, second) = root
        .then::<_, OrchSig<u32, Out2<u32, u32>>, _>(alias_subflow(), input)
        .expect("alias step");
    let root: Flow<(u32,), Out2<u32, u32>> = root
        .finish::<Out2<u32, u32>, _>((first, second))
        .expect("root finish");

    let error = block_on(Runtime::execute(&root, 5u32)).expect_err("duplicate data id");
    assert_eq!(error.stage(), RunErrorStage::Preflight);
    assert_eq!(error.kind(), RunErrorKind::DuplicateRootDataId);
    // 预检在任何 take 之前整体拒绝：不产生部分正常输出，公开诊断逐字段固定且不含内部身份。
    assert_eq!(
        format!("{error:?}"),
        "RunError { stage: Preflight, kind: DuplicateRootDataId, message: \"root output preflight rejected\", business_note: None, cleanup_failed: false, close_failed: false }"
    );
}
