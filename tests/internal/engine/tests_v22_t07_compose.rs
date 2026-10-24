//! V22-T07 定向测试（二）：5×5 组合缺格、同一片段六容器复用、定义闭包计数与重复声明边界。
//!
//! 覆盖判别性证据：
//! - 5×5 直接嵌套表的缺格：Choose 分支内直接创建 Each 与 Iter；Iter Round 内直接创建 Chain；
//! - 同一 `fn build_fragment(&mut Flow)` 在 Root／Chain／Choose 分支／Each Item／Retry Attempt／
//!   Iter Round 六处**直接复用**，内部含自动 Node 与短借用同步 Factory，逐容器业务计数精确；
//! - Each／Retry／Iter 的 Body 定义闭包每次 execute 只执行一次（不随 item／attempt／轮次重复）；
//! - 两份都声明 output 的 Body 直接叠在同一 Flow 被重复声明拒绝且业务零执行；
//!   同一函数分别放入两个 Chain 合法并各自执行。
//!
//! 全部经正式 `Runtime::execute`（v2.2 签名：Root 闭包 `|flow, refs|` 返回 unit）。
#![allow(clippy::let_unit_value, clippy::unused_unit, clippy::type_complexity)]

use crate::core::engine::data::Data;
use crate::core::engine::error::{BodyError, BuildErrorDetail, RunError};
use crate::core::engine::flow::Flow;
use crate::core::engine::observe;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, Ref};
use futures::executor::block_on;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

// ---------------------------------------------------------------- 业务类型与计数

#[derive(Debug, PartialEq, Eq, Clone)]
struct FragOut(u32);
impl Data for FragOut {}

#[derive(Debug, PartialEq, Eq, Clone)]
struct FragLite(u32);
impl Data for FragLite {}

#[derive(Debug, PartialEq, Eq, Clone)]
struct FragItem(u32);
impl Data for FragItem {}

#[derive(Debug, PartialEq, Eq, Clone)]
struct FragKey(u32);
impl Data for FragKey {}

#[derive(Debug, PartialEq, Eq, Clone)]
struct FragScore(u32);
impl Data for FragScore {}

thread_local! {
    static FRAG_NODE_RUNS: Cell<usize> = const { Cell::new(0) };
    static FRAG_FACTORY_RUNS: Cell<usize> = const { Cell::new(0) };
    static BODY_DEFS: Cell<usize> = const { Cell::new(0) };
    static NODE_RUNS: Cell<usize> = const { Cell::new(0) };
}

fn reset_all() {
    FRAG_NODE_RUNS.with(|c| c.set(0));
    FRAG_FACTORY_RUNS.with(|c| c.set(0));
    BODY_DEFS.with(|c| c.set(0));
    NODE_RUNS.with(|c| c.set(0));
}

fn frag_node_runs() -> usize {
    FRAG_NODE_RUNS.with(Cell::get)
}

fn frag_factory_runs() -> usize {
    FRAG_FACTORY_RUNS.with(Cell::get)
}

fn body_defs() -> usize {
    BODY_DEFS.with(Cell::get)
}

fn node_runs() -> usize {
    NODE_RUNS.with(Cell::get)
}

// ---------------------------------------------------------------- 片段与业务 Node

/// 片段内的自动 Node（零输入，单 Data 输出）。
async fn frag_node(_query: Query<()>) -> Result<FragOut, BodyError> {
    FRAG_NODE_RUNS.with(|c| c.set(c.get() + 1));
    Ok(FragOut(9))
}

/// 推进一格并计数（Iter Round／Chain 子体使用）。
async fn frag_bump(query: Query<&u32>) -> Result<u32, BodyError> {
    NODE_RUNS.with(|c| c.set(c.get() + 1));
    Ok(query.get() + 1)
}

/// 状态达到 2 时发出 `IterBreak`。
async fn frag_judge(query: Query<&u32>) -> Result<(), BodyError> {
    if *query.get() >= 2 {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}

/// 必定发出 Retry 请求的 unit Node（Retry attempt 使用；Failure 不隐含重试，故用 Control）。
async fn frag_retry_unit(_query: Query<()>) -> Result<(), BodyError> {
    Err(BodyError::retry(std::io::Error::other(
        "fragment retry probe",
    )))
}

/// Item → Score（Each 聚合使用）。
async fn frag_score(query: Query<&FragItem>) -> Result<FragScore, BodyError> {
    NODE_RUNS.with(|c| c.set(c.get() + 1));
    Ok(FragScore(query.get().0 * 10))
}

/// 汇总聚合结果（Choose 命中分支的返回值来源）。
async fn frag_sum_scores(query: Query<&Vec<FragScore>>) -> Result<FragScore, BodyError> {
    NODE_RUNS.with(|c| c.set(c.get() + 1));
    Ok(FragScore(query.get().iter().map(|score| score.0).sum()))
}

/// 被六类容器直接复用的同一片段：**不声明任何边界输出**，含自动 Node 与短借用同步 Factory。
fn build_fragment<'n>(flow: &mut Flow<'n>, seed: &'n u32) {
    let _ = flow.then(frag_node);
    let _ = flow.provide(move || {
        FRAG_FACTORY_RUNS.with(|c| c.set(c.get() + 1));
        FragLite(*seed)
    });
}

// ---------------------------------------------------------------- 六容器直接复用

#[test]
fn t07_fragment_is_reused_by_six_containers_with_exact_counts() {
    let seed = 41u32;
    let runtime = Runtime::new();

    // ① Root Body
    reset_all();
    block_on(runtime.execute::<(), (), _>(
        |flow, _input| {
            build_fragment(flow, &seed);
            flow.output_with(());
        },
        (),
    ))
    .expect("Root 直接复用片段");
    assert_eq!(
        (frag_node_runs(), frag_factory_runs()),
        (1, 1),
        "Root：片段各一步"
    );

    // ② Chain child Body
    reset_all();
    block_on(runtime.execute::<(), (), _>(
        |flow, _input| {
            let _ = flow.chain(|sub| {
                build_fragment(sub, &seed);
                sub.output_with(());
            });
            flow.output_with(());
        },
        (),
    ))
    .expect("Chain child 直接复用片段");
    assert_eq!(
        (frag_node_runs(), frag_factory_runs()),
        (1, 1),
        "Chain：片段各一步"
    );

    // ③ Choose 命中分支 Body
    reset_all();
    block_on(runtime.execute::<FragKey, (), _>(
        |flow, _input| {
            let _: () = flow.choose_output::<FragKey, _, _>(|choice| {
                choice.case(FragKey(1), |branch| {
                    build_fragment(branch, &seed);
                    branch.output_with(());
                });
                choice.otherwise(|branch| {
                    branch.output_with(());
                });
            });
            flow.output_with(());
        },
        FragKey(1),
    ))
    .expect("Choose 命中分支直接复用片段");
    assert_eq!(
        (frag_node_runs(), frag_factory_runs()),
        (1, 1),
        "Choose：仅命中分支各一步"
    );

    // ④ Each Item Body（3 项）
    reset_all();
    block_on(runtime.execute::<Vec<FragItem>, (), _>(
        |flow, items| {
            let _ = flow.each_output::<FragItem, (), _>(items, |sub| {
                build_fragment(sub, &seed);
                sub.output::<()>();
            });
            flow.output_with(());
        },
        vec![FragItem(1), FragItem(2), FragItem(3)],
    ))
    .expect("Each Item 直接复用片段");
    assert_eq!(
        (frag_node_runs(), frag_factory_runs()),
        (3, 3),
        "Each：每项一次片段"
    );

    // ⑤ Retry Attempt Body（预算 1：两次 attempt 均失败，片段逐 attempt 执行）
    reset_all();
    let outcome = block_on(runtime.execute::<(), (), _>(
        |flow, _input| {
            let _: () = flow.retry_output::<(), _>(1, |attempt| {
                build_fragment(attempt, &seed);
                let _: () = attempt.then_output(frag_retry_unit);
                attempt.output_with(());
            });
            flow.output_with(());
        },
        (),
    ));
    assert!(outcome.is_err(), "预算耗尽必须返回错误");
    assert_eq!(
        (frag_node_runs(), frag_factory_runs()),
        (2, 2),
        "Retry：两次 attempt 各一次片段"
    );

    // ⑥ Iter Round Body（第三轮 IterBreak；片段逐轮执行；Round 自己恰好一次 next）
    reset_all();
    let delivered = block_on(runtime.execute::<u32, u32, _>(
        |flow, initial| {
            let out = flow.iter_output(initial, 5, |round| {
                build_fragment(round, &seed);
                let current = round.current::<u32>();
                let _: () = round.then_with_output(&frag_judge, current);
                let next = round.then_with_output(frag_bump, current);
                round.next_with(next);
            });
            flow.output_with(out);
        },
        0u32,
    ))
    .expect("Iter Round 直接复用片段并交付断点 Current");
    assert_eq!(
        delivered, 2,
        "第三轮进入时 current=2 触发 IterBreak 并交付该轮 Current"
    );
    assert_eq!(
        (frag_node_runs(), frag_factory_runs()),
        (3, 3),
        "Iter：三轮各一次片段"
    );
}

// ---------------------------------------------------------------- 5×5 缺格

#[test]
fn t07_choose_branch_directly_creates_each_and_iter() {
    reset_all();
    let (score_total, iter_state) = block_on(
        Runtime::new().execute::<(FragKey, Vec<FragItem>), (FragScore, u32), _>(
            |flow, (selector, items)| {
                let picked: (Ref<FragScore>, Ref<u32>) =
                    flow.choose_output::<FragKey, _, _>(|choice| {
                        let _ = selector;
                        choice.case(FragKey(1), |branch| {
                            let scores: Ref<Vec<FragScore>> = branch.each_output(items, |sub| {
                                let item = sub.ref_of::<FragItem>();
                                let score = sub.then_with_output(frag_score, item);
                                sub.output_with(score);
                            });
                            let summarized: Ref<FragScore> =
                                branch.then_with_output(frag_sum_scores, scores);
                            let zero: Ref<u32> = branch.provide_output(|| 0u32);
                            let state: Ref<u32> = branch.iter_output(zero, 4, |round| {
                                let current = round.current::<u32>();
                                let _: () = round.then_with_output(&frag_judge, current);
                                let next = round.then_with_output(frag_bump, current);
                                round.next_with(next);
                            });
                            branch.output_with((summarized, state));
                        });
                        choice.otherwise(|branch| {
                            let _ = branch.then(frag_node);
                            let fallback: Ref<FragScore> = branch.then_output(frag_score_dummy);
                            let zero: Ref<u32> = branch.then_output(frag_zero);
                            branch.output_with((fallback, zero));
                        });
                    });
                flow.output_with(picked);
            },
            (FragKey(1), vec![FragItem(1), FragItem(2)]),
        ),
    )
    .expect("Choose 命中分支内直接创建 Each 与 Iter 必须真实执行");

    assert_eq!(score_total, FragScore(30), "Each 聚合 [10, 20] 之和");
    assert_eq!(iter_state, 2, "分支内 Iter 由 IterBreak 交付断点 Current");
    assert_eq!(
        node_runs(),
        5,
        "Each 2 次 + Iter bump 2 次 + 汇总 1 次；未选分支零执行"
    );
}

/// 未选分支占位（不执行）。
async fn frag_score_dummy(_query: Query<()>) -> Result<FragScore, BodyError> {
    NODE_RUNS.with(|c| c.set(c.get() + 1));
    Ok(FragScore(0))
}

/// 未选分支占位（不执行）。
async fn frag_zero(_query: Query<()>) -> Result<u32, BodyError> {
    NODE_RUNS.with(|c| c.set(c.get() + 1));
    Ok(0)
}

#[test]
fn t07_iter_round_directly_creates_chain() {
    reset_all();
    let delivered = block_on(Runtime::new().execute::<u32, u32, _>(
        |flow, initial| {
            let out = flow.iter_output(initial, 6, |round| {
                let current = round.current::<u32>();
                let chained: Ref<u32> = round.chain_output(|sub| {
                    let stepped = sub.then_with_output(frag_bump, current);
                    sub.output_with(stepped);
                });
                let _: () = round.then_with_output(&frag_judge, chained);
                round.next_with(chained);
            });
            flow.output_with(out);
        },
        0u32,
    ))
    .expect("Iter Round 内直接创建 Chain 并交付");

    assert_eq!(
        delivered, 1,
        "第二轮由 Chain 推进到 2 后 IterBreak，交付该轮 Current"
    );
    assert_eq!(node_runs(), 2, "Chain 子体在两轮各执行一次 bump");
}

// ---------------------------------------------------------------- 定义闭包计数

#[test]
fn t07_body_definition_closures_run_once_per_execute() {
    let runtime = Runtime::new();

    // Each：3 项，定义闭包 1 次
    reset_all();
    block_on(runtime.execute::<Vec<FragItem>, (), _>(
        |flow, items| {
            let _ = flow.each_output::<FragItem, (), _>(items, |sub| {
                BODY_DEFS.with(|c| c.set(c.get() + 1));
                let item = sub.ref_of::<FragItem>();
                let _ = sub.then_with_output(frag_score, item);
                sub.output::<()>();
            });
            flow.output_with(());
        },
        vec![FragItem(1), FragItem(2), FragItem(3)],
    ))
    .expect("Each 定义闭包计数");
    assert_eq!(body_defs(), 1, "Each 定义闭包每次 execute 只执行一次");
    assert_eq!(node_runs(), 3, "业务 Node 仍按 item 执行");

    // Retry：预算 1（两次 attempt），定义闭包 1 次
    reset_all();
    let _ = block_on(runtime.execute::<(), (), _>(
        |flow, _input| {
            let _: () = flow.retry_output::<(), _>(1, |attempt| {
                BODY_DEFS.with(|c| c.set(c.get() + 1));
                let _: () = attempt.then_output(frag_retry_unit);
                attempt.output_with(());
            });
            flow.output_with(());
        },
        (),
    ));
    assert_eq!(body_defs(), 1, "Retry 定义闭包不随 attempt 数重复");

    // Iter：三轮（IterBreak），定义闭包 1 次
    reset_all();
    let _ = block_on(runtime.execute::<u32, u32, _>(
        |flow, initial| {
            let out = flow.iter_output(initial, 5, |round| {
                BODY_DEFS.with(|c| c.set(c.get() + 1));
                let current = round.current::<u32>();
                let _: () = round.then_with_output(&frag_judge, current);
                let next = round.then_with_output(frag_bump, current);
                round.next_with(next);
            });
            flow.output_with(out);
        },
        0u32,
    ))
    .expect("Iter 定义闭包计数");
    assert_eq!(body_defs(), 1, "Iter 定义闭包不随轮次重复");
}

// ---------------------------------------------------------------- 重复声明边界

#[derive(Debug, PartialEq, Eq, Clone)]
struct BodyOut(u32);
impl Data for BodyOut {}

async fn body_value(query: Query<&u32>) -> Result<BodyOut, BodyError> {
    NODE_RUNS.with(|c| c.set(c.get() + 1));
    Ok(BodyOut(query.get() + 1))
}

/// 带一次完整 `output` 的 unit Body 函数（声明 unit 输出）。
fn body_declares_unit(flow: &mut Flow<'_>) {
    let source = flow.ref_of::<u32>();
    let _ = flow.then_with_output(body_value, source);
    flow.output_with(());
}

/// 带一次完整 `output` 的取值 Body 函数（声明 BodyOut 输出）。
fn body_declares_out(flow: &mut Flow<'_>) {
    let source = flow.ref_of::<u32>();
    let value = flow.then_with_output(body_value, source);
    flow.output_with(value);
}

#[test]
fn t07_two_output_declaring_bodies_stack_is_rejected_but_chains_are_legal() {
    let runtime = Runtime::new();

    // 直接叠在同一 Root Body：第二次 output 声明被重复声明拒绝，业务零执行。
    reset_all();
    let outcome = block_on(runtime.execute::<u32, (), _>(
        |flow, _input| {
            body_declares_unit(flow);
            body_declares_unit(flow);
        },
        5u32,
    ));
    match outcome {
        Err(RunError::Definition(error)) => match &error.detail {
            BuildErrorDetail::DuplicateDeclaration { .. } => {}
            other => panic!("应拒绝重复声明，实际：{other:?}"),
        },
        other => panic!("两份都声明 output 的 Body 直接叠加必须拒绝，实际：{other:?}"),
    }
    assert_eq!(node_runs(), 0, "构建失败不执行任何业务 Node");

    // 同一函数分别放入两个 Chain：各自独立 Body，合法且都执行。
    reset_all();
    let (left, right) = block_on(runtime.execute::<u32, (BodyOut, BodyOut), _>(
        |flow, _input| {
            let left: Ref<BodyOut> = flow.chain_output(body_declares_out);
            let right: Ref<BodyOut> = flow.chain_output(body_declares_out);
            flow.output_with((left, right));
        },
        5u32,
    ))
    .expect("同一声明 output 的函数分别放入 Chain 合法");
    assert_eq!((left, right), (BodyOut(6), BodyOut(6)));
    assert_eq!(node_runs(), 2, "两个 Chain 各自执行一次");
}

// ---------------------------------------------------------------- 深层路径：三层真实边界

/// 子层临时 owned（非输出）：Drop 时写日志。
#[derive(Debug, PartialEq, Eq, Clone)]
struct DeepTemp {
    journal: Rc<RefCell<Vec<String>>>,
}

impl Drop for DeepTemp {
    fn drop(&mut self) {
        self.journal.borrow_mut().push("drop:temp".to_string());
    }
}

impl Data for DeepTemp {}

#[derive(Debug, PartialEq, Eq, Clone)]
struct DeepSeed {
    value: u32,
    journal: Rc<RefCell<Vec<String>>>,
}

impl Data for DeepSeed {}

async fn deep_temp(query: Query<&DeepSeed>) -> Result<DeepTemp, BodyError> {
    let seed = query.get();
    Ok(DeepTemp {
        journal: Rc::clone(&seed.journal),
    })
}

async fn deep_read(query: Query<&DeepSeed>) -> Result<u32, BodyError> {
    Ok(query.get().value + 1)
}

/// 父层后继步骤：读取最深子层导出的 u32，并把“父层继续”写进日志。
async fn deep_note(query: Query<(&u32, &DeepSeed)>) -> Result<(), BodyError> {
    let (value, seed) = query.get();
    assert_eq!(*value, 21, "父层读到最深子层导出值");
    seed.journal.borrow_mut().push("note:after".to_string());
    Ok(())
}

fn observe_root_scope() -> crate::core::identity::ScopeId {
    observe::take_frames()
        .into_iter()
        .find(|frame| frame.kind == "root" && frame.phase == "enter")
        .expect("root frame record")
        .scope
}

#[test]
fn t07_three_layer_boundaries_import_the_ancestor_once_per_layer() {
    observe::reset();
    crate::core::test_support::reset_observations();
    crate::core::test_support::take_log_reset();

    let journal = Rc::new(RefCell::new(Vec::new()));
    let seed = DeepSeed {
        value: 20,
        journal: Rc::clone(&journal),
    };

    let output = block_on(Runtime::new().execute::<DeepSeed, u32, _>(
        |flow, seed| {
            let exported: Ref<u32> = flow.chain_output(|outer| {
                let inner: Ref<u32> = outer.chain_output(|middle| {
                    let deepest: Ref<u32> = middle.chain_output(|deep| {
                        // 非输出临时值：在父层下一步之前销毁。
                        let _temp = deep.then_with_output(deep_temp, seed);
                        let read = deep.then_with_output(deep_read, seed);
                        deep.output_with(read);
                    });
                    middle.output_with(deepest);
                });
                outer.output_with(inner);
            });
            let _: () = flow.then_with_output(deep_note, (exported, seed));
            flow.output_with(exported);
        },
        seed,
    ))
    .expect("三层边界深层 capture 必须执行");

    assert_eq!(output, 21, "最深子层导出值可被父层继续读取");
    assert_eq!(
        journal.borrow().as_slice(),
        ["drop:temp".to_string(), "note:after".to_string()],
        "非输出临时值在父层下一步之前销毁，随后父层继续"
    );

    // 逐层真实 Import：caller→child 链为 Root→A→B→C，同一祖先 RefId 每层各绑定一次。
    let imports = observe::take_chain_imports();
    assert_eq!(imports.len(), 3, "三层边界各一条 Import 批次：{imports:?}");
    let root_scope = observe_root_scope();
    let layer0 = imports
        .iter()
        .find(|record| record.caller == root_scope)
        .expect("第一层 caller 为 Root");
    let layer1 = imports
        .iter()
        .find(|record| record.caller == layer0.child)
        .expect("第二层 caller 为第一层 child");
    let layer2 = imports
        .iter()
        .find(|record| record.caller == layer1.child)
        .expect("第三层 caller 为第二层 child");
    let seed_core = layer0.pairs[0].0.clone();
    for (index, layer) in [layer0, layer1, layer2].iter().enumerate() {
        assert_eq!(
            layer.pairs,
            vec![(seed_core.clone(), seed_core.clone())],
            "第 {} 层：祖先只登记一次且 source == target（同一静态 Core RefId）",
            index + 1
        );
    }

    // 原 owner：Root 输入实例在整条深层路径后仍存活且 owner 仍为 Root Scope。
    let snapshots = observe::take_export_snapshots();
    let root_before = snapshots
        .iter()
        .find(|record| record.phase == "before" && record.scope == root_scope)
        .expect("Root 边界前快照");
    assert_eq!(
        root_before.owned.len(),
        1,
        "Root 在该时点只拥有声明的输入实例"
    );
    let seed_data = root_before.owned[0].clone();
    let root_final = snapshots
        .iter()
        .rfind(|record| record.scope == root_scope)
        .expect("Root 最终快照");
    assert_eq!(
        root_final
            .owned_alive
            .iter()
            .find(|(id, _, _)| *id == seed_data)
            .map(|(_, alive, owner)| (*alive, owner.clone())),
        Some((true, Some(root_scope.clone()))),
        "Root 输入实例存活且 owner 不变"
    );
}
