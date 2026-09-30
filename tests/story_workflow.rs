//! T08 验收：核心端到端与公共边界（外部使用者视角）。
//!
//! 用离线 Fake Node 把 `Retry`／`Match`／`Each`／`Iter`／`Binding`／`SubFlow` 组合成一条真实形状的
//! 决策流程，并覆盖正常路径、各条失败路径与 Flow 归属边界。Fake 与构图定义在
//! `examples/support/story_workflow.rs`（经 `#[path]` 共享，不进入 crate 公共 API）。

#[path = "../examples/support/story_workflow.rs"]
mod support;

use std::error::Error as StdError;

use futures::executor::block_on;
use srflow::{ExecutionError, FlowBuildError, FlowBuilder, Node, Runtime};
use support::{
    Brief, PlanAttempt, Route, StoryConfig, StoryFailure, build_story_workflow, run_story_workflow,
};

/// 事件序列断言辅助。
fn events(workflow: &support::StoryWorkflow) -> String {
    workflow.events.text()
}

/// 取错误而不要求业务 Output 实现 `Debug`（Fake 业务类型刻意都不实现它）。
fn expect_error(result: Result<support::FinalResult, ExecutionError>) -> ExecutionError {
    match result {
        Ok(_) => panic!("期望执行失败，但成功返回"),
        Err(error) => error,
    }
}

// ---------------------------------------------------------------- A01／A02：正常路径

#[test]
fn the_story_workflow_runs_end_to_end() {
    let workflow = build_story_workflow(StoryConfig::default());
    let result = run_story_workflow(&workflow, "雨夜来信").expect("正常路径应当成功");

    // 最终业务 Output 来自 Iter 的最后一轮状态，并由四个来源装配成最终结构。
    assert_eq!(result.topic, "雨夜来信");
    assert_eq!(result.plan, "雨夜来信的三幕计划 v2");
    assert_eq!(
        result.prose,
        "雨夜来信-转折(加工)；雨夜来信-高潮(加工)；雨夜来信-收束(加工)；"
    );
    assert!(result.needs_revision, "正常业务状态被带到最终结果");
    assert_eq!(
        result.revision_rounds, 2,
        "第三个独立位置（RevisionPolicy）的值进入了最终装配"
    );

    assert_eq!(
        workflow
            .plan_attempts
            .load(std::sync::atomic::Ordering::SeqCst),
        2,
        "Retry 先拒绝一稿、接受第二稿"
    );
    assert_eq!(
        events(&workflow),
        "plan(1),check(reject),plan(2),check(accept),route(Deep),expand(deep),keys(deep),policy,\
         refine(1),refine(2),refine(3),draft(1),review(ok),draft(2),review(revise),draft(3),review(revise)",
        "顺序、唯一分支与每轮／每项调用次数"
    );
}

#[test]
fn each_receives_the_original_items_in_order() {
    // 判别式证据：Each 的 Body 记录实际收到的原始 Item；若上一项 Output 回流，这里会不同。
    let workflow = build_story_workflow(StoryConfig::default());
    run_story_workflow(&workflow, "雨夜来信").expect("正常路径应当成功");

    assert_eq!(
        workflow.refined_seeds.lock().expect("poisoned").clone(),
        vec![
            String::from("雨夜来信-转折"),
            String::from("雨夜来信-高潮"),
            String::from("雨夜来信-收束"),
        ],
        "Each 收到的是 Match 分支产出的原始种子，且保持顺序"
    );
    assert_eq!(
        workflow.drafted_nodes.lock().expect("poisoned").clone(),
        vec![
            String::from("雨夜来信-转折(加工)"),
            String::from("雨夜来信-高潮(加工)"),
            String::from("雨夜来信-收束(加工)"),
        ],
        "Iter 收到的是 Each 的输出元素，顺序一致"
    );
}

#[test]
fn only_the_selected_match_branch_runs() {
    // 分支一（Node）与分支二（SubFlow）各自独立可选中，另一边零调用。
    let quick = build_story_workflow(StoryConfig {
        force_route: Some(Route::Quick),
        ..StoryConfig::default()
    });
    let result = run_story_workflow(&quick, "雨夜来信").expect("Quick 分支应当成功");
    assert_eq!(result.prose, "雨夜来信-开场(加工)；雨夜来信-冲突(加工)；");
    assert_eq!(quick.events.count("keys(quick)"), 1);
    assert_eq!(quick.events.count("keys(deep)"), 0, "未选分支零调用");
    assert_eq!(
        quick.events.count("expand(deep)"),
        0,
        "未选 SubFlow 分支的步骤零调用"
    );
    assert_eq!(
        quick.events.count("keys(fallback)"),
        0,
        "命中时 default 零调用"
    );

    let deep = build_story_workflow(StoryConfig {
        force_route: Some(Route::Deep),
        ..StoryConfig::default()
    });
    run_story_workflow(&deep, "雨夜来信").expect("Deep 分支应当成功");
    assert_eq!(deep.events.count("keys(deep)"), 1);
    assert_eq!(
        deep.events.count("expand(deep)"),
        1,
        "SubFlow 分支的两个步骤都执行过"
    );
    assert_eq!(deep.events.count("keys(quick)"), 0);
    assert_eq!(deep.events.count("keys(fallback)"), 0);
}

#[test]
fn three_rounds_are_order_sensitive() {
    // 对照：**同一组三个节点**，只把顺序反转（`reverse_seeds` 只改变种子顺序，不改变节点集合）。
    let forward = build_story_workflow(StoryConfig::default());
    let forward_result = run_story_workflow(&forward, "雨夜来信").expect("应当成功");

    let reversed = build_story_workflow(StoryConfig {
        reverse_seeds: true,
        ..StoryConfig::default()
    });
    let reversed_result = run_story_workflow(&reversed, "雨夜来信").expect("应当成功");

    let mut forward_nodes = forward.drafted_nodes.lock().expect("poisoned").clone();
    let mut reversed_nodes = reversed.drafted_nodes.lock().expect("poisoned").clone();
    assert_ne!(forward_nodes, reversed_nodes, "两次运行的节点顺序确实不同");
    forward_nodes.sort();
    reversed_nodes.sort();
    assert_eq!(
        forward_nodes, reversed_nodes,
        "对照成立：节点集合完全相同，差别只在顺序"
    );

    assert_eq!(
        forward_result.prose,
        "雨夜来信-转折(加工)；雨夜来信-高潮(加工)；雨夜来信-收束(加工)；"
    );
    assert_eq!(
        reversed_result.prose,
        "雨夜来信-收束(加工)；雨夜来信-高潮(加工)；雨夜来信-转折(加工)；"
    );
    assert_ne!(
        forward_result.prose, reversed_result.prose,
        "正文按节点顺序累积：结果对顺序敏感"
    );
}

// ---------------------------------------------------------------- A05：失败路径

#[test]
fn retry_exhaustion_returns_the_last_normal_output_and_the_flow_continues() {
    let workflow = build_story_workflow(StoryConfig {
        accept_on_attempt: 0, // 从不接受
        retry_limit: 2,
        force_route: Some(Route::Quick),
        ..StoryConfig::default()
    });
    let result = run_story_workflow(&workflow, "雨夜来信").expect("耗尽不是技术错误");

    assert_eq!(
        workflow
            .plan_attempts
            .load(std::sync::atomic::Ordering::SeqCst),
        2,
        "用满上限，不再继续"
    );
    assert_eq!(
        result.plan, "雨夜来信的三幕计划 v2",
        "返回最后一轮正常 Output"
    );
    assert!(
        workflow.events.count("refine(") > 0 && workflow.events.count("draft(") > 0,
        "耗尽之后父 Flow 仍继续执行后续 child"
    );
    assert_eq!(
        events(&workflow),
        "plan(1),check(reject),plan(2),check(reject),route(Quick),keys(quick),policy,\
         refine(1),refine(2),draft(1),review(ok),draft(2),review(revise)"
    );
}

#[test]
fn a_retry_body_error_propagates_without_retrying() {
    let workflow = build_story_workflow(StoryConfig {
        fail_plan_on: Some(1),
        ..StoryConfig::default()
    });
    let error = expect_error(run_story_workflow(&workflow, "雨夜来信"));

    assert!(
        error
            .source()
            .is_some_and(|source| source.is::<StoryFailure>()),
        "保留来源"
    );
    assert_eq!(
        events(&workflow),
        "plan(1)",
        "技术错误不重试，且后续 child 未启动"
    );
}

#[test]
fn a_match_miss_without_default_returns_typed_no_match() {
    let workflow = build_story_workflow(StoryConfig {
        force_route: Some(Route::Unknown),
        with_default: false,
        ..StoryConfig::default()
    });
    let error = expect_error(run_story_workflow(&workflow, "雨夜来信"));

    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<srflow::NoMatch>())
            .is_some(),
        "未命中以类型化的 NoMatch 暴露"
    );
    assert_eq!(
        events(&workflow),
        "plan(1),check(reject),plan(2),check(accept),route(Unknown)",
        "未命中之后 Each／Iter 等后续 child 零调用"
    );
}

#[test]
fn a_match_miss_with_default_runs_only_the_default() {
    let workflow = build_story_workflow(StoryConfig {
        force_route: Some(Route::Unknown),
        ..StoryConfig::default()
    });
    let result = run_story_workflow(&workflow, "雨夜来信").expect("default 路径应当成功");

    assert_eq!(result.prose, "雨夜来信-兜底(加工)；");
    assert_eq!(workflow.events.count("keys(fallback)"), 1);
    assert_eq!(workflow.events.count("keys(quick)"), 0);
    assert_eq!(workflow.events.count("keys(deep)"), 0);
}

#[test]
fn a_selected_branch_error_does_not_fall_back_to_default() {
    let workflow = build_story_workflow(StoryConfig {
        force_route: Some(Route::Quick),
        fail_branch: true,
        ..StoryConfig::default()
    });
    let error = expect_error(run_story_workflow(&workflow, "雨夜来信"));

    assert!(
        error
            .source()
            .is_some_and(|source| source.is::<StoryFailure>())
    );
    assert_eq!(
        events(&workflow),
        "plan(1),check(reject),plan(2),check(accept),route(Quick),keys(quick)",
        "分支错误原样传播：不改走 default，后续 child 未启动"
    );
}

#[test]
fn an_empty_key_set_makes_each_and_iter_no_ops() {
    let workflow = build_story_workflow(StoryConfig {
        empty_keys: true,
        ..StoryConfig::default()
    });
    let result = run_story_workflow(&workflow, "雨夜来信").expect("空集合不是错误");

    assert_eq!(
        workflow.events.count("refine("),
        0,
        "Each 空集合不执行 Body"
    );
    assert_eq!(workflow.events.count("draft("), 0, "Iter 空集合不执行 Body");
    assert_eq!(result.prose, "", "空集合产出空正文");
    assert!(!result.needs_revision);
    assert_eq!(result.plan, "雨夜来信的三幕计划 v2", "Iter 返回原始 T0");
}

#[test]
fn an_each_body_error_stops_later_items() {
    let workflow = build_story_workflow(StoryConfig {
        fail_refine_on: Some(2),
        ..StoryConfig::default()
    });
    let error = expect_error(run_story_workflow(&workflow, "雨夜来信"));

    assert!(
        error
            .source()
            .is_some_and(|source| source.is::<StoryFailure>())
    );
    assert_eq!(
        events(&workflow),
        "plan(1),check(reject),plan(2),check(accept),route(Deep),expand(deep),keys(deep),policy,\
         refine(1),refine(2)",
        "出错项之后的 Item 未启动，Iter 也未启动"
    );
    assert_eq!(
        workflow.refined_seeds.lock().expect("poisoned").len(),
        2,
        "副作用已经发生（测试替身记录），但框架不返回部分成功结果"
    );
}

#[test]
fn an_iter_body_error_stops_later_rounds() {
    let workflow = build_story_workflow(StoryConfig {
        fail_draft_on: Some(2),
        ..StoryConfig::default()
    });
    let error = expect_error(run_story_workflow(&workflow, "雨夜来信"));

    assert!(
        error
            .source()
            .is_some_and(|source| source.is::<StoryFailure>())
    );
    assert_eq!(
        events(&workflow),
        "plan(1),check(reject),plan(2),check(accept),route(Deep),expand(deep),keys(deep),policy,\
         refine(1),refine(2),refine(3),draft(1),review(ok),draft(2)",
        "出错轮之后的 Item 未启动，且没有部分正常 Output"
    );
}

#[test]
fn a_needs_revision_state_is_not_an_error_and_does_not_stop_iteration() {
    let workflow = build_story_workflow(StoryConfig::default());
    let result = run_story_workflow(&workflow, "雨夜来信").expect("正常业务状态不是技术错误");

    assert!(result.needs_revision);
    assert_eq!(
        workflow.events.count("draft("),
        3,
        "第二轮起虽然需要修订，仍然处理完全部节点"
    );
    assert_eq!(workflow.events.text().matches("review(revise)").count(), 2);
}

// ---------------------------------------------------------------- A03／A06：边界与归属

/// 把主题变成候选计划（归属测试用）。
struct MakeAttempt;

impl Node for MakeAttempt {
    type Input = String;
    type Output = PlanAttempt;

    async fn run(&self, topic: String) -> Result<PlanAttempt, ExecutionError> {
        Ok(PlanAttempt {
            plan: format!("{topic}-plan"),
            accepted: true,
            attempt: 1,
        })
    }
}

/// 原样返回 `PlanAttempt` 的 Node（归属测试的直接接线目标）。
struct Reattempt;

impl Node for Reattempt {
    type Input = PlanAttempt;
    type Output = PlanAttempt;

    async fn run(&self, input: PlanAttempt) -> Result<PlanAttempt, ExecutionError> {
        Ok(input)
    }
}

/// 一个把 `String` 变成大写的外部 Node，用于归属测试。
struct Upper;

impl Node for Upper {
    type Input = String;
    type Output = String;

    async fn run(&self, input: String) -> Result<String, ExecutionError> {
        Ok(input.to_uppercase())
    }
}

/// 命名装配的目标结构（归属测试用）。
struct Wrapped {
    plan: String,
}

struct UseWrapped;

impl Node for UseWrapped {
    type Input = Wrapped;
    type Output = String;

    async fn run(&self, input: Wrapped) -> Result<String, ExecutionError> {
        Ok(input.plan)
    }
}

#[test]
fn a_foreign_ref_is_rejected_even_through_projection_or_named_assembly() {
    // 归属 Flow 自己产生的 `Ref<PlanAttempt>`：只在它自己的 Flow 内合法。
    let mut owner = FlowBuilder::<Brief>::new();
    let owner_brief = owner.input();
    let attempt = owner
        .then(MakeAttempt, srflow::field!(owner_brief.topic))
        .expect("连接失败");
    // 对照：同一个 Flow 内该位置可以被多次投影复用（同一 Flow 的 Ref 正常）。
    let reused = owner
        .then(
            UseWrapped,
            srflow::bind!(Wrapped {
                plan: srflow::field!(attempt.plan)
            }),
        )
        .expect("连接失败");
    let _ = owner.output(reused).expect("连接失败");

    // 1) 直接使用外来 Ref（`PlanAttempt` 非 `Clone`，因此走消费读取）
    let mut direct = FlowBuilder::<PlanAttempt>::new();
    assert!(
        matches!(
            direct.then(Reattempt, srflow::consume(attempt)),
            Err(FlowBuildError::ForeignRef)
        ),
        "跨 Flow 的 Ref 在构建期被拒绝"
    );

    // 2) 藏在字段投影里（普通使用者写法）
    let mut projected = FlowBuilder::<Brief>::new();
    assert!(
        matches!(
            projected.then(Upper, srflow::field!(attempt.plan)),
            Err(FlowBuildError::ForeignRef)
        ),
        "外来 Ref 藏在字段投影里同样被拒绝"
    );

    // 3) 藏在命名结构装配里
    let mut assembled = FlowBuilder::<Brief>::new();
    assert!(
        matches!(
            assembled.then(
                UseWrapped,
                srflow::bind!(Wrapped {
                    plan: srflow::field!(attempt.plan),
                })
            ),
            Err(FlowBuildError::ForeignRef)
        ),
        "外来 Ref 藏在命名装配里同样被拒绝"
    );
}

#[test]
fn the_workflow_output_is_not_shared_as_a_ref_across_flows() {
    // 父 Flow 只通过 Input／Output 与子执行交换数据：SubFlow 内部的位置在父级不可见。
    let workflow = build_story_workflow(StoryConfig::default());
    let mut parent = FlowBuilder::<Brief>::new();
    let brief = parent.input();

    // 把整条 story 流程当普通 child 使用：只接触它的 Input/Output。
    let nested = parent
        .then(
            workflow.flow,
            srflow::bind!(Brief {
                topic: srflow::field!(brief.topic)
            }),
        )
        .expect("连接失败");
    let parent = parent.output(nested).expect("连接失败");

    let runtime = Runtime::new();
    let result = block_on(runtime.execute(
        &parent,
        Brief {
            topic: String::from("雨夜来信"),
        },
    ))
    .expect("嵌套执行应当成功");
    assert_eq!(result.plan, "雨夜来信的三幕计划 v2");
}

#[test]
fn the_story_flow_uses_only_public_crate_root_api() {
    // 编译期证据：本文件与 support 只从 crate 根导入公共项（无 `core::` 私有模块、无 `Any`）。
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<Brief>();
    assert_send_sync::<Route>();

    // 类型化错误的公开识别方式（构建期错误 vs 执行期错误）。
    let build_error = FlowBuildError::ForeignRef;
    let execution_error = ExecutionError::new(StoryFailure::Plan);
    assert!(matches!(execution_error, ExecutionError::Failed(_)));
    assert!(build_error.source().is_none());
}
