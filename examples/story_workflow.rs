//! 示例：story_workflow —— 一条端到端的"计划 → 路由 → 逐项加工 → 逐轮推进"流程。
//!
//! 全部由**离线 Fake Node** 组成，不访问网络、模型、文件或真实业务数据；正文内容是示意压力场景，
//! 不代表任何真实生成质量。
//!
//! ```text
//! Brief ─(投影 topic)→ Retry(GeneratePlan → CheckPlan 的 SubFlow) → PlanAttempt
//!   PlanAttempt ─(投影 plan)→ JudgeRoute → Route ─┐
//!   PlanAttempt ─(后继消费)→ MakeInitial → T0     │
//! Brief ─(投影 topic)→ Match 的业务 Input ────────┤
//! Match((Route, 业务 Input)) → 唯一分支 → Vec<KeySeed> → Each → Vec<KeyNode>
//! Brief ─(投影 topic)→ PlanRevision → RevisionPolicy（第三个独立位置）
//! Iter((Vec<KeyNode>, T0)) → StoryState
//! Finalize：bind!{ topic←Brief、plan／prose／needs_revision←StoryState、policy←RevisionPolicy } → FinalResult
//! ```
//!
//! 四种控制型 Executable 各在其位：
//!
//! | 控制器 | 在这个流程里的理由 |
//! | --- | --- |
//! | `Retry` | 计划可能不被接受，需要用**同一个**主题重做同一段 Body（GeneratePlan → CheckPlan） |
//! | `Match` | 路由值 `Route` 已由 `JudgeRoute` 判断完成，只需要选择**唯一**一个关键节点生成分支 |
//! | `Each` | 关键节点的加工彼此独立，**不需要**上一项的结果 |
//! | `Iter` | 后一个节点必须看到前一个节点**已经写进正文**的内容，状态沿轮次推进 |
//!
//! 运行：`cargo run --example story_workflow`

#[path = "support/story_workflow.rs"]
mod support;

use support::{StoryConfig, build_story_workflow, run_story_workflow};

fn main() {
    // 正常路径：第 2 稿计划被接受，路由走 Deep（SubFlow 分支），三个关键节点逐轮推进正文。
    let workflow = build_story_workflow(StoryConfig::default());
    match run_story_workflow(&workflow, "雨夜来信") {
        Ok(result) => {
            println!("最终结果：");
            println!("  topic          = {}", result.topic);
            println!("  plan           = {}", result.plan);
            println!("  prose          = {}", result.prose);
            println!("  needs_revision = {}", result.needs_revision);
            println!(
                "  revision_rounds = {}（来自第三个独立位置）",
                result.revision_rounds
            );
            println!(
                "  计划轮数 = {}，Each 收到的原始 Item = {:?}，Iter 收到的 Item = {:?}",
                workflow
                    .plan_attempts
                    .load(std::sync::atomic::Ordering::SeqCst),
                workflow.refined_seeds.lock().expect("poisoned"),
                workflow.drafted_nodes.lock().expect("poisoned"),
            );
            println!("事件序列：{}", workflow.events.text());
        }
        Err(error) => println!("执行失败：{error}"),
    }

    // 未命中路径：判断给出没有登记 case 的路由值，于是执行 Match 的 default。
    let fallback = build_story_workflow(StoryConfig {
        force_route: Some(support::Route::Unknown),
        ..StoryConfig::default()
    });
    match run_story_workflow(&fallback, "雨夜来信") {
        Ok(result) => println!("\n未命中 default：prose = {}", result.prose),
        Err(error) => println!("\n未命中 default 执行失败：{error}"),
    }
    println!("未命中事件序列：{}", fallback.events.text());
}
