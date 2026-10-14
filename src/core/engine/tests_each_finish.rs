//! WE-T06 §5.1／X17：Each `finish_collectors` 的真实 Engine 失败主链样本。
//!
//! 目标：从真实 Definition 入口 `run_body → invoke_each → EachCollect::finish_collectors`
//! 在第 2 个 finish 的真实前提处造成代表拒绝，证明无 caller 半成功、无 Each 最终 Export、
//! 沿公开 Runtime 返回 `Runtime` 且 `source` 保留真实 Core 诊断、Root take 零、逐实例析构恰一次。
//!
//! 钩子：`boundary::install_each_finish_occupy_fault(1)` 只在下一次 Each 整组 finish 之前，
//! 以生产既有的正式 `register_owned` 占用第 2 个槽的 Each 本地最终输出位置——即破坏“目标
//! 位置在 finish 时仍未绑定”这一真实前提。它不返回伪造 `Err`、不绕过 `finish_collectors`、
//! 不放宽任何判断；拒绝仍由 `finish_collector` 的既有预检产生真实 `RefAlreadyBound`。
//!
//! 归属：本文件由 WE-T06 的 Each-finish 工作项独占；新增窄 `#[cfg(test)]` 钩子只破坏真实
//! 前提或只读观察。原 T05 P26 Core 手动样本（`tests_each_consume.rs::p26_*`）保留原样。

#![allow(clippy::unused_unit, clippy::let_unit_value, clippy::type_complexity)]

use super::boundary;
use super::data::Data;
use super::error::{BodyError, FrameworkDiagnostic, RunError};
use super::flow::Flow;
use super::observe;
use super::runtime::Runtime;
use super::shape::{Query, Ref};
use crate::core::internal_error::ScopeError;
use crate::core::scope::TargetSnapshot;
use crate::core::test_support;
use futures::executor::block_on;
use std::cell::RefCell;
use std::error::Error;

// ---------------------------------------------------------------- 业务类型与事件记录

thread_local! {
    /// 单线程执行下的业务调用与 Drop 顺序。
    static LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn log(event: impl Into<String>) {
    LOG.with(|entries| entries.borrow_mut().push(event.into()));
}

fn take_log() -> Vec<String> {
    LOG.with(|entries| std::mem::take(&mut *entries.borrow_mut()))
}

/// 每样本起点：复位引擎观察通道、Core 观察通道与本模块事件序列。
fn reset() {
    observe::reset();
    test_support::reset_observations();
    take_log();
}

#[derive(Debug, PartialEq, Eq)]
struct Item(u32);
impl Data for Item {}
impl Drop for Item {
    fn drop(&mut self) {
        log(format!("drop:item:{}", self.0));
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Out(u32);
impl Data for Out {}
impl Drop for Out {
    fn drop(&mut self) {
        log(format!("drop:out:{}", self.0));
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Tag(u32);
impl Data for Tag {}
impl Drop for Tag {
    fn drop(&mut self) {
        log(format!("drop:tag:{}", self.0));
    }
}

async fn score(query: Query<&Item>) -> Result<Out, BodyError> {
    let value = query.get().0;
    log(format!("node:score:{value}"));
    Ok(Out(value * 2))
}

async fn tagger(query: Query<&Item>) -> Result<Tag, BodyError> {
    let value = query.get().0;
    log(format!("node:tagger:{value}"));
    Ok(Tag(value + 10))
}

/// 两槽 item Body：集合逐项产生 `(Out, Tag)`，每个槽聚合成 Each-owned `Vec`。
fn two_slots<'n>(flow: &mut Flow<'n>, items: Ref<Vec<Item>>) -> (Ref<Vec<Out>>, Ref<Vec<Tag>>) {
    flow.each(items, |item, each| {
        let doubled = each.then(&score, item);
        let labelled = each.then(&tagger, item);
        (doubled, labelled)
    })
}

// ================================================================ 正例基线

/// 同一 Definition 在不安装钩子时正常导出两个聚合 `Vec`：证明失败确由预占钩子造成，
/// 且真实 Root take 恰为两个聚合身份。
#[test]
fn x17_baseline_two_slot_each_exports_without_fault() {
    reset();
    let runtime = Runtime::new();
    let outcome: Result<(Vec<Out>, Vec<Tag>), RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| two_slots(flow, items),
        vec![Item(1), Item(2), Item(3)],
    ));
    let (doubled, labelled) = outcome.expect("the two-slot each exports without a fault");
    assert_eq!(
        doubled.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![2, 4, 6]
    );
    assert_eq!(
        labelled.iter().map(|tag| tag.0).collect::<Vec<_>>(),
        vec![11, 12, 13]
    );

    let rounds = observe::take_controller_rounds();
    let exported = rounds
        .iter()
        .filter(|record| record.kind == "each" && record.at == "exported")
        .count();
    assert_eq!(exported, 1, "the unfaulted each exports exactly once");

    let takes = test_support::take_log_snapshot();
    assert_eq!(
        takes.len(),
        2,
        "two aggregated identities are taken: {takes:?}"
    );
    assert_ne!(
        takes[0], takes[1],
        "each slot keeps an independent Vec identity"
    );
}

// ================================================================ X17：第 2 槽 finish 拒绝

/// X17：真实 Definition 经 `run_body → invoke_each → finish_collectors`，在第 2 个 finish
/// 的真实前提（目标位置未绑定）被正式操作破坏后逐槽拒绝。第 1 槽已形成 Each-owned Vec，
/// 第 2 槽的 collector 仍在未完成建构区；无 caller 半成功、无 Each 最终 Export、沿公开
/// Runtime 返回 `Runtime` 且 `source` 保留真实 Core 诊断、Root take 零、逐实例析构恰一次。
#[test]
fn x17_second_slot_finish_reject_closes_the_engine_path() {
    reset();
    // 正式操作预占第 2 个槽（index = 1）的 Each 本地最终输出位置。
    boundary::install_each_finish_occupy_fault(1);

    let runtime = Runtime::new();
    let outcome: Result<(Vec<Out>, Vec<Tag>), RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| two_slots(flow, items),
        vec![Item(1), Item(2), Item(3)],
    ));

    // 1) 沿公开 Runtime 返回 Runtime 分类，note 为真实收口措辞。
    let error = outcome.expect_err("the occupied second target rejects the finish");
    let runtime_error = match &error {
        RunError::Runtime(failure) => failure,
        other => panic!("expected RunError::Runtime, got {other:?}"),
    };
    assert_eq!(runtime_error.note(), "each collector finish rejected");
    assert!(
        runtime_error.cleanup_source().is_none(),
        "no additional cleanup fault is introduced by this sample"
    );

    // 2) 真实失败现场：只登记一次。
    let scenes = test_support::take_each_finish_reject();
    assert_eq!(scenes.len(), 1, "exactly one Each finish reject scene");
    let scene = &scenes[0];
    assert_eq!(scene.slot_locals.len(), 2);
    assert_eq!(scene.slot_outputs.len(), 2);
    assert_eq!(scene.collectors.len(), 2);
    assert_ne!(scene.slot_locals[0], scene.slot_locals[1]);
    assert_ne!(scene.collectors[0], scene.collectors[1]);

    // 3) 后槽拒绝原因与真实位置／collector 身份正确：第 2 槽（非第 1 槽）的本地目标被拒。
    match &scene.rejected {
        ScopeError::RefAlreadyBound { scope, position } => {
            assert_eq!(scope, &scene.each, "the rejected scope is the Each scope");
            assert_eq!(
                position, &scene.slot_locals[1],
                "the rejected position is the second slot's Each-local target"
            );
            assert_ne!(
                position, &scene.slot_locals[0],
                "the first slot is not the rejected target"
            );
        }
        other => panic!("expected RefAlreadyBound, got {other:?}"),
    }

    // 4) 第 1 槽已 finish（collector 已离开建构区），第 2 槽仍有未完成 collector（3 次收项）。
    assert_eq!(
        scene.collector_present,
        vec![false, true],
        "the first collector is finished, the second is still unterminated"
    );
    assert_eq!(
        scene.collector_moves,
        vec![None, Some(3)],
        "the second collector already took all three items"
    );

    // 5) 第 1 槽已形成 Each-owned 普通 Vec；第 2 槽的本地位置被占位值绑定，二者同为 Each-owned。
    let first = scene
        .each_refs
        .iter()
        .find(|(position, _)| position == &scene.slot_locals[0])
        .expect("the first slot's local target is bound to the finished Vec");
    let TargetSnapshot::Data(first_vec) = &first.1 else {
        panic!("the first slot binds a complete Data (the aggregated Vec)");
    };
    assert!(
        scene.each_owned.contains(first_vec),
        "the finished Vec is owned by the Each scope"
    );
    let second = scene
        .each_refs
        .iter()
        .find(|(position, _)| position == &scene.slot_locals[1])
        .expect("the second slot's local target is bound to the occupant");
    let TargetSnapshot::Data(occupant) = &second.1 else {
        panic!("the second slot binds the occupant Data");
    };
    assert!(scene.each_owned.contains(occupant));
    assert_ne!(
        first_vec, occupant,
        "the Vec and the occupant are distinct Data"
    );
    assert_eq!(
        scene.each_owned.len(),
        2,
        "the Each scope owns exactly the finished Vec and the occupant"
    );

    // 6) 无 caller 半成功：caller 的每个声明输出位置都未被绑定。
    assert_ne!(
        scene.caller, scene.each,
        "the caller is the Each's parent, not the Each"
    );
    assert!(
        !scene.caller_refs.is_empty(),
        "the caller still binds its own collection input"
    );
    for output in &scene.slot_outputs {
        assert!(
            !scene
                .caller_refs
                .iter()
                .any(|(position, _)| position == output),
            "caller output must stay unbound (no partial success)"
        );
    }
    assert!(
        !scene.caller_owned.is_empty(),
        "the caller still owns the collection input value"
    );
    assert!(
        !scene.caller_owned.contains(first_vec) && !scene.caller_owned.contains(occupant),
        "no Each aggregate or occupant is handed to the caller"
    );

    // 7) `source` 保留真实 Core 诊断（同一 Display，variant 为 RefAlreadyBound）。
    let source = runtime_error
        .source()
        .expect("the original Scope diagnostic is preserved");
    let diagnostic = source
        .downcast_ref::<FrameworkDiagnostic>()
        .expect("the preserved source is the framework's Scope diagnostic");
    let debug = format!("{diagnostic:?}");
    assert!(
        debug.starts_with("FrameworkDiagnostic { scope_error: RefAlreadyBound {"),
        "exact diagnostic variant: {debug}"
    );
    assert_eq!(
        format!("{source}"),
        format!("{}", scene.rejected),
        "the public source is byte-for-byte the real Core diagnostic"
    );

    // 8) 无 Each 最终 Export：真实控制事件序列在最后一个 item 收口后即中断。
    let rounds = observe::take_controller_rounds();
    let each_events: Vec<(&'static str, usize)> = rounds
        .iter()
        .filter(|record| record.kind == "each")
        .map(|record| (record.at, record.index))
        .collect();
    assert_eq!(
        each_events,
        vec![
            ("scope-created", 0),
            ("item-created", 0),
            ("item-consumed", 0),
            ("item-created", 1),
            ("item-consumed", 1),
            ("item-created", 2),
            ("item-consumed", 2),
        ],
        "the each stops at the last item's consume and never exports: {each_events:?}"
    );
    assert!(
        !each_events.iter().any(|(at, _)| *at == "exported"),
        "no Each final Export is recorded"
    );

    // 9) Root 正常 take 为零。
    assert!(
        test_support::take_log_snapshot().is_empty(),
        "the failed run removes no root target"
    );

    // 10) 逐实例析构恰一次：输入集合、已聚合 Vec、未完成 collector 建构值。
    let drops = take_log();
    let count = |prefix: &str| {
        drops
            .iter()
            .filter(|event| event.starts_with(prefix))
            .count()
    };
    assert_eq!(count("drop:item:"), 3, "collection elements: {drops:?}");
    assert_eq!(count("drop:out:"), 3, "the finished slot's Vec: {drops:?}");
    assert_eq!(count("drop:tag:"), 3, "the unfinished collector: {drops:?}");
    for event in [
        "drop:item:1",
        "drop:item:2",
        "drop:item:3",
        "drop:out:2",
        "drop:out:4",
        "drop:out:6",
        "drop:tag:11",
        "drop:tag:12",
        "drop:tag:13",
    ] {
        assert_eq!(
            drops.iter().filter(|observed| *observed == event).count(),
            1,
            "{event} drops exactly once: {drops:?}"
        );
    }
}
