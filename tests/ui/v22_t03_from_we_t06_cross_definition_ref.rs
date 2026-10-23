//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_cross_definition_ref.rs`）：跨执行／跨 Definition 的 `Ref`。
//!
//! 原目的（X24/X05／C13／C15）：第一次执行把 owned 产物的 `Ref` 带出定义闭包（跨执行泄漏），
//! 第二次执行把它作为节点输入（场景 A）或 child 声明输出（场景 B）→ **Definition 构建期拒绝**
//! （`RunError::Definition`，诊断含 "belongs to another definition"），业务 Node 计数不变。
//!
//! 判定与归因：这是 **Definition 构建期拒绝**，**不是 rustc 拒绝**——两者在类型层面完全合法
//! （`Ref` 是 `Copy`，形状一致）。因此本夹具以 `run` 模式真实编译并运行，运行时断言拒绝。
//!
//! 迁移手法：三处 Root 闭包改为固定返回 `()` 并经 `output_with` 声明（`Mark`／`Amount`／`Mark`），
//! 旧两参 `flow.then(node, refs)` 改为 `then_with_output`；泄漏与场景语义、断言完全不变。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t06-build-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Item(u32);

#[derive(Data)]
struct Mark(u32);

#[derive(Data)]
struct Amount(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn touch(query: Query<&Item>) -> Result<Mark, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Mark(query.get().0))
}

async fn review(query: Query<&Mark>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    // 第一次执行：把产物 Ref 带出定义闭包，成为「跨执行泄漏」的 foreign Ref。
    let mut leaked: Option<Ref<Mark>> = None;
    let first = block_on(runtime.execute::<_, Mark, _>(
        |flow: &mut Flow<'_>, item: Ref<Item>| {
            let mark = flow.then_with_output(&touch, item);
            leaked = Some(mark);
            flow.output_with(mark);
        },
        Item(7),
    ))
    .expect("first execution succeeds");
    assert_eq!(first.0, 7, "第一次执行真实跑过一次业务 Node");
    let foreign = leaked.expect("the output ref leaked out of the definition closure");
    let calls_after_first = CALLS.load(Ordering::SeqCst);
    assert_eq!(calls_after_first, 1);

    // 场景 A：foreign Ref 作为第二次执行的节点输入 → Definition 拒绝，任何业务 Node 不执行。
    let error_a = block_on(runtime.execute::<_, Amount, _>(
        move |flow: &mut Flow<'_>, _item: Ref<Item>| {
            let out_refs = flow.then_with_output(&review, foreign);
            flow.output_with(out_refs);
        },
        Item(1),
    ))
    .err()
    .expect("a foreign Ref as a node input is rejected before any business Node runs");
    assert!(
        matches!(error_a, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error_a}"
    );
    assert!(
        error_a.to_string().contains("another definition"),
        "诊断应指出跨 Definition 来源：{error_a}"
    );

    // 场景 B：foreign Ref 作为 child 的声明输出 → 同样在构建期拒绝。
    let error_b = block_on(runtime.execute::<_, Mark, _>(
        move |flow: &mut Flow<'_>, _item: Ref<Item>| {
            let out_refs = flow.chain(|_sub| foreign);
            flow.output_with(out_refs);
        },
        Item(2),
    ))
    .err()
    .expect("a foreign Ref as a declared child output is rejected");
    assert!(
        matches!(error_b, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error_b}"
    );

    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        calls_after_first,
        "被拒绝的 Definition 不执行任何业务 Node"
    );

    println!("we_t06 foreign input:  {error_a}");
    println!("we_t06 foreign output: {error_b}");
    println!("we_t06-build-reject-ok");
}
