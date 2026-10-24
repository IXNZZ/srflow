//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_round_local_leak.rs`）：round-local 位置泄漏到 parent。
//!
//! 原目的（X24/X05／C13／C15）：Iter 每轮的 formal `current` 由 StateImport 导入，其 owner 是该轮
//! RoundScope：只在轮内词法可见（SPEC §11「current round-local」）。把轮内 `current` 带到轮外使用，
//! 必须在 Definition 构建期按 `OutOfScope(ChildLocal)` 拒绝（诊断含 "is not lexically visible"），
//! 业务 Node 零调用。
//!
//! 判定与归因：**Definition 构建期拒绝**（`RunError::Definition`），不是 rustc 拒绝。
//!
//! 迁移手法：Root 闭包改为固定返回 `()` 并经 `output_with` 声明 owned `Out = Amount`；旧两参
//! `flow.then(node, refs)`（含轮内与 parent 内各一次）改为 `then_with_output`。泄漏路径与零调用
//! 断言不变。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t06-build-reject-ok`。

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Amount(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn bump(query: Query<&Amount>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0 + 1))
}

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    let leaked: Cell<Option<Ref<Amount>>> = Cell::new(None);
    let error = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, amount: Ref<Amount>| {
            let declared = flow.iter_output(amount, 3, |round| {
                let current = round.current::<Amount>();
                leaked.set(Some(current));
                let next = round.then_with_output(&bump, current);
                round.next_with(next);
            });
            // declared 是本轮 StateShape 的声明输出（合法）；leaked 是轮内 current（非法）。
            let _ = declared;
            let out_refs = flow.then_with_output(&double, leaked.get().expect("round-local ref was captured"));
            flow.output_with(out_refs);
        },
        Amount(1),
    ))
    .err()
    .expect("a round-local Ref used in the parent is rejected at build time");

    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );
    assert!(
        error.to_string().contains("lexically visible"),
        "诊断应指出词法可见性越界：{error}"
    );
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "被拒绝的 Definition 不执行任何业务 Node"
    );

    println!("we_t06 round-local leak: {error}");
    println!("we_t06-build-reject-ok");
}
