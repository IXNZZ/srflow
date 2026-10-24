//! T03 迁移自 `we_t04_e02_iter_state_shape_mismatch.rs`（WE-T04 E02 负例）：Iter 的 next 与
//! initial **Shape 不一致**。
//!
//! `Flow::iter<S, F>` 要求 initial 与 round Body 声明的 next **同一个** `S`（任务书 §3.3：
//! “Body 返回同 Shape 的 next；各槽类型与顺序一致”）。initial 是 2 元 flat tuple，而 next 声明
//! 3 元 tuple：必须被拒绝，而不是运行期来源校验。
//!
//! ## T06 迁移（阶段迁移）
//!
//! 旧两参闭包 `|(current_a, current_b), round| { …; (current_a, current_b, c) }` 的**返回类型**
//! 与 initial 的 `S` 不一致，故旧契约在编译期报 **E0308**。T06 起 Round Body 固定为单参 unit 闭包
//! （`FnOnce(&mut Flow) -> ()`），next 改由 `round.next_with(…)` 显式声明；因此“initial 2 槽 vs
//! next 3 槽”的同一 arity/Shape 不一致不再由 rustc 检出，而是在 **Definition 构建期** 被拒绝
//! （`RunError::Definition`，主因 `DeclarationArityMismatch { … expected: 2, found: 3 }`）。
//!
//! 判定目的与断言不变：initial 与 Next 的 arity/逐槽 Shape 不一致必须被拒，且被拒的 Definition
//! 不执行任何业务 Node。载体路径与 case_id 保持稳定（`WE_T04_PROBE::…shape_mismatch`）。
//!
//! 期望：编译成功，运行退出码 0，stdout 含
//! `we-t06-from-t04-iter-state-shape-mismatch-reject-ok`。

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};

use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

/// 最小同步执行器：`Runtime::execute` 的 Future 在首次 poll 即就绪，故用 noop Waker 轮询即可；
/// 由此夹具不依赖 `futures`，与 `ui_check.py` 的 rustc 装配一致。
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = TaskContext::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[derive(Data)]
struct A(u32);
#[derive(Data)]
#[allow(dead_code)] // 仅作为 initial/next 的形状槽，字段不参与业务
struct B(u32);
#[derive(Data)]
#[allow(dead_code)] // 仅作为 next 的第 3 槽，字段不参与业务
struct C(u32);

/// 业务 Node 计数：被拒绝的 Definition 不得开始任何 Node。
static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn step(query: Query<&A>) -> Result<A, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(A(query.get().0 + 1))
}

fn main() {
    let runtime = Runtime::new();

    let error = block_on(runtime.execute::<_, (), _>(
        |flow: &mut Flow<'_>, (a, b, c): (Ref<A>, Ref<B>, Ref<C>)| {
            // initial 是 2 元 flat tuple (A, B)；Body 声明 3 元 next (A, B, C)：arity/Shape 不一致。
            let _ = flow.iter((a, b), 3, |round| {
                let (current_a, current_b) = round.current::<(A, B)>();
                let _ = round.then_with_output(&step, current_a);
                // T06：unit Body 下由 `next_with` 显式声明 next，Definition 期收口拒绝。
                round.next_with((current_a, current_b, c));
            });
            flow.output_with(());
        },
        (A(1), B(2), C(3)),
    ))
    .err()
    .expect("an initial/next arity-shape mismatch is rejected at definition build time");

    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );
    let text = error.to_string();
    // 2 元 initial vs 3 元 next：Definition 期按 arity 收口拒绝（expected 2 / found 3）。
    assert!(
        text.contains("has 3 slots, expected 2"),
        "诊断应指出 Iter 的 next 声明 arity 不一致：{text}"
    );
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "被拒绝的 Definition 不执行任何业务 Node"
    );

    println!("t06 iter state shape mismatch: {error}");
    println!("we-t06-from-t04-iter-state-shape-mismatch-reject-ok");
}
