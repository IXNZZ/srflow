//! T06 新增定义期样本 ⑩：`current::<T>()` 的请求槽数必须与本轮 formal Current 完全一致。
//!
//! 两槽 Round 里写 `current::<State>()`（请求 1 槽）会记录 `CurrentArityMismatch` 并使构建失败；
//! 同一 Round Body 内的合法 `current::<(State, State)>()` 不改变这一点，业务 Node 零调用。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we-t06-current-arity-mismatch-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct State(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn bump(query: Query<&State>) -> Result<State, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(State(query.get().0 + 1))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);
    let error = block_on(runtime.execute::<_, (State, State), _>(
        |flow: &mut Flow<'_>, (a, b): (Ref<State>, Ref<State>)| {
            let out_refs = flow.iter_output((a, b), 1, |round| {
                // 非法：本轮是 2 槽，请求 1 槽必须记录 Definition 期首因。
                let only = round.current::<State>();
                let (x, y) = round.current::<(State, State)>();
                round.then_with_output(&bump, x);
                round.next_with((x, y));
                let _ = only;
            });
            flow.output_with(out_refs);
        },
        (State(1), State(2)),
    ))
    .err()
    .expect("请求槽数与本轮不符必须在 Definition 阶段被拒绝");
    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "被拒绝的 Definition 不执行任何业务 Node"
    );
    println!("we_t06 current arity mismatch rejected: {error}");
    println!("we-t06-current-arity-mismatch-ok");
}
