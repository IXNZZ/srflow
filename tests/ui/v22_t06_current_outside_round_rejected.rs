//! T06 新增定义期样本 ⑦：普通 Body 调用 `current::<T>()` 必须在 Definition 阶段被拒绝。
//!
//! `current` 只允许在**直接 Iter Round Body** 内使用；普通 Body（这里是 Root）调用会记录
//! `CurrentOutsideRound` 并使构建失败，业务 Node 零调用。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we-t06-current-outside-round-reject-ok`。

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
    let error = block_on(runtime.execute::<_, State, _>(
        |flow: &mut Flow<'_>, initial: Ref<State>| {
            // 非法：current 只能在直接 Round Body 内使用。
            let escaped: Ref<State> = flow.current::<State>();
            let out_refs = flow.then_with_output(&bump, escaped);
            flow.output_with(out_refs);
        },
        State(1),
    ))
    .err()
    .expect("普通 Body 使用 current 必须在 Definition 阶段被拒绝");
    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "被拒绝的 Definition 不执行任何业务 Node"
    );
    println!("we_t06 current outside round rejected: {error}");
    println!("we-t06-current-outside-round-reject-ok");
}
