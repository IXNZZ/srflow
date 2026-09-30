//! 负向：[错误] Retry 的 Body 需要 u32，父 Flow 的位置是 String。
use std::num::NonZeroUsize;

use srflow::{ExecutionError, FlowBuilder, Node, Retry, RetryDecision};

struct Body;
impl Node for Body {
    type Input = u32;
    type Output = (u32, bool);
    async fn run(&self, input: u32) -> Result<(u32, bool), ExecutionError> {
        Ok((input, true))
    }
}

fn build() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let _ = flow.then( // 此行报错：<Ref<String> as Binding>::Output == u32
        Retry::with_limit(
            Body,
            |_: &(u32, bool)| RetryDecision::Stop,
            NonZeroUsize::new(2).unwrap(),
        ),
        input,
    );
}
