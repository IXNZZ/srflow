//! 正向对照：Retry 的 Body 契约与父 Flow 提供的位置类型一致。
use std::num::NonZeroUsize;

use srflow::{ExecutionError, FlowBuilder, Node, Retry, RetryDecision};

struct Body;
impl Node for Body {
    type Input = String;
    type Output = (String, bool);
    async fn run(&self, input: String) -> Result<(String, bool), ExecutionError> {
        Ok((input, true))
    }
}

fn build() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let retried = flow
        .then(
            Retry::with_limit(
                Body,
                |_: &(String, bool)| RetryDecision::Stop,
                NonZeroUsize::new(2).unwrap(),
            ),
            input,
        )
        .unwrap();
    let _ = flow.output(retried).unwrap();
}
