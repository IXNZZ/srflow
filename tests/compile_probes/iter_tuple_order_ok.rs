//! 正向对照：父 Flow 提供 `(Vec<Item>, T)`，顺序正确。
use srflow::{ExecutionError, FlowBuilder, Iter, Node};

struct Advance;
impl Node for Advance {
    type Input = (String, String);
    type Output = String;
    async fn run(&self, (state, item): (String, String)) -> Result<String, ExecutionError> {
        Ok(format!("{state}{item}"))
    }
}

fn build() {
    let mut flow = FlowBuilder::<(Vec<String>, String)>::new();
    let input = flow.input();
    let state = flow.then_move(Iter::new(Advance), input).unwrap();
    let _ = flow.output(state).unwrap();
}
