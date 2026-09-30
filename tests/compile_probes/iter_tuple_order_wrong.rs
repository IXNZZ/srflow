//! 负向：[错误] 父 Flow 的 tuple 顺序写反（T 在前、集合在后）。
use srflow::{ExecutionError, FlowBuilder, Iter, Node, consume};

struct Advance;
impl Node for Advance {
    type Input = (String, String);
    type Output = String;
    async fn run(&self, (state, item): (String, String)) -> Result<String, ExecutionError> {
        Ok(format!("{state}{item}"))
    }
}

struct MakeItems;
impl Node for MakeItems {
    type Input = String;
    type Output = Vec<String>;
    async fn run(&self, input: String) -> Result<Vec<String>, ExecutionError> {
        Ok(vec![input])
    }
}

struct MakeInitial;
impl Node for MakeInitial {
    type Input = String;
    type Output = String;
    async fn run(&self, input: String) -> Result<String, ExecutionError> {
        Ok(input)
    }
}

fn build() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let items = flow.then(MakeItems, input).unwrap();
    let initial = flow.then(MakeInitial, input).unwrap();
    // 此行报错：<(Consume<String>, Consume<Vec<String>>) as Binding>::Output == (Vec<String>, String)
    let _ = flow.then(Iter::new(Advance), (consume(initial), consume(items)));
}
