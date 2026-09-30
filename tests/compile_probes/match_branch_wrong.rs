//! 负向：[错误] Match 需要 Vec<String>，分支产出 Vec<usize>。
use srflow::{ExecutionError, Match, Node};

#[derive(Debug, PartialEq, Eq)]
enum Route {
    A,
}

struct Branch;
impl Node for Branch {
    type Input = String;
    type Output = Vec<usize>;
    async fn run(&self, input: String) -> Result<Vec<usize>, ExecutionError> {
        Ok(vec![input.len()])
    }
}

fn build() {
    let mut builder = Match::<Route, String, Vec<String>>::builder();
    let _ = builder.case(Route::A, Branch); // 此行报错：<Branch as Executable>::Output == Vec<String>
}
