use srflow::{BodyError, Data, Flow, Query, RunError, Runtime};
#[derive(Data, Debug, PartialEq)]
struct Text(String);
#[derive(Data, Debug, PartialEq)]
struct Report(String);
async fn compare(q: Query<(&Text, &Text)>) -> Result<Report, BodyError> {
    let (a, b) = q.get();
    Ok(Report(format!("{} -> {}", a.0, b.0)))
}
fn audit_fragment(_flow: &mut Flow) {}
async fn run(runtime: &Runtime) -> Result<Report, RunError> {
    let report: Report = runtime
        .execute(
            |flow, (original, revised)| {
                audit_fragment(flow);
                flow.then_with(compare, (original, revised));
                flow.output::<Report>();
            },
            (Text("before".into()), Text("after".into())),
        )
        .await?;
    Ok(report)
}
fn main() {
    let report = futures::executor::block_on(run(&Runtime::new())).unwrap();
    assert_eq!(report.0, "before -> after");
    println!("{}", report.0);
}
