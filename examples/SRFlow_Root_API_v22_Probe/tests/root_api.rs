use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};
use std::{cell::Cell, rc::Rc};
#[derive(Data, Debug, PartialEq)]
struct Request(u32);
#[derive(Data, Debug, PartialEq)]
struct Report(u32);
#[derive(Data, Debug, PartialEq)]
struct Other(u32);
async fn generate(q: Query<&Request>) -> Result<Report, BodyError> {
    Ok(Report(q.get().0 * 2))
}
async fn compare(q: Query<(&Request, &Request)>) -> Result<Report, BodyError> {
    let (a, b) = q.get();
    Ok(Report(a.0 * 10 + b.0))
}
fn definition(err: RunError, fragment: &str) {
    assert!(matches!(err, RunError::Definition(_)));
    assert!(err.to_string().contains(fragment), "{err}");
}
#[test]
fn r01_expected_owned_type_and_question_mark() {
    async fn run(rt: &Runtime) -> Result<Report, RunError> {
        let report: Report = rt
            .execute(
                |f, _input| {
                    f.then(generate);
                    f.output::<Report>();
                },
                Request(21),
            )
            .await?;
        Ok(report)
    }
    assert_eq!(block_on(run(&Runtime::new())).unwrap(), Report(42));
}
#[test]
fn r02_enclosing_result_type_infers_output() {
    async fn run(rt: &Runtime) -> Result<Report, RunError> {
        rt.execute(
            |f, _| {
                f.then(generate);
                f.output::<Report>();
            },
            Request(4),
        )
        .await
    }
    assert_eq!(block_on(run(&Runtime::new())).unwrap(), Report(8));
}
#[test]
fn r03_root_input_same_type_distinct_explicit_refs() {
    let report: Report = block_on(Runtime::new().execute(
        |f, (original, revised)| {
            f.then_with(compare, (original, revised));
            f.output::<Report>();
        },
        (Request(1), Request(9)),
    ))
    .unwrap();
    assert_eq!(report, Report(19));
}
#[test]
fn r04_explicit_repeated_read_only_ref_is_legal() {
    let report: Report = block_on(Runtime::new().execute(
        |f, (original, _)| {
            f.then_with(compare, (original, original));
            f.output::<Report>();
        },
        (Request(2), Request(9)),
    ))
    .unwrap();
    assert_eq!(report, Report(22));
}
#[test]
fn r05_wrong_output_rejected_before_node_and_factory() {
    let nodes = Cell::new(0);
    let factories = Cell::new(0);
    let node = async |_: Query<()>| -> Result<Report, BodyError> {
        nodes.set(nodes.get() + 1);
        Ok(Report(2))
    };
    let result: Result<Other, RunError> = block_on(Runtime::new().execute(
        |f, _| {
            f.provide(|| {
                factories.set(factories.get() + 1);
                9u32
            })
            .then(&node);
            f.output::<Report>();
        },
        (),
    ));
    definition(result.unwrap_err(), "does not match");
    assert_eq!((nodes.get(), factories.get()), (0, 0));
}
#[test]
fn r06_non_unit_missing_output_rejected_before_execution() {
    let calls = Cell::new(0);
    let result: Result<Report, RunError> = block_on(Runtime::new().execute(
        |f, _| {
            f.provide(|| {
                calls.set(calls.get() + 1);
                Report(4)
            });
        },
        (),
    ));
    definition(result.unwrap_err(), "missing Root output");
    assert_eq!(calls.get(), 0);
}
#[test]
fn r07_unit_absent_and_explicit_legal() {
    let _: () = block_on(Runtime::new().execute(|_, _| {}, ())).unwrap();
    let _: () = block_on(Runtime::new().execute(
        |f, _| {
            f.output::<()>();
        },
        (),
    ))
    .unwrap();
}
#[test]
fn r08_unit_and_non_unit_do_not_silently_coerce() {
    let result: Result<(), RunError> = block_on(Runtime::new().execute(
        |f, _| {
            f.provide(|| Report(3));
            f.output::<Report>();
        },
        (),
    ));
    definition(result.unwrap_err(), "does not match");
    let result: Result<Report, RunError> = block_on(Runtime::new().execute(
        |f, _| {
            f.output::<()>();
        },
        (),
    ));
    definition(result.unwrap_err(), "does not match");
}
#[test]
fn r09_non_clone_multi_output_uses_original_typed_extraction() {
    let result: (Request, Report) = block_on(Runtime::new().execute(
        |f, r| {
            let p: Ref<Report> = f.then_output(generate);
            f.output_with((r, p));
        },
        Request(5),
    ))
    .unwrap();
    assert_eq!(result, (Request(5), Report(10)));
}
#[test]
fn r10_auto_duplicate_query_rejected_even_with_two_sources() {
    let calls = Cell::new(0);
    let result: Result<Report, RunError> = block_on(Runtime::new().execute(
        |f, _| {
            f.provide(|| {
                calls.set(calls.get() + 1);
                Other(8)
            })
            .then(compare);
            f.output::<Report>();
        },
        (Request(1), Request(9)),
    ));
    definition(result.unwrap_err(), "duplicate input types");
    assert_eq!(calls.get(), 0);
}
#[test]
fn r11_duplicate_output_declaration_rejected() {
    let result: Result<(), RunError> = block_on(Runtime::new().execute(
        |f, _| {
            f.output::<()>();
            f.output::<()>();
        },
        (),
    ));
    definition(result.unwrap_err(), "duplicate output");
}
#[test]
fn r12_rebuild_per_execute_instead_of_public_definition_cache() {
    let builds = Cell::new(0);
    let rt = Runtime::new();
    for value in [2, 7] {
        let result: Report = block_on(rt.execute(
            |f, _| {
                builds.set(builds.get() + 1);
                f.then(generate);
                f.output::<Report>();
            },
            Request(value),
        ))
        .unwrap();
        assert_eq!(result, Report(value * 2));
    }
    assert_eq!(builds.get(), 2);
}
#[test]
fn r13_unpolled_execute_does_not_build_or_call_factory() {
    let builds = Cell::new(0);
    let rt = Runtime::new();
    let pending = rt.execute::<_, Report, _>(
        |f, _| {
            builds.set(builds.get() + 1);
            f.provide(|| Report(4));
            f.output::<Report>();
        },
        (),
    );
    assert_eq!(builds.get(), 0);
    drop(pending);
    assert_eq!(builds.get(), 0);
}
#[test]
fn r14_regular_business_body_reused_under_root_chain() {
    fn business(f: &mut Flow) {
        f.then(generate);
        f.output::<Report>();
    }
    let result: Report = block_on(Runtime::new().execute(
        |f, _| {
            f.chain(business);
            f.output::<Report>();
        },
        Request(6),
    ))
    .unwrap();
    assert_eq!(result, Report(12));
}
#[test]
fn r15_non_send_borrowed_node_keeps_input_across_await() {
    let increment = Rc::new(Cell::new(5));
    let node = async |q: Query<&Request>| -> Result<Report, BodyError> {
        let r = q.get();
        futures::future::ready(()).await;
        Ok(Report(r.0 + increment.get()))
    };
    let result: Report = block_on(Runtime::new().execute(
        |f, _| {
            f.then(&node);
            f.output::<Report>();
        },
        Request(2),
    ))
    .unwrap();
    assert_eq!(result, Report(7));
}
#[test]
fn r16_distinct_aliases_to_same_data_still_rejected_before_take() {
    let result: Result<(Request, Request), RunError> = block_on(Runtime::new().execute(
        |f, input| {
            f.chain(|sub| {
                sub.output_with(input);
            });
            let alias = f.ref_of::<Request>();
            f.output_with((input, alias));
        },
        Request(2),
    ));
    assert!(matches!(result, Err(RunError::Runtime(_))));
}
#[test]
fn r17_expected_type_can_be_explicit_type_argument() {
    let value = block_on(Runtime::new().execute::<_, Report, _>(
        |f, _| {
            f.then(generate);
            f.output::<Report>();
        },
        Request(8),
    ))
    .unwrap();
    assert_eq!(value, Report(16));
}
