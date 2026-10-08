use futures::executor::block_on;
use srflow_public_api_v21_probe::*;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
struct CharacterState(u32);
impl Data for CharacterState {}
struct BackgroundFacts(u32);
impl Data for BackgroundFacts {}
struct ExpressionTask(u32);
impl Data for ExpressionTask {}
struct WritingBoundary(Rc<RefCell<Vec<String>>>);
impl Data for WritingBoundary {}
struct Basis(u32);
impl Data for Basis {}
struct Plan(u32);
impl Data for Plan {}
#[derive(Debug)]
struct Prose {
    score: u32,
    events: Rc<RefCell<Vec<String>>>,
}
impl Data for Prose {}
struct Diagnosis {
    score: u32,
    acceptable: bool,
    events: Rc<RefCell<Vec<String>>>,
}
impl Data for Diagnosis {}
#[derive(PartialEq)]
enum RevisionAction {
    Reduce,
    Supplement,
    Rewrite,
}
impl Data for RevisionAction {}
struct BasisMaker(Rc<Cell<usize>>);
impl Node for BasisMaker {
    type Input = (
        CharacterState,
        BackgroundFacts,
        ExpressionTask,
        WritingBoundary,
    );
    type Output = Basis;
    async fn run(&self, q: Query<&Self::Input>) -> Result<Basis, BodyError> {
        let (_, _, task, boundary) = q.get();
        self.0.set(self.0.get() + 1);
        boundary.0.borrow_mut().push("basis".into());
        Ok(Basis(task.0))
    }
}
async fn make_plan(
    q: Query<(&CharacterState, &BackgroundFacts, &ExpressionTask)>,
) -> Result<Plan, BodyError> {
    let (c, f, _) = q.get();
    Ok(Plan(c.0 + f.0))
}
async fn write_prose(
    q: Query<(&Plan, &CharacterState, &BackgroundFacts, &WritingBoundary)>,
) -> Result<Prose, BodyError> {
    let (plan, _, _, boundary) = q.get();
    boundary.0.borrow_mut().push("write".into());
    Ok(Prose {
        score: plan.0,
        events: boundary.0.clone(),
    })
}
async fn diagnose(q: Query<(&Prose, &Basis)>) -> Result<Diagnosis, BodyError> {
    let (p, b) = q.get();
    p.events.borrow_mut().push(format!("diagnose:{}", p.score));
    Ok(Diagnosis {
        score: p.score,
        acceptable: p.score >= b.0,
        events: p.events.clone(),
    })
}
async fn stop_if_acceptable(q: Query<&Diagnosis>) -> Result<(), BodyError> {
    let d = q.get();
    d.events.borrow_mut().push(format!("stop:{}", d.score));
    if d.acceptable {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}
async fn choose_revision(q: Query<&Diagnosis>) -> Result<RevisionAction, BodyError> {
    let d = q.get();
    d.events.borrow_mut().push(format!("choose:{}", d.score));
    Ok(match d.score % 3 {
        0 => RevisionAction::Reduce,
        1 => RevisionAction::Supplement,
        _ => RevisionAction::Rewrite,
    })
}
fn revised(p: &Prose, action: &str) -> Prose {
    p.events.borrow_mut().push(action.into());
    Prose {
        score: p.score + 1,
        events: p.events.clone(),
    }
}
async fn revise(q: Query<(&Prose, &Diagnosis, &WritingBoundary)>) -> Result<Prose, BodyError> {
    let (p, _, _) = q.get();
    Ok(revised(p, "revise"))
}
async fn reduce(q: Query<(&Prose, &Diagnosis, &WritingBoundary)>) -> Result<Prose, BodyError> {
    let (p, _, _) = q.get();
    Ok(revised(p, "reduce"))
}
async fn supplement(
    q: Query<(&Prose, &Diagnosis, &ExpressionTask, &WritingBoundary)>,
) -> Result<Prose, BodyError> {
    let (p, _, _, _) = q.get();
    Ok(revised(p, "supplement"))
}
async fn rewrite(
    q: Query<(&Prose, &Diagnosis, &CharacterState, &BackgroundFacts)>,
) -> Result<Prose, BodyError> {
    let (p, _, _, _) = q.get();
    Ok(revised(p, "rewrite"))
}
type SESInput = (
    CharacterState,
    BackgroundFacts,
    ExpressionTask,
    WritingBoundary,
);
async fn scheme_a(r: &Runtime, maker: &BasisMaker, input: SESInput) -> Result<Prose, RunError> {
    r.execute(
        |flow, (characters, facts, task, boundary)| {
            let basis = flow.then(maker, (characters, facts, task, boundary));
            let draft = flow.chain(|writing| {
                let plan = writing.then(make_plan, (characters, facts, task));
                writing.then(write_prose, (plan, characters, facts, boundary))
            });
            let diagnosis = flow.then(diagnose, (draft, basis));
            flow.then(revise, (draft, diagnosis, boundary))
        },
        input,
    )
    .await
}
async fn scheme_b(
    r: &Runtime,
    maker: &BasisMaker,
    input: SESInput,
    limit: usize,
) -> Result<Prose, RunError> {
    r.execute(
        |flow, (characters, facts, task, boundary)| {
            let basis = flow.then(maker, (characters, facts, task, boundary));
            let draft = flow.chain(|writing| {
                let plan = writing.then(make_plan, (characters, facts, task));
                writing.then(write_prose, (plan, characters, facts, boundary))
            });
            flow.iter(draft, limit, |current, round| {
                let diagnosis = round.then(diagnose, (current, basis));
                round.then(stop_if_acceptable, diagnosis);
                let action = round.then(choose_revision, diagnosis);
                round.choose(action, |choice| {
                    choice.case(RevisionAction::Reduce, |branch| {
                        branch.then(reduce, (current, diagnosis, boundary))
                    });
                    choice.case(RevisionAction::Supplement, |branch| {
                        branch.then(supplement, (current, diagnosis, task, boundary))
                    });
                    choice.case(RevisionAction::Rewrite, |branch| {
                        branch.then(rewrite, (current, diagnosis, characters, facts))
                    });
                })
            })
        },
        input,
    )
    .await
}
fn inputs(target: u32) -> (SESInput, Rc<RefCell<Vec<String>>>) {
    let e = Rc::new(RefCell::new(vec![]));
    (
        (
            CharacterState(0),
            BackgroundFacts(0),
            ExpressionTask(target),
            WritingBoundary(e.clone()),
        ),
        e,
    )
}
#[test]
fn s10_schemes_reuse_business_nodes_and_frozen_basis() {
    block_on(async {
        let r = Runtime::new();
        let count = Rc::new(Cell::new(0));
        let maker = BasisMaker(count.clone());
        let (input, a) = inputs(2);
        assert_eq!(scheme_a(&r, &maker, input).await.unwrap().score, 1);
        assert_eq!(*a.borrow(), vec!["basis", "write", "diagnose:0", "revise"]);
        assert_eq!(count.get(), 1);
        let (input, b) = inputs(2);
        assert_eq!(scheme_b(&r, &maker, input, 3).await.unwrap().score, 2);
        assert_eq!(
            *b.borrow(),
            vec![
                "basis",
                "write",
                "diagnose:0",
                "stop:0",
                "choose:0",
                "reduce",
                "diagnose:1",
                "stop:1",
                "choose:1",
                "supplement",
                "diagnose:2",
                "stop:2"
            ]
        );
        assert_eq!(count.get(), 2);
    })
}
#[test]
fn s10_all_business_routes_and_no_extra_last_judge() {
    block_on(async {
        let r = Runtime::new();
        let maker = BasisMaker(Rc::new(Cell::new(0)));
        let (input, e) = inputs(4);
        assert_eq!(scheme_b(&r, &maker, input, 5).await.unwrap().score, 4);
        assert!(e.borrow().iter().any(|s| s == "rewrite"));
        let (input, e) = inputs(3);
        assert!(matches!(
            scheme_b(&r, &maker, input, 3).await,
            Err(RunError::IterationLimitReached {
                max_iterations: 3,
                ..
            })
        ));
        assert!(!e.borrow().iter().any(|s| s == "diagnose:3"));
    })
}

#[test]
fn s08_complete_nested_six_operations() {
    block_on(async {
        struct Stop;
        impl Node for Stop {
            type Input = (u32, u32);
            type Output = ();
            async fn run(&self, q: Query<&Self::Input>) -> Result<(), BodyError> {
                let (cur, _) = q.get();
                if *cur >= 2 {
                    Err(BodyError::iter_break())
                } else {
                    Ok(())
                }
            }
        }
        struct Deep(Rc<Cell<usize>>);
        impl Node for Deep {
            type Input = (u32, u32, u32);
            type Output = u32;
            async fn run(&self, q: Query<&Self::Input>) -> Result<u32, BodyError> {
                self.0.set(self.0.get() + 1);
                let (p, c, r) = q.get();
                Ok(p + c + r)
            }
        }
        struct Validate(Cell<usize>);
        impl Node for Validate {
            type Input = u32;
            type Output = ();
            async fn run(&self, q: Query<&Self::Input>) -> Result<(), BodyError> {
                let _ = q.get();
                let n = self.0.get() + 1;
                self.0.set(n);
                if n == 1 {
                    Err(BodyError::retry("first score"))
                } else {
                    Ok(())
                }
            }
        }
        async fn prepare(q: Query<(&u32, &u32)>) -> Result<u32, BodyError> {
            let (a, b) = q.get();
            Ok(a + b)
        }
        async fn route(q: Query<(&u32, &u32)>) -> Result<u32, BodyError> {
            let (p, _) = q.get();
            Ok(p % 2)
        }
        async fn score(q: Query<(&u32, &u32)>) -> Result<u32, BodyError> {
            let (p, c) = q.get();
            Ok(p + c)
        }
        async fn revise(q: Query<(&u32, &Vec<u32>, &u32)>) -> Result<u32, BodyError> {
            let (cur, scores, rules) = q.get();
            assert_eq!(scores.len(), 2);
            Ok((cur + 1).min(*rules))
        }
        let r = Runtime::new();
        let count = Rc::new(Cell::new(0));
        let deep = Deep(count.clone());
        let validate = Validate(Cell::new(0));
        let stop = Stop;
        let out = r
            .execute(
                |flow, (draft, rules, checks)| {
                    flow.chain(|pipeline| {
                        pipeline.iter(draft, 5, |current, round| {
                            round.then(&stop, (current, rules));
                            let scores = round.each(checks, |check, item_flow| {
                                let prepared =
                                    item_flow.chain(|sub| sub.then(prepare, (check, current)));
                                let route = item_flow.then(route, (prepared, rules));
                                item_flow.choose(route, |choice| {
                                    choice.case(0u32, |branch| {
                                        branch.then(score, (prepared, current))
                                    });
                                    choice.case(1u32, |branch| {
                                        branch.retry(2, |attempt| {
                                            let score =
                                                attempt.then(&deep, (prepared, current, rules));
                                            attempt.then(&validate, score);
                                            score
                                        })
                                    });
                                    choice.otherwise(|branch| {
                                        branch.then(score, (prepared, current))
                                    });
                                })
                            });
                            round.then(revise, (current, scores, rules))
                        })
                    })
                },
                (0u32, 10u32, vec![0u32, 1]),
            )
            .await
            .unwrap();
        assert_eq!(out, 2);
        assert_eq!(count.get(), 3);
        assert_eq!(validate.0.get(), 3);
    })
}
