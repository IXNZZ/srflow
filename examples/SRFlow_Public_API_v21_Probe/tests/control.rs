use futures::executor::block_on;
use srflow_public_api_v21_probe::*;
use std::{
    cell::{Cell, RefCell},
    error::Error,
    rc::Rc,
};
async fn inc(q: Query<&u32>) -> Result<u32, BodyError> {
    Ok(q.get() + 1)
}
async fn same(q: Query<&u32>) -> Result<u32, BodyError> {
    Ok(*q.get())
}
async fn retry(q: Query<&u32>) -> Result<(), BodyError> {
    let _ = q.get();
    Err(BodyError::retry("rejected"))
}
async fn stop(q: Query<&u32>) -> Result<(), BodyError> {
    let _ = q.get();
    Err(BodyError::iter_break())
}
async fn fail(q: Query<&u32>) -> Result<u32, BodyError> {
    let _ = q.get();
    Err(BodyError::fail(std::io::Error::other("body failure")))
}
struct Judge {
    threshold: u32,
}

#[test]
fn ap07_exhaustion_preserves_dynamic_retry_source() {
    block_on(async {
        let reject = async |q: Query<&u32>| -> Result<(), BodyError> {
            let n = q.get();
            Err(BodyError::Control(ControlSignal::Retry(
                RetryError::caused_by(
                    format!("rejected {n}"),
                    std::io::Error::other("retry cause"),
                ),
            )))
        };
        let r = Runtime::new();
        let e = r
            .execute(
                |f, a| {
                    f.retry(1, |s| {
                        s.then(reject, a);
                        a
                    })
                },
                3u32,
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::RetryExhausted { attempts: 2, .. }));
        let source = e.source().unwrap().source().unwrap();
        assert_eq!(
            source.downcast_ref::<std::io::Error>().unwrap().to_string(),
            "retry cause"
        );
    })
}

impl Node for Judge {
    type Input = u32;
    type Output = ();
    async fn run(&self, q: Query<&Self::Input>) -> Result<(), BodyError> {
        if *q.get() >= self.threshold {
            Err(BodyError::iter_break())
        } else {
            Ok(())
        }
    }
}
struct RetryGate {
    calls: Rc<Cell<usize>>,
    reject: usize,
    events: Rc<RefCell<Vec<u32>>>,
}
impl Node for RetryGate {
    type Input = u32;
    type Output = ();
    async fn run(&self, q: Query<&Self::Input>) -> Result<(), BodyError> {
        self.events.borrow_mut().push(*q.get());
        let n = self.calls.get() + 1;
        self.calls.set(n);
        if n <= self.reject {
            Err(BodyError::retry(format!("attempt {n}")))
        } else {
            Ok(())
        }
    }
}
fn gate(reject: usize) -> RetryGate {
    RetryGate {
        calls: Rc::new(Cell::new(0)),
        reject,
        events: Rc::new(RefCell::new(vec![])),
    }
}

#[test]
fn ap07_budget_overflow_and_empty_choose_rejected() {
    block_on(async {
        let r = Runtime::new();
        let g = gate(0);
        let e = r
            .execute(
                |f, a| {
                    f.retry(usize::MAX, |s| {
                        s.then(&g, a);
                        a
                    })
                },
                1u32,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            e,
            RunError::Definition(BuildError::RetryLimitOverflow)
        ));
        assert_eq!(g.calls.get(), 0);
        let e = r
            .execute(
                |f, route| {
                    let _: () = f.choose(route, |_| {});
                },
                1u32,
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Definition(BuildError::EmptyChoose)));
    })
}
#[test]
fn ap07_retry_success_and_original_inputs() {
    block_on(async {
        let r = Runtime::new();
        let gate = gate(2);
        let out = r
            .execute(
                |f, a| {
                    f.retry(3, |s| {
                        let candidate = s.then(inc, a);
                        s.then(&gate, candidate);
                        candidate
                    })
                },
                10u32,
            )
            .await
            .unwrap();
        assert_eq!(out, 11);
        assert_eq!(gate.calls.get(), 3);
        assert_eq!(*gate.events.borrow(), vec![11, 11, 11]);
    })
}
#[test]
fn ap07_retry_zero_and_exhaustion() {
    block_on(async {
        let r = Runtime::new();
        let first = gate(0);
        assert_eq!(
            r.execute(
                |f, a| f.retry(0, |s| {
                    s.then(&first, a);
                    a
                }),
                3u32
            )
            .await
            .unwrap(),
            3
        );
        assert_eq!(first.calls.get(), 1);
        for budget in [0, 2] {
            let g = gate(99);
            let e = r
                .execute(
                    |f, a| {
                        f.retry(budget, |s| {
                            s.then(&g, a);
                            a
                        })
                    },
                    3u32,
                )
                .await
                .unwrap_err();
            match e {
                RunError::RetryExhausted {
                    max_retries,
                    attempts,
                    last_error,
                } => {
                    assert_eq!(max_retries, budget);
                    assert_eq!(attempts, budget + 1);
                    assert_eq!(last_error.message, format!("attempt {}", budget + 1));
                }
                e => panic!("{e:?}"),
            };
            assert_eq!(g.calls.get(), budget + 1);
        }
    })
}
#[test]
fn ap07_retry_multi_and_unit_outputs() {
    block_on(async {
        let r = Runtime::new();
        let g = gate(1);
        let out = r
            .execute(
                |f, a| {
                    f.retry(2, |s| {
                        let b = s.then(inc, a);
                        let c = s.then(inc, b);
                        s.then(&g, c);
                        (b, c)
                    })
                },
                2u32,
            )
            .await
            .unwrap();
        assert_eq!(out, (3, 4));
        let g = gate(1);
        r.execute(
            |f, a| {
                f.retry(2, |s| {
                    s.then(&g, a);
                });
            },
            2u32,
        )
        .await
        .unwrap();
        assert_eq!(g.calls.get(), 2);
    })
}
#[test]
fn ap07_iteration_break_current_first_and_later() {
    block_on(async {
        let r = Runtime::new();
        for (initial, threshold, expected) in [(7, 5, 7), (0, 2, 2)] {
            let j = Judge { threshold };
            let out = r
                .execute(
                    |f, a| {
                        f.iter(a, 5, |cur, s| {
                            s.then(&j, cur);
                            s.then(inc, cur)
                        })
                    },
                    initial,
                )
                .await
                .unwrap();
            assert_eq!(out, expected);
        }
    })
}
#[test]
fn ap07_iteration_limit_and_zero() {
    block_on(async {
        let r = Runtime::new();
        let e = r
            .execute(|f, a| f.iter(a, 3, |cur, s| s.then(inc, cur)), 0u32)
            .await
            .unwrap_err();
        assert!(matches!(
            e,
            RunError::IterationLimitReached {
                max_iterations: 3,
                completed_iterations: 3
            }
        ));
        let e = r
            .execute(|f, a| f.iter(a, 0, |cur, s| s.then(inc, cur)), 0u32)
            .await
            .unwrap_err();
        assert!(matches!(
            e,
            RunError::Definition(BuildError::InvalidIterationLimit)
        ));
    })
}
#[test]
fn ap07_same_target_state_and_original_alias() {
    block_on(async {
        let r = Runtime::new();
        let g = gate(0);
        // First check normal same-target Promote, then issue Break via a call-count adapter.
        struct StopSecond(Cell<usize>);
        impl Node for StopSecond {
            type Input = u32;
            type Output = ();
            async fn run(&self, q: Query<&Self::Input>) -> Result<(), BodyError> {
                let _ = q.get();
                let n = self.0.get() + 1;
                self.0.set(n);
                if n == 2 {
                    Err(BodyError::iter_break())
                } else {
                    Ok(())
                }
            }
        }
        let j = StopSecond(Cell::new(0));
        let out = r
            .execute(
                |f, a| {
                    let final_ = f.iter(a, 3, |cur, s| {
                        s.then(&j, cur);
                        cur
                    });
                    f.then(&g, a);
                    f.then(same, final_)
                },
                4u32,
            )
            .await
            .unwrap();
        assert_eq!(out, 4);
        assert_eq!(j.0.get(), 2);
    })
}
#[test]
fn ap08_inner_retry_exhaustion_not_retried_by_outer() {
    block_on(async {
        let r = Runtime::new();
        let g = gate(99);
        let e = r
            .execute(
                |f, a| {
                    f.retry(4, |outer| {
                        outer.retry(2, |inner| {
                            inner.then(&g, a);
                            a
                        })
                    })
                },
                0u32,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            e,
            RunError::RetryExhausted {
                max_retries: 2,
                attempts: 3,
                ..
            }
        ));
        assert_eq!(g.calls.get(), 3);
    })
}
#[test]
fn ap08_inner_iter_break_does_not_break_outer() {
    block_on(async {
        let r = Runtime::new();
        let e = r
            .execute(
                |f, a| {
                    f.iter(a, 2, |cur, outer| {
                        outer.iter(cur, 2, |inner_cur, inner| {
                            inner.then(stop, inner_cur);
                            inner_cur
                        })
                    })
                },
                5u32,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            e,
            RunError::IterationLimitReached {
                max_iterations: 2,
                ..
            }
        ));
    })
}
#[test]
fn ap08_retry_crosses_chain_and_iter_restarts() {
    block_on(async {
        let r = Runtime::new();
        let g = gate(1);
        let j = Judge { threshold: 2 };
        let out = r
            .execute(
                |f, a| {
                    f.retry(2, |attempt| {
                        attempt.iter(a, 5, |cur, round| {
                            round.chain(|sub| {
                                sub.then(&g, cur);
                                sub.then(&j, cur);
                            });
                            round.then(inc, cur)
                        })
                    })
                },
                0u32,
            )
            .await
            .unwrap();
        assert_eq!(out, 2);
        assert_eq!(*g.events.borrow(), vec![0, 0, 1, 2]);
    })
}
#[test]
fn ap08_iter_break_crosses_retry() {
    block_on(async {
        let r = Runtime::new();
        let out = r
            .execute(
                |f, a| {
                    f.iter(a, 3, |cur, round| {
                        round.retry(2, |attempt| {
                            attempt.then(stop, cur);
                            attempt.then(inc, cur)
                        })
                    })
                },
                4u32,
            )
            .await
            .unwrap();
        assert_eq!(out, 4);
    })
}
#[test]
fn ap08_break_crosses_each_choose_and_chain() {
    block_on(async {
        let r = Runtime::new();
        let events = Rc::new(RefCell::new(vec![]));
        async fn judge_item(q: Query<(&u32, &Rc<RefCell<Vec<u32>>>)>) -> Result<(), BodyError> {
            let (item, events) = q.get();
            events.borrow_mut().push(*item);
            if *item == 2 {
                Err(BodyError::iter_break())
            } else {
                Ok(())
            }
        }
        let out = r
            .execute(
                |f, (state, items, route, events)| {
                    f.iter(state, 3, |cur, round| {
                        round.each(items, |item, s| {
                            s.choose(route, |c| {
                                c.case(0u32, |b| {
                                    b.chain(|sub| {
                                        sub.then(judge_item, (item, events));
                                    });
                                });
                            });
                        });
                        round.then(inc, cur)
                    })
                },
                (7u32, vec![1u32, 2, 3], 0u32, events.clone()),
            )
            .await
            .unwrap();
        assert_eq!(out, 7);
        assert_eq!(*events.borrow(), vec![1, 2]);
    })
}
#[test]
fn ap08_unhandled_controls_are_distinct() {
    block_on(async {
        let r = Runtime::new();
        assert!(matches!(
            r.execute(
                |f, a| {
                    f.then(retry, a);
                },
                0u32
            )
            .await,
            Err(RunError::UnhandledControl(ControlSignal::Retry(_)))
        ));
        assert!(matches!(
            r.execute(
                |f, a| {
                    f.then(stop, a);
                },
                0u32
            )
            .await,
            Err(RunError::UnhandledControl(ControlSignal::IterBreak(_)))
        ));
    })
}
#[test]
fn ap08_failure_not_retried_and_source_retained() {
    block_on(async {
        let r = Runtime::new();
        let g = gate(0);
        let e = r
            .execute(
                |f, a| {
                    f.retry(4, |s| {
                        s.then(&g, a);
                        s.then(fail, a)
                    })
                },
                0u32,
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Body(_)));
        assert_eq!(g.calls.get(), 1);
        let source = e.source().unwrap().source().unwrap();
        assert_eq!(
            source.downcast_ref::<std::io::Error>().unwrap().to_string(),
            "body failure"
        );
    })
}
#[test]
fn ap06_failed_branch_never_runs_otherwise() {
    block_on(async {
        let r = Runtime::new();
        let g = gate(0);
        let e = r
            .execute(
                |f, (route, a)| {
                    f.choose(route, |c| {
                        c.case(0, |s| s.then(fail, a));
                        c.otherwise(|s| {
                            s.then(&g, a);
                            s.then(inc, a)
                        });
                    })
                },
                (0u32, 1u32),
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Body(_)));
        assert_eq!(g.calls.get(), 0);
    })
}
