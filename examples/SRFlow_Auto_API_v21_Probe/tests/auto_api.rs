use futures::executor::block_on;
use srflow::probe::{Flow, ProbeError, Runtime, Workflow};
use srflow::{BodyError, Data, Node, Query, Ref, RunError};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};

#[derive(Data, Debug, PartialEq)]
struct Number(u32);
#[derive(Data, Debug, PartialEq)]
struct Label(String);
async fn plus(q: Query<&Number>) -> Result<Number, BodyError> {
    Ok(Number(q.get().0 + 1))
}
async fn text(q: Query<&Number>) -> Result<Label, BodyError> {
    Ok(Label(q.get().0.to_string()))
}
async fn compare(q: Query<(&Number, &Number)>) -> Result<Label, BodyError> {
    let (a, b) = q.get();
    Ok(Label(format!("{}:{}", a.0, b.0)))
}
async fn unit(_: Query<()>) -> Result<(), BodyError> {
    Ok(())
}
fn rejected<I: srflow::RootInput, O: srflow::RootInput>(w: &Workflow<'_, I, O>, message: &str) {
    let err = w.validate().unwrap_err().to_string();
    assert!(err.contains(message), "{err}");
}

#[test]
fn p01_modular_uniform_body_and_chaining() {
    fn module(f: &mut Flow) {
        f.then(plus).chain(|s| {
            s.then(text);
            s.output::<Label>();
        });
        f.output::<Label>();
    }
    let w: Workflow<Number, Label> = Flow::build(|f, _| {
        f.chain(module).then(unit);
        f.output::<Label>();
    });
    assert_eq!(
        block_on(Runtime::new().execute(&w, Number(4))).unwrap(),
        Label("5".into())
    );
}
#[test]
fn p02_nearest_is_fixed_at_definition_position() {
    let w: Workflow<(), (Label, Label)> = Flow::build(|f, _| {
        f.provide(|| Number(10));
        let a = f.then_output(text);
        f.provide(|| Number(20));
        let b = f.then_output(text);
        f.output_with((a, b));
    });
    let (a, b) = block_on(Runtime::new().execute(&w, ())).unwrap();
    assert_eq!((a.0, b.0), ("10".into(), "20".into()));
}
#[test]
fn p02_ancestor_lookup_sibling_hidden_and_export_visible() {
    let w: Workflow<Number, (Label, Number)> = Flow::build(|f, _| {
        f.chain(|s| {
            s.provide(|| Number(99));
        });
        f.chain(|s| {
            s.chain(|deep| {
                deep.then(text);
                deep.output::<Label>();
            });
            s.output::<Label>();
        });
        f.output::<(Label, Number)>();
    });
    let (a, b) = block_on(Runtime::new().execute(&w, Number(7))).unwrap();
    assert_eq!((a.0, b.0), ("7".into(), 7));
}
#[test]
fn p02_missing_dependency_is_build_error_before_any_factory() {
    let calls = Rc::new(Cell::new(0));
    let c = calls.clone();
    let w: Workflow<(), Label> = Flow::build(|f, _| {
        f.provide(move || {
            c.set(c.get() + 1);
            7u32
        });
        f.then(text);
        f.output::<Label>();
    });
    rejected(&w, "missing visible source");
    assert!(matches!(
        block_on(Runtime::new().execute(&w, ())),
        Err(ProbeError::Definition(_))
    ));
    assert_eq!(calls.get(), 0);
}
#[test]
fn p03_ref_of_selects_without_step_and_preserves_source() {
    let calls = Rc::new(Cell::new(0));
    let c = calls.clone();
    let w: Workflow<(), Label> = Flow::build(|f, _| {
        let a = f.provide_output(move || {
            c.set(c.get() + 1);
            Number(1)
        });
        let saved = f.ref_of::<Number>();
        assert_eq!(format!("{a:?}"), format!("{saved:?}"));
        f.provide(|| Number(2));
        let b = f.ref_of::<Number>();
        let result = f.then_with_output(compare, (saved, b));
        f.output_with(result);
    });
    assert_eq!(calls.get(), 0);
    assert_eq!(block_on(Runtime::new().execute(&w, ())).unwrap().0, "1:2");
    assert_eq!(calls.get(), 1);
}
#[test]
fn p03_ref_of_does_not_export_or_import_when_unused() {
    let w: Workflow<Number, ()> = Flow::build(|f, _| {
        f.chain(|s| {
            let _ = s.ref_of::<Number>();
            s.provide(|| Label("hidden".into()));
        });
    });
    assert!(w.validate().is_ok());
    block_on(Runtime::new().execute(&w, Number(2))).unwrap();
    let hidden: Workflow<(), Label> = Flow::build(|f, _| {
        f.chain(|s| {
            s.provide(|| Label("hidden".into()));
        });
        f.output::<Label>();
    });
    rejected(&hidden, "missing visible source");
}
#[test]
fn p04_all_node_forms_same_type_explicit_and_struct_arc() {
    struct Double;
    impl Node for Double {
        type Input = Number;
        type Output = Number;
        async fn run(&self, q: Query<&Self::Input>) -> Result<Number, BodyError> {
            Ok(Number(q.get().0 * 2))
        }
    }
    let n = Double;
    let arc = Arc::new(Double);
    let w: Workflow<Number, Label> = Flow::build(|f, input| {
        f.then(&n).then(arc).then_with(plus, input);
        let a: Ref<Number> = f.then_output(plus);
        let b: Ref<Number> = f.then_with_output(&plus, input);
        let l: Ref<Label> = f.then_with_output(compare, (a, b));
        f.output_with(l);
    });
    assert_eq!(
        block_on(Runtime::new().execute(&w, Number(3))).unwrap().0,
        "5:4"
    );
}
#[test]
fn p04_captured_non_send_async_closure_and_await_borrow() {
    let adjustment = Rc::new(Cell::new(7));
    let node = async |q: Query<&Number>| -> Result<Number, BodyError> {
        let value = q.get();
        futures::future::ready(()).await;
        Ok(Number(value.0 + adjustment.get()))
    };
    let w: Workflow<Number, Number> = Flow::build(|f, _| {
        f.then(&node);
        f.output::<Number>();
    });
    assert_eq!(
        block_on(Runtime::new().execute(&w, Number(2))).unwrap().0,
        9
    );
}
#[derive(Data)]
struct Token {
    serial: usize,
    drops: Rc<RefCell<Vec<usize>>>,
}
impl Drop for Token {
    fn drop(&mut self) {
        self.drops.borrow_mut().push(self.serial);
    }
}
async fn token_serial(q: Query<&Token>) -> Result<Number, BodyError> {
    Ok(Number(q.get().serial as u32))
}
#[test]
fn p05_each_factory_creates_fresh_non_clone_values_and_builds_once() {
    let made = Rc::new(Cell::new(0));
    let drops = Rc::new(RefCell::new(vec![]));
    let builds = Cell::new(0);
    let c = made.clone();
    let d = drops.clone();
    let w: Workflow<Vec<Number>, Vec<Number>> = Flow::build(|f, items| {
        f.each(items, |s| {
            builds.set(builds.get() + 1);
            s.provide(move || {
                let serial = c.get();
                c.set(serial + 1);
                Token {
                    serial,
                    drops: d.clone(),
                }
            })
            .then(token_serial);
            s.output::<Number>();
        });
        f.output::<Vec<Number>>();
    });
    assert_eq!((builds.get(), made.get()), (1, 0));
    let rt = Runtime::new();
    let first = block_on(rt.execute(&w, vec![Number(4), Number(5)])).unwrap();
    let second = block_on(rt.execute(&w, vec![Number(6)])).unwrap();
    assert_eq!(
        (first, second),
        (vec![Number(0), Number(1)], vec![Number(2)])
    );
    assert_eq!((builds.get(), made.get()), (1, 3));
    assert_eq!(*drops.borrow(), vec![0, 1, 2]);
}
#[test]
fn p05_retry_factory_repeats_and_cleans_every_attempt() {
    let made = Rc::new(Cell::new(0));
    let drops = Rc::new(RefCell::new(vec![]));
    let c = made.clone();
    let d = drops.clone();
    let builds = Cell::new(0);
    let check = async |q: Query<&Token>| -> Result<Number, BodyError> {
        let t = q.get();
        if t.serial < 2 {
            Err(BodyError::retry(std::io::Error::other("again")))
        } else {
            Ok(Number(t.serial as u32))
        }
    };
    let w: Workflow<(), Number> = Flow::build(|f, _| {
        f.retry(2, |s| {
            builds.set(builds.get() + 1);
            s.provide(move || {
                let serial = c.get();
                c.set(serial + 1);
                Token {
                    serial,
                    drops: d.clone(),
                }
            })
            .then(check);
            s.output::<Number>();
        });
        f.output::<Number>();
    });
    assert_eq!(block_on(Runtime::new().execute(&w, ())).unwrap(), Number(2));
    assert_eq!(builds.get(), 1);
    assert_eq!(*drops.borrow(), vec![0, 1, 2]);
}
#[test]
fn p06_unit_and_absence_remain_distinct_contracts() {
    let absent: Workflow<(), ()> = Flow::build(|f, _| {
        f.chain(|_| {});
    });
    let explicit: Workflow<(), ()> = Flow::build(|f, _| {
        f.chain(|s| {
            s.output::<()>();
        });
        f.output::<()>();
    });
    assert!(!absent.contracts()[0].output_declared);
    assert!(!absent.contracts()[1].output_declared);
    assert!(explicit.contracts()[0].output_declared);
    assert!(explicit.contracts()[1].output_declared);
    block_on(Runtime::new().execute(&absent, ())).unwrap();
    block_on(Runtime::new().execute(&explicit, ())).unwrap();
}
#[test]
fn p06_duplicate_outputs_missing_root_and_shape_mismatch_rejected() {
    let duplicate: Workflow<Number, Number> = Flow::build(|f, _| {
        f.output::<Number>();
        f.output::<Number>();
    });
    rejected(&duplicate, "duplicate output");
    let duplicate_unit: Workflow<(), ()> = Flow::build(|f, _| {
        f.output::<()>();
        f.output::<()>();
    });
    rejected(&duplicate_unit, "duplicate output");
    let missing: Workflow<Number, Number> = Flow::build(|_, _| {});
    rejected(&missing, "missing Root output");
    let mismatch: Workflow<Number, Label> = Flow::build(|f, _| {
        f.output::<Number>();
    });
    rejected(&mismatch, "Root output shape");
    let wrong: Workflow<Number, ()> = Flow::build(|f, _| {
        let _: Ref<Label> = f.chain_output(|s| {
            s.output::<Number>();
        });
    });
    rejected(&wrong, "requested RefShape");
}
#[test]
fn p06_tuple_output_and_each_group_are_separate_slots() {
    let w: Workflow<Vec<Number>, (Vec<Number>, Vec<Label>)> = Flow::build(|f, items| {
        let (ns, ls): (Ref<Vec<Number>>, Ref<Vec<Label>>) = f.each_output(items, |s| {
            s.then(plus).then(text);
            s.output::<(Number, Label)>();
        });
        f.output_with((ns, ls));
    });
    let (ns, ls) = block_on(Runtime::new().execute(&w, vec![Number(3), Number(8)])).unwrap();
    assert_eq!(ns, vec![Number(4), Number(9)]);
    assert_eq!(ls, vec![Label("4".into()), Label("9".into())]);
}
#[derive(Data, PartialEq)]
enum Route {
    A,
    B,
}
#[test]
fn p07_choose_automatic_selector_and_only_selected_factory_runs() {
    let calls = Rc::new(Cell::new(0));
    let c = calls.clone();
    let builds = Cell::new(0);
    let w: Workflow<Route, Number> = Flow::build(|f, _| {
        f.choose(|choice| {
            choice.case(Route::A, |s| {
                builds.set(builds.get() + 1);
                s.provide(move || {
                    c.set(c.get() + 1);
                    Number(1)
                });
                s.output::<Number>();
            });
            choice.case(Route::B, |s| {
                builds.set(builds.get() + 1);
                s.provide(|| Number(2));
                s.output::<Number>();
            });
        });
        f.output::<Number>();
    });
    assert_eq!(builds.get(), 2);
    assert_eq!(
        block_on(Runtime::new().execute(&w, Route::B)).unwrap(),
        Number(2)
    );
    assert_eq!(calls.get(), 0);
    assert_eq!(
        block_on(Runtime::new().execute(&w, Route::A)).unwrap(),
        Number(1)
    );
    assert_eq!(calls.get(), 1);
}
#[test]
fn p07_choose_explicit_selector_otherwise_and_no_match() {
    let w: Workflow<Route, Number> = Flow::build(|f, r| {
        f.provide(|| Route::B);
        let n: Ref<Number> = f.choose_with_output(r, |c| {
            c.case(Route::A, |s| {
                s.provide(|| Number(1));
                s.output::<Number>();
            });
            c.otherwise(|s| {
                s.provide(|| Number(9));
                s.output::<Number>();
            });
        });
        f.output_with(n);
    });
    assert_eq!(
        block_on(Runtime::new().execute(&w, Route::A)).unwrap(),
        Number(1)
    );
    let no_match: Workflow<Route, ()> = Flow::build(|f, _| {
        f.choose(|c| c.case(Route::A, |_| {}));
    });
    assert!(matches!(
        block_on(Runtime::new().execute(&no_match, Route::B)),
        Err(ProbeError::Execution(RunError::NoMatchingCase))
    ));
}
#[test]
fn p07_choose_mismatch_and_hidden_sibling_rejected_before_run() {
    let bad: Workflow<Route, ()> = Flow::build(|f, _| {
        f.choose(|c| {
            c.case(Route::A, |s| {
                s.provide(|| Number(1));
                s.output::<Number>();
            });
            c.case(Route::B, |s| {
                s.output::<()>();
            });
        });
    });
    rejected(&bad, "Choose branch output shape");
    let sibling: Workflow<Route, ()> = Flow::build(|f, _| {
        f.choose(|c| {
            c.case(Route::A, |s| {
                s.provide(|| Number(1));
            });
            c.case(Route::B, |s| {
                s.then(plus);
            });
        });
    });
    rejected(&sibling, "missing visible source");
}
async fn stop(q: Query<&Number>) -> Result<(), BodyError> {
    if q.get().0 >= 3 {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}
#[test]
fn p07_iter_ref_of_current_next_and_break_keep_current() {
    let builds = Cell::new(0);
    let w: Workflow<Number, Number> = Flow::build(|f, initial| {
        let final_state: Ref<Number> = f.iter_output(initial, 5, |round| {
            builds.set(builds.get() + 1);
            let current = round.ref_of::<Number>();
            round.then(plus); // Uncommitted Next exists before break; judge saved Current.
            round.then_with(stop, current);
            round.next::<Number>();
        });
        f.output_with(final_state);
    });
    assert_eq!(
        block_on(Runtime::new().execute(&w, Number(0))).unwrap(),
        Number(3)
    );
    assert_eq!(builds.get(), 1);
    assert!(w.contracts()[1].next_declared);
    assert!(!w.contracts()[1].output_declared);
}
#[test]
fn p07_iter_explicit_next_multi_state_and_limit() {
    async fn gate(q: Query<&Number>) -> Result<(), BodyError> {
        if q.get().0 >= 2 {
            Err(BodyError::iter_break())
        } else {
            Ok(())
        }
    }
    let w: Workflow<(Number, Label), (Number, Label)> = Flow::build(|f, initial| {
        f.iter(initial, 5, |s| {
            let current = s.ref_of::<Number>();
            s.then_with(gate, current);
            let n = s.then_output(plus);
            let label = s.then_output(text);
            s.next_with((n, label));
        });
        f.output::<(Number, Label)>();
    });
    let (n, l) =
        block_on(Runtime::new().execute(&w, (Number(0), Label("initial".into())))).unwrap();
    assert_eq!((n.0, l.0), (2, "2".into()));
    let limited: Workflow<Number, Number> = Flow::build(|f, i| {
        f.iter(i, 1, |s| {
            s.then(plus);
            s.next::<Number>();
        });
        f.output::<Number>();
    });
    assert!(matches!(
        block_on(Runtime::new().execute(&limited, Number(0))),
        Err(ProbeError::Execution(
            RunError::IterationLimitReached { .. }
        ))
    ));
}
#[test]
fn p07_iter_invalid_contracts_are_build_errors() {
    let absent: Workflow<Number, ()> = Flow::build(|f, i| {
        f.iter(i, 2, |_| {});
    });
    rejected(&absent, "missing next");
    let duplicate: Workflow<Number, ()> = Flow::build(|f, i| {
        f.iter(i, 2, |s| {
            s.next::<Number>();
            s.next::<Number>();
        });
    });
    rejected(&duplicate, "duplicate next");
    let output: Workflow<Number, ()> = Flow::build(|f, i| {
        f.iter(i, 2, |s| {
            s.output::<Number>();
            s.next::<Number>();
        });
    });
    rejected(&output, "uses next");
    let wrong: Workflow<Number, ()> = Flow::build(|f, i| {
        f.iter(i, 2, |s| {
            s.provide(|| Label("wrong".into()));
            s.next::<Label>();
        });
    });
    rejected(&wrong, "next shape");
    let outside: Workflow<Number, ()> = Flow::build(|f, _| {
        f.next::<Number>();
    });
    rejected(&outside, "next outside");
}
#[test]
fn p08_root_inputs_owned_output_and_reuse_no_rebuild() {
    let builds = Cell::new(0);
    let w: Workflow<Number, Number> = Flow::build(|f, i| {
        builds.set(builds.get() + 1);
        f.then_with(plus, i);
        f.output::<Number>();
    });
    let rt = Runtime::new();
    assert_eq!(block_on(rt.execute(&w, Number(5))).unwrap().0, 6);
    assert_eq!(block_on(rt.execute(&w, Number(9))).unwrap().0, 10);
    assert_eq!(builds.get(), 1);
}
#[test]
fn p10_explicit_foreign_child_local_and_each_alias_rejected() {
    let mut foreign = None;
    let _: Workflow<Number, ()> = Flow::build(|_, i| foreign = Some(i));
    let w: Workflow<(), Label> = Flow::build(|f, _| {
        f.then_with(text, foreign.unwrap());
        f.output::<Label>();
    });
    rejected(&w, "ForeignDefinition");
    let mut child = None;
    let w: Workflow<(), Number> = Flow::build(|f, _| {
        f.chain(|s| child = Some(s.provide_output(|| Number(1))));
        f.output_with(child.unwrap());
    });
    rejected(&w, "ChildLocal");
    let alias: Workflow<(Vec<Number>, Number), Vec<Number>> = Flow::build(|f, (items, n)| {
        f.each(items, |s| {
            s.output_with(n);
        });
        f.output::<Vec<Number>>();
    });
    assert!(matches!(
        block_on(Runtime::new().execute(&alias, (vec![Number(1)], Number(9)))),
        Err(ProbeError::Execution(_))
    ));
}
#[test]
fn p10_duplicate_root_alias_still_refuses_owned_take() {
    let w: Workflow<Number, (Number, Number)> = Flow::build(|f, n| {
        let alias: Ref<Number> = f.chain_output(|s| {
            s.output_with(n);
        });
        f.output_with((n, alias));
    });
    assert!(w.validate().is_ok());
    assert!(matches!(
        block_on(Runtime::new().execute(&w, Number(8))),
        Err(ProbeError::Execution(_))
    ));
}
#[test]
fn p10_empty_each_validates_definition_without_running_factory() {
    let calls = Rc::new(Cell::new(0));
    let c = calls.clone();
    let w: Workflow<Vec<Number>, Vec<Number>> = Flow::build(|f, items| {
        f.each(items, |s| {
            s.provide(move || {
                c.set(c.get() + 1);
                Number(8)
            });
            s.output::<Number>();
        });
        f.output::<Vec<Number>>();
    });
    assert!(
        block_on(Runtime::new().execute(&w, vec![]))
            .unwrap()
            .is_empty()
    );
    assert_eq!(calls.get(), 0);
}
#[test]
fn p10_failure_stops_steps_cleans_scope_and_no_implicit_retry() {
    let made = Rc::new(Cell::new(0));
    let drops = Rc::new(RefCell::new(vec![]));
    let c = made.clone();
    let d = drops.clone();
    let after = Rc::new(Cell::new(0));
    let a = after.clone();
    async fn fail(_: Query<&Token>) -> Result<(), BodyError> {
        Err(BodyError::fail(std::io::Error::other("failure")))
    }
    let w: Workflow<(), ()> = Flow::build(|f, _| {
        f.retry(3, |s| {
            s.provide(move || {
                c.set(c.get() + 1);
                Token {
                    serial: 0,
                    drops: d.clone(),
                }
            })
            .then(fail)
            .provide(move || {
                a.set(a.get() + 1);
                Number(1)
            });
        });
    });
    assert!(matches!(
        block_on(Runtime::new().execute(&w, ())),
        Err(ProbeError::Execution(RunError::Body(_)))
    ));
    assert_eq!(made.get(), 1);
    assert_eq!(after.get(), 0);
    assert_eq!(*drops.borrow(), vec![0]);
}
#[test]
fn p10_cancellation_drops_pending_future_before_scope_data() {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };
    struct BorrowGuard<'a>(&'a Token);
    impl Drop for BorrowGuard<'_> {
        fn drop(&mut self) {
            self.0.drops.borrow_mut().push(100);
        }
    }
    async fn pending(q: Query<&Token>) -> Result<(), BodyError> {
        let guard = BorrowGuard(q.get());
        futures::future::pending::<()>().await;
        drop(guard);
        Ok(())
    }
    let drops = Rc::new(RefCell::new(vec![]));
    let d = drops.clone();
    let w: Workflow<(), ()> = Flow::build(|f, _| {
        f.chain(|s| {
            s.provide(move || Token {
                serial: 1,
                drops: d.clone(),
            })
            .then(pending);
        });
    });
    let rt = Runtime::new();
    let mut future = Box::pin(rt.execute(&w, ()));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(Pin::new(&mut future).poll(&mut cx), Poll::Pending));
    assert!(drops.borrow().is_empty());
    drop(future);
    assert_eq!(*drops.borrow(), vec![100, 1]);
}
#[test]
fn p03_item_ref_and_current_shadowing_are_explicitly_observable() {
    let w: Workflow<Vec<Number>, Vec<Label>> = Flow::build(|f, items| {
        f.each(items, |s| {
            let item = s.ref_of::<Number>();
            s.then(plus);
            let latest = s.ref_of::<Number>();
            s.then_with(compare, (item, latest));
            s.output::<Label>();
        });
        f.output::<Vec<Label>>();
    });
    assert_eq!(
        block_on(Runtime::new().execute(&w, vec![Number(4), Number(8)])).unwrap(),
        vec![Label("4:5".into()), Label("8:9".into())]
    );
}
#[test]
fn p06_output_selection_does_not_follow_later_shadowing() {
    let w: Workflow<Number, Number> = Flow::build(|f, _| {
        f.output::<Number>();
        f.provide(|| Number(999));
    });
    assert_eq!(
        block_on(Runtime::new().execute(&w, Number(4))).unwrap(),
        Number(4)
    );
}
#[test]
fn p07_nested_chain_choose_retry_each_iter_and_nearest_control() {
    let w: Workflow<Vec<Number>, Vec<Number>> = Flow::build(|f, items| {
        f.each(items, |s| {
            s.retry(0, |attempt| {
                attempt.chain(|inner| {
                    inner.provide(|| Route::A).choose(|c| {
                        c.case(Route::A, |branch| {
                            let item = branch.ref_of::<Number>();
                            branch.iter(item, 8, |r| {
                                r.then(stop).then(plus);
                                r.next::<Number>();
                            });
                            branch.output::<Number>();
                        });
                        c.otherwise(|branch| {
                            branch.output::<Number>();
                        });
                    });
                    inner.output::<Number>();
                });
                attempt.output::<Number>();
            });
            s.output::<Number>();
        });
        f.output::<Vec<Number>>();
    });
    assert_eq!(
        block_on(Runtime::new().execute(&w, vec![Number(0), Number(2)])).unwrap(),
        vec![Number(3), Number(3)]
    );
}
#[test]
fn p07_retry_exhaustion_budget_and_unhandled_control_unchanged() {
    async fn again(_: Query<()>) -> Result<(), BodyError> {
        Err(BodyError::retry(std::io::Error::other("again")))
    }
    let w: Workflow<(), ()> = Flow::build(|f, _| {
        f.retry(1, |s| {
            s.then(again);
        });
    });
    assert!(matches!(
        block_on(Runtime::new().execute(&w, ())),
        Err(ProbeError::Execution(RunError::RetryExhausted {
            max_retries: 1,
            attempts: 2,
            ..
        }))
    ));
    let unhandled: Workflow<(), ()> = Flow::build(|f, _| {
        f.then(again);
    });
    assert!(matches!(
        block_on(Runtime::new().execute(&unhandled, ())),
        Err(ProbeError::Execution(RunError::UnhandledControl(_)))
    ));
    let zero: Workflow<Number, ()> = Flow::build(|f, i| {
        f.iter(i, 0, |s| {
            s.next::<Number>();
        });
    });
    rejected(&zero, "IterZeroRounds");
    let overflow: Workflow<(), ()> = Flow::build(|f, _| {
        f.retry(usize::MAX, |_| {});
    });
    rejected(&overflow, "RetryBudgetUnrepresentable");
}
#[derive(Data)]
struct Slot0(u32);
#[derive(Data)]
struct Slot1(u32);
#[derive(Data)]
struct Slot2(u32);
#[derive(Data)]
struct Slot3(u32);
#[derive(Data)]
struct Slot4(u32);
#[derive(Data)]
struct Slot5(u32);
#[derive(Data)]
struct Slot6(u32);
#[derive(Data)]
struct Slot7(u32);
#[derive(Data)]
struct Slot8(u32);
#[derive(Data)]
struct Slot9(u32);
#[derive(Data)]
struct Slot10(u32);
#[derive(Data)]
struct Slot11(u32);
#[derive(Data)]
struct Slot12(u32);
#[derive(Data)]
struct Slot13(u32);
#[derive(Data)]
struct Slot14(u32);
#[derive(Data)]
struct Slot15(u32);
#[test]
#[allow(clippy::type_complexity)] // Exercise the actual 16-slot public boundary.
fn p09_sixteen_input_inference_and_output_shape() {
    async fn sum(
        q: Query<(
            &Slot0,
            &Slot1,
            &Slot2,
            &Slot3,
            &Slot4,
            &Slot5,
            &Slot6,
            &Slot7,
            &Slot8,
            &Slot9,
            &Slot10,
            &Slot11,
            &Slot12,
            &Slot13,
            &Slot14,
            &Slot15,
        )>,
    ) -> Result<Number, BodyError> {
        let (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15) = q.get();
        Ok(Number(
            a0.0 + a1.0
                + a2.0
                + a3.0
                + a4.0
                + a5.0
                + a6.0
                + a7.0
                + a8.0
                + a9.0
                + a10.0
                + a11.0
                + a12.0
                + a13.0
                + a14.0
                + a15.0,
        ))
    }
    let w: Workflow<
        (
            Slot0,
            Slot1,
            Slot2,
            Slot3,
            Slot4,
            Slot5,
            Slot6,
            Slot7,
            Slot8,
            Slot9,
            Slot10,
            Slot11,
            Slot12,
            Slot13,
            Slot14,
            Slot15,
        ),
        (
            Slot0,
            Slot1,
            Slot2,
            Slot3,
            Slot4,
            Slot5,
            Slot6,
            Slot7,
            Slot8,
            Slot9,
            Slot10,
            Slot11,
            Slot12,
            Slot13,
            Slot14,
            Slot15,
        ),
    > = Flow::build(|f, _| {
        f.then(sum);
        f.output::<(
            Slot0,
            Slot1,
            Slot2,
            Slot3,
            Slot4,
            Slot5,
            Slot6,
            Slot7,
            Slot8,
            Slot9,
            Slot10,
            Slot11,
            Slot12,
            Slot13,
            Slot14,
            Slot15,
        )>();
    });
    let (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15) =
        block_on(Runtime::new().execute(
            &w,
            (
                Slot0(0),
                Slot1(1),
                Slot2(2),
                Slot3(3),
                Slot4(4),
                Slot5(5),
                Slot6(6),
                Slot7(7),
                Slot8(8),
                Slot9(9),
                Slot10(10),
                Slot11(11),
                Slot12(12),
                Slot13(13),
                Slot14(14),
                Slot15(15),
            ),
        ))
        .unwrap();
    assert_eq!(
        a0.0 + a1.0
            + a2.0
            + a3.0
            + a4.0
            + a5.0
            + a6.0
            + a7.0
            + a8.0
            + a9.0
            + a10.0
            + a11.0
            + a12.0
            + a13.0
            + a14.0
            + a15.0,
        120
    );
}

#[test]
fn p04_control_output_methods_return_the_registered_parent_ref() {
    let w: Workflow<Route, Label> = Flow::build(|f, _| {
        let chosen: Ref<Number> = f.choose_output(|c| {
            c.case(Route::A, |s| {
                s.provide(|| Number(5));
                s.output::<Number>();
            });
            c.otherwise(|s| {
                s.provide(|| Number(8));
                s.output::<Number>();
            });
        });
        let retried: Ref<Label> = f.retry_output(0, |s| {
            s.then_with(text, chosen);
            s.output::<Label>();
        });
        f.output_with(retried);
    });
    assert_eq!(
        block_on(Runtime::new().execute(&w, Route::A)).unwrap(),
        Label("5".into())
    );
}
#[test]
fn p06_same_type_auto_output_does_not_invent_distinct_roles() {
    let w: Workflow<(Number, Number), (Number, Number)> = Flow::build(|f, _| {
        f.output::<(Number, Number)>();
    });
    rejected(&w, "DuplicateRootOutput");
    let explicit: Workflow<(Number, Number), Label> = Flow::build(|f, (a, b)| {
        f.then_with(compare, (a, b));
        f.output::<Label>();
    });
    assert_eq!(
        block_on(Runtime::new().execute(&explicit, (Number(1), Number(2)))).unwrap(),
        Label("1:2".into())
    );
}
