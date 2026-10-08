use futures::executor::block_on;
use srflow_public_api_v21_probe::*;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};

async fn inc(q: Query<&u32>) -> Result<u32, BodyError> {
    Ok(q.get() + 1)
}

#[test]
fn ap03_repeated_root_ref_rejected_before_steps() {
    block_on(async {
        let r = Runtime::new();
        let events = Rc::new(RefCell::new(vec![]));
        let e = r
            .execute(
                |f, (a, events)| {
                    f.then(record, (a, events));
                    (a, a)
                },
                (1u32, events.clone()),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            e,
            RunError::Definition(BuildError::DuplicateRootRef)
        ));
        assert!(events.borrow().is_empty());
    })
}
async fn sum(q: Query<(&u32, &u32)>) -> Result<u32, BodyError> {
    let (a, b) = q.get();
    Ok(a + b)
}
async fn report(q: Query<(&u32, &u32)>) -> Result<String, BodyError> {
    let (a, b) = q.get();
    Ok(format!("{a}:{b}"))
}
async fn record(q: Query<(&u32, &Rc<RefCell<Vec<u32>>>)>) -> Result<(), BodyError> {
    let (a, events) = q.get();
    events.borrow_mut().push(*a);
    Ok(())
}
struct Add {
    delta: u32,
}
impl Node for Add {
    type Input = u32;
    type Output = u32;
    async fn run(&self, q: Query<&Self::Input>) -> Result<u32, BodyError> {
        Ok(q.get() + self.delta)
    }
}
fn fragment<'n>(node: &'n Add, input: Ref<u32>) -> impl FnOnce(&mut Flow<'n>) -> Ref<u32> + 'n {
    move |f| {
        let a = f.then(node, input);
        f.then(inc, a)
    }
}
fn context_fragment<'n>(f: &mut Flow<'n>, node: &'n Add, input: Ref<u32>) -> Ref<u32> {
    let a = f.then(node, input);
    f.then(inc, a)
}

#[test]
fn ap04_nested_capture_and_outputs() {
    block_on(async {
        let r = Runtime::new();
        let out = r
            .execute(
                |f, (a, b)| {
                    f.chain(|s| {
                        s.chain(|inner| {
                            let c = inner.then(sum, (a, b));
                            let text = inner.then(report, (a, c));
                            (c, text)
                        })
                    })
                },
                (2u32, 3u32),
            )
            .await
            .unwrap();
        assert_eq!(out, (5, "2:5".into()));
    })
}
#[test]
fn ap05_borrowed_fragment_factory_and_context() {
    block_on(async {
        let r = Runtime::new();
        let node = Add { delta: 5 };
        for _ in 0..2 {
            let out = r
                .execute(
                    |f, input| {
                        let a = f.chain(fragment(&node, input));
                        f.chain(|s| context_fragment(s, &node, a))
                    },
                    1u32,
                )
                .await
                .unwrap();
            assert_eq!(out, 13);
        }
    })
}
#[test]
fn ap04_unit_chain_keeps_step() {
    block_on(async {
        let events = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        r.execute(
            |f, (a, events)| {
                f.chain(|s| {
                    s.then(record, (a, events));
                });
                f.then(record, (a, events));
            },
            (3u32, events.clone()),
        )
        .await
        .unwrap();
        assert_eq!(*events.borrow(), vec![3, 3]);
    })
}
#[test]
fn ap04_child_ref_escape_rejected_before_nodes() {
    block_on(async {
        let saved = Cell::new(None);
        let events = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let error = r
            .execute(
                |f, (a, events)| {
                    f.chain(|s| {
                        let local = s.then(inc, a);
                        saved.set(Some(local));
                        s.then(record, (local, events));
                    });
                    f.then(inc, saved.get().unwrap())
                },
                (1u32, events.clone()),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RunError::Definition(BuildError::InvisibleRef)
        ));
        assert!(events.borrow().is_empty());
    })
}
#[test]
fn ap04_sibling_ref_rejected_before_nodes() {
    block_on(async {
        let saved = Cell::new(None);
        let r = Runtime::new();
        let error = r
            .execute(
                |f, a| {
                    f.chain(|s| {
                        saved.set(Some(s.then(inc, a)));
                    });
                    f.chain(|s| s.then(inc, saved.get().unwrap()))
                },
                1u32,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RunError::Definition(BuildError::InvisibleRef)
        ));
    })
}
#[test]
fn ap01_foreign_definition_rejected() {
    block_on(async {
        let saved = Cell::new(None);
        let r = Runtime::new();
        r.execute(
            |_, a| {
                saved.set(Some(a));
                a
            },
            1u32,
        )
        .await
        .unwrap();
        let e = r
            .execute(|f, _| f.then(inc, saved.get().unwrap()), 2u32)
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Definition(BuildError::ForeignRef)));
    })
}
#[test]
fn ap03_root_alias_rejected() {
    block_on(async {
        let r = Runtime::new();
        let e = r
            .execute(
                |f, a| {
                    let alias = f.chain(|_| a);
                    (a, alias)
                },
                1u32,
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Runtime(_)));
        assert!(e.to_string().contains("DuplicateRootDataId"));
    })
}
#[test]
fn ap06_each_single_multi_unit_empty() {
    block_on(async {
        let r = Runtime::new();
        let scores = r
            .execute(
                |f, (items, rules)| f.each(items, |item, s| s.then(sum, (item, rules))),
                (vec![1u32, 2, 3], 10u32),
            )
            .await
            .unwrap();
        assert_eq!(scores, vec![11, 12, 13]);
        let (scores, reports) = r
            .execute(
                |f, (items, rules)| {
                    f.each(items, |item, s| {
                        let score = s.then(sum, (item, rules));
                        let report = s.then(report, (item, score));
                        (score, report)
                    })
                },
                (vec![1u32, 2], 10u32),
            )
            .await
            .unwrap();
        assert_eq!(scores, vec![11, 12]);
        assert_eq!(reports, vec!["1:11", "2:12"]);
        let empty = r
            .execute(
                |f, items| {
                    f.each(items, |item, s| {
                        let a = s.then(inc, item);
                        let b = s.then(report, (item, a));
                        (a, b)
                    })
                },
                Vec::<u32>::new(),
            )
            .await
            .unwrap();
        assert_eq!(empty, (vec![], vec![]));
        let events = Rc::new(RefCell::new(vec![]));
        r.execute(
            |f, (items, events)| {
                f.each(items, |item, s| {
                    s.then(record, (item, events));
                });
            },
            (vec![2u32, 4, 6], events.clone()),
        )
        .await
        .unwrap();
        assert_eq!(*events.borrow(), vec![2, 4, 6]);
    })
}
#[test]
fn ap06_item_cap_can_enter_descendants() {
    block_on(async {
        let r = Runtime::new();
        let out = r
            .execute(
                |f, items| {
                    f.each(items, |item, s| {
                        let alias = s.chain(|c| c.chain(|_| item));
                        s.then(inc, alias)
                    })
                },
                vec![1u32, 2],
            )
            .await;
        assert_eq!(out.unwrap(), vec![2, 3]);
    })
}
#[test]
fn ap06_cannot_collect_item_or_ancestor() {
    block_on(async {
        let r = Runtime::new();
        assert!(matches!(
            r.execute(|f, items| f.each(items, |item, _| item), vec![1u32])
                .await,
            Err(RunError::Runtime(_))
        ));
        assert!(matches!(
            r.execute(|f, (items, a)| f.each(items, |_, _| a), (vec![1u32], 7u32))
                .await,
            Err(RunError::Runtime(_))
        ));
    })
}
#[test]
fn ap06_duplicate_item_output_rejected() {
    block_on(async {
        let r = Runtime::new();
        let e = r
            .execute(
                |f, items| {
                    f.each(items, |item, s| {
                        let a = s.then(inc, item);
                        (a, a)
                    })
                },
                vec![1u32],
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Runtime(_)));
    })
}
#[test]
fn ap06_choose_value_otherwise_and_no_match() {
    block_on(async {
        let r = Runtime::new();
        let make = |route: u32| {
            r.execute(
                |f, (route, a)| {
                    f.choose(route, |c| {
                        c.case(0, |s| s.then(inc, a));
                        c.otherwise(|s| s.then(sum, (a, a)));
                    })
                },
                (route, 3u32),
            )
        };
        assert_eq!(make(0).await.unwrap(), 4);
        assert_eq!(make(1).await.unwrap(), 6);
        let e = r
            .execute(
                |f, (route, a)| {
                    f.choose(route, |c| {
                        c.case(0, |s| s.then(inc, a));
                    })
                },
                (1u32, 3u32),
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::NoMatchingCase));
    })
}
#[test]
fn ap06_choose_unit_only_selected_executes() {
    block_on(async {
        let events = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        r.execute(
            |f, (route, a, events)| {
                f.choose(route, |c| {
                    c.case(0, |s| {
                        s.then(record, (a, events));
                    });
                    c.case(1, |s| {
                        let a = s.then(inc, a);
                        s.then(record, (a, events));
                    });
                });
            },
            (1u32, 3u32, events.clone()),
        )
        .await
        .unwrap();
        assert_eq!(*events.borrow(), vec![4]);
    })
}
#[test]
fn ap06_duplicate_cases_rejected_at_definition() {
    block_on(async {
        let r = Runtime::new();
        let e = r
            .execute(
                |f, (route, a)| {
                    f.choose(route, |c| {
                        c.case(0, |s| s.then(inc, a));
                        c.case(0, |s| s.then(inc, a));
                    })
                },
                (0u32, 1u32),
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Definition(BuildError::DuplicateCase)));
        let e = r
            .execute(
                |f, (route, a)| {
                    f.choose(route, |c| {
                        c.otherwise(|s| s.then(inc, a));
                        c.otherwise(|s| s.then(inc, a));
                    })
                },
                (0u32, 1u32),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            e,
            RunError::Definition(BuildError::DuplicateOtherwise)
        ));
    })
}
#[test]
fn ap02_arc_independent_node_config() {
    block_on(async {
        let shared = Arc::new(Add { delta: 9 });
        let r = Runtime::new();
        for _ in 0..2 {
            assert_eq!(
                r.execute(|f, a| f.chain(|s| s.then(shared.clone(), a)), 1u32)
                    .await
                    .unwrap(),
                10
            );
        }
    })
}
