use futures::{executor::block_on, task::noop_waker_ref};
use srflow_public_api_v21_probe::*;
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    task::{Context, Poll},
};
#[derive(Debug)]
struct Tracked {
    n: u32,
    drops: Rc<RefCell<Vec<u32>>>,
}
impl Data for Tracked {}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.borrow_mut().push(self.n)
    }
}
fn tracked(n: u32, drops: &Rc<RefCell<Vec<u32>>>) -> Tracked {
    Tracked {
        n,
        drops: drops.clone(),
    }
}
async fn next(q: Query<&Tracked>) -> Result<Tracked, BodyError> {
    let a = q.get();
    Ok(Tracked {
        n: a.n + 1,
        drops: a.drops.clone(),
    })
}
async fn keep_value(q: Query<&Tracked>) -> Result<u32, BodyError> {
    Ok(q.get().n)
}
async fn fail(q: Query<&Tracked>) -> Result<Tracked, BodyError> {
    let _ = q.get();
    Err(BodyError::fail(std::io::Error::other("failed")))
}
async fn retry(q: Query<&Tracked>) -> Result<(), BodyError> {
    let _ = q.get();
    Err(BodyError::retry("reject"))
}
async fn yielding(q: Query<&Tracked>) -> Result<u32, BodyError> {
    let a = q.get();
    let mut first = true;
    futures::future::poll_fn(|cx| {
        if first {
            first = false;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    Ok(a.n)
}
async fn pending(q: Query<&Tracked>) -> Result<u32, BodyError> {
    let a = q.get();
    futures::future::pending::<()>().await;
    Ok(a.n)
}

#[test]
fn chain_drops_only_unexported_data_before_next_step() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let observe = async |q: Query<&Tracked>| -> Result<u32, BodyError> {
            let a = q.get();
            assert_eq!(*a.drops.borrow(), vec![1]);
            Ok(a.n)
        };
        let out = r
            .execute(
                |f, a| {
                    let final_ = f.chain(|s| {
                        let tmp = s.then(next, a);
                        s.then(next, tmp)
                    });
                    f.then(observe, final_)
                },
                tracked(0, &drops),
            )
            .await
            .unwrap();
        assert_eq!(out, 2);
        let mut ids = drops.borrow().clone();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2]);
    })
}
#[test]
fn copied_refs_need_no_business_clone_or_copy() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let out = r
            .execute(
                |f, a| {
                    let copy = a;
                    let x = f.then(keep_value, a);
                    let y = f.then(keep_value, copy);
                    (x, y)
                },
                tracked(9, &drops),
            )
            .await
            .unwrap();
        assert_eq!(out, (9, 9));
        assert_eq!(*drops.borrow(), vec![9]);
    })
}
#[test]
fn root_alias_failure_takes_no_partial_output() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let e = r
            .execute(
                |f, a| {
                    let alias = f.chain(|_| a);
                    (a, alias)
                },
                tracked(9, &drops),
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Runtime(_)));
        assert_eq!(*drops.borrow(), vec![9]);
    })
}
#[test]
fn retry_cleans_entire_attempt_before_reexecution() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let count = Cell::new(0);
        let r = Runtime::new();
        let reject = async |q: Query<&Tracked>| -> Result<(), BodyError> {
            let a = q.get();
            let n = count.get() + 1;
            count.set(n);
            if n == 2 {
                assert_eq!(*a.drops.borrow(), vec![1]);
                Ok(())
            } else {
                Err(BodyError::retry("first reject"))
            }
        };
        let out = r
            .execute(
                |f, a| {
                    f.retry(1, |s| {
                        let next = s.then(next, a);
                        s.then(&reject, next);
                        next
                    })
                },
                tracked(0, &drops),
            )
            .await
            .unwrap();
        assert_eq!(out.n, 1);
        assert_eq!(count.get(), 2);
        assert!(drops.borrow().contains(&0));
        drop(out);
        assert_eq!(drops.borrow().iter().filter(|&&n| n == 1).count(), 2);
    })
}
#[test]
fn iteration_recycles_owned_old_state_and_preserves_imported_initial() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let stop = async |q: Query<&Tracked>| -> Result<(), BodyError> {
            let a = q.get();
            if a.n == 2 {
                assert!(a.drops.borrow().contains(&1));
                assert!(!a.drops.borrow().contains(&0));
                Err(BodyError::iter_break())
            } else {
                Ok(())
            }
        };
        let out = r
            .execute(
                |f, a| {
                    let result = f.iter(a, 4, |cur, s| {
                        s.then(&stop, cur);
                        s.then(next, cur)
                    });
                    f.then(keep_value, a);
                    result
                },
                tracked(0, &drops),
            )
            .await
            .unwrap();
        assert_eq!(out.n, 2);
        assert!(drops.borrow().contains(&0));
        assert!(!drops.borrow().contains(&2));
        drop(out);
        let mut ids = drops.borrow().clone();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2]);
    })
}
#[test]
fn each_failure_drops_partial_collector_and_stops_items() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let visited = RefCell::new(vec![]);
        let r = Runtime::new();
        let process = async |q: Query<&Tracked>| -> Result<Tracked, BodyError> {
            let a = q.get();
            visited.borrow_mut().push(a.n);
            if a.n == 1 {
                Err(BodyError::fail(std::io::Error::other("stop")))
            } else {
                Ok(Tracked {
                    n: a.n + 10,
                    drops: a.drops.clone(),
                })
            }
        };
        let items = vec![tracked(0, &drops), tracked(1, &drops), tracked(2, &drops)];
        assert!(matches!(
            r.execute(
                |f, items| f.each(items, |item, s| s.then(&process, item)),
                items
            )
            .await,
            Err(RunError::Body(_))
        ));
        assert_eq!(*visited.borrow(), vec![0, 1]);
        let mut ids = drops.borrow().clone();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2, 10]);
    })
}
#[test]
fn each_group_validates_all_outputs_before_consuming() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let e = r
            .execute(
                |f, (items, ancestor)| {
                    f.each(items, |item, s| {
                        let owned = s.then(next, item);
                        (owned, ancestor)
                    })
                },
                (vec![tracked(0, &drops)], tracked(9, &drops)),
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Runtime(_)));
        let mut ids = drops.borrow().clone();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 9]);
    })
}
#[test]
fn each_multi_output_keeps_each_value_owned_once() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let (a, b) = r
            .execute(
                |f, items| {
                    f.each(items, |item, s| {
                        let a = s.then(next, item);
                        let b = s.then(next, a);
                        (a, b)
                    })
                },
                vec![tracked(0, &drops)],
            )
            .await
            .unwrap();
        assert_eq!(a[0].n, 1);
        assert_eq!(b[0].n, 2);
        assert_eq!(*drops.borrow(), vec![0]);
        drop((a, b));
        let mut ids = drops.borrow().clone();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2]);
    })
}
#[test]
fn suspended_query_borrow_survives_then_finishes() {
    let drops = Rc::new(RefCell::new(vec![]));
    let r = Runtime::new();
    let mut future =
        Box::pin(r.execute(|f, a| f.chain(|s| s.then(yielding, a)), tracked(7, &drops)));
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    assert!(drops.borrow().is_empty());
    assert_eq!(block_on(future).unwrap(), 7);
    assert_eq!(*drops.borrow(), vec![7]);
}
#[test]
fn cancel_pending_nested_call_drops_each_owned_value_once() {
    let drops = Rc::new(RefCell::new(vec![]));
    let r = Runtime::new();
    let mut future = Box::pin(r.execute(
        |f, a| {
            f.retry(2, |attempt| {
                attempt.chain(|s| {
                    let tmp = s.then(next, a);
                    s.then(pending, tmp)
                })
            })
        },
        tracked(7, &drops),
    ));
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    assert!(drops.borrow().is_empty());
    drop(future);
    let mut ids = drops.borrow().clone();
    ids.sort();
    assert_eq!(ids, vec![7, 8]);
}
#[test]
fn failure_in_nested_chain_cleans_all_scopes() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let e = r
            .execute(
                |f, a| {
                    f.retry(2, |s| {
                        s.chain(|sub| {
                            let a = sub.then(next, a);
                            sub.then(fail, a)
                        })
                    })
                },
                tracked(0, &drops),
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Body(_)));
        let mut ids = drops.borrow().clone();
        ids.sort();
        assert_eq!(ids, vec![0, 1]);
    })
}
#[test]
fn exhausted_retry_cleans_failed_data_without_returning_state() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let e = r
            .execute(
                |f, a| {
                    f.retry(1, |s| {
                        let next = s.then(next, a);
                        s.then(retry, next);
                        next
                    })
                },
                tracked(0, &drops),
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::RetryExhausted { attempts: 2, .. }));
        let mut ids = drops.borrow().clone();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 1]);
    })
}

#[test]
fn break_returns_current_and_drops_already_generated_next() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let r = Runtime::new();
        let stop =
            async |_: Query<&Tracked>| -> Result<(), BodyError> { Err(BodyError::iter_break()) };
        let out = r
            .execute(
                |f, a| {
                    f.iter(a, 3, |cur, s| {
                        let unused = s.then(next, cur);
                        s.then(stop, cur);
                        unused
                    })
                },
                tracked(0, &drops),
            )
            .await
            .unwrap();
        assert_eq!(out.n, 0);
        assert_eq!(*drops.borrow(), vec![1]);
        drop(out);
        assert_eq!(*drops.borrow(), vec![1, 0]);
    })
}

#[test]
fn retry_from_second_round_drops_promoted_state_and_restarts_initial() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let visited = RefCell::new(vec![]);
        let rejected = Cell::new(false);
        let r = Runtime::new();
        let service = async |q: Query<&Tracked>| -> Result<(), BodyError> {
            let a = q.get();
            visited.borrow_mut().push(a.n);
            if a.n == 0 && rejected.get() {
                assert!(a.drops.borrow().contains(&1));
            }
            if a.n == 1 && !rejected.replace(true) {
                Err(BodyError::retry("second round transient"))
            } else {
                Ok(())
            }
        };
        let stop = async |q: Query<&Tracked>| -> Result<(), BodyError> {
            if q.get().n == 2 {
                Err(BodyError::iter_break())
            } else {
                Ok(())
            }
        };
        let out = r
            .execute(
                |f, a| {
                    f.retry(1, |attempt| {
                        attempt.iter(a, 4, |cur, round| {
                            round.chain(|s| {
                                s.then(&service, cur);
                                s.then(stop, cur);
                            });
                            round.then(next, cur)
                        })
                    })
                },
                tracked(0, &drops),
            )
            .await
            .unwrap();
        assert_eq!(out.n, 2);
        assert_eq!(*visited.borrow(), vec![0, 1, 0, 1, 2]);
        drop(out);
        let mut ids = drops.borrow().clone();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 1, 2]);
    })
}

#[test]
fn tuple_states_sharing_one_next_target_never_duplicate_ownership() {
    block_on(async {
        let drops = Rc::new(RefCell::new(vec![]));
        let count = Cell::new(0);
        let r = Runtime::new();
        let judge = async |q: Query<(&Tracked, &Tracked)>| -> Result<(), BodyError> {
            let _ = q.get();
            let n = count.get() + 1;
            count.set(n);
            if n == 2 {
                Err(BodyError::iter_break())
            } else {
                Ok(())
            }
        };
        let e = r
            .execute(
                |f, initial| {
                    f.iter(initial, 3, |(a, b), round| {
                        round.then(judge, (a, b));
                        let next = round.then(next, a);
                        (next, next)
                    })
                },
                (tracked(0, &drops), tracked(10, &drops)),
            )
            .await
            .unwrap_err();
        assert!(matches!(e, RunError::Runtime(_)));
        let mut ids = drops.borrow().clone();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 10]);
    })
}
