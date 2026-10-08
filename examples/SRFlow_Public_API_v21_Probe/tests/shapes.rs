use futures::executor::block_on;
use srflow_public_api_v21_probe::*;
async fn add6(q: Query<(&u32, &u32, &u32, &u32, &u32, &u32)>) -> Result<u32, BodyError> {
    let (a, b, c, d, e, f) = q.get();
    Ok(a + b + c + d + e + f)
}
async fn inc(q: Query<&u32>) -> Result<u32, BodyError> {
    Ok(q.get() + 1)
}
struct Zero;
#[test]
fn ap03_single_boxed_tuple_value_is_not_a_multi_output_shape() {
    block_on(async {
        async fn pair(q: Query<&u32>) -> Result<Box<(u32, u32)>, BodyError> {
            let a = q.get();
            Ok(Box::new((*a, *a + 1)))
        }
        #[allow(clippy::borrowed_box)] // The Box is the single registered Data type, not a tuple Shape.
        async fn product(q: Query<&Box<(u32, u32)>>) -> Result<u32, BodyError> {
            let (a, b) = q.get().as_ref();
            Ok(a * b)
        }
        let r = Runtime::new();
        let (pair, product) = r
            .execute(
                |f, a| {
                    let pair = f.then(pair, a);
                    let product = f.then(product, pair);
                    (pair, product)
                },
                3u32,
            )
            .await
            .unwrap();
        assert_eq!(*pair, (3, 4));
        assert_eq!(product, 12);
    })
}
impl Node for Zero {
    type Input = ();
    type Output = ();
    async fn run(&self, q: Query<&Self::Input>) -> Result<(), BodyError> {
        let () = q.get();
        Ok(())
    }
}
#[test]
fn ap03_six_inputs_outputs_and_each_shape() {
    block_on(async {
        let r = Runtime::new();
        assert_eq!(
            r.execute(
                |f, args| f.then(add6, args),
                (1u32, 2u32, 3u32, 4u32, 5u32, 6u32)
            )
            .await
            .unwrap(),
            21
        );
        let out = r
            .execute(
                |f, (a, b, c, d, e, g)| {
                    let a = f.then(inc, a);
                    let b = f.then(inc, b);
                    let c = f.then(inc, c);
                    let d = f.then(inc, d);
                    let e = f.then(inc, e);
                    let g = f.then(inc, g);
                    (a, b, c, d, e, g)
                },
                (1u32, 2u32, 3u32, 4u32, 5u32, 6u32),
            )
            .await
            .unwrap();
        assert_eq!(out, (2, 3, 4, 5, 6, 7));
        let out = r
            .execute(
                |f, items| {
                    f.each(items, |item, s| {
                        let a = s.then(inc, item);
                        let b = s.then(inc, a);
                        let c = s.then(inc, b);
                        let d = s.then(inc, c);
                        let e = s.then(inc, d);
                        let g = s.then(inc, e);
                        (a, b, c, d, e, g)
                    })
                },
                vec![0u32, 1],
            )
            .await
            .unwrap();
        assert_eq!(
            out,
            (
                vec![1, 2],
                vec![2, 3],
                vec![3, 4],
                vec![4, 5],
                vec![5, 6],
                vec![6, 7]
            )
        );
    })
}
#[test]
fn ap02_zero_struct_node_and_same_type_positions() {
    block_on(async {
        let r = Runtime::new();
        let zero = Zero;
        let out = r
            .execute(
                |f, (a, b)| {
                    f.then(&zero, ());
                    (a, b)
                },
                (2u32, 7u32),
            )
            .await
            .unwrap();
        assert_eq!(out, (2, 7));
    })
}

#[test]
fn ap07_tuple_state_shape() {
    block_on(async {
        let r = Runtime::new();
        let judge = async |q: Query<(&u32, &u32)>| -> Result<(), BodyError> {
            let (a, b) = q.get();
            assert_eq!(*b, *a + 10);
            if *a >= 2 {
                Err(BodyError::iter_break())
            } else {
                Ok(())
            }
        };
        let out = r
            .execute(
                |f, initial| {
                    f.iter(initial, 4, |(a, b), round| {
                        round.then(judge, (a, b));
                        (round.then(inc, a), round.then(inc, b))
                    })
                },
                (0u32, 10u32),
            )
            .await
            .unwrap();
        assert_eq!(out, (2, 12));
    })
}

#[test]
fn ap07_tuple_state_keeps_imported_targets_on_swap() {
    block_on(async {
        let r = Runtime::new();
        let count = std::cell::Cell::new(0);
        let judge = async |q: Query<(&u32, &u32)>| -> Result<(), BodyError> {
            let _ = q.get();
            let n = count.get() + 1;
            count.set(n);
            if n == 2 {
                Err(BodyError::iter_break())
            } else {
                Ok(())
            }
        };
        let out = r
            .execute(
                |f, initial| {
                    f.iter(initial, 3, |(a, b), round| {
                        round.then(judge, (a, b));
                        (b, a)
                    })
                },
                (2u32, 7u32),
            )
            .await
            .unwrap();
        assert_eq!(out, (7, 2));
    })
}

#[test]
fn ap03_three_four_five_position_shapes() {
    block_on(async {
        let r = Runtime::new();
        assert_eq!(
            r.execute(|_, refs| refs, (1u32, 2u32, 3u32)).await.unwrap(),
            (1, 2, 3)
        );
        assert_eq!(
            r.execute(|_, refs| refs, (1u32, 2u32, 3u32, 4u32))
                .await
                .unwrap(),
            (1, 2, 3, 4)
        );
        async fn five(q: Query<(&u32, &u32, &u32, &u32, &u32)>) -> Result<u32, BodyError> {
            let (a, b, c, d, e) = q.get();
            Ok(a + b + c + d + e)
        }
        assert_eq!(
            r.execute(|f, args| f.then(five, args), (1u32, 2u32, 3u32, 4u32, 5u32))
                .await
                .unwrap(),
            15
        );
        assert_eq!(
            r.execute(|_, refs| refs, (1u32, 2u32, 3u32, 4u32, 5u32))
                .await
                .unwrap(),
            (1, 2, 3, 4, 5)
        );
    })
}
