// Deliberately spell the boundary-size inputs; aliases would hide the API proof.
#![allow(clippy::type_complexity)]
use futures::executor::block_on;
use srflow_public_api_v21_probe::*;
async fn sum16(
    q: Query<(
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
        &u32,
    )>,
) -> Result<u32, BodyError> {
    let q = q.get();
    Ok(q.0
        + q.1
        + q.2
        + q.3
        + q.4
        + q.5
        + q.6
        + q.7
        + q.8
        + q.9
        + q.10
        + q.11
        + q.12
        + q.13
        + q.14
        + q.15)
}
async fn inc(q: Query<&u32>) -> Result<u32, BodyError> {
    Ok(q.get() + 1)
}
#[test]
fn ap03_sixteen_positions_without_debug_or_clone_bounds() {
    block_on(async {
        let r = Runtime::new();
        assert_eq!(
            r.execute(
                |f, a| f.then(sum16, a),
                (
                    1u32, 2u32, 3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32, 10u32, 11u32, 12u32,
                    13u32, 14u32, 15u32, 16u32
                )
            )
            .await
            .unwrap(),
            136
        );
        let out = r
            .execute(
                |_, a| a,
                (
                    1u32, 2u32, 3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32, 10u32, 11u32, 12u32,
                    13u32, 14u32, 15u32, 16u32,
                ),
            )
            .await
            .unwrap();
        assert_eq!(out.0, 1);
        assert_eq!(out.7, 8);
        assert_eq!(out.15, 16);
        let out = r
            .execute(
                |f, items| {
                    f.each(items, |a, f| {
                        (
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                            f.then(inc, a),
                        )
                    })
                },
                vec![2u32],
            )
            .await
            .unwrap();
        assert_eq!(out.0, vec![3]);
        assert_eq!(out.15, vec![3]);
    })
}
#[test]
fn ap07_sixteen_state_positions_share_one_control_boundary() {
    block_on(async {
        let count = std::cell::Cell::new(0);
        let judge = async |q: Query<(
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
            &u32,
        )>|
               -> Result<(), BodyError> {
            let q = q.get();
            assert_eq!(*q.15, 16);
            let n = count.get() + 1;
            count.set(n);
            if n == 2 {
                Err(BodyError::iter_break())
            } else {
                Ok(())
            }
        };
        let r = Runtime::new();
        let out = r
            .execute(
                |f, initial| {
                    f.iter(initial, 3, |current, round| {
                        round.then(judge, current);
                        current
                    })
                },
                (
                    1u32, 2u32, 3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32, 10u32, 11u32, 12u32,
                    13u32, 14u32, 15u32, 16u32,
                ),
            )
            .await
            .unwrap();
        assert_eq!(out.0, 1);
        assert_eq!(out.15, 16);
        assert_eq!(count.get(), 2);
    })
}
