use srflow_public_api_v21_probe::{BodyError, Flow, Node, Query, Ref, Runtime};

struct Add;
impl Node for Add {
    type Input = (u32, u32);
    type Output = u32;
    async fn run(&self, query: Query<&Self::Input>) -> Result<u32, BodyError> {
        let (a, b) = query.get();
        futures::future::ready(()).await;
        Ok(a + b)
    }
}

async fn add(query: Query<(&u32, &u32)>) -> Result<u32, BodyError> {
    let (a, b) = query.get();
    futures::future::ready(()).await;
    Ok(a + b)
}

struct Concrete;
impl Node for Concrete {
    type Input = (u32, u32);
    type Output = u32;
    async fn run(&self, query: Query<&(u32, u32)>) -> Result<u32, BodyError> {
        let (a, b) = query.get();
        futures::future::ready(()).await;
        Ok(a + b)
    }
}
struct Scalar;
impl Node for Scalar {
    type Input = u32;
    type Output = u32;
    async fn run(&self, query: Query<&u32>) -> Result<u32, BodyError> {
        Ok(*query.get())
    }
}

#[test]
fn struct_concrete_input_tag_also_omits_lifetimes() {
    futures::executor::block_on(async {
        let r = Runtime::new();
        let concrete = Concrete;
        let scalar = Scalar;
        assert_eq!(
            r.execute(
                |f, (a, b)| {
                    let out = f.then(&concrete, (a, b));
                    f.then(&scalar, out)
                },
                (2u32, 3u32)
            )
            .await
            .unwrap(),
            5
        );
    });
}

async fn twice(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() * 2)
}
async fn noop(query: Query<&u32>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}
async fn zero(query: Query<()>) -> Result<u32, BodyError> {
    query.get();
    Ok(7)
}

fn fragment<'n>(a: Ref<u32>, b: Ref<u32>) -> impl FnOnce(&mut Flow<'n>) -> Ref<u32> {
    move |flow| flow.then(add, (a, b))
}

#[test]
fn query_function_and_struct() {
    futures::executor::block_on(async {
        let runtime = Runtime::new();
        let node = Add;
        let out = runtime
            .execute(
                |flow, (a, b)| {
                    let sum = flow.then(add, (a, b));
                    let double = flow.then(twice, sum);
                    let structured = flow.then(&node, (double, b));
                    flow.then(noop, structured);
                    structured
                },
                (2u32, 3u32),
            )
            .await
            .unwrap();
        assert_eq!(out, 13);
        assert_eq!(
            runtime
                .execute(
                    |f, input| {
                        let a = f.then(zero, ());
                        f.chain(fragment(a, input))
                    },
                    3u32
                )
                .await
                .unwrap(),
            10
        );
    });
}

#[test]
fn natural_root_and_output_shapes() {
    futures::executor::block_on(async {
        let r = Runtime::new();
        assert_eq!(r.execute(|f, a| f.then(twice, a), 2u32).await.unwrap(), 4);
        assert_eq!(
            r.execute(
                |f, (a, b, c, d)| {
                    let x = f.then(add, (a, b));
                    let y = f.then(add, (c, d));
                    (x, y)
                },
                (1u32, 2u32, 3u32, 4u32)
            )
            .await
            .unwrap(),
            (3, 7)
        );
        r.execute(
            |f, a| {
                f.then(noop, a);
            },
            2u32,
        )
        .await
        .unwrap();
    })
}

#[test]
fn arc_node_and_borrowed_function() {
    futures::executor::block_on(async {
        let r = Runtime::new();
        let shared = std::sync::Arc::new(Add);
        for _ in 0..2 {
            assert_eq!(
                r.execute(
                    |f, (a, b)| {
                        let c = f.then(std::sync::Arc::clone(&shared), (a, b));
                        f.then(&twice, c)
                    },
                    (1u32, 2u32)
                )
                .await
                .unwrap(),
                6
            )
        }
    })
}
