use srflow_public_api_v21_probe::{BodyError, Data, Node, Query, Runtime};

#[derive(Data)]
struct Amount(u32);

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

struct AddOne;
impl Node for AddOne {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

fn main() {
    let output = futures::executor::block_on(async {
        let runtime = Runtime::new();
        let add = AddOne;
        runtime
            .execute(
                |flow, amount| {
                    let doubled = flow.then(double, amount);
                    flow.chain(|sub| sub.then(&add, doubled))
                },
                Amount(21),
            )
            .await
    })
    .unwrap();
    assert_eq!(output.0, 43);
}
