use srflow_public_api_v21_probe::{BodyError, Data, Node, Query, Runtime};

#[derive(Data)]
pub struct Prose {
    name: String,
}

#[derive(Data)]
pub struct Rules {
    trans: String,
}

pub async fn add(query: Query<(&Prose, &Rules)>) -> Result<String, BodyError> {
    let (prose, rules) = query.get();
    Ok(format!("names: {}, {}", prose.name, rules.trans))
}

pub async fn gener(_: Query<()>) -> Result<String, BodyError> {
    Ok(String::from("Hello, World!"))
}

pub struct Scope {
    name: String,
}

impl Node for Scope {
    type Input = String;
    type Output = String;

    async fn run(&self, q: Query<&Self::Input>) -> Result<Self::Output, BodyError> {
        let input = q.get();
        Ok(format!("scope name: {}, {}", self.name, input))
    }
}

pub async fn add_rules(q: Query<&String>) -> Result<Rules, BodyError> {
    Ok(Rules {
        trans: format!("hello: {}", q.get()),
    })
}

pub fn main() {
    futures::executor::block_on(run());
}

pub async fn run() {
    let runtime = Runtime::new();

    let prose = Prose {
        name: String::from("value"),
    };

    let rules = Rules {
        trans: String::from("trans"),
    };

    let result = runtime
        .execute(
            |flow, (prose, _rules)| {
                let scope = Scope {
                    name: String::from("scope1"),
                };
                let r = flow.chain(|sub| {
                    let g = sub.then(gener, ());

                    sub.then(scope, g)
                });
                let rules2 = flow.then(add_rules, r);
                flow.then(add, (prose, rules2))
            },
            (prose, rules),
        )
        .await;
    println!("result: {:?}", result);
}
