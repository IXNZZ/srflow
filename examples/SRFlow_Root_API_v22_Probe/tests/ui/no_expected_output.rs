use srflow::Runtime;
async fn run(){let _value=Runtime::new().execute(|f,_|{f.output::<u32>();},1u32).await;}
fn main(){}
