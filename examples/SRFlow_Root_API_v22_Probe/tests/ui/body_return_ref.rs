use srflow::Runtime;
async fn run(){let _value:Result<u32,_>=Runtime::new().execute(|_f,input|input,1u32).await;}
fn main(){}
