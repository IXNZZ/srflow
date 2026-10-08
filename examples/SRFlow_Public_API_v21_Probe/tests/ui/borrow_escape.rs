use srflow_public_api_v21_probe::*;
async fn leak<'a>(q:Query<&'a String>)->Result<&'a str,BodyError>{Ok(q.get().as_str())}
fn main(){let r=Runtime::new();let _=r.execute(|f,a|f.then(leak,a),String::from("hello"));}
