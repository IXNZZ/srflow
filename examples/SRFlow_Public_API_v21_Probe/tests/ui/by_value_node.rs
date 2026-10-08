use srflow_public_api_v21_probe::*;
async fn consume(a:u32)->Result<u32,BodyError>{Ok(a)}
fn main(){let r=Runtime::new();let _=r.execute(|f,a|f.then(consume,a),3u32);}
