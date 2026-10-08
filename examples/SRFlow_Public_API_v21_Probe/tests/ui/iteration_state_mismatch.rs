use srflow_public_api_v21_probe::*;
async fn text(_:Query<&u32>)->Result<String,BodyError>{Ok("text".into())}
fn main(){let r=Runtime::new();let _=r.execute(|f,a|f.iter(a,3,|cur,s|s.then(text,cur)),3u32);}
