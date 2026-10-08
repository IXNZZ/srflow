use srflow_public_api_v21_probe::*;
async fn pair(q:Query<&u32>)->Result<(u32,u32),BodyError>{let a=q.get();Ok((*a,*a))}
fn main(){let r=Runtime::new();let _=r.execute(|f,a|f.then(pair,a),3u32);}
