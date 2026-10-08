use srflow_public_api_v21_probe::*;
async fn text(q:Query<&String>)->Result<u32,BodyError>{Ok(q.get().len() as u32)}
fn main(){let r=Runtime::new();let _=r.execute(|f,a|f.then(text,a),3u32);}
