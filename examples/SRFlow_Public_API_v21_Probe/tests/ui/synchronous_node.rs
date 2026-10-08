use srflow_public_api_v21_probe::*;
fn sync(q:Query<&u32>)->Result<u32,BodyError>{Ok(*q.get())}
fn main(){let r=Runtime::new();let _=r.execute(|f,a|f.then(sync,a),3u32);}
