use srflow::{Flow,Callable,RefShape,Query,BodyError};
// The original projected Callable bound cannot invert Borrowed<'a> to a RefShape.
fn auto<N,A,K>(_: &mut Flow<'_>, _: N) where A:RefShape, K:'static, N:Callable<A,K> {}
async fn plus(q:Query<&u32>)->Result<u32,BodyError>{Ok(*q.get()+1)}
fn main(){}
fn witness(f:&mut Flow<'_>){auto(f,plus);}
