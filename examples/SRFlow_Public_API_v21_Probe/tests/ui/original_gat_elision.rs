// Original candidate, preserved independently of the revised public protocol.
struct Query<T>(T);
trait Input {type Borrowed<'a>;}
impl Input for (u32,u32){type Borrowed<'a>=(&'a u32,&'a u32);}
trait Node{type Input:Input;async fn run(&self,q:Query<<Self::Input as Input>::Borrowed<'_>>)->u32;}
struct Add;
impl Node for Add{type Input=(u32,u32);async fn run(&self,q:Query<(&u32,&u32)>)->u32{let(a,b)=q.0;a+b}}
fn main(){}
