//! Independent stable/safe Public API probe. Not a production implementation.
#![forbid(unsafe_code)]
#![allow(private_interfaces, private_bounds, async_fn_in_trait)]
#[rustfmt::skip]
#[allow(dead_code, rustdoc::broken_intra_doc_links)] mod core;
mod error;
mod flow;
mod shape;
pub use error::*;
pub use flow::{Choice, Flow, Runtime};
#[doc(hidden)]
pub use shape::{Callable, FunctionCall, NodeOutput, RefShape, RootInput, StateShape, StructCall};
pub use shape::{Data, InputSpec, Node, Query, Ref};
pub use srflow_data_derive_probe::Data;
