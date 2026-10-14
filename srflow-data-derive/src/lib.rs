//! SRFlow 的 `#[derive(Data)]` 辅助宏。
//!
//! 生成空 marker 实现：只要求完整业务类型满足 `'static`，不增加字段级约束。
//! 展开路径通过 `proc-macro-crate` 解析消费者实际使用的依赖名，因此库自身、
//! 标准独立消费者与重命名依赖都能正确展开。
#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, Ident, parse_macro_input, parse_quote};

/// 为业务类型生成 `srflow::Data` 的空实现。
#[proc_macro_derive(Data)]
pub fn derive_data(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    // Any 只要求完整 Self 为 'static；字段类型不需要实现 Data／Clone／Send 等。
    input
        .generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(Self: 'static));
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let path = srflow_path();
    quote! {
        impl #impl_generics #path::Data for #name #type_generics #where_clause {}
    }
    .into()
}

/// 解析 `srflow` 在消费者视角下的实际路径。
///
/// - 库自身（含其 doctest）：`::srflow`——主包以 `extern crate self as srflow;` 提供该路径；
/// - 外部消费者：实际依赖名（支持重命名依赖）。
fn srflow_path() -> TokenStream2 {
    match crate_name("srflow") {
        Ok(FoundCrate::Itself) => quote!(::srflow),
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        // 理论上不可达：derive 只能由依赖 srflow 的 crate 使用；保守回退到包名。
        Err(_) => quote!(::srflow),
    }
}
