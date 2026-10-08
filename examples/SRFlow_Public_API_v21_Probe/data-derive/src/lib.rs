//! Minimal derive implementation for the independent Public API probe.
#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input, parse_quote};

#[proc_macro_derive(Data)]
pub fn derive_data(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    // Any requires only the complete business type to be 'static. Field types
    // need not implement Data, Clone, Copy, Send, or Sync.
    input
        .generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(Self: 'static));
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    quote! {
        impl #impl_generics ::srflow_public_api_v21_probe::Data
            for #name #type_generics #where_clause {}
    }
    .into()
}
