//! Derives for the `domain` crate. Each one only writes code you could write by hand: trait impls
//! and the enums that collect a command's or an aggregate's events and rejections.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as Tokens;
use quote::{format_ident, quote};
use syn::meta::ParseNestedMeta;
use syn::punctuated::Punctuated;
use syn::{DeriveInput, Ident, Path, Token, Type, Visibility, parse_macro_input};

/// `impl Event`, named after the type.
#[proc_macro_derive(Event)]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let tag = name.to_string();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    quote! {
        impl #impl_generics ::domain::Event for #name #ty_generics #where_clause {
            const NAME: &'static str = #tag;
        }
    }
    .into()
}

/// `impl Rejection`, plus `Display` (its `Debug` form) and `std::error::Error`.
#[proc_macro_derive(Rejection)]
pub fn derive_rejection(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let tag = name.to_string();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    quote! {
        impl #impl_generics ::domain::Rejection for #name #ty_generics #where_clause {
            const NAME: &'static str = #tag;
        }

        impl #impl_generics ::std::fmt::Display for #name #ty_generics #where_clause {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::write!(f, "{:?}", self)
            }
        }

        impl #impl_generics ::std::error::Error for #name #ty_generics #where_clause {}
    }
    .into()
}

/// `#[command(target = Id, events(..), rejects(..), reply = Type)]`
///
/// Generates `<Name>Event` and `<Name>Rejection` enums and `impl Command`. `rejects` and `reply`
/// are optional; without `reply` the reply is `()`.
#[proc_macro_derive(Command, attributes(command))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    command(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// `#[aggregate(id = Id, state = State, events(..), commands(..))]`
///
/// Generates the `<Name>Event` enum, `impl Aggregate`, `impl AggregateId` for the id, and checks
/// that the aggregate implements `Evolve` and decides every listed command.
#[proc_macro_derive(Aggregate, attributes(aggregate))]
pub fn derive_aggregate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    aggregate(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn command(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut target, mut events, mut rejects, mut reply) = (None, Vec::new(), Vec::new(), None);
    parse_attr(input, "command", |meta| {
        if meta.path.is_ident("target") {
            target = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("events") {
            events = paths(&meta)?;
        } else if meta.path.is_ident("rejects") {
            rejects = paths(&meta)?;
        } else if meta.path.is_ident("reply") {
            reply = Some(meta.value()?.parse::<Type>()?);
        } else {
            return Err(meta.error("expected `target`, `events`, `rejects` or `reply`"));
        }
        Ok(())
    })?;
    let target =
        target.ok_or_else(|| syn::Error::new_spanned(name, "a command needs `#[command(target = SomeId, ..)]`"))?;
    let reply = reply.map_or_else(|| quote!(()), |t| quote!(#t));
    let tag = name.to_string();
    let event_enum = format_ident!("{}Event", name);
    let rejection_enum = format_ident!("{}Rejection", name);
    let events_doc = format!("The events `{name}` may record.");
    let rejections_doc = format!("The reasons `{name}` may be refused.");

    let event_union = union(&input.vis, &event_enum, &events, &events_doc, Kind::Events);
    let rejection_union = union(&input.vis, &rejection_enum, &rejects, &rejections_doc, Kind::Rejections);
    let variants: Vec<&Ident> = events.iter().map(last_ident).collect();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    Ok(quote! {
        #event_union
        #rejection_union

        impl<__E> ::domain::Widen<__E> for #event_enum
        where #( __E: ::std::convert::From<#events>, )*
        {
            fn widen(self) -> __E {
                match self { #( Self::#variants(e) => __E::from(e), )* }
            }
        }

        impl #impl_generics ::domain::Command for #name #ty_generics #where_clause {
            const NAME: &'static str = #tag;
            type Target = #target;
            type Event = #event_enum;
            type Rejection = #rejection_enum;
            type Reply = #reply;
        }
    })
}

fn aggregate(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut id, mut state, mut events, mut commands) = (None, None, Vec::new(), Vec::new());
    parse_attr(input, "aggregate", |meta| {
        if meta.path.is_ident("id") {
            id = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("state") {
            state = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("events") {
            events = paths(&meta)?;
        } else if meta.path.is_ident("commands") {
            commands = paths(&meta)?;
        } else {
            return Err(meta.error("expected `id`, `state`, `events` or `commands`"));
        }
        Ok(())
    })?;
    let missing =
        |what: &str| syn::Error::new_spanned(name, format!("an aggregate needs `#[aggregate({what} = .., ..)]`"));
    let id = id.ok_or_else(|| missing("id"))?;
    let state = state.ok_or_else(|| missing("state"))?;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(&input.generics, "an aggregate can't be generic"));
    }
    let tag = name.to_string();
    let event_enum = format_ident!("{}Event", name);
    let doc = format!("The events `{name}` records.");
    let event_union = union(&input.vis, &event_enum, &events, &doc, Kind::Events);

    Ok(quote! {
        #event_union

        impl ::domain::Aggregate for #name {
            const NAME: &'static str = #tag;
            type Id = #id;
            type State = #state;
            type Event = #event_enum;
        }

        impl ::domain::AggregateId for #id {
            type Aggregate = #name;
        }

        const _: () = {
            #[allow(dead_code)]
            fn checks() {
                ::domain::__assert_evolve::<#name>();
                #( ::domain::__assert_handles::<#name, #commands>(); )*
            }
        };
    })
}

#[derive(Clone, Copy)]
enum Kind {
    Events,
    Rejections,
}

/// An enum with one variant per member, named after the member, with `From` for each.
fn union(vis: &Visibility, name: &Ident, members: &[Path], doc: &str, kind: Kind) -> Tokens {
    let variants: Vec<&Ident> = members.iter().map(last_ident).collect();
    let (item_trait, set_trait) = match kind {
        Kind::Events => (quote!(::domain::Event), quote!(::domain::Events)),
        Kind::Rejections => (quote!(::domain::Rejection), quote!(::domain::Rejections)),
    };
    let error_impls = match kind {
        Kind::Events => quote!(),
        Kind::Rejections => quote! {
            impl ::std::fmt::Display for #name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    match *self { #( Self::#variants(ref r) => ::std::fmt::Display::fmt(r, f), )* }
                }
            }

            impl ::std::error::Error for #name {}
        },
    };
    quote! {
        #[doc = #doc]
        #[derive(::std::clone::Clone, ::std::fmt::Debug, ::std::cmp::PartialEq)]
        #vis enum #name {
            #( #variants(#members), )*
        }

        #(
            impl ::std::convert::From<#members> for #name {
                fn from(member: #members) -> Self {
                    Self::#variants(member)
                }
            }
        )*

        impl #set_trait for #name {
            fn name(&self) -> &'static str {
                match *self { #( Self::#variants(_) => <#members as #item_trait>::NAME, )* }
            }
        }

        #error_impls
    }
}

fn parse_attr(
    input: &DeriveInput,
    attr_name: &str,
    mut f: impl FnMut(ParseNestedMeta) -> syn::Result<()>,
) -> syn::Result<()> {
    let mut found = false;
    for attr in input.attrs.iter().filter(|a| a.path().is_ident(attr_name)) {
        found = true;
        attr.parse_nested_meta(&mut f)?;
    }
    if !found {
        let message = format!("missing `#[{attr_name}(..)]`");
        return Err(syn::Error::new_spanned(&input.ident, message));
    }
    Ok(())
}

fn paths(meta: &ParseNestedMeta) -> syn::Result<Vec<Path>> {
    let content;
    syn::parenthesized!(content in meta.input);
    let list = Punctuated::<Path, Token![,]>::parse_terminated(&content)?;
    Ok(list.into_iter().collect())
}

fn last_ident(path: &Path) -> &Ident {
    &path.segments.last().expect("a path has at least one segment").ident
}
