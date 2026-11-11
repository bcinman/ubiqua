//! Derives for the `domain` crate. Each one only writes code you could write by hand: trait impls
//! and the enums that collect a command's or an aggregate's events and rejections.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as Tokens;
use quote::{format_ident, quote, quote_spanned};
use syn::meta::ParseNestedMeta;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
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

        impl ::domain::Source for #name {
            type SourceId = #id;
            type SourceEvent = #event_enum;
            type SourceState = ();

            fn register_source<__R: ::domain::Registry>(registry: &mut __R) {
                registry.aggregate::<Self>();
            }
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

/// `#[view(key = Key, from(Source(Event, ..), ..), state = State, events(..), key_of(Source, ..), version = N)]`
///
/// Generates `<Name>Input` (the events it reads), `<Name>Event` when `events(..)` is given,
/// `impl View`, and `impl Consumes` per source. `state` defaults to `()`. A source listed in
/// `key_of(..)` is routed by `impl KeyOf<Source>`; any other source's id must be the key.
#[proc_macro_derive(View, attributes(view))]
pub fn derive_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    view(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// `#[model(aggregates(..), views(..))]`: `impl Model`, registering each part.
#[proc_macro_derive(Model, attributes(model))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    model(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// `#[api(commands(..), views(..))]`: `impl Api`, plus `Sends` and `Reads` for each one listed.
#[proc_macro_derive(Api, attributes(api))]
pub fn derive_api(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    api(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn api(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut commands, mut views) = (Vec::new(), Vec::new());
    parse_attr(input, "api", |meta| {
        if meta.path.is_ident("commands") {
            commands = paths(&meta)?;
        } else if meta.path.is_ident("views") {
            views = paths(&meta)?;
        } else {
            return Err(meta.error("expected `commands` or `views`"));
        }
        Ok(())
    })?;
    let tag = name.to_string();
    Ok(quote! {
        impl ::domain::Api for #name {
            const NAME: &'static str = #tag;
        }
        #( impl ::domain::Sends<#commands> for #name {} )*
        #( impl ::domain::Reads<#views> for #name {} )*
    })
}

fn view(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut key, mut state, mut sources, mut events, mut key_of) = (None, None, Vec::new(), None, Vec::new());
    let mut version = None;
    parse_attr(input, "view", |meta| {
        if meta.path.is_ident("key") {
            key = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("state") {
            state = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("from") {
            meta.parse_nested_meta(|source| {
                let reads = paths(&source)?;
                if reads.is_empty() {
                    return Err(source.error("list the events the view reads from this source"));
                }
                sources.push((source.path.clone(), reads));
                Ok(())
            })?;
        } else if meta.path.is_ident("events") {
            events = Some(paths(&meta)?);
        } else if meta.path.is_ident("key_of") {
            key_of = paths(&meta)?;
        } else if meta.path.is_ident("version") {
            version = Some(meta.value()?.parse::<syn::LitInt>()?.base10_parse::<u64>()?);
        } else {
            return Err(meta.error("expected `key`, `from`, `state`, `events`, `key_of` or `version`"));
        }
        Ok(())
    })?;
    let key = key.ok_or_else(|| syn::Error::new_spanned(name, "a view needs `#[view(key = .., ..)]`"))?;
    if sources.is_empty() {
        return Err(syn::Error::new_spanned(name, "a view needs `#[view(from(Source(Event, ..)), ..)]`"));
    }
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(&input.generics, "a view can't be generic"));
    }
    for path in &key_of {
        if !sources.iter().any(|(source, _)| last_ident(source) == last_ident(path)) {
            return Err(syn::Error::new_spanned(path, "`key_of` names a source that isn't in `from(..)`"));
        }
    }
    let state = state.map_or_else(|| quote!(()), |t| quote!(#t));
    let tag = name.to_string();
    let version = version.map(|v| quote!(const VERSION: u64 = #v;));

    // Every event the view reads, once each, in the order first listed.
    let mut reads: Vec<Path> = Vec::new();
    for (_, list) in &sources {
        for path in list {
            if !reads.iter().any(|r| last_ident(r) == last_ident(path)) {
                reads.push(path.clone());
            }
        }
    }
    let input_enum = format_ident!("{}Input", name);
    let input_doc = format!("The events `{name}` reads.");
    let input_union = union(&input.vis, &input_enum, &reads, &input_doc, Kind::Events);

    let (event_union, event_ty, auto_emit) = match &events {
        Some(list) => {
            let event_enum = format_ident!("{}Event", name);
            let doc = format!("The events `{name}` emits.");
            let union = union(&input.vis, &event_enum, list, &doc, Kind::Events);
            (union, quote!(#event_enum), quote!(::std::option::Option::None))
        }
        None => (quote!(), quote!(#input_enum), quote!(::std::option::Option::Some(input.clone()))),
    };

    let source_paths: Vec<&Path> = sources.iter().map(|(source, _)| source).collect();
    let consumes = sources.iter().map(|(source, list)| {
        let variants = list.iter().map(last_ident);
        let key_expr = if key_of.iter().any(|path| last_ident(path) == last_ident(source)) {
            quote!(<Self as ::domain::KeyOf<#source>>::key_of(::domain::Origin { id, state }, &input)?)
        } else {
            quote_spanned!(source.span() =>
                <#key as ::domain::KeyFrom<<#source as ::domain::Source>::SourceId>>::key_from(id))
        };
        quote! {
            impl ::domain::Consumes<#source> for #name {
                #[allow(clippy::question_mark)]
                fn route(
                    id: &<#source as ::domain::Source>::SourceId,
                    event: &<#source as ::domain::Source>::SourceEvent,
                    state: &<#source as ::domain::Source>::SourceState,
                ) -> ::std::option::Option<(#key, #input_enum)> {
                    let _ = state;
                    let input = #(
                        if let ::std::option::Option::Some(e) =
                            <<#source as ::domain::Source>::SourceEvent as ::domain::Member<#list>>::get(event)
                        {
                            #input_enum::#variants(::std::clone::Clone::clone(e))
                        } else
                    )* {
                        return ::std::option::Option::None;
                    };
                    let key = #key_expr;
                    ::std::option::Option::Some((key, input))
                }
            }
        }
    });

    Ok(quote! {
        #input_union
        #event_union

        impl ::domain::View for #name {
            const NAME: &'static str = #tag;
            #version
            type Key = #key;
            type State = #state;
            type Input = #input_enum;
            type Event = #event_ty;

            fn sources<__S: ::domain::Sources<Self>>(sources: &mut __S) {
                #( sources.source::<#source_paths>(); )*
            }

            fn auto_emit(input: &#input_enum) -> ::std::option::Option<#event_ty> {
                #auto_emit
            }
        }

        impl ::domain::Source for #name {
            type SourceId = #key;
            type SourceEvent = #event_ty;
            type SourceState = #state;

            fn register_source<__R: ::domain::Registry>(registry: &mut __R) {
                registry.view::<Self>();
            }
        }

        #( #consumes )*

        const _: () = {
            #[allow(dead_code)]
            fn checks() {
                ::domain::__assert_apply::<#name>();
            }
        };
    })
}

fn model(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut aggregates, mut views) = (Vec::new(), Vec::new());
    parse_attr(input, "model", |meta| {
        if meta.path.is_ident("aggregates") {
            aggregates = paths(&meta)?;
        } else if meta.path.is_ident("views") {
            views = paths(&meta)?;
        } else {
            return Err(meta.error("expected `aggregates` or `views`"));
        }
        Ok(())
    })?;
    let tag = name.to_string();
    Ok(quote! {
        impl ::domain::Model for #name {
            const NAME: &'static str = #tag;

            fn register<__R: ::domain::Registry>(registry: &mut __R) {
                #( registry.aggregate::<#aggregates>(); )*
                #( registry.view::<#views>(); )*
            }
        }
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
    let kind_impls = match kind {
        Kind::Events => quote! {
            #(
                impl ::domain::Member<#members> for #name {
                    #[allow(unreachable_patterns)]
                    fn get(&self) -> ::std::option::Option<&#members> {
                        match self {
                            Self::#variants(member) => ::std::option::Option::Some(member),
                            _ => ::std::option::Option::None,
                        }
                    }
                }
            )*
        },
        Kind::Rejections => quote! {
            impl ::std::fmt::Display for #name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    match *self { #( Self::#variants(ref r) => ::std::fmt::Display::fmt(r, f), )* }
                }
            }

            impl ::std::error::Error for #name {}
        },
    };
    // Event sets are stored, so they serialize: `{"MoneyDeposited": {..}}`.
    let serde = match kind {
        Kind::Events => quote! {
            #[derive(::domain::__private::serde::Serialize, ::domain::__private::serde::Deserialize)]
            #[serde(crate = "::domain::__private::serde")]
        },
        Kind::Rejections => quote!(),
    };
    quote! {
        #[doc = #doc]
        #[derive(::std::clone::Clone, ::std::fmt::Debug, ::std::cmp::PartialEq)]
        #serde
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

        #kind_impls
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
