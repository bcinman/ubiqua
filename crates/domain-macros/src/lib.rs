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
    event_impl(&input).into()
}

/// `impl Rejection`, plus `Display` (its `Debug` form) and `std::error::Error`.
#[proc_macro_derive(Rejection)]
pub fn derive_rejection(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    rejection_impl(&input).into()
}

/// `#[domain::event]`: `#[derive(Event)]` plus the derives `Event` requires: `Clone`, `Debug`,
/// `PartialEq`, `Serialize` and `Deserialize`.
#[proc_macro_attribute]
pub fn event(args: TokenStream, item: TokenStream) -> TokenStream {
    declaration(args, item, "event", |input, _| Ok(event_impl(input)))
}

/// `#[domain::rejection]`: `#[derive(Rejection)]` plus the derives `Rejection` requires: `Clone`,
/// `Debug`, `PartialEq`, `Serialize` and `Deserialize`.
#[proc_macro_attribute]
pub fn rejection(args: TokenStream, item: TokenStream) -> TokenStream {
    declaration(args, item, "rejection", |input, _| Ok(rejection_impl(input)))
}

/// `#[domain::command(target = Id, events(..), rejects(..), reply = Type)]`: `#[derive(Command)]`
/// with those arguments, plus the derives `Command` requires: `Debug`, `Serialize` and
/// `Deserialize`.
#[proc_macro_attribute]
pub fn command(args: TokenStream, item: TokenStream) -> TokenStream {
    declaration(args, item, "command", |input, args| {
        let mut input = input.clone();
        input.attrs.push(syn::parse_quote!(#[command(#args)]));
        command_impl(&input)
    })
}

/// Expands an attribute form: the item with the std and serde derives its trait needs, then what
/// the derive would generate. It doesn't emit the derive itself, so a `#[command(..)]` helper
/// can't clash with the `command` attribute when both are in scope.
fn declaration(
    args: TokenStream,
    item: TokenStream,
    kind: &str,
    generate: impl FnOnce(&DeriveInput, Tokens) -> syn::Result<Tokens>,
) -> TokenStream {
    let args = Tokens::from(args);
    let input = parse_macro_input!(item as DeriveInput);
    if kind != "command" && !args.is_empty() {
        let message = format!("`#[domain::{kind}]` takes no arguments");
        return syn::Error::new_spanned(args, message).into_compile_error().into();
    }
    let derives = match kind {
        "command" => quote!(::std::fmt::Debug),
        _ => quote!(::std::clone::Clone, ::std::fmt::Debug, ::std::cmp::PartialEq),
    };
    let generated = generate(&input, args).unwrap_or_else(syn::Error::into_compile_error);
    quote! {
        #[derive(#derives, ::domain::__private::serde::Serialize, ::domain::__private::serde::Deserialize)]
        #[serde(crate = "::domain::__private::serde")]
        #input
        #generated
    }
    .into()
}

fn event_impl(input: &DeriveInput) -> Tokens {
    let name = &input.ident;
    let tag = name.to_string();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    quote! {
        impl #impl_generics ::domain::Event for #name #ty_generics #where_clause {
            const NAME: &'static str = #tag;
        }
    }
}

fn rejection_impl(input: &DeriveInput) -> Tokens {
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
}

/// `#[command(target = Id, events(..), rejects(..), reply = Type)]`
///
/// Generates `<Name>Event` and `<Name>Rejection` enums and `impl Command`. `rejects` and `reply`
/// are optional; without `reply` the reply is `()`, and `impl Reply` for every aggregate.
#[proc_macro_derive(Command, attributes(command))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    command_impl(&input).unwrap_or_else(syn::Error::into_compile_error).into()
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

fn command_impl(input: &DeriveInput) -> syn::Result<Tokens> {
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
    let reply_type = reply.as_ref().map_or_else(|| quote!(()), |t| quote!(#t));
    let tag = name.to_string();
    let event_enum = format_ident!("{}Event", name);
    let rejection_enum = format_ident!("{}Rejection", name);
    let events_doc = format!("The events `{name}` may record.");
    let rejections_doc = format!("The reasons `{name}` may be refused.");

    let event_union = union(&input.vis, &event_enum, &events, &events_doc, Kind::Events);
    let rejection_union = union(&input.vis, &rejection_enum, &rejects, &rejections_doc, Kind::Rejections);
    let variants: Vec<&Ident> = events.iter().map(last_ident).collect();
    let rejection_variants: Vec<&Ident> = rejects.iter().map(last_ident).collect();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let never_rejects = rejects
        .is_empty()
        .then(|| quote!(impl #impl_generics ::domain::NeverRejects for #name #ty_generics #where_clause {}));
    // Without `reply = ..` the reply is `()` on every aggregate, so nobody writes an empty one.
    let unit_reply = reply.is_none().then(|| {
        let mut generics = input.generics.clone();
        generics.params.push(syn::parse_quote!(__A: ::domain::Aggregate));
        let (impl_generics, _, _) = generics.split_for_impl();
        quote! {
            impl #impl_generics ::domain::Reply<__A> for #name #ty_generics #where_clause {
                fn reply(_: &<__A as ::domain::Aggregate>::State) {}
            }
        }
    });

    Ok(quote! {
        #event_union
        #rejection_union
        #never_rejects
        #unit_reply

        impl<__E> ::domain::host::Widen<__E> for #event_enum
        where #( __E: ::std::convert::From<#events>, )*
        {
            fn widen(self) -> __E {
                match self { #( Self::#variants(e) => __E::from(e), )* }
            }
        }

        impl<__R> ::domain::host::WidenRejection<__R> for #rejection_enum
        where #( __R: ::std::convert::From<#rejects>, )*
        {
            fn widen(self) -> __R {
                match self { #( Self::#rejection_variants(r) => __R::from(r), )* }
            }
        }

        impl #impl_generics ::domain::Command for #name #ty_generics #where_clause {
            const NAME: &'static str = #tag;
            type Target = #target;
            type Event = #event_enum;
            type Rejection = #rejection_enum;
            type Reply = #reply_type;
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

            fn register_commands<__R: ::domain::host::Registry>(registry: &mut __R) {
                #( registry.command::<#commands>(); )*
            }
        }

        impl ::domain::AggregateId for #id {
            type Aggregate = #name;
        }

        impl ::domain::Source for #name {
            type SourceId = #id;
            type SourceEvent = #event_enum;
            type SourceState = ();

            fn register_source<__R: ::domain::host::Registry>(registry: &mut __R) {
                registry.aggregate::<Self>();
            }
        }

        const _: () = {
            #[allow(dead_code)]
            fn checks() {
                ::domain::__assert_evolve::<#name>();
                #( ::domain::__assert_reply::<#name, #commands>(); )*
                #( ::domain::__assert_handles::<#name, #commands>(); )*
            }
        };
    })
}

/// `#[view(key = Key, from(Source(Event, ..), ..), state = State, events(..), key_of(Source, ..), rejects(..), predicts(..), version = N)]`
///
/// Generates `<Name>Input` (the events it reads), `<Name>Event` when `events(..)` is given,
/// `<Name>Rejection`, `impl View`, and `impl Consumes` per source. With `rejects(..)` you implement
/// `Validate`; without, every read is allowed. `state` defaults to `()`. A source listed in
/// `key_of(..)` is routed by `impl KeyOf<Source>`; any other source's id must be the key. Each
/// command in `predicts(..)` needs an `impl Predict<Command>`.
#[proc_macro_derive(View, attributes(view))]
pub fn derive_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    view(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// `#[model(aggregates(..), views(..), policies(..), sagas(..))]`: `impl Model`, registering each
/// aggregate and view, and `impl Hosts`, handing a host each policy and saga.
#[proc_macro_derive(Model, attributes(model))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    model(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// `#[api(commands(..), views(..))]`: `impl Api`, registering each one listed, plus `Sends` and
/// `Reads` for each.
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

            fn register<__R: ::domain::host::ApiRegistry>(registry: &mut __R) {
                #( registry.command::<#commands>(); )*
                #( registry.view::<#views>(); )*
            }
        }
        #( impl ::domain::Sends<#commands> for #name {} )*
        #( impl ::domain::Reads<#views> for #name {} )*
    })
}

fn view(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut key, mut state, mut sources, mut events, mut key_of) = (None, None, Vec::new(), None, Vec::new());
    let (mut version, mut rejects, mut predicts) = (None, Vec::new(), Vec::new());
    parse_attr(input, "view", |meta| {
        if meta.path.is_ident("key") {
            key = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("state") {
            state = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("from") {
            sources = parse_from(&meta, "view")?;
        } else if meta.path.is_ident("events") {
            events = Some(paths(&meta)?);
        } else if meta.path.is_ident("key_of") {
            key_of = paths(&meta)?;
        } else if meta.path.is_ident("version") {
            version = Some(meta.value()?.parse::<syn::LitInt>()?.base10_parse::<u64>()?);
        } else if meta.path.is_ident("rejects") {
            rejects = paths(&meta)?;
        } else if meta.path.is_ident("predicts") {
            predicts = paths(&meta)?;
        } else {
            return Err(
                meta.error("expected `key`, `from`, `state`, `events`, `key_of`, `rejects`, `predicts` or `version`")
            );
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

    let reads = reads_of(&sources);
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
    let keyed = keyed(name, &key, &sources, &key_of, &input_enum);

    let rejection_enum = format_ident!("{}Rejection", name);
    let rejection_doc = format!("Why a read of `{name}` may be refused.");
    let rejection_union = union(&input.vis, &rejection_enum, &rejects, &rejection_doc, Kind::Rejections);
    // Without `rejects(..)` a view can't refuse anyone, so every read is allowed.
    let validate = if rejects.is_empty() {
        quote! {
            impl ::domain::Validate for #name {
                fn validate(
                    _: &::domain::ReadCx<'_>,
                    _: &#key,
                    _: ::std::option::Option<&#state>,
                ) -> ::std::result::Result<(), #rejection_enum> {
                    ::std::result::Result::Ok(())
                }
            }
        }
    } else {
        quote!(const _: () = { #[allow(dead_code)] fn validates() { ::domain::__assert_validate::<#name>(); } };)
    };

    let one_source = one_source(name, &sources);
    Ok(quote! {
        #input_union
        #event_union
        #rejection_union
        #validate
        #keyed
        #one_source

        impl ::domain::View for #name {
            const NAME: &'static str = #tag;
            #version
            type State = #state;
            type Event = #event_ty;
            type Rejection = #rejection_enum;

            fn sources<__S: ::domain::host::Sources<Self>>(sources: &mut __S) {
                #( sources.source::<#source_paths>(); )*
            }

            fn predictions<__P: ::domain::host::Predictions<Self>>(predictions: &mut __P) {
                let _ = &predictions;
                #( predictions.predicts::<#predicts>(); )*
            }

            fn auto_emit(input: &#input_enum) -> ::std::option::Option<#event_ty> {
                #auto_emit
            }
        }

        impl ::domain::Source for #name {
            type SourceId = #key;
            type SourceEvent = #event_ty;
            type SourceState = #state;

            fn register_source<__R: ::domain::host::Registry>(registry: &mut __R) {
                registry.view::<Self>();
            }
        }

        const _: () = {
            #[allow(dead_code)]
            fn checks() {
                ::domain::__assert_apply::<#name>();
            }
        };
    })
}

/// `#[projection(key = Key, from(Source(Event, ..), ..), key_of(Source, ..), version = N)]`
///
/// Generates `<Name>Input` (the events it reads), `impl Projection`, and `impl Consumes` per
/// source, routed as a view's are.
#[proc_macro_derive(Projection, attributes(projection))]
pub fn derive_projection(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    projection(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn projection(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut key, mut sources, mut key_of, mut version) = (None, Vec::new(), Vec::new(), None);
    parse_attr(input, "projection", |meta| {
        if meta.path.is_ident("key") {
            key = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("from") {
            sources = parse_from(&meta, "projection")?;
        } else if meta.path.is_ident("key_of") {
            key_of = paths(&meta)?;
        } else if meta.path.is_ident("version") {
            version = Some(meta.value()?.parse::<syn::LitInt>()?.base10_parse::<u64>()?);
        } else {
            return Err(meta.error("expected `key`, `from`, `key_of` or `version`"));
        }
        Ok(())
    })?;
    let key = key.ok_or_else(|| syn::Error::new_spanned(name, "a projection needs `#[projection(key = .., ..)]`"))?;
    if sources.is_empty() {
        return Err(syn::Error::new_spanned(name, "a projection needs `#[projection(from(Source(Event, ..)), ..)]`"));
    }
    for path in &key_of {
        if !sources.iter().any(|(source, _)| last_ident(source) == last_ident(path)) {
            return Err(syn::Error::new_spanned(path, "`key_of` names a source that isn't in `from(..)`"));
        }
    }
    let tag = name.to_string();
    let version = version.map(|v| quote!(const VERSION: u64 = #v;));
    let input_enum = format_ident!("{}Input", name);
    let doc = format!("The events `{name}` reads.");
    let input_union = union(&input.vis, &input_enum, &reads_of(&sources), &doc, Kind::Events);
    let keyed = keyed(name, &key, &sources, &key_of, &input_enum);
    let source_paths: Vec<&Path> = sources.iter().map(|(source, _)| source).collect();
    Ok(quote! {
        #input_union
        #keyed

        impl ::domain::Projection for #name {
            const NAME: &'static str = #tag;
            #version

            fn sources<__S: ::domain::host::Sources<Self>>(sources: &mut __S) {
                #( sources.source::<#source_paths>(); )*
            }
        }
    })
}

/// `#[saga(key = Key, rejects(..), sends(..))]`
///
/// Generates `<Name>Rejection` and `impl Saga`. For each command in `sends(..)`, its rejections
/// convert into the saga's with `?`; the saga's `rejects(..)` must include them all.
#[proc_macro_derive(Saga, attributes(saga))]
pub fn derive_saga(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    saga(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn saga(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut key, mut rejects, mut sends) = (None, Vec::new(), Vec::new());
    parse_attr(input, "saga", |meta| {
        if meta.path.is_ident("key") {
            key = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("rejects") {
            rejects = paths(&meta)?;
        } else if meta.path.is_ident("sends") {
            sends = paths(&meta)?;
        } else {
            return Err(meta.error("expected `key`, `rejects` or `sends`"));
        }
        Ok(())
    })?;
    let key = key.ok_or_else(|| syn::Error::new_spanned(name, "a saga needs `#[saga(key = .., ..)]`"))?;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(&input.generics, "a saga can't be generic"));
    }
    let tag = name.to_string();
    let rejection_enum = format_ident!("{}Rejection", name);
    let doc = format!("Why a run of `{name}` may be refused.");
    let rejection_union = union(&input.vis, &rejection_enum, &rejects, &doc, Kind::Rejections);
    Ok(quote! {
        #rejection_union

        impl ::domain::Saga for #name {
            const NAME: &'static str = #tag;
            type Key = #key;
            type Rejection = #rejection_enum;
        }

        #(
            impl ::std::convert::From<<#sends as ::domain::Command>::Rejection> for #rejection_enum {
                fn from(rejection: <#sends as ::domain::Command>::Rejection) -> Self {
                    ::domain::host::WidenRejection::widen(rejection)
                }
            }
        )*
    })
}

/// `#[policy(from(Source(Event, ..), ..))]`
///
/// Generates `<Name>Input` (the events it reacts to), `impl Policy`, and `impl Reacts` per source.
#[proc_macro_derive(Policy, attributes(policy))]
pub fn derive_policy(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    policy(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn policy(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let mut sources = Vec::new();
    parse_attr(input, "policy", |meta| {
        if meta.path.is_ident("from") {
            sources = parse_from(&meta, "policy")?;
            Ok(())
        } else {
            Err(meta.error("expected `from`"))
        }
    })?;
    if sources.is_empty() {
        return Err(syn::Error::new_spanned(name, "a policy needs `#[policy(from(Source(Event, ..)))]`"));
    }
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(&input.generics, "a policy can't be generic"));
    }
    let tag = name.to_string();
    let input_enum = format_ident!("{}Input", name);
    let doc = format!("The events `{name}` reacts to.");
    let input_union = union(&input.vis, &input_enum, &reads_of(&sources), &doc, Kind::Events);
    let source_paths: Vec<&Path> = sources.iter().map(|(source, _)| source).collect();
    let one_source = one_source(name, &sources);
    let reacts = sources.iter().map(|(source, list)| {
        let pick = pick_input(source, list, &input_enum);
        quote! {
            impl ::domain::host::Reacts<#source> for #name {
                #[allow(clippy::question_mark)]
                fn route(
                    event: &<#source as ::domain::Source>::SourceEvent,
                ) -> ::std::option::Option<#input_enum> {
                    ::std::option::Option::Some(#pick)
                }
            }
        }
    });
    Ok(quote! {
        #input_union

        impl ::domain::Policy for #name {
            const NAME: &'static str = #tag;
            type Input = #input_enum;

            fn sources<__S: ::domain::host::PolicySources<Self>>(sources: &mut __S) {
                #( sources.source::<#source_paths>(); )*
            }
        }

        #( #reacts )*
        #one_source
    })
}

/// `impl OneSource` when `from(..)` lists one source.
fn one_source(name: &Ident, sources: &[(Path, Vec<Path>)]) -> Option<Tokens> {
    let [(source, _)] = sources else { return None };
    Some(quote! {
        impl ::domain::host::OneSource for #name {
            type Source = #source;
        }
    })
}

/// `#[call(system = System, returns = Type, errors(..))]`
///
/// Generates `<Name>Error` and `impl Call`. `returns` defaults to `()`, `errors` to none.
#[proc_macro_derive(Call, attributes(call))]
pub fn derive_call(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    call(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn call(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut system, mut returns, mut errors) = (None, None, Vec::new());
    parse_attr(input, "call", |meta| {
        if meta.path.is_ident("system") {
            system = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("returns") {
            returns = Some(meta.value()?.parse::<Type>()?);
        } else if meta.path.is_ident("errors") {
            errors = paths(&meta)?;
        } else {
            return Err(meta.error("expected `system`, `returns` or `errors`"));
        }
        Ok(())
    })?;
    let system = system.ok_or_else(|| syn::Error::new_spanned(name, "a call needs `#[call(system = .., ..)]`"))?;
    let returns = returns.map_or_else(|| quote!(()), |t| quote!(#t));
    let tag = name.to_string();
    let error_enum = format_ident!("{}Error", name);
    let doc = format!("The refusals `{name}` may be answered with.");
    let error_union = union(&input.vis, &error_enum, &errors, &doc, Kind::Rejections);
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #error_union

        impl #impl_generics ::domain::Call for #name #ty_generics #where_clause {
            const NAME: &'static str = #tag;
            const SYSTEM: &'static str = <#system as ::domain::ExternalSystem>::NAME;
            type Returns = #returns;
            type Error = #error_enum;
        }
    })
}

/// `#[external_system(calls(..))]`: `impl ExternalSystem`, and `impl Adapter<I>` for every `I`
/// that answers each call.
#[proc_macro_derive(ExternalSystem, attributes(external_system))]
pub fn derive_external_system(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    external_system(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn external_system(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let mut calls = Vec::new();
    parse_attr(input, "external_system", |meta| {
        if meta.path.is_ident("calls") {
            calls = paths(&meta)?;
            Ok(())
        } else {
            Err(meta.error("expected `calls`"))
        }
    })?;
    let tag = name.to_string();
    Ok(quote! {
        impl ::domain::ExternalSystem for #name {
            const NAME: &'static str = #tag;
        }

        impl<__I> ::domain::host::Adapter<__I> for #name
        where #( __I: ::domain::Answer<#calls>, )*
        {
            fn register<__R: ::domain::host::Calls>(adapter: ::std::sync::Arc<__I>, calls: &mut __R) {
                let _ = &adapter;
                #( calls.answer::<#calls, __I>(::std::clone::Clone::clone(&adapter)); )*
            }
        }
    })
}

/// `from(Source(Event, ..), ..)`: each source with the events read from it.
fn parse_from(meta: &ParseNestedMeta, what: &str) -> syn::Result<Vec<(Path, Vec<Path>)>> {
    let mut sources = Vec::new();
    meta.parse_nested_meta(|source| {
        let reads = paths(&source)?;
        if reads.is_empty() {
            return Err(source.error(format!("list the events the {what} reads from this source")));
        }
        sources.push((source.path.clone(), reads));
        Ok(())
    })?;
    Ok(sources)
}

/// Every event read from any source, once each, in the order first listed.
fn reads_of(sources: &[(Path, Vec<Path>)]) -> Vec<Path> {
    let mut reads: Vec<Path> = Vec::new();
    for (_, list) in sources {
        for path in list {
            if !reads.iter().any(|r| last_ident(r) == last_ident(path)) {
                reads.push(path.clone());
            }
        }
    }
    reads
}

/// An expression turning `event`, from `source`, into the input enum, or returning `None`.
/// `impl Keyed`, and `impl Consumes` per source: a source in `key_of` is routed by its `KeyOf`;
/// any other's id is the key.
fn keyed(name: &Ident, key: &Type, sources: &[(Path, Vec<Path>)], key_of: &[Path], input_enum: &Ident) -> Tokens {
    let consumes = sources.iter().map(|(source, list)| {
        let pick = pick_input(source, list, input_enum);
        let key_expr = if key_of.iter().any(|path| last_ident(path) == last_ident(source)) {
            quote!(<Self as ::domain::KeyOf<#source>>::key_of(::domain::Origin { id, state }, &input)?)
        } else {
            quote_spanned!(source.span() =>
                <#key as ::domain::host::KeyFrom<<#source as ::domain::Source>::SourceId>>::key_from(id))
        };
        quote! {
            impl ::domain::host::Consumes<#source> for #name {
                #[allow(clippy::question_mark)]
                fn route(
                    id: &<#source as ::domain::Source>::SourceId,
                    event: &<#source as ::domain::Source>::SourceEvent,
                    state: &<#source as ::domain::Source>::SourceState,
                ) -> ::std::option::Option<(#key, #input_enum)> {
                    let _ = state;
                    let input = #pick;
                    let key = #key_expr;
                    ::std::option::Option::Some((key, input))
                }
            }
        }
    });
    quote! {
        impl ::domain::host::Keyed for #name {
            type Key = #key;
            type Input = #input_enum;
        }
        #( #consumes )*
    }
}

fn pick_input(source: &Path, list: &[Path], input_enum: &Ident) -> Tokens {
    let variants = list.iter().map(last_ident);
    quote! {
        #(
            if let ::std::option::Option::Some(e) =
                <<#source as ::domain::Source>::SourceEvent as ::domain::host::Member<#list>>::get(event)
            {
                #input_enum::#variants(::std::clone::Clone::clone(e))
            } else
        )* {
            return ::std::option::Option::None;
        }
    }
}

fn model(input: &DeriveInput) -> syn::Result<Tokens> {
    let name = &input.ident;
    let (mut aggregates, mut views, mut policies, mut sagas) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    parse_attr(input, "model", |meta| {
        if meta.path.is_ident("aggregates") {
            aggregates = paths(&meta)?;
        } else if meta.path.is_ident("views") {
            views = paths(&meta)?;
        } else if meta.path.is_ident("policies") {
            policies = paths(&meta)?;
        } else if meta.path.is_ident("sagas") {
            sagas = paths(&meta)?;
        } else {
            return Err(meta.error("expected `aggregates`, `views`, `policies` or `sagas`"));
        }
        Ok(())
    })?;
    let tag = name.to_string();
    Ok(quote! {
        impl ::domain::Model for #name {
            const NAME: &'static str = #tag;

            fn register<__R: ::domain::host::Registry>(registry: &mut __R) {
                #( registry.aggregate::<#aggregates>(); )*
                #( registry.view::<#views>(); )*
            }
        }

        impl<__H> ::domain::host::Hosts<__H> for #name
        where
            #( __H: ::domain::host::HostPolicy<#policies>, )*
            #( __H: ::domain::host::HostSaga<#sagas>, )*
        {
            fn host(host: __H) -> __H {
                #( let host = ::domain::host::HostPolicy::<#policies>::host_policy(host); )*
                #( let host = ::domain::host::HostSaga::<#sagas>::host_saga(host); )*
                host
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
                impl ::domain::host::Member<#members> for #name {
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
    // Events are stored and rejections cross the network, so both serialize: `{"MoneyDeposited": {..}}`.
    quote! {
        #[doc = #doc]
        #[derive(::std::clone::Clone, ::std::fmt::Debug, ::std::cmp::PartialEq)]
        #[derive(::domain::__private::serde::Serialize, ::domain::__private::serde::Deserialize)]
        #[serde(crate = "::domain::__private::serde")]
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
