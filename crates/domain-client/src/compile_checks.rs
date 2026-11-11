//! Mistakes the compiler catches. Each example below must fail to compile; `cargo test --doc`
//! fails if one starts compiling.
//!
//! Sending a command the Api doesn't list names the Api and the command, even when the Api lists
//! one other command:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug)] #[command(target = DoorId, events(Moved))] pub struct Open;
//! # #[derive(Command, Debug)] #[command(target = DoorId, events(Moved))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Moved;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = (), events(Moved), commands(Open, Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut (), _: &DoorEvent) {} }
//! # impl Decide<Open> for Door { fn decide(_: &(), _: &Open, _: &Cx) -> Decision<Open> { Ok(vec![]) } fn reply(_: &()) {} }
//! # impl Decide<Close> for Door { fn decide(_: &(), _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![]) } fn reply(_: &()) {} }
//! # #[derive(Model)] #[model(aggregates(Door))] pub struct Doors;
//! #[derive(Api)]
//! #[api(commands(Open))]
//! pub struct DoorApi;
//!
//! let memory = std::sync::Arc::new(domain_memory::Memory::new().with::<Doors>());
//! let client = domain_client::Client::<DoorApi>::inline(memory);
//! client.send(&DoorId::new("front"), Close); // `DoorApi` doesn't let clients send `Close`
//! ```
