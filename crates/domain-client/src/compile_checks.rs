//! Mistakes the compiler catches. Each example below must fail to compile; `cargo test --doc`
//! fails if one starts compiling.
//!
//! Sending a command the Api doesn't list names the Api and the command, even when the Api lists
//! one other command:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Moved))] pub struct Open;
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Moved))] pub struct Close;
//! # #[domain::event] pub struct Moved;
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
//!
//! Scheduling a command that can be rejected: nobody waits for a scheduled command's answer.
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[domain::event] pub struct Closed;
//! # #[domain::rejection] pub struct Jammed;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = (), events(Closed), commands(Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut (), _: &DoorEvent) {} }
//! # impl Decide<Close> for Door { fn decide(_: &(), _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![]) } fn reply(_: &()) {} }
//! #[domain::command(target = DoorId, events(Closed), rejects(Jammed))]
//! pub struct Close;
//!
//! let memory = domain_memory::Memory::new();
//! memory.schedule(&DoorId::new("front"), Close, Timestamp::from_millis(0), "close-1"); // `Close` can be rejected
//! ```
//!
//! An adapter that doesn't answer every call of its system:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! #[derive(Call, Debug, PartialEq)] #[call(system = Locks)] pub struct Lock;
//! #[derive(Call, Debug, PartialEq)] #[call(system = Locks)] pub struct Unlock;
//!
//! #[derive(ExternalSystem)]
//! #[external_system(calls(Lock, Unlock))]
//! pub struct Locks;
//!
//! pub struct HalfLocks;
//! impl Answer<Lock> for HalfLocks {
//!     async fn answer(&self, _: Lock) -> Answered<Lock> { Ok(()) }
//! }
//!
//! domain_memory::Memory::new().external(Locks, HalfLocks); // `HalfLocks` doesn't answer `Unlock`
//! ```
