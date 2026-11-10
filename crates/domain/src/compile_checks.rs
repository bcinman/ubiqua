//! Mistakes the compiler catches. Each example below must fail to compile; `cargo test --doc`
//! fails if one starts compiling.
//!
//! The model every example starts from compiles:
//!
//! ```
//! # use domain::*;
//! domain::id!(pub struct DoorId;);
//!
//! #[derive(Command, Debug)]
//! #[command(target = DoorId, events(Closed), rejects(Jammed))]
//! pub struct Close;
//!
//! #[derive(Event, Clone, Debug, PartialEq)]
//! pub struct Closed;
//!
//! #[derive(Event, Clone, Debug, PartialEq)]
//! pub struct Opened;
//!
//! #[derive(Rejection, Clone, Debug, PartialEq)]
//! pub struct Jammed;
//!
//! #[derive(Rejection, Clone, Debug, PartialEq)]
//! pub struct Locked;
//!
//! #[derive(Aggregate)]
//! #[aggregate(id = DoorId, state = bool, events(Closed, Opened), commands(Close))]
//! pub struct Door;
//!
//! impl Evolve for Door {
//!     fn evolve(open: &mut bool, event: &DoorEvent) {
//!         *open = matches!(event, DoorEvent::Opened(_));
//!     }
//! }
//!
//! impl Decide<Close> for Door {
//!     fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> {
//!         Ok(vec![Closed.into()])
//!     }
//!     fn reply(_: &bool) {}
//! }
//! ```
//!
//! `decide` returns an event its command doesn't declare:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug)] #[command(target = DoorId, events(Closed), rejects(Jammed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Opened;
//! # #[derive(Rejection, Clone, Debug, PartialEq)] pub struct Jammed;
//! fn decide() -> Decision<Close> {
//!     Ok(vec![Opened.into()])
//! }
//! ```
//!
//! `decide` returns a rejection its command doesn't declare:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug)] #[command(target = DoorId, events(Closed), rejects(Jammed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! # #[derive(Rejection, Clone, Debug, PartialEq)] pub struct Jammed;
//! # #[derive(Rejection, Clone, Debug, PartialEq)] pub struct Locked;
//! fn decide() -> Decision<Close> {
//!     Err(Locked.into())
//! }
//! ```
//!
//! An aggregate lists a command it has no `Decide` for:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug)] #[command(target = DoorId, events(Closed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! #[derive(Aggregate)]
//! #[aggregate(id = DoorId, state = bool, events(Closed), commands(Close))]
//! pub struct Door;
//!
//! impl Evolve for Door {
//!     fn evolve(_: &mut bool, _: &DoorEvent) {}
//! }
//! ```
//!
//! A command records an event its aggregate doesn't list:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug)] #[command(target = DoorId, events(Closed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Opened;
//! #[derive(Aggregate)]
//! #[aggregate(id = DoorId, state = bool, events(Opened), commands(Close))]
//! pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door {
//! #     fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![Closed.into()]) }
//! #     fn reply(_: &bool) {}
//! # }
//! ```
//!
//! An aggregate without `Evolve`:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! #[derive(Aggregate)]
//! #[aggregate(id = DoorId, state = bool, events(Closed), commands())]
//! pub struct Door;
//! ```
