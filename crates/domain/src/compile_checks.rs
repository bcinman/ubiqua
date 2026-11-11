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
//!
//! ## Views
//!
//! A view that reads an event its source never records:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Opened;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands())] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! #[derive(View)]
//! #[view(key = DoorId, from(Door(Opened)))]
//! pub struct Log;
//! # impl Apply for Log { fn apply(_: &mut (), _: LogInput, _: &mut ViewCx<Self>) {} }
//! ```
//!
//! A view keyed by something other than its source's id, without `key_of`:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # domain::id!(pub struct HouseId;);
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands())] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! #[derive(View)]
//! #[view(key = HouseId, from(Door(Closed)))]
//! pub struct Log;
//! # impl Apply for Log { fn apply(_: &mut (), _: LogInput, _: &mut ViewCx<Self>) {} }
//! ```
//!
//! A view without `Apply`:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands())] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! #[derive(View)]
//! #[view(key = DoorId, from(Door(Closed)))]
//! pub struct Log;
//! ```
//!
//! The same view with `Apply` compiles:
//!
//! ```
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands())] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! #[derive(View)]
//! #[view(key = DoorId, from(Door(Closed)))]
//! pub struct Log;
//!
//! impl Apply for Log {
//!     fn apply(_: &mut (), _: LogInput, _: &mut ViewCx<Self>) {}
//! }
//! ```
//!
//! A view that reads an event another view doesn't emit:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Closed;
//! # #[derive(Event, Clone, Debug, PartialEq)] pub struct Opened;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed, Opened), commands())] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! // Reads Closed only, so emits Closed only.
//! #[derive(View)]
//! #[view(key = DoorId, from(Door(Closed)))]
//! pub struct Closings;
//! # impl Apply for Closings { fn apply(_: &mut (), _: ClosingsInput, _: &mut ViewCx<Self>) {} }
//!
//! #[derive(View)]
//! #[view(key = DoorId, from(Closings(Opened)))]
//! pub struct Log;
//! # impl Apply for Log { fn apply(_: &mut (), _: LogInput, _: &mut ViewCx<Self>) {} }
//! ```
