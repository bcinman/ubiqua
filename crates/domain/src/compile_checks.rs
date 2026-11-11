//! Mistakes the compiler catches. Each example below must fail to compile; `cargo test --doc`
//! fails if one starts compiling.
//!
//! The model every example starts from compiles:
//!
//! ```
//! # use domain::*;
//! domain::id!(pub struct DoorId;);
//!
//! #[derive(Command, Debug, serde::Serialize, serde::Deserialize)]
//! #[command(target = DoorId, events(Closed), rejects(Jammed))]
//! pub struct Close;
//!
//! #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
//! pub struct Closed;
//!
//! #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
//! pub struct Opened;
//!
//! #[derive(Rejection, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
//! pub struct Jammed;
//!
//! #[derive(Rejection, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
//! }
//!
//! #[derive(Saga, serde::Serialize, serde::Deserialize)]
//! #[saga(key = DoorId, rejects(Jammed, Locked))]
//! pub struct Shut;
//!
//! // A step's rejections pass on with `?`.
//! fn step(refused: CloseRejection) -> Result<(), ShutRejection> {
//!     Err(refused)?
//! }
//! ```
//!
//! `decide` returns an event its command doesn't declare:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed), rejects(Jammed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Opened;
//! # #[derive(Rejection, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Jammed;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed, Opened), commands(Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door { fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { decide() } }
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
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed), rejects(Jammed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Rejection, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Jammed;
//! # #[derive(Rejection, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Locked;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands(Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door { fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { decide() } }
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
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
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
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Opened;
//! #[derive(Aggregate)]
//! #[aggregate(id = DoorId, state = bool, events(Opened), commands(Close))]
//! pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door {
//! #     fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![Closed.into()]) }
//! # }
//! ```
//!
//! An aggregate without `Evolve`:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! #[derive(Aggregate)]
//! #[aggregate(id = DoorId, state = bool, events(Closed), commands())]
//! pub struct Door;
//! ```
//!
//! A command declares a reply that nothing computes:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! #[derive(Command, Debug, serde::Serialize, serde::Deserialize)]
//! #[command(target = DoorId, events(Closed), reply = bool)]
//! pub struct Close;
//!
//! #[derive(Aggregate)]
//! #[aggregate(id = DoorId, state = bool, events(Closed), commands(Close))]
//! pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door {
//! #     fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![Closed.into()]) }
//! # }
//! // no impl Reply<Door> for Close
//! ```
//!
//! ## Views
//!
//! A view that reads two sources asks for `source_id`, which only a view with one source has:
//!
//! ```compile_fail,E0599
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # domain::id!(pub struct GateId;);
//! # #[domain::event] pub struct Closed;
//! # #[domain::event] pub struct Shut;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = (), events(Closed), commands())] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut (), _: &DoorEvent) {} }
//! # #[derive(Aggregate)] #[aggregate(id = GateId, state = (), events(Shut), commands())] pub struct Gate;
//! # impl Evolve for Gate { fn evolve(_: &mut (), _: &GateEvent) {} }
//! #[derive(View)]
//! #[view(key = DoorId, from(Door(Closed), Gate(Shut)), key_of(Gate))]
//! pub struct Log;
//! # impl KeyOf<Gate> for Log { fn key_of(_: Origin<'_, Gate>, _: &LogInput) -> Option<DoorId> { None } }
//!
//! impl Apply for Log {
//!     fn apply(_: &mut (), _: LogInput, cx: &mut ViewCx<Self>) {
//!         let door = cx.source_id(); // use `cx.source::<Door>()`
//!     }
//! }
//! ```
//!
//! A view that reads an event its source never records:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Opened;
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
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
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
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
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
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
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
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Opened;
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
//!
//! An Api that lists a command and a view compiles:
//!
//! ```
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands(Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door { fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![]) } }
//! # #[derive(View)] #[view(key = DoorId, from(Door(Closed)))] pub struct Log;
//! # impl Apply for Log { fn apply(_: &mut (), _: LogInput, _: &mut ViewCx<Self>) {} }
//! fn client_can<A: Sends<Close> + Reads<Log>>() {}
//!
//! #[derive(Api)]
//! #[api(commands(Close), views(Log))]
//! pub struct DoorApi;
//!
//! client_can::<DoorApi>();
//! ```
//!
//! A client sends a command the Api doesn't list:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands(Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door { fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![]) } }
//! # #[derive(View)] #[view(key = DoorId, from(Door(Closed)))] pub struct Log;
//! # impl Apply for Log { fn apply(_: &mut (), _: LogInput, _: &mut ViewCx<Self>) {} }
//! fn client_can_send<A: Sends<Close>>() {}
//!
//! #[derive(Api)]
//! #[api(commands(), views(Log))]
//! pub struct DoorApi;
//!
//! client_can_send::<DoorApi>();
//! ```
//!
//! A client reads a view the Api doesn't list:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands(Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door { fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![]) } }
//! # #[derive(View)] #[view(key = DoorId, from(Door(Closed)))] pub struct Log;
//! # impl Apply for Log { fn apply(_: &mut (), _: LogInput, _: &mut ViewCx<Self>) {} }
//! fn client_can_read<A: Reads<Log>>() {}
//!
//! #[derive(Api)]
//! #[api(commands(Close), views())]
//! pub struct DoorApi;
//!
//! client_can_read::<DoorApi>();
//! ```
//!
//! A view declares `rejects(..)` but doesn't say who it refuses:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Rejection, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct NotYours;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands(Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door { fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![]) } }
//! #[derive(View)]
//! #[view(key = DoorId, from(Door(Closed)), rejects(NotYours))]
//! pub struct Log;
//! # impl Apply for Log { fn apply(_: &mut (), _: LogInput, _: &mut ViewCx<Self>) {} }
//! // no impl Validate for Log
//! ```
//!
//! A view lists a command in `predicts(..)` but doesn't say how it changes the view:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Clone, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands(Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door { fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![]) } }
//! #[derive(View)]
//! #[view(key = DoorId, from(Door(Closed)), predicts(Close))]
//! pub struct Log;
//! # impl Apply for Log { fn apply(_: &mut (), _: LogInput, _: &mut ViewCx<Self>) {} }
//! // no impl Predict<Close> for Log
//! ```
//!
//! A saga's step can be refused in a way the saga doesn't list:
//!
//! ```compile_fail,E0277
//! # use domain::*;
//! # domain::id!(pub struct DoorId;);
//! # #[derive(Command, Debug, serde::Serialize, serde::Deserialize)] #[command(target = DoorId, events(Closed), rejects(Jammed))] pub struct Close;
//! # #[derive(Event, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Closed;
//! # #[derive(Rejection, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Jammed;
//! # #[derive(Rejection, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] pub struct Locked;
//! # #[derive(Aggregate)] #[aggregate(id = DoorId, state = bool, events(Closed), commands(Close))] pub struct Door;
//! # impl Evolve for Door { fn evolve(_: &mut bool, _: &DoorEvent) {} }
//! # impl Decide<Close> for Door { fn decide(_: &bool, _: &Close, _: &Cx) -> Decision<Close> { Ok(vec![]) } }
//! #[derive(Saga, serde::Serialize, serde::Deserialize)]
//! #[saga(key = DoorId, rejects(Locked))]
//! pub struct Shut;
//!
//! fn step(refused: CloseRejection) -> Result<(), ShutRejection> {
//!     Err(refused)? // Jammed isn't in `rejects(..)`
//! }
//! ```
