//! Who is signed in, and what they may touch: only their own accounts.

/// A signed-in user: an account owner's name. The server's session check produces it; an
/// in-process client takes it from `acting_as`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User(pub String);

/// The account belongs to someone else.
#[domain::rejection]
pub struct NotOwner;

/// `user` may act on what `owner` owns. Nobody signed in means code in the app, which may.
pub fn must_own(user: Option<&User>, owner: Option<&str>) -> Result<(), NotOwner> {
    match (user, owner) {
        (Some(User(name)), Some(owner)) if name != owner => Err(NotOwner),
        _ => Ok(()),
    }
}
