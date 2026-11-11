# Who is asking

Every command and read can carry who it's from: the actor. Commands check it in `decide`; views check it in `validate`.

```diagram
token ──▶ server's session check ──▶ actor ──▶ decide      may sam withdraw?
                                          └──▶ validate    may kim read this?
```

## Name your actor

Any type works:

```rust
pub struct User(pub String);
```

## Check it in a command

```rust
impl Decide<WithdrawMoney> for Account {
    fn decide(state: &AccountState, cmd: &WithdrawMoney, cx: &Cx) -> Decision<WithdrawMoney> {
        let balance = open(state)?;
        must_own(cx.actor::<User>(), owner(state))?;      // Err(NotOwner) for anyone else
        ..
    }
}

fn must_own(user: Option<&User>, owner: Option<&str>) -> Result<(), NotOwner> {
    match (user, owner) {
        (Some(User(name)), Some(owner)) if name != owner => Err(NotOwner),
        _ => Ok(()),
    }
}
```

`NotOwner` goes in the command's `rejects(..)`, like any rejection.

`cx.actor::<User>()` is `None` when nobody is signed in: a policy, a schedule, or code in the app. Decide what that means. The bank trusts it.

## Guard a view

List what a read can be refused with, then implement `Validate`:

```rust
#[domain::view(key = AccountId, from(Account(..)), state = Summary, rejects(NotOwner))]
pub struct AccountSummary;

impl Validate for AccountSummary {
    fn validate(cx: &ReadCx, _: &AccountId, summary: Option<&Summary>) -> Result<(), AccountSummaryRejection> {
        Ok(must_own(cx.actor::<User>(), summary.and_then(|s| s.owner.as_deref()))?)
    }
}
```

It runs on every read, and again on every change while someone watches:

| Read | When refused |
| --- | --- |
| `get`, `snapshot`, `list` | `Err(ReadError::Rejected(NotOwner))` |
| `live` | holds `Err(ReadError::Rejected(NotOwner))` and stops |
| `events` | ends with `Frame::Rejected(NotOwner)` |

```text
kim watches sam-main   Ok(None)                        not anyone's yet
sam opens it           Err(Rejected(NotOwner))         kim's watch ends
```

A view without `rejects(..)` lets everyone read it.

## Where the actor comes from

| Caller | Actor |
| --- | --- |
| A client connected to a [server](server.md) | what the server's session check returns for its token |
| `Client::in_process(..).acting_as(User(..))` | that user |
| `Memory::acting_as(User(..)).send(..)` | that user |
| A story's `.acting_as(User(..))` | that user, for the commands after it |
| A policy's `cx.acting_as(User(..)).send(..)` | that user |
| A saga step's `.acting_as(User(..))` | that user |
| Anything else | nobody: `None` |

## In a story

```rust
Story::new()
    .given(&a1(), (opened(), MoneyDeposited { amount: 100, balance_after: 100 }))
    .acting_as(User("kim".into()))
    .when(&a1(), WithdrawMoney { amount: 10 })
    .rejected(NotOwner)
    .acting_as(User("sam".into()))
    .when(&a1(), WithdrawMoney { amount: 10 })
    .then(&a1(), [MoneyWithdrawn { amount: 10, balance_after: 90 }]);
```

Next: [Serve it](server.md).
