# Interceptor Contract

nORM exposes command and `SaveChanges` interceptors as stable v1 extension
points. Interceptors are privileged code: they can inspect commands, parameters,
tracked entities, and transactions.

## Command Interceptors

Command interceptors run for commands executed through nORM command helpers,
including queries, raw SQL, direct writes, bulk fallback commands, and migration
runners when options are supplied to the runner.

| Phase | Contract |
| --- | --- |
| Before execution | `*Executing*` hooks run in registration order. Interceptors may inspect and mutate `DbCommand` before execution. |
| Suppression | Returning `InterceptionResult<T>.SuppressWithResult` skips database execution and returns that result. Remaining `*Executing*` hooks are not called after suppression. nORM still calls the matching `*Executed*` hook for all registered interceptors with `TimeSpan.Zero`. |
| After success | `*Executed*` hooks run in registration order after command execution succeeds. |
| Failure | `CommandFailed*` hooks run in registration order when execution throws. The original exception is rethrown after hooks complete. |
| Cancellation | The caller cancellation token is passed to async hooks and command execution. Cancellation from the command path is treated as command failure and is rethrown. |
| Sync APIs | Synchronous execution paths call the synchronous interceptor hooks. They do not block on async hooks. |
| Transactions | Interceptors see the command after nORM has assigned the current transaction. Mutating transaction ownership is unsupported. |
| Logging | `BaseDbCommandInterceptor` redacts SQL literals and does not log parameter values, and logs **identically on the sync and async paths** (its sync and async hooks share one logging core). Custom interceptors own their own redaction policy. |

Command interceptors may change command text, timeout, parameters, and command
type before execution. They should not change the connection, replace the active
transaction, dispose the command, or retain mutable command/reader instances
after the callback returns.

For providers that prefer serialized synchronous execution on a shared
connection, nORM serializes the actual command execution. Pre-execution hooks run
before the serialization gate; post-execution hooks run after the command has
completed.

### Implement BOTH hook families (or a command-observing control fails open)

`IDbCommandInterceptor` has two hook families: async (`*Async`) and sync. **A command-observing
control — an audit log, a "record every write" trail, a "reject any command without a tenant
predicate" policy guard — must implement the sync member of every hook it implements.** The sync
default implementations are no-ops, and several execution paths run synchronously regardless of how
the calling code awaits:

- **SQLite** sets `PrefersSyncExecution`, so its commands run on the sync hooks;
- the **small-query fast paths**, **compiled queries**, and **query-plan** paths execute
  synchronously on every provider.

A guard implemented on the async members alone therefore silently passes the majority of traffic on
those paths — a security control that **fails open with no error**, which is worse than none. Either
implement both members of each hook, or derive from `BaseDbCommandInterceptor`: its sync and async
hooks delegate to one shared logging core, so the built-in redacted logging is emitted on **both**
paths and a base-derived interceptor is path-agnostic by default. When you add behavior to a subclass,
override **both** the sync and async member of the hook.

## SaveChanges Interceptors

`ISaveChangesInterceptor` runs around `SaveChangesAsync`.

| Phase | Contract |
| --- | --- |
| `SavingChangesAsync` | Runs before database writes. Interceptors receive a snapshot of tracked entries and may add or modify tracked entities. Exceptions fail the save before commit. |
| Commit | nORM writes changes and commits according to the transaction ownership contract. |
| `SavedChangesAsync` | Runs after a successful commit with `CancellationToken.None`, so caller cancellation after commit does not prevent post-commit notifications. |
| Post-commit failure | Exceptions from `SavedChangesAsync` are logged as warnings and do not roll back or change the already committed result. All post-commit interceptors are attempted. |

There is no v1 `SaveChangesFailedAsync` hook. Failed saves surface through the
original exception path, and any transaction owned by nORM is rolled back by the
normal transaction manager.

## Threading Rules

Interceptors are invoked sequentially for a single command or save operation.
nORM does not serialize independent contexts or unrelated commands except where
provider connection rules require it. Interceptor implementations that keep
state must be thread-safe when the same instance is registered on multiple
contexts or used concurrently.

## Test Evidence

The v1 gate covers command suppression, sync and async command hooks, command
mutation visibility, registration-order execution, command failure
notification, redacted base-interceptor logging, post-commit save interceptor
cancellation behavior, post-commit exception swallowing/logging, multiple save
interceptors, and save-interceptor mutation before commit.
