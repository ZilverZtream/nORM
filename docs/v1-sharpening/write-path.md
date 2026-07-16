# Domain 2 — Write path & change tracking

**Scope:** `SaveChangesAsync`, the direct-write API (`InsertAsync`/`UpdateAsync`/`DeleteAsync`),
change tracking, identity map, cascade delete, relationship fixup, and graph writes.

## 1.0 exit criteria

- [ ] The CRUD state-machine fuzzer (explicit-key, generated-key, and relationship machines;
      keys / relationship-graphs / OCC-interleavings / fault injection vs SQLite + a live server)
      runs **dry for a sustained window** with zero new correctness kills. (NH-0201: dry on the
      current tree - 59 tests; the sustained multi-week window is the cross-cutting bar, open.)
- [x] No silent data loss on any write path: delete-then-re-add, detached-update,
      nav-retarget/reparent cascade, required-nav clear, tracked-dependent cascade, and
      direct+bulk tracker sync are all covered and green (NH-0201).
- [x] Cascade semantics are exact: `CascadeMarkDeletedDependents` (mark by FK/nav) and the
      accept-phase `CascadeDelete` (detach graph members) only ever touch entities that
      genuinely belong to the principal (NH-0201; KILL 40/42 regressions).
- [x] Relationship fixup covers both directions (collection add and reference/graph add),
      pending db-generated-key fixups, and FK-edit-outranks-stale-nav (NH-0201).
- [x] The direct-write vs tracked-write mental model is documented (when to use which, and the
      hazards) — see Domain 12 (NH-1201, `docs/write-model.md`).

## Current confidence

Strong. This is the most heavily fuzzed area: kills #1–#42 in the ledger, including the recent
cascade-reparent (KILL 40), required-nav-clear crash (KILL 41), and re-parented-child
cascade-DETACH tracking-loss (KILL 42 — root cause of all prior residuals). The relationship
machine is fully green; a 1600-seed sweep post-KILL-42 was clean.

## Open items

- [~] Sustain the state-machine fuzzer dry window across all three machines; record seed ranges.
      (Env-directed sweeps feed `docs/v1-sharpening/fuzzer-dry-log.md` via
      `NORM_CRUD_FUZZ_SWEEP="start:count"` — every sweep runs all three machines per seed;
      relationship-machine seeds 600000-601600 clean post-KILL-42, plus 602000+ swept dry
      2026-07-16. The sustained window accumulates in the log.)
- [x] Direct-vs-tracked write-model guidance doc written (NH-1201): `docs/write-model.md`.
- [x] Retry-write invariants (Domain 9) hold under fault injection (NH-0201,
      `SaveChangesFaultInjectionAtomicity`): reset rolled-back db-generated keys on retry; never
      retry past commit-attempted.

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~CrudStateMachineFuzzTests"`
- `dotnet test tests/ --filter "FullyQualifiedName~CascadeDeleteReparentedChildTests|FullyQualifiedName~RequiredReferenceNavClearTests"`
- Live write parity via the release gate's live provider phase.

## Risks

"Isolated repro passes" ≠ harness bug — isolation can drop the triggering op (KILL 42's
parent-delete). Instrument the real run via `ctx.ChangeTracker.Entries` before concluding a
fuzzer false-positive.
