# nORM hardening tickets

Granular, **evidence-gated** hardening tickets that roll up into the 1.0 gate in
`docs/v1-sharpening/`. This system exists to kill one specific failure mode:

> A broad checklist item lets an agent implement the smallest visible fix, mark a box
> done, and leave the larger category broken.

A ticket is therefore **not closed by documentation**. Code, tests, and a named gate must
agree, and closure claims must carry real gate/test output — never prose.

## How it fits together

- `RELEASE.md` — the promise (shippable barring a showstopper; RCs earned, not scheduled).
- `docs/v1-sharpening/` — the rc1 **gate + 14-domain map + dashboard** (the top-level bar).
- `docs/hardening/tickets/NH-NNNN.md` — the **granular execution units**. Each domain in
  v1-sharpening decomposes into many subdomain tickets. A domain flips 🟢 in the dashboard
  only when all of its tickets are `Verified`.

## Ticket IDs

`NH-<domain><seq>` — a 4-digit number grouped by domain so tickets sort by area:

| Range | Domain (v1-sharpening) |
|-------|------------------------|
| NH-00xx | Process / gate (this system itself) |
| NH-01xx | Query & LINQ translation |
| NH-02xx | Write path & change tracking |
| NH-03xx | Bulk operations |
| NH-04xx | Schema & migrations |
| NH-05xx | Provider mobility |
| NH-06xx | Temporal & versioning |
| NH-07xx | Multi-tenancy |
| NH-08xx | Caching |
| NH-09xx | Resilience & concurrency |
| NH-10xx | Scaffolding & CLI |
| NH-11xx | AOT, trimming & source generation |
| NH-12xx | Public API, DI & documentation |
| NH-13xx | Performance & benchmarks |
| NH-14xx | Security & data protection |

## Status vocabulary

Ticket `Status:` — exactly one of:

- **Draft** — scoped but not started; cannot be relied on.
- **In Progress** — real partial closure; some acceptance criteria may be `[x]` with evidence.
- **Verified** — every acceptance criterion is `[x]`, evidence is present, and the named gate
  passes on a clean tree. Only `Verified` may be cited as done.
- **Quarantined** — a restriction is deliberately accepted; requires a Parity-audit
  classification (see below). Rare; nORM is enterprise-grade and avoids documented limitations.

Row markers inside a ticket:

- `[ ]` open · `[~]` partial (honest, with what remains stated) · `[x]` done **with evidence**.

## Closure rules (no overclaim)

1. `Status: Verified` is **forbidden** while any acceptance criterion is `[ ]` or `[~]`.
2. A `[x]` row must map to durable code + tests + a gate result; documentation alone never
   closes a row.
3. Verification evidence is the **latest compact gate/test output** per surface (e.g.
   `dotnet test … Passed! Failed: 0, Passed: N`), not a dated command diary. Detail lives in
   git history.
4. Every ticket cites its **named gate** — the exact command(s) that prove it (a focused
   filter, a fuzzer seed sweep, the live provider gate, an AOT scan). A change to code in the
   ticket's area with no matching gate result fails review.

## Correctness-first (nORM's implementation-first rule)

Mirrors the compiler's implementation-first audit, specialised for a data-correctness ORM:

- Prefer **implementing** a portable feature over throwing `NormUnsupportedFeatureException`.
  A throw-pin is a cop-out unless the shape is genuinely non-portable or invalid
  (see `feedback_implement_dont_pin_throws`).
- Correctness is proven **differentially**, not by happy-path assertion: against a
  LINQ-to-Objects / oracle model, and against real providers. Adversarial probes beat N
  passing happy-path probes (the caching stale-read bug survived ~10 passing probes).
- Behaviour parity is measured against **EF Core, Dapper, and raw ADO.NET** where relevant.

## Parity audit (classifying any restriction)

Any new restriction — a throw, a documented limitation, a "not supported" — must be classified:

- **IMPLEMENTATION-DEBT** — a real, portable gap that stays open (not a closure).
- **DESIGN-EXCEPTION** — deliberately out of scope; audited in the owning ticket + ledger.
- **CORRECT-REJECT** — genuinely invalid or non-portable input; rejecting it is correct.
- **INTERNAL-INVARIANT** — owned by another ticket / an internal guard, not a feature gap.

No feature may be closed as "unsupported" merely by adding an exception + a negative test.

## Restriction-language policy

New occurrences of `unsupported`, `not implemented`, `TODO`, `FIXME`, `deferred`, `rejected`,
`missing`, `non-goal`, or `limitation` in source (outside policy docs or negative tests) require
a Parity-audit classification in the owning ticket. This prevents quietly narrowing scope.

## Allowed paths & reference-app impact

- Each ticket declares **Allowed paths** — the files it may change. Changes outside fail the gate.
- Each ticket states its **Reference-app impact**: `samples/nORM.Sample.Store` and the live
  provider suite are the real-world gates; a ticket says whether it makes them pass or defers.

## Enforcement

> **Status of this process: In Progress — NOT yet enforced.** Per the rule this very document
> defines, the process is not "done" until a validator + git hook enforce it. That is ticket
> **NH-0001** (the nORM analogue of the compiler's MLH-0001). Until `scripts/validate_hardening_ticket.ps1`
> exists and a pre-commit/pre-push hook invokes it — with demonstrated failing runs (bad id,
> out-of-allowed-path, Verified-with-open-criteria, unclassified restriction) — this README is
> itself only `In Progress`. No overclaim, including here.

See `ticket-template.md` for the required fields.
