# NH-NNNN - <short title>

Ticket: NH-NNNN
Status: Draft            # Draft | In Progress | Verified | Quarantined
Owner: unassigned
Area: <domain/subdomain, e.g. write-path/cascade-delete>
Priority: <P0 | P1 | P2>
Quarantine allowed: no   # yes only with a Parity-audit DESIGN-EXCEPTION/CORRECT-REJECT
Named gate: <the exact command that proves this ticket>

## Problem

<What is broken or unproven, and why happy-path evidence is insufficient. State the
correctness/security/mobility risk concretely.>

## Non-negotiable scope

This ticket is not complete until the behaviour is proven, not merely described.

Allowed paths:

- <glob or path this ticket may change>
- tests/<...>
- docs/hardening/tickets/NH-NNNN.md

Acceptance criteria:

- [ ] <concrete, checkable outcome 1 — with the differential/oracle or live proof that shows it>
- [ ] <concrete, checkable outcome 2>
- [ ] <the named gate passes on a clean tree>

## Parity audit

Classify every restriction this ticket introduces or documents (or state "none"):

- [ ] IMPLEMENTATION-DEBT: <portable gap that stays open>
- [ ] DESIGN-EXCEPTION: <deliberate, audited>
- [ ] CORRECT-REJECT: <genuinely invalid / non-portable input>
- [ ] INTERNAL-INVARIANT: <owned elsewhere>

## Correctness-first audit

- [ ] Implementation attempted before any throw-pin / documented limitation
      (`feedback_implement_dont_pin_throws`).
- [ ] Proof is differential (oracle / LINQ-to-Objects / live provider), not happy-path only;
      an adversarial probe was added where the surface is correctness-sensitive.
- [ ] Behaviour compared against EF Core / Dapper / raw ADO.NET where relevant.

## Required verification

- <exact command 1 — e.g. `dotnet test tests/ --filter "FullyQualifiedName~<X>"`>
- <exact command 2 — e.g. a fuzzer seed sweep with its range>
- <a demonstrated failing / negative case where applicable>

## Reference app impact

<Does this make `samples/nORM.Sample.Store` and the live provider suite pass, or is that
deferred to another ticket? Name it.>

## Verification evidence

<Latest compact gate/test output per surface. Not a dated command diary; git history holds
detail. Example: `dotnet test … Passed! Failed: 0, Passed: 42`.>
