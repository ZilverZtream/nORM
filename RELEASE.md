# Release policy

nORM is currently **0.x — pre-1.0**: usable and actively developed, but the
public API and behaviour may still change, and it is **not yet certified for
production data**. Use it, report bugs, and pin your version.

## Why 0.x (and why not `1.0.0-rc`)

An earlier series of `1.0.0-rc.N` tags (rc.4–rc.7) was cut prematurely. Each was
labelled a *release candidate* while the write path was still surfacing
correctness defects — for example a re-parented child silently losing change
tracking on cascade, cascade delete following the wrong relationship, and a
crash when clearing a required reference navigation (all fixed; see git history).

A release candidate is a claim that a build is **shippable barring a showstopper**.
Those builds were not, so the label was misleading, and cutting a new RC every
day or two turned the label into noise.

The project has reset to honest pre-1.0 versioning:

- `NormVersion` is `0.9.0`.
- The premature `rc.7` tag (never published) has been removed.
- `rc.4`–`rc.6` remain in history but are **superseded — do not treat them as
  release candidates**.

## The bar for the *first real* release candidate

`1.0.0-rc.1` is cut only when **all** of the following hold — not before:

1. **Correctness is settled.** The differential/oracle fuzzers (read-path parity,
   the CRUD state machines, bulk CUD, OCC interleaving, temporal reconstruction,
   migration data-preservation) run **dry for a sustained window — on the order
   of weeks of active development with zero new correctness kills** — not "clean
   today after fixing three data-loss bugs today."
2. **The public API is frozen and documented.** No planned breaking changes; the
   API-snapshot baseline is intentional; XML docs and a real getting-started exist.
3. **Real-world validation.** A handful of non-synthetic workloads / external
   users have run it against real databases without a correctness surprise.
4. **No known correctness defects**, and the release gate is green on the exact
   tagged commit.

When a build genuinely clears this bar, `-rc` is used again — and an RC means
"we intend to ship this." If an RC reveals a blocker, fix it and cut the next RC;
**RCs are not cut on a cadence.**

## Version scheme going forward

| Stage | Version | Meaning |
|-------|---------|---------|
| Pre-1.0 | `0.x.y` | Patch = fixes, minor = features/behaviour changes. Not production-certified. |
| Candidate | `1.0.0-rc.N` | Only per the bar above. "We intend to ship this." |
| Release | `1.0.0` | An RC that survived real use with no blocker. |

## The 0.9.x quality bar

The bar above is for `1.0` and blends **quality** (in our control) with **maturity**
(sustained fuzzer-dry weeks + external validation, which only time and adoption can
earn). [`docs/production-confidence.md`](docs/production-confidence.md) defines the
separate, **intrinsic-quality "A+" bar** — the honest standard for when a company can
run `0.9.x` in production on the merits of the code, tests, build, and docs alone. An
A+ `0.9.x` plus the maturity gate is what earns `1.0`.
