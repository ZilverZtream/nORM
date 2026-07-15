# Domain 14 — Security & data protection

**Scope:** raw SQL / stored-procedure safety, log redaction, tenant isolation (cross-ref
Domain 7), and injection resistance across the query/write/bulk paths.

## 1.0 exit criteria

- [x] All parameterisation is safe: no user value is ever concatenated into SQL; identifier
      validation is strict (output-parameter names, table/column identifiers) (NH-1401:
      `RawSqlSecurity`/`RawSqlBoundary`/`ParameterMetadataContamination` green).
- [x] Raw SQL and stored-procedure surfaces are documented as provider-bound escape hatches,
      blocked under strict provider mobility, and safe by construction (`docs/raw-sql-security.md`,
      `docs/stored-procedure-security.md`) (NH-1401: `RawSqlSecurity`/`StoredProcedureSecurity`/
      `RawSqlSecurityDocContract` green).
- [x] Log redaction never leaks parameter values / secrets (`docs/logging-redaction.md`)
      (NH-1401: `Redaction` green).
- [x] Tenant isolation (Domain 7) has zero cross-tenant leaks — this is a security boundary
      (NH-0701: 157-test adversarial sweep green; live native-RLS deferred to the live gate).
- [x] Live DB credentials are never committed to source or echoed; they stay in environment
      variables (`NORM_TEST_*`) and are test-only (practice invariant).

## Current confidence

Strong. Culture-invariant comparison sites are enforced (CA1304/CA1310 as build errors). Strict
output-parameter and identifier validators are in place. Raw-SQL/stored-proc security docs exist.
Redaction is documented.

## Open items

- [x] Injection resistance confirmed: parameterisation is safe by construction and identifiers are
      strictly validated across query/write/bulk/raw-SQL (NH-1401).
- [x] Redaction covers the log sites (NH-1401: `Redaction` green).
- [x] Tenant-isolation sweep cross-checked as the security-critical case (NH-0701).

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~RawSql|FullyQualifiedName~StoredProcedure|FullyQualifiedName~Redaction|FullyQualifiedName~Security"`
- `docs/raw-sql-security.md`, `docs/stored-procedure-security.md`, `docs/logging-redaction.md`, `docs/multi-tenancy-security.md`.

## Risks

Security bars are absolute: an injection path or a redaction leak is a defect, not a limitation,
and blocks the cross-cutting "zero known defects" gate.
