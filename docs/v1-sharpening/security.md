# Domain 14 — Security & data protection

**Scope:** raw SQL / stored-procedure safety, log redaction, tenant isolation (cross-ref
Domain 7), and injection resistance across the query/write/bulk paths.

## 1.0 exit criteria

- [ ] All parameterisation is safe: no user value is ever concatenated into SQL; identifier
      validation is strict (output-parameter names, table/column identifiers).
- [ ] Raw SQL and stored-procedure surfaces are documented as provider-bound escape hatches,
      blocked under strict provider mobility, and safe by construction (`docs/raw-sql-security.md`,
      `docs/stored-procedure-security.md`).
- [ ] Log redaction never leaks parameter values / secrets (`docs/logging-redaction.md`).
- [ ] Tenant isolation (Domain 7) has zero cross-tenant leaks — this is a security boundary.
- [ ] Live DB credentials are never committed to source or echoed; they stay in environment
      variables and are test-only.

## Current confidence

Strong. Culture-invariant comparison sites are enforced (CA1304/CA1310 as build errors). Strict
output-parameter and identifier validators are in place. Raw-SQL/stored-proc security docs exist.
Redaction is documented.

## Open items

- [ ] Add/confirm an injection-resistance adversarial suite over query/write/bulk/raw-SQL paths.
- [ ] Verify redaction covers every log site (query, bulk, error, interceptor).
- [ ] Cross-check tenant isolation sweep (Domain 7) as the security-critical case.

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~RawSql|FullyQualifiedName~StoredProcedure|FullyQualifiedName~Redaction|FullyQualifiedName~Security"`
- `docs/raw-sql-security.md`, `docs/stored-procedure-security.md`, `docs/logging-redaction.md`, `docs/multi-tenancy-security.md`.

## Risks

Security bars are absolute: an injection path or a redaction leak is a defect, not a limitation,
and blocks the cross-cutting "zero known defects" gate.
