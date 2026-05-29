# Tenant Deployment Patterns

This document is operational guidance, not a wider tenant-isolation claim.
nORM's automatic tenant boundary applies to generated query and write paths.
Deployment topology still belongs to the application.

## Shared Table

Shared-table tenancy stores all tenants in the same tables with a mapped tenant
column such as `TenantId`.

Use this when tenant count is high, tenant data volume is moderate, and the app
can use shared operational limits. Configure:

- `DbContextOptions.TenantProvider`;
- `DbContextOptions.TenantColumnName`;
- tenant-leading indexes for common filters, for example `(TenantId, Id)` and
  `(TenantId, CreatedUtc)`;
- tests that run generated select, write, bulk, compiled-query, include, cache,
  and temporal paths under at least two tenants.

Generated nORM paths fail closed when the mapped tenant column is missing. Raw
SQL, stored procedures, migrations, scaffolding, and direct connection usage
remain privileged paths and must supply tenant predicates explicitly.

## Database Per Tenant

Database-per-tenant tenancy uses a separate database or catalog per tenant.
nORM can still use `TenantProvider` for generated-path defense in depth, but
connection selection is the primary boundary.

Use this when tenants need separate backup/restore, regional placement,
customer-managed keys, or strict operational isolation. Configure connection
factories so the selected tenant controls the connection string before the
`DbContext` is created. Keep connection pools bounded; one pool per tenant can
be expensive.

## Connection Per Tenant

Connection-per-tenant tenancy uses the same server but different credentials,
schemas, or roles per tenant. nORM treats the connection as caller-provided.
The application must choose the credential or schema safely before constructing
the context.

This pattern composes well with provider-native security such as SQL Server
security policies or PostgreSQL row-level security. nORM does not create those
policies automatically in v1.

## Operational Checklist

| Area | Requirement |
| --- | --- |
| Tenant source | Tenant ID comes from authenticated identity, not request body data. |
| Tenant type | `TenantProvider.GetCurrentTenantId()` returns a non-null value compatible with the mapped tenant column. |
| Generated paths | Query, write, bulk, include, compiled query, cache, and temporal tests run under two tenants. |
| Privileged paths | Raw SQL, stored procedures, migrations, and direct connection code are reviewed separately. |
| Indexes | Shared tables have tenant-leading indexes for hot query and write predicates. |
| Temporal | `AsOf`, restore, history reads, and pruning are tested with tenant mode enabled. |

