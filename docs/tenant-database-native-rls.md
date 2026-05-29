# Tenant Database-Native RLS

nORM's tenant boundary is generated-path enforcement. Database-native row-level
security can be added underneath nORM for defense in depth. nORM can now help
with the two parts that should be provider-owned and deterministic:

- write the current tenant ID into provider-native session context before
  generated commands execute;
- generate reviewable SQL Server/PostgreSQL RLS policy DDL for a mapped tenant
  table.

nORM does not silently install, migrate, or own database-native RLS policies.
Applications execute that DDL through reviewed migrations, operational
change-control, or the explicit nORM apply/drop APIs described below.

For clarity: v1 does not make native RLS policy execution automatic.

## nORM API

Enable native session context only when `TenantProvider` is configured:

```csharp
var options = new DbContextOptions
{
    TenantProvider = tenantProvider
}.EnableNativeTenantSessionContext("norm.tenant_id");
```

When enabled, `DbContext` writes the current tenant value to provider session
state before generated queries/writes run. If the provider does not support
native session context, nORM fails closed with `NormUnsupportedFeatureException`.
If the tenant provider returns `null` or throws, nORM fails before running the
tenant-scoped command.

Generate policy SQL for review:

```csharp
var ddl = context.GenerateNativeTenantPolicySql<Order>();
```

The returned script is provider-specific DDL. Treat it like migration output:
review it, execute it through your deployment process, and test it against the
same live provider matrix as the application.

When the deployment process wants nORM to execute the reviewed script, use the
explicit apply/drop APIs:

```csharp
await context.ApplyNativeTenantPolicyAsync<Order>();
await context.DropNativeTenantPolicyAsync<Order>();
```

These methods use the active nORM connection, transaction, retry strategy, and
command interception pipeline. They do not run automatically during normal
queries, `SaveChanges`, or temporal bootstrap.

## SQL Server Pattern

SQL Server RLS commonly uses a predicate function and security policy over the
tenant column. The application sets tenant context on the opened connection,
then every table policy compares the row tenant to that context.

Example shape:

```sql
EXEC sys.sp_set_session_context @key = N'norm.tenant_id', @value = @tenantId;
```

Then a security predicate can compare `TenantId` to
`SESSION_CONTEXT(N'norm.tenant_id')`.

`GenerateNativeTenantPolicySql<T>()` emits a predicate function plus security
policy shape for the mapped table. Keep policy creation in migrations,
operational DDL, or an explicit `ApplyNativeTenantPolicyAsync<T>()` deployment
step, not hidden runtime bootstrap. Tests should prove both:

- generated nORM paths include tenant predicates;
- direct SQL without a tenant predicate is still blocked by the database policy.

`LiveProviderNativeTenantSecurityTests` installs the generated SQL Server
policy against a live database, proves direct reads are filtered by session
context, and proves cross-tenant direct inserts are blocked by the database
block predicate.

## PostgreSQL Pattern

PostgreSQL RLS commonly uses policies that compare the row tenant to a custom
session setting.

Example shape:

```sql
SELECT set_config('norm.tenant_id', @tenantId, false);
```

Then a policy can compare `TenantId` to `current_setting('norm.tenant_id')`.

`GenerateNativeTenantPolicySql<T>()` emits `ENABLE ROW LEVEL SECURITY`,
`FORCE ROW LEVEL SECURITY`, and a policy with both `USING` and `WITH CHECK`
predicates. `FORCE` is intentional: PostgreSQL table owners normally bypass RLS,
and many application deployments connect as the table owner. nORM uses session
context because generated commands may run outside an explicit transaction. In
pooled applications, nORM reapplies the current tenant when the context opens or
the tenant provider value changes.

`LiveProviderNativeTenantSecurityTests` creates a non-bypass PostgreSQL role
when the configured test connection is a superuser, sets that role, and proves
the generated policy filters direct reads and blocks cross-tenant direct
inserts.

## MySQL And SQLite

MySQL and SQLite do not provide the same first-class RLS model as SQL Server or
PostgreSQL. For these providers, keep nORM generated-path tenant enforcement,
least-privilege credentials, views where appropriate, and explicit tests around
privileged paths.

## nORM Contract

The supported v1 native-tenant contract is:

- generated nORM tenant paths include tenant predicates or fail closed;
- SQL Server and PostgreSQL can receive nORM-managed tenant session context;
- SQL Server and PostgreSQL can generate reviewable native RLS policy DDL;
- applications own executing, migrating, and auditing native policy DDL;
- MySQL and SQLite fail closed when native session context is requested;
- raw SQL and stored procedures remain privileged even when RLS is present.

Evidence:

- `NativeTenantSecurityTests` covers fail-closed behavior, policy generation,
  explicit policy apply/drop execution, session-context replay, and tenant-value
  changes.
- `LiveProviderNativeTenantSecurityTests` proves SQL Server/PostgreSQL session
  context when live provider connection strings are configured.
