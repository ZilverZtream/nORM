# Namespace Resolution Guide

## Fixed Namespace Collisions

The benchmark project had namespace collisions between:
- `nORM.Core.DbContext` (our implementation)
- `Microsoft.EntityFrameworkCore.DbContext` (EF Core)

## Resolution Strategy

### 1. **Using Aliases**
```csharp
using EfDbContext = Microsoft.EntityFrameworkCore.DbContext;
```

### 2. **Fully Qualified Names**
```csharp
// nORM Context
private nORM.Core.DbContext? _nOrmContext;

// EF Core Context  
public class EfCoreContext : EfDbContext

// EF Core DbSet
public Microsoft.EntityFrameworkCore.DbSet<BenchmarkUser> Users { get; set; }
```

### 3. **Clear Separation**
- nORM types: Use `nORM.Core.*` namespace
- EF Core types: Use `Microsoft.EntityFrameworkCore.*` or aliases
- Keep contexts clearly named: `EfCoreContext` vs `_nOrmContext`

## Verification

The benchmark should now compile without CS0104 ambiguous reference errors.

```bash
cd benchmarks
dotnet build
# Should complete without errors
```
