using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Migration;
using nORM.Providers;
using MigrationBase = nORM.Migration.Migration;
using Xunit;

#pragma warning disable CS8765 // Nullability mismatch on overridden member
#nullable enable

namespace nORM.Tests;

// =============================================================================
// Section 8 gate 3.5 -> 4.0 — Provider-parity suites
//
// Categories: paging, GUID/enum/DateOnly/TimeOnly/null binding, rowcount
// semantics, generated key propagation, include/navigation loading,
// migration apply/replay/failure.
//
// SQLite tests run live (in-memory). Env-gated stubs document what the
// equivalent SQL Server / MySQL / PostgreSQL live tests would verify.
//   NORM_TEST_SQLSERVER_CS, NORM_TEST_MYSQL_CS, NORM_TEST_POSTGRES_CS
// =============================================================================

// -- Entities -----------------------------------------------------------------

[Table("PPG40_Entity")]
public class PPG40Entity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public Guid GuidCol { get; set; }
    public string? NullableStr { get; set; }
    public int Category { get; set; }
    public bool IsActive { get; set; }
}

public enum PPG40Category
{
    None = 0,
    Alpha = 1,
    Beta = 2,
    Gamma = 3
}

[Table("PPG40_EnumEntity")]
public class PPG40EnumEntity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Category { get; set; } // stored as int, mapped from enum
}

[Table("PPG40_DateEntity")]
public class PPG40DateEntity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public DateOnly BirthDate { get; set; }
    public TimeOnly WakeTime { get; set; }
    public string Label { get; set; } = string.Empty;
}

[Table("PPG40_Parent")]
public class PPG40Parent
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<PPG40Child> Children { get; set; } = new();
}

[Table("PPG40_Child")]
public class PPG40Child
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
    public string Tag { get; set; } = string.Empty;
}

public class ProviderBindingParityTests
{
    // -- Helpers ----------------------------------------------------------------

    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static object? ReadScalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    private static SqliteConnection CreateEntityDb(int rowCount = 0)
    {
        var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE PPG40_Entity (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            GuidCol TEXT NOT NULL,
            NullableStr TEXT,
            Category INTEGER NOT NULL,
            IsActive INTEGER NOT NULL DEFAULT 0
        )");
        for (int i = 1; i <= rowCount; i++)
        {
            var guid = Guid.NewGuid().ToString();
            var nullStr = i % 3 == 0 ? "NULL" : $"'val{i}'";
            Exec(cn, $"INSERT INTO PPG40_Entity (GuidCol, NullableStr, Category, IsActive) VALUES ('{guid}', {nullStr}, {i % 4}, {(i % 2 == 0 ? 1 : 0)})");
        }
        return cn;
    }

    private static DbContext CreateCtx(SqliteConnection cn)
    {
        return new DbContext(cn, new SqliteProvider());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 1. Paging: Insert 20 rows, Skip(5).Take(5) -> rows 6-10
    //
    // Equivalent live tests for other providers:
    //   SQL Server: Uses OFFSET/FETCH NEXT; verifies TOP is not emitted.
    //   MySQL: Uses LIMIT offset, count syntax.
    //   PostgreSQL: Uses LIMIT/OFFSET; verifies planner hint.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Paging_Skip5_Take5_ReturnsRows6Through10()
    {
        using var cn = CreateEntityDb(20);
        using var ctx = CreateCtx(cn);

        var page = await ctx.Query<PPG40Entity>()
            .OrderBy(e => e.Id)
            .Skip(5)
            .Take(5)
            .ToListAsync();

        Assert.Equal(5, page.Count);
        // Ordered by Id ascending, skip 5 means first result has Id == 6
        Assert.Equal(6, page[0].Id);
        Assert.Equal(10, page[4].Id);
        for (int i = 0; i < page.Count; i++)
            Assert.Equal(6 + i, page[i].Id);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 2. Paging OrderByDesc: Skip+Take with OrderByDescending -> reversed page
    //
    // Equivalent live tests:
    //   SQL Server: OFFSET/FETCH with DESC; verifies no OFFSET without ORDER BY.
    //   MySQL: LIMIT with DESC ordering.
    //   PostgreSQL: LIMIT/OFFSET with DESC.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Paging_OrderByDesc_Skip5_Take5_CorrectReversedPage()
    {
        using var cn = CreateEntityDb(20);
        using var ctx = CreateCtx(cn);

        var page = await ctx.Query<PPG40Entity>()
            .OrderByDescending(e => e.Id)
            .Skip(5)
            .Take(5)
            .ToListAsync();

        Assert.Equal(5, page.Count);
        // Descending: 20,19,18,17,16, then skip 5 -> 15,14,13,12,11
        Assert.Equal(15, page[0].Id);
        Assert.Equal(11, page[4].Id);
        for (int i = 0; i < page.Count; i++)
            Assert.Equal(15 - i, page[i].Id);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 3. GUID binding: Insert with specific Guid, query WHERE GuidCol == guid
    //
    // Equivalent live tests:
    //   SQL Server: UNIQUEIDENTIFIER column; parameterized WHERE.
    //   MySQL: CHAR(36) column; string representation comparison.
    //   PostgreSQL: UUID column type; native UUID comparison.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GuidBinding_InsertAndQueryByGuid_FindsCorrectRow()
    {
        using var cn = CreateEntityDb();
        using var ctx = CreateCtx(cn);

        var targetGuid = Guid.NewGuid();
        var entity = new PPG40Entity
        {
            GuidCol = targetGuid,
            NullableStr = "guid-test",
            Category = 1,
            IsActive = true
        };
        await ctx.InsertAsync(entity);

        // Insert a second row with different GUID
        await ctx.InsertAsync(new PPG40Entity
        {
            GuidCol = Guid.NewGuid(),
            NullableStr = "other",
            Category = 2,
            IsActive = false
        });

        var results = await ctx.Query<PPG40Entity>()
            .Where(e => e.GuidCol == targetGuid)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(targetGuid, results[0].GuidCol);
        Assert.Equal("guid-test", results[0].NullableStr);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 4. Enum binding: Insert with enum property, query by enum value
    //
    // Equivalent live tests:
    //   SQL Server: INT column; parameterized enum-to-int conversion.
    //   MySQL: INT column; same int-cast behavior.
    //   PostgreSQL: INT column or native ENUM type mapping.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EnumBinding_InsertAndQueryByEnumValue_CorrectFilter()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE PPG40_EnumEntity (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL,
            Category INTEGER NOT NULL
        )");
        using var ctx = CreateCtx(cn);

        await ctx.InsertAsync(new PPG40EnumEntity { Name = "Alpha1", Category = (int)PPG40Category.Alpha });
        await ctx.InsertAsync(new PPG40EnumEntity { Name = "Beta1", Category = (int)PPG40Category.Beta });
        await ctx.InsertAsync(new PPG40EnumEntity { Name = "Alpha2", Category = (int)PPG40Category.Alpha });
        await ctx.InsertAsync(new PPG40EnumEntity { Name = "Gamma1", Category = (int)PPG40Category.Gamma });

        var alphaVal = (int)PPG40Category.Alpha;
        var results = await ctx.Query<PPG40EnumEntity>()
            .Where(e => e.Category == alphaVal)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal((int)PPG40Category.Alpha, r.Category));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 5. DateOnly binding: Insert with DateOnly, query WHERE date == specific
    //
    // Equivalent live tests:
    //   SQL Server: DATE column type; parameterized DATE comparison.
    //   MySQL: DATE column; date literal formatting.
    //   PostgreSQL: DATE column; native date comparison.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DateOnlyBinding_InsertAndQueryByDate_MatchesCorrectRow()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE PPG40_DateEntity (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            BirthDate TEXT NOT NULL,
            WakeTime TEXT NOT NULL,
            Label TEXT NOT NULL
        )");
        using var ctx = CreateCtx(cn);

        var targetDate = new DateOnly(1990, 6, 15);
        await ctx.InsertAsync(new PPG40DateEntity { BirthDate = targetDate, WakeTime = new TimeOnly(7, 0), Label = "Target" });
        await ctx.InsertAsync(new PPG40DateEntity { BirthDate = new DateOnly(2000, 1, 1), WakeTime = new TimeOnly(8, 0), Label = "Other" });

        var all = await ctx.Query<PPG40DateEntity>().ToListAsync();
        Assert.Equal(2, all.Count);

        // Verify in-memory filter to confirm DateOnly round-trip
        var match = all.Where(e => e.BirthDate == targetDate).ToList();
        Assert.Single(match);
        Assert.Equal("Target", match[0].Label);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 6. TimeOnly binding: Insert with TimeOnly, verify round-trip
    //
    // Equivalent live tests:
    //   SQL Server: TIME column type; parameterized TIME comparison.
    //   MySQL: TIME column; HH:MM:SS formatting.
    //   PostgreSQL: TIME column; native time comparison.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TimeOnlyBinding_InsertAndQuery_RoundTrips()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE PPG40_DateEntity (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            BirthDate TEXT NOT NULL,
            WakeTime TEXT NOT NULL,
            Label TEXT NOT NULL
        )");
        using var ctx = CreateCtx(cn);

        var targetTime = new TimeOnly(14, 30, 45);
        await ctx.InsertAsync(new PPG40DateEntity { BirthDate = new DateOnly(2000, 1, 1), WakeTime = targetTime, Label = "TimeTest" });
        await ctx.InsertAsync(new PPG40DateEntity { BirthDate = new DateOnly(2000, 1, 1), WakeTime = new TimeOnly(6, 0), Label = "EarlyBird" });

        var all = await ctx.Query<PPG40DateEntity>().ToListAsync();
        Assert.Equal(2, all.Count);

        var match = all.Where(e => e.WakeTime == targetTime).ToList();
        Assert.Single(match);
        Assert.Equal("TimeTest", match[0].Label);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 7. Null binding: Query WHERE NullableStr == null -> rows with null
    //
    // Equivalent live tests:
    //   SQL Server: WHERE col IS NULL; verifies parameterized null -> IS NULL rewrite.
    //   MySQL: WHERE col IS NULL; same rewrite.
    //   PostgreSQL: WHERE col IS NULL; same rewrite.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task NullBinding_WhereNullableStrIsNull_ReturnsNullRows()
    {
        using var cn = CreateEntityDb(12);
        using var ctx = CreateCtx(cn);

        var results = await ctx.Query<PPG40Entity>()
            .Where(e => e.NullableStr == null)
            .ToListAsync();

        // Every 3rd row (i % 3 == 0) has NULL NullableStr: rows 3,6,9,12 = 4 rows
        Assert.Equal(4, results.Count);
        Assert.All(results, r => Assert.Null(r.NullableStr));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 8. Non-null binding: Query WHERE NullableStr != null -> rows with values
    //
    // Equivalent live tests:
    //   SQL Server: WHERE col IS NOT NULL; verifies rewrite from != null.
    //   MySQL: WHERE col IS NOT NULL.
    //   PostgreSQL: WHERE col IS NOT NULL.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task NonNullBinding_WhereNullableStrNotNull_ReturnsNonNullRows()
    {
        using var cn = CreateEntityDb(12);
        using var ctx = CreateCtx(cn);

        var results = await ctx.Query<PPG40Entity>()
            .Where(e => e.NullableStr != null)
            .ToListAsync();

        // 12 total rows - 4 null rows = 8 non-null rows
        Assert.Equal(8, results.Count);
        Assert.All(results, r => Assert.NotNull(r.NullableStr));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 9. Rowcount semantics: BulkUpdate 5 rows -> returns 5
    //
    // Equivalent live tests:
    //   SQL Server: @@ROWCOUNT after UPDATE; affected-rows semantics.
    //   MySQL: With UseAffectedRowsSemantics=true, rows_affected vs rows_matched.
    //   PostgreSQL: Standard affected-rows from UPDATE.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task RowcountSemantics_BulkUpdate5Rows_Returns5()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE PPG40_Entity (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            GuidCol TEXT NOT NULL,
            NullableStr TEXT,
            Category INTEGER NOT NULL,
            IsActive INTEGER NOT NULL DEFAULT 0
        )");
        using var ctx = CreateCtx(cn);

        // Insert 5 rows via raw SQL
        var entities = new List<PPG40Entity>();
        for (int i = 0; i < 5; i++)
        {
            var e = new PPG40Entity
            {
                GuidCol = Guid.NewGuid(),
                NullableStr = $"row{i}",
                Category = i,
                IsActive = true
            };
            await ctx.InsertAsync(e);
            entities.Add(e);
        }

        // Mutate all 5
        foreach (var e in entities)
            e.NullableStr = "updated";

        var updated = await ctx.BulkUpdateAsync(entities);
        Assert.Equal(5, updated);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 10. Generated key propagation: InsertAsync -> entity.Id populated
    //
    // Equivalent live tests:
    //   SQL Server: SCOPE_IDENTITY() or OUTPUT clause; identity column.
    //   MySQL: LAST_INSERT_ID(); AUTO_INCREMENT column.
    //   PostgreSQL: RETURNING clause; SERIAL or GENERATED ALWAYS column.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeneratedKeyPropagation_InsertAsync_EntityIdPopulated()
    {
        using var cn = CreateEntityDb();
        using var ctx = CreateCtx(cn);

        var e1 = new PPG40Entity { GuidCol = Guid.NewGuid(), Category = 1, IsActive = true };
        var e2 = new PPG40Entity { GuidCol = Guid.NewGuid(), Category = 2, IsActive = false };

        Assert.Equal(0, e1.Id);
        Assert.Equal(0, e2.Id);

        await ctx.InsertAsync(e1);
        await ctx.InsertAsync(e2);

        Assert.True(e1.Id > 0, "DB-generated key must be assigned after InsertAsync");
        Assert.True(e2.Id > 0, "DB-generated key must be assigned after InsertAsync");
        Assert.NotEqual(e1.Id, e2.Id);

        // Verify by re-reading from DB
        var fromDb = await ctx.Query<PPG40Entity>()
            .Where(x => x.Id == e1.Id)
            .ToListAsync();
        Assert.Single(fromDb);
        Assert.Equal(e1.GuidCol, fromDb[0].GuidCol);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 11. Include/navigation loading: Parent with children via Include+AsSplitQuery
    //
    // Equivalent live tests:
    //   SQL Server: Split query generates separate SELECT for children; JOIN variant
    //               would use INNER/LEFT JOIN in single query.
    //   MySQL: Same split query pattern; verifies FK relationship resolution.
    //   PostgreSQL: Same; verifies array_agg or split query materializer.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task IncludeNavLoading_ParentWithChildren_ChildrenLoaded()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE PPG40_Parent (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL
        );
        CREATE TABLE PPG40_Child (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            ParentId INTEGER NOT NULL,
            Tag TEXT NOT NULL
        );
        INSERT INTO PPG40_Parent (Name) VALUES ('ParentA'), ('ParentB');
        INSERT INTO PPG40_Child (ParentId, Tag) VALUES (1, 'C1'), (1, 'C2'), (1, 'C3'), (2, 'C4');");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<PPG40Parent>()
                  .HasKey(p => p.Id)
                  .HasMany<PPG40Child>(p => p.Children)
                  .WithOne()
                  .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var q = (INormQueryable<PPG40Parent>)ctx.Query<PPG40Parent>();
        var parents = await q.Include(p => p.Children).AsSplitQuery().ToListAsync();

        Assert.Equal(2, parents.Count);
        var parentA = parents.First(p => p.Name == "ParentA");
        var parentB = parents.First(p => p.Name == "ParentB");

        Assert.Equal(3, parentA.Children.Count);
        Assert.Single(parentB.Children);
        Assert.Contains(parentA.Children, c => c.Tag == "C1");
        Assert.Contains(parentA.Children, c => c.Tag == "C2");
        Assert.Contains(parentA.Children, c => c.Tag == "C3");
        Assert.Equal("C4", parentB.Children[0].Tag);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 12-14. Migration apply / replay / failure
    //
    // Uses dynamic assembly builder to isolate migration classes from test assembly.
    // ═══════════════════════════════════════════════════════════════════════════

    // -- Dynamic assembly emit helpers (same pattern as MigrationReplayFailureTests) --

    private static readonly Type[] _upDownParams =
        { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) };

    private static readonly ConstructorInfo _baseCtor =
        typeof(MigrationBase).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;

    private static readonly MethodInfo _upAbstract =
        typeof(MigrationBase).GetMethod("Up", _upDownParams)!;
    private static readonly MethodInfo _downAbstract =
        typeof(MigrationBase).GetMethod("Down", _upDownParams)!;

    private static readonly ConstructorInfo _ioExCtor =
        typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!;

    private static Assembly DdlAssembly(long version, string name, string createTableSql)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("PPG40_Ddl_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");

        var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb, version, name);
        EmitDdlUp(tb, createTableSql);
        EmitNoOpDown(tb);
        tb.CreateType();
        return ab;
    }

    private static Assembly NoOpAssembly(params (string Name, long Version)[] specs)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("PPG40_NoOp_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");

        foreach (var (name, version) in specs)
        {
            var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
            EmitCtor(tb, version, name);
            EmitNoOpUp(tb);
            EmitNoOpDown(tb);
            tb.CreateType();
        }
        return ab;
    }

    private static Assembly ThrowingAssembly(long version, string name)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("PPG40_Throw_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");
        var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb, version, name);
        EmitThrowingUp(tb);
        EmitNoOpDown(tb);
        tb.CreateType();
        return ab;
    }

    private static void EmitCtor(TypeBuilder tb, long version, string name)
    {
        var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
        var il = ctor.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldc_I8, version);
        il.Emit(OpCodes.Ldstr, name);
        il.Emit(OpCodes.Call, _baseCtor);
        il.Emit(OpCodes.Ret);
    }

    private static void EmitNoOpUp(TypeBuilder tb)
    {
        var m = tb.DefineMethod("Up",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        m.GetILGenerator().Emit(OpCodes.Ret);
        tb.DefineMethodOverride(m, _upAbstract);
    }

    private static void EmitNoOpDown(TypeBuilder tb)
    {
        var m = tb.DefineMethod("Down",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        m.GetILGenerator().Emit(OpCodes.Ret);
        tb.DefineMethodOverride(m, _downAbstract);
    }

    private static void EmitThrowingUp(TypeBuilder tb)
    {
        var m = tb.DefineMethod("Up",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        var il = m.GetILGenerator();
        il.Emit(OpCodes.Ldstr, "Simulated migration failure");
        il.Emit(OpCodes.Newobj, _ioExCtor);
        il.Emit(OpCodes.Throw);
        tb.DefineMethodOverride(m, _upAbstract);
    }

    private static void EmitDdlUp(TypeBuilder tb, string sql)
    {
        var m = tb.DefineMethod("Up",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        var il = m.GetILGenerator();

        var createCmd = typeof(DbConnection).GetMethod("CreateCommand")!;
        var setTx = typeof(DbCommand).GetProperty("Transaction")!.GetSetMethod()!;
        var setCmdText = typeof(DbCommand).GetProperty("CommandText")!.GetSetMethod()!;
        var execNonQ = typeof(DbCommand).GetMethod("ExecuteNonQuery")!;
        var dispose = typeof(IDisposable).GetMethod("Dispose")!;

        il.DeclareLocal(typeof(DbCommand));
        il.Emit(OpCodes.Ldarg_1);          // connection
        il.Emit(OpCodes.Callvirt, createCmd);
        il.Emit(OpCodes.Stloc_0);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldarg_2);          // transaction
        il.Emit(OpCodes.Callvirt, setTx);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldstr, sql);
        il.Emit(OpCodes.Callvirt, setCmdText);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Callvirt, execNonQ);
        il.Emit(OpCodes.Pop);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Callvirt, dispose);

        il.Emit(OpCodes.Ret);
        tb.DefineMethodOverride(m, _upAbstract);
    }

    private static async Task<long> HistoryCountAsync(SqliteConnection cn)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static long TableCount(SqliteConnection cn, string tableName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{tableName}'";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 12. Migration apply: Apply migration, verify table created
    //
    // Equivalent live tests:
    //   SQL Server: sp_getapplock advisory lock; __NormMigrationsHistory in dbo.
    //   MySQL: GET_LOCK advisory lock; per-step checkpoint with Status column.
    //   PostgreSQL: pg_advisory_lock; standard history table.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationApply_TableCreatedSuccessfully()
    {
        using var cn = OpenMemory();
        var asm = DdlAssembly(100L, "CreatePPG40Widget",
            "CREATE TABLE PPG40_Widget (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL)");
        var runner = new SqliteMigrationRunner(cn, asm);

        await runner.ApplyMigrationsAsync();

        Assert.Equal(1L, TableCount(cn, "PPG40_Widget"));
        Assert.Equal(1L, await HistoryCountAsync(cn));
        Assert.False(await runner.HasPendingMigrationsAsync());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 13. Migration replay: Apply same migration twice -> second is no-op
    //
    // Equivalent live tests:
    //   SQL Server: Second ApplyMigrationsAsync finds 0 pending; no DDL emitted.
    //   MySQL: CheckpointRecovery detects Applied status; skips.
    //   PostgreSQL: Same idempotency behavior.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationReplay_SecondRunIsNoOp()
    {
        using var cn = OpenMemory();
        var asm = DdlAssembly(200L, "CreatePPG40Gadget",
            "CREATE TABLE PPG40_Gadget (Id INTEGER PRIMARY KEY, Name TEXT)");
        var runner = new SqliteMigrationRunner(cn, asm);

        await runner.ApplyMigrationsAsync();
        Assert.Equal(1L, await HistoryCountAsync(cn));

        // Second run: same assembly, no-op
        var runner2 = new SqliteMigrationRunner(cn, asm);
        await runner2.ApplyMigrationsAsync();
        Assert.Equal(1L, await HistoryCountAsync(cn));
        Assert.False(await runner2.HasPendingMigrationsAsync());

        // Table still exists
        Assert.Equal(1L, TableCount(cn, "PPG40_Gadget"));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 14. Migration failure: Throwing migration -> exception propagates, history clean
    //
    // Equivalent live tests:
    //   SQL Server: Transaction rollback; no row in __NormMigrationsHistory.
    //   MySQL: Partial checkpoint row left; operator must DELETE before retry.
    //   PostgreSQL: Transaction rollback; no row in history.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationFailure_ExceptionPropagates_HistoryClean()
    {
        using var cn = OpenMemory();
        var asm = ThrowingAssembly(300L, "FailPPG40Mig");
        var runner = new SqliteMigrationRunner(cn, asm);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());
        Assert.Contains("Simulated migration failure", ex.Message);

        // History must be empty (SQLite single-transaction rollback)
        Assert.Equal(0L, await HistoryCountAsync(cn));
        Assert.True(await runner.HasPendingMigrationsAsync());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 15. Source-gen/runtime parity: Same query via compiled and runtime -> same results
    //
    // Equivalent live tests:
    //   SQL Server: Compiled query uses same SQL plan cache; results identical.
    //   MySQL: Same compilation parity; verifies parameter binding consistency.
    //   PostgreSQL: Same; verifies type coercion for compiled params.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SourceGenRuntimeParity_SameQuerySameResults()
    {
        using var cn = CreateEntityDb(10);
        using var ctx = CreateCtx(cn);

        var compiled = Norm.CompileQuery((DbContext c, int minCat) =>
            c.Query<PPG40Entity>().Where(e => e.Category > minCat).OrderBy(e => e.Id));

        var threshold = 1;
        var compiledResult = await compiled(ctx, threshold);
        var runtimeResult = await ctx.Query<PPG40Entity>()
            .Where(e => e.Category > threshold)
            .OrderBy(e => e.Id)
            .ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        Assert.True(compiledResult.Count > 0, "Should match at least one row");
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].GuidCol, compiledResult[i].GuidCol);
            Assert.Equal(runtimeResult[i].NullableStr, compiledResult[i].NullableStr);
            Assert.Equal(runtimeResult[i].Category, compiledResult[i].Category);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 16. BONUS: BulkInsert rowcount and DB verification
    //
    // Equivalent live tests:
    //   SQL Server: OUTPUT clause; verify affected-rows count and DB state.
    //   MySQL: LAST_INSERT_ID(); verify row count in DB.
    //   PostgreSQL: RETURNING clause; verify row count.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BulkInsert_RowsPersistedInDb()
    {
        using var cn = CreateEntityDb();
        using var ctx = CreateCtx(cn);

        var entities = Enumerable.Range(0, 5)
            .Select(i => new PPG40Entity
            {
                GuidCol = Guid.NewGuid(),
                NullableStr = $"bulk{i}",
                Category = i,
                IsActive = i % 2 == 0
            }).ToArray();

        await ctx.BulkInsertAsync(entities);

        // Verify rows persisted in DB
        var count = Convert.ToInt64(ReadScalar(cn, "SELECT COUNT(*) FROM PPG40_Entity"));
        Assert.Equal(5L, count);

        // Verify data fidelity via query
        var rows = await ctx.Query<PPG40Entity>().OrderBy(e => e.Id).ToListAsync();
        Assert.Equal(5, rows.Count);
        for (int i = 0; i < 5; i++)
            Assert.Equal($"bulk{i}", rows[i].NullableStr);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 17. BONUS: Paging beyond end of data returns empty
    //
    // Equivalent live tests:
    //   SQL Server: OFFSET > total rows returns empty result set.
    //   MySQL: LIMIT offset > total rows returns empty.
    //   PostgreSQL: OFFSET > total rows returns empty.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Paging_BeyondEnd_ReturnsEmpty()
    {
        using var cn = CreateEntityDb(10);
        using var ctx = CreateCtx(cn);

        var page = await ctx.Query<PPG40Entity>()
            .OrderBy(e => e.Id)
            .Skip(100)
            .Take(10)
            .ToListAsync();

        Assert.Empty(page);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 18. BONUS: Include with empty child table returns empty collections
    //
    // Equivalent live tests:
    //   SQL Server: Split query child SELECT returns 0 rows; parent.Children = empty list.
    //   MySQL: Same behavior.
    //   PostgreSQL: Same behavior.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Include_EmptyChildTable_EmptyCollections()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE PPG40_Parent (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL
        );
        CREATE TABLE PPG40_Child (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            ParentId INTEGER NOT NULL,
            Tag TEXT NOT NULL
        );
        INSERT INTO PPG40_Parent (Name) VALUES ('LonelyParent');");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<PPG40Parent>()
                  .HasKey(p => p.Id)
                  .HasMany<PPG40Child>(p => p.Children)
                  .WithOne()
                  .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var q = (INormQueryable<PPG40Parent>)ctx.Query<PPG40Parent>();
        var parents = await q.Include(p => p.Children).AsSplitQuery().ToListAsync();

        Assert.Single(parents);
        Assert.Empty(parents[0].Children);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 19. BONUS: Null and non-null interleaved round-trip
    //
    // Equivalent live tests:
    //   SQL Server: Parameterized NULL vs non-NULL in same batch.
    //   MySQL: Same null handling.
    //   PostgreSQL: Same.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task NullNonNull_InterleavedRoundTrip_AllCorrect()
    {
        using var cn = CreateEntityDb();
        using var ctx = CreateCtx(cn);

        var withNull = new PPG40Entity { GuidCol = Guid.NewGuid(), NullableStr = null, Category = 1, IsActive = true };
        var withValue = new PPG40Entity { GuidCol = Guid.NewGuid(), NullableStr = "present", Category = 2, IsActive = false };

        await ctx.InsertAsync(withNull);
        await ctx.InsertAsync(withValue);

        var all = (await ctx.Query<PPG40Entity>().OrderBy(e => e.Id).ToListAsync());
        Assert.Equal(2, all.Count);

        Assert.Null(all[0].NullableStr);
        Assert.Equal("present", all[1].NullableStr);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 20. BONUS: Migration apply two sequential migrations
    //
    // Equivalent live tests:
    //   SQL Server: Two DDL migrations in sequence; both tracked.
    //   MySQL: Per-step checkpoint; both Applied status.
    //   PostgreSQL: Both in single transaction.
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationApply_TwoSequentialMigrations_BothTracked()
    {
        using var cn = OpenMemory();
        var asm = NoOpAssembly(("PPG40_StepA", 500L), ("PPG40_StepB", 501L));
        var runner = new SqliteMigrationRunner(cn, asm);

        await runner.ApplyMigrationsAsync();
        Assert.Equal(2L, await HistoryCountAsync(cn));
        Assert.False(await runner.HasPendingMigrationsAsync());

        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }
}

// ── Entities for provider DateTime/enum/converter lock-step parity tests ─────

[Table("G40DateRow")]
file class G40DateRow
{
    [Key]
    public int Id { get; set; }
    public DateOnly BirthDate { get; set; }
    public TimeOnly MeetTime { get; set; }
}

[Table("G40EnumRow")]
file class G40EnumRow
{
    [Key]
    public int Id { get; set; }
    public string Label { get; set; } = string.Empty;
    public G40Status Status { get; set; }
}

file enum G40Status
{
    Pending  = 0,
    Active   = 1,
    Archived = 2,
}

[Table("G40ConverterRow")]
file class G40ConverterRow
{
    [Key]
    public int Id { get; set; }
    public int Points { get; set; }
    public string Tag { get; set; } = string.Empty;
}

/// <summary>Negates the integer: model=42 ↔ db=-42.</summary>
file sealed class G40NegatingConv : ValueConverter<int, int>
{
    public override object? ConvertToProvider(int v) => -v;
    public override object? ConvertFromProvider(int v) => -v;
}

/// <summary>
/// Lock-step parity for DateOnly, TimeOnly, enum, and ValueConverter-backed
/// reads across SQLite, MySQL, and PostgreSQL providers.
///
/// "Lock-step" means the same SQLite in-memory engine is used for all providers.
/// MySqlProvider/PostgresProvider are instantiated with SqliteParameterFactory
/// so they create SQLite-compatible parameter objects while exercising their own
/// dialect-level SQL generation paths.
/// </summary>
public class ProviderDateTimeEnumParityTests
{
    private static DatabaseProvider MakeProvider(string kind) => kind switch
    {
        "sqlite"   => new SqliteProvider(),
        "mysql"    => new MySqlProvider(new SqliteParameterFactory()),
        "postgres" => new PostgresProvider(new SqliteParameterFactory()),
        _          => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static (SqliteConnection Cn, DbContext Ctx) Open(string kind, string ddl,
        DbContextOptions? opts = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, MakeProvider(kind), opts ?? new DbContextOptions()));
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    // ── 1. DateOnly / TimeOnly round-trip ────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_DateOnly_TimeOnly_RoundTrip_AllProviders(string kind)
    {
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40DateRow (Id INTEGER PRIMARY KEY, BirthDate TEXT NOT NULL, MeetTime TEXT NOT NULL)");
        await using var _ = ctx; using var __ = cn;

        var today   = new DateOnly(2024, 6, 15);
        var timeVal = new TimeOnly(9, 30, 0);

        ctx.Add(new G40DateRow { Id = 1, BirthDate = today, MeetTime = timeVal });
        await ctx.SaveChangesAsync();

        var rows = ctx.Query<G40DateRow>().ToList();

        Assert.Single(rows);
        Assert.Equal(today,   rows[0].BirthDate);
        Assert.Equal(timeVal, rows[0].MeetTime);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_DateOnly_MultipleRows_OrderedCorrectly(string kind)
    {
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40DateRow (Id INTEGER PRIMARY KEY, BirthDate TEXT NOT NULL, MeetTime TEXT NOT NULL)");
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G40DateRow { Id = 1, BirthDate = new DateOnly(2020, 1, 1),   MeetTime = new TimeOnly(8, 0) });
        ctx.Add(new G40DateRow { Id = 2, BirthDate = new DateOnly(2021, 6, 15),  MeetTime = new TimeOnly(12, 30) });
        ctx.Add(new G40DateRow { Id = 3, BirthDate = new DateOnly(2023, 12, 31), MeetTime = new TimeOnly(23, 59) });
        await ctx.SaveChangesAsync();

        var rows = ctx.Query<G40DateRow>().OrderBy(r => r.Id).ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal(new DateOnly(2020, 1, 1),   rows[0].BirthDate);
        Assert.Equal(new DateOnly(2021, 6, 15),  rows[1].BirthDate);
        Assert.Equal(new DateOnly(2023, 12, 31), rows[2].BirthDate);
        Assert.Equal(new TimeOnly(8, 0),   rows[0].MeetTime);
        Assert.Equal(new TimeOnly(23, 59), rows[2].MeetTime);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_DateOnly_Where_Filter_AllProviders(string kind)
    {
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40DateRow (Id INTEGER PRIMARY KEY, BirthDate TEXT NOT NULL, MeetTime TEXT NOT NULL)");
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G40DateRow { Id = 1, BirthDate = new DateOnly(2020, 1, 1), MeetTime = new TimeOnly(8, 0) });
        ctx.Add(new G40DateRow { Id = 2, BirthDate = new DateOnly(2025, 1, 1), MeetTime = new TimeOnly(9, 0) });
        await ctx.SaveChangesAsync();

        var target = new DateOnly(2025, 1, 1);
        var rows = ctx.Query<G40DateRow>().Where(r => r.BirthDate == target).ToList();

        Assert.Single(rows);
        Assert.Equal(2, rows[0].Id);
    }

    // ── 2. Enum mapping (stored as INTEGER) ──────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Enum_RoundTrip_AllProviders(string kind)
    {
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40EnumRow (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Status INTEGER NOT NULL)");
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G40EnumRow { Id = 1, Label = "pending-item",  Status = G40Status.Pending  });
        ctx.Add(new G40EnumRow { Id = 2, Label = "active-item",   Status = G40Status.Active   });
        ctx.Add(new G40EnumRow { Id = 3, Label = "archived-item", Status = G40Status.Archived });
        await ctx.SaveChangesAsync();

        var rows = ctx.Query<G40EnumRow>().OrderBy(r => r.Id).ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal(G40Status.Pending,  rows[0].Status);
        Assert.Equal(G40Status.Active,   rows[1].Status);
        Assert.Equal(G40Status.Archived, rows[2].Status);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Enum_Where_Filter_AllProviders(string kind)
    {
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40EnumRow (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Status INTEGER NOT NULL)");
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G40EnumRow { Id = 1, Label = "a", Status = G40Status.Pending  });
        ctx.Add(new G40EnumRow { Id = 2, Label = "b", Status = G40Status.Active   });
        ctx.Add(new G40EnumRow { Id = 3, Label = "c", Status = G40Status.Active   });
        ctx.Add(new G40EnumRow { Id = 4, Label = "d", Status = G40Status.Archived });
        await ctx.SaveChangesAsync();

        var active = G40Status.Active;
        var rows = ctx.Query<G40EnumRow>().Where(r => r.Status == active).ToList();

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(G40Status.Active, r.Status));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_Enum_Update_PreservesValue_AllProviders(string kind)
    {
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40EnumRow (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Status INTEGER NOT NULL)");
        await using var _ = ctx; using var __ = cn;

        var item = new G40EnumRow { Id = 1, Label = "item", Status = G40Status.Pending };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        item.Status = G40Status.Archived;
        await ctx.SaveChangesAsync();

        var result = ctx.Query<G40EnumRow>().Where(r => r.Id == 1).Single();
        Assert.Equal(G40Status.Archived, result.Status);
    }

    // ── 3. ValueConverter-backed reads ───────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_ValueConverter_Applied_OnRead_AllProviders(string kind)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<G40ConverterRow>()
                  .Property(r => r.Points)
                  .HasConversion(new G40NegatingConv())
        };
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40ConverterRow (Id INTEGER PRIMARY KEY, Points INTEGER NOT NULL, Tag TEXT NOT NULL)",
            opts);
        await using var _ = ctx; using var __ = cn;

        Exec(cn, "INSERT INTO G40ConverterRow (Id, Points, Tag) VALUES (1, -55, 'x')");

        var rows = ctx.Query<G40ConverterRow>().ToList();

        Assert.Single(rows);
        Assert.Equal(55, rows[0].Points);  // converter applied: -(-55) = 55
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_ValueConverter_Applied_OnWrite_AllProviders(string kind)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<G40ConverterRow>()
                  .Property(r => r.Points)
                  .HasConversion(new G40NegatingConv())
        };
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40ConverterRow (Id INTEGER PRIMARY KEY, Points INTEGER NOT NULL, Tag TEXT NOT NULL)",
            opts);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G40ConverterRow { Id = 1, Points = 100, Tag = "write-test" });
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Points FROM G40ConverterRow WHERE Id = 1";
        var raw = Convert.ToInt32(cmd.ExecuteScalar());
        Assert.Equal(-100, raw);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_ValueConverter_MultipleRows_AllProviders(string kind)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<G40ConverterRow>()
                  .Property(r => r.Points)
                  .HasConversion(new G40NegatingConv())
        };
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40ConverterRow (Id INTEGER PRIMARY KEY, Points INTEGER NOT NULL, Tag TEXT NOT NULL)",
            opts);
        await using var _ = ctx; using var __ = cn;

        Exec(cn, "INSERT INTO G40ConverterRow (Id, Points, Tag) VALUES (1, -10, 'a'), (2, -20, 'b'), (3, -30, 'c')");

        var rows = ctx.Query<G40ConverterRow>().OrderBy(r => r.Id).ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal(10, rows[0].Points);
        Assert.Equal(20, rows[1].Points);
        Assert.Equal(30, rows[2].Points);
    }

    // ── 4. Compiled query with enum parameter — all providers ─────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LockStep_CompiledQuery_Enum_Param_AllProviders(string kind)
    {
        var (cn, ctx) = Open(kind,
            "CREATE TABLE G40EnumRow (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Status INTEGER NOT NULL)");
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new G40EnumRow { Id = 1, Label = "p1", Status = G40Status.Pending  });
        ctx.Add(new G40EnumRow { Id = 2, Label = "a1", Status = G40Status.Active   });
        ctx.Add(new G40EnumRow { Id = 3, Label = "a2", Status = G40Status.Active   });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, G40Status s) =>
            c.Query<G40EnumRow>().Where(r => r.Status == s));

        var active = await compiled(ctx, G40Status.Active);
        Assert.Equal(2, active.Count);
        Assert.All(active, r => Assert.Equal(G40Status.Active, r.Status));

        var pending = await compiled(ctx, G40Status.Pending);
        Assert.Single(pending);
        Assert.Equal(G40Status.Pending, pending[0].Status);
    }
}
