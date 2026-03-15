using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that fast-path SQL and materializer caches scope their keys correctly
/// so that different providers, namespace-colliding type names, and divergent fluent
/// mappings each produce independent, correct cache entries.
/// </summary>
public class FastPathCacheCollisionTests
{
    private static (SqliteConnection Cn, DbContext Ctx) CreateCtx<T>(
        string tableName, DatabaseProvider? provider = null) where T : class
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<T>().ToTable(tableName) };
        var ctx = new DbContext(cn, provider ?? new SqliteProvider(), opts);
        return (cn, ctx);
    }

    // ── S10-1/C1: _fullSqlCache includes provider and mapping identity ─────

    /// <summary>
    /// Same CLR type mapped to different tables in two contexts must each query the
    /// correct table — the full SQL cache must not serve TableA SQL to a TableB context.
    /// </summary>
    [Fact]
    public async Task FullSqlCache_DivergentFluentMapping_CorrectSqlPerContext()
    {
        using var cnA = new SqliteConnection("Data Source=:memory:");
        cnA.Open();
        using var setupA = cnA.CreateCommand();
        setupA.CommandText =
            "CREATE TABLE TableA (Id INTEGER PRIMARY KEY, Label TEXT);" +
            "INSERT INTO TableA VALUES (1,'fromA');";
        setupA.ExecuteNonQuery();

        using var cnB = new SqliteConnection("Data Source=:memory:");
        cnB.Open();
        using var setupB = cnB.CreateCommand();
        setupB.CommandText =
            "CREATE TABLE TableB (Id INTEGER PRIMARY KEY, Label TEXT);" +
            "INSERT INTO TableB VALUES (1,'fromB');";
        setupB.ExecuteNonQuery();

        var optsA = new DbContextOptions { OnModelCreating = mb => mb.Entity<MappedEntity>().ToTable("TableA") };
        var optsB = new DbContextOptions { OnModelCreating = mb => mb.Entity<MappedEntity>().ToTable("TableB") };

        using var ctxA = new DbContext(cnA, new SqliteProvider(), optsA);
        using var ctxB = new DbContext(cnB, new SqliteProvider(), optsB);

        var a = await ctxA.Query<MappedEntity>().Where(e => e.Id == 1).ToListAsync();
        var b = await ctxB.Query<MappedEntity>().Where(e => e.Id == 1).ToListAsync();

        Assert.Equal("fromA", a[0].Label);
        Assert.Equal("fromB", b[0].Label);
    }

    /// <summary>
    /// Types with the same short name in different namespaces must not share a SQL cache
    /// entry. The full type name (including namespace) is used as the key discriminant.
    /// </summary>
    [Fact]
    public async Task FullSqlCache_SameShortName_DifferentNamespace_NoCrossContamination()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE Alpha_Collision (Id INTEGER PRIMARY KEY, Value TEXT);" +
            "CREATE TABLE Beta_Collision  (Id INTEGER PRIMARY KEY, Score INTEGER);" +
            "INSERT INTO Alpha_Collision VALUES (1,'hello');" +
            "INSERT INTO Beta_Collision  VALUES (1,42);";
        setup.ExecuteNonQuery();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Alpha.Collision>().ToTable("Alpha_Collision");
                mb.Entity<Beta.Collision>().ToTable("Beta_Collision");
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var a = await ctx.Query<Alpha.Collision>().Where(x => x.Id == 1).ToListAsync();
        var b = await ctx.Query<Beta.Collision>().Where(x => x.Id == 1).ToListAsync();

        Assert.Equal("hello", a[0].Value);
        Assert.Equal(42, b[0].Score);
    }

    /// <summary>
    /// Two contexts with different provider subclasses querying the same CLR type
    /// must each produce correct results — provider identity is part of the cache key.
    /// </summary>
    [Fact]
    public async Task FullSqlCache_DifferentProviderSubclass_SameType_CorrectResultsEach()
    {
        using var cnA = new SqliteConnection("Data Source=:memory:");
        cnA.Open();
        using var setupA = cnA.CreateCommand();
        setupA.CommandText =
            "CREATE TABLE CacheProduct (Id INTEGER PRIMARY KEY, Name TEXT, Active INTEGER);" +
            "INSERT INTO CacheProduct VALUES (1,'Alpha',1);" +
            "INSERT INTO CacheProduct VALUES (2,'Beta',0);";
        setupA.ExecuteNonQuery();

        using var cnB = new SqliteConnection("Data Source=:memory:");
        cnB.Open();
        using var setupB = cnB.CreateCommand();
        setupB.CommandText =
            "CREATE TABLE CacheProduct (Id INTEGER PRIMARY KEY, Name TEXT, Active INTEGER);" +
            "INSERT INTO CacheProduct VALUES (1,'Alpha',1);" +
            "INSERT INTO CacheProduct VALUES (2,'Beta',0);";
        setupB.ExecuteNonQuery();

        using var ctxA = new DbContext(cnA, new SqliteProvider());
        using var ctxB = new DbContext(cnB, new CustomSqliteProvider());

        var rA = await ctxA.Query<CacheProduct>().Where(p => p.Active).ToListAsync();
        var rB = await ctxB.Query<CacheProduct>().Where(p => p.Active).ToListAsync();

        Assert.Single(rA);
        Assert.Single(rB);
        Assert.Equal("Alpha", rA[0].Name);
        Assert.Equal("Alpha", rB[0].Name);
    }

    // ── S4-1/M1: _syncMaterializerCache includes mapping hash ─────────────

    /// <summary>
    /// Same CLR type mapped to different tables in two contexts must materialize
    /// rows from the correct table in each context.
    /// </summary>
    [Fact]
    public async Task SyncMaterializerCache_DivergentMapping_MaterializesCorrectlyEach()
    {
        using var cnA = new SqliteConnection("Data Source=:memory:");
        cnA.Open();
        using var setupA = cnA.CreateCommand();
        setupA.CommandText =
            "CREATE TABLE OrderA (Id INTEGER PRIMARY KEY, Name TEXT, Score INTEGER);" +
            "INSERT INTO OrderA VALUES (1,'Alice',10);";
        setupA.ExecuteNonQuery();

        using var cnB = new SqliteConnection("Data Source=:memory:");
        cnB.Open();
        using var setupB = cnB.CreateCommand();
        setupB.CommandText =
            "CREATE TABLE OrderB (Id INTEGER PRIMARY KEY, Name TEXT, Score INTEGER);" +
            "INSERT INTO OrderB VALUES (2,'Bob',20);";
        setupB.ExecuteNonQuery();

        var optsA = new DbContextOptions { OnModelCreating = mb => mb.Entity<OrderedEntity>().ToTable("OrderA") };
        var optsB = new DbContextOptions { OnModelCreating = mb => mb.Entity<OrderedEntity>().ToTable("OrderB") };
        using var ctxA = new DbContext(cnA, new SqliteProvider(), optsA);
        using var ctxB = new DbContext(cnB, new SqliteProvider(), optsB);

        var a = (await ctxA.Query<OrderedEntity>().ToListAsync())[0];
        var b = (await ctxB.Query<OrderedEntity>().ToListAsync())[0];

        Assert.Equal("Alice", a.Name);
        Assert.Equal(10, a.Score);
        Assert.Equal("Bob", b.Name);
        Assert.Equal(20, b.Score);
    }

    // ── S2-1/SQL1: UsesFetchOffsetPaging capability property ──────────────

    /// <summary>
    /// SqlServerProvider reports UsesFetchOffsetPaging=true so that the fast path
    /// emits TOP(n) syntax rather than LIMIT.
    /// </summary>
    [Fact]
    public void SqlServerProvider_UsesFetchOffsetPaging_IsTrue()
    {
        Assert.True(new SqlServerProvider().UsesFetchOffsetPaging);
    }

    /// <summary>
    /// SQLite, MySQL and PostgreSQL providers report UsesFetchOffsetPaging=false
    /// so LIMIT syntax is emitted on the fast path.
    /// </summary>
    [Fact]
    public void NonSqlServerProviders_UsesFetchOffsetPaging_IsFalse()
    {
        Assert.False(new SqliteProvider().UsesFetchOffsetPaging);
        Assert.False(new MySqlProvider(new SqliteParameterFactory()).UsesFetchOffsetPaging);
        Assert.False(new PostgresProvider(new SqliteParameterFactory()).UsesFetchOffsetPaging);
    }

    /// <summary>
    /// A custom DatabaseProvider subclass that overrides UsesFetchOffsetPaging=true
    /// produces the TOP(n) paging dialect without needing "SqlServer" in its type name.
    /// The capability contract is driven by the property, not by name inspection.
    /// </summary>
    [Fact]
    public void CustomProvider_UsesFetchOffsetPaging_OverrideIsRespected()
    {
        var provider = new FetchOffsetCapableProvider();
        Assert.True(provider.UsesFetchOffsetPaging);

        var limitProvider = new CustomSqliteProvider();
        Assert.False(limitProvider.UsesFetchOffsetPaging);
    }

    /// <summary>
    /// Fast-path Take(n) on SQLite returns the correct number of rows via LIMIT.
    /// </summary>
    [Fact]
    public async Task FastPath_Take_Sqlite_ReturnsCorrectCount()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE TakeItem (Id INTEGER PRIMARY KEY, Val TEXT);" +
            "INSERT INTO TakeItem VALUES (1,'a');" +
            "INSERT INTO TakeItem VALUES (2,'b');" +
            "INSERT INTO TakeItem VALUES (3,'c');";
        setup.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = await ctx.Query<TakeItem>().Take(2).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    /// <summary>
    /// Concurrent queries from contexts backed by different provider subclasses
    /// for the same CLR type must never corrupt each other's results.
    /// </summary>
    [Fact]
    public async Task ConcurrentContexts_DifferentProviders_NoCachePoisoning()
    {
        // Each task uses its own connection to avoid cross-task disposal races.
        var tasks = Enumerable.Range(0, 20).Select(async i =>
        {
            var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var setup = cn.CreateCommand();
            setup.CommandText =
                "CREATE TABLE ConcItem (Id INTEGER PRIMARY KEY, Name TEXT);" +
                "INSERT INTO ConcItem VALUES (1,'one');" +
                "INSERT INTO ConcItem VALUES (2,'two');";
            setup.ExecuteNonQuery();

            var provider = i % 2 == 0
                ? (DatabaseProvider)new SqliteProvider()
                : new CustomSqliteProvider();
            using var ctx = new DbContext(cn, provider);
            var results = await ctx.Query<ConcItem>().Where(x => x.Id == 1).ToListAsync();
            Assert.Single(results);
            Assert.Equal("one", results[0].Name);
            cn.Close();
        });

        await Task.WhenAll(tasks);
    }

    // ── Entity types ──────────────────────────────────────────────────────

    public class CacheProduct   { public int Id { get; set; } public string Name { get; set; } = ""; public bool Active { get; set; } }
    public class MappedEntity   { public int Id { get; set; } public string Label { get; set; } = ""; }
    public class OrderedEntity  { public int Id { get; set; } public string Name { get; set; } = ""; public int Score { get; set; } }
    public class TakeItem       { public int Id { get; set; } public string Val { get; set; } = ""; }
    public class ConcItem       { public int Id { get; set; } public string Name { get; set; } = ""; }

    // ── Provider stubs ────────────────────────────────────────────────────

    /// <summary>SqliteProvider subclass with a neutral class name.</summary>
    private sealed class CustomSqliteProvider : SqliteProvider { }

    /// <summary>Provider that opts into OFFSET/FETCH paging via capability property.</summary>
    private sealed class FetchOffsetCapableProvider : SqliteProvider
    {
        public override bool UsesFetchOffsetPaging => true;
    }
}
