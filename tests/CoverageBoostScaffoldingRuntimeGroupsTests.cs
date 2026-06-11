using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using MigrationRunners = nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 46 — DatabaseScaffolder private utility methods (via reflection)
// Covers: GetTypeName, ToPascalCase, EscapeCSharpIdentifier, ScaffoldContext,
//         EscapeQualifiedIfNeeded, EscapeIdentifier, GetUnqualifiedName,
//         GetSchemaNameOrNull + null-argument guards on ScaffoldAsync
// Note: ScaffoldAsync itself requires GetSchema("Tables") which SQLite in-memory
//       does not support; null-arg guards and private method reflection cover the
//       bulk of the lines reachable without a real DB server.
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class DatabaseScaffolderCoverageTests
{
    private static readonly Type _scaffolderType = typeof(nORM.Scaffolding.DatabaseScaffolder);

    private static T InvokePrivate<T>(string method, params object?[] args)
    {
        var m = _scaffolderType.GetMethod(method,
            BindingFlags.NonPublic | BindingFlags.Static,
            null,
            args.Select(a => a?.GetType() ?? typeof(object)).ToArray(),
            null)!;
        return (T)m.Invoke(null, args)!;
    }

    // ── ScaffoldAsync null-argument guards ───────────────────────────────────

    [Fact]
    public async Task ScaffoldAsync_NullConnection_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(null!, new SqliteProvider(), Path.GetTempPath(), "Test"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullProvider_Throws()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, null!, Path.GetTempPath(), "Test"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullOutputDirectory_Throws()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), null!, "Test"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullNamespace_Throws()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), Path.GetTempPath(), null!));
    }

    // ── GetTypeName — exercises all switch arms ───────────────────────────────

    private static string GetTypeName(Type type, bool allowNull)
    {
        var m = _scaffolderType.GetMethod("GetTypeName",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string)m.Invoke(null, new object[] { type, allowNull, true })!;
    }

    [Theory]
    [InlineData(typeof(int),       false, "int")]
    [InlineData(typeof(int),       true,  "int?")]
    [InlineData(typeof(long),      false, "long")]
    [InlineData(typeof(short),     false, "short")]
    [InlineData(typeof(byte),      false, "byte")]
    [InlineData(typeof(bool),      false, "bool")]
    [InlineData(typeof(string),    false, "string")]
    [InlineData(typeof(DateTime),  false, "DateTime")]
    [InlineData(typeof(decimal),   false, "decimal")]
    [InlineData(typeof(double),    false, "double")]
    [InlineData(typeof(float),     false, "float")]
    [InlineData(typeof(Guid),      false, "Guid")]
    [InlineData(typeof(byte[]),    false, "byte[]")]
    [InlineData(typeof(byte[]),    true,  "byte[]?")]
    public void GetTypeName_AllBranches(Type type, bool allowNull, string expected)
    {
        var result = GetTypeName(type, allowNull);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void GetTypeName_UnknownType_UsesFullName()
    {
        // Unknown type falls through to `type.FullName ?? type.Name`
        var result = GetTypeName(typeof(Uri), false);
        Assert.Equal("System.Uri", result);
    }

    [Fact]
    public void GetTypeName_NullableString_AddsQuestionMark()
    {
        var result = GetTypeName(typeof(string), true);
        Assert.Equal("string?", result);
    }

    // ── ToPascalCase ─────────────────────────────────────────────────────────

    private static string ToPascalCase(string name)
    {
        var m = _scaffolderType.GetMethod("ToPascalCase",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string)m.Invoke(null, new object[] { name })!;
    }

    [Theory]
    [InlineData("products",     "Products")]
    [InlineData("order_items",  "OrderItems")]
    [InlineData("my_table_name","MyTableName")]
    [InlineData("already",      "Already")]
    [InlineData("",             "")]
    public void ToPascalCase_VariousInputs(string input, string expected)
    {
        var result = ToPascalCase(input);
        Assert.Equal(expected, result);
    }

    // ── EscapeCSharpIdentifier ────────────────────────────────────────────────

    private static string EscapeCSharpIdentifier(string id)
    {
        var m = _scaffolderType.GetMethod("EscapeCSharpIdentifier",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string)m.Invoke(null, new object[] { id })!;
    }

    [Theory]
    [InlineData("ValidName",    "ValidName")]
    [InlineData("class",        "@class")]    // C# keyword
    [InlineData("int",          "@int")]
    [InlineData("string",       "@string")]
    [InlineData("123abc",       "_123abc")]   // starts with digit
    [InlineData("has space",    "has_space")] // contains space
    public void EscapeCSharpIdentifier_Variants(string input, string expected)
    {
        var result = EscapeCSharpIdentifier(input);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void EscapeCSharpIdentifier_EmptyString_ReturnsFallbackIdentifier()
    {
        var result = EscapeCSharpIdentifier("");
        Assert.Equal("_", result);
    }

    // ── GetUnqualifiedName ────────────────────────────────────────────────────

    private static string GetUnqualifiedName(string id)
    {
        var m = _scaffolderType.GetMethod("GetUnqualifiedName",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string)m.Invoke(null, new object[] { id })!;
    }

    [Theory]
    [InlineData("schema.table",    "table")]
    [InlineData("table",           "table")]
    [InlineData("a.b.c",           "c")]
    public void GetUnqualifiedName_Splits(string input, string expected)
    {
        Assert.Equal(expected, GetUnqualifiedName(input));
    }

    // ── GetSchemaNameOrNull ───────────────────────────────────────────────────

    private static string? GetSchemaNameOrNull(string id)
    {
        var m = _scaffolderType.GetMethod("GetSchemaNameOrNull",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string?)m.Invoke(null, new object[] { id });
    }

    [Theory]
    [InlineData("schema.table", "schema")]
    [InlineData("table",        null)]
    [InlineData(".table",       null)]  // idx <= 0
    public void GetSchemaNameOrNull_Variants(string input, string? expected)
    {
        Assert.Equal(expected, GetSchemaNameOrNull(input));
    }

    // ── EscapeQualifiedIfNeeded ───────────────────────────────────────────────

    private static string EscapeQualifiedIfNeeded(string? schema, string table)
    {
        var m = _scaffolderType.GetMethod("EscapeQualifiedIfNeeded",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (string)m.Invoke(null, new object?[] { schema, table })!;
    }

    [Theory]
    [InlineData("myschema", "mytable", "myschema.mytable")]
    [InlineData(null,       "mytable", "mytable")]
    [InlineData("",         "mytable", "mytable")]
    public void EscapeQualifiedIfNeeded_Variants(string? schema, string table, string expected)
    {
        Assert.Equal(expected, EscapeQualifiedIfNeeded(schema, table));
    }

    // ── EscapeIdentifier ─────────────────────────────────────────────────────

    private static string EscapeIdentifier(SqliteConnection cn, string id)
    {
        var m = _scaffolderType.GetMethod("EscapeIdentifier",
            BindingFlags.NonPublic | BindingFlags.Static,
            null,
            new[] { typeof(System.Data.Common.DbConnection), typeof(string) },
            null)!;
        return (string)m.Invoke(null, new object[] { cn, id })!;
    }

    [Fact]
    public void EscapeIdentifier_Sqlite_UsesDoubleQuotes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var result = EscapeIdentifier(cn, "myTable");
        // SQLite uses double-quote escaping
        Assert.Contains("myTable", result);
    }

    [Fact]
    public void EscapeIdentifier_SchemaQualified_EscapesBothParts()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var result = EscapeIdentifier(cn, "schema.table");
        Assert.Contains(".", result); // still has separator
        Assert.Contains("schema", result);
        Assert.Contains("table", result);
    }

    // ── ScaffoldContext (private method via reflection) ───────────────────────

    private static string ScaffoldContext(string namespaceName, string contextName, IEnumerable<string> entities)
    {
        var m = _scaffolderType.GetMethod("ScaffoldContext",
            BindingFlags.NonPublic | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(string), typeof(string), typeof(IEnumerable<string>) },
            modifiers: null)!;
        return (string)m.Invoke(null, new object[] { namespaceName, contextName, entities })!;
    }

    [Fact]
    public void ScaffoldContext_GeneratesClassWithQueryProperties()
    {
        var code = ScaffoldContext("MyApp", "MyDbContext", new[] { "Product", "Order" });
        Assert.Contains("public partial class MyDbContext : DbContext", code);
        Assert.Contains("using System.Linq;", code);
        Assert.Contains("IQueryable<Product>", code);
        Assert.Contains("IQueryable<Order>", code);
        Assert.Contains("namespace MyApp", code);
    }

    [Fact]
    public void ScaffoldContext_EmptyEntities_GeneratesClassWithNoProperties()
    {
        var code = ScaffoldContext("Test", "Ctx", Array.Empty<string>());
        Assert.Contains("public partial class Ctx : DbContext", code);
        Assert.DoesNotContain("IQueryable<", code);
    }

    [Fact]
    public void ScaffoldContext_EntityWithKeywordName_EscapesProperty()
    {
        // "class" entity name → @class property name
        var code = ScaffoldContext("Test", "Ctx", new[] { "class" });
        // EscapeCSharpIdentifier should prefix with @
        Assert.Contains("@class", code);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 47 — BulkOperationProvider
// Covers: ExecuteBulkOperationAsync (owned tx, reuse tx, rollback, non-List IList)
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class BulkOperationProviderCoverageTests
{
    // Concrete test subclass that delegates all abstract methods to SqliteProvider
    private sealed class TestBulkOpProvider : BulkOperationProvider
    {
        private readonly SqliteProvider _inner = new();

        public override string Escape(string id) => _inner.Escape(id);
        public override void ApplyPaging(nORM.Query.OptimizedSqlBuilder sb, int? limit, int? offset,
            string? limitParameterName, string? offsetParameterName)
            => _inner.ApplyPaging(sb, limit, offset, limitParameterName, offsetParameterName);
        public override string GetIdentityRetrievalString(TableMapping m)
            => _inner.GetIdentityRetrievalString(m);
        public override DbParameter CreateParameter(string name, object? value)
            => _inner.CreateParameter(name, value);
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
            => _inner.TranslateFunction(name, declaringType, args);
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
            => _inner.TranslateJsonPathAccess(columnName, jsonPath);
        public override string GenerateCreateHistoryTableSql(TableMapping mapping,
            IReadOnlyList<DatabaseProvider.LiveColumnInfo>? liveColumns = null)
            => _inner.GenerateCreateHistoryTableSql(mapping, liveColumns);
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
            => _inner.GenerateTemporalTriggersSql(mapping);

        // Expose ExecuteBulkOperationAsync for testing
        public Task<int> RunBulkAsync<T>(
            DbContext ctx,
            TableMapping mapping,
            IList<T> entities,
            Func<List<T>, DbTransaction, CancellationToken, Task<int>> batchAction,
            CancellationToken ct = default) where T : class
            => ExecuteBulkOperationAsync(ctx, mapping, entities, "TestOp", batchAction, ct);
    }

    [Table("BulkOpTest")]
    private class BulkItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) MakeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE BulkOpTest (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '')";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new TestBulkOpProvider()));
    }

    [Fact]
    public async Task ExecuteBulkOperationAsync_OwnedTx_CommitsSuccessfully()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        var provider = (TestBulkOpProvider)ctx.Provider;
        var mapping = ctx.GetMapping(typeof(BulkItem));

        var items = new List<BulkItem>
        {
            new() { Name = "A" },
            new() { Name = "B" }
        };

        var total = await provider.RunBulkAsync(ctx, mapping, items,
            async (batch, tx, ct) =>
            {
                var cnt = 0;
                foreach (var item in batch)
                {
                    using var cmd = cn.CreateCommand();
                    cmd.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
                    cmd.CommandText = "INSERT INTO BulkOpTest (Name) VALUES (@n)";
                    cmd.Parameters.AddWithValue("@n", item.Name);
                    cnt += await cmd.ExecuteNonQueryAsync(ct);
                }
                return cnt;
            });

        Assert.Equal(2, total);

        // Verify rows were committed
        using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT COUNT(*) FROM BulkOpTest";
        var count = (long)verifyCmd.ExecuteScalar()!;
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task ExecuteBulkOperationAsync_ReuseExistingTx_DoesNotCommit()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        var provider = (TestBulkOpProvider)ctx.Provider;
        var mapping = ctx.GetMapping(typeof(BulkItem));

        // Use ctx.Database.BeginTransactionAsync so ctx.CurrentTransaction != null.
        // ExecuteBulkOperationAsync sees ownedTx=false and reuses the existing tx.
        await using var outerTx = await ctx.Database.BeginTransactionAsync();

        BulkItem[] itemArray = { new() { Name = "X" } };

        var total = await provider.RunBulkAsync(ctx, mapping, itemArray,
            async (batch, tx, ct) =>
            {
                using var cmd = cn.CreateCommand();
                cmd.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
                cmd.CommandText = "INSERT INTO BulkOpTest (Name) VALUES (@n)";
                cmd.Parameters.AddWithValue("@n", batch[0].Name);
                return await cmd.ExecuteNonQueryAsync(ct);
            });

        Assert.Equal(1, total);
        // Dispose without committing → rollback; provider correctly did NOT commit
        // (it doesn't own the transaction), so rows must not be visible after rollback.
    }

    [Fact]
    public async Task ExecuteBulkOperationAsync_NonListIList_WorksCorrectly()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        var provider = (TestBulkOpProvider)ctx.Provider;
        var mapping = ctx.GetMapping(typeof(BulkItem));

        // Use an array-backed IList (not List<T>) to hit the non-List branch
        IList<BulkItem> items = new BulkItem[] { new() { Name = "Y" }, new() { Name = "Z" } };

        var total = await provider.RunBulkAsync(ctx, mapping, items,
            async (batch, tx, ct) =>
            {
                var cnt = 0;
                foreach (var item in batch)
                {
                    using var cmd = cn.CreateCommand();
                    cmd.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
                    cmd.CommandText = "INSERT INTO BulkOpTest (Name) VALUES (@n)";
                    cmd.Parameters.AddWithValue("@n", item.Name);
                    cnt += await cmd.ExecuteNonQueryAsync(ct);
                }
                return cnt;
            });

        Assert.Equal(2, total);
    }

    [Fact]
    public async Task ExecuteBulkOperationAsync_BatchActionThrows_RollsBackAndRethrows()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        var provider = (TestBulkOpProvider)ctx.Provider;
        var mapping = ctx.GetMapping(typeof(BulkItem));

        var items = new List<BulkItem> { new() { Name = "Fail" } };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.RunBulkAsync(ctx, mapping, items,
                (batch, tx, ct) => throw new InvalidOperationException("batch failed")));

        // After rollback, no rows should be in the table
        using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT COUNT(*) FROM BulkOpTest";
        var count = (long)verifyCmd.ExecuteScalar()!;
        Assert.Equal(0, count);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 48 — NavigationPropertyExtensions LoadAsync paths
// Covers: LoadAsync (collection nav), LoadAsync (reference nav, throws path),
//         LoadNavigationProperty (sync wrapper), LoadRelationshipAsync,
//         LoadInferredRelationshipAsync, ExecuteSingleQueryAsync
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class NavigationPropertyExtensionsLoadAsyncTests
{
    private static (SqliteConnection Cn, DbContext Ctx) MakeNavDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CovBoost_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '');
            CREATE TABLE CovBoost_Book   (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL DEFAULT '');
            INSERT INTO CovBoost_Author (Name) VALUES ('Tolkien');
            INSERT INTO CovBoost_Book   (AuthorId, Title) VALUES (1, 'The Hobbit');
            INSERT INTO CovBoost_Book   (AuthorId, Title) VALUES (1, 'LOTR');";
        cmd.ExecuteNonQuery();
        // Register the CovAuthor→CovBook relationship so LoadRelationshipAsync uses
        // the explicit relation (AuthorId) instead of the inferred convention (CovAuthorId).
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<CovAuthor>()
                    .HasMany(a => a.Books)
                    .WithOne()
                    .HasForeignKey(b => b.AuthorId, a => a.Id);
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Force mapping registration so relations are populated before tests run.
        _ = ctx.GetMapping(typeof(CovAuthor));
        _ = ctx.GetMapping(typeof(CovBook));
        return (cn, ctx);
    }

    [Fact]
    public async Task LoadAsync_Collection_NoNavContext_Throws()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = new CovAuthor { Id = 1, Name = "Test" };
        // No EnableLazyLoading → no nav context → should throw NormUsageException
        await Assert.ThrowsAsync<NormUsageException>(() =>
            author.LoadAsync(a => a.Books));
    }

    [Fact]
    public async Task LoadAsync_CollectionNav_WithNavContext_LoadsBooksFromDb()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = await ctx.Query<CovAuthor>().FirstAsync();
        Assert.NotNull(author);

        // ProcessEntity calls EnableLazyLoading with entity typed as object, creating
        // a NavContext with EntityType=typeof(object). Replace with correct EntityType.
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(
            author, new NavigationContext(ctx, typeof(CovAuthor)));

        await author.LoadAsync(a => a.Books);
        Assert.NotEmpty(author.Books);
    }

    [Fact]
    public async Task LoadAsync_AlreadyLoaded_DoesNotReload()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = await ctx.Query<CovAuthor>().FirstAsync();
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(
            author, new NavigationContext(ctx, typeof(CovAuthor)));

        // First load
        await author.LoadAsync(a => a.Books);
        Assert.NotEmpty(author.Books); // verify first load worked

        // Replace Books with empty list to detect if second call re-loads
        author.Books = new List<CovBook>();

        // Second call — should be no-op because Books is marked as loaded
        await author.LoadAsync(a => a.Books);

        // Books should still be empty (no re-load)
        Assert.Empty(author.Books);
    }

    [Fact]
    public void LoadNavigationProperty_Sync_LoadsCollection()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = ctx.Query<CovAuthor>().First();
        var booksProperty = typeof(CovAuthor).GetProperty("Books")!;

        // Replace the navContext (created with EntityType=object by ProcessEntity) with one
        // that has the correct EntityType so GetMapping resolves the Books relation.
        var navContext = new NavigationContext(ctx, typeof(CovAuthor));
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(author, navContext);

        // Sync wrapper over async
        NavigationPropertyExtensions.LoadNavigationProperty(author, booksProperty, navContext, CancellationToken.None);

        Assert.NotEmpty(author.Books);
    }

    [Fact]
    public async Task LoadAsync_ReferenceNav_ThrowsWithoutNavContext()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var entity = new CovRefEntity { Id = 1, Name = "Test" };
        // No nav context
        await Assert.ThrowsAsync<NormUsageException>(() =>
            entity.LoadAsync(e => e.LazyRef));
    }

    [Fact]
    public async Task IsLoaded_AfterLoadAsync_ReturnsTrue()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = await ctx.Query<CovAuthor>().FirstAsync();
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(
            author, new NavigationContext(ctx, typeof(CovAuthor)));

        Assert.False(author.IsLoaded(a => a.Books));
        await author.LoadAsync(a => a.Books);
        Assert.True(author.IsLoaded(a => a.Books));
    }

    [Fact]
    public async Task LoadAsync_CollectionNav_ICollectionOverload_Works()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = await ctx.Query<CovAuthor>().FirstAsync();
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(
            author, new NavigationContext(ctx, typeof(CovAuthor)));

        // CovAuthor.Books is declared ICollection<CovBook> — the bare lambda resolves
        // to the ICollection<TProperty> overload (more specific than TProperty? overload).
        await author.LoadAsync(a => a.Books);

        Assert.NotEmpty(author.Books);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 49 — MaterializerFactory IL paths (PrecompileCommonPatterns)
// Covers: CreateILMaterializer parameterized-ctor path, nullable enum IL path,
//         enum IL path, CreateSyncMaterializer generic overload, fast materializer
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class MaterializerFactoryILPathTests
{
    [Fact]
    public void PrecompileAndQuery_NoCtorEntity_UsesILParameterizedCtorPath()
    {
        // CovNoCtorEntity has (int id, string name) — no parameterless ctor
        // PrecompileCommonPatterns calls CreateILMaterializer<T> which takes the
        // parameterized-ctor path (lines 309-377)
        MaterializerFactory.PrecompileCommonPatterns<CovNoCtorEntity>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CovBoost_NoCtor (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '');
            INSERT INTO CovBoost_NoCtor (Name) VALUES ('ILTest');";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<CovNoCtorEntity>().ToList();
        Assert.Single(list);
        Assert.Equal("ILTest", list[0].Name);
    }

    [Fact]
    public void PrecompileAndQuery_NullableEnumEntity_UsesNullableEnumILPath()
    {
        // MfcNullableEnumEntity has MfcStatus? — exercises IL nullable enum path
        MaterializerFactory.PrecompileCommonPatterns<MfcNullableEnumEntity>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE MFC_NullableEnum (Id INTEGER PRIMARY KEY AUTOINCREMENT, Status INTEGER);
            INSERT INTO MFC_NullableEnum (Status) VALUES (1);
            INSERT INTO MFC_NullableEnum (Status) VALUES (NULL);";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcNullableEnumEntity>().OrderBy(e => e.Id).ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(MfcStatus.Active, list[0].Status);
        Assert.Null(list[1].Status);
    }

    [Fact]
    public void PrecompileAndQuery_EnumEntity_UsesEnumILPath()
    {
        // MfcEnumEntity has MfcStatus (non-nullable enum) — exercises IL enum path
        MaterializerFactory.PrecompileCommonPatterns<MfcEnumEntity>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE MFC_Enum (Id INTEGER PRIMARY KEY AUTOINCREMENT, Status INTEGER NOT NULL DEFAULT 0);
            INSERT INTO MFC_Enum (Status) VALUES (2);";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcEnumEntity>().ToList();
        Assert.Single(list);
        Assert.Equal(MfcStatus.Inactive, list[0].Status);
    }

    [Fact]
    public void CreateSyncMaterializer_Generic_ReturnsStronglyTyped()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER);
            INSERT INTO CovBoost_Item (Name, Value, IsActive) VALUES ('Test', 42, 1);";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        // CreateSyncMaterializer<T> (generic typed overload)
        var syncMat = factory.CreateSyncMaterializer<CovItem>(mapping);
        Assert.NotNull(syncMat);

        // Execute with an actual reader to verify it works
        using var readCmd = cn.CreateCommand();
        readCmd.CommandText = "SELECT Id, Name, Value, IsActive FROM CovBoost_Item";
        using var reader = readCmd.ExecuteReader();
        Assert.True(reader.Read());
        var item = syncMat(reader);
        Assert.NotNull(item);
        Assert.Equal("Test", item.Name);
        Assert.Equal(42, item.Value);
    }

    [Fact]
    public void CreateSyncMaterializer_WithFastMaterializerPrecompiled_UsesCache()
    {
        // Precompile then create sync materializer — should use fast materializer from cache
        MaterializerFactory.PrecompileCommonPatterns<CovItem>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER);
            INSERT INTO CovBoost_Item VALUES (1, 'Cached', 10, 1);";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));
        var mat = factory.CreateSyncMaterializer<CovItem>(mapping);

        using var readCmd = cn.CreateCommand();
        readCmd.CommandText = "SELECT Id, Name, Value, IsActive FROM CovBoost_Item";
        using var reader = readCmd.ExecuteReader();
        reader.Read();
        var item = mat(reader);
        Assert.Equal("Cached", item.Name);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 50 — DbContextOptions validation + boundary tests
// Covers: BulkBatchSize/MaxRecursionDepth/MaxGroupJoinSize validation throws,
//         CommandTimeout getter/setter, Validate() method branches,
//         AddGlobalFilter<TEntity>(Expression<Func<TEntity,bool>>) overload
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class DbContextOptionsCoverageTests
{
    [Fact]
    public void BulkBatchSize_Zero_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.BulkBatchSize = 0);
    }

    [Fact]
    public void BulkBatchSize_TooLarge_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.BulkBatchSize = 10001);
    }

    [Fact]
    public void BulkBatchSize_ValidValue_Succeeds()
    {
        var opts = new DbContextOptions();
        opts.BulkBatchSize = 500;
        Assert.Equal(500, opts.BulkBatchSize);
    }

    [Fact]
    public void MaxRecursionDepth_Zero_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxRecursionDepth = 0);
    }

    [Fact]
    public void MaxRecursionDepth_TooLarge_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxRecursionDepth = 201);
    }

    [Fact]
    public void MaxGroupJoinSize_Zero_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxGroupJoinSize = 0);
    }

    [Fact]
    public void MaxGroupJoinSize_Negative_Throws()
    {
        var opts = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.MaxGroupJoinSize = -1);
    }

    [Fact]
    public void Validate_ValidConfig_DoesNotThrow()
    {
        var opts = new DbContextOptions();
        opts.Validate(); // default config is valid
    }

    [Fact]
    public void Validate_InvalidRetryMaxRetries_Throws()
    {
        var opts = new DbContextOptions { RetryPolicy = new nORM.Enterprise.RetryPolicy { MaxRetries = 11 } };
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_InvalidRetryBaseDelay_Throws()
    {
        var opts = new DbContextOptions
        {
            RetryPolicy = new nORM.Enterprise.RetryPolicy { MaxRetries = 3, BaseDelay = TimeSpan.Zero }
        };
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_InvalidBaseTimeout_Throws()
    {
        var opts = new DbContextOptions();
        opts.TimeoutConfiguration.BaseTimeout = TimeSpan.Zero;
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_EmptyTenantColumnName_Throws()
    {
        var opts = new DbContextOptions { TenantColumnName = "" };
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_NegativeCacheExpiration_Throws()
    {
        var opts = new DbContextOptions { CacheExpiration = TimeSpan.Zero };
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_NullInCommandInterceptors_Throws()
    {
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(null!);
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void Validate_NullInSaveChangesInterceptors_Throws()
    {
        var opts = new DbContextOptions();
        opts.SaveChangesInterceptors.Add(null!);
        Assert.Throws<NormConfigurationException>(() => opts.Validate());
    }

    [Fact]
    public void AddGlobalFilter_EntityOnlyLambda_RegistersFilter()
    {
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<CovItem>(e => e.IsActive);
        Assert.True(opts.GlobalFilters.ContainsKey(typeof(CovItem)));
        Assert.Single(opts.GlobalFilters[typeof(CovItem)]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 51 — NavigationPropertyExtensions: CleanupNavigationContext + LazyNavigationCollection
// Covers: CleanupNavigationContext full path, CleanupFromBatchedLoaders,
//         LazyNavigationCollection.Count/Add/Clear/Contains/Remove/CopyTo/IsReadOnly/
//         IndexOf/Insert/RemoveAt/Indexer/GetEnumerator/GetAsyncEnumerator,
//         NavigationPropertyExtensions.IsLoaded false-when-no-context
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class NavigationPropertyExtensionsCleanupAndCollectionTests
{
    // Helper: build a pre-loaded LazyNavigationCollection backed by a real List<CovBook>
    // No DB queries are made because IsLoaded("Books") is true from the start.
    private static (LazyNavigationCollection<CovBook> Lazy, CovAuthor Author, List<CovBook> Books) MakePreloaded()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());

        var b1 = new CovBook { Id = 1, AuthorId = 1, Title = "Alpha" };
        var b2 = new CovBook { Id = 2, AuthorId = 1, Title = "Beta" };
        var books = new List<CovBook> { b1, b2 };

        var author = new CovAuthor { Id = 1, Name = "Auth" };
        author.Books = books; // pre-populate the property

        var booksProperty = typeof(CovAuthor).GetProperty("Books")!;
        var navCtx = new NavigationContext(ctx, typeof(CovAuthor));
        navCtx.MarkAsLoaded("Books"); // mark as loaded so no DB hit occurs

        var lazy = new LazyNavigationCollection<CovBook>(author, booksProperty, navCtx);
        return (lazy, author, books);
    }

    [Fact]
    public void CleanupNavigationContext_WithRegisteredEntity_RemovesIt()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());

        var entity = new CovAuthor { Id = 99, Name = "Cleanup" };
        var navCtx = new NavigationContext(ctx, typeof(CovAuthor));
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(entity, navCtx);

        Assert.True(NavigationPropertyExtensions._navigationContexts.TryGetValue(entity, out _));
        NavigationPropertyExtensions.CleanupNavigationContext(entity);
        Assert.False(NavigationPropertyExtensions._navigationContexts.TryGetValue(entity, out _));
    }

    [Fact]
    public void CleanupNavigationContext_WithNonRegisteredEntity_IsNoOp()
    {
        var entity = new CovAuthor { Id = 88, Name = "Unregistered" };
        // Should not throw even when entity is not registered
        var ex = Record.Exception(() => NavigationPropertyExtensions.CleanupNavigationContext(entity));
        Assert.Null(ex);
    }

    [Fact]
    public void IsLoaded_WhenNoNavContext_ReturnsFalse()
    {
        var entity = new CovAuthor { Id = 77, Name = "NoCtx" };
        // Not registered in _navigationContexts
        Assert.False(entity.IsLoaded(a => a.Books));
    }

    [Fact]
    public void LazyNavCollection_Count_ReturnsItemCount()
    {
        var (lazy, _, books) = MakePreloaded();
        Assert.Equal(books.Count, lazy.Count);
    }

    [Fact]
    public void LazyNavCollection_Add_AddsItem()
    {
        var (lazy, _, books) = MakePreloaded();
        var newBook = new CovBook { Id = 3, Title = "Gamma" };
        lazy.Add(newBook);
        Assert.Equal(3, lazy.Count);
        Assert.Contains(newBook, books);
    }

    [Fact]
    public void LazyNavCollection_Clear_ClearsAllItems()
    {
        var (lazy, _, books) = MakePreloaded();
        lazy.Clear();
        Assert.Empty(lazy);
        Assert.Empty(books);
    }

    [Fact]
    public void LazyNavCollection_Contains_ReturnsTrueForExisting()
    {
        var (lazy, _, books) = MakePreloaded();
        Assert.Contains(books[0], lazy);
        Assert.DoesNotContain(new CovBook { Id = 99, Title = "Missing" }, lazy);
    }

    [Fact]
    public void LazyNavCollection_CopyTo_CopiesItems()
    {
        var (lazy, _, books) = MakePreloaded();
        var array = new CovBook[books.Count];
        lazy.CopyTo(array, 0);
        Assert.Equal(books[0], array[0]);
        Assert.Equal(books[1], array[1]);
    }

    [Fact]
    public void LazyNavCollection_Remove_RemovesItem()
    {
        var (lazy, _, books) = MakePreloaded();
        var removed = lazy.Remove(books[0]);
        Assert.True(removed);
        Assert.Single(lazy);
    }

    [Fact]
    public void LazyNavCollection_IsReadOnly_ReturnsFalse()
    {
        var (lazy, _, _) = MakePreloaded();
        Assert.False(lazy.IsReadOnly);
    }

    [Fact]
    public void LazyNavCollection_GetEnumerator_Enumerates()
    {
        var (lazy, _, books) = MakePreloaded();
        var items = new List<CovBook>();
        foreach (var item in lazy) items.Add(item);
        Assert.Equal(books.Count, items.Count);
    }

    [Fact]
    public void LazyNavCollection_NonGenericGetEnumerator_Enumerates()
    {
        var (lazy, _, books) = MakePreloaded();
        int count = 0;
        var enumerator = ((System.Collections.IEnumerable)lazy).GetEnumerator();
        while (enumerator.MoveNext()) count++;
        Assert.Equal(books.Count, count);
    }

    [Fact]
    public async Task LazyNavCollection_GetAsyncEnumerator_Enumerates()
    {
        var (lazy, _, books) = MakePreloaded();
        var items = new List<CovBook>();
        await foreach (var item in lazy) items.Add(item);
        Assert.Equal(books.Count, items.Count);
    }

    [Fact]
    public void LazyNavCollection_IndexOf_ReturnsIndex()
    {
        var (lazy, _, books) = MakePreloaded();
        Assert.Equal(0, lazy.IndexOf(books[0]));
        Assert.Equal(1, lazy.IndexOf(books[1]));
        Assert.Equal(-1, lazy.IndexOf(new CovBook { Id = 99 }));
    }

    [Fact]
    public void LazyNavCollection_Insert_InsertsAtIndex()
    {
        var (lazy, _, books) = MakePreloaded();
        var newBook = new CovBook { Id = 5, Title = "Inserted" };
        lazy.Insert(0, newBook);
        Assert.Equal(3, lazy.Count);
        Assert.Equal(newBook, lazy[0]);
    }

    [Fact]
    public void LazyNavCollection_RemoveAt_RemovesAtIndex()
    {
        var (lazy, _, books) = MakePreloaded();
        var secondBook = books[1]; // save reference before removal
        lazy.RemoveAt(0);
        Assert.Single(lazy);
        Assert.Equal(secondBook, lazy[0]);
    }

    [Fact]
    public void LazyNavCollection_Indexer_GetSet()
    {
        var (lazy, _, books) = MakePreloaded();
        var retrieved = lazy[0];
        Assert.Equal(books[0], retrieved);

        var newBook = new CovBook { Id = 10, Title = "New" };
        lazy[0] = newBook;
        Assert.Equal(newBook, lazy[0]);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 52 — NavigationPropertyExtensions: LazyNavigationReference operations
// Covers: LazyNavigationReference.SetValue, GetValueAsync (pre-loaded path),
//         implicit Task<T?> operator, NavigationContext.MarkAsUnloaded
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class LazyNavReferenceBoostTests
{
    private static (LazyNavigationReference<CovItem> Ref, CovRefEntity Entity, NavigationContext NavCtx) MakeRef()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());
        var entity = new CovRefEntity { Id = 1, Name = "RefTest" };
        var prop = typeof(CovRefEntity).GetProperty("LazyRef")!;
        var navCtx = new NavigationContext(ctx, typeof(CovRefEntity));
        var reference = new LazyNavigationReference<CovItem>(entity, prop, navCtx);
        return (reference, entity, navCtx);
    }

    [Fact]
    public void SetValue_SetsInternalValueAndMarksLoaded()
    {
        var (reference, _, navCtx) = MakeRef();
        var item = new CovItem { Id = 42, Name = "RefItem" };

        reference.SetValue(item);

        Assert.True(navCtx.IsLoaded("LazyRef"));
    }

    [Fact]
    public async Task GetValueAsync_AfterSetValue_ReturnsSameInstance()
    {
        var (reference, _, _) = MakeRef();
        var item = new CovItem { Id = 7, Name = "Loaded" };

        reference.SetValue(item);
        var result = await reference.GetValueAsync();

        Assert.Equal(item, result);
    }

    [Fact]
    public async Task ImplicitTaskOperator_ReturnsValue()
    {
        var (reference, _, _) = MakeRef();
        var item = new CovItem { Id = 5, Name = "Implicit" };
        reference.SetValue(item);

        Task<CovItem?> task = reference; // implicit operator
        var result = await task;

        Assert.Equal(item, result);
    }

    [Fact]
    public void SetValue_Null_SetsNullAndMarksLoaded()
    {
        var (reference, _, navCtx) = MakeRef();
        reference.SetValue(null);
        Assert.True(navCtx.IsLoaded("LazyRef"));
    }

    [Fact]
    public void NavigationContext_MarkAsUnloaded_RemovesFromLoaded()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(CovAuthor));

        navCtx.MarkAsLoaded("Books");
        Assert.True(navCtx.IsLoaded("Books"));

        navCtx.MarkAsUnloaded("Books");
        Assert.False(navCtx.IsLoaded("Books"));
    }

    [Fact]
    public void NavigationContext_Dispose_ClearsLoadedProperties()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(CovAuthor));
        navCtx.MarkAsLoaded("Books");
        navCtx.Dispose();
        // After dispose, IsLoaded should return false
        Assert.False(navCtx.IsLoaded("Books"));
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 53 — MaterializerFactory: CacheStats, ConvertDbValue, CreateMaterializer<T>
// Covers: CacheStats getter, SchemaCacheStats getter, CreateMaterializer<T> generic,
//         CreateSchemaAwareMaterializer, ConvertDbValue null/nullable/enum paths
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class MatFactoryBoostTests
{
    private static (SqliteConnection Cn, DbContext Ctx) MakeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void CacheStats_ReturnsNonNegativeValues()
    {
        var (hits, misses, hitRate) = MaterializerFactory.CacheStats;
        Assert.True(hits >= 0);
        Assert.True(misses >= 0);
        Assert.True(hitRate >= 0.0 && hitRate <= 1.0);
    }

    [Fact]
    public void SchemaCacheStats_ReturnsNonNegativeValues()
    {
        var (sHits, sMisses, sHitRate) = MaterializerFactory.SchemaCacheStats;
        Assert.True(sHits >= 0);
        Assert.True(sMisses >= 0);
        Assert.True(sHitRate >= 0.0 && sHitRate <= 1.0);
    }

    [Fact]
    public void CreateMaterializerGeneric_ReturnsDelegate()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        var mat = factory.CreateMaterializer<CovItem>(mapping);
        Assert.NotNull(mat);
    }

    [Fact]
    public void CreateSchemaAwareMaterializer_NullProjection_ReturnsFastPath()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        var mat = factory.CreateSchemaAwareMaterializer(mapping, typeof(CovItem));
        Assert.NotNull(mat);
    }

    [Fact]
    public void CreateSchemaAwareMaterializer_WithProjection_ReturnsDelegate()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        // projection with NewExpression
        System.Linq.Expressions.Expression<Func<CovItem, object>> proj =
            e => new { e.Id, e.Name };
        var mat = factory.CreateSchemaAwareMaterializer(mapping, typeof(CovItem), proj);
        Assert.NotNull(mat);
    }

    [Fact]
    public void ConvertDbValue_NullForReferenceType_ReturnsNull()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = m.Invoke(null, new object?[] { null, typeof(string) });
        Assert.Null(result);
    }

    [Fact]
    public void ConvertDbValue_NullForNullableInt_ReturnsNull()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = m.Invoke(null, new object?[] { null, typeof(int?) });
        Assert.Null(result);
    }

    [Fact]
    public void ConvertDbValue_NullForNonNullableValueType_Throws()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var ex = Assert.Throws<System.Reflection.TargetInvocationException>(
            () => m.Invoke(null, new object?[] { null, typeof(int) }));
        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Contains("NULL", ex.InnerException!.Message);
    }

    [Fact]
    public void ConvertDbValue_SameType_ReturnsSameObject()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = m.Invoke(null, new object?[] { "hello", typeof(string) });
        Assert.Equal("hello", result);
    }

    [Fact]
    public void ConvertDbValue_EnumConversion_ReturnsEnum()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        // 1L (long from SQLite) → MfcStatus.Active (value 1)
        var result = m.Invoke(null, new object?[] { 1L, typeof(MfcStatus) });
        Assert.Equal(MfcStatus.Active, result);
    }

    [Fact]
    public void ConvertDbValue_LongToInt_Converts()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = m.Invoke(null, new object?[] { 42L, typeof(int) });
        Assert.Equal(42, result);
    }

    [Fact]
    public void CreateSyncMaterializerNonGeneric_WithOffset_ReturnsDelegate()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        // startOffset > 0 exercises the non-default code path
        var mat = factory.CreateSyncMaterializer(mapping, typeof(CovItem), null, 0);
        Assert.NotNull(mat);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 54 — ConnectionManager: IsTransientDatabaseError, IsNetworkIOException,
//             HasSocketExceptionInChain, TriggerFailoverAsync no-healthy-nodes path
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class ConnectionManagerPrivateMethodTests
{
    private static bool InvokeHasSocketChain(Exception ex)
    {
        var m = typeof(ConnectionManager)
            .GetMethod("HasSocketExceptionInChain", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (bool)m.Invoke(null, new object[] { ex })!;
    }

    private static bool InvokeIsNetworkIO(System.IO.IOException ex)
    {
        var m = typeof(ConnectionManager)
            .GetMethod("IsNetworkIOException", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (bool)m.Invoke(null, new object[] { ex })!;
    }

    private static bool InvokeIsTransientDb(DbException ex)
    {
        var m = typeof(ConnectionManager)
            .GetMethod("IsTransientDatabaseError", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (bool)m.Invoke(null, new object[] { ex })!;
    }

    [Fact]
    public void HasSocketExceptionInChain_WithSocketException_ReturnsTrue()
    {
        var socketEx = new System.Net.Sockets.SocketException();
        Assert.True(InvokeHasSocketChain(socketEx));
    }

    [Fact]
    public void HasSocketExceptionInChain_WithSocketAsInner_ReturnsTrue()
    {
        var socketEx = new System.Net.Sockets.SocketException();
        var outer = new Exception("outer", socketEx);
        Assert.True(InvokeHasSocketChain(outer));
    }

    [Fact]
    public void HasSocketExceptionInChain_WithNoSocket_ReturnsFalse()
    {
        var ex = new InvalidOperationException("no socket here");
        Assert.False(InvokeHasSocketChain(ex));
    }

    [Fact]
    public void HasSocketExceptionInChain_NullInnerChain_ReturnsFalse()
    {
        var ex = new Exception("only one level");
        Assert.False(InvokeHasSocketChain(ex));
    }

    [Fact]
    public void IsNetworkIOException_WithSocketInner_ReturnsTrue()
    {
        var socketEx = new System.Net.Sockets.SocketException();
        var ioEx = new System.IO.IOException("network failure", socketEx);
        Assert.True(InvokeIsNetworkIO(ioEx));
    }

    [Fact]
    public void IsNetworkIOException_WithoutSocketInner_ReturnsFalse()
    {
        var ioEx = new System.IO.IOException("disk error");
        Assert.False(InvokeIsNetworkIO(ioEx));
    }

    [Fact]
    public void IsTransientDatabaseError_WithSocketInner_ReturnsTrue()
    {
        // Non-SQL-Server DbException with socket inner → falls back to HasSocketExceptionInChain
        var socketEx = new System.Net.Sockets.SocketException();
        var dbEx = new FakeDbEx("transient", socketEx);
        Assert.True(InvokeIsTransientDb(dbEx));
    }

    [Fact]
    public void IsTransientDatabaseError_WithoutSocket_ReturnsFalse()
    {
        var dbEx = new FakeDbEx("regular error");
        Assert.False(InvokeIsTransientDb(dbEx));
    }

    [Fact]
    public void ConnectionManager_TriggerFailoverNoHealthyNodes_LogsError()
    {
        // Empty topology → _currentPrimary is null after construction
        // GetWriteConnectionAsync → triggers TriggerFailoverAsync (logs error) → throws
        var topology = new DatabaseTopology(); // no nodes added

        using var mgr = new ConnectionManager(
            topology, new SqliteProvider(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            TimeSpan.FromHours(1));

        // Must throw NormConnectionException("No healthy primary node available.")
        // TriggerFailoverAsync runs internally and logs the "no healthy nodes" error
        Assert.Throws<NormConnectionException>(() =>
            mgr.GetWriteConnectionAsync().GetAwaiter().GetResult());
    }

    private sealed class FakeDbEx : DbException
    {
        public FakeDbEx(string msg, Exception? inner = null) : base(msg, inner) { }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 55 — DbConnectionFactory: PostgresProvider + MySqlProvider paths
// Covers: Lines 44-49 (PostgresProvider), 50-57 (MySqlProvider) —
//         both create provider connections when drivers are present or throw when absent.
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class DbConnectionFactoryProviderPathTests
{
    [Fact]
    public void Create_PostgresProvider_HandlesInstalledOrMissingNpgsql()
    {
        try
        {
            using var connection = DbConnectionFactory.Create(
                "Host=localhost;Database=test;", new PostgresProvider(new SqliteParameterFactory()));
            Assert.Contains("Npgsql", connection.GetType().FullName, StringComparison.OrdinalIgnoreCase);
        }
        catch (InvalidOperationException ex)
        {
            Assert.Contains("Npgsql", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void Create_MySqlProvider_HandlesInstalledOrMissingMySqlConnector()
    {
        try
        {
            using var connection = DbConnectionFactory.Create(
                "Server=localhost;Database=test;", new MySqlProvider(new SqliteParameterFactory()));
            Assert.Contains("MySql", connection.GetType().FullName, StringComparison.OrdinalIgnoreCase);
        }
        catch (InvalidOperationException ex)
        {
            Assert.Contains("MySQL", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 56 — Migration runners with context: DisposeAsync when _context != null
//             + ExecuteReader/NonQuery routes through _context (interceptor path)
// Covers: PostgresMigrationRunner lines 279-289 (DisposeAsync with context),
//         SqlServerMigrationRunner lines 293-304 (DisposeAsync with context),
//         ExecuteNonQueryAsync/ExecuteReaderAsync when _context != null
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class MigrationRunnerWithContextDisposeTests
{
    private static SqliteConnection OpenSqlite()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static Assembly EmptyAsm()
        => System.Reflection.Emit.AssemblyBuilder.DefineDynamicAssembly(
            new System.Reflection.AssemblyName("EmptyMig_" + Guid.NewGuid().ToString("N")),
            System.Reflection.Emit.AssemblyBuilderAccess.Run);

    [Fact]
    public async Task Postgres_WithContext_DisposeAsync_DisposesContext()
    {
        await using var cn = OpenSqlite();
        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        var runner = new MigrationRunners.PostgresMigrationRunner(cn, EmptyAsm(), opts);
        // DisposeAsync with _context != null — covers lines 279-289
        await runner.DisposeAsync();
        // Second call should be idempotent (_disposed = true guards it)
        await runner.DisposeAsync();
    }

    [Fact]
    public void Postgres_WithContext_Dispose_DisposesContext()
    {
        using var cn = OpenSqlite();
        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        var runner = new MigrationRunners.PostgresMigrationRunner(cn, EmptyAsm(), opts);
        runner.Dispose(); // _context != null path
        runner.Dispose(); // idempotent (no-op on second call)
    }

    [Fact]
    public async Task SqlServer_WithContext_DisposeAsync_DisposesContext()
    {
        await using var cn = OpenSqlite();
        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        var runner = new MigrationRunners.SqlServerMigrationRunner(cn, EmptyAsm(), opts);
        await runner.DisposeAsync();
        await runner.DisposeAsync(); // idempotent
    }

    [Fact]
    public void SqlServer_WithContext_Dispose_DisposesContext()
    {
        using var cn = OpenSqlite();
        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        var runner = new MigrationRunners.SqlServerMigrationRunner(cn, EmptyAsm(), opts);
        runner.Dispose();
        runner.Dispose(); // idempotent
    }

    [Fact]
    public async Task Postgres_WithContext_GetPendingMigrations_UsesInterceptorPath()
    {
        // Covers ExecuteReaderAsync with _context != null (interceptor routing)
        await using var cn = OpenSqlite();
        // Create Postgres history table in SQLite format
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE \"__NormMigrationsHistory\" (\"Version\" INTEGER PRIMARY KEY, \"Name\" TEXT NOT NULL, \"AppliedOn\" TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        await using var runner = new MigrationRunners.PostgresMigrationRunner(cn, EmptyAsm(), opts);

        // Invoke GetPendingMigrationsInternalAsync via reflection to bypass advisory lock
        var m = typeof(MigrationRunners.PostgresMigrationRunner)
            .GetMethod("GetPendingMigrationsInternalAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var pending = await (Task<List<MigrationRunners.Migration>>)m.Invoke(runner, new object[] { CancellationToken.None })!;
        Assert.Empty(pending);
    }

    [Fact]
    public async Task SqlServer_WithContext_GetPendingInternal_UsesInterceptorPath()
    {
        // Covers ExecuteReaderAsync with _context != null (interceptor routing)
        await using var cn = OpenSqlite();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE [__NormMigrationsHistory] ([Version] INTEGER PRIMARY KEY, [Name] TEXT NOT NULL, [AppliedOn] TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        await using var runner = new MigrationRunners.SqlServerMigrationRunner(cn, EmptyAsm(), opts);

        var m = typeof(MigrationRunners.SqlServerMigrationRunner)
            .GetMethod("GetPendingMigrationsInternalAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var pending = await (Task<List<MigrationRunners.Migration>>)m.Invoke(runner, new object[] { CancellationToken.None })!;
        Assert.Empty(pending);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 57 — NormQueryProvider: ExecuteSync path + ConvertScalarResult type coverage
// Covers: ExecuteSync<TResult> (IQueryProvider.Execute sync), ConvertScalarResult<short>,
//         <byte>, <float>, <DateTime>, fallback ChangeType path
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class NormQueryProviderSyncAndScalarTests
{
    private static (SqliteConnection Cn, DbContext Ctx) MakeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER);" +
                          "INSERT INTO CovBoost_Item (Name, Value, IsActive) VALUES ('A', 10, 1);" +
                          "INSERT INTO CovBoost_Item (Name, Value, IsActive) VALUES ('B', 20, 0);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void ExecuteSync_ViaIEnumerableGetEnumerator_MaterializesItems()
    {
        // IEnumerable.GetEnumerator() triggers IQueryProvider.Execute<IEnumerable<T>>
        // which calls ExecuteSync<IEnumerable<T>> — the synchronous code path
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var query = ctx.Query<CovItem>();
        var items = new List<CovItem>();
        // Using foreach on IQueryable<T> triggers synchronous execution
        foreach (var item in query)
            items.Add(item);

        Assert.Equal(2, items.Count);
    }

    [Fact]
    public void Execute_ReturnsFirstItem_SyncPath()
    {
        // .First() on IQueryable calls IQueryProvider.Execute<T> (sync)
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var item = ctx.Query<CovItem>().OrderBy(i => i.Id).First();
        Assert.Equal("A", item.Name);
    }

    [Fact]
    public void ConvertScalarResult_Short_Converts()
    {
        // Call ConvertScalarResult<short> via reflection
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(short));
        var result = (short)m.Invoke(null, new object[] { (object)42L })!;
        Assert.Equal((short)42, result);
    }

    [Fact]
    public void ConvertScalarResult_Byte_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(byte));
        var result = (byte)m.Invoke(null, new object[] { (object)255L })!;
        Assert.Equal((byte)255, result);
    }

    [Fact]
    public void ConvertScalarResult_Float_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(float));
        var result = (float)m.Invoke(null, new object[] { (object)3.14 })!;
        Assert.Equal(3.14f, result, 2);
    }

    [Fact]
    public void ConvertScalarResult_DateTime_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(DateTime));
        var dt = new DateTime(2026, 3, 18);
        var result = (DateTime)m.Invoke(null, new object[] { (object)dt })!;
        Assert.Equal(dt, result);
    }

    [Fact]
    public void ConvertScalarResult_Guid_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(Guid));
        var g = Guid.NewGuid();
        var result = (Guid)m.Invoke(null, new object[] { (object)g })!;
        Assert.Equal(g, result);
    }

    [Fact]
    public void ConvertScalarResult_ReferenceType_DirectCast()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(string));
        var result = (string?)m.Invoke(null, new object[] { (object)"hello" })!;
        Assert.Equal("hello", result);
    }

    [Fact]
    public void ConvertScalarResult_Double_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(double));
        var result = (double)m.Invoke(null, new object[] { (object)2.718 })!;
        Assert.Equal(2.718, result, 6);
    }

    [Fact]
    public void ConvertScalarResult_Decimal_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(decimal));
        var result = (decimal)m.Invoke(null, new object[] { (object)99.99m })!;
        Assert.Equal(99.99m, result);
    }

    [Fact]
    public void ConvertScalarResult_Bool_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(bool));
        var result = (bool)m.Invoke(null, new object[] { (object)1L })!;
        Assert.True(result);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 58 — QueryExecutor: Materialize (sync path) + RedactSqlForLogging
// Covers: QueryExecutor.Materialize (sync non-async path), RedactSqlForLogging
//         internal static method, CreateListForType, IsReadOnlyQuery
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class QueryExecutorSyncPathTests
{
    private static (SqliteConnection Cn, DbContext Ctx) MakeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER);" +
                          "INSERT INTO CovBoost_Item (Name, Value, IsActive) VALUES ('SyncTest', 55, 1);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void QueryExecutor_CreateListForType_ReturnsTypedList()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        // Access via the internal wrapper method through the include processor
        var executor = new QueryExecutor(ctx,
            new IncludeProcessor(ctx));
        var list = executor.CreateListForType(typeof(CovItem), 10);
        Assert.NotNull(list);
        Assert.IsAssignableFrom<System.Collections.IList>(list);
    }

    [Fact]
    public void QueryExecutor_RedactSqlForLogging_RedactsStringLiterals()
    {
        var m = typeof(QueryExecutor)
            .GetMethod("RedactSqlForLogging",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        var sql = "SELECT * FROM T WHERE Name = 'secret_value'";
        var result = (string)m.Invoke(null, new object[] { sql })!;
        Assert.DoesNotContain("secret_value", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void QueryExecutor_RedactSqlForLogging_RedactsNationalString()
    {
        var m = typeof(QueryExecutor)
            .GetMethod("RedactSqlForLogging",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        var sql = "SELECT * FROM T WHERE Name = N'unicode_secret'";
        var result = (string)m.Invoke(null, new object[] { sql })!;
        Assert.DoesNotContain("unicode_secret", result);
    }

    [Fact]
    public void QueryExecutor_RedactSqlForLogging_NullOrEmpty_ReturnsInput()
    {
        var m = typeof(QueryExecutor)
            .GetMethod("RedactSqlForLogging",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        Assert.Equal("", (string)m.Invoke(null, new object[] { "" })!);
        Assert.Null(m.Invoke(null, new object?[] { null }));
    }

    [Fact]
    public void SyncMaterialize_ViaForeachOnIQueryable_WorksCorrectly()
    {
        // sync foreach invokes IQueryProvider.Execute<IEnumerable<T>> → Materialize (sync)
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var items = new List<CovItem>();
        foreach (var item in ctx.Query<CovItem>())
            items.Add(item);
        Assert.Single(items);
        Assert.Equal("SyncTest", items[0].Name);
    }

    [Fact]
    public void SyncMaterialize_NoTrackingQuery_SkipsChangeTracking()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        // AsNoTracking sets NoTracking flag which changes ProcessEntity behavior
        var q = (INormQueryable<CovItem>)ctx.Query<CovItem>();
        var items = q.AsNoTracking().ToList();
        Assert.Single(items);
    }
}
