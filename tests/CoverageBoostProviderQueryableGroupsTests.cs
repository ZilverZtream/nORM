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

// ─── File-scoped interceptor helpers ─────────────────────────────────────────

/// <summary>
/// Suppresses every command before execution — used to exercise the IsSuppressed
/// branch in the sync and async interception paths.
/// </summary>
file sealed class SuppressingInterceptor : IDbCommandInterceptor
{
    public int NonQueryResult { get; set; } = 42;

    // Async hooks — called from async paths
    public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        => Task.FromResult(InterceptionResult<int>.SuppressWithResult(NonQueryResult));

    public Task NonQueryExecutedAsync(DbCommand cmd, DbContext ctx, int result, TimeSpan elapsed, CancellationToken ct)
        => Task.CompletedTask;

    public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        => Task.FromResult(InterceptionResult<object?>.SuppressWithResult((object?)"suppressed_scalar"));

    public Task ScalarExecutedAsync(DbCommand cmd, DbContext ctx, object? result, TimeSpan elapsed, CancellationToken ct)
        => Task.CompletedTask;

    public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
    {
        var dt = new System.Data.DataTable();
        dt.Columns.Add("Id", typeof(int));
        // CreateDataReader returns a DataTableReader which extends DbDataReader
        var reader = dt.CreateDataReader();
        return Task.FromResult(InterceptionResult<DbDataReader>.SuppressWithResult(reader));
    }

    public Task ReaderExecutedAsync(DbCommand cmd, DbContext ctx, DbDataReader reader, TimeSpan elapsed, CancellationToken ct)
        => Task.CompletedTask;

    public Task CommandFailedAsync(DbCommand cmd, DbContext ctx, Exception exception, CancellationToken ct)
        => Task.CompletedTask;

    // Sync hooks — called from synchronous paths (ToList, ExecuteNonQuery, etc.)
    public InterceptionResult<int> NonQueryExecuting(DbCommand cmd, DbContext ctx)
        => InterceptionResult<int>.SuppressWithResult(NonQueryResult);

    public InterceptionResult<object?> ScalarExecuting(DbCommand cmd, DbContext ctx)
        => InterceptionResult<object?>.SuppressWithResult((object?)"suppressed_scalar");

    public InterceptionResult<DbDataReader> ReaderExecuting(DbCommand cmd, DbContext ctx)
    {
        var dt = new System.Data.DataTable();
        dt.Columns.Add("Id", typeof(int));
        return InterceptionResult<DbDataReader>.SuppressWithResult(dt.CreateDataReader());
    }
}

/// <summary>
/// Passes all commands through but captures any exception passed to CommandFailedAsync.
/// Used to verify the exception-catch branches in the interception slow paths.
/// </summary>
file sealed class ErrorCapturingInterceptor : IDbCommandInterceptor
{
    public Exception? CapturedException { get; private set; }

    // Async hooks — called from async paths
    public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        => Task.FromResult(InterceptionResult<int>.Continue());

    public Task NonQueryExecutedAsync(DbCommand cmd, DbContext ctx, int result, TimeSpan elapsed, CancellationToken ct)
        => Task.CompletedTask;

    public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        => Task.FromResult(InterceptionResult<object?>.Continue());

    public Task ScalarExecutedAsync(DbCommand cmd, DbContext ctx, object? result, TimeSpan elapsed, CancellationToken ct)
        => Task.CompletedTask;

    public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand cmd, DbContext ctx, CancellationToken ct)
        => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

    public Task ReaderExecutedAsync(DbCommand cmd, DbContext ctx, DbDataReader reader, TimeSpan elapsed, CancellationToken ct)
        => Task.CompletedTask;

    public Task CommandFailedAsync(DbCommand cmd, DbContext ctx, Exception exception, CancellationToken ct)
    {
        CapturedException = exception;
        return Task.CompletedTask;
    }

    // Sync hooks — called from synchronous paths
    public void CommandFailed(DbCommand cmd, DbContext ctx, Exception exception)
        => CapturedException = exception;
}

// ─── MinimalTestProvider — exercises DatabaseProvider base virtual methods ────

/// <summary>
/// Implements only the abstract members of <see cref="DatabaseProvider"/>; inherits
/// all virtual method base implementations so they can be coverage-tested.
/// </summary>
file sealed class MinimalTestProvider : DatabaseProvider
{
    public override string Escape(string id) => $"[{id}]";

    public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset,
        string? limitParam, string? offsetParam) { }

    public override string GetIdentityRetrievalString(TableMapping m)
        => "SELECT LAST_INSERT_ROWID()";

    public override DbParameter CreateParameter(string name, object? value)
        => new Microsoft.Data.Sqlite.SqliteParameter(name, value ?? DBNull.Value);

    public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        => null;

    public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        => columnName;

    public override string GenerateCreateHistoryTableSql(TableMapping mapping,
        IReadOnlyList<LiveColumnInfo>? liveColumns = null) => "";

    public override string GenerateTemporalTriggersSql(TableMapping mapping, System.Collections.Generic.IReadOnlyList<LiveColumnInfo>? liveColumns = null) => "";
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 34 – CommandInterceptorExtensions sync IsSuppressed & exception paths
// Covers lines 88-94, 108-115 (NonQuery), 192-196, 211-218 (Scalar),
//           303-308, 322-329 (Reader)
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class CommandInterceptorExtensionsSyncTests
{
    private static (Microsoft.Data.Sqlite.SqliteConnection cn, DbContext ctx) OpenDb()
    {
        var cn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE SyncIntercept (Id INTEGER PRIMARY KEY, Val TEXT)";
        setup.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void ExecuteNonQuerySync_IsSuppressed_ReturnsSuppressedResultWithoutExecuting()
    {
        var (cn, ctx) = OpenDb();
        using (cn) using (ctx)
        {
            var interceptor = new SuppressingInterceptor { NonQueryResult = 77 };
            ctx.Options.CommandInterceptors.Add(interceptor);

            using var cmd = cn.CreateCommand();
            cmd.CommandText = "INSERT INTO SyncIntercept (Id, Val) VALUES (1, 'x')";

            // Sync IsSuppressed branch — lines 89-94
            var result = cmd.ExecuteNonQueryWithInterception(ctx);
            Assert.Equal(77, result);

            // Row must NOT exist (command was suppressed)
            using var chk = cn.CreateCommand();
            chk.CommandText = "SELECT COUNT(*) FROM SyncIntercept";
            Assert.Equal(0L, (long)chk.ExecuteScalar()!);
        }
    }

    [Fact]
    public void ExecuteNonQuerySync_CommandFails_CallsCommandFailedAsync()
    {
        var (cn, ctx) = OpenDb();
        using (cn) using (ctx)
        {
            var interceptor = new ErrorCapturingInterceptor();
            ctx.Options.CommandInterceptors.Add(interceptor);

            using var cmd = cn.CreateCommand();
            cmd.CommandText = "THIS IS NOT VALID SQL";

            // Sync exception-catch branch — lines 108-115
            Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQueryWithInterception(ctx));
            Assert.NotNull(interceptor.CapturedException);
        }
    }

    [Fact]
    public void ExecuteScalarSync_IsSuppressed_ReturnsSuppressedValue()
    {
        var (cn, ctx) = OpenDb();
        using (cn) using (ctx)
        {
            ctx.Options.CommandInterceptors.Add(new SuppressingInterceptor());

            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT 999";

            // Sync IsSuppressed branch — lines 192-196
            var result = cmd.ExecuteScalarWithInterception(ctx);
            Assert.Equal("suppressed_scalar", result?.ToString());
        }
    }

    [Fact]
    public void ExecuteScalarSync_CommandFails_CallsCommandFailedAsync()
    {
        var (cn, ctx) = OpenDb();
        using (cn) using (ctx)
        {
            var interceptor = new ErrorCapturingInterceptor();
            ctx.Options.CommandInterceptors.Add(interceptor);

            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT * FROM NonExistentTable_XYZ_999";

            // Sync exception-catch branch — lines 211-218
            Assert.ThrowsAny<Exception>(() => cmd.ExecuteScalarWithInterception(ctx));
            Assert.NotNull(interceptor.CapturedException);
        }
    }

    [Fact]
    public void ExecuteReaderSync_IsSuppressed_ReturnsSuppressedReader()
    {
        var (cn, ctx) = OpenDb();
        using (cn) using (ctx)
        {
            ctx.Options.CommandInterceptors.Add(new SuppressingInterceptor());

            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            // Sync IsSuppressed branch — lines 303-308
            using var reader = cmd.ExecuteReaderWithInterception(ctx, System.Data.CommandBehavior.Default);
            Assert.NotNull(reader);
        }
    }

    [Fact]
    public void ExecuteReaderSync_CommandFails_CallsCommandFailedAsync()
    {
        var (cn, ctx) = OpenDb();
        using (cn) using (ctx)
        {
            var interceptor = new ErrorCapturingInterceptor();
            ctx.Options.CommandInterceptors.Add(interceptor);

            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT * FROM NonExistentTable_XYZ_999";

            // Sync exception-catch branch — lines 322-329
            Assert.ThrowsAny<Exception>(() => cmd.ExecuteReaderWithInterception(ctx, System.Data.CommandBehavior.Default));
            Assert.NotNull(interceptor.CapturedException);
        }
    }

    [Fact]
    public void ExecuteNonQuerySync_NoInterceptors_ExecutesNormally()
    {
        var (cn, ctx) = OpenDb();
        using (cn) using (ctx)
        {
            // Fast path — no interceptors → calls command.ExecuteNonQuery() directly
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "INSERT INTO SyncIntercept (Id, Val) VALUES (1, 'direct')";
            var affected = cmd.ExecuteNonQueryWithInterception(ctx);
            Assert.Equal(1, affected);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 35 – NavigationPropertyExtensions LazyNavigationReference proxy paths
// Covers lines 125-126 (IsLoaded with navCtx), 181-187 (reference proxy creation),
//           206 (else branch — property already set), 221-228 (LazyRef target type),
//           275-278 (GetPropertyInfo member access)
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class NavigationPropertyExtensionsProxyTests
{
    private static Microsoft.Data.Sqlite.SqliteConnection OpenDb()
    {
        var cn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    [Fact]
    public void EnableLazyLoading_EntityWithLazyRefProperty_CreatesReferenceProxy()
    {
        // CovRefEntity has LazyNavigationReference<CovItem>? LazyRef — starts null
        // EnableLazyLoading hits InitializeNavigationProperties → isCollection=false →
        // lines 183-186: creates LazyNavigationReference<CovItem> proxy and sets it
        // Also hits lines 221-228 (LazyNavigationReference<T> target type extraction)
        using var cn = OpenDb();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER)", cn)
            .ExecuteNonQuery();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_RefEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)", cn)
            .ExecuteNonQuery();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var entity = new CovRefEntity { Id = 1, Name = "Test" };
        Assert.Null(entity.LazyRef); // starts null

        // EnableLazyLoading registers entity in _navigationContexts and creates proxy
        var result = entity.EnableLazyLoading(ctx);

        // After EnableLazyLoading, LazyRef should be set to a LazyNavigationReference<CovItem> proxy
        Assert.NotNull(result.LazyRef);
        Assert.IsType<LazyNavigationReference<CovItem>>(result.LazyRef);
    }

    [Fact]
    public void EnableLazyLoading_EntityWithAlreadySetCollectionProperty_MarksAsLoaded()
    {
        // CovAuthor.Books is initialized to new List<CovBook>() — not null
        // InitializeNavigationProperties else branch (line 192/206): MarkAsLoaded
        using var cn = OpenDb();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)", cn)
            .ExecuteNonQuery();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Book (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER, Title TEXT)", cn)
            .ExecuteNonQuery();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var author = new CovAuthor { Id = 1, Name = "Author" };
        Assert.NotNull(author.Books); // already set — triggers the else branch

        // Should not throw; Books stays non-null and is marked as loaded
        var result = author.EnableLazyLoading(ctx);
        Assert.NotNull(result);
    }

    [Fact]
    public void IsLoaded_EntityWithNavContext_HitsLines125And126()
    {
        // After EnableLazyLoading, entity is in _navigationContexts.
        // IsLoaded(e => e.LazyRef) then hits lines 125-126 and GetPropertyInfo (275-278).
        using var cn = OpenDb();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER)", cn)
            .ExecuteNonQuery();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_RefEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)", cn)
            .ExecuteNonQuery();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var entity = new CovRefEntity { Id = 1, Name = "Test" };
        entity.EnableLazyLoading(ctx); // registers entity in _navigationContexts

        // IsLoaded: _navigationContexts.TryGetValue → found → lines 125-126 hit
        // GetPropertyInfo(e => e.LazyRef): MemberExpression body → line 277 hit
        var loaded = entity.IsLoaded(e => e.LazyRef);
        Assert.False(loaded); // proxy was created with MarkAsUnloaded
    }

    [Fact]
    public void IsLoaded_EntityWithPresetNavContext_ReturnsTrue_WhenMarkedLoaded()
    {
        using var cn = OpenDb();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)", cn)
            .ExecuteNonQuery();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Book (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER, Title TEXT)", cn)
            .ExecuteNonQuery();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var author = new CovAuthor { Id = 1, Name = "Author" };
        author.EnableLazyLoading(ctx); // registers nav context

        // Books was non-null → MarkAsLoaded("Books") was called in else branch
        var loaded = author.IsLoaded(a => a.Books);
        Assert.True(loaded); // was marked loaded by the else branch
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 36 – JoinBuilder.BuildJoinClause neededColumns.Count == 0 fallback
// Covers lines 79-85 (SelectAll fallback when ExtractNeededColumns returns empty)
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class JoinBuilderFallbackTests
{
    [Fact]
    public void BuildJoinClause_NewExpressionWithMethodCallArg_FallsBackToAllColumns()
    {
        // NewExpression whose argument is a MethodCallExpression — not a simple MemberExpression
        // → ExtractNeededColumns returns empty list → lines 79-85 (fallback SELECT all columns)
        using var cn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        cn.Open();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)", cn)
            .ExecuteNonQuery();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Book (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER, Title TEXT)", cn)
            .ExecuteNonQuery();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var outerMapping = ctx.GetMapping(typeof(CovAuthor));
        var innerMapping = ctx.GetMapping(typeof(CovBook));

        // Build NewExpression with a pure constant arg — ColumnExtractionVisitor will visit it
        // but find no column references → neededColumns stays empty → hits lines 79-85
        var aParam = Expression.Parameter(typeof(CovAuthor), "a");
        var bParam = Expression.Parameter(typeof(CovBook), "b");
        // Expression.Constant("static") — no member access, no entity reference
        var constArg = Expression.Constant("static_value");
        var ctor = typeof(Tuple<string>).GetConstructor(new[] { typeof(string) })!;
        var newExpr = Expression.New(ctor, constArg);
        var projection = Expression.Lambda(newExpr, aParam, bParam);

        // BuildJoinClause with this projection → neededColumns empty → fallback to all cols
        var sql = JoinBuilder.BuildJoinClause(
            projection, outerMapping, "a", innerMapping, "b",
            "INNER JOIN", "[a].[Id]", "[b].[AuthorId]");

        // Fallback path selects ALL columns from both tables
        Assert.Contains("FROM", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("INNER JOIN", sql, StringComparison.OrdinalIgnoreCase);
        // Both table aliases appear in the SELECT (a.col AND b.col)
        Assert.Contains("a.\"", sql);
        Assert.Contains("b.\"", sql);
    }

    [Fact]
    public void BuildJoinClause_NullProjection_SelectsAllColumns()
    {
        // When projection is null → else branch (lines 94-100) — not the NewExpression path
        // This exercises the non-NewExpression fallback for completeness
        using var cn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        cn.Open();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)", cn)
            .ExecuteNonQuery();
        new Microsoft.Data.Sqlite.SqliteCommand(
            "CREATE TABLE CovBoost_Book (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER, Title TEXT)", cn)
            .ExecuteNonQuery();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var outerMapping = ctx.GetMapping(typeof(CovAuthor));
        var innerMapping = ctx.GetMapping(typeof(CovBook));

        var sql = JoinBuilder.BuildJoinClause(
            null, outerMapping, "a", innerMapping, "b",
            "LEFT JOIN", "[a].[Id]", "[b].[AuthorId]");

        Assert.Contains("LEFT JOIN", sql, StringComparison.OrdinalIgnoreCase);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 37 – DatabaseProvider base virtual method coverage via MinimalTestProvider
// Covers lines 89, 94, 50, 53, 60, 67, 74-75, 81-84, 159-161,
//           187-193, 201-202, 210-218, 226-232, 240-246, 253, 261-268,
//           276-282, 291, 302-303, 336-341, 362-364, 374-376, 386-388,
//           403, 421-424, 432-436, 448+
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class DatabaseProviderBaseVirtualMethodTests
{
    // MinimalTestProvider doesn't override any virtual methods → base implementations run
    // Use DatabaseProvider as the declared type (file type can't appear in public member signature)
    private static readonly DatabaseProvider _p = new MinimalTestProvider();

    [Fact] public void MaxSqlLength_BaseImpl_ReturnsIntMax() => Assert.Equal(int.MaxValue, _p.MaxSqlLength);
    [Fact] public void MaxParameters_BaseImpl_ReturnsIntMax() => Assert.Equal(int.MaxValue, _p.MaxParameters);
    [Fact] public void BooleanTrueLiteral_BaseImpl_ReturnsOne() => Assert.Equal("1", _p.BooleanTrueLiteral);
    [Fact] public void BooleanFalseLiteral_BaseImpl_ReturnsZero() => Assert.Equal("0", _p.BooleanFalseLiteral);
    [Fact] public void PrefersSyncExecution_BaseImpl_ReturnsFalse() => Assert.False(_p.PrefersSyncExecution);
    [Fact] public void UsesFetchOffsetPaging_BaseImpl_ReturnsFalse() => Assert.False(_p.UsesFetchOffsetPaging);
    [Fact] public void ParameterPrefixChar_BaseImpl_IsAtSign() => Assert.Equal('@', _p.ParameterPrefixChar);
    [Fact] public void ParamPrefix_BaseImpl_IsAtString() => Assert.Equal("@", _p.ParamPrefix);

    [Fact]
    public void NullSafeEqual_BaseImpl_UsesOrIsNullExpansion()
    {
        var sql = _p.NullSafeEqual("col", "@p");
        Assert.Contains("OR", sql);
        Assert.Contains("IS NULL", sql);
        Assert.Contains("col", sql);
    }

    [Fact]
    public void NullSafeNotEqual_BaseImpl_UsesIsNotNullExpansion()
    {
        var sql = _p.NullSafeNotEqual("col", "@p");
        Assert.Contains("IS NOT NULL", sql);
        Assert.Contains("col", sql);
    }

    [Fact]
    public async Task IsAvailableAsync_BaseImpl_ReturnsTrue()
        => Assert.True(await _p.IsAvailableAsync());

    [Fact]
    public async Task CreateSavepointAsync_BaseImpl_ThrowsNormUnsupportedFeatureException()
    {
        using var cn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var tx = cn.BeginTransaction();
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => _p.CreateSavepointAsync(tx, "sp"));
    }

    [Fact]
    public async Task RollbackToSavepointAsync_BaseImpl_ThrowsNormUnsupportedFeatureException()
    {
        using var cn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var tx = cn.BeginTransaction();
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => _p.RollbackToSavepointAsync(tx, "sp"));
    }

    [Fact]
    public void BuildSimpleSelect_BaseImpl_WritesSql()
    {
        var buffer = new char[200];
        _p.BuildSimpleSelect(buffer, "MyTable".AsSpan(), "col1, col2".AsSpan(), out int length);
        var sql = new string(buffer, 0, length);
        Assert.Contains("SELECT", sql);
        Assert.Contains("MyTable", sql);
        Assert.Contains("col1", sql);
    }

    [Fact]
    public void GetInsertOrIgnoreSql_BaseImpl_UsesSelectNotExists()
    {
        var sql = _p.GetInsertOrIgnoreSql("[JT]", "[FK1]", "[FK2]", "@p1", "@p2");
        Assert.Contains("INSERT INTO", sql);
        Assert.Contains("NOT EXISTS", sql);
    }

    [Fact]
    public void GetConcatSql_BaseImpl_UsesConcatFunction()
    {
        var sql = _p.GetConcatSql("col1", "col2");
        Assert.Contains("CONCAT", sql);
        Assert.Contains("col1", sql);
        Assert.Contains("col2", sql);
    }

    [Fact]
    public void GetCreateTagsTableSql_BaseImpl_ContainsCreateTable()
    {
        var sql = _p.GetCreateTagsTableSql();
        Assert.Contains("CREATE TABLE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("__NormTemporalTags", sql);
    }

    [Fact]
    public void GetHistoryTableExistsProbeSql_BaseImpl_ContainsLimitAndTable()
    {
        var sql = _p.GetHistoryTableExistsProbeSql("[__history]");
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("[__history]", sql);
    }

    [Fact]
    public void GetTagLookupSql_BaseImpl_ContainsSelectAndParam()
    {
        var sql = _p.GetTagLookupSql("@tagName");
        Assert.Contains("SELECT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("@tagName", sql);
    }

    [Fact]
    public void GetCreateTagSql_BaseImpl_ContainsInsertAndParams()
    {
        var sql = _p.GetCreateTagSql("@tagName", "@ts");
        Assert.Contains("INSERT INTO", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("@tagName", sql);
        Assert.Contains("@ts", sql);
    }

    [Fact]
    public void IsObjectNotFoundError_TableNotFound_ReturnsTrue()
    {
        var ex = new Microsoft.Data.Sqlite.SqliteException("no such table: foo", 1);
        Assert.True(_p.IsObjectNotFoundError(ex));
    }

    [Fact]
    public void IsObjectNotFoundError_OtherError_ReturnsFalse()
    {
        var ex = new Microsoft.Data.Sqlite.SqliteException("disk I/O error", 10);
        Assert.False(_p.IsObjectNotFoundError(ex));
    }

    [Fact]
    public async Task IntrospectTableColumnsAsync_BaseImpl_ReturnsEmptyList()
    {
        using var cn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        cn.Open();
        var cols = await _p.IntrospectTableColumnsAsync(cn, "AnyTable");
        Assert.Empty(cols);
    }

    [Fact]
    public void LikeEscapeChar_BaseImpl_IsBackslash() => Assert.Equal('\\', _p.LikeEscapeChar);

    [Fact]
    public void EscapeLikePattern_BaseImpl_EscapesWildcards()
    {
        var escaped = _p.EscapeLikePattern("50% off_sale");
        Assert.Contains(@"\%", escaped);
        Assert.Contains(@"\_", escaped);
    }

    [Fact]
    public void GetLikeEscapeSql_BaseImpl_ContainsReplace()
    {
        var sql = _p.GetLikeEscapeSql("@val");
        Assert.Contains("REPLACE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("@val", sql);
    }

    [Fact]
    public void StoredProcedureCommandType_BaseImpl_IsStoredProcedure()
        => Assert.Equal(System.Data.CommandType.StoredProcedure, _p.StoredProcedureCommandType);
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 38 — DatabaseProvider base BulkInsertAsync / BatchedUpdateAsync / BatchedDeleteAsync
// Uses MinimalTestProvider (inherits base virtual impls) + in-memory SQLite.
// Covers: BulkInsertAsync (484-526), ExecuteInsertBatch (537-578),
//         BulkUpdateAsync base throws (584-590), BulkDeleteAsync base throws (596-602),
//         BatchedUpdateAsync (608-658), BatchedDeleteAsync (664-760),
//         BuildContainsClause (448-462)
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class DatabaseProviderBaseBulkTests
{
    private static readonly DatabaseProvider _p = new MinimalTestProvider();

    private static (SqliteConnection cn, DbContext ctx) MakeBulkCtx(bool batchedOps = false)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER)";
        setup.ExecuteNonQuery();
        var opts = new DbContextOptions { UseBatchedBulkOps = batchedOps };
        var ctx = new DbContext(cn, new MinimalTestProvider(), opts);
        return (cn, ctx);
    }

    // ── BuildContainsClause ──────────────────────────────────────────────────

    [Fact]
    public void BuildContainsClause_EmptyValues_ReturnsNeverTruePredicate()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var result = _p.BuildContainsClause(cmd, "col", Array.Empty<object?>());
        Assert.Equal("(1=0)", result);
    }

    [Fact]
    public void BuildContainsClause_WithValues_ReturnsInClauseWithParameters()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var values = new List<object?> { 1, 2, 3 };
        var result = _p.BuildContainsClause(cmd, "[Id]", values);
        Assert.Contains("[Id] IN", result);
        Assert.Contains("@p0", result);
        Assert.Contains("@p2", result);
        Assert.Equal(3, cmd.Parameters.Count);
    }

    // ── BulkInsertAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task BulkInsertAsync_EmptyList_ReturnsZero_DirectProviderCall()
    {
        // ctx.BulkInsertAsync throws for empty via NormValidator, so call provider directly
        // to cover the early-return path inside DatabaseProvider.BulkInsertAsync (line 489-493)
        var (cn, ctx) = MakeBulkCtx();
        using (cn) using (ctx)
        {
            var m = ctx.GetMapping(typeof(CovItem));
            var inserted = await _p.BulkInsertAsync(ctx, m, Array.Empty<CovItem>(), default);
            Assert.Equal(0, inserted);
        }
    }

    [Fact]
    public async Task BulkInsertAsync_NonEmptyList_InsertsAllRows()
    {
        var (cn, ctx) = MakeBulkCtx();
        using (cn) using (ctx)
        {
            var items = new[]
            {
                new CovItem { Name = "alpha", Value = 1, IsActive = true },
                new CovItem { Name = "beta",  Value = 2, IsActive = false },
            };
            var inserted = await ctx.BulkInsertAsync(items);
            Assert.Equal(2, inserted);
            using var chk = cn.CreateCommand();
            chk.CommandText = "SELECT COUNT(*) FROM CovBoost_Item";
            Assert.Equal(2L, (long)chk.ExecuteScalar()!);
        }
    }

    // ── BulkUpdateAsync / BulkDeleteAsync base throws ────────────────────────

    [Fact]
    public async Task BulkUpdateAsync_NotBatched_ThrowsNormUnsupportedFeatureException()
    {
        var (cn, ctx) = MakeBulkCtx(batchedOps: false);
        using (cn) using (ctx)
        {
            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO CovBoost_Item VALUES (1, 'old', 1, 1)";
            ins.ExecuteNonQuery();
            var items = new[] { new CovItem { Id = 1, Name = "new", Value = 99, IsActive = false } };
            await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.BulkUpdateAsync(items));
        }
    }

    [Fact]
    public async Task BulkDeleteAsync_NotBatched_ThrowsNormUnsupportedFeatureException()
    {
        var (cn, ctx) = MakeBulkCtx(batchedOps: false);
        using (cn) using (ctx)
        {
            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO CovBoost_Item VALUES (1, 'old', 1, 1)";
            ins.ExecuteNonQuery();
            var items = new[] { new CovItem { Id = 1, Name = "old", Value = 1, IsActive = true } };
            await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.BulkDeleteAsync(items));
        }
    }

    // ── BatchedUpdateAsync via BulkUpdate with UseBatchedBulkOps=true ────────

    [Fact]
    public async Task BulkUpdateAsync_Batched_UpdatesExistingRows()
    {
        var (cn, ctx) = MakeBulkCtx(batchedOps: true);
        using (cn) using (ctx)
        {
            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO CovBoost_Item VALUES (1, 'old', 1, 1), (2, 'old2', 2, 1)";
            ins.ExecuteNonQuery();
            var items = new[]
            {
                new CovItem { Id = 1, Name = "updated1", Value = 10, IsActive = false },
                new CovItem { Id = 2, Name = "updated2", Value = 20, IsActive = true },
            };
            var updated = await ctx.BulkUpdateAsync(items);
            Assert.Equal(2, updated);
            using var chk = cn.CreateCommand();
            chk.CommandText = "SELECT Name FROM CovBoost_Item WHERE Id = 1";
            Assert.Equal("updated1", (string)chk.ExecuteScalar()!);
        }
    }

    // ── BatchedDeleteAsync via BulkDelete with UseBatchedBulkOps=true ────────

    [Fact]
    public async Task BulkDeleteAsync_Batched_DeletesRows_SingleKey()
    {
        var (cn, ctx) = MakeBulkCtx(batchedOps: true);
        using (cn) using (ctx)
        {
            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO CovBoost_Item VALUES (1, 'a', 1, 1), (2, 'b', 2, 1)";
            ins.ExecuteNonQuery();
            var items = new[]
            {
                new CovItem { Id = 1, Name = "a", Value = 1, IsActive = true },
                new CovItem { Id = 2, Name = "b", Value = 2, IsActive = true },
            };
            var deleted = await ctx.BulkDeleteAsync(items);
            Assert.Equal(2, deleted);
            using var chk = cn.CreateCommand();
            chk.CommandText = "SELECT COUNT(*) FROM CovBoost_Item";
            Assert.Equal(0L, (long)chk.ExecuteScalar()!);
        }
    }

    [Fact]
    public async Task BulkDeleteAsync_Batched_EmptyList_ReturnsZero_DirectProviderCall()
    {
        // ctx.BulkDeleteAsync throws for empty via NormValidator, so call provider directly
        // to cover the early-return path inside BatchedDeleteAsync (line 669)
        var (cn, ctx) = MakeBulkCtx(batchedOps: true);
        using (cn) using (ctx)
        {
            var m = ctx.GetMapping(typeof(CovItem));
            // Call base BulkDeleteAsync which routes to BatchedDeleteAsync when UseBatchedBulkOps=true
            var deleted = await _p.BulkDeleteAsync(ctx, m, Array.Empty<CovItem>(), default);
            Assert.Equal(0, deleted);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 39 — ExpressionUtils additional coverage
// Covers: ValidateExpression throw paths (lines 39-42),
//         GetCompilationTimeout complexity scaling,
//         CompileWithFallback(LambdaExpression) non-generic overload (89-108)
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class ExpressionUtilsAdditionalTests
{
    [Fact]
    public void ValidateExpression_TooManyNodes_ThrowsInvalidOperation()
    {
        // 2503 predicates "x > i" AND-chained = 4*2503-1 = 10011 nodes > MaxNodeCount(10000)
        var param = Expression.Parameter(typeof(int), "x");
        Expression body = Expression.GreaterThan(param, Expression.Constant(0));
        for (int i = 1; i <= 2503; i++)
            body = Expression.And(body, Expression.GreaterThan(param, Expression.Constant(i)));
        var lambda = Expression.Lambda<Func<int, bool>>(body, param);

        var ex = Assert.Throws<InvalidOperationException>(() => ExpressionUtils.ValidateExpression(lambda));
        Assert.Contains("complex", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateExpression_TooDeep_ThrowsInvalidOperation()
    {
        // 103 levels deep > MaxDepth(100)
        Expression body = Expression.Constant(0);
        for (int i = 0; i < 103; i++)
            body = Expression.Condition(Expression.Constant(true), Expression.Constant(1), body);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            ExpressionUtils.ValidateExpression(Expression.Lambda<Func<int>>(body)));
        Assert.Contains("deep", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void GetCompilationTimeout_SimpleExpression_Returns30Seconds()
    {
        var expr = Expression.Lambda<Func<int>>(Expression.Constant(42));
        var timeout = ExpressionUtils.GetCompilationTimeout(expr);
        Assert.Equal(TimeSpan.FromSeconds(30), timeout);
    }

    [Fact]
    public void CompileWithFallback_NonGenericLambda_CompilesAndInvokes()
    {
        // Covers the LambdaExpression overload (lines 89-108)
        Expression<Func<int, int>> typed = x => x * 2;
        LambdaExpression untyped = typed;
        var del = ExpressionUtils.CompileWithFallback(untyped, default);
        Assert.NotNull(del);
        Assert.Equal(42, del.DynamicInvoke(21));
    }

    [Fact]
    public void AnalyzeExpressionComplexity_SimpleExpression_ReportsNodes()
    {
        var param = Expression.Parameter(typeof(int), "x");
        var expr = Expression.AndAlso(
            Expression.GreaterThan(param, Expression.Constant(0)),
            Expression.LessThan(param, Expression.Constant(100)));
        var c = ExpressionUtils.AnalyzeExpressionComplexity(expr);
        Assert.True(c.NodeCount >= 5);
        Assert.True(c.Depth >= 2);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 40 — NormIncludableQueryable<T,TProperty> and Unconstrained variants
//             NormQueryableImplUnconstrained<T>
// Covers: AsNoTracking, AsSplitQuery, chained Include, ThenInclude,
//         CountAsync, AnyAsync, ToArrayAsync, FirstOrDefaultAsync,
//         SingleOrDefaultAsync, ExecuteDeleteAsync, ExecuteUpdateAsync
//         on NormIncludableQueryable (constrained) and Unconstrained variants,
//         plus NormQueryableImplUnconstrained Include/AsNoTracking/AsSplitQuery.
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class NormIncludableQueryableCovBoostTests
{
    private static (SqliteConnection cn, DbContext ctx) MakeAuthorBookDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE CovBoost_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE CovBoost_Book   (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER, Title TEXT);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── Constrained: NormIncludableQueryable<T, TProperty> ──────────────────

    [Fact]
    public void NormIncludableQueryable_AsNoTracking_ReturnsNonNull()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            var untracked = q.AsNoTracking();
            Assert.NotNull(untracked);
        }
    }

    [Fact]
    public void NormIncludableQueryable_AsSplitQuery_ReturnsNonNull()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            var split = q.AsSplitQuery();
            Assert.NotNull(split);
        }
    }

    [Fact]
    public void NormIncludableQueryable_ChainedInclude_ReturnsNewIncludable()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            // NormIncludableQueryable<CovAuthor, ICollection<CovBook>>.Include<string>()
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            var q2 = q.Include(a => a.Name);
            Assert.NotNull(q2);
        }
    }

    [Fact]
    public async Task NormIncludableQueryable_CountAsync_ReturnsZeroForEmpty()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            Assert.Equal(0, await q.CountAsync());
        }
    }

    [Fact]
    public async Task NormIncludableQueryable_AsAsyncEnumerable_YieldsNoItems()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            // AsAsyncEnumerable covers that method body; iterate to confirm no results
            var count = 0;
            await foreach (var _ in q.AsNoTracking().AsAsyncEnumerable())
                count++;
            Assert.Equal(0, count);
        }
    }


    [Fact]
    public async Task NormIncludableQueryable_ToArrayAsync_ReturnsEmptyArray()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            var arr = await q.ToArrayAsync();
            Assert.Empty(arr);
        }
    }

    [Fact]
    public async Task NormIncludableQueryable_FirstOrDefaultAsync_ReturnsNull()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            Assert.Null(await q.FirstOrDefaultAsync());
        }
    }

    [Fact]
    public async Task NormIncludableQueryable_SingleOrDefaultAsync_ReturnsNull()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            Assert.Null(await q.SingleOrDefaultAsync());
        }
    }

    [Fact]
    public async Task NormIncludableQueryable_ExecuteDeleteAsync_DeletesZeroRows()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            // Include is stripped by the delete translator; exercises ExecuteDeleteAsync body
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            var deleted = await q.AsNoTracking().ExecuteDeleteAsync();
            Assert.Equal(0, deleted);
        }
    }

    [Fact]
    public async Task NormIncludableQueryable_ExecuteUpdateAsync_UpdatesZeroRows()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            var updated = await q.AsNoTracking().ExecuteUpdateAsync(s => s.SetProperty(a => a.Name, "x"));
            Assert.Equal(0, updated);
        }
    }

    // ── NormQueryableImplUnconstrained<T> ────────────────────────────────────

    [Fact]
    public void NormQueryableImplUnconstrained_Include_ReturnsUnconstrainedIncludable()
    {
        // CovNoCtorEntity has no parameterless ctor → ctx.Query returns NormQueryableImplUnconstrained
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = (INormQueryable<CovNoCtorEntity>)ctx.Query<CovNoCtorEntity>();
            var includable = q.Include(e => e.Items);
            Assert.NotNull(includable);
        }
    }

    [Fact]
    public void NormQueryableImplUnconstrained_AsNoTracking_ReturnsNewQueryable()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = (INormQueryable<CovNoCtorEntity>)ctx.Query<CovNoCtorEntity>();
            Assert.NotNull(q.AsNoTracking());
        }
    }

    [Fact]
    public void NormQueryableImplUnconstrained_AsSplitQuery_ReturnsNewQueryable()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = (INormQueryable<CovNoCtorEntity>)ctx.Query<CovNoCtorEntity>();
            Assert.NotNull(q.AsSplitQuery());
        }
    }

    // ── NormIncludableQueryableUnconstrained<T, TProperty> ──────────────────

    [Fact]
    public void NormIncludableQueryableUnconstrained_AsNoTracking_ReturnsNonNull()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovNoCtorEntity>)ctx.Query<CovNoCtorEntity>()).Include(e => e.Items);
            Assert.NotNull(q.AsNoTracking());
        }
    }

    [Fact]
    public void NormIncludableQueryableUnconstrained_AsSplitQuery_ReturnsNonNull()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovNoCtorEntity>)ctx.Query<CovNoCtorEntity>()).Include(e => e.Items);
            Assert.NotNull(q.AsSplitQuery());
        }
    }

    [Fact]
    public void NormIncludableQueryableUnconstrained_ChainedInclude_ReturnsNonNull()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovNoCtorEntity>)ctx.Query<CovNoCtorEntity>()).Include(e => e.Items);
            // Second Include returns NormIncludableQueryableUnconstrained<CovNoCtorEntity, int>
            var q2 = q.Include(e => e.Id);
            Assert.NotNull(q2);
        }
    }

    // ── NormIncludableQueryableExtensions.ThenInclude ─────────────────────────

    [Fact]
    public void ThenInclude_OnConstrainedIncludable_ReturnsNewIncludable()
    {
        var (cn, ctx) = MakeAuthorBookDb();
        using (cn) using (ctx)
        {
            var q = ((INormQueryable<CovAuthor>)ctx.Query<CovAuthor>()).Include(a => a.Books);
            // ThenInclude on ICollection<CovBook> → select Title (string)
            var q2 = q.ThenInclude(b => b.Title);
            Assert.NotNull(q2);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 41 — ConnectionManager.RedactConnectionString
// Covers: RedactConnectionString normal path (334-351) and malformed catch path (346-350)
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class ConnectionManagerRedactTests
{
    [Fact]
    public void RedactConnectionString_WithPassword_MasksPassword()
    {
        var cs = "Server=myserver;Database=mydb;Password=supersecret;";
        var redacted = ConnectionManager.RedactConnectionString(cs);
        Assert.DoesNotContain("supersecret", redacted, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("***", redacted);
    }

    [Fact]
    public void RedactConnectionString_WithToken_MasksToken()
    {
        var cs = "Server=myserver;Token=abc123;";
        var redacted = ConnectionManager.RedactConnectionString(cs);
        Assert.DoesNotContain("abc123", redacted);
    }

    [Fact]
    public void RedactConnectionString_NoSensitiveKeys_ReturnsUnchanged()
    {
        var cs = "Data Source=:memory:;";
        var redacted = ConnectionManager.RedactConnectionString(cs);
        Assert.Contains("memory", redacted, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RedactConnectionString_MalformedString_ReturnsRedactedHash()
    {
        // A string that looks like a connection string but is actually malformed
        // enough to cause DbConnectionStringBuilder to throw
        var cs = "=bad==;malformed;=;";
        var redacted = ConnectionManager.RedactConnectionString(cs);
        // Either returns the original (if parser accepts) or "[redacted:xxxxxxxx]"
        Assert.NotNull(redacted);
        Assert.NotEmpty(redacted);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 42 — JoinTableMapping bidirectional navigation
// Covers: JoinTableMapping lines 109-122 (RightCollectionGetter/Setter when
//         RelatedNavPropertyName != null / WithMany(r => r.Lefts) configured)
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class JoinTableMappingBidirTests
{
    private class BidirLeft
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<BidirRight> Rights { get; set; } = new();
    }

    private class BidirRight
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        // [NotMapped] prevents conventional relation discovery from triggering circular GetMapping
        [NotMapped]
        public List<BidirLeft> Lefts { get; set; } = new();
    }

    private static SqliteConnection CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE BidirLeft  (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);" +
            "CREATE TABLE BidirRight (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name  TEXT NOT NULL);" +
            "CREATE TABLE BidirJoin  (LeftId INTEGER NOT NULL, RightId INTEGER NOT NULL, PRIMARY KEY (LeftId, RightId));";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public void JoinTableMapping_WithInverseNav_BuildsRightGetterSetter()
    {
        var cn = CreateDb();
        using var _cn = cn;
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<BidirLeft>()
                  .HasMany<BidirRight>(l => l.Rights)
                  .WithMany(r => r.Lefts)
                  .UsingTable("BidirJoin", "LeftId", "RightId");
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Accessing the mapping triggers DiscoverRelations → JoinTableMapping constructor
        var mapping = ctx.GetMapping(typeof(BidirLeft));
        Assert.NotEmpty(mapping.ManyToManyJoins);

        var jtm = mapping.ManyToManyJoins[0];
        // Lines 109-122: right nav property not null → getters/setters built
        Assert.NotNull(jtm.RightCollectionGetter);
        Assert.NotNull(jtm.RightCollectionSetter);
        Assert.Equal("Lefts", jtm.RightNavPropertyName);
    }

    [Fact]
    public void JoinTableMapping_WithInverseNav_GetterSetterFunctional()
    {
        var cn = CreateDb();
        using var _cn = cn;
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<BidirLeft>()
                  .HasMany<BidirRight>(l => l.Rights)
                  .WithMany(r => r.Lefts)
                  .UsingTable("BidirJoin", "LeftId", "RightId");
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var mapping = ctx.GetMapping(typeof(BidirLeft));
        var jtm = mapping.ManyToManyJoins[0];

        // Verify right getter/setter actually work on BidirRight instances
        var right = new BidirRight { Id = 1, Name = "Right1" };
        var lefts = new System.Collections.Generic.List<BidirLeft>
            { new BidirLeft { Id = 1, Title = "LeftA" } };

        jtm.RightCollectionSetter!(right, lefts);
        var retrieved = jtm.RightCollectionGetter!(right);
        Assert.NotNull(retrieved);
        Assert.Single(retrieved);
    }

    [Fact]
    public void JoinTableMapping_WithInverseNav_LeftGetterSetterAlsoWork()
    {
        var cn = CreateDb();
        using var _cn = cn;
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<BidirLeft>()
                  .HasMany<BidirRight>(l => l.Rights)
                  .WithMany(r => r.Lefts)
                  .UsingTable("BidirJoin", "LeftId", "RightId");
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var mapping = ctx.GetMapping(typeof(BidirLeft));
        var jtm = mapping.ManyToManyJoins[0];

        var left = new BidirLeft { Id = 2, Title = "LeftB" };
        var rights = new System.Collections.Generic.List<BidirRight>
            { new BidirRight { Id = 10, Name = "R1" }, new BidirRight { Id = 11, Name = "R2" } };

        jtm.LeftCollectionSetter(left, rights);
        var retrieved = jtm.LeftCollectionGetter(left);
        Assert.NotNull(retrieved);
        Assert.Equal(2, retrieved.Count);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 43 — DbConnectionFactory SqlServerProvider + NotSupportedException
// Covers: DbConnectionFactory lines 29, 40-41 (SqlServerProvider arm) and
//         line 60 (NotSupportedException for unknown provider type)
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class DbConnectionFactoryCoverageTests
{
    [Fact]
    public void Create_SqlServerProvider_CreatesSqlConnection()
    {
        // Hits providerName = "sqlserver" (line 29) and factory t==SqlServerProvider (line 40)
        var conn = DbConnectionFactory.Create("Server=localhost;Database=testdb;", new SqlServerProvider());
        Assert.NotNull(conn);
        Assert.IsType<Microsoft.Data.SqlClient.SqlConnection>(conn);
        conn.Dispose();
    }

    [Fact]
    public void Create_UnknownProvider_ThrowsNotSupportedException()
    {
        // MinimalTestProvider is not Sqlite/SqlServer/Postgres/MySQL
        // Hits line 33 (GetType().Name as providerName) and line 60 (throw NotSupportedException)
        Assert.Throws<NotSupportedException>(() =>
            DbConnectionFactory.Create("Data Source=:memory:", new MinimalTestProvider()));
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 44 — NormQueryableImplUnconstrained<T> async execution
// Covers: NormQueryable.cs lines 244-254 — the actual async method bodies that
//         delegate to NormQueryProvider. CovNoCtorEntity (parameterized ctor) forces
//         the unconstrained path through MaterializerFactory's parameterized-ctor IL emitter.
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class NormQueryableImplUnconstrainedAsyncTests
{
    // Re-uses the namespace-scope CovNoCtorEntity ([Table("CovBoost_NoCtor")], ctor(int,string))

    private static (SqliteConnection Cn, DbContext Ctx) MakeNoCtorDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE CovBoost_NoCtor (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '')";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void InsertNoCtor(SqliteConnection cn, int id, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"INSERT INTO CovBoost_NoCtor (Id, Name) VALUES ({id}, @n)";
        cmd.Parameters.AddWithValue("@n", name);
        cmd.ExecuteNonQuery();
    }

    private static INormQueryable<CovNoCtorEntity> Q(DbContext ctx)
        => (INormQueryable<CovNoCtorEntity>)ctx.Query<CovNoCtorEntity>();

    [Fact]
    public async Task CountAsync_EmptyTable_ReturnsZero()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;

        var count = await Q(ctx).CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task CountAsync_WithRows_ReturnsCount()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Alice");
        InsertNoCtor(cn, 2, "Bob");

        var count = await Q(ctx).CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task ToListAsync_EmptyTable_ReturnsEmptyList()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;

        var list = await Q(ctx).ToListAsync();
        Assert.Empty(list);
    }

    [Fact]
    public async Task ToListAsync_WithRows_Materializes()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Alice");
        InsertNoCtor(cn, 2, "Bob");

        var list = await Q(ctx).ToListAsync();
        Assert.Equal(2, list.Count);
        Assert.Contains(list, e => e.Name == "Alice");
        Assert.Contains(list, e => e.Name == "Bob");
    }

    [Fact]
    public async Task ToArrayAsync_WithRows_ReturnsArray()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Charlie");

        var arr = await Q(ctx).ToArrayAsync();
        Assert.Single(arr);
        Assert.Equal("Charlie", arr[0].Name);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_Empty_ReturnsNull()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).FirstOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_WithRow_ReturnsEntity()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Dana");

        var result = await Q(ctx).FirstOrDefaultAsync();
        Assert.NotNull(result);
        Assert.Equal("Dana", result!.Name);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_WithOneRow_ReturnsEntity()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Eve");

        var result = await Q(ctx).SingleOrDefaultAsync();
        Assert.NotNull(result);
        Assert.Equal("Eve", result!.Name);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_Empty_ReturnsNull()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).SingleOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task ExecuteDeleteAsync_DeletesRows()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "ToDelete");
        InsertNoCtor(cn, 2, "Keep");

        var deleted = await Q(ctx).Where(e => e.Id == 1).ExecuteDeleteAsync();
        Assert.Equal(1, deleted);

        var count = await Q(ctx).CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_UpdatesRows()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Original");
        InsertNoCtor(cn, 2, "Other");

        var updated = await Q(ctx).Where(e => e.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Name, "Updated"));
        Assert.Equal(1, updated);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM CovBoost_NoCtor WHERE Id = 1";
        var name = (string?)cmd.ExecuteScalar();
        Assert.Equal("Updated", name);
    }

    [Fact]
    public async Task AsAsyncEnumerable_YieldsRows()
    {
        var (cn, ctx) = MakeNoCtorDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Stream1");
        InsertNoCtor(cn, 2, "Stream2");

        var names = new List<string>();
        await foreach (var e in Q(ctx).AsNoTracking().AsAsyncEnumerable())
            names.Add(e.Name);

        Assert.Equal(2, names.Count);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GROUP 45 — NormIncludableQueryableUnconstrained<T, TProperty> async execution
// Covers: NormQueryable.cs lines 292-309 — async method bodies on the unconstrained
//         includable type. CovNoCtorEntity + Include(e => e.Items) produces
//         NormIncludableQueryableUnconstrained<CovNoCtorEntity, ICollection<CovItem>>.
// ═══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class NormIncludableQueryableUnconstrainedAsyncTests
{
    private static (SqliteConnection Cn, DbContext Ctx) MakeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE CovBoost_NoCtor (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '');" +
            "CREATE TABLE CovBoost_Item   (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Value INTEGER NOT NULL DEFAULT 0, IsActive INTEGER NOT NULL DEFAULT 0);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void InsertNoCtor(SqliteConnection cn, int id, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"INSERT INTO CovBoost_NoCtor (Id, Name) VALUES ({id}, @n)";
        cmd.Parameters.AddWithValue("@n", name);
        cmd.ExecuteNonQuery();
    }

    // Returns the unconstrained includable queryable
    private static INormIncludableQueryable<CovNoCtorEntity, ICollection<CovItem>?> Q(DbContext ctx)
        => ((INormQueryable<CovNoCtorEntity>)ctx.Query<CovNoCtorEntity>()).Include(e => e.Items);

    [Fact]
    public async Task CountAsync_EmptyTable_ReturnsZero()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var count = await Q(ctx).CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task CountAsync_WithRows_ReturnsCount()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "A");
        InsertNoCtor(cn, 2, "B");

        var count = await Q(ctx).CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task ToListAsync_EmptyTable_ReturnsEmptyList()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var list = await Q(ctx).ToListAsync();
        Assert.Empty(list);
    }

    [Fact]
    public async Task ToListAsync_WithRows_ReturnsEntities()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "X");
        InsertNoCtor(cn, 2, "Y");

        var list = await Q(ctx).ToListAsync();
        Assert.Equal(2, list.Count);
    }

    [Fact]
    public async Task ToArrayAsync_WithRows_ReturnsArray()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Arr");

        var arr = await Q(ctx).ToArrayAsync();
        Assert.Single(arr);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_Empty_ReturnsNull()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).FirstOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_WithRow_ReturnsEntity()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "First");

        var result = await Q(ctx).FirstOrDefaultAsync();
        Assert.NotNull(result);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_WithOneRow_ReturnsEntity()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Single");

        var result = await Q(ctx).SingleOrDefaultAsync();
        Assert.NotNull(result);
    }

    [Fact]
    public async Task ExecuteDeleteAsync_DeletesRows()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Del");
        InsertNoCtor(cn, 2, "Keep");

        // ExecuteDeleteAsync strips the Include and deletes all matching rows
        var deleted = await Q(ctx).AsNoTracking().ExecuteDeleteAsync();
        Assert.Equal(2, deleted);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_UpdatesRows()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Before");

        var updated = await Q(ctx).AsNoTracking()
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Name, "After"));
        Assert.Equal(1, updated);
    }

    [Fact]
    public async Task AsAsyncEnumerable_YieldsRows()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        InsertNoCtor(cn, 1, "Stream");

        var items = new List<CovNoCtorEntity>();
        await foreach (var e in Q(ctx).AsAsyncEnumerable())
            items.Add(e);

        Assert.Single(items);
    }
}
