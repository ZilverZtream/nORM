using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that ExecuteInsertBatch validates the number of generated keys returned
/// by the database equals the number of entities in the batch.
/// </summary>
public class InsertKeyCardinalityTests
{
    [Table("IkcItem")]
    private class IkcItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(DbContextOptions? options = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IkcItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider(), options));
    }

 // ─── Fake DbDataReader returning exactly N result sets ────────────────

 /// <summary>
 /// Fake reader that simulates a database returning only <paramref name="keysToReturn"/> result sets
 /// (each with a single row containing the row ID). Used to test the cardinality guard.
 /// </summary>
    private sealed class PartialKeyReader : DbDataReader
    {
        private readonly int _keysToReturn;
        private int _resultSetIdx = 0;
        private bool _rowRead = false;

        public PartialKeyReader(int keysToReturn) => _keysToReturn = keysToReturn;

        public override Task<bool> ReadAsync(CancellationToken ct)
        {
            if (_resultSetIdx < _keysToReturn && !_rowRead)
            {
                _rowRead = true;
                return Task.FromResult(true);
            }
            return Task.FromResult(false);
        }

        public override Task<bool> NextResultAsync(CancellationToken ct)
        {
            _resultSetIdx++;
            _rowRead = false;
            return Task.FromResult(_resultSetIdx < _keysToReturn);
        }

        public override object GetValue(int ordinal) => (long)(_resultSetIdx + 1);
        public override long GetInt64(int ordinal) => (long)(_resultSetIdx + 1);

 // Minimal abstract members
        public override bool Read() => throw new NotSupportedException();
        public override bool NextResult() => throw new NotSupportedException();
        public override void Close() { }
        public override bool IsClosed => false;
        public override int RecordsAffected => _keysToReturn;
        public override int FieldCount => 1;
        public override bool HasRows => _keysToReturn > 0;
        public override int Depth => 0;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(0);
        public override bool GetBoolean(int ordinal) => throw new NotImplementedException();
        public override byte GetByte(int ordinal) => throw new NotImplementedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
        public override char GetChar(int ordinal) => throw new NotImplementedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
        public override string GetDataTypeName(int ordinal) => "INTEGER";
        public override DateTime GetDateTime(int ordinal) => throw new NotImplementedException();
        public override decimal GetDecimal(int ordinal) => throw new NotImplementedException();
        public override double GetDouble(int ordinal) => throw new NotImplementedException();
        public override Type GetFieldType(int ordinal) => typeof(long);
        public override float GetFloat(int ordinal) => throw new NotImplementedException();
        public override Guid GetGuid(int ordinal) => throw new NotImplementedException();
        public override short GetInt16(int ordinal) => throw new NotImplementedException();
        public override int GetInt32(int ordinal) => throw new NotImplementedException();
        public override string GetName(int ordinal) => "id";
        public override int GetOrdinal(string name) => 0;
        public override string GetString(int ordinal) => throw new NotImplementedException();
        public override bool IsDBNull(int ordinal) => false;
        public override int GetValues(object[] values) { values[0] = GetValue(0); return 1; }
        public override IEnumerator GetEnumerator() => throw new NotImplementedException();
    }

 // ─── Command interceptor that returns a PartialKeyReader ──────────────

    private sealed class ShortKeyReturnInterceptor : IDbCommandInterceptor
    {
        private readonly int _keysToReturn;

        public ShortKeyReturnInterceptor(int keysToReturn) => _keysToReturn = keysToReturn;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
 // Only intercept INSERT commands
            if (command.CommandText.Contains("INSERT", StringComparison.OrdinalIgnoreCase))
                return Task.FromResult(InterceptionResult<DbDataReader>.SuppressWithResult(new PartialKeyReader(_keysToReturn)));
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(InterceptionResult<int>.Continue());
        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;
        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken ct)
            => Task.CompletedTask;
    }

 // ─── Execution tests ───────────────────────────────────────────────────

    [Fact]
    public async Task BatchInsert_CorrectKeyReturn_Succeeds()
    {
 // Normal SQLite batch insert of 2 entities must succeed and assign real PKs.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        ctx.Add(new IkcItem { Name = "Alpha" });
        ctx.Add(new IkcItem { Name = "Beta" });
        await ctx.SaveChangesAsync();

        var results = await ctx.Query<IkcItem>().ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.True(r.Id > 0));
    }

    [Fact]
    public async Task BatchInsert_ShortKeyReturn_ThrowsInvalidOperation()
    {
 // When the DB returns fewer result sets than entities, the cardinality guard
 // throws InvalidOperationException describing the mismatch.
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new ShortKeyReturnInterceptor(keysToReturn: 1));

        var (cn, ctx) = CreateContext(options);
        using var _cn = cn;
        using var _ctx = ctx;

        ctx.Add(new IkcItem { Name = "Alpha" });
        ctx.Add(new IkcItem { Name = "Beta" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.SaveChangesAsync());

        Assert.Contains("Generated key mismatch", ex.Message);
        Assert.Contains("IkcItem", ex.Message);
    }

    [Fact]
    public async Task BatchInsert_EmptyResultSet_ThrowsInvalidOperation()
    {
 // When ReadAsync returns false for every result set, keysAssigned=0 != batch.Count.
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new ShortKeyReturnInterceptor(keysToReturn: 0));

        var (cn, ctx) = CreateContext(options);
        using var _cn = cn;
        using var _ctx = ctx;

        ctx.Add(new IkcItem { Name = "Alpha" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.SaveChangesAsync());

        Assert.Contains("Generated key mismatch", ex.Message);
    }

 // ─── Provider-matrix: identity retrieval string tests ─────────────────

    [Fact]
    public void IdentityRetrieval_Sqlite_ContainsIdentityRetrieval()
    {
        var provider = new SqliteProvider();
 // With null mapping (no key columns), falls back to last_insert_rowid
        var retrieval = provider.GetIdentityRetrievalString(null!);
        Assert.Contains("last_insert_rowid()", retrieval, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void IdentityRetrieval_SqlServer_ContainsScopeIdentity()
    {
        var provider = new SqlServerProvider();
        var retrieval = provider.GetIdentityRetrievalString(null!);
        Assert.Contains("SCOPE_IDENTITY()", retrieval, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void IdentityRetrieval_MySql_ContainsLastInsertId()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var retrieval = provider.GetIdentityRetrievalString(null!);
        Assert.Contains("LAST_INSERT_ID()", retrieval, StringComparison.OrdinalIgnoreCase);
    }
}
