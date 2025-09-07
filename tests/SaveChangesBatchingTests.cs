using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Enterprise;
using nORM.Mapping;
using nORM.Configuration;
using Xunit;

namespace nORM.Tests;

public class SaveChangesBatchingTests
{
    private sealed class FallbackSqliteProvider : DatabaseProvider
    {
        public override int MaxSqlLength => 1_000_000;
        public override int MaxParameters => 5;
        public override string Escape(string id) => $"\"{id}\"";

        public override void ApplyPaging(StringBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            if (limitParameterName != null) sb.Append(" LIMIT ").Append(limitParameterName);
            if (offsetParameterName != null) sb.Append(" OFFSET ").Append(offsetParameterName);
        }

        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT last_insert_rowid();";
        public override DbParameter CreateParameter(string name, object? value) => new SqliteParameter(name, value ?? DBNull.Value);
        public override string? TranslateFunction(string name, Type declaringType, params string[] args) => null;
        public override string TranslateJsonPathAccess(string columnName, string jsonPath) => $"json_extract({columnName}, '{jsonPath}')";

        public override string GenerateCreateHistoryTableSql(TableMapping mapping) => throw new NotImplementedException();
        public override string GenerateTemporalTriggersSql(TableMapping mapping) => throw new NotImplementedException();

        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection is not SqliteConnection)
                throw new InvalidOperationException("A SqliteConnection is required for FallbackSqliteProvider.");
        }
    }

    private sealed class ParamLimitInterceptor : IDbCommandInterceptor
    {
        private readonly int _max;
        public ParamLimitInterceptor(int max) => _max = max;
        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            Assert.True(command.Parameters.Count <= _max);
            return Task.FromResult(InterceptionResult<int>.Continue());
        }
        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken) => Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken) => Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private class User
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public async Task SaveChangesAsync_respects_MaxParameters_limit()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER PRIMARY KEY, Name TEXT);";
            cmd.ExecuteNonQuery();
        }

        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new ParamLimitInterceptor(5));

        using var ctx = new DbContext(cn, new FallbackSqliteProvider(), options);
        for (int i = 0; i < 10; i++)
            ctx.Add(new User { Id = i + 1, Name = $"User{i}" });

        var saved = await ctx.SaveChangesAsync();
        Assert.Equal(10, saved);
    }
}
