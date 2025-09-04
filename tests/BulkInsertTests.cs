using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Mapping;
using Xunit;

namespace nORM.Tests;

public class BulkInsertTests
{
    private class TestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class FallbackSqliteProvider : DatabaseProvider
    {
        public override int MaxSqlLength => 1_000_000;
        public override int MaxParameters => 999;
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

        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection is not SqliteConnection)
                throw new InvalidOperationException("A SqliteConnection is required for FallbackSqliteProvider.");
        }
    }

    [Fact]
    public async Task BulkInsertAsync_respects_MaxParameters_limit()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TestItem(Id INTEGER, Name TEXT);";
            cmd.ExecuteNonQuery();
        }

        var provider = new FallbackSqliteProvider();
        using var ctx = new DbContext(cn, provider);
        var items = Enumerable.Range(1, 600).Select(i => new TestItem { Id = i, Name = $"Name{i}" }).ToList();

        var inserted = await ctx.BulkInsertAsync(items);
        Assert.Equal(items.Count, inserted);
    }
}
