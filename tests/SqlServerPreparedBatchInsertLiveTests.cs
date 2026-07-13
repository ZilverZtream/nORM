using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SaveChanges batches multi-row inserts into one prepared command when the key
/// is not database-generated. SqlCommand.Prepare requires an explicit Scale on
/// time/datetime2/datetimeoffset parameters and a non-zero Size on strings
/// (including empty ones) - without them every such batch threw
/// "variable length parameters must have an explicitly set non-zero Size".
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class SqlServerPreparedBatchInsertLiveTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("PreparedBatch_Test")]
    private class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public DateTime Created { get; set; }
    }

    [Fact]
    public async Task Batched_insert_with_empty_string_prepares_on_sqlserver()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
        if (string.IsNullOrEmpty(cs)) return;
        var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
        DbConnection Open()
        {
            var c = (DbConnection)Activator.CreateInstance(t, cs)!;
            c.Open();
            return c;
        }

        void Exec(string sql)
        {
            using var c = Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        Exec("IF OBJECT_ID('PreparedBatch_Test') IS NOT NULL DROP TABLE PreparedBatch_Test");
        Exec("CREATE TABLE PreparedBatch_Test (Id INT PRIMARY KEY, Name NVARCHAR(64) NOT NULL, Amount DECIMAL(18,6) NOT NULL, Created DATETIME2 NOT NULL)");
        try
        {
            await using var ctx = new DbContext(Open(), new SqlServerProvider());
            for (var i = 1; i <= 3; i++)
                ctx.Add(new Row { Id = i, Name = i == 2 ? "" : "n" + i, Amount = i - 1.5m, Created = new DateTime(2026, 1, i) });
            await ctx.SaveChangesAsync();

            var count = ctx.Query<Row>().Count();
            Assert.Equal(3, count);
        }
        finally
        {
            Exec("IF OBJECT_ID('PreparedBatch_Test') IS NOT NULL DROP TABLE PreparedBatch_Test");
        }
    }
}
