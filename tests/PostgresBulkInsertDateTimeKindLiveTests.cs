using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Npgsql 6+ refuses to write a DateTime whose Kind is Utc into a
/// 'timestamp without time zone' column (and Local into 'timestamptz'), so any
/// bind path that skips nORM's Kind normalization breaks on the most common
/// write pattern there is: entity.CreatedAt = DateTime.UtcNow. The regular
/// parameter layers normalize to Unspecified; the binary COPY bulk-insert path
/// hands raw values to NpgsqlBinaryImporter and must normalize the same way.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class PostgresBulkInsertDateTimeKindLiveTests
{
    [Table("PgBulkKind_Test")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenPostgres()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
        if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_POSTGRES not set");
        var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
        return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
    }

    private static DbConnection Open(Type connectionType, string cs)
    {
        var cn = (DbConnection)Activator.CreateInstance(connectionType, cs)!;
        cn.Open();
        return cn;
    }

    private static void Exec(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Bulk_insert_accepts_utc_and_local_kinds_like_savechanges_does()
    {
        var (factory, provider, skip) = OpenPostgres();
        if (skip != null) return;

        Exec(factory!, "DROP TABLE IF EXISTS \"PgBulkKind_Test\"");
        Exec(factory!, "CREATE TABLE \"PgBulkKind_Test\" (\"Id\" INT PRIMARY KEY, \"CreatedAt\" TIMESTAMP NOT NULL)");
        try
        {
            var utcStamp = new DateTime(2026, 7, 13, 12, 30, 15, DateTimeKind.Utc).AddTicks(7_004_560);
            var localStamp = new DateTime(2026, 7, 13, 6, 5, 4, DateTimeKind.Local).AddTicks(3_210_990);

            await using (var ctx = new DbContext(factory!(), provider!))
            {
                var count = await ctx.BulkInsertAsync(new[]
                {
                    new Row { Id = 1, CreatedAt = utcStamp },
                    new Row { Id = 2, CreatedAt = localStamp },
                });
                Assert.Equal(2, count);
            }

            await using (var ctx = new DbContext(factory!(), provider!))
            {
                var rows = ctx.Query<Row>().OrderBy(r => r.Id).ToList();
                // The wall-clock value is stored as-is (matching the SaveChanges
                // path, which binds Kind=Unspecified without converting).
                Assert.Equal(utcStamp, rows[0].CreatedAt);
                Assert.Equal(localStamp, rows[1].CreatedAt);
            }
        }
        finally
        {
            Exec(factory!, "DROP TABLE IF EXISTS \"PgBulkKind_Test\"");
        }
    }
}
