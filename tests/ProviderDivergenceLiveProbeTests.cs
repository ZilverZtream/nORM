using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live probes for the two recorded provider divergences (provider-mobility matrix cells).
/// SQL Server orders uniqueidentifier by its byte groups from the END first, so ORDER BY over
/// Guid columns DIVERGES from .NET Guid.CompareTo - pinned here as the documented design
/// exception with a crafted pair that sorts opposite ways. And MySQL's native BIGINT UNSIGNED
/// column obeys the portable signed-64 ulong contract: values within long range round-trip,
/// values above it fail loud at write time.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ProviderDivergenceLiveProbeTests
{
    private static DbConnection Open(string typeName, string cs)
    {
        var cn = (DbConnection)Activator.CreateInstance(Type.GetType(typeName)!, cs)!;
        cn.Open();
        return cn;
    }

    private static void ExecIgnore(Func<DbConnection> factory, params string[] sqls)
    {
        foreach (var sql in sqls)
        {
            try
            {
                using var cn = factory();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
            catch { }
        }
    }

    [Table("GuidOrderLive")]
    private class GuidRow
    {
        [Key] public int Id { get; set; }
        public Guid G { get; set; }
    }

    [Fact]
    public async Task SqlServer_uniqueidentifier_order_by_diverges_from_dotnet_guid_comparison()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
        if (string.IsNullOrEmpty(cs)) return;
        Func<DbConnection> factory = () => Open("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs);

        ExecIgnore(factory, "DROP TABLE GuidOrderLive");
        ExecIgnore(factory, "CREATE TABLE GuidOrderLive (Id INT PRIMARY KEY, G UNIQUEIDENTIFIER NOT NULL)");
        try
        {
            // .NET compares the FIRST int group first; SQL Server compares the LAST byte group
            // first - this pair sorts opposite ways under the two orders.
            var a = Guid.Parse("00000000-0000-0000-0000-010000000000");
            var b = Guid.Parse("01000000-0000-0000-0000-000000000000");
            ExecIgnore(factory,
                $"INSERT INTO GuidOrderLive VALUES (1, '{a}')",
                $"INSERT INTO GuidOrderLive VALUES (2, '{b}')");

            await using var ctx = new DbContext(factory(), new SqlServerProvider(), new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<GuidRow>()
            });
            var serverOrder = (await ctx.Query<GuidRow>().OrderBy(r => r.G).ToListAsync()).Select(r => r.G).ToArray();
            var dotnetOrder = new[] { a, b }.OrderBy(g => g).ToArray();

            // The DIVERGENCE is the pinned contract: uniqueidentifier ordering is a
            // provider-bound design exception, not .NET Guid.CompareTo.
            Assert.Equal(new[] { b, a }, serverOrder);
            Assert.Equal(new[] { a, b }, dotnetOrder);
        }
        finally
        {
            ExecIgnore(factory, "DROP TABLE GuidOrderLive");
        }
    }

    [Table("UlongUnsignedLive")]
    private class UlongRow
    {
        [Key] public int Id { get; set; }
        public ulong U { get; set; }
    }

    [Fact]
    public async Task MySql_bigint_unsigned_column_obeys_the_signed64_ulong_contract()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
        if (string.IsNullOrEmpty(cs)) return;
        Func<DbConnection> factory = () => Open("MySqlConnector.MySqlConnection, MySqlConnector", cs);

        ExecIgnore(factory, "DROP TABLE UlongUnsignedLive");
        ExecIgnore(factory, "CREATE TABLE UlongUnsignedLive (Id INT PRIMARY KEY, U BIGINT UNSIGNED NOT NULL)");
        try
        {
            await using var ctx = new DbContext(factory(), new MySqlProvider(new SqliteParameterFactory()), new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<UlongRow>()
            });

            // In-range values round-trip exactly, even against the WIDER native column.
            ctx.Add(new UlongRow { Id = 1, U = (ulong)long.MaxValue });
            await ctx.SaveChangesAsync();
            var back = await ctx.Query<UlongRow>().Where(r => r.Id == 1).ToListAsync();
            Assert.Equal((ulong)long.MaxValue, Assert.Single(back).U);

            // Above long.MaxValue the portable contract fails LOUD at write time - never a
            // silent wrap, even though BIGINT UNSIGNED could store the value natively.
            ctx.Add(new UlongRow { Id = 2, U = (ulong)long.MaxValue + 1 });
            await Assert.ThrowsAnyAsync<NormException>(() => ctx.SaveChangesAsync());
        }
        finally
        {
            ExecIgnore(factory, "DROP TABLE UlongUnsignedLive");
        }
    }
}
