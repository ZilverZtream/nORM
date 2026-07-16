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
/// Live proof for the char-needle string predicates on the LIKE-path providers: the
/// ordinal (case-sensitive) contract must hold through each server's collation — SQL
/// Server and MySQL fold case under their default collations unless the translation
/// forces the binary comparison, and SQLite's instr/substr bypass cannot stand in for
/// that evidence. Also proves the closure-captured char needle re-binds across cached
/// plans against real servers.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class StringCharNeedlePredicateLiveTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CharNeedleLive_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string? Name { get; set; }
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        static DbConnection Open(Type t, string cs)
        {
            var cn = (DbConnection)Activator.CreateInstance(t, cs)!;
            cn.Open();
            return cn;
        }
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_MYSQL not set");
                if (!cs.Contains("UseAffectedRows", StringComparison.OrdinalIgnoreCase))
                    cs = cs.TrimEnd(';') + ";UseAffectedRows=false";
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                var csMysql = cs;
                return (() => Open(t, csMysql), new MySqlProvider(new SqliteParameterFactory(), useAffectedRowsSemantics: false), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            default:
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), null);
            }
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Char_needle_predicates_match_ordinally_and_rebind_closures(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        void Exec(string sql)
        {
            using var cn = factory!();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        Exec(kind == "postgres"
            ? "DROP TABLE IF EXISTS \"CharNeedleLive_Test\""
            : kind == "sqlserver"
                ? "IF OBJECT_ID('CharNeedleLive_Test') IS NOT NULL DROP TABLE CharNeedleLive_Test"
                : "DROP TABLE IF EXISTS CharNeedleLive_Test");
        Exec(kind == "postgres"
            ? "CREATE TABLE \"CharNeedleLive_Test\" (\"Id\" INT PRIMARY KEY, \"Name\" TEXT NULL)"
            : kind == "sqlserver"
                ? "CREATE TABLE CharNeedleLive_Test (Id INT PRIMARY KEY, Name NVARCHAR(64) NULL)"
                : "CREATE TABLE CharNeedleLive_Test (Id INT PRIMARY KEY, Name VARCHAR(64) NULL)");
        try
        {
            using (var seedCtx = new DbContext(factory!(), provider!))
            {
                seedCtx.Add(new Row { Id = 1, Name = "alpha" });
                seedCtx.Add(new Row { Id = 2, Name = "Beta" });
                seedCtx.Add(new Row { Id = 3, Name = "gamma" });
                seedCtx.Add(new Row { Id = 4, Name = null });
                seedCtx.Add(new Row { Id = 5, Name = "AZURE" });
                await seedCtx.SaveChangesAsync();
            }

            using var ctx = new DbContext(factory!(), provider!);
            async Task<int[]> IdsAsync(IQueryable<Row> q)
                => (await q.ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

            // Ordinal case-sensitivity through the provider's LIKE path: 'A' only in AZURE.
            Assert.Equal(new[] { 5 }, await IdsAsync(ctx.Query<Row>().Where(x => x.Name!.Contains('A'))));
            Assert.Equal(new[] { 1, 2, 3 }, await IdsAsync(ctx.Query<Row>().Where(x => x.Name!.Contains('a'))));
            Assert.Equal(new[] { 2 }, await IdsAsync(ctx.Query<Row>().Where(x => x.Name!.StartsWith('B'))));
            Assert.Empty(await IdsAsync(ctx.Query<Row>().Where(x => x.Name!.StartsWith('b'))));
            Assert.Equal(new[] { 5 }, await IdsAsync(ctx.Query<Row>().Where(x => x.Name!.EndsWith('E'))));

            // Ignore-case comparison overload folds case on the server.
            Assert.Equal(new[] { 5 }, await IdsAsync(ctx.Query<Row>()
                .Where(x => x.Name!.Contains('z', StringComparison.OrdinalIgnoreCase))));

            // Negation keeps the NULL row.
            Assert.Equal(new[] { 4, 5 }, await IdsAsync(ctx.Query<Row>().Where(x => !x.Name!.Contains('a'))));

            // Closure-captured char needle re-binds across the cached plan.
            var needle = 'g';
            Assert.Equal(new[] { 3 }, await IdsAsync(ctx.Query<Row>().Where(x => x.Name!.Contains(needle))));
            needle = 'B';
            Assert.Equal(new[] { 2 }, await IdsAsync(ctx.Query<Row>().Where(x => x.Name!.Contains(needle))));

            // LIKE metacharacters stay literal.
            using (var addCtx = new DbContext(factory!(), provider!))
            {
                addCtx.Add(new Row { Id = 6, Name = "100%" });
                addCtx.Add(new Row { Id = 7, Name = "100x" });
                await addCtx.SaveChangesAsync();
            }
            Assert.Equal(new[] { 6 }, await IdsAsync(ctx.Query<Row>().Where(x => x.Name!.Contains('%'))));
        }
        finally
        {
            Exec(kind == "postgres"
                ? "DROP TABLE IF EXISTS \"CharNeedleLive_Test\""
                : kind == "sqlserver"
                    ? "IF OBJECT_ID('CharNeedleLive_Test') IS NOT NULL DROP TABLE CharNeedleLive_Test"
                    : "DROP TABLE IF EXISTS CharNeedleLive_Test");
        }
    }
}
