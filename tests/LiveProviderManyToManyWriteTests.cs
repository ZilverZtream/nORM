using System;
using System.Collections.Generic;
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
/// Many-to-many collection writes through SaveChanges against real servers: each
/// provider's join-table INSERT-if-missing and DELETE SQL must apply the same
/// add/remove deltas the SQLite tests pin. Runs the full add / remove / swap
/// sequence and verifies the live join table.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderManyToManyWriteTests
{
    private const string Post = "M2mLive_Post";
    private const string Tag = "M2mLive_Tag";
    private const string Join = "M2mLive_PostTag";

    [Table(Post)]
    public class PostEntity
    {
        [Key] public int Id { get; set; }
        public List<TagEntity> Tags { get; set; } = new();
    }

    [Table(Tag)]
    public class TagEntity
    {
        [Key] public int Id { get; set; }
    }

    private static async Task ExecAsync(DbConnection cn, string sql)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string IntType(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string DropTable(ProviderKind kind, DatabaseProvider p, string t) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{t}', N'U') IS NOT NULL DROP TABLE {p.Escape(t)}"
        : $"DROP TABLE IF EXISTS {p.Escape(t)}";

    private static async Task<List<int>> ReadJoin(DbConnection cn, DatabaseProvider p)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT {p.Escape("TagId")} FROM {p.Escape(Join)} WHERE {p.Escape("PostId")} = 1 ORDER BY {p.Escape("TagId")}";
        var ids = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) ids.Add(Convert.ToInt32(reader.GetValue(0)));
        return ids;
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task M2M_add_remove_swap_hold_on_live_server(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;
        var (cn, provider) = live!.Value;
        // An open connection's ConnectionString strips the password, so re-open
        // fresh connections from the environment string, not from cn.
        var connectionString = LiveProviderEnvironment.GetConnectionString(kind.ToString().ToLowerInvariant())!;
        var connectionType = cn.GetType();

        DbConnection OpenFresh()
        {
            var fresh = (DbConnection)Activator.CreateInstance(connectionType, connectionString)!;
            fresh.Open();
            return fresh;
        }

        // The DbContext disposes the connection it is handed, so each context gets
        // its OWN connection; DDL and verification reads use the keeper `cn`.
        await using (cn)
        {
            DbContext Make()
            {
                var opts = new DbContextOptions
                {
                    OnModelCreating = mb =>
                        mb.Entity<PostEntity>().HasMany<TagEntity>(x => x.Tags).WithMany().UsingTable(Join, "PostId", "TagId")
                };
                return new DbContext(OpenFresh(), provider, opts);
            }

            await ExecAsync(cn, DropTable(kind, provider, Join));
            await ExecAsync(cn, DropTable(kind, provider, Post));
            await ExecAsync(cn, DropTable(kind, provider, Tag));
            await ExecAsync(cn, $"CREATE TABLE {provider.Escape(Post)} ({provider.Escape("Id")} {IntType(kind)} PRIMARY KEY)");
            await ExecAsync(cn, $"CREATE TABLE {provider.Escape(Tag)} ({provider.Escape("Id")} {IntType(kind)} PRIMARY KEY)");
            await ExecAsync(cn, $"CREATE TABLE {provider.Escape(Join)} ({provider.Escape("PostId")} {IntType(kind)} NOT NULL, {provider.Escape("TagId")} {IntType(kind)} NOT NULL)");
            await ExecAsync(cn, $"INSERT INTO {provider.Escape(Post)} ({provider.Escape("Id")}) VALUES (1)");
            for (var t = 1; t <= 4; t++)
                await ExecAsync(cn, $"INSERT INTO {provider.Escape(Tag)} ({provider.Escape("Id")}) VALUES ({t})");
            await ExecAsync(cn, $"INSERT INTO {provider.Escape(Join)} ({provider.Escape("PostId")}, {provider.Escape("TagId")}) VALUES (1, 1)");

            try
            {
                // Add tag 2.
                using (var ctx = Make())
                {
                    var post = ((INormQueryable<PostEntity>)ctx.Query<PostEntity>()).Include(x => x.Tags).ToList().Single();
                    post.Tags.Add(ctx.Query<TagEntity>().ToList().Single(t => t.Id == 2));
                    await ctx.SaveChangesAsync();
                }
                Assert.Equal(new[] { 1, 2 }, (await ReadJoin(cn, provider)).ToArray());

                // Swap: remove 1, add 3 and 4.
                using (var ctx = Make())
                {
                    var post = ((INormQueryable<PostEntity>)ctx.Query<PostEntity>()).Include(x => x.Tags).ToList().Single();
                    var tags = ctx.Query<TagEntity>().ToList();
                    post.Tags.RemoveAll(t => t.Id == 1);
                    post.Tags.Add(tags.Single(t => t.Id == 3));
                    post.Tags.Add(tags.Single(t => t.Id == 4));
                    await ctx.SaveChangesAsync();
                }
                Assert.Equal(new[] { 2, 3, 4 }, (await ReadJoin(cn, provider)).ToArray());

                // Clear everything.
                using (var ctx = Make())
                {
                    var post = ((INormQueryable<PostEntity>)ctx.Query<PostEntity>()).Include(x => x.Tags).ToList().Single();
                    post.Tags.Clear();
                    await ctx.SaveChangesAsync();
                }
                Assert.Empty(await ReadJoin(cn, provider));
            }
            finally
            {
                await ExecAsync(cn, DropTable(kind, provider, Join));
                await ExecAsync(cn, DropTable(kind, provider, Post));
                await ExecAsync(cn, DropTable(kind, provider, Tag));
            }
        }
    }
}
