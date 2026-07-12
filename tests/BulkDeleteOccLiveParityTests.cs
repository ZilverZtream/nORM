using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider proof that bulk delete honours the concurrency token on every
/// provider's native path. A stale-token bulk delete must skip the row (not
/// destroy a row another writer has updated); a matching-token delete removes it.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class BulkDeleteOccLiveParityTests
{
    private const string Table = "BdoccRow";

    [Table(Table)]
    public sealed class BdoccRow
    {
        [Key] public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Bulk_delete_honours_concurrency_token_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupAsync(ctx, kind);
            try
            {
                // Read the two rows so the tokens are the real, provider-assigned values.
                var rows = (await ctx.Query<BdoccRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
                Assert.Equal(2, rows.Length);

                // Row 1: corrupt the in-memory token to simulate a concurrent update by
                // another writer — the delete must skip it.
                var staleToken = (byte[])rows[0].Token.Clone();
                unchecked { staleToken[staleToken.Length - 1] ^= 0xFF; }
                var stale = new BdoccRow { Id = rows[0].Id, Payload = rows[0].Payload, Token = staleToken };

                var skipped = await ctx.BulkDeleteAsync(new[] { stale });
                Assert.Equal(0, skipped);
                Assert.Equal(2, await ctx.Query<BdoccRow>().CountAsync());

                // Row 2: correct token — deletes.
                var deleted = await ctx.BulkDeleteAsync(new[] { rows[1] });
                Assert.Equal(1, deleted);
                Assert.Equal(1, await ctx.Query<BdoccRow>().CountAsync());
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(Table);
        var id = ctx.Provider.Escape(nameof(BdoccRow.Id));
        var payload = ctx.Provider.Escape(nameof(BdoccRow.Payload));
        var token = ctx.Provider.Escape(nameof(BdoccRow.Token));
        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(40)" : "VARCHAR(40)";
        var tokenType = kind switch
        {
            ProviderKind.SqlServer => "ROWVERSION",
            ProviderKind.Postgres => "BYTEA",
            ProviderKind.MySql => "VARBINARY(8)",
            _ => "BLOB",
        };

        await ExecuteAsync(ctx, drop);
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {payload} {textType} NOT NULL, {token} {tokenType} {(kind == ProviderKind.SqlServer ? "" : "NOT NULL")})");

        // SQL Server ROWVERSION is auto-generated; other providers get explicit seed tokens.
        if (kind == ProviderKind.SqlServer)
        {
            await ExecuteAsync(ctx, $"INSERT INTO {table} ({id},{payload}) VALUES (1,'a'),(2,'b')");
        }
        else if (kind == ProviderKind.Postgres)
        {
            await ExecuteAsync(ctx, $"INSERT INTO {table} ({id},{payload},{token}) VALUES (1,'a',E'\\\\x0000000000000001'),(2,'b',E'\\\\x0000000000000001')");
        }
        else if (kind == ProviderKind.MySql)
        {
            await ExecuteAsync(ctx, $"INSERT INTO {table} ({id},{payload},{token}) VALUES (1,'a',0x0000000000000001),(2,'b',0x0000000000000001)");
        }
        else
        {
            await ExecuteAsync(ctx, $"INSERT INTO {table} ({id},{payload},{token}) VALUES (1,'a',X'0000000000000001'),(2,'b',X'0000000000000001')");
        }
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            var table = ctx.Provider.Escape(Table);
            var drop = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table}"
                : $"DROP TABLE IF EXISTS {table}";
            await ExecuteAsync(ctx, drop);
        }
        catch
        {
            // best-effort cleanup only
        }
    }

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
