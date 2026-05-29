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

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for the v1 temporal/versioning contract.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
[Collection(LiveTemporalProviderCollection.Name)]
public class LiveProviderTemporalParityTests
{
    private const string Table = "TlpLiveRow";
    private const string HistoryTable = Table + "_History";
    private const string TagsTable = "__NormTemporalTags";

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        _ => $"VARCHAR({len})"
    };

    private static string DropTableDdl(ProviderKind kind, string tableName, string escapedTable) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{tableName}', N'U') IS NOT NULL DROP TABLE {escapedTable};"
        : $"DROP TABLE IF EXISTS {escapedTable};";

    private static async Task ExecuteAsync(DbConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task ExecuteScalarIgnoredAsync(DbConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        _ = await cmd.ExecuteScalarAsync();
    }

    private static async Task SetupAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownAsync(connection, provider, kind);

        var table = provider.Escape(Table);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntCol(kind)} PRIMARY KEY, {name} {VarCol(kind, 40)} NOT NULL)");
    }

    private static async Task TeardownAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            if (kind == ProviderKind.Postgres)
            {
                await ExecuteAsync(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_TemporalTrigger")} ON {provider.Escape(Table)}");
                await ExecuteAsync(connection,
                    $"DROP FUNCTION IF EXISTS {provider.Escape(Table + "_TemporalFunction")}()");
            }
            else if (kind == ProviderKind.MySql)
            {
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_ai")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_au")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_ad")}");
            }
            else if (kind == ProviderKind.SqlServer)
            {
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_TemporalInsert")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_TemporalUpdate")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_TemporalDelete")}");
            }

            await ExecuteAsync(connection, DropTableDdl(kind, Table, provider.Escape(Table)));
            await ExecuteAsync(connection, DropTableDdl(kind, HistoryTable, provider.Escape(HistoryTable)));
        }
        catch
        {
            // Best-effort cleanup; the actual test operation reports failures.
        }
    }

    private static async Task<bool> TableExistsAsync(DbConnection connection, ProviderKind kind, string tableName)
    {
        var sql = kind switch
        {
            ProviderKind.Sqlite =>
                $"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{tableName}'",
            ProviderKind.SqlServer =>
                $"SELECT CASE WHEN OBJECT_ID(N'{tableName}', N'U') IS NULL THEN 0 ELSE 1 END",
            ProviderKind.Postgres =>
                $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '{tableName}'",
            ProviderKind.MySql =>
                $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{tableName}'",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync()) > 0;
    }

    private static async Task<long> CountAsync(DbConnection connection, DatabaseProvider provider, string tableName)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {provider.Escape(tableName)}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<long> CountTagAsync(DbConnection connection, DatabaseProvider provider, string tagName)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            $"SELECT COUNT(*) FROM {provider.Escape(TagsTable)} WHERE {provider.Escape("TagName")} = {provider.ParamPrefix}p0";
        var parameter = cmd.CreateParameter();
        parameter.ParameterName = provider.ParamPrefix + "p0";
        parameter.Value = tagName;
        cmd.Parameters.Add(parameter);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<string?> ReadPayloadRawAsync(DbContext ctx, int idValue)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText =
            $"SELECT {ctx.Provider.Escape("Name")} FROM {ctx.Provider.Escape(Table)} WHERE {ctx.Provider.Escape("Id")} = {idValue}";
        var result = await cmd.ExecuteScalarAsync();
        return result is DBNull or null ? null : (string)result;
    }

    private static async Task ExerciseHistoryTriggersAsync(DbConnection connection, DatabaseProvider provider)
    {
        var table = provider.Escape(Table);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection, $"INSERT INTO {table} ({id}, {name}) VALUES (1, 'alpha')");
        await ExecuteAsync(connection, $"UPDATE {table} SET {name} = 'beta' WHERE {id} = 1");
        await ExecuteAsync(connection, $"DELETE FROM {table} WHERE {id} = 1");
    }

    [Table(Table)]
    private sealed class TlpLiveRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Temporal_bootstrap_tags_and_history_triggers_work_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            var options = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<TlpLiveRow>()
            };
            options.EnableTemporalVersioning();

            using var ctx = new DbContext(connection, provider, options);
            await SetupAsync(connection, provider, kind);
            try
            {
                Assert.Equal(0, await ctx.Query<TlpLiveRow>().CountAsync());
                Assert.True(await TableExistsAsync(connection, kind, TagsTable));
                Assert.True(await TableExistsAsync(connection, kind, HistoryTable));

                var tagName = "tlp-" + Guid.NewGuid().ToString("N");
                await ctx.CreateTagAsync(tagName);
                Assert.Equal(1, await CountTagAsync(connection, provider, tagName));

                await ExerciseHistoryTriggersAsync(connection, provider);

                var currentAsOfFuture = await ctx.Query<TlpLiveRow>()
                    .AsOf(DateTime.UtcNow.AddMinutes(1))
                    .CountAsync();

                Assert.Equal(0, currentAsOfFuture);
                Assert.Equal(3, await CountAsync(connection, provider, HistoryTable));
            }
            finally
            {
                await TeardownAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Temporal_as_of_tag_returns_previous_state_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            var options = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<TlpLiveRow>()
            };
            options.EnableTemporalVersioning();

            using var ctx = new DbContext(connection, provider, options);
            await SetupAsync(connection, provider, kind);
            try
            {
                Assert.Equal(0, await ctx.Query<TlpLiveRow>().CountAsync());

                ctx.Add(new TlpLiveRow { Id = 10, Name = "before" });
                await ctx.SaveChangesAsync();

                var tagName = "tlp-asof-" + Guid.NewGuid().ToString("N");
                await ctx.CreateTagAsync(tagName);

                // SQLite temporal trigger timestamps have second precision.
                await Task.Delay(TimeSpan.FromMilliseconds(1100));

                var current = await ctx.Query<TlpLiveRow>().SingleAsync(r => r.Id == 10);
                current.Name = "after";
                await ctx.SaveChangesAsync();

                var historical = await ctx.Query<TlpLiveRow>()
                    .AsOf(tagName)
                    .SingleAsync(r => r.Id == 10);
                var currentName = await ReadPayloadRawAsync(ctx, 10);

                Assert.Equal("before", historical.Name);
                Assert.Equal("after", currentName);
            }
            finally
            {
                await TeardownAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Temporal_history_diff_restore_and_prune_work_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            var options = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<TlpLiveRow>()
            };
            options.EnableTemporalVersioning();

            using var ctx = new DbContext(connection, provider, options);
            await SetupAsync(connection, provider, kind);
            try
            {
                ctx.Add(new TlpLiveRow { Id = 20, Name = "alpha" });
                await ctx.SaveChangesAsync();

                var tagName = "tlp-restore-" + Guid.NewGuid().ToString("N");
                await ctx.CreateTagAsync(tagName);
                await Task.Delay(TimeSpan.FromMilliseconds(1100));

                var current = await ctx.Query<TlpLiveRow>().SingleAsync(r => r.Id == 20);
                current.Name = "beta";
                await ctx.SaveChangesAsync();

                var diff = await ctx.GetTemporalDiffAsync<TlpLiveRow>(20);
                var entry = Assert.Single(diff);
                Assert.Contains(entry.Changes, c => c.PropertyName == nameof(TlpLiveRow.Name) &&
                                                    Equals(c.PreviousValue, "alpha") &&
                                                    Equals(c.CurrentValue, "beta"));

                var restored = await ctx.RestoreTemporalVersionAsync<TlpLiveRow>(20, tagName);
                var restoredName = await ReadPayloadRawAsync(ctx, 20);
                Assert.Equal(1, restored);
                Assert.Equal("alpha", restoredName);

                var beforePrune = await CountAsync(connection, provider, HistoryTable);
                var pruned = await ctx.PruneTemporalHistoryAsync<TlpLiveRow>(DateTime.UtcNow.AddMinutes(1));
                var afterPrune = await CountAsync(connection, provider, HistoryTable);

                Assert.True(beforePrune > 0);
                Assert.True(pruned > 0);
                Assert.True(afterPrune < beforePrune);
            }
            finally
            {
                await TeardownAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Temporal_tag_and_history_paths_participate_in_explicit_transaction_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            var options = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<TlpLiveRow>()
            };
            options.EnableTemporalVersioning();

            using var ctx = new DbContext(connection, provider, options);
            await SetupAsync(connection, provider, kind);
            try
            {
                Assert.Equal(0, await ctx.Query<TlpLiveRow>().CountAsync());

                var tagName = "tlp-tx-" + Guid.NewGuid().ToString("N");
                await using (var tx = await ctx.Database.BeginTransactionAsync())
                {
                    await ctx.CreateTagAsync(tagName);
                    ctx.Add(new TlpLiveRow { Id = 30, Name = "inside-tx" });
                    await ctx.SaveChangesAsync();

                    var history = await ctx.GetTemporalHistoryAsync<TlpLiveRow>(30);
                    Assert.Single(history);
                    Assert.Equal("inside-tx", history[0].Entity.Name);

                    await tx.RollbackAsync();
                }

                Assert.Equal(0, await CountTagAsync(connection, provider, tagName));
                Assert.Equal(0, await ctx.Query<TlpLiveRow>().CountAsync());
            }
            finally
            {
                await TeardownAsync(connection, provider, kind);
            }
        }
    }
}
