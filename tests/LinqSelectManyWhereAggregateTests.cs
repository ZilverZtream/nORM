using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the flatten-filter-aggregate composition:
/// <c>parents.SelectMany(p =&gt; p.Items).Where(i =&gt; i.X &gt; n).Sum(i =&gt; i.Amount)</c>.
/// Each step uses different translator paths (SelectMany rewrite, Where on
/// the flattened element, scalar aggregate); the composition is a real-world
/// "total amount of qualifying items across all parents" reporting query
/// and is the most natural way silent-wrongness shapes (wrong filter scope,
/// wrong SUM target) would surface.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSelectManyWhereAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmwParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SmwChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO SmwParent VALUES (1, 'A'), (2, 'B'), (3, 'C');
            -- Per-parent items (all-items totals: 1->60, 2->105, 3->0)
            -- Items > 10:
            --   Parent 1: 20, 30          -> sum 50, count 2
            --   Parent 2: 100             -> sum 100, count 1
            --   Parent 3: (no children)
            -- Global qualifying sum: 50 + 100 = 150, count = 3.
            INSERT INTO SmwChild VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 5), (14, 2, 100);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SmwParent>().HasKey(p => p.Id);
                mb.Entity<SmwChild>().HasKey(c => c.Id);
                mb.Entity<SmwParent>()
                    .HasMany(p => p.Items!)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task SelectMany_then_where_then_sum_aggregates_qualifying_children_across_all_parents()
    {
        // Flatten all children, filter Amount > 10, sum the qualifying amounts.
        // Silent-wrongness shapes:
        //   * Where applied at parent level not child level -> drops all
        //     parents whose first child <= 10, undercounts
        //   * SUM target wrong column -> result diverges from 150
        var total = await _ctx.Query<SmwParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 10)
            .SumAsync(i => i.Amount);
        Assert.Equal(150, total);
    }

    [Fact]
    public async Task SelectMany_then_where_then_materialise_then_count_returns_correct_count()
    {
        // Diagnostic: materialise the IQueryable<SmwChild> sequence first then
        // .Count() in memory. The SelectMany+Where projection works correctly
        // and produces the expected 3-row sequence -- isolating the bug below
        // to the terminal CountAsync translation path.
        var rows = await _ctx.Query<SmwParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 10)
            .ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    private sealed class SqlCapturingInterceptor : nORM.Enterprise.IDbCommandInterceptor
    {
        private readonly System.Collections.Generic.List<string> _sink;
        public SqlCapturingInterceptor(System.Collections.Generic.List<string> sink) { _sink = sink; }

        public Task<nORM.Enterprise.InterceptionResult<int>> NonQueryExecutingAsync(System.Data.Common.DbCommand command, nORM.Core.DbContext context, System.Threading.CancellationToken ct)
        { _sink.Add($"[NonQuery] {command.CommandText}"); return Task.FromResult(default(nORM.Enterprise.InterceptionResult<int>)); }
        public Task NonQueryExecutedAsync(System.Data.Common.DbCommand command, nORM.Core.DbContext context, int result, System.TimeSpan duration, System.Threading.CancellationToken ct) => Task.CompletedTask;
        public Task<nORM.Enterprise.InterceptionResult<object?>> ScalarExecutingAsync(System.Data.Common.DbCommand command, nORM.Core.DbContext context, System.Threading.CancellationToken ct)
        { _sink.Add($"[Scalar] {command.CommandText}"); return Task.FromResult(default(nORM.Enterprise.InterceptionResult<object?>)); }
        public Task ScalarExecutedAsync(System.Data.Common.DbCommand command, nORM.Core.DbContext context, object? result, System.TimeSpan duration, System.Threading.CancellationToken ct) => Task.CompletedTask;
        public Task<nORM.Enterprise.InterceptionResult<System.Data.Common.DbDataReader>> ReaderExecutingAsync(System.Data.Common.DbCommand command, nORM.Core.DbContext context, System.Threading.CancellationToken ct)
        { _sink.Add($"[Reader] {command.CommandText}"); return Task.FromResult(default(nORM.Enterprise.InterceptionResult<System.Data.Common.DbDataReader>)); }
        public Task ReaderExecutedAsync(System.Data.Common.DbCommand command, nORM.Core.DbContext context, System.Data.Common.DbDataReader reader, System.TimeSpan duration, System.Threading.CancellationToken ct) => Task.CompletedTask;
        public Task CommandFailedAsync(System.Data.Common.DbCommand command, nORM.Core.DbContext context, System.Exception exception, System.Threading.CancellationToken ct) => Task.CompletedTask;

        // Sync hooks -- SQLite's PrefersSyncExecution=true takes the sync paths, so
        // override these too to catch the actual emit path.
        public nORM.Enterprise.InterceptionResult<int> NonQueryExecuting(System.Data.Common.DbCommand command, nORM.Core.DbContext context)
        { _sink.Add($"[NonQuery-sync] {command.CommandText}"); return nORM.Enterprise.InterceptionResult<int>.Continue(); }
        public void NonQueryExecuted(System.Data.Common.DbCommand command, nORM.Core.DbContext context, int result, System.TimeSpan duration) { }
        public nORM.Enterprise.InterceptionResult<object?> ScalarExecuting(System.Data.Common.DbCommand command, nORM.Core.DbContext context)
        { _sink.Add($"[Scalar-sync] {command.CommandText}"); return nORM.Enterprise.InterceptionResult<object?>.Continue(); }
        public void ScalarExecuted(System.Data.Common.DbCommand command, nORM.Core.DbContext context, object? result, System.TimeSpan duration) { }
        public nORM.Enterprise.InterceptionResult<System.Data.Common.DbDataReader> ReaderExecuting(System.Data.Common.DbCommand command, nORM.Core.DbContext context)
        { _sink.Add($"[Reader-sync] {command.CommandText}"); return nORM.Enterprise.InterceptionResult<System.Data.Common.DbDataReader>.Continue(); }
        public void ReaderExecuted(System.Data.Common.DbCommand command, nORM.Core.DbContext context, System.Data.Common.DbDataReader reader, System.TimeSpan duration) { }
        public void CommandFailed(System.Data.Common.DbCommand command, nORM.Core.DbContext context, System.Exception exception) { }
    }

    [Fact]
    public async Task SelectMany_then_where_then_countasync_returns_correct_count()
    {
        // Earlier silent-wrongness (8c12072 / 5215fdd diagnostic): SelectMany
        // populates _sql with `SELECT <cols> FROM Parent JOIN Child ...` via
        // HandleSelectMany; QueryTranslator's Generate then skipped the
        // entire prefix-build (including the aggregate COUNT(*) prepend)
        // because it's guarded by `if (_sql.Length == 0)`. ExecuteScalar
        // on the raw SELECT returned the first row's first column (T1.Id
        // == 11) instead of an aggregate.
        // Fix rewrites the SELECT-clause column list to COUNT(*) when
        // _isAggregate && _sql.Length > 0.
        var count = await _ctx.Query<SmwParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 10)
            .CountAsync();
        Assert.Equal(3, count);
    }

    [Table("SmwParent")]
    public sealed class SmwParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<SmwChild>? Items { get; set; }
    }

    [Table("SmwChild")]
    public sealed class SmwChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
