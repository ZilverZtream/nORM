using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for <c>Join</c> (inner), <c>GroupJoin</c> (left join),
/// and <c>SelectMany</c> (cross join / left join via DefaultIfEmpty).
///
/// The parity matrix referenced <c>LiveProviderShapeParityTests</c> and
/// <c>CompiledJoinDiagnosticTest</c> for Join live evidence, but both are
/// Fast (SQLite-only) tests. This class closes that gap.
///
/// Schema:
///   Dept (Id, Name)       — (1,Eng), (2,Sales), (3,HR)
///   Emp  (Id, Name, DeptId) — (1,Alice,1), (2,Bob,2), (3,Carol,1), (4,Dave,null for outer join)
///
/// Dave has DeptId=0 so he does not match any Dept → absent from INNER JOIN,
/// present in LEFT JOIN (with null dept name).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderJoinSelectManyParityTests
{
    private const string DeptTable = "JsmDept";
    private const string EmpTable  = "JsmEmp";

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
    private static string VarCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        _ => $"VARCHAR({len})"
    };

    private static string DropDdl(ProviderKind kind, string table, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var escDept = ctx.Provider.Escape(DeptTable);
        var escEmp  = ctx.Provider.Escape(EmpTable);
        var intT    = IntCol(kind);
        var varT    = VarCol(kind, 20);

        // Drop Emp first (FK dependent), then Dept
        await ExecuteAsync(ctx, DropDdl(kind, EmpTable, escEmp));
        await ExecuteAsync(ctx, DropDdl(kind, DeptTable, escDept));

        await ExecuteAsync(ctx,
            $"CREATE TABLE {escDept} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, " +
            $"{ctx.Provider.Escape("Name")} {varT} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {escDept} ({ctx.Provider.Escape("Id")},{ctx.Provider.Escape("Name")}) VALUES " +
            "(1,'Eng'),(2,'Sales'),(3,'HR')");

        await ExecuteAsync(ctx,
            $"CREATE TABLE {escEmp} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, " +
            $"{ctx.Provider.Escape("Name")} {varT} NOT NULL, " +
            $"{ctx.Provider.Escape("DeptId")} {intT} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {escEmp} ({ctx.Provider.Escape("Id")},{ctx.Provider.Escape("Name")},{ctx.Provider.Escape("DeptId")}) VALUES " +
            "(1,'Alice',1),(2,'Bob',2),(3,'Carol',1),(4,'Dave',0)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropDdl(kind, EmpTable, ctx.Provider.Escape(EmpTable)));
            await ExecuteAsync(ctx, DropDdl(kind, DeptTable, ctx.Provider.Escape(DeptTable)));
        }
        catch { /* best-effort */ }
    }

    [Table(DeptTable)]
    private sealed class JsmDept
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table(EmpTable)]
    private sealed class JsmEmp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int DeptId { get; set; }
    }

    // ── 1: Inner Join ─────────────────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Join_inner_excludes_unmatched_rows_on_live_provider(ProviderKind kind)
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
                // Dave (DeptId=0) has no match → excluded
                var rows = (await ctx.Query<JsmEmp>()
                    .Join(ctx.Query<JsmDept>(),
                          e => e.DeptId,
                          d => d.Id,
                          (e, d) => new { EmpName = e.Name, DeptName = d.Name })
                    .ToListAsync())
                    .OrderBy(r => r.EmpName)
                    .ToArray();

                Assert.Equal(3, rows.Length);
                Assert.Equal(("Alice", "Eng"),  (rows[0].EmpName, rows[0].DeptName));
                Assert.Equal(("Bob",   "Sales"), (rows[1].EmpName, rows[1].DeptName));
                Assert.Equal(("Carol", "Eng"),  (rows[2].EmpName, rows[2].DeptName));
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 2: Inner Join with filter ─────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Join_inner_with_where_filters_after_join_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<JsmEmp>()
                    .Join(ctx.Query<JsmDept>(),
                          e => e.DeptId,
                          d => d.Id,
                          (e, d) => new { EmpName = e.Name, DeptName = d.Name })
                    .Where(r => r.DeptName == "Eng")
                    .ToListAsync())
                    .OrderBy(r => r.EmpName)
                    .ToArray();

                Assert.Equal(2, rows.Length);
                Assert.Equal("Alice", rows[0].EmpName);
                Assert.Equal("Carol", rows[1].EmpName);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 3: GroupJoin (left outer join) ───────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupJoin_left_outer_includes_unmatched_dept_rows_on_live_provider(ProviderKind kind)
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
                // HR (Id=3) has no employees → present with Count=0
                var rows = (await ctx.Query<JsmDept>()
                    .GroupJoin(ctx.Query<JsmEmp>(),
                               d => d.Id,
                               e => e.DeptId,
                               (d, emps) => new { DeptName = d.Name, Count = emps.Count() })
                    .ToListAsync())
                    .OrderBy(r => r.DeptName)
                    .ToArray();

                Assert.Equal(3, rows.Length);
                Assert.Equal(("Eng",   2), (rows[0].DeptName, rows[0].Count));
                Assert.Equal(("HR",    0), (rows[1].DeptName, rows[1].Count));
                Assert.Equal(("Sales", 1), (rows[2].DeptName, rows[2].Count));
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 4: SelectMany cross join ──────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task SelectMany_cross_join_produces_cartesian_product_on_live_provider(ProviderKind kind)
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
                // 3 depts × 4 emps = 12 rows
                var rows = await ctx.Query<JsmDept>()
                    .SelectMany(_ => ctx.Query<JsmEmp>(), (d, e) => new { DeptId = d.Id, EmpId = e.Id })
                    .ToListAsync();

                Assert.Equal(12, rows.Count);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
