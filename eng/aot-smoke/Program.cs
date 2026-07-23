// NativeAOT smoke matrix for nORM's source-generated path.
//
// Every entity opts into a source-generated materializer ([GenerateMaterializer]) and every read goes
// through a [CompileTimeQuery] method, so the read path uses generated delegates (no runtime IL emit).
// The write path (Insert/Update) still reflects over property metadata, which the trimmer would remove
// without the <TrimmerRootAssembly> in AotSmoke.csproj — see that file for the AOT-consumer recipe.
//
// Prints PASS/FAIL per scenario and returns the failing count as the process exit code, so eng/aot-smoke.ps1
// can assert exit 0. Covers reads (simple, parameterized, rich types) and both write models (direct + tracked).

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.SourceGeneration;

[Table("S1")]
[GenerateMaterializer]
public sealed class S1 { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

[Table("S3")]
[GenerateMaterializer]
public sealed class S3
{
    [Key] public int Id { get; set; }
    public string Name { get; set; } = "";
    public long Big { get; set; }
    public double Amount { get; set; }
    public bool Flag { get; set; }
    public DateTime Moment { get; set; }
    public Guid Gid { get; set; }
}

public static partial class Q
{
    [CompileTimeQuery("SELECT Id, Name FROM S1")]
    public static partial Task<List<S1>> AllS1(DbContext ctx);

    [CompileTimeQuery("SELECT Id, Name FROM S1 WHERE Id = @id")]
    public static partial Task<List<S1>> S1ById(DbContext ctx, int id);

    [CompileTimeQuery("SELECT Id, Name, Big, Amount, Flag, Moment, Gid FROM S3")]
    public static partial Task<List<S3>> AllS3(DbContext ctx);
}

public static class Program
{
    private static int _fail = 0;
    private static void Check(string name, bool ok, string detail = "")
    {
        Console.WriteLine((ok ? "PASS " : "FAIL ") + name + (detail.Length > 0 ? " :: " + detail : ""));
        if (!ok) _fail++;
    }

    public static async Task<int> Main()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var g = Guid.Parse("11112222-3333-4444-5555-666677778888");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE S1 (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "INSERT INTO S1 VALUES (1,'ann'),(2,'bob');" +
                "CREATE TABLE S3 (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Big INTEGER NOT NULL, Amount REAL NOT NULL, Flag INTEGER NOT NULL, Moment TEXT NOT NULL, Gid TEXT NOT NULL);" +
                "INSERT INTO S3 VALUES (1,'x',9007199254740993,3.5,1,'2023-06-15 12:30:45','" + g + "');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Read: simple source-gen materializer + compile-time query.
        try { var r = await Q.AllS1(ctx); Check("S1_simple_select", r.Count == 2 && r[0].Name == "ann"); }
        catch (Exception e) { Check("S1_simple_select", false, e.GetType().Name + ": " + e.Message); }

        // Read: parameterized compile-time query.
        try { var r = await Q.S1ById(ctx, 2); Check("S2_parameterized_query", r.Count == 1 && r[0].Name == "bob"); }
        catch (Exception e) { Check("S2_parameterized_query", false, e.GetType().Name + ": " + e.Message); }

        // Read: rich-type materialization (long, double, bool, DateTime, Guid) under AOT.
        try
        {
            var r = await Q.AllS3(ctx);
            var row = r[0];
            bool ok = r.Count == 1 && row.Big == 9007199254740993L && Math.Abs(row.Amount - 3.5) < 1e-9
                      && row.Flag && row.Moment == new DateTime(2023, 6, 15, 12, 30, 45) && row.Gid == g;
            Check("S3_rich_types", ok, $"Big={row.Big} Amount={row.Amount} Flag={row.Flag} Moment={row.Moment:o} Gid={row.Gid}");
        }
        catch (Exception e) { Check("S3_rich_types", false, e.GetType().Name + ": " + e.Message); }

        // Write: direct path (expression-compiled getters + parameter binding under AOT).
        try
        {
            await ctx.InsertAsync(new S1 { Id = 3, Name = "cid" });
            var r = await Q.AllS1(ctx);
            Check("S4_direct_insert", r.Count == 3, "count=" + r.Count);
        }
        catch (Exception e) { Check("S4_direct_insert", false, e.GetType().Name + ": " + e.Message); }

        // Write: tracked path (Update + SaveChanges — change detection + snapshots).
        try
        {
            var u = new S1 { Id = 2, Name = "bob-updated" };
            ctx.Update(u);
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
            var r = await Q.S1ById(ctx, 2);
            Check("S5_tracked_save", r.Count == 1 && r[0].Name == "bob-updated", r.Count == 1 ? r[0].Name : "count=" + r.Count);
        }
        catch (Exception e) { Check("S5_tracked_save", false, e.GetType().Name + ": " + e.Message); }

        Console.WriteLine(_fail == 0 ? "ALL SCENARIOS PASSED under NativeAOT" : $"{_fail} SCENARIO(S) FAILED under NativeAOT");
        return _fail;
    }
}
