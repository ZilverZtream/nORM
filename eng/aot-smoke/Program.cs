// NativeAOT smoke matrix for nORM's source-generated path.
//
// Every entity opts into a source-generated materializer ([GenerateMaterializer]) and every read goes
// through a [CompileTimeQuery] method, so the read path uses generated delegates (no runtime IL emit).
// The write path reflects over property/attribute metadata to build its mapping; the generator preserves
// exactly that metadata via an emitted [DynamicDependency], so it survives trimming with no consumer
// rooting (see AotSmoke.csproj for the zero-ceremony contract).
//
// Prints PASS/FAIL per scenario and returns the failing count as the process exit code, so eng/aot-smoke.ps1
// can assert exit 0. Covers reads (simple, parameterized, rich types), both write models (direct + tracked),
// and attribute-based mapping (non-Id [Key] + [Column] rename).

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

// Attribute-dependent entity: a non-"Id" [Key] and a [Column] rename. Under trimming the write path
// reflects over these attributes to build the mapping (key for the UPDATE WHERE, renamed column name),
// so this proves the generator preserves attribute metadata — not just properties by name convention.
[Table("S6")]
[GenerateMaterializer]
public sealed class S6
{
    [Key] public int Code { get; set; }
    [Column("full_name")] public string FullName { get; set; } = "";
}

public static partial class Q
{
    [CompileTimeQuery("SELECT Id, Name FROM S1")]
    public static partial Task<List<S1>> AllS1(DbContext ctx);

    [CompileTimeQuery("SELECT Id, Name FROM S1 WHERE Id = @id")]
    public static partial Task<List<S1>> S1ById(DbContext ctx, int id);

    [CompileTimeQuery("SELECT Id, Name, Big, Amount, Flag, Moment, Gid FROM S3")]
    public static partial Task<List<S3>> AllS3(DbContext ctx);

    [CompileTimeQuery("SELECT Code, full_name FROM S6 WHERE Code = @code")]
    public static partial Task<List<S6>> S6ByCode(DbContext ctx, int code);
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
                "INSERT INTO S3 VALUES (1,'x',9007199254740993,3.5,1,'2023-06-15 12:30:45','" + g + "');" +
                "CREATE TABLE S6 (Code INTEGER PRIMARY KEY, full_name TEXT NOT NULL);";
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

        // Write: attribute-dependent mapping (non-Id [Key] + [Column] rename). Proves the write mapping's
        // reflection over [Key]/[Column] survives trimming — insert uses the renamed column, update keys off Code.
        try
        {
            await ctx.InsertAsync(new S6 { Code = 5, FullName = "alpha" });
            var u = new S6 { Code = 5, FullName = "beta" };
            ctx.Update(u);
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
            var r = await Q.S6ByCode(ctx, 5);
            Check("S6_attribute_mapping", r.Count == 1 && r[0].FullName == "beta", r.Count == 1 ? r[0].FullName : "count=" + r.Count);
        }
        catch (Exception e) { Check("S6_attribute_mapping", false, e.GetType().Name + ": " + e.Message); }

        Console.WriteLine(_fail == 0 ? "ALL SCENARIOS PASSED under NativeAOT" : $"{_fail} SCENARIO(S) FAILED under NativeAOT");
        return _fail;
    }
}
