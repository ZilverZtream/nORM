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
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
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

// One-to-many relationship: exercises Include (which reflects over navigation metadata and uses
// MakeGenericMethod to build the related query — both AOT hazards) under trimming.
[Table("Blog")]
[GenerateMaterializer]
public sealed class Blog
{
    [Key] public int Id { get; set; }
    public string Title { get; set; } = "";
    public List<Post> Posts { get; set; } = new();
}

[Table("Post")]
[GenerateMaterializer]
public sealed class Post
{
    [Key] public int Id { get; set; }
    public int BlogId { get; set; }
    public string Body { get; set; } = "";
}

// Value-converter entity. A configured converter deliberately skips the source-generated materializer
// (it reads raw provider values), so the runtime query path falls back to the REFLECTION materializer.
// That path constructs the entity and sets properties reflectively — exercising a different slice of the
// preserved metadata than the generated scenarios do.
[Table("S9")]
[GenerateMaterializer]
public sealed class S9Row
{
    [Key] public int Id { get; set; }
    public bool Active { get; set; }
}

public sealed class BoolYnConverter : ValueConverter<bool, string>
{
    public override object? ConvertToProvider(bool value) => value ? "Y" : "N";
    public override object? ConvertFromProvider(string value) => value == "Y";
}

// Owned value object embedded as columns (Home_City, Home_Zip). The owned getters/setters traverse
// owner -> owned reflectively, and the owned type is constructed (new Address()) during materialization,
// so this exercises the generator's per-owned-type [DynamicDependency] (properties + parameterless ctor).
[Owned]
public sealed class Address
{
    public string City { get; set; } = "";
    public string Zip { get; set; } = "";
}

[Table("Customer")]
[GenerateMaterializer]
public sealed class Customer
{
    [Key] public int Id { get; set; }
    public string Name { get; set; } = "";
    public Address Home { get; set; } = new();
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
                "CREATE TABLE S6 (Code INTEGER PRIMARY KEY, full_name TEXT NOT NULL);" +
                "CREATE TABLE Blog (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);" +
                "INSERT INTO Blog VALUES (1,'first');" +
                "CREATE TABLE Post (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Body TEXT NOT NULL);" +
                "INSERT INTO Post VALUES (1,1,'p1'),(2,1,'p2');" +
                "CREATE TABLE S9 (Id INTEGER PRIMARY KEY, Active TEXT NOT NULL);" +
                "INSERT INTO S9 VALUES (1,'Y'),(2,'N');" +
                "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Home_City TEXT NOT NULL, Home_Zip TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<S9Row>().Property(r => r.Active).HasConversion(new BoolYnConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

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

        // Read: RUNTIME LINQ query path (not [CompileTimeQuery]) — expression translation + the registered
        // generated materializer, over the reflection-built mapping. The DbContext ctor is annotated dynamic
        // for exactly this path; this proves it runs under trimming for a source-generated entity (where the
        // negative PublishTrimmed test's non-source-gen entity does not).
        try
        {
            var r = await ctx.Query<S1>().Where(x => x.Id >= 1).OrderByDescending(x => x.Id).ToListAsync();
            Check("S7_runtime_linq_query", r.Count >= 2 && r[0].Id >= r[1].Id, "count=" + r.Count);
        }
        catch (Exception e) { Check("S7_runtime_linq_query", false, e.GetType().Name + ": " + e.Message); }

        // Read: Include (one-to-many). Relationship inference + the eager-load query both reflect over
        // navigation/related-entity metadata and use MakeGenericMethod — the deepest AOT surface so far.
        try
        {
            var r = await ctx.Query<Blog>().Include(b => b.Posts).ToListAsync();
            Check("S8_include_one_to_many", r.Count == 1 && r[0].Posts.Count == 2,
                r.Count == 1 ? "posts=" + r[0].Posts.Count : "blogs=" + r.Count);
        }
        catch (Exception e) { Check("S8_include_one_to_many", false, e.GetType().Name + ": " + e.Message); }

        // Value converter (bool <-> 'Y'/'N'). The converter skips the generated materializer, forcing the
        // REFLECTION materializer + converter application on both read and write under trimming.
        try
        {
            var rows = await ctx.Query<S9Row>().OrderBy(x => x.Id).ToListAsync();
            bool readOk = rows.Count == 2 && rows[0].Active && !rows[1].Active;   // 'Y'->true, 'N'->false
            await ctx.InsertAsync(new S9Row { Id = 3, Active = true });           // true->'Y' on write
            var stored = new SqliteCommand("SELECT Active FROM S9 WHERE Id = 3", cn).ExecuteScalar() as string;
            var back = await ctx.Query<S9Row>().Where(x => x.Id == 3).ToListAsync();
            Check("S9_value_converter", readOk && stored == "Y" && back.Count == 1 && back[0].Active,
                $"read={readOk} stored={stored} back={(back.Count == 1 ? back[0].Active.ToString() : "n=" + back.Count)}");
        }
        catch (Exception e) { Check("S9_value_converter", false, e.GetType().Name + ": " + e.Message); }

        // Owned type (value object embedded as columns). Write flattens Home -> Home_City/Home_Zip via
        // owned getters; read reconstructs Home = new Address() and sets its properties under trimming.
        try
        {
            await ctx.InsertAsync(new Customer { Id = 1, Name = "acme", Home = new Address { City = "NYC", Zip = "10001" } });
            var r = await ctx.Query<Customer>().Where(x => x.Id == 1).ToListAsync();
            bool ok = r.Count == 1 && r[0].Home != null && r[0].Home.City == "NYC" && r[0].Home.Zip == "10001";
            Check("S10_owned_type", ok, r.Count == 1 ? $"City={r[0].Home?.City} Zip={r[0].Home?.Zip}" : "n=" + r.Count);
        }
        catch (Exception e) { Check("S10_owned_type", false, e.GetType().Name + ": " + e.Message); }

        Console.WriteLine(_fail == 0 ? "ALL SCENARIOS PASSED under NativeAOT" : $"{_fail} SCENARIO(S) FAILED under NativeAOT");
        return _fail;
    }
}
