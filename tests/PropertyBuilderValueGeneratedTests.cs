using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// EF Core parity: <c>Property(x => x.Col).ValueGeneratedOnAdd()</c> marks the column store-generated so
/// nORM omits it from INSERT and the database default supplies the value. The control (no verb) inserts the
/// CLR value, proving the omission is what changes the outcome. <c>ValueGeneratedNever()</c> clears an
/// attribute-derived database-generated flag so the CLR value is written.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PropertyBuilderValueGeneratedTests
{
    [Table("VgWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public int Counter { get; set; }
    }

    [Table("VgComputed")]
    public class Computed
    {
        [Key] public int Id { get; set; }
        [DatabaseGenerated(DatabaseGeneratedOption.Computed)] public int Amount { get; set; }
    }

    private static SqliteConnection NewDb(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static long RawScalar(SqliteConnection cn, string sql)
    {
        using var c = cn.CreateCommand(); c.CommandText = sql;
        return (long)c.ExecuteScalar()!;
    }

    [Fact]
    public async Task ValueGeneratedOnAdd_omits_the_column_from_insert_so_the_db_default_applies()
    {
        using var cn = NewDb("CREATE TABLE VgWidget (Id INTEGER PRIMARY KEY, Counter INTEGER NOT NULL DEFAULT 42);");
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property(w => w.Counter).ValueGeneratedOnAdd();
            }
        }, ownsConnection: false);

        ctx.Add(new Widget { Id = 1, Counter = 7 });   // CLR value 7 is ignored — column is store-generated
        await ctx.SaveChangesAsync();

        Assert.Equal(42, RawScalar(cn, "SELECT Counter FROM VgWidget WHERE Id = 1"));   // DB default won
    }

    [Fact]
    public async Task Without_value_generated_the_clr_value_is_inserted()
    {
        using var cn = NewDb("CREATE TABLE VgWidget (Id INTEGER PRIMARY KEY, Counter INTEGER NOT NULL DEFAULT 42);");
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
        }, ownsConnection: false);

        ctx.Add(new Widget { Id = 1, Counter = 7 });
        await ctx.SaveChangesAsync();

        Assert.Equal(7, RawScalar(cn, "SELECT Counter FROM VgWidget WHERE Id = 1"));   // control: CLR value inserted
    }

    [Fact]
    public async Task ValueGeneratedNever_overrides_the_attribute_and_writes_the_clr_value()
    {
        using var cn = NewDb("CREATE TABLE VgComputed (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL DEFAULT 99);");
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Computed>().HasKey(c => c.Id);
                // The [DatabaseGenerated(Computed)] attribute would omit Amount; the fluent verb overrides it.
                mb.Entity<Computed>().Property(c => c.Amount).ValueGeneratedNever();
            }
        }, ownsConnection: false);

        ctx.Add(new Computed { Id = 1, Amount = 5 });
        await ctx.SaveChangesAsync();

        Assert.Equal(5, RawScalar(cn, "SELECT Amount FROM VgComputed WHERE Id = 1"));   // CLR value written, not the default
    }
}
