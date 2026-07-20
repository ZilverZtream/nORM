using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// EF Core parity: <c>Property(x => x.Col).HasColumnType("...")</c> emits the explicit provider store type in
/// the generated schema (EnsureCreated / migrations), overriding the CLR-derived type. Verified end-to-end
/// through the SQLite migration generator by reading the declared column types.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PropertyBuilderHasColumnTypeTests
{
    [Table("HctWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public decimal Amount { get; set; }
        public decimal Plain { get; set; }
    }

    private static Dictionary<string, string> DeclaredTypes(SqliteConnection cn, string table)
    {
        var map = new Dictionary<string, string>();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info({table})";
        using var r = cmd.ExecuteReader();
        while (r.Read())
            map[r.GetString(1)] = r.GetString(2);   // column 1 = name, column 2 = declared type
        return map;
    }

    [Fact]
    public void HasColumnType_emits_the_explicit_store_type_in_the_generated_schema()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property<decimal>(w => w.Amount).HasColumnType("DECIMAL(18,2)");
                // Plain is left to the CLR-derived type as a contrast.
            }
        }, ownsConnection: false);

        ctx.Database.EnsureCreated();

        var types = DeclaredTypes(cn, "HctWidget");
        Assert.Equal("DECIMAL(18,2)", types["Amount"]);       // explicit type emitted verbatim
        Assert.NotEqual("DECIMAL(18,2)", types["Plain"]);     // untouched column keeps the CLR-derived type
    }
}
