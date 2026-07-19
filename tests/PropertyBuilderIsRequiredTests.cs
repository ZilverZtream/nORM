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
/// EF Core parity: <c>Property(x => x.Col).IsRequired()</c> makes the generated column NOT NULL, overriding
/// the CLR/attribute-derived nullability. Verified end-to-end through EnsureCreated by reading the SQLite
/// schema — a required column is NOT NULL, a sibling nullable column left alone stays nullable.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PropertyBuilderIsRequiredTests
{
    [Table("ReqWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string? Name { get; set; }   // nullable by default
        public string? Note { get; set; }   // nullable, left alone
    }

    private static Dictionary<string, bool> NotNullByColumn(SqliteConnection cn, string table)
    {
        var map = new Dictionary<string, bool>();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info({table})";
        using var r = cmd.ExecuteReader();
        while (r.Read())
            map[r.GetString(1)] = r.GetInt32(3) == 1;   // column 1 = name, column 3 = "notnull"
        return map;
    }

    [Fact]
    public void IsRequired_makes_the_column_not_null_in_the_generated_schema()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property(w => w.Name).IsRequired();
            }
        }, ownsConnection: false);

        ctx.Database.EnsureCreated();

        var notnull = NotNullByColumn(cn, "ReqWidget");
        Assert.True(notnull["Name"]);    // IsRequired() → NOT NULL
        Assert.False(notnull["Note"]);   // untouched nullable column stays nullable
    }
}
