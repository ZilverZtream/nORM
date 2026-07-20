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
/// EF Core parity: <c>Property(x => x.Col).HasDefaultValue(value)</c> emits a provider-correct SQL
/// literal DEFAULT in the generated schema (EnsureCreated / migrations). Verified end-to-end through the
/// SQLite migration generator by reading each column's declared default expression, and that the verb is
/// mutually exclusive with <see cref="EntityTypeBuilder.PropertyBuilder.HasDefaultValueSql(string)"/>
/// (last configured wins), matching EF Core.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PropertyBuilderHasDefaultValueTests
{
    [Table("HdvWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public int Priority { get; set; }
        public bool IsActive { get; set; }
        public string Label { get; set; } = "";
        public int Plain { get; set; }
    }

    [Table("HdvExcl")]
    public class ExclusivityWidget
    {
        [Key] public int Id { get; set; }
        public int Count { get; set; }
    }

    private static Dictionary<string, string?> DeclaredDefaults(SqliteConnection cn, string table)
    {
        var map = new Dictionary<string, string?>();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info({table})";
        using var r = cmd.ExecuteReader();
        while (r.Read())
            map[r.GetString(1)] = r.IsDBNull(4) ? null : r.GetString(4);   // 1 = name, 4 = dflt_value
        return map;
    }

    [Fact]
    public void HasDefaultValue_emits_provider_correct_literal_defaults()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property<int>(w => w.Priority).HasDefaultValue(5);
                mb.Entity<Widget>().Property<bool>(w => w.IsActive).HasDefaultValue(true);
                mb.Entity<Widget>().Property<string>(w => w.Label).HasDefaultValue("draft");
                // Plain is left with no configured default as a contrast.
            }
        }, ownsConnection: false);

        ctx.Database.EnsureCreated();

        var defaults = DeclaredDefaults(cn, "HdvWidget");
        Assert.Equal("5", defaults["Priority"]);        // numeric literal, verbatim
        Assert.Equal("1", defaults["IsActive"]);        // SQLite bool literal (0/1)
        Assert.Equal("'draft'", defaults["Label"]);     // quoted string literal
        Assert.Null(defaults["Plain"]);                 // untouched column has no DEFAULT
    }

    [Fact]
    public void HasDefaultValue_after_HasDefaultValueSql_wins_last()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<ExclusivityWidget>().HasKey(w => w.Id);
                // SQL default configured first, then a literal default overrides it (EF last-wins).
                mb.Entity<ExclusivityWidget>().Property<int>(w => w.Count)
                    .HasDefaultValueSql("7")
                    .HasDefaultValue(42);
            }
        }, ownsConnection: false);

        ctx.Database.EnsureCreated();

        var defaults = DeclaredDefaults(cn, "HdvExcl");
        Assert.Equal("42", defaults["Count"]);          // literal default replaced the earlier SQL default
    }
}
