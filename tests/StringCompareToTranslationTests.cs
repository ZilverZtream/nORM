using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Relational string comparisons via CompareTo / string.Compare / string.CompareOrdinal.
/// The comparison-to-zero shapes translate to SQL relational operators; ordinal overloads
/// must compare ordinally on every provider (aligning with the ordinal-equality campaign),
/// and the culture overloads follow the server's ordering the same way OrderBy does.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringCompareToTranslationTests
{
    [Table("CmpTo_Item")]
    private class Item
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CmpTo_Item (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL
                );
                INSERT INTO CmpTo_Item (Name) VALUES ('apple'),('melon'),('zebra'),('Melon');
                """;
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void CompareTo_greater_than_zero_filters_relationally()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Item>().Where(x => x.Name.CompareTo("melon") > 0).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 3 }, ids);
    }

    [Fact]
    public void CompareTo_less_than_zero_filters_relationally()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Item>().Where(x => x.Name.CompareTo("melon") < 0).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 4 }, ids);
    }

    [Fact]
    public void CompareTo_equals_zero_is_equality()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Item>().Where(x => x.Name.CompareTo("melon") == 0).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public void Static_compare_ordinal_comparison_filters_ordinally()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // Ordinal: 'M' (77) < 'm' (109), so "Melon" < "melon"; >= 0 keeps melon, zebra.
        var ids = ctx.Query<Item>().Where(x => string.CompareOrdinal(x.Name, "melon") >= 0).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Fact]
    public void Static_compare_with_ordinal_mode_filters_ordinally()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Item>().Where(x => string.Compare(x.Name, "melon", StringComparison.Ordinal) >= 0).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Fact]
    public void CompareTo_reversed_operand_order_flips_operator()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Item>().Where(x => 0 < x.Name.CompareTo("melon")).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 3 }, ids);
    }
}
