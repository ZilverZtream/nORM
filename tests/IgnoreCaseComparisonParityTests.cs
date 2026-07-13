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
/// StringComparison.OrdinalIgnoreCase overloads of Equals / Contains / StartsWith /
/// EndsWith must match case variants on case-SENSITIVE providers (the mirror of the
/// ordinal campaign, which forced case-sensitivity onto CI collations). Pinned after a
/// probe run showed the family fully handled.
/// </summary>

[Trait("Category", TestCategory.Fast)]
public class IgnoreCaseComparisonParityTests
{
    [Table("IgnoreCase_Row")]
    private class Row
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
                CREATE TABLE IgnoreCase_Row (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL
                );
                INSERT INTO IgnoreCase_Row (Name) VALUES ('abc'),('ABC'),('AbC'),('xyz');
                """;
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Equals_ordinal_ignore_case_matches_all_variants()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Row>().Where(r => r.Name.Equals("aBc", StringComparison.OrdinalIgnoreCase))
            .Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }

    [Fact]
    public void Contains_ordinal_ignore_case_matches_all_variants()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Row>().Where(r => r.Name.Contains("BC", StringComparison.OrdinalIgnoreCase))
            .Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }

    [Fact]
    public void StartsWith_ordinal_ignore_case_matches_all_variants()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Row>().Where(r => r.Name.StartsWith("AB", StringComparison.OrdinalIgnoreCase))
            .Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }

    [Fact]
    public void EndsWith_ordinal_ignore_case_matches_all_variants()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Row>().Where(r => r.Name.EndsWith("bc", StringComparison.OrdinalIgnoreCase))
            .Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }
}
