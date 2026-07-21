using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the set-op-over-scalar-projection with an IDENTITY order key
/// (`.Select(r => r.Scalar).Union(...).OrderBy(x => x)`), which previously threw
/// NormUnsupportedFeatureException ("Member '...' is not supported in this context")
/// because the set-op OrderBy handler only matched a member key (`x => x.Member`).
/// Identity ordering only compiles for a comparable scalar, so the compound query has
/// one result column; nORM now orders by its ordinal. Verified against LINQ-to-objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class SetOpScalarIdentityOrderTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("SoiRow")]
    private sealed class SoiRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool Flag { get; set; }
    }

    private static readonly SoiRow[] Rows = Enumerable.Range(1, 40).Select(i => new SoiRow
    {
        Id = i,
        IntVal = ((i * 37) % 61) - 30, // spread negatives + positives, duplicates across arms
        Name = "N" + (i % 7),
        Flag = i % 3 == 0,
    }).ToArray();

    private static DbContext NewContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SoiRow (Id INTEGER PRIMARY KEY, IntVal INTEGER NOT NULL, Name TEXT NOT NULL, Flag INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        foreach (var r in Rows)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "INSERT INTO SoiRow VALUES ($id,$v,$n,$f);";
            cmd.Parameters.AddWithValue("$id", r.Id);
            cmd.Parameters.AddWithValue("$v", r.IntVal);
            cmd.Parameters.AddWithValue("$n", r.Name);
            cmd.Parameters.AddWithValue("$f", r.Flag ? 1 : 0);
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Union_scalar_identity_orderby_desc_matches_linq()
    {
        var expected = Rows.Where(r => r.IntVal >= 0).Select(r => r.IntVal)
            .Union(Rows.Where(r => r.Flag).Select(r => r.IntVal))
            .OrderByDescending(x => x).ToList();
        using var ctx = NewContext();
        var actual = ctx.Query<SoiRow>().Where(r => r.IntVal >= 0).Select(r => r.IntVal)
            .Union(ctx.Query<SoiRow>().Where(r => r.Flag).Select(r => r.IntVal))
            .OrderByDescending(x => x).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Union_scalar_identity_orderby_asc_matches_linq()
    {
        var expected = Rows.Where(r => r.IntVal >= 0).Select(r => r.IntVal)
            .Union(Rows.Where(r => r.Flag).Select(r => r.IntVal))
            .OrderBy(x => x).ToList();
        using var ctx = NewContext();
        var actual = ctx.Query<SoiRow>().Where(r => r.IntVal >= 0).Select(r => r.IntVal)
            .Union(ctx.Query<SoiRow>().Where(r => r.Flag).Select(r => r.IntVal))
            .OrderBy(x => x).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Concat_scalar_identity_orderby_desc_matches_linq()
    {
        // Concat preserves duplicates; ordering the multiset by identity must match.
        var expected = Rows.Where(r => r.IntVal > 0).Select(r => r.IntVal)
            .Concat(Rows.Where(r => r.IntVal < 0).Select(r => r.IntVal))
            .OrderByDescending(x => x).ToList();
        using var ctx = NewContext();
        var actual = ctx.Query<SoiRow>().Where(r => r.IntVal > 0).Select(r => r.IntVal)
            .Concat(ctx.Query<SoiRow>().Where(r => r.IntVal < 0).Select(r => r.IntVal))
            .OrderByDescending(x => x).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Union_string_scalar_identity_orderby_matches_linq()
    {
        // String scalar identity ordering — SQLite BINARY collation coincides with LINQ ordinal
        // for these ASCII "N<digit>" values, so this is a valid discriminating probe here.
        var expected = Rows.Where(r => r.Flag).Select(r => r.Name)
            .Union(Rows.Where(r => r.IntVal > 10).Select(r => r.Name))
            .OrderBy(x => x).ToList();
        using var ctx = NewContext();
        var actual = ctx.Query<SoiRow>().Where(r => r.Flag).Select(r => r.Name)
            .Union(ctx.Query<SoiRow>().Where(r => r.IntVal > 10).Select(r => r.Name))
            .OrderBy(x => x).ToList();
        Assert.Equal(expected, actual);
    }
}
