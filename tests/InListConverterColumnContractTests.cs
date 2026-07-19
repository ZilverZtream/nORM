using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that <c>list.Contains(convCol)</c> → <c>col IN (...)</c> applies the column's value converter to
/// each list element. The IN-list translator bound the RAW model values, but the column stores PROVIDER
/// values, so the IN silently matched nothing — the worst kind of silent-wrong. Mirrors the scalar <c>==</c>
/// path, which already converts. Covers a type-preserving offset converter and a type-changing enum→string
/// converter (where raw binding is a hard type mismatch), in both inline-array and closure-variable forms.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class InListConverterColumnContractTests
{
    [Table("InWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }

    // Model N stored as N + 1000.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    public enum Status { Open, Pending, Closed }

    [Table("InOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public Status Status { get; set; }
    }

    private sealed class StatusConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<Status>(value);
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE InWidget (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);
                INSERT INTO InWidget VALUES (1,1005),(2,1007),(3,1009);
                CREATE TABLE InOrder (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL);
                INSERT INTO InOrder VALUES (1,'Open'),(2,'Pending'),(3,'Closed');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Widget>().HasKey(w => w.Id);
            mb.Entity<Widget>().Property<int>(w => w.Score).HasConversion(new OffsetConverter());
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Order>().Property<Status>(o => o.Status).HasConversion(new StatusConverter());
        }};
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void Inline_array_in_list_on_offset_converter_matches_stored_values()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // Model values 5 and 9 are stored as 1005 and 1009 → must match Ids 1 and 3.
        var ids = ctx.Query<Widget>().Where(w => new[] { 5, 9 }.Contains(w.Score)).ToList().Select(w => w.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1, 3 }, ids);
    }

    [Fact]
    public void Closure_list_in_list_on_offset_converter_matches_stored_values()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        var wanted = new List<int> { 5, 7 };
        var ids = ctx.Query<Widget>().Where(w => wanted.Contains(w.Score)).ToList().Select(w => w.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1, 2 }, ids);
    }

    [Fact]
    public void In_list_on_converter_column_with_no_model_match_returns_empty()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // Model 6 is stored as 1006, which no row has. Raw-binding the bug would also return empty here,
        // so this guards against a fix that over-matches (e.g. binding 6 against 1005/1007/1009).
        var ids = ctx.Query<Widget>().Where(w => new[] { 6 }.Contains(w.Score)).ToList();
        Assert.Empty(ids);
    }

    [Fact]
    public void Inline_array_in_list_on_enum_string_converter_matches()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // Raw binding would compare the enum's int (0, 2) against a TEXT column → zero rows.
        var ids = ctx.Query<Order>().Where(o => new[] { Status.Open, Status.Closed }.Contains(o.Status)).ToList().Select(o => o.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1, 3 }, ids);
    }

    [Fact]
    public void Closure_in_list_on_enum_string_converter_matches()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        var wanted = new[] { Status.Pending };
        var ids = ctx.Query<Order>().Where(o => wanted.Contains(o.Status)).ToList().Select(o => o.Id).ToArray();
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public void Scalar_subquery_contains_converts_the_tested_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // subquery projects the converter column → yields PROVIDER values ("Open"...); the tested value
        // must be converted too, or `0 IN ('Open',...)` is always false.
        Assert.True(ctx.Query<Order>().Select(o => o.Status).Contains(Status.Open));
        Assert.False(ctx.Query<Order>().Where(o => o.Id > 100).Select(o => o.Status).Contains(Status.Open));
    }
}
