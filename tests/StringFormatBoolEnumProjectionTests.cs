using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A bool or enum rendered via string.Format / string interpolation in a projection must match .NET's
/// ToString: "True"/"False" for a bool, the member name for an enum — the same text
/// <c>x.Flag.ToString()</c> already produces. The string.Format path cast the raw stored value to text
/// instead, silently yielding "1"/"0" for a bool and the integer for an enum.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringFormatBoolEnumProjectionTests
{
    private enum Status { Draft = 0, Published = 5 }

    [Table("SfbeRow_Test")]
    private sealed class Row
    {
        [Key] public int Id { get; set; }
        public bool Active { get; set; }
        public Status Status { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE SfbeRow_Test (Id INTEGER PRIMARY KEY, Active INTEGER NOT NULL, Status INTEGER NOT NULL);" +
                "INSERT INTO SfbeRow_Test VALUES (1, 1, 5);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void Interpolated_bool_projects_dotnet_text()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var s = ctx.Query<Row>().Select(r => $"A={r.Active}").First();
        Assert.Equal("A=True", s);
    }

    [Fact]
    public void Interpolated_enum_projects_member_name()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var s = ctx.Query<Row>().Select(r => $"S={r.Status}").First();
        Assert.Equal("S=Published", s);
    }

    [Fact]
    public void String_format_bool_projects_dotnet_text()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var s = ctx.Query<Row>().Select(r => string.Format("{0}", r.Active)).First();
        Assert.Equal("True", s);
    }

    [Fact]
    public void Where_side_convert_tostring_bool_matches_dotnet_text()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // WHERE-side Convert.ToString(bool) must render "True", or this predicate matches nothing.
        var count = ctx.Query<Row>().Count(r => Convert.ToString(r.Active) == "True");
        Assert.Equal(1, count);
    }

    [Fact]
    public void Where_side_string_format_bool_matches_dotnet_text()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // The WHERE-side string.Format path must render the bool the same way, or this predicate matches nothing.
        var count = ctx.Query<Row>().Count(r => string.Format("[{0}]", r.Active) == "[True]");
        Assert.Equal(1, count);
    }
}
