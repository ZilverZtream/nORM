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
/// A projection combining a constructor argument with member-init bindings — <c>new Dto(x.A) { B = x.B }</c>
/// — must populate BOTH the constructor parameter and the initialized members. The projection only emitted
/// the binding columns and materialized via the parameterless constructor, so the constructor argument was
/// silently dropped (A defaulted to 0).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MemberInitWithCtorArgProjectionTests
{
    [Table("MiwcRow_Test")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public int X { get; set; }
        public int Y { get; set; }
        public string Name { get; set; } = "";
    }

    public sealed class Dto
    {
        public Dto() { }
        public Dto(int a) { A = a; }
        public int A { get; set; }
        public int B { get; set; }
    }

    public sealed class TwoArgDto
    {
        public TwoArgDto() { }
        public TwoArgDto(int a, string label) { A = a; Label = label; }
        public int A { get; set; }
        public string Label { get; set; } = "";
        public int C { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE MiwcRow_Test (Id INTEGER PRIMARY KEY, X INTEGER NOT NULL, Y INTEGER NOT NULL, Name TEXT NOT NULL);" +
                "INSERT INTO MiwcRow_Test VALUES (1, 11, 22, 'hi');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void MemberInit_with_constructor_argument_populates_both()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var dto = ctx.Query<Row>().Select(r => new Dto(r.X) { B = r.Y }).First();

        Assert.Equal(11, dto.A); // constructor argument
        Assert.Equal(22, dto.B); // member-init binding
    }

    [Fact]
    public void MemberInit_with_multiple_constructor_arguments_populates_all()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var dto = ctx.Query<Row>().Select(r => new TwoArgDto(r.X, r.Name) { C = r.Y }).First();

        Assert.Equal(11, dto.A);      // ctor arg 0
        Assert.Equal("hi", dto.Label); // ctor arg 1
        Assert.Equal(22, dto.C);       // binding
    }
}
