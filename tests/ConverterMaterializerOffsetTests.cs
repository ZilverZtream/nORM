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
/// The converter-aware entity/DTO materializer resolved each column BY NAME across the whole reader row and
/// silently skipped NULLs. In a GroupJoin the outer and inner entities share a single result set with
/// duplicated column names (Id/Name/...), so GetOrdinal("Name") returned the INNER's column and the OUTER
/// entity was materialized from the inner's values — silent corruption whenever the outer had a value
/// converter. And a NULL read into a non-nullable value member silently defaulted to 0 instead of failing
/// loud. Both must match the ordinal-based, fail-loud behavior of every other materializer path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ConverterMaterializerOffsetTests
{
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object ConvertToProvider(int value) => value + 1000;   // model -> stored (+1000)
        public override object ConvertFromProvider(int value) => value - 1000; // stored -> model (-1000)
    }

    [Table("CmoDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }   // value-converted
    }

    [Table("CmoEmp")]
    public class Emp
    {
        [Key] public int Id { get; set; }
        public int DeptId { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CmoDept (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL);" +
                "CREATE TABLE CmoEmp (Id INTEGER PRIMARY KEY, DeptId INTEGER NOT NULL, Name TEXT NOT NULL, Score INTEGER NOT NULL);" +
                "INSERT INTO CmoDept VALUES (1, 'DeptOne', 1500);" +    // model Score = 500
                "INSERT INTO CmoEmp VALUES (77, 1, 'Alice', 1042);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Dept>().Property(d => d.Score).HasConversion(new OffsetConverter())
        });
    }

    [Fact]
    public void GroupJoin_outer_with_converter_binds_the_outer_columns_not_the_inner()
    {
        var ctx = Ctx(out var cn);
        using var _cn = cn; using var _ctx = ctx;

        var row = ctx.Query<Dept>()
            .GroupJoin(ctx.Query<Emp>(), d => d.Id, e => e.DeptId, (d, emps) => new { Dept = d, Emps = emps.ToList() })
            .First();

        // The outer Dept must be its own row, not the inner Emp's like-named columns.
        Assert.Equal("DeptOne", row.Dept.Name);
        Assert.Equal(1, row.Dept.Id);
        Assert.Equal(500, row.Dept.Score);   // 1500 stored - 1000 converter
        Assert.Equal("Alice", row.Emps.Single().Name);
    }

    [Table("CmoNull")]
    private class NullRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }   // value-converted -> forces the converter-aware path
        public int Age { get; set; }     // non-nullable; DB column is nullable and holds NULL
    }

    [Fact]
    public void Converter_path_null_into_non_nullable_member_fails_loud()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CmoNull (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Age INTEGER NULL);" +
                "INSERT INTO CmoNull VALUES (1, 1000, NULL);";
            cmd.ExecuteNonQuery();
        }
        using var _cn = cn;
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<NullRow>().Property(n => n.Score).HasConversion(new OffsetConverter())
        });

        // Age is NULL but the member is non-nullable int — must throw, not silently return 0.
        Assert.ThrowsAny<Exception>(() => ctx.Query<NullRow>().ToList());
    }
}
