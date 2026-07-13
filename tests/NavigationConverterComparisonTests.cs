using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A comparison against a navigation member whose PRINCIPAL column has a value
/// converter must bind the provider representation (enum-to-name stores 'Active',
/// not 1) — binding the model value silently matched nothing. A missing parent is a
/// null value under C# semantics: equality drops the row, inequality keeps it.
/// </summary>

[Trait("Category", TestCategory.Fast)]
public class NavigationConverterComparisonTests
{
    private enum DeptStatus { Active = 1, Archived = 2 }

    private sealed class StatusToNameConverter : ValueConverter<DeptStatus, string>
    {
        public override object? ConvertToProvider(DeptStatus model) => model.ToString();
        public override object? ConvertFromProvider(string provider) => Enum.Parse<DeptStatus>(provider);
    }

    private class Dept
    {
        [Key] public int Id { get; set; }
        public DeptStatus Status { get; set; }
    }

    private class Emp
    {
        [Key] public int Id { get; set; }
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    [Fact]
    public void Nav_member_with_converter_binds_provider_value()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE Dept (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL);
                CREATE TABLE Emp (Id INTEGER PRIMARY KEY, DeptId INTEGER NULL);
                INSERT INTO Dept VALUES (1, 'Active'), (2, 'Archived');
                INSERT INTO Emp VALUES (1, 1), (2, 2), (3, NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Dept>().Property<DeptStatus>(d => d.Status).HasConversion(new StatusToNameConverter());
                mb.Entity<Emp>().HasKey(e => e.Id);
            }
        });

        var ids = ctx.Query<Emp>().Where(e => e.Dept!.Status == DeptStatus.Active).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1 }, ids);

        // Inequality keeps the missing-parent row: null != Active is TRUE in C#.
        var notActive = ctx.Query<Emp>().Where(e => e.Dept!.Status != DeptStatus.Active).Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2, 3 }, notActive);

        // Closure-captured value converts at bind time (plan-cache-safe).
        var wanted = DeptStatus.Archived;
        var archived = ctx.Query<Emp>().Where(e => e.Dept!.Status == wanted).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 2 }, archived);
    }
}
