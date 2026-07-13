using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Single-reference navigation traversal (dependent to principal) translates to a
/// correlated scalar subquery: predicates filter through the parent's column,
/// projections yield the parent's value, and a missing optional parent produces SQL
/// NULL, which flows through nORM's C# null semantics: equality drops the row,
/// null-safe inequality keeps it (as if written e.Dept?.Title), and projections
/// yield null. Chains nest one subquery per hop.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ReferenceNavigationScalarTests
{
    private class Region
    {
        [Key] public int Id { get; set; }
        public string Zone { get; set; } = "";
    }

    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public int? RegionId { get; set; }
        [ForeignKey(nameof(RegionId))] public Region? Region { get; set; }
    }

    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public int? BackupDeptId { get; set; }
        [ForeignKey(nameof(BackupDeptId))] public Dept? BackupDept { get; set; }
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE Region (Id INTEGER PRIMARY KEY, Zone TEXT NOT NULL);
                CREATE TABLE Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, RegionId INTEGER NULL);
                CREATE TABLE Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL, BackupDeptId INTEGER NULL);
                INSERT INTO Region VALUES (1, 'EU');
                INSERT INTO Dept VALUES (1, 'Eng', 1), (2, 'Ops', NULL);
                INSERT INTO Emp VALUES (1, 'ann', 1, 1), (2, 'bob', NULL, NULL), (3, 'cid', 2, 1);
                """;
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Region>().HasKey(r => r.Id);
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
            }
        });
        return (cn, ctx);
    }

    [Fact]
    public void Predicate_on_nav_member_filters_through_parent()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng").Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void Inequality_on_nav_member_keeps_rows_with_missing_parent()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Emp>().Where(e => e.Dept!.Title != "Eng").Select(e => e.Id).OrderBy(i => i).ToList();
        // A missing parent behaves as a null VALUE (like e.Dept?.Title), matching
        // nORM's C# null semantics everywhere else: null != "Eng" is TRUE, so bob
        // stays — consistent with the projection yielding null for him.
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Fact]
    public void Projection_of_nav_member_yields_parent_value_or_null()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var titles = ctx.Query<Emp>().OrderBy(e => e.Id).Select(e => e.Dept!.Title).ToList();
        Assert.Equal(new string?[] { "Eng", null, "Ops" }, titles);
    }

    [Fact]
    public void Anonymous_projection_with_nav_member()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var rows = ctx.Query<Emp>().OrderBy(e => e.Id)
            .Select(e => new { e.Name, DeptTitle = e.Dept!.Title }).ToList();
        Assert.Equal(new[] { "ann:Eng", "bob:", "cid:Ops" }, rows.Select(r => $"{r.Name}:{r.DeptTitle}").ToArray());
    }

    [Fact]
    public void Two_hop_nav_chain_nests_subqueries()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var zones = ctx.Query<Emp>().OrderBy(e => e.Id).Select(e => e.Dept!.Region!.Zone).ToList();
        // ann -> Eng -> EU; bob has no dept; cid -> Ops has no region.
        Assert.Equal(new string?[] { "EU", null, null }, zones);
    }

    [Fact]
    public void Two_hop_nav_predicate_filters()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Emp>().Where(e => e.Dept!.Region!.Zone == "EU").Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void OrderBy_nav_member_sorts_through_parent()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Emp>().Where(e => e.DeptId != null)
            .OrderBy(e => e.Dept!.Title).Select(e => e.Id).ToList();
        // Eng (ann) before Ops (cid).
        Assert.Equal(new[] { 1, 3 }, ids);
    }

    [Fact]
    public void Nav_equals_null_finds_orphans()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // A whole-entity null test asks whether the parent is missing — the FK is NULL.
        var ids = ctx.Query<Emp>().Where(e => e.Dept == null).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public void Nav_not_equals_null_finds_parented()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Emp>().Where(e => e.Dept != null).Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 3 }, ids);
    }

    [Fact]
    public void Two_hop_nav_equals_null_tests_nested_fk()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // cid's dept (Ops) has no region; bob has no dept at all (nested FK subquery
        // yields NULL either way).
        var ids = ctx.Query<Emp>().Where(e => e.Dept!.Region == null).Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Fact]
    public void Nav_equals_entity_instance_compares_fk_to_pk()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var eng = new Dept { Id = 1, Title = "Eng" };
        var ids = ctx.Query<Emp>().Where(e => e.Dept == eng).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void Nav_equals_entity_instance_rebinds_key_across_cached_plans()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // The captured instance binds through a converter that extracts the PK at
        // execution time, so a cached plan serves a different instance correctly.
        var dept = new Dept { Id = 1, Title = "Eng" };
        Assert.Equal(new[] { 1 }, ctx.Query<Emp>().Where(e => e.Dept == dept).Select(e => e.Id).ToList());
        dept = new Dept { Id = 2, Title = "Ops" };
        Assert.Equal(new[] { 3 }, ctx.Query<Emp>().Where(e => e.Dept == dept).Select(e => e.Id).ToList());
    }

    [Fact]
    public void Nav_not_equals_entity_instance_keeps_orphans()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var eng = new Dept { Id = 1, Title = "Eng" };
        var ids = ctx.Query<Emp>().Where(e => e.Dept != eng).Select(e => e.Id).OrderBy(i => i).ToList();
        // bob's missing parent is a null value: null != eng is TRUE (C# null semantics).
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Fact]
    public void Bool_projection_of_nav_null_test()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var rows = ctx.Query<Emp>().OrderBy(e => e.Id)
            .Select(e => new { e.Id, IsOrphan = e.Dept == null }).ToList();
        Assert.Equal(new[] { false, true, false }, rows.Select(r => r.IsOrphan).ToArray());
    }

    [Fact]
    public void Bool_projection_of_column_null_test()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // Previously emitted `col = NULL` — always UNKNOWN — and silently
        // materialized false for every row.
        var rows = ctx.Query<Emp>().OrderBy(e => e.Id)
            .Select(e => new { e.Id, NoDept = e.DeptId == null }).ToList();
        Assert.Equal(new[] { false, true, false }, rows.Select(r => r.NoDept).ToArray());
    }

    [Fact]
    public void Conditional_over_null_test_keeps_predicate_position()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // Inside a CASE WHEN test the null check must stay a bare predicate.
        var labels = ctx.Query<Emp>().OrderBy(e => e.Id)
            .Select(e => e.DeptId == null ? "orphan" : "parented").ToList();
        Assert.Equal(new[] { "parented", "orphan", "parented" }, labels);
    }

    [Fact]
    public void Nav_vs_nav_comparison_compares_fk_values()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // ann: dept 1 == backup 1; bob: both null (C# null == null is true);
        // cid: dept 2 != backup 1.
        var same = ctx.Query<Emp>().Where(e => e.Dept == e.BackupDept).Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 2 }, same);
        var diff = ctx.Query<Emp>().Where(e => e.Dept != e.BackupDept).Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 3 }, diff);
    }

    [Fact]
    public void Two_navs_to_same_principal_resolve_their_own_fk()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // cid: Dept=Ops (2), BackupDept=Eng (1) — a type-name FK match would read
        // BOTH navigations through DeptId and return Ops twice.
        var rows = ctx.Query<Emp>().Where(e => e.Id == 3)
            .Select(e => new { Main = e.Dept!.Title, Backup = e.BackupDept!.Title }).ToList();
        Assert.Equal("Ops", rows[0].Main);
        Assert.Equal("Eng", rows[0].Backup);
    }

    [Fact]
    public void Nav_member_in_string_method_translates()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Emp>().Where(e => e.Dept!.Title.StartsWith("En")).Select(e => e.Id).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }
}
