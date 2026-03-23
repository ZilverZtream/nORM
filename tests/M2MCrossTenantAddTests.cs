using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// SEC1: ExecuteJoinTableSyncAsync must validate right-entity tenant before inserting join rows.
// Previously, the add path only had the right PK, with no tenant check; a cross-tenant right
// entity could be attached to a tenant-A owner and the join row would be silently persisted.

public class M2MCrossTenantAddTests
{
    [Table("Sec1Student")]
    private class Sec1Student
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Name { get; set; } = "";
        public List<Sec1Course> Courses { get; set; } = new();
    }

    [Table("Sec1Course")]
    private class Sec1Course
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Name { get; set; } = "";
    }

    private sealed class FixedTenant : ITenantProvider
    {
        public FixedTenant(string id) => Id = id;
        public string Id { get; }
        public object GetCurrentTenantId() => Id;
    }

    private const string Ddl =
        "CREATE TABLE Sec1Student (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Name TEXT NOT NULL);" +
        "CREATE TABLE Sec1Course  (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Name TEXT NOT NULL);" +
        "CREATE TABLE Sec1StudentCourse (StudentId INTEGER NOT NULL, CourseId INTEGER NOT NULL, PRIMARY KEY (StudentId, CourseId));";

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = Ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext MakeCtx(SqliteConnection cn, string tenantId) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenant(tenantId),
            TenantColumnName = "TenantId",
            OnModelCreating = mb => mb.Entity<Sec1Student>()
                .HasMany<Sec1Course>(s => s.Courses)
                .WithMany()
                .UsingTable("Sec1StudentCourse", "StudentId", "CourseId")
        });

    private static void SeedCourse(SqliteConnection cn, int id, string tenantId, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO Sec1Course (Id, TenantId, Name) VALUES (@id, @tid, @name)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.ExecuteNonQuery();
    }

    private static long CountJoinRows(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Sec1StudentCourse";
        return (long)cmd.ExecuteScalar()!;
    }

    // ── SEC1-1: Same-tenant add succeeds ─────────────────────────────────

    [Fact]
    public async Task SEC1_SameTenantAdd_SavesJoinRowSuccessfully()
    {
        using var cn = OpenDb();
        SeedCourse(cn, 10, "T1", "Math");

        await using var ctx = MakeCtx(cn, "T1");

        var student = new Sec1Student
        {
            Id = 1, TenantId = "T1", Name = "Alice",
            Courses = new List<Sec1Course> { new() { Id = 10, TenantId = "T1", Name = "Math" } }
        };
        ctx.Add(student);
        await ctx.SaveChangesAsync();

        Assert.Equal(1L, CountJoinRows(cn));
    }

    // ── SEC1-2: Cross-tenant add throws ───────────────────────────────────

    [Fact]
    public async Task SEC1_CrossTenantAdd_ThrowsInvalidOperationException()
    {
        using var cn = OpenDb();
        SeedCourse(cn, 20, "T2", "Physics"); // belongs to T2

        await using var ctx = MakeCtx(cn, "T1");

        var student = new Sec1Student
        {
            Id = 2, TenantId = "T1", Name = "Bob",
            Courses = new List<Sec1Course> { new() { Id = 20, TenantId = "T2", Name = "Physics" } }
        };
        ctx.Add(student);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.SaveChangesAsync());
        Assert.Contains("T2", ex.Message);
        Assert.Contains("T1", ex.Message);
        // No join rows must have been inserted
        Assert.Equal(0L, CountJoinRows(cn));
    }

    // ── SEC1-3: Null right-entity tenant throws ───────────────────────────

    [Fact]
    public async Task SEC1_NullRightEntityTenant_ThrowsInvalidOperationException()
    {
        using var cn = OpenDb();
        // Course seeded with valid tenant — but the in-memory object has null
        SeedCourse(cn, 30, "T1", "Art");

        await using var ctx = MakeCtx(cn, "T1");

        var badCourse = new Sec1Course { Id = 30, TenantId = null!, Name = "Art" };
        var student = new Sec1Student
        {
            Id = 3, TenantId = "T1", Name = "Carol",
            Courses = new List<Sec1Course> { badCourse }
        };
        ctx.Add(student);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.SaveChangesAsync());
        Assert.Contains("null", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0L, CountJoinRows(cn));
    }

    // ── SEC1-4: No tenant config → no validation (backward compat) ────────

    [Fact]
    public async Task SEC1_NoTenantConfig_CrossTenantAddDoesNotThrow()
    {
        using var cn = OpenDb();
        SeedCourse(cn, 40, "T2", "History");

        // Context without TenantProvider — no validation should occur
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Sec1Student>()
                .HasMany<Sec1Course>(s => s.Courses)
                .WithMany()
                .UsingTable("Sec1StudentCourse", "StudentId", "CourseId")
        });

        var student = new Sec1Student
        {
            Id = 4, TenantId = "T1", Name = "Dan",
            Courses = new List<Sec1Course> { new() { Id = 40, TenantId = "T2", Name = "History" } }
        };
        ctx.Add(student);
        await ctx.SaveChangesAsync(); // must not throw

        Assert.Equal(1L, CountJoinRows(cn));
    }

    // ── SEC1-5: Update path — attaching cross-tenant course throws ─────────

    [Fact]
    public async Task SEC1_Update_AddCrossTenantCourse_ThrowsInvalidOperationException()
    {
        using var cn = OpenDb();

        // Seed a valid student + course, insert join row
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText =
                "INSERT INTO Sec1Student (Id, TenantId, Name) VALUES (5, 'T1', 'Eve');" +
                "INSERT INTO Sec1Course  (Id, TenantId, Name) VALUES (50, 'T1', 'Music');" +
                "INSERT INTO Sec1Course  (Id, TenantId, Name) VALUES (60, 'T2', 'Dance');" +
                "INSERT INTO Sec1StudentCourse (StudentId, CourseId) VALUES (5, 50);";
            cmd.ExecuteNonQuery();
        }

        await using var ctx = MakeCtx(cn, "T1");

        // Attach the student with original course list
        var music = new Sec1Course { Id = 50, TenantId = "T1", Name = "Music" };
        var student = new Sec1Student
        {
            Id = 5, TenantId = "T1", Name = "Eve",
            Courses = new List<Sec1Course> { music }
        };
        ctx.Attach(student);
        ctx.Attach(music);

        // Now add a cross-tenant course to the collection
        var dance = new Sec1Course { Id = 60, TenantId = "T2", Name = "Dance" };
        student.Courses.Add(dance);
        ctx.Update(student);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.SaveChangesAsync());
        Assert.Contains("T2", ex.Message);
        Assert.Contains("T1", ex.Message);
    }
}
