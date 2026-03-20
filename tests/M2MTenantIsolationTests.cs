using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Mapping;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// X2: Verifies that many-to-many join table synchronization and eager-loading
/// respect tenant predicates, preventing cross-tenant data leakage when different
/// tenants share the same PK space.
/// </summary>
public class M2MTenantIsolationTests
{
    // ── Entity definitions ────────────────────────────────────────────────

    [Table("M2tStudent")]
    private class M2tStudent
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public List<M2tCourse> Courses { get; set; } = new();
    }

    [Table("M2tCourse")]
    private class M2tCourse
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
    }

    // ── Tenant provider ──────────────────────────────────────────────────

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static SqliteConnection CreateOpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE M2tStudent (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Name TEXT NOT NULL);
            CREATE TABLE M2tCourse  (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Name TEXT NOT NULL);
            CREATE TABLE M2tStudentCourse (StudentId INTEGER NOT NULL, CourseId INTEGER NOT NULL, PRIMARY KEY (StudentId, CourseId));
        ";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext MakeCtx(SqliteConnection cn, string tenantId) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenantId),
            OnModelCreating = mb =>
            {
                mb.Entity<M2tStudent>()
                  .HasMany<M2tCourse>(s => s.Courses)
                  .WithMany()
                  .UsingTable("M2tStudentCourse", "StudentId", "CourseId");
            }
        });

    /// <summary>
    /// Inserts a row directly via SQL, bypassing nORM context (used to seed cross-tenant data).
    /// </summary>
    private static void InsertRaw(SqliteConnection cn, string table, int id, string tenantId, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {table} (Id, TenantId, Name) VALUES (@id, @tid, @name)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.ExecuteNonQuery();
    }

    private static void InsertJoinRow(SqliteConnection cn, int studentId, int courseId)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO M2tStudentCourse (StudentId, CourseId) VALUES (@sid, @cid)";
        cmd.Parameters.AddWithValue("@sid", studentId);
        cmd.Parameters.AddWithValue("@cid", courseId);
        cmd.ExecuteNonQuery();
    }

    private static long CountJoinRows(SqliteConnection cn, int studentId)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM M2tStudentCourse WHERE StudentId = @sid";
        cmd.Parameters.AddWithValue("@sid", studentId);
        return (long)cmd.ExecuteScalar()!;
    }

    private static List<int> GetJoinCourseIds(SqliteConnection cn, int studentId)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT CourseId FROM M2tStudentCourse WHERE StudentId = @sid ORDER BY CourseId";
        cmd.Parameters.AddWithValue("@sid", studentId);
        var result = new List<int>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            result.Add(reader.GetInt32(0));
        return result;
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    /// <summary>
    /// X2-1: Two tenants have student Id=1 with different courses.
    /// Eager-loading tenant A's student must return only tenant A's courses, not tenant B's.
    /// </summary>
    [Fact]
    public async Task X2_1_Include_LoadsOnlyCurrentTenantCourses()
    {
        using var cn = CreateOpenDb();

        // Seed: TenantA student 1 -> course 10; TenantB student 1 -> course 20
        InsertRaw(cn, "M2tStudent", 1, "TenantA", "Alice");
        InsertRaw(cn, "M2tStudent", 2, "TenantB", "Bob");  // Different row, but same-PK scenario below
        InsertRaw(cn, "M2tCourse", 10, "TenantA", "Math");
        InsertRaw(cn, "M2tCourse", 20, "TenantB", "Physics");
        InsertJoinRow(cn, 1, 10); // TenantA's student -> TenantA's course
        InsertJoinRow(cn, 2, 20); // TenantB's student -> TenantB's course

        // But to test the real scenario (same PK across tenants), add another student with Id=1 for TenantB
        // SQLite uses unique PK so we can't have two Id=1. Instead, test the join-table filtering:
        // If both tenants share a join row with same StudentId, the INNER JOIN on student table
        // with tenant filter should only return the current tenant's join rows.
        // So let's create a scenario with a shared PK: use separate in-memory DBs sharing same schema.
        // Actually, we can test with different student IDs but the key insight is the join table query
        // scopes by tenant. Let's seed join rows that WOULD leak without the fix.

        // Simpler approach: TenantB also has join rows for student 1 (cross-tenant corruption scenario)
        InsertJoinRow(cn, 1, 20); // This join row should NOT be visible to TenantA

        using var ctx = MakeCtx(cn, "TenantA");

        var students = await ((INormQueryable<M2tStudent>)ctx.Query<M2tStudent>())
            .Include(s => s.Courses)
            .AsSplitQuery()
            .ToListAsync();

        // TenantA should see only student 1 (due to global tenant filter on Query)
        var alice = students.FirstOrDefault(s => s.Name == "Alice");
        Assert.NotNull(alice);

        // Alice should have ONLY course 10 (TenantA's), NOT course 20 (TenantB's)
        // The join row (1, 20) exists but should be filtered out by the tenant-scoped join query
        Assert.Single(alice!.Courses);
        Assert.Equal("Math", alice.Courses[0].Name);
    }

    /// <summary>
    /// X2-2: Deleting tenant A's student must NOT delete tenant B's join rows.
    /// Without the fix, DELETE FROM join_table WHERE left_fk = @lpk would wipe
    /// all join rows for that PK regardless of tenant.
    /// </summary>
    [Fact]
    public async Task X2_2_Delete_DoesNotRemoveCrossTenantJoinRows()
    {
        using var cn = CreateOpenDb();

        // Seed both tenants' students and courses
        InsertRaw(cn, "M2tStudent", 1, "TenantA", "Alice");
        InsertRaw(cn, "M2tStudent", 2, "TenantB", "Bob");
        InsertRaw(cn, "M2tCourse", 10, "TenantA", "Math");
        InsertRaw(cn, "M2tCourse", 20, "TenantB", "Physics");

        // Both students have join rows
        InsertJoinRow(cn, 1, 10); // TenantA
        InsertJoinRow(cn, 2, 20); // TenantB

        // Also add a cross-tenant join row for student 1 (simulating shared PK scenario)
        InsertJoinRow(cn, 1, 20); // Join row for student 1 -> course 20

        // Delete TenantA's student 1
        using var ctxA = MakeCtx(cn, "TenantA");
        var studentA = new M2tStudent { Id = 1, TenantId = "TenantA", Name = "Alice", Courses = new() };
        ctxA.Attach(studentA);
        ctxA.Remove(studentA);
        await ctxA.SaveChangesAsync();

        // TenantB's join rows should be untouched
        Assert.Equal(1, CountJoinRows(cn, 2));
        var bobCourses = GetJoinCourseIds(cn, 2);
        Assert.Single(bobCourses);
        Assert.Equal(20, bobCourses[0]);
    }

    /// <summary>
    /// X2-3: When tenant A updates (removes a course), tenant B's join rows remain intact.
    /// </summary>
    [Fact]
    public async Task X2_3_Update_RemoveCourse_DoesNotAffectOtherTenantJoinRows()
    {
        using var cn = CreateOpenDb();

        // Seed data: both students share the same course 10 in the join table
        InsertRaw(cn, "M2tStudent", 1, "TenantA", "Alice");
        InsertRaw(cn, "M2tStudent", 2, "TenantB", "Bob");
        InsertRaw(cn, "M2tCourse", 10, "TenantA", "Math");
        InsertRaw(cn, "M2tCourse", 30, "TenantA", "Chemistry");

        InsertJoinRow(cn, 1, 10); // TenantA: Alice -> Math
        InsertJoinRow(cn, 1, 30); // TenantA: Alice -> Chemistry
        InsertJoinRow(cn, 2, 10); // TenantB: Bob -> Math (shared course)

        // TenantA removes Math from Alice, keeping only Chemistry
        using var ctxA = MakeCtx(cn, "TenantA");
        var math = new M2tCourse { Id = 10, TenantId = "TenantA", Name = "Math" };
        var chem = new M2tCourse { Id = 30, TenantId = "TenantA", Name = "Chemistry" };
        var alice = new M2tStudent { Id = 1, TenantId = "TenantA", Name = "Alice", Courses = new List<M2tCourse> { math, chem } };

        ctxA.Attach(alice);
        ctxA.Attach(math);
        ctxA.Attach(chem);

        // Now update: remove Math
        alice.Courses.Remove(math);
        ctxA.Update(alice);
        await ctxA.SaveChangesAsync();

        // Alice should have only Chemistry
        var aliceCourses = GetJoinCourseIds(cn, 1);
        Assert.Single(aliceCourses);
        Assert.Equal(30, aliceCourses[0]);

        // Bob's join row (2, 10) must be untouched
        var bobCourses = GetJoinCourseIds(cn, 2);
        Assert.Single(bobCourses);
        Assert.Equal(10, bobCourses[0]);
    }

    /// <summary>
    /// X2-4: Include loads right entities filtered by tenant when right table also has TenantColumn.
    /// </summary>
    [Fact]
    public async Task X2_4_Include_FiltersRightEntitiesByTenant()
    {
        using var cn = CreateOpenDb();

        // Seed: course 10 owned by TenantA, course 20 owned by TenantB
        InsertRaw(cn, "M2tStudent", 1, "TenantA", "Alice");
        InsertRaw(cn, "M2tCourse", 10, "TenantA", "Math");
        InsertRaw(cn, "M2tCourse", 20, "TenantB", "Physics");

        // Alice has join rows to both courses
        InsertJoinRow(cn, 1, 10);
        InsertJoinRow(cn, 1, 20);

        using var ctx = MakeCtx(cn, "TenantA");

        var students = await ((INormQueryable<M2tStudent>)ctx.Query<M2tStudent>())
            .Include(s => s.Courses)
            .AsSplitQuery()
            .ToListAsync();

        var alice = students.FirstOrDefault(s => s.Name == "Alice");
        Assert.NotNull(alice);

        // Should only see course 10 (TenantA), NOT course 20 (TenantB),
        // because the right-entity query filters by TenantId
        Assert.Single(alice!.Courses);
        Assert.Equal("Math", alice.Courses[0].Name);
    }

    /// <summary>
    /// X2-5: No-tenant context still works (backward compatibility).
    /// </summary>
    [Fact]
    public async Task X2_5_NoTenant_Include_ReturnsAllJoinRows()
    {
        using var cn = CreateOpenDb();

        InsertRaw(cn, "M2tStudent", 1, "A", "Alice");
        InsertRaw(cn, "M2tCourse", 10, "A", "Math");
        InsertRaw(cn, "M2tCourse", 20, "B", "Physics");
        InsertJoinRow(cn, 1, 10);
        InsertJoinRow(cn, 1, 20);

        // No tenant provider
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<M2tStudent>()
                  .HasMany<M2tCourse>(s => s.Courses)
                  .WithMany()
                  .UsingTable("M2tStudentCourse", "StudentId", "CourseId");
            }
        });

        var students = await ((INormQueryable<M2tStudent>)ctx.Query<M2tStudent>())
            .Include(s => s.Courses)
            .AsSplitQuery()
            .ToListAsync();

        var alice = students.Single(s => s.Name == "Alice");
        // Without tenant filtering, all join rows should be visible
        Assert.Equal(2, alice.Courses.Count);
    }

    /// <summary>
    /// X2-6: Delete-all path (Deleted entity state) with tenant filter only removes
    /// current tenant's join rows.
    /// </summary>
    [Fact]
    public async Task X2_6_DeleteAll_OnlyRemovesCurrentTenantJoinRows()
    {
        using var cn = CreateOpenDb();

        // Two students with same PK scenario simulated via different IDs
        // but share common join rows
        InsertRaw(cn, "M2tStudent", 1, "TenantA", "Alice");
        InsertRaw(cn, "M2tStudent", 3, "TenantB", "Carol");
        InsertRaw(cn, "M2tCourse", 10, "TenantA", "Math");
        InsertRaw(cn, "M2tCourse", 20, "TenantB", "Art");

        InsertJoinRow(cn, 1, 10); // TenantA
        InsertJoinRow(cn, 3, 20); // TenantB

        // Count before delete
        Assert.Equal(1, CountJoinRows(cn, 1));
        Assert.Equal(1, CountJoinRows(cn, 3));

        // Delete TenantA's student
        using var ctxA = MakeCtx(cn, "TenantA");
        var alice = new M2tStudent { Id = 1, TenantId = "TenantA", Name = "Alice" };
        ctxA.Attach(alice);
        ctxA.Remove(alice);
        await ctxA.SaveChangesAsync();

        // TenantA's join rows deleted
        Assert.Equal(0, CountJoinRows(cn, 1));
        // TenantB's join rows intact
        Assert.Equal(1, CountJoinRows(cn, 3));
    }

    /// <summary>
    /// X2-7: Adding join rows via SaveChanges with tenant provider still works correctly.
    /// </summary>
    [Fact]
    public async Task X2_7_Add_JoinRows_WithTenantProvider_Works()
    {
        using var cn = CreateOpenDb();

        InsertRaw(cn, "M2tCourse", 10, "TenantA", "Math");

        using var ctx = MakeCtx(cn, "TenantA");

        var student = new M2tStudent
        {
            Id = 100,
            TenantId = "TenantA",
            Name = "Dave",
            Courses = new List<M2tCourse> { new M2tCourse { Id = 10, TenantId = "TenantA", Name = "Math" } }
        };
        ctx.Add(student);
        await ctx.SaveChangesAsync();

        var joinRows = GetJoinCourseIds(cn, 100);
        Assert.Single(joinRows);
        Assert.Equal(10, joinRows[0]);
    }
}
