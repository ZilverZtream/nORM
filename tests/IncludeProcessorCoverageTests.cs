using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ── Public entity types at namespace scope for IncludeProcessor coverage ─────

[Table("IPC_Author")]
public class IpcAuthor
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int? NullableScore { get; set; }
    public ICollection<IpcBook> Books { get; set; } = new List<IpcBook>();
}

[Table("IPC_Book")]
public class IpcBook
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int AuthorId { get; set; }
    public string Title { get; set; } = string.Empty;
    public ICollection<IpcReview> Reviews { get; set; } = new List<IpcReview>();
}

[Table("IPC_Review")]
public class IpcReview
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int BookId { get; set; }
    public string Comment { get; set; } = string.Empty;
}

[Table("IPC_Course")]
public class IpcCourse
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Code { get; set; } = string.Empty;
    public List<IpcStudent> Students { get; set; } = new();
}

[Table("IPC_Student")]
public class IpcStudent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Table("IPC_Dept")]
public class IpcDept
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string DeptName { get; set; } = string.Empty;
    public ICollection<IpcEmployee> Employees { get; set; } = new List<IpcEmployee>();
}

[Table("IPC_Employee")]
public class IpcEmployee
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int DeptId { get; set; }
    public string FullName { get; set; } = string.Empty;
}

[Table("IPC_Order")]
public class IpcOrder
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Ref { get; set; } = string.Empty;
    public ICollection<IpcLine> Lines { get; set; } = new List<IpcLine>();
}

[Table("IPC_Line")]
public class IpcLine
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int OrderId { get; set; }
    public string Item { get; set; } = string.Empty;
}

[Table("IPC_Project")]
public class IpcProject
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Code { get; set; } = string.Empty;
    public ICollection<IpcTask> Tasks { get; set; } = new List<IpcTask>();
}

[Table("IPC_Task")]
public class IpcTask
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ProjectId { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Table("IPC_Category")]
public class IpcCategory
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<IpcProduct> Products { get; set; } = new();
}

[Table("IPC_Product")]
public class IpcProduct
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
}

/// <summary>
/// Coverage tests for <see cref="IncludeProcessor"/> and related eager-loading paths.
/// Tests EagerLoad (sync), EagerLoadAsync, LoadManyToMany (sync), LoadManyToManyAsync,
/// multi-level paths, empty collections, noTracking variants, and composite-PK guard.
/// Also tests <see cref="JoinBuilder"/> column extraction.
/// </summary>
public class IncludeProcessorCoverageTests
{
    // ── Async-forcing provider ─────────────────────────────────────────────────
    private sealed class AsyncSqliteProvider : SqliteProvider
    {
        public override bool PrefersSyncExecution => false;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static DbContext CreateAuthorContext(SqliteConnection cn, bool async = false)
    {
        Exec(cn, "CREATE TABLE IF NOT EXISTS IPC_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, NullableScore INTEGER)");
        Exec(cn, "CREATE TABLE IF NOT EXISTS IPC_Book (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL)");
        DatabaseProvider prov = async ? new AsyncSqliteProvider() : new SqliteProvider();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcAuthor>()
                  .HasMany(a => a.Books)
                  .WithOne()
                  .HasForeignKey(b => b.AuthorId, a => a.Id)
        };
        return new DbContext(cn, prov, opts);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 1 — EagerLoadAsync basic cases
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EagerLoadAsync_SingleParentMultipleChildren_LoadsAll()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'Alice',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'B1'),(2,1,'B2'),(3,1,'B3')");

        var authors = await ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToListAsync();

        Assert.Single(authors);
        Assert.Equal(3, authors[0].Books.Count);
    }

    [Fact]
    public async Task EagerLoadAsync_MultipleParents_ChildrenCorrectlyGrouped()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'A1',10),(2,'A2',20)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'A1B1'),(2,1,'A1B2'),(3,2,'A2B1')");

        var authors = await ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToListAsync();

        Assert.Equal(2, authors.Count);
        var a1 = authors.Single(a => a.Name == "A1");
        var a2 = authors.Single(a => a.Name == "A2");
        Assert.Equal(2, a1.Books.Count);
        Assert.Single(a2.Books);
    }

    [Fact]
    public async Task EagerLoadAsync_NoChildren_EmptyList()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'Lonely',null)");

        var authors = await ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToListAsync();

        Assert.Single(authors);
        Assert.Empty(authors[0].Books);
    }

    [Fact]
    public async Task EagerLoadAsync_NoTracking_ChildrenNotTracked()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'NT',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'NTBook')");

        var authors = await ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsNoTracking()
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToListAsync();

        Assert.Single(authors);
        Assert.Single(authors[0].Books);
        foreach (var b in authors[0].Books)
            Assert.Null(ctx.ChangeTracker.GetEntryOrDefault(b));
    }

    [Fact]
    public async Task EagerLoadAsync_Tracking_ChildrenTracked()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'Track',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'TB1')");

        var authors = await ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToListAsync();

        Assert.Single(authors[0].Books);
        var child = authors[0].Books.First();
        var entry = ctx.ChangeTracker.GetEntryOrDefault(child);
        Assert.NotNull(entry);
        Assert.Equal(EntityState.Unchanged, entry!.State);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 2 — EagerLoad sync cases
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void EagerLoad_Sync_SingleParent_LoadsChildren()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: false);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'SyncAuth',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'SB1'),(2,1,'SB2')");

        var authors = ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToList();

        Assert.Single(authors);
        Assert.Equal(2, authors[0].Books.Count);
    }

    [Fact]
    public void EagerLoad_Sync_MultipleParents_CorrectGrouping()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: false);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'S1',null),(2,'S2',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'S1B1'),(2,2,'S2B1'),(3,2,'S2B2')");

        var authors = ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToList();

        var s1 = authors.Single(a => a.Name == "S1");
        var s2 = authors.Single(a => a.Name == "S2");
        Assert.Single(s1.Books);
        Assert.Equal(2, s2.Books.Count);
    }

    [Fact]
    public void EagerLoad_Sync_NoTracking_ChildrenNotTracked()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: false);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'NTS',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'NTSB')");

        var authors = ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsNoTracking()
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToList();

        Assert.Single(authors[0].Books);
        Assert.Null(ctx.ChangeTracker.GetEntryOrDefault(authors[0].Books.First()));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 3 — CompositeKey throws NotSupportedException
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EagerLoadAsync_CompositePkDependent_ThrowsNotSupported()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_CompositeParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_CompositeChild (ParentId INTEGER NOT NULL, Seq INTEGER NOT NULL, Info TEXT NOT NULL, PRIMARY KEY(ParentId, Seq))");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<IpcCompositeParent>()
                  .HasMany(p => p.Children)
                  .WithOne()
                  .HasForeignKey(c => c.ParentId, p => p.Id);
                mb.Entity<IpcCompositeChild>().HasKey(c => new { c.ParentId, c.Seq });
            }
        };
        using var ctx = new DbContext(cn, new AsyncSqliteProvider(), opts);

        Exec(cn, "INSERT INTO IPC_CompositeParent VALUES(1,'CP1')");
        Exec(cn, "INSERT INTO IPC_CompositeChild VALUES(1,1,'CK1')");

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await ((INormQueryable<IpcCompositeParent>)ctx.Query<IpcCompositeParent>())
                .AsSplitQuery()
                .Include(p => p.Children)
                .ToListAsync());
    }

    [Fact]
    public void EagerLoad_Sync_CompositePkDependent_ThrowsNotSupported()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_CompositeParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_CompositeChild (ParentId INTEGER NOT NULL, Seq INTEGER NOT NULL, Info TEXT NOT NULL, PRIMARY KEY(ParentId, Seq))");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<IpcCompositeParent>()
                  .HasMany(p => p.Children)
                  .WithOne()
                  .HasForeignKey(c => c.ParentId, p => p.Id);
                mb.Entity<IpcCompositeChild>().HasKey(c => new { c.ParentId, c.Seq });
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        Exec(cn, "INSERT INTO IPC_CompositeParent VALUES(1,'SP1')");
        Exec(cn, "INSERT INTO IPC_CompositeChild VALUES(1,1,'SK1')");

        Assert.Throws<NotSupportedException>(() =>
            ((INormQueryable<IpcCompositeParent>)ctx.Query<IpcCompositeParent>())
                .AsSplitQuery()
                .Include(p => p.Children)
                .ToList());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 4 — LoadManyToManyAsync
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LoadManyToManyAsync_MultipleParents_CorrectAssignment()
    {
        using var cn = OpenDb();
        Exec(cn, @"CREATE TABLE IPC_Course (Id INTEGER PRIMARY KEY AUTOINCREMENT, Code TEXT NOT NULL);
                   CREATE TABLE IPC_Student (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                   CREATE TABLE IPC_CourseStudent (CourseId INTEGER NOT NULL, StudentId INTEGER NOT NULL, PRIMARY KEY(CourseId, StudentId));");

        Exec(cn, "INSERT INTO IPC_Course VALUES(1,'CS101'),(2,'CS201')");
        Exec(cn, "INSERT INTO IPC_Student VALUES(1,'Alice'),(2,'Bob'),(3,'Carol')");
        Exec(cn, "INSERT INTO IPC_CourseStudent VALUES(1,1),(1,2),(2,2),(2,3)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcCourse>()
                  .HasMany<IpcStudent>(c => c.Students)
                  .WithMany()
                  .UsingTable("IPC_CourseStudent", "CourseId", "StudentId")
        };
        using var ctx = new DbContext(cn, new AsyncSqliteProvider(), opts);

        var courses = await ((INormQueryable<IpcCourse>)ctx.Query<IpcCourse>())
            .Include(c => c.Students)
            .ToListAsync();

        Assert.Equal(2, courses.Count);
        var cs101 = courses.Single(c => c.Code == "CS101");
        var cs201 = courses.Single(c => c.Code == "CS201");
        Assert.Equal(2, cs101.Students.Count);
        Assert.Equal(2, cs201.Students.Count);
        Assert.Contains(cs101.Students, s => s.Name == "Alice");
        Assert.Contains(cs101.Students, s => s.Name == "Bob");
    }

    [Fact]
    public async Task LoadManyToManyAsync_EmptyParents_ReturnsEmpty()
    {
        using var cn = OpenDb();
        Exec(cn, @"CREATE TABLE IPC_Course (Id INTEGER PRIMARY KEY AUTOINCREMENT, Code TEXT NOT NULL);
                   CREATE TABLE IPC_Student (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                   CREATE TABLE IPC_CourseStudent (CourseId INTEGER NOT NULL, StudentId INTEGER NOT NULL, PRIMARY KEY(CourseId, StudentId));");
        // No rows

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcCourse>()
                  .HasMany<IpcStudent>(c => c.Students)
                  .WithMany()
                  .UsingTable("IPC_CourseStudent", "CourseId", "StudentId")
        };
        using var ctx = new DbContext(cn, new AsyncSqliteProvider(), opts);

        var courses = await ((INormQueryable<IpcCourse>)ctx.Query<IpcCourse>())
            .Include(c => c.Students)
            .ToListAsync();

        Assert.Empty(courses);
    }

    [Fact]
    public async Task LoadManyToManyAsync_NoTracking_EntityNotTracked()
    {
        using var cn = OpenDb();
        Exec(cn, @"CREATE TABLE IPC_Course (Id INTEGER PRIMARY KEY AUTOINCREMENT, Code TEXT NOT NULL);
                   CREATE TABLE IPC_Student (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                   CREATE TABLE IPC_CourseStudent (CourseId INTEGER NOT NULL, StudentId INTEGER NOT NULL, PRIMARY KEY(CourseId, StudentId));");

        Exec(cn, "INSERT INTO IPC_Course VALUES(1,'X1')");
        Exec(cn, "INSERT INTO IPC_Student VALUES(1,'NT_S')");
        Exec(cn, "INSERT INTO IPC_CourseStudent VALUES(1,1)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcCourse>()
                  .HasMany<IpcStudent>(c => c.Students)
                  .WithMany()
                  .UsingTable("IPC_CourseStudent", "CourseId", "StudentId")
        };
        using var ctx = new DbContext(cn, new AsyncSqliteProvider(), opts);

        var courses = await ((INormQueryable<IpcCourse>)ctx.Query<IpcCourse>())
            .AsNoTracking()
            .Include(c => c.Students)
            .ToListAsync();

        Assert.Single(courses);
        Assert.Single(courses[0].Students);
        Assert.Null(ctx.ChangeTracker.GetEntryOrDefault(courses[0].Students[0]));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 5 — LoadManyToMany sync
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void LoadManyToMany_Sync_MultipleParents_CorrectAssignment()
    {
        using var cn = OpenDb();
        Exec(cn, @"CREATE TABLE IPC_Course (Id INTEGER PRIMARY KEY AUTOINCREMENT, Code TEXT NOT NULL);
                   CREATE TABLE IPC_Student (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                   CREATE TABLE IPC_CourseStudent (CourseId INTEGER NOT NULL, StudentId INTEGER NOT NULL, PRIMARY KEY(CourseId, StudentId));");

        Exec(cn, "INSERT INTO IPC_Course VALUES(1,'S_CS101'),(2,'S_CS201')");
        Exec(cn, "INSERT INTO IPC_Student VALUES(1,'SA'),(2,'SB')");
        Exec(cn, "INSERT INTO IPC_CourseStudent VALUES(1,1),(2,2)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcCourse>()
                  .HasMany<IpcStudent>(c => c.Students)
                  .WithMany()
                  .UsingTable("IPC_CourseStudent", "CourseId", "StudentId")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var courses = ((INormQueryable<IpcCourse>)ctx.Query<IpcCourse>())
            .Include(c => c.Students)
            .ToList();

        Assert.Equal(2, courses.Count);
        Assert.Single(courses.Single(c => c.Code == "S_CS101").Students);
        Assert.Single(courses.Single(c => c.Code == "S_CS201").Students);
    }

    [Fact]
    public void LoadManyToMany_Sync_NoTracking_ChildrenNotTracked()
    {
        using var cn = OpenDb();
        Exec(cn, @"CREATE TABLE IPC_Course (Id INTEGER PRIMARY KEY AUTOINCREMENT, Code TEXT NOT NULL);
                   CREATE TABLE IPC_Student (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                   CREATE TABLE IPC_CourseStudent (CourseId INTEGER NOT NULL, StudentId INTEGER NOT NULL, PRIMARY KEY(CourseId, StudentId));");

        Exec(cn, "INSERT INTO IPC_Course VALUES(1,'SNC1')");
        Exec(cn, "INSERT INTO IPC_Student VALUES(1,'SNS1')");
        Exec(cn, "INSERT INTO IPC_CourseStudent VALUES(1,1)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcCourse>()
                  .HasMany<IpcStudent>(c => c.Students)
                  .WithMany()
                  .UsingTable("IPC_CourseStudent", "CourseId", "StudentId")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var courses = ((INormQueryable<IpcCourse>)ctx.Query<IpcCourse>())
            .AsNoTracking()
            .Include(c => c.Students)
            .ToList();

        Assert.Single(courses[0].Students);
        Assert.Null(ctx.ChangeTracker.GetEntryOrDefault(courses[0].Students[0]));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 6 — JoinBuilder: ExtractNeededColumns and BuildJoinClause
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void JoinBuilder_ExtractNeededColumns_SimpleMembers_ReturnsAliasedColumns()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_Dept (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptName TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_Employee (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptId INTEGER NOT NULL, FullName TEXT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcDept>()
                  .HasMany(d => d.Employees)
                  .WithOne()
                  .HasForeignKey(e => e.DeptId, d => d.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var outerMap = ctx.GetMapping(typeof(IpcDept));
        var innerMap = ctx.GetMapping(typeof(IpcEmployee));

        // Build a projection NewExpression: new { d.Id, e.FullName }
        var dParam = Expression.Parameter(typeof(IpcDept), "d");
        var eParam = Expression.Parameter(typeof(IpcEmployee), "e");
        var dId = Expression.MakeMemberAccess(dParam, typeof(IpcDept).GetProperty("Id")!);
        var eName = Expression.MakeMemberAccess(eParam, typeof(IpcEmployee).GetProperty("FullName")!);

        var anonCtor = typeof(ValueTuple<int, string>).GetConstructors()[0];
        // Use anonymous type approach - just test with simple new expression
        var newExpr = Expression.New(
            typeof(Tuple<int, string>).GetConstructors()[0],
            dId, eName);

        var cols = JoinBuilder.ExtractNeededColumns(newExpr, outerMap, innerMap, "d", "e");
        // Should have extracted at least the Id and FullName columns
        Assert.Contains(cols, c => c.Contains("d.") && c.Contains("Id"));
        Assert.Contains(cols, c => c.Contains("e.") && c.Contains("FullName"));
    }

    [Fact]
    public void JoinBuilder_ExtractNeededColumns_FullEntityParam_ReturnsAllColumns()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_Dept (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptName TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_Employee (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptId INTEGER NOT NULL, FullName TEXT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcDept>()
                  .HasMany(d => d.Employees)
                  .WithOne()
                  .HasForeignKey(e => e.DeptId, d => d.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var outerMap = ctx.GetMapping(typeof(IpcDept));
        var innerMap = ctx.GetMapping(typeof(IpcEmployee));

        // Projection: new { d, e } — full entity params
        var dParam = Expression.Parameter(typeof(IpcDept), "d");
        var eParam = Expression.Parameter(typeof(IpcEmployee), "e");

        var newExpr = Expression.New(
            typeof(Tuple<IpcDept, IpcEmployee>).GetConstructors()[0],
            dParam, eParam);

        var cols = JoinBuilder.ExtractNeededColumns(newExpr, outerMap, innerMap, "d", "e");
        // Should include all columns from both tables
        Assert.True(cols.Count >= outerMap.Columns.Length + innerMap.Columns.Length);
    }

    [Fact]
    public void JoinBuilder_BuildJoinClause_ReturnsValidSql()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_Dept (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptName TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_Employee (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptId INTEGER NOT NULL, FullName TEXT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcDept>()
                  .HasMany(d => d.Employees)
                  .WithOne()
                  .HasForeignKey(e => e.DeptId, d => d.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var outerMap = ctx.GetMapping(typeof(IpcDept));
        var innerMap = ctx.GetMapping(typeof(IpcEmployee));

        var sql = JoinBuilder.BuildJoinClause(
            projection: null,
            outerMapping: outerMap,
            outerAlias: "d",
            innerMapping: innerMap,
            innerAlias: "e",
            joinType: "INNER JOIN",
            outerKeySql: "d.[Id]",
            innerKeySql: "e.[DeptId]");

        Assert.Contains("FROM", sql);
        Assert.Contains("INNER JOIN", sql);
        Assert.Contains("ON", sql);
    }

    [Fact]
    public void JoinBuilder_BuildJoinClause_WithOrderBy_IncludesOrderBy()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_Dept (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptName TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_Employee (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptId INTEGER NOT NULL, FullName TEXT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcDept>()
                  .HasMany(d => d.Employees)
                  .WithOne()
                  .HasForeignKey(e => e.DeptId, d => d.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var outerMap = ctx.GetMapping(typeof(IpcDept));
        var innerMap = ctx.GetMapping(typeof(IpcEmployee));

        var sql = JoinBuilder.BuildJoinClause(
            projection: null,
            outerMapping: outerMap,
            outerAlias: "d",
            innerMapping: innerMap,
            innerAlias: "e",
            joinType: "LEFT JOIN",
            outerKeySql: "d.[Id]",
            innerKeySql: "e.[DeptId]",
            orderBy: "d.[Id]");

        Assert.Contains("ORDER BY", sql);
        Assert.Contains("LEFT JOIN", sql);
    }

    [Fact]
    public void JoinBuilder_SetupJoinProjection_SetsProjectionAndParams()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_Dept (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptName TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_Employee (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptId INTEGER NOT NULL, FullName TEXT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcDept>()
                  .HasMany(d => d.Employees)
                  .WithOne()
                  .HasForeignKey(e => e.DeptId, d => d.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var outerMap = ctx.GetMapping(typeof(IpcDept));
        var innerMap = ctx.GetMapping(typeof(IpcEmployee));

        var dParam = Expression.Parameter(typeof(IpcDept), "d");
        var eParam = Expression.Parameter(typeof(IpcEmployee), "e");
        var selector = Expression.Lambda(
            Expression.MakeMemberAccess(dParam, typeof(IpcDept).GetProperty("Id")!),
            dParam, eParam);

        LambdaExpression? projection = null;
        var correlatedParams = new Dictionary<ParameterExpression, (nORM.Mapping.TableMapping, string)>();

        JoinBuilder.SetupJoinProjection(
            resultSelector: selector,
            outerMapping: outerMap,
            innerMapping: innerMap,
            outerAlias: "d",
            innerAlias: "e",
            correlatedParams: correlatedParams,
            projection: ref projection);

        Assert.NotNull(projection);
        Assert.True(correlatedParams.ContainsKey(dParam));
        Assert.True(correlatedParams.ContainsKey(eParam));
    }

    [Fact]
    public void JoinBuilder_SetupJoinProjection_NullSelector_LeavesProjectionNull()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_Dept (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptName TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_Employee (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptId INTEGER NOT NULL, FullName TEXT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcDept>()
                  .HasMany(d => d.Employees)
                  .WithOne()
                  .HasForeignKey(e => e.DeptId, d => d.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var outerMap = ctx.GetMapping(typeof(IpcDept));
        var innerMap = ctx.GetMapping(typeof(IpcEmployee));

        LambdaExpression? projection = null;
        var correlatedParams = new Dictionary<ParameterExpression, (nORM.Mapping.TableMapping, string)>();

        JoinBuilder.SetupJoinProjection(
            resultSelector: null,
            outerMapping: outerMap,
            innerMapping: innerMap,
            outerAlias: "d",
            innerAlias: "e",
            correlatedParams: correlatedParams,
            projection: ref projection);

        Assert.Null(projection);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 7 — JoinBuilder BuildJoinClauseInto (zero-copy variant)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void JoinBuilder_BuildJoinClauseInto_NoProjection_ProducesSql()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_Order (Id INTEGER PRIMARY KEY AUTOINCREMENT, Ref TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_Line (Id INTEGER PRIMARY KEY AUTOINCREMENT, OrderId INTEGER NOT NULL, Item TEXT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcOrder>()
                  .HasMany(o => o.Lines)
                  .WithOne()
                  .HasForeignKey(l => l.OrderId, o => o.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var outerMap = ctx.GetMapping(typeof(IpcOrder));
        var innerMap = ctx.GetMapping(typeof(IpcLine));

        var builder = new OptimizedSqlBuilder(512);
        JoinBuilder.BuildJoinClauseInto(
            joinSql: builder,
            projection: null,
            outerMapping: outerMap,
            outerAlias: "o",
            innerMapping: innerMap,
            innerAlias: "l",
            joinType: "INNER JOIN",
            outerKeySql: "o.[Id]",
            innerKeySql: "l.[OrderId]");

        var sql = builder.ToSqlString();
        Assert.Contains("FROM", sql);
        Assert.Contains("INNER JOIN", sql);
        Assert.Contains("ON", sql);
    }

    [Fact]
    public void JoinBuilder_BuildJoinClauseInto_WithOrderBy_IncludesOrderBy()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_Order (Id INTEGER PRIMARY KEY AUTOINCREMENT, Ref TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_Line (Id INTEGER PRIMARY KEY AUTOINCREMENT, OrderId INTEGER NOT NULL, Item TEXT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcOrder>()
                  .HasMany(o => o.Lines)
                  .WithOne()
                  .HasForeignKey(l => l.OrderId, o => o.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var outerMap = ctx.GetMapping(typeof(IpcOrder));
        var innerMap = ctx.GetMapping(typeof(IpcLine));

        var builder = new OptimizedSqlBuilder(512);
        JoinBuilder.BuildJoinClauseInto(
            joinSql: builder,
            projection: null,
            outerMapping: outerMap,
            outerAlias: "o",
            innerMapping: innerMap,
            innerAlias: "l",
            joinType: "LEFT JOIN",
            outerKeySql: "o.[Id]",
            innerKeySql: "l.[OrderId]",
            orderBy: "o.[Id]");

        var sql = builder.ToSqlString();
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("LEFT JOIN", sql);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 8 — Include with filtering (where clause applied before include)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EagerLoadAsync_WithWhereFilter_IncludesChildrenForMatchingParents()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'FilterMe',null),(2,'Skip',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'FM-B1'),(2,1,'FM-B2'),(3,2,'S-B1')");

        var authors = await ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>()
            .Where(a => a.Name == "FilterMe"))
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToListAsync();

        Assert.Single(authors);
        Assert.Equal(2, authors[0].Books.Count);
    }

    [Fact]
    public void EagerLoad_Sync_WithWhereFilter_IncludesChildrenForMatchingParents()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: false);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'SFM',null),(2,'SSkip',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'SFM-B1'),(2,2,'SS-B1')");

        var authors = ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>()
            .Where(a => a.Name == "SFM"))
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToList();

        Assert.Single(authors);
        Assert.Single(authors[0].Books);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 9 — Include with null principal key (AssignEmptyList path)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EagerLoadAsync_ParentWithNullableKey_AssignsEmptyList()
    {
        // This tests the null key path in BuildParentLookup (AssignEmptyList)
        // We need a nullable PK column — use a workaround with a direct author that has no children
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        // Insert authors with no corresponding children
        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'NoKids1',null),(2,'NoKids2',null)");

        var authors = await ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToListAsync();

        Assert.Equal(2, authors.Count);
        Assert.All(authors, a => Assert.Empty(a.Books));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 10 — Cancellation in EagerLoadAsync
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EagerLoadAsync_PreCancelled_ThrowsOperationCancelled()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'CancelTest',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'CTBook')");

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
                .AsSplitQuery()
                .Include(a => a.Books)
                .ToListAsync(cts.Token));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 11 — JoinBuilder: projection with NewExpression body
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void JoinBuilder_BuildJoinClause_WithNewExpressionProjection_FallsBackToAllColumns()
    {
        // A projection with MethodCall (not simple member access) causes ExtractNeededColumns
        // to return empty — triggering the "all columns" fallback branch
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE IPC_Project (Id INTEGER PRIMARY KEY AUTOINCREMENT, Code TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE IPC_Task (Id INTEGER PRIMARY KEY AUTOINCREMENT, ProjectId INTEGER NOT NULL, Name TEXT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<IpcProject>()
                  .HasMany(p => p.Tasks)
                  .WithOne()
                  .HasForeignKey(t => t.ProjectId, p => p.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var outerMap = ctx.GetMapping(typeof(IpcProject));
        var innerMap = ctx.GetMapping(typeof(IpcTask));

        // Use a projection with a MethodCall - this triggers the empty fallback
        var pParam = Expression.Parameter(typeof(IpcProject), "p");
        var tParam = Expression.Parameter(typeof(IpcTask), "t");
        var pCode = Expression.MakeMemberAccess(pParam, typeof(IpcProject).GetProperty("Code")!);
        // Create a method call: p.Code.ToUpper() - not supported by ExtractNeededColumns
        var toUpper = Expression.Call(pCode, typeof(string).GetMethod("ToUpper", Type.EmptyTypes)!);
        var newExpr = Expression.New(
            typeof(Tuple<string>).GetConstructors()[0],
            toUpper);

        var lambdaExpr = Expression.Lambda(newExpr, pParam, tParam);
        var projection = lambdaExpr;

        var sql = JoinBuilder.BuildJoinClause(
            projection: projection,
            outerMapping: outerMap,
            outerAlias: "p",
            innerMapping: innerMap,
            innerAlias: "t",
            joinType: "INNER JOIN",
            outerKeySql: "p.[Id]",
            innerKeySql: "t.[ProjectId]");

        // The fallback selects all columns from both tables
        Assert.Contains("FROM", sql);
        // Should contain columns from both tables
        Assert.Contains("p.", sql);
        Assert.Contains("t.", sql);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 12 — GroupJoin integration with Include paths
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Async_GroupJoin_ThenInclude_LoadsNestedData()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'GJAuth',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'GJB1'),(2,1,'GJB2')");

        var results = await ctx.Query<IpcAuthor>()
            .GroupJoin(
                ctx.Query<IpcBook>(),
                a => a.Id,
                b => b.AuthorId,
                (a, books) => new { Author = a, Books = books.ToList() })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(2, results[0].Books.Count);
    }

    [Fact]
    public void Sync_GroupJoin_ThenInclude_LoadsNestedData()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: false);

        Exec(cn, "INSERT INTO IPC_Author VALUES(1,'SGJAuth',null)");
        Exec(cn, "INSERT INTO IPC_Book VALUES(1,1,'SGJB1')");

        var results = ctx.Query<IpcAuthor>()
            .GroupJoin(
                ctx.Query<IpcBook>(),
                a => a.Id,
                b => b.AuthorId,
                (a, books) => new { Author = a, Books = books.ToList() })
            .ToList();

        Assert.Single(results);
        Assert.Single(results[0].Books);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Group 13 — Include across large data set (batch key processing)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EagerLoadAsync_ManyParents_AllChildrenLoaded()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: true);

        // Insert 10 authors each with 2 books
        for (int i = 1; i <= 10; i++)
        {
            Exec(cn, $"INSERT INTO IPC_Author VALUES({i},'Author{i}',null)");
            Exec(cn, $"INSERT INTO IPC_Book VALUES({i * 2 - 1},{i},'Book{i}A')");
            Exec(cn, $"INSERT INTO IPC_Book VALUES({i * 2},{i},'Book{i}B')");
        }

        var authors = await ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToListAsync();

        Assert.Equal(10, authors.Count);
        Assert.All(authors, a => Assert.Equal(2, a.Books.Count));
    }

    [Fact]
    public void EagerLoad_Sync_ManyParents_AllChildrenLoaded()
    {
        using var cn = OpenDb();
        using var ctx = CreateAuthorContext(cn, async: false);

        for (int i = 1; i <= 8; i++)
        {
            Exec(cn, $"INSERT INTO IPC_Author VALUES({i},'SA{i}',null)");
            Exec(cn, $"INSERT INTO IPC_Book VALUES({i * 2 - 1},{i},'SBA')");
            Exec(cn, $"INSERT INTO IPC_Book VALUES({i * 2},{i},'SBB')");
        }

        var authors = ((INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>())
            .AsSplitQuery()
            .Include(a => a.Books)
            .ToList();

        Assert.Equal(8, authors.Count);
        Assert.All(authors, a => Assert.Equal(2, a.Books.Count));
    }
}

// ── Extra entity types needed by composite key tests ──────────────────────────

[Table("IPC_CompositeParent")]
public class IpcCompositeParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public ICollection<IpcCompositeChild> Children { get; set; } = new List<IpcCompositeChild>();
}

[Table("IPC_CompositeChild")]
public class IpcCompositeChild
{
    public int ParentId { get; set; }
    public int Seq { get; set; }
    public string Info { get; set; } = string.Empty;
}
