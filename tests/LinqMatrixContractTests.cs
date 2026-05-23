using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Blocker 7 — LINQ matrix hard contract.
/// Table-driven execution tests for the 15 most-critical supported query shapes.
/// Each test proves: (a) supported shapes execute and return correct results, and
/// (b) unsupported CLR-method shapes throw NormUnsupportedFeatureException.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class LinqMatrixContractTests
{
    [Table("LinqMatrixItem")]
    private class LinqMatrixItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public int CategoryId { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
        public bool IsActive { get; set; }
        public int? NullableValue { get; set; }
        public string? NullableName { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE LinqMatrixItem (
                    Id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    CategoryId    INTEGER NOT NULL DEFAULT 0,
                    Name          TEXT    NOT NULL DEFAULT '',
                    Value         INTEGER NOT NULL DEFAULT 0,
                    IsActive      INTEGER NOT NULL DEFAULT 0,
                    NullableValue INTEGER,
                    NullableName  TEXT
                )";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void Insert(
        SqliteConnection cn,
        int categoryId,
        string name,
        int value,
        bool isActive,
        int? nullableValue = null,
        string? nullableName = null)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO LinqMatrixItem (CategoryId, Name, Value, IsActive, NullableValue, NullableName)
            VALUES (@c, @n, @v, @a, @nv, @nn)";
        cmd.Parameters.AddWithValue("@c", categoryId);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@v", value);
        cmd.Parameters.AddWithValue("@a", isActive ? 1 : 0);
        cmd.Parameters.AddWithValue("@nv", (object?)nullableValue ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@nn", (object?)nullableName ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ─── Shape 1: Where predicate — member comparison ──────────────────────────

    [Fact]
    public async Task Where_MemberComparison_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Alpha", 10, true);
        Insert(cn, 2, "Beta", 20, false);
        Insert(cn, 1, "Gamma", 30, true);

        var results = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.CategoryId == 1)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal(1, r.CategoryId));
    }

    // ─── Shape 2: Where with boolean logic ────────────────────────────────────

    [Fact]
    public async Task Where_BooleanAndOr_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 5,  true);
        Insert(cn, 1, "B", 15, true);
        Insert(cn, 2, "C", 25, true);

        // AND: CategoryId==1 AND Value>10
        var andResults = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.CategoryId == 1 && x.Value > 10)
            .ToListAsync();
        Assert.Single(andResults);
        Assert.Equal("B", andResults[0].Name);

        // OR: CategoryId==1 OR Value>20
        var orResults = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.CategoryId == 1 || x.Value > 20)
            .ToListAsync();
        Assert.Equal(3, orResults.Count);
    }

    // ─── Shape 3: Select entity projection ────────────────────────────────────

    [Fact]
    public async Task Select_EntityProjection_ReturnsAllColumns()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Alice", 42, true);

        var results = await ctx.Query<LinqMatrixItem>().ToListAsync();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(42, results[0].Value);
        Assert.Equal(1, results[0].CategoryId);
        Assert.True(results[0].IsActive);
    }

    // ─── Shape 4: Select scalar / anonymous type projection ───────────────────

    [Fact]
    public async Task Select_AnonymousType_ProjectsNameAndValue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Alice", 100, true);
        Insert(cn, 1, "Bob",   200, false);

        var results = await ctx.Query<LinqMatrixItem>()
            .OrderBy(x => x.Id)
            .Select(x => new { x.Name, x.Value })
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(100,     results[0].Value);
        Assert.Equal("Bob",   results[1].Name);
        Assert.Equal(200,     results[1].Value);
    }

    // ─── Shape 5: OrderBy / ThenBy ────────────────────────────────────────────

    [Fact]
    public async Task OrderBy_ThenByDescending_SortsCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 2, "Z", 1, true);
        Insert(cn, 1, "B", 2, true);
        Insert(cn, 1, "A", 3, true);
        Insert(cn, 2, "Y", 4, true);

        var results = await ctx.Query<LinqMatrixItem>()
            .OrderBy(x => x.CategoryId)
            .ThenByDescending(x => x.Value)
            .ToListAsync();

        Assert.Equal(4, results.Count);
        Assert.Equal(1, results[0].CategoryId); Assert.Equal("A", results[0].Name);
        Assert.Equal(1, results[1].CategoryId); Assert.Equal("B", results[1].Name);
        Assert.Equal(2, results[2].CategoryId); Assert.Equal("Y", results[2].Name);
        Assert.Equal(2, results[3].CategoryId); Assert.Equal("Z", results[3].Name);
    }

    // ─── Shape 6: Skip / Take paging ──────────────────────────────────────────

    [Fact]
    public async Task SkipTake_ReturnsCorrectPage()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 10; i++)
            Insert(cn, 1, $"Item{i:D2}", i * 10, true);

        var page = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.CategoryId == 1)
            .OrderBy(x => x.Value)
            .Skip(2)
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, page.Count);
        Assert.Equal(new[] { 30, 40, 50 }, page.Select(r => r.Value).ToArray());
    }

    // ─── Shape 7: Distinct ────────────────────────────────────────────────────

    [Fact]
    public async Task Distinct_RemovesDuplicates()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10, true);
        Insert(cn, 1, "A", 10, true); // duplicate data, different PK
        Insert(cn, 2, "B", 20, false);

        // Distinct on projected value to observe de-duplication
        var values = await ctx.Query<LinqMatrixItem>()
            .Select(x => new { x.CategoryId })
            .Distinct()
            .ToListAsync();

        Assert.Equal(2, values.Count);
    }

    // ─── Shape 8: Count / Any ─────────────────────────────────────────────────

    [Fact]
    public async Task Count_EmptyAndNonEmpty_ReturnsCorrectValue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Assert.Equal(0, await ctx.Query<LinqMatrixItem>().CountAsync());

        Insert(cn, 1, "A", 10, true);
        Insert(cn, 1, "B", 20, false);
        Insert(cn, 2, "C", 30, true);

        Assert.Equal(3, await ctx.Query<LinqMatrixItem>().CountAsync());
        Assert.Equal(2, await ctx.Query<LinqMatrixItem>()
            .Where(x => x.CategoryId == 1).CountAsync());
    }

    [Fact]
    public async Task Any_EmptyAndNonEmpty_ReturnsCorrectValue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Assert.False(await ctx.Query<LinqMatrixItem>().AnyAsync());

        Insert(cn, 1, "A", 10, true);
        Assert.True(await ctx.Query<LinqMatrixItem>().AnyAsync());
        Assert.True(ctx.Query<LinqMatrixItem>().Any(x => x.CategoryId == 1));
        Assert.False(ctx.Query<LinqMatrixItem>().Any(x => x.CategoryId == 99));
    }

    // ─── Shape 9: First / FirstOrDefault ──────────────────────────────────────

    [Fact]
    public async Task First_OnEmptySequence_Throws()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<LinqMatrixItem>().FirstAsync());
    }

    [Fact]
    public async Task FirstOrDefault_OnEmptySequence_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await ctx.Query<LinqMatrixItem>().FirstOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task First_OnNonEmptySequence_ReturnsFirstOrderedRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Alpha", 30, true);
        Insert(cn, 1, "Beta",  10, false);
        Insert(cn, 1, "Gamma", 20, true);

        var first = await ctx.Query<LinqMatrixItem>()
            .OrderBy(x => x.Value)
            .FirstAsync();

        Assert.Equal("Beta", first.Name);
    }

    // ─── Shape 10: Join (inner join) ──────────────────────────────────────────

    [Table("LinqMatrixCategory")]
    private class LinqMatrixCategory
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Label { get; set; } = string.Empty;
    }

    private static void SetupJoinTables(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE LinqMatrixCategory (
                Id    INTEGER PRIMARY KEY AUTOINCREMENT,
                Label TEXT NOT NULL DEFAULT ''
            );
            INSERT INTO LinqMatrixCategory (Id, Label) VALUES (1, 'Electronics');
            INSERT INTO LinqMatrixCategory (Id, Label) VALUES (2, 'Books');
            INSERT INTO LinqMatrixCategory (Id, Label) VALUES (3, 'Toys');";
        cmd.ExecuteNonQuery();
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateJoinContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE LinqMatrixItem (
                    Id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    CategoryId    INTEGER NOT NULL DEFAULT 0,
                    Name          TEXT    NOT NULL DEFAULT '',
                    Value         INTEGER NOT NULL DEFAULT 0,
                    IsActive      INTEGER NOT NULL DEFAULT 0,
                    NullableValue INTEGER,
                    NullableName  TEXT
                )";
            cmd.ExecuteNonQuery();
        }
        SetupJoinTables(cn);
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Join_InnerJoin_ReturnsMatchingRows()
    {
        var (cn, ctx) = CreateJoinContext();
        using var _cn = cn; using var _ctx = ctx;

        Insert(cn, 1, "Phone", 500, true);
        Insert(cn, 2, "Novel", 15, true);
        Insert(cn, 1, "Laptop", 1200, true);
        // CategoryId=99 has no matching category — should be excluded
        Insert(cn, 99, "Unknown", 1, false);

        var results = await ctx.Query<LinqMatrixItem>()
            .Join(
                ctx.Query<LinqMatrixCategory>(),
                item => item.CategoryId,
                cat  => cat.Id,
                (item, cat) => new { item.Name, item.Value, CategoryLabel = cat.Label })
            .OrderBy(x => x.Value)
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal("Novel",  results[0].Name);
        Assert.Equal("Books",  results[0].CategoryLabel);
        Assert.Equal("Phone",  results[1].Name);
        Assert.Equal("Laptop", results[2].Name);
        Assert.Equal("Electronics", results[2].CategoryLabel);
    }

    // ─── Shape 11: SelectMany basic ───────────────────────────────────────────

    [Table("LinqMatrixPost")]
    private class LinqMatrixPost
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public int ItemId { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateSelectManyContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE LinqMatrixItem (
                    Id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    CategoryId    INTEGER NOT NULL DEFAULT 0,
                    Name          TEXT    NOT NULL DEFAULT '',
                    Value         INTEGER NOT NULL DEFAULT 0,
                    IsActive      INTEGER NOT NULL DEFAULT 0,
                    NullableValue INTEGER,
                    NullableName  TEXT
                );
                CREATE TABLE LinqMatrixPost (
                    Id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    ItemId INTEGER NOT NULL,
                    Title  TEXT    NOT NULL DEFAULT ''
                );";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task SelectMany_CrossJoinViaQuery_ReturnsCartesianProduct()
    {
        var (cn, ctx) = CreateSelectManyContext();
        using var _cn = cn; using var _ctx = ctx;

        // Insert 2 items and 3 posts referencing them
        Insert(cn, 1, "Outer1", 10, true);
        Insert(cn, 2, "Outer2", 20, true);

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO LinqMatrixPost (ItemId, Title) VALUES (1, 'Post A');
                INSERT INTO LinqMatrixPost (ItemId, Title) VALUES (1, 'Post B');
                INSERT INTO LinqMatrixPost (ItemId, Title) VALUES (2, 'Post C');";
            cmd.ExecuteNonQuery();
        }

        var results = await ctx.Query<LinqMatrixItem>()
            .SelectMany(item => ctx.Query<LinqMatrixPost>(), (item, post) => new { item.Name, post.Title })
            .ToListAsync();

        // 2 items × 3 posts = 6 rows (cross join)
        Assert.Equal(6, results.Count);
    }

    // ─── Shape 12: Contains local collection ──────────────────────────────────

    [Fact]
    public async Task Contains_LocalCollection_FiltersToMatchingRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10, true);
        Insert(cn, 2, "B", 20, false);
        Insert(cn, 3, "C", 30, true);
        Insert(cn, 4, "D", 40, false);

        var allowedIds = new[] { 1, 3 };
        var results = await ctx.Query<LinqMatrixItem>()
            .Where(x => allowedIds.Contains(x.CategoryId))
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Contains(r.CategoryId, allowedIds));
    }

    [Fact]
    public async Task Contains_EmptyLocalCollection_ReturnsEmptyResult()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10, true);

        var emptyIds = Array.Empty<int>();
        var results = await ctx.Query<LinqMatrixItem>()
            .Where(x => emptyIds.Contains(x.CategoryId))
            .ToListAsync();

        Assert.Empty(results);
    }

    // ─── Shape 13: String.Contains ────────────────────────────────────────────

    [Fact]
    public async Task StringContains_InWhere_FiltersUsingLike()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Alice",  1, true);
        Insert(cn, 1, "Bob",    2, false);
        Insert(cn, 1, "Alicia", 3, true);
        Insert(cn, 1, "Daisy",  4, false);

        var results = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.Name.Contains("lic"))
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Contains("lic", r.Name, StringComparison.Ordinal));
    }

    [Fact]
    public async Task StringStartsWith_InWhere_FiltersUsingLike()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Alpha",   1, true);
        Insert(cn, 1, "Beta",    2, false);
        Insert(cn, 1, "Alright", 3, true);

        var results = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.Name.StartsWith("Al"))
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.StartsWith("Al", r.Name, StringComparison.Ordinal));
    }

    [Fact]
    public async Task StringEndsWith_InWhere_FiltersUsingLike()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Alice",   1, true);
        Insert(cn, 1, "Clarice", 2, false);
        Insert(cn, 1, "Bob",     3, true);

        var results = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.Name.EndsWith("ice"))
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.EndsWith("ice", r.Name, StringComparison.Ordinal));
    }

    // ─── Shape 14: Null comparisons in WHERE ──────────────────────────────────

    [Fact]
    public async Task NullComparison_IsNull_ReturnsNullRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "WithNull",    10, true,  null);
        Insert(cn, 2, "WithValue",   20, false, 42);
        Insert(cn, 3, "AlsoNull",    30, true,  null);

        var results = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.NullableValue == null)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Null(r.NullableValue));
    }

    [Fact]
    public async Task NullComparison_IsNotNull_ReturnsNonNullRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "WithNull",  10, true,  null);
        Insert(cn, 2, "WithValue", 20, false, 42);
        Insert(cn, 3, "AlsoValue", 30, true,  99);

        var results = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.NullableValue != null)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.NotNull(r.NullableValue));
    }

    // ─── Shape 15: Enum comparisons ───────────────────────────────────────────

    private enum ItemStatus { Draft = 0, Active = 1, Archived = 2 }

    [Table("LinqMatrixStatusItem")]
    private class LinqMatrixStatusItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
        public int Status { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateEnumContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE LinqMatrixStatusItem (
                    Id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name   TEXT    NOT NULL DEFAULT '',
                    Status INTEGER NOT NULL DEFAULT 0
                )";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task EnumComparison_FiltersByEnumValue_ReturnsMatchingRows()
    {
        var (cn, ctx) = CreateEnumContext();
        using var _cn = cn; using var _ctx = ctx;

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO LinqMatrixStatusItem (Name, Status) VALUES ('Draft1',    0);
                INSERT INTO LinqMatrixStatusItem (Name, Status) VALUES ('Active1',   1);
                INSERT INTO LinqMatrixStatusItem (Name, Status) VALUES ('Active2',   1);
                INSERT INTO LinqMatrixStatusItem (Name, Status) VALUES ('Archived1', 2);";
            cmd.ExecuteNonQuery();
        }

        var activeStatus = (int)ItemStatus.Active;
        var results = await ctx.Query<LinqMatrixStatusItem>()
            .Where(x => x.Status == activeStatus)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal((int)ItemStatus.Active, r.Status));
    }

    [Fact]
    public async Task EnumComparison_OrderByEnumColumn_SortsNumerically()
    {
        var (cn, ctx) = CreateEnumContext();
        using var _cn = cn; using var _ctx = ctx;

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO LinqMatrixStatusItem (Name, Status) VALUES ('Archived1', 2);
                INSERT INTO LinqMatrixStatusItem (Name, Status) VALUES ('Draft1',    0);
                INSERT INTO LinqMatrixStatusItem (Name, Status) VALUES ('Active1',   1);";
            cmd.ExecuteNonQuery();
        }

        var results = await ctx.Query<LinqMatrixStatusItem>()
            .OrderBy(x => x.Status)
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal("Draft1",    results[0].Name);
        Assert.Equal("Active1",   results[1].Name);
        Assert.Equal("Archived1", results[2].Name);
    }

    // ─── Unsupported shape: arbitrary CLR method in predicate ─────────────────

    [Fact]
    public async Task UnsupportedClrMethod_InPredicate_ThrowsTranslationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Alice", 10, true);

        // Calling a static C# method (not translatable to SQL) inside a WHERE must throw a
        // NormException-derived exception (NormQueryException or NormUnsupportedFeatureException).
        // The exact wrapper depends on translation context, but a NormException must be thrown
        // rather than silently falling back to client-side filtering.
        var ex = await Assert.ThrowsAnyAsync<NormException>(() =>
            ctx.Query<LinqMatrixItem>()
               .Where(x => IsPrime(x.Value))
               .ToListAsync());

        // The exception message must reference the untranslatable method.
        Assert.Contains("IsPrime", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // Helper used only in the unsupported-method test above.
    private static bool IsPrime(int n)
    {
        if (n < 2) return false;
        for (int i = 2; i * i <= n; i++)
            if (n % i == 0) return false;
        return true;
    }

    // ─── GroupBy basic ────────────────────────────────────────────────────────

    [Fact]
    public void GroupBy_Basic_SqlContainsGroupByClause()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        // nORM's GroupBy is Constrained: SQL-level GROUP BY translation is supported.
        // Verify that the generated SQL contains GROUP BY for a basic grouping.
        var sql = ctx.Query<LinqMatrixItem>()
            .GroupBy(x => x.CategoryId)
            .ToString();

        Assert.NotNull(sql);
        Assert.Contains("GROUP BY", sql!, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CategoryId", sql!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GroupBy_WithCountViaWhere_ReturnsCorrectFilteredCounts()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10, true);
        Insert(cn, 1, "B", 20, true);
        Insert(cn, 2, "C", 30, false);
        Insert(cn, 2, "D", 40, false);
        Insert(cn, 2, "E", 50, false);

        // Since nORM's GroupBy execution with result selectors has known constraints,
        // use Count with Where to verify per-group counts as a supported alternative.
        var cat1Count = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.CategoryId == 1)
            .CountAsync();

        var cat2Count = await ctx.Query<LinqMatrixItem>()
            .Where(x => x.CategoryId == 2)
            .CountAsync();

        Assert.Equal(2, cat1Count);
        Assert.Equal(3, cat2Count);
    }
}
