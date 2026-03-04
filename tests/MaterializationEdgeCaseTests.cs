using System;
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
/// Tests materialization against real SQLite databases for edge cases:
/// primitive types, nullable types, special values, special strings.
/// </summary>
public class MaterializationEdgeCaseTests
{
    // ─── Primitive types entity ────────────────────────────────────────────

    [Table("PrimitivesEntity")]
    private class PrimitivesEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int IntVal { get; set; }
        public long LongVal { get; set; }
        public double DoubleVal { get; set; }
        public bool BoolVal { get; set; }
        public string StringVal { get; set; } = string.Empty;
        public decimal DecimalVal { get; set; }
    }

    [Table("NullablePrimitivesEntity")]
    private class NullablePrimitivesEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int? NullableInt { get; set; }
        public long? NullableLong { get; set; }
        public double? NullableDouble { get; set; }
        public bool? NullableBool { get; set; }
        public string? NullableString { get; set; }
        public decimal? NullableDecimal { get; set; }
    }

    [Table("SpecialStringEntity")]
    private class SpecialStringEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Content { get; set; } = string.Empty;
    }

    // ─── Primitive type materialization ───────────────────────────────────

    [Fact]
    public async Task Materialize_AllPrimitiveTypes_CorrectValues()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE PrimitivesEntity (
                    Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    IntVal      INTEGER NOT NULL,
                    LongVal     INTEGER NOT NULL,
                    DoubleVal   REAL NOT NULL,
                    BoolVal     INTEGER NOT NULL,
                    StringVal   TEXT NOT NULL,
                    DecimalVal  NUMERIC NOT NULL
                );
                INSERT INTO PrimitivesEntity (IntVal, LongVal, DoubleVal, BoolVal, StringVal, DecimalVal)
                VALUES (42, 9876543210, 3.14159, 1, 'hello world', 12345.67)";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<PrimitivesEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal(42, entity.IntVal);
        Assert.Equal(9876543210L, entity.LongVal);
        Assert.True(Math.Abs(entity.DoubleVal - 3.14159) < 0.00001);
        Assert.True(entity.BoolVal);
        Assert.Equal("hello world", entity.StringVal);
        Assert.Equal(12345.67m, entity.DecimalVal);
    }

    [Fact]
    public async Task Materialize_PrimitiveMaxMinValues_CorrectValues()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE PrimitivesEntity (
                    Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    IntVal      INTEGER NOT NULL,
                    LongVal     INTEGER NOT NULL,
                    DoubleVal   REAL NOT NULL,
                    BoolVal     INTEGER NOT NULL,
                    StringVal   TEXT NOT NULL,
                    DecimalVal  NUMERIC NOT NULL
                )";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO PrimitivesEntity (IntVal, LongVal, DoubleVal, BoolVal, StringVal, DecimalVal) VALUES (@i, @l, @d, @b, @s, @dec)";
            cmd.Parameters.AddWithValue("@i", int.MaxValue);
            cmd.Parameters.AddWithValue("@l", long.MaxValue);
            cmd.Parameters.AddWithValue("@d", double.MaxValue);
            cmd.Parameters.AddWithValue("@b", 0);
            cmd.Parameters.AddWithValue("@s", "max test");
            cmd.Parameters.AddWithValue("@dec", 99999999.99m);
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<PrimitivesEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal(int.MaxValue, entity.IntVal);
        Assert.Equal(long.MaxValue, entity.LongVal);
        Assert.False(entity.BoolVal);
    }

    // ─── Nullable primitive types ──────────────────────────────────────────

    [Fact]
    public async Task Materialize_NullableTypes_AllNull_CorrectValues()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE NullablePrimitivesEntity (
                    Id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    NullableInt     INTEGER,
                    NullableLong    INTEGER,
                    NullableDouble  REAL,
                    NullableBool    INTEGER,
                    NullableString  TEXT,
                    NullableDecimal NUMERIC
                );
                INSERT INTO NullablePrimitivesEntity (NullableInt, NullableLong, NullableDouble, NullableBool, NullableString, NullableDecimal)
                VALUES (NULL, NULL, NULL, NULL, NULL, NULL)";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<NullablePrimitivesEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Null(entity.NullableInt);
        Assert.Null(entity.NullableLong);
        Assert.Null(entity.NullableDouble);
        Assert.Null(entity.NullableBool);
        Assert.Null(entity.NullableString);
        Assert.Null(entity.NullableDecimal);
    }

    [Fact]
    public async Task Materialize_NullableTypes_NonNull_CorrectValues()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE NullablePrimitivesEntity (
                    Id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    NullableInt     INTEGER,
                    NullableLong    INTEGER,
                    NullableDouble  REAL,
                    NullableBool    INTEGER,
                    NullableString  TEXT,
                    NullableDecimal NUMERIC
                )";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO NullablePrimitivesEntity (NullableInt, NullableLong, NullableDouble, NullableBool, NullableString, NullableDecimal) VALUES (42, 100, 1.5, 1, 'test', 9.99)";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<NullablePrimitivesEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal(42, entity.NullableInt);
        Assert.Equal(100L, entity.NullableLong);
        Assert.Equal(1.5, entity.NullableDouble);
        Assert.True(entity.NullableBool);
        Assert.Equal("test", entity.NullableString);
        Assert.Equal(9.99m, entity.NullableDecimal);
    }

    // ─── Anonymous type projection ─────────────────────────────────────────

    [Fact]
    public async Task Materialize_AnonymousTypeProjection_CorrectValues()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE PrimitivesEntity (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    IntVal INTEGER NOT NULL,
                    LongVal INTEGER NOT NULL,
                    DoubleVal REAL NOT NULL,
                    BoolVal INTEGER NOT NULL,
                    StringVal TEXT NOT NULL,
                    DecimalVal NUMERIC NOT NULL
                );
                INSERT INTO PrimitivesEntity (IntVal, LongVal, DoubleVal, BoolVal, StringVal, DecimalVal)
                VALUES (7, 8, 9.0, 1, 'projected', 10.5)";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = await ctx.Query<PrimitivesEntity>()
            .Select(x => new { x.StringVal, x.IntVal })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("projected", results[0].StringVal);
        Assert.Equal(7, results[0].IntVal);
    }

    // ─── Special string values ─────────────────────────────────────────────

    [Fact]
    public async Task Materialize_VeryLongString_CorrectValue()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SpecialStringEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Content TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var longString = new string('x', 10000);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO SpecialStringEntity (Content) VALUES (@c)";
            cmd.Parameters.AddWithValue("@c", longString);
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<SpecialStringEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal(10000, entity.Content.Length);
        Assert.Equal(longString, entity.Content);
    }

    [Fact]
    public async Task Materialize_StringWithNewlineTab_CorrectValue()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SpecialStringEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Content TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var specialString = "line1\nline2\ttabbed\r\nwindows";
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO SpecialStringEntity (Content) VALUES (@c)";
            cmd.Parameters.AddWithValue("@c", specialString);
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<SpecialStringEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal(specialString, entity.Content);
    }

    [Fact]
    public async Task Materialize_StringWithSingleQuote_CorrectValue()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SpecialStringEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Content TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var singleQuoteString = "it's a test with O'Connor's";
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO SpecialStringEntity (Content) VALUES (@c)";
            cmd.Parameters.AddWithValue("@c", singleQuoteString);
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<SpecialStringEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal(singleQuoteString, entity.Content);
    }

    [Fact]
    public async Task Materialize_EmptyString_CorrectValue()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SpecialStringEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Content TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO SpecialStringEntity (Content) VALUES ('')";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<SpecialStringEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal(string.Empty, entity.Content);
    }

    [Fact]
    public async Task Materialize_StringWithUnicode_CorrectValue()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SpecialStringEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Content TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var unicodeString = "Hello \u4e16\u754c \u00e9\u00e0\u00fc \ud83d\ude00"; // Chinese + accented + emoji
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO SpecialStringEntity (Content) VALUES (@c)";
            cmd.Parameters.AddWithValue("@c", unicodeString);
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<SpecialStringEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal(unicodeString, entity.Content);
    }

    // ─── Multiple rows materialization ────────────────────────────────────

    [Fact]
    public async Task Materialize_MultipleRows_AllReturnedCorrectly()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE PrimitivesEntity (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    IntVal INTEGER NOT NULL,
                    LongVal INTEGER NOT NULL,
                    DoubleVal REAL NOT NULL,
                    BoolVal INTEGER NOT NULL,
                    StringVal TEXT NOT NULL,
                    DecimalVal NUMERIC NOT NULL
                )";
            cmd.ExecuteNonQuery();
        }

        for (int i = 1; i <= 5; i++)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "INSERT INTO PrimitivesEntity (IntVal, LongVal, DoubleVal, BoolVal, StringVal, DecimalVal) VALUES (@i, @l, @d, @b, @s, @dec)";
            cmd.Parameters.AddWithValue("@i", i);
            cmd.Parameters.AddWithValue("@l", (long)i * 100);
            cmd.Parameters.AddWithValue("@d", i * 0.5);
            cmd.Parameters.AddWithValue("@b", i % 2 == 0 ? 1 : 0);
            cmd.Parameters.AddWithValue("@s", $"item{i}");
            cmd.Parameters.AddWithValue("@dec", i * 1.1m);
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = await ctx.Query<PrimitivesEntity>().OrderBy(x => x.IntVal).ToListAsync();

        Assert.Equal(5, results.Count);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(i + 1, results[i].IntVal);
            Assert.Equal($"item{i + 1}", results[i].StringVal);
        }
    }

    // ─── Boolean edge cases ────────────────────────────────────────────────

    [Fact]
    public async Task Materialize_BoolFalse_CorrectValue()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE PrimitivesEntity (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    IntVal INTEGER NOT NULL,
                    LongVal INTEGER NOT NULL,
                    DoubleVal REAL NOT NULL,
                    BoolVal INTEGER NOT NULL,
                    StringVal TEXT NOT NULL,
                    DecimalVal NUMERIC NOT NULL
                );
                INSERT INTO PrimitivesEntity (IntVal, LongVal, DoubleVal, BoolVal, StringVal, DecimalVal) VALUES (0, 0, 0.0, 0, 'false', 0.0)";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = await ctx.Query<PrimitivesEntity>().FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.False(entity.BoolVal);
    }
}
