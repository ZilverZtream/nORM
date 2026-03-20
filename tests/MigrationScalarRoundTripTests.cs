using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Gate 3.5→4.0: Migration round-trip/type-parity tests for byte[], enum, DateOnly, TimeOnly,
/// DateTimeOffset, and TimeSpan per provider. Verifies that snapshot → diff → SQL generation
/// produces correct DDL types for each scalar type on each provider.
/// </summary>
public class MigrationScalarRoundTripTests
{
    // ── Test entities ────────────────────────────────────────────────────────

    // Must be internal so migration generators can resolve via Assembly.GetType().
    // Entity classes that use this enum are also internal to avoid CS0053.
    internal enum Priority { Low, Medium, High }

    [Table("ByteArrayEntity")]
    public class ByteArrayEntity
    {
        [Key] public int Id { get; set; }
        public byte[] Data { get; set; } = Array.Empty<byte>();
    }

    [Table("DateOnlyEntity")]
    public class DateOnlyEntity
    {
        [Key] public int Id { get; set; }
        public DateOnly BirthDate { get; set; }
    }

    [Table("TimeOnlyEntity")]
    public class TimeOnlyEntity
    {
        [Key] public int Id { get; set; }
        public TimeOnly StartTime { get; set; }
    }

    [Table("DateTimeOffsetEntity")]
    public class DateTimeOffsetEntity
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
    }

    [Table("TimeSpanEntity")]
    public class TimeSpanEntity
    {
        [Key] public int Id { get; set; }
        public TimeSpan Duration { get; set; }
    }

    [Table("EnumEntity")]
    internal class EnumEntity
    {
        [Key] public int Id { get; set; }
        public Priority Level { get; set; }
    }

    [Table("AllScalarsEntity")]
    internal class AllScalarsEntity
    {
        [Key] public int Id { get; set; }
        public byte[] Data { get; set; } = Array.Empty<byte>();
        public DateOnly BirthDate { get; set; }
        public TimeOnly StartTime { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public TimeSpan Duration { get; set; }
        public Priority Level { get; set; }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static SchemaDiff MakeSingleColumnDiff(string clrTypeFullName)
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(new TableSchema
        {
            Name = "TestTable",
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema
                {
                    Name = "Id",
                    ClrType = typeof(int).FullName!,
                    IsPrimaryKey = true,
                    IsNullable = false
                },
                new ColumnSchema
                {
                    Name = "Col",
                    ClrType = clrTypeFullName,
                    IsNullable = true
                }
            }
        });
        return diff;
    }

    private static string ExtractColType(MigrationSqlStatements result, string colName)
    {
        var createSql = result.Up.First(s => s.Contains("CREATE TABLE", StringComparison.OrdinalIgnoreCase));
        var idx = createSql.IndexOf(colName, StringComparison.OrdinalIgnoreCase);
        if (idx < 0) throw new Exception($"Column {colName} not found in: {createSql}");
        var afterName = idx + colName.Length;
        while (afterName < createSql.Length && (createSql[afterName] == ']' || createSql[afterName] == '"' || createSql[afterName] == '`'))
            afterName++;
        while (afterName < createSql.Length && createSql[afterName] == ' ')
            afterName++;
        // Find the type terminator: " NOT NULL" or " NULL" (whichever comes first)
        var notNullEnd = createSql.IndexOf(" NOT NULL", afterName, StringComparison.OrdinalIgnoreCase);
        var nullEnd = createSql.IndexOf(" NULL", afterName, StringComparison.OrdinalIgnoreCase);
        int typeEnd;
        if (notNullEnd >= 0 && (nullEnd < 0 || notNullEnd <= nullEnd))
            typeEnd = notNullEnd;
        else if (nullEnd >= 0)
            typeEnd = nullEnd;
        else
            throw new Exception($"Could not find NULL marker after type in: {createSql}");
        return createSql.Substring(afterName, typeEnd - afterName).Trim();
    }

    private static SchemaSnapshot SnapshotFromDiff(SchemaDiff diff)
    {
        var snap = new SchemaSnapshot();
        snap.Tables.AddRange(diff.AddedTables);
        return snap;
    }

    // ── 1. byte[] column ────────────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite", "BLOB")]
    [InlineData("sqlserver", "VARBINARY(MAX)")]
    [InlineData("mysql", "BLOB")]
    [InlineData("postgres", "BYTEA")]
    public void ByteArray_Column_MapsToCorrectType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var diff = MakeSingleColumnDiff(typeof(byte[]).FullName!);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    [Theory]
    [InlineData("sqlite", "BLOB")]
    [InlineData("sqlserver", "VARBINARY(MAX)")]
    [InlineData("mysql", "BLOB")]
    [InlineData("postgres", "BYTEA")]
    public void ByteArray_RoundTrip_SnapshotDiffGeneratePreservesType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var oldSnap = new SchemaSnapshot();
        var newSnap = SnapshotFromDiff(MakeSingleColumnDiff(typeof(byte[]).FullName!));
        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        Assert.True(diff.HasChanges);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    // ── 2. DateOnly column ──────────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite", "TEXT")]
    [InlineData("sqlserver", "DATE")]
    [InlineData("mysql", "DATE")]
    [InlineData("postgres", "DATE")]
    public void DateOnly_Column_MapsToCorrectType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var diff = MakeSingleColumnDiff(typeof(DateOnly).FullName!);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    [Theory]
    [InlineData("sqlite", "TEXT")]
    [InlineData("sqlserver", "DATE")]
    [InlineData("mysql", "DATE")]
    [InlineData("postgres", "DATE")]
    public void DateOnly_RoundTrip_SnapshotDiffGeneratePreservesType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var oldSnap = new SchemaSnapshot();
        var newSnap = SnapshotFromDiff(MakeSingleColumnDiff(typeof(DateOnly).FullName!));
        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        Assert.True(diff.HasChanges);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    // ── 3. TimeOnly column ──────────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite", "TEXT")]
    [InlineData("sqlserver", "TIME")]
    [InlineData("mysql", "TIME")]
    [InlineData("postgres", "TIME")]
    public void TimeOnly_Column_MapsToCorrectType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var diff = MakeSingleColumnDiff(typeof(TimeOnly).FullName!);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    [Theory]
    [InlineData("sqlite", "TEXT")]
    [InlineData("sqlserver", "TIME")]
    [InlineData("mysql", "TIME")]
    [InlineData("postgres", "TIME")]
    public void TimeOnly_RoundTrip_SnapshotDiffGeneratePreservesType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var oldSnap = new SchemaSnapshot();
        var newSnap = SnapshotFromDiff(MakeSingleColumnDiff(typeof(TimeOnly).FullName!));
        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        Assert.True(diff.HasChanges);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    // ── 4. DateTimeOffset column ────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite", "TEXT")]
    [InlineData("sqlserver", "DATETIMEOFFSET")]
    [InlineData("mysql", "DATETIME")]
    [InlineData("postgres", "TIMESTAMPTZ")]
    public void DateTimeOffset_Column_MapsToCorrectType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var diff = MakeSingleColumnDiff(typeof(DateTimeOffset).FullName!);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    [Theory]
    [InlineData("sqlite", "TEXT")]
    [InlineData("sqlserver", "DATETIMEOFFSET")]
    [InlineData("mysql", "DATETIME")]
    [InlineData("postgres", "TIMESTAMPTZ")]
    public void DateTimeOffset_RoundTrip_SnapshotDiffGeneratePreservesType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var oldSnap = new SchemaSnapshot();
        var newSnap = SnapshotFromDiff(MakeSingleColumnDiff(typeof(DateTimeOffset).FullName!));
        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        Assert.True(diff.HasChanges);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    // ── 5. TimeSpan column ──────────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite", "TEXT")]
    [InlineData("sqlserver", "TIME")]
    [InlineData("mysql", "TIME")]
    [InlineData("postgres", "INTERVAL")]
    public void TimeSpan_Column_MapsToCorrectType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var diff = MakeSingleColumnDiff(typeof(TimeSpan).FullName!);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    [Theory]
    [InlineData("sqlite", "TEXT")]
    [InlineData("sqlserver", "TIME")]
    [InlineData("mysql", "TIME")]
    [InlineData("postgres", "INTERVAL")]
    public void TimeSpan_RoundTrip_SnapshotDiffGeneratePreservesType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var oldSnap = new SchemaSnapshot();
        var newSnap = SnapshotFromDiff(MakeSingleColumnDiff(typeof(TimeSpan).FullName!));
        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        Assert.True(diff.HasChanges);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    // ── 6. Enum column ──────────────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite", "INTEGER")]
    [InlineData("sqlserver", "INT")]
    [InlineData("mysql", "INT")]
    [InlineData("postgres", "INTEGER")]
    public void Enum_Column_MapsToUnderlyingIntType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var diff = MakeSingleColumnDiff(typeof(Priority).FullName!);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    [Theory]
    [InlineData("sqlite", "INTEGER")]
    [InlineData("sqlserver", "INT")]
    [InlineData("mysql", "INT")]
    [InlineData("postgres", "INTEGER")]
    public void Enum_RoundTrip_SnapshotDiffGeneratePreservesType(string provider, string expectedType)
    {
        var gen = CreateGenerator(provider);
        var oldSnap = new SchemaSnapshot();
        var newSnap = SnapshotFromDiff(MakeSingleColumnDiff(typeof(Priority).FullName!));
        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        Assert.True(diff.HasChanges);
        var result = gen.GenerateSql(diff);
        Assert.Equal(expectedType, ExtractColType(result, "Col"));
    }

    // ── 7. Full round-trip: all 6 types present ─────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void FullRoundTrip_AllSixScalarTypes_AllPresentInDDL(string provider)
    {
        var gen = CreateGenerator(provider);

        // Build a table with all 6 scalar types
        var table = new TableSchema
        {
            Name = "AllScalarsEntity",
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false },
                new ColumnSchema { Name = "Data", ClrType = typeof(byte[]).FullName!, IsNullable = true },
                new ColumnSchema { Name = "BirthDate", ClrType = typeof(DateOnly).FullName!, IsNullable = false },
                new ColumnSchema { Name = "StartTime", ClrType = typeof(TimeOnly).FullName!, IsNullable = false },
                new ColumnSchema { Name = "CreatedAt", ClrType = typeof(DateTimeOffset).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Duration", ClrType = typeof(TimeSpan).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Level", ClrType = typeof(Priority).FullName!, IsNullable = false }
            }
        };

        // Snapshot → Diff → SQL round-trip
        var oldSnap = new SchemaSnapshot();
        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(table);

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        Assert.True(diff.HasChanges, "Diff from empty to 7-column table must have changes");
        Assert.Single(diff.AddedTables);

        var result = gen.GenerateSql(diff);
        var createSql = result.Up.First(s => s.Contains("CREATE TABLE", StringComparison.OrdinalIgnoreCase));

        // Verify each column's expected type is present in the DDL
        var expectedTypes = GetExpectedTypesForProvider(provider);
        Assert.Equal(expectedTypes["byte[]"], ExtractColType(result, "Data"));
        Assert.Equal(expectedTypes["DateOnly"], ExtractColType(result, "BirthDate"));
        Assert.Equal(expectedTypes["TimeOnly"], ExtractColType(result, "StartTime"));
        Assert.Equal(expectedTypes["DateTimeOffset"], ExtractColType(result, "CreatedAt"));
        Assert.Equal(expectedTypes["TimeSpan"], ExtractColType(result, "Duration"));
        Assert.Equal(expectedTypes["Enum"], ExtractColType(result, "Level"));

        // Down migration must also be generated
        Assert.NotEmpty(result.Down);
        Assert.Contains(result.Down, s => s.Contains("DROP TABLE", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void FullRoundTrip_DiffIsSymmetric_DownReversesUp(string provider)
    {
        var gen = CreateGenerator(provider);

        var table = new TableSchema
        {
            Name = "ScalarRoundTrip",
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false },
                new ColumnSchema { Name = "Payload", ClrType = typeof(byte[]).FullName!, IsNullable = true },
                new ColumnSchema { Name = "When", ClrType = typeof(DateTimeOffset).FullName!, IsNullable = false },
                new ColumnSchema { Name = "Elapsed", ClrType = typeof(TimeSpan).FullName!, IsNullable = false }
            }
        };

        var emptySnap = new SchemaSnapshot();
        var fullSnap = new SchemaSnapshot();
        fullSnap.Tables.Add(table);

        // Forward diff: empty → full (creates table)
        var forwardDiff = SchemaDiffer.Diff(emptySnap, fullSnap);
        var forwardSql = gen.GenerateSql(forwardDiff);
        Assert.Contains(forwardSql.Up, s => s.Contains("CREATE TABLE", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(forwardSql.Down, s => s.Contains("DROP TABLE", StringComparison.OrdinalIgnoreCase));

        // Reverse diff: full → empty (drops table)
        var reverseDiff = SchemaDiffer.Diff(fullSnap, emptySnap);
        var reverseSql = gen.GenerateSql(reverseDiff);
        Assert.Contains(reverseSql.Up, s => s.Contains("DROP TABLE", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(reverseSql.Down, s => s.Contains("CREATE TABLE", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void FullRoundTrip_AddColumn_ScalarTypes_CorrectDDL(string provider)
    {
        var gen = CreateGenerator(provider);
        var expectedTypes = GetExpectedTypesForProvider(provider);

        // Before: table with only Id
        var beforeTable = new TableSchema
        {
            Name = "Expandable",
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false }
            }
        };

        // After: table with Id + all scalar types
        var afterTable = new TableSchema
        {
            Name = "Expandable",
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false },
                new ColumnSchema { Name = "Blob", ClrType = typeof(byte[]).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Day", ClrType = typeof(DateOnly).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Time", ClrType = typeof(TimeOnly).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Stamp", ClrType = typeof(DateTimeOffset).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Span", ClrType = typeof(TimeSpan).FullName!, IsNullable = true },
                new ColumnSchema { Name = "Status", ClrType = typeof(Priority).FullName!, IsNullable = true }
            }
        };

        var oldSnap = new SchemaSnapshot();
        oldSnap.Tables.Add(beforeTable);
        var newSnap = new SchemaSnapshot();
        newSnap.Tables.Add(afterTable);

        var diff = SchemaDiffer.Diff(oldSnap, newSnap);
        Assert.True(diff.HasChanges);
        Assert.Equal(6, diff.AddedColumns.Count);

        var result = gen.GenerateSql(diff);

        // Each ADD COLUMN statement must reference the correct type
        AssertAddColumnContainsType(result.Up, "Blob", expectedTypes["byte[]"]);
        AssertAddColumnContainsType(result.Up, "Day", expectedTypes["DateOnly"]);
        AssertAddColumnContainsType(result.Up, "Time", expectedTypes["TimeOnly"]);
        AssertAddColumnContainsType(result.Up, "Stamp", expectedTypes["DateTimeOffset"]);
        AssertAddColumnContainsType(result.Up, "Span", expectedTypes["TimeSpan"]);
        AssertAddColumnContainsType(result.Up, "Status", expectedTypes["Enum"]);
    }

    // ── Generator factory ───────────────────────────────────────────────────

    private static IMigrationSqlGenerator CreateGenerator(string provider) => provider switch
    {
        "sqlite" => new SqliteMigrationSqlGenerator(),
        "sqlserver" => new SqlServerMigrationSqlGenerator(),
        "mysql" => new MySqlMigrationSqlGenerator(),
        "postgres" => new PostgresMigrationSqlGenerator(),
        _ => throw new ArgumentException($"Unknown provider: {provider}")
    };

    private static Dictionary<string, string> GetExpectedTypesForProvider(string provider) => provider switch
    {
        "sqlite" => new Dictionary<string, string>
        {
            ["byte[]"] = "BLOB",
            ["DateOnly"] = "TEXT",
            ["TimeOnly"] = "TEXT",
            ["DateTimeOffset"] = "TEXT",
            ["TimeSpan"] = "TEXT",
            ["Enum"] = "INTEGER"
        },
        "sqlserver" => new Dictionary<string, string>
        {
            ["byte[]"] = "VARBINARY(MAX)",
            ["DateOnly"] = "DATE",
            ["TimeOnly"] = "TIME",
            ["DateTimeOffset"] = "DATETIMEOFFSET",
            ["TimeSpan"] = "TIME",
            ["Enum"] = "INT"
        },
        "mysql" => new Dictionary<string, string>
        {
            ["byte[]"] = "BLOB",
            ["DateOnly"] = "DATE",
            ["TimeOnly"] = "TIME",
            ["DateTimeOffset"] = "DATETIME",
            ["TimeSpan"] = "TIME",
            ["Enum"] = "INT"
        },
        "postgres" => new Dictionary<string, string>
        {
            ["byte[]"] = "BYTEA",
            ["DateOnly"] = "DATE",
            ["TimeOnly"] = "TIME",
            ["DateTimeOffset"] = "TIMESTAMPTZ",
            ["TimeSpan"] = "INTERVAL",
            ["Enum"] = "INTEGER"
        },
        _ => throw new ArgumentException($"Unknown provider: {provider}")
    };

    private static void AssertAddColumnContainsType(IReadOnlyList<string> upStatements, string colName, string expectedType)
    {
        // Find the ADD COLUMN statement for the given column name.
        // Different providers use different syntax:
        //   SQLite/MySQL/Postgres: ALTER TABLE ... ADD COLUMN ...
        //   SQL Server:            ALTER TABLE ... ADD ...
        var stmt = upStatements.FirstOrDefault(s =>
            s.Contains(colName, StringComparison.OrdinalIgnoreCase) &&
            (s.Contains("ADD COLUMN", StringComparison.OrdinalIgnoreCase) ||
             s.Contains("ALTER TABLE", StringComparison.OrdinalIgnoreCase)));

        // Fall back to checking CREATE TABLE statements (table recreation)
        if (stmt == null)
        {
            stmt = upStatements.FirstOrDefault(s =>
                s.Contains("CREATE TABLE", StringComparison.OrdinalIgnoreCase) &&
                s.Contains(colName, StringComparison.OrdinalIgnoreCase));
        }

        Assert.NotNull(stmt);
        Assert.Contains(expectedType, stmt, StringComparison.OrdinalIgnoreCase);
    }
}
