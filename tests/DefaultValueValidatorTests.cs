using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// M1 — DefaultValue SQL injection in migration generators (Gate 4.0→4.5)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that <c>DefaultValueValidator</c> rejects adversarial SQL payloads and
/// accepts all legitimate default value forms. Also exercises all four migration SQL
/// generators end-to-end to confirm the validator is called before interpolation.
///
/// M1 root cause: all migration generators interpolated <c>ColumnSchema.DefaultValue</c>
/// directly into DDL SQL strings without validation, allowing an attacker-controlled value
/// (e.g., from a mis-configured schema diff) to inject arbitrary SQL into migration scripts.
///
/// Fix: <c>DefaultValueValidator.Validate()</c> checks the value against a strict allowlist
/// (numeric literals, single-quoted strings, boolean literals, NULL, and a fixed set of
/// standard SQL no-argument functions) and throws <see cref="ArgumentException"/> on
/// anything else. All 9 interpolation points across the 4 generators now call it.
/// </summary>
public class DefaultValueValidatorTests
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ColumnSchema NotNullCol(string name, string defaultValue) =>
        new ColumnSchema
        {
            Name = name,
            ClrType = typeof(string).FullName!,
            IsNullable = false,
            DefaultValue = defaultValue
        };

    private static ColumnSchema NullableCol(string name) =>
        new ColumnSchema
        {
            Name = name,
            ClrType = typeof(string).FullName!,
            IsNullable = true
        };

    private static TableSchema MakeTable(string name, params ColumnSchema[] cols)
    {
        var t = new TableSchema { Name = name };
        foreach (var c in cols) t.Columns.Add(c);
        return t;
    }

    // Produce a SchemaDiff with a single "added column" on an existing table.
    private static SchemaDiff AddColumnDiff(string tableName, ColumnSchema col)
    {
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((MakeTable(tableName, col), col));
        return diff;
    }

    // Produce a SchemaDiff with a single "altered column" that changes DefaultValue.
    private static SchemaDiff AlterDefaultDiff(
        string tableName, string colName, string clrType,
        string? oldDefault, string? newDefault)
    {
        var oldCol = new ColumnSchema { Name = colName, ClrType = clrType, IsNullable = true, DefaultValue = oldDefault };
        var newCol = new ColumnSchema { Name = colName, ClrType = clrType, IsNullable = true, DefaultValue = newDefault };
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((MakeTable(tableName, oldCol), newCol, oldCol));
        return diff;
    }

    // ── DefaultValueValidator: adversarial payloads are rejected ─────────────

    public static IEnumerable<object[]> AdversarialPayloads => new[]
    {
        new object[] { "'; DROP TABLE Users--" },
        new object[] { "0; DROP TABLE Users" },
        new object[] { "1 OR 1=1" },
        new object[] { "/* comment */ 0" },
        new object[] { "-- comment\n0" },
        new object[] { "SELECT 1" },
        new object[] { "INSERT INTO T VALUES(1)" },
        new object[] { "GETDATE() OR 1=1" },
        new object[] { "CURRENT_TIMESTAMP; DELETE FROM Users" },
        new object[] { "'; EXEC xp_cmdshell('cmd')--" },
        new object[] { "0 UNION SELECT password FROM Users" },
    };

    [Theory]
    [MemberData(nameof(AdversarialPayloads))]
    public void Validate_AdversarialPayload_ThrowsArgumentException(string payload)
    {
        var ex = Record.Exception(() => InvokeValidator(payload));
        Assert.NotNull(ex);
        Assert.IsType<ArgumentException>(ex);
    }

    // ── DefaultValueValidator: legitimate values are accepted ─────────────────

    public static IEnumerable<object[]> LegitimateValues => new[]
    {
        new object[] { "0" },
        new object[] { "1" },
        new object[] { "-1" },
        new object[] { "42" },
        new object[] { "3.14" },
        new object[] { "-99.9" },
        new object[] { "NULL" },
        new object[] { "null" },
        new object[] { "TRUE" },
        new object[] { "FALSE" },
        new object[] { "true" },
        new object[] { "false" },
        new object[] { "'hello'" },
        new object[] { "'it''s ok'" },
        new object[] { "''" },
        new object[] { "CURRENT_TIMESTAMP" },
        new object[] { "current_timestamp" },
        new object[] { "CURRENT_DATE" },
        new object[] { "CURRENT_TIME" },
        new object[] { "NOW()" },
        new object[] { "GETDATE()" },
        new object[] { "GETUTCDATE()" },
        new object[] { "NEWID()" },
        new object[] { "NEWSEQUENTIALID()" },
        new object[] { "UUID()" },
        new object[] { "GEN_RANDOM_UUID()" },
        new object[] { "SYSDATE()" },
        new object[] { "SYSDATETIME()" },
    };

    [Theory]
    [MemberData(nameof(LegitimateValues))]
    public void Validate_LegitimateValue_DoesNotThrow(string value)
    {
        var result = Record.Exception(() => InvokeValidator(value));
        Assert.Null(result);
    }

    [Fact]
    public void Validate_Null_ReturnsNull()
    {
        // null means "no DEFAULT clause" — must pass through without throwing.
        var result = InvokeValidator(null);
        Assert.Null(result);
    }

    // ── Per-generator end-to-end: adversarial payload rejected at DDL time ────

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [InlineData("0; DELETE FROM Migrations")]
    [InlineData("SELECT secret FROM vault")]
    public void SqliteGenerator_AddColumn_AdversarialDefault_Throws(string payload)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", payload));
        Assert.Throws<ArgumentException>(() =>
            new SqliteMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [InlineData("0; DELETE FROM Migrations")]
    public void PostgresGenerator_AddColumn_AdversarialDefault_Throws(string payload)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", payload));
        Assert.Throws<ArgumentException>(() =>
            new PostgresMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [InlineData("0; DELETE FROM Migrations")]
    public void SqlServerGenerator_AddColumn_AdversarialDefault_Throws(string payload)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", payload));
        Assert.Throws<ArgumentException>(() =>
            new SqlServerMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [InlineData("0; DELETE FROM Migrations")]
    public void MySqlGenerator_AddColumn_AdversarialDefault_Throws(string payload)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", payload));
        Assert.Throws<ArgumentException>(() =>
            new MySqlMigrationSqlGenerator().GenerateSql(diff));
    }

    // ── AlteredColumns path: adversarial payload in new or old default ────────

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [InlineData("0; SELECT * FROM secrets")]
    public void PostgresGenerator_AlterColumn_AdversarialNewDefault_Throws(string payload)
    {
        var diff = AlterDefaultDiff("T", "Col", typeof(string).FullName!, "'old'", payload);
        Assert.Throws<ArgumentException>(() =>
            new PostgresMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    public void PostgresGenerator_AlterColumn_AdversarialOldDefault_ThrowsOnDown(string payload)
    {
        // old default is adversarial — this affects the Down migration.
        var diff = AlterDefaultDiff("T", "Col", typeof(string).FullName!, payload, "'new'");
        Assert.Throws<ArgumentException>(() =>
            new PostgresMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    public void MySqlGenerator_AlterColumn_AdversarialNewDefault_Throws(string payload)
    {
        var diff = AlterDefaultDiff("T", "Col", typeof(string).FullName!, "'old'", payload);
        Assert.Throws<ArgumentException>(() =>
            new MySqlMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    public void SqlServerGenerator_AlterColumn_AdversarialNewDefault_Throws(string payload)
    {
        var diff = AlterDefaultDiff("T", "Col", typeof(string).FullName!, "'old'", payload);
        Assert.Throws<ArgumentException>(() =>
            new SqlServerMigrationSqlGenerator().GenerateSql(diff));
    }

    // ── Per-generator: legitimate defaults flow through without modification ──

    [Theory]
    [InlineData("0")]
    [InlineData("'default_val'")]
    [InlineData("CURRENT_TIMESTAMP")]
    public void SqliteGenerator_AddColumn_LegitimateDefault_SqlContainsValue(string defaultValue)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", defaultValue));
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains(defaultValue));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("'default_val'")]
    [InlineData("NOW()")]
    public void PostgresGenerator_AddColumn_LegitimateDefault_SqlContainsValue(string defaultValue)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", defaultValue));
        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains(defaultValue));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("'default_val'")]
    [InlineData("GETDATE()")]
    public void SqlServerGenerator_AddColumn_LegitimateDefault_SqlContainsValue(string defaultValue)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", defaultValue));
        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains(defaultValue));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("'default_val'")]
    [InlineData("NOW()")]
    public void MySqlGenerator_AddColumn_LegitimateDefault_SqlContainsValue(string defaultValue)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", defaultValue));
        var sql = new MySqlMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains(sql.Up, s => s.Contains(defaultValue));
    }

    // ── Private reflection accessor (DefaultValueValidator is internal) ───────

    private static string? InvokeValidator(string? value)
    {
        // Access internal static method via reflection.
        var method = typeof(SqliteMigrationSqlGenerator).Assembly
            .GetType("nORM.Migration.DefaultValueValidator")!
            .GetMethod("Validate",
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic |
                System.Reflection.BindingFlags.Public)!;
        try
        {
            return (string?)method.Invoke(null, new object?[] { value });
        }
        catch (System.Reflection.TargetInvocationException tie)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo
                .Capture(tie.InnerException!).Throw();
            throw; // unreachable
        }
    }
}
