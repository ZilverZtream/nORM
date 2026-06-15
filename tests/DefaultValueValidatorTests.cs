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
/// (numeric literals, single-quoted ANSI/Unicode strings, boolean literals, NULL, and a fixed set of
/// standard SQL no-argument functions) and throws <see cref="ArgumentException"/> on
/// anything else. All 9 interpolation points across the 4 generators now call it.
/// </summary>
[Xunit.Trait("Category", "Fast")]
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
        new object[] { "CURRENT_TIMESTAMP(7)" },
        new object[] { "CURRENT_TIMESTAMP(foo)" },
        new object[] { "NOW(6); DROP TABLE Users" },
        new object[] { "UTC_TIMESTAMP(6 /* comment */)" },
        new object[] { "'; EXEC xp_cmdshell('cmd')--" },
        new object[] { "0 UNION SELECT password FROM Users" },
        new object[] { "nextval('')" },
        new object[] { "nextval('public.orders; DROP')" },
        new object[] { "nextval('public.orders_id_seq'::text)" },
        new object[] { "nextval('public.orders_id_seq'); DROP TABLE Users" },
        new object[] { "0xDEADBEEF; DROP TABLE Users" },
        new object[] { "0x" },
        new object[] { "X'DEADBEEF'; DROP TABLE Users" },
        new object[] { "X'DEADZ0'" },
        new object[] { "X'DEA'" },
        new object[] { "B'102'" },
        new object[] { "B''" },
        new object[] { "B'1010'; DROP TABLE Users" },
        new object[] { "DATE '2026-00-15'" },
        new object[] { "TIME '25:00:00'" },
        new object[] { "TIMESTAMP '2026-06-15 12:34:56'; DROP TABLE Users" },
        new object[] { "INTERVAL '1 fortnight'" },
        new object[] { "INTERVAL '1 hour'; DROP TABLE Users" },
        new object[] { "now() AT TIME ZONE 'utc'; DROP TABLE Users" },
        new object[] { "now() AT TIME ZONE current_user" },
        new object[] { "timezone('utc', unsafe())" },
        new object[] { "timezone('utc', now()); DROP TABLE Users" },
        new object[] { "'active'::text; DROP TABLE Users" },
        new object[] { "'active'::text -- comment" },
        new object[] { "0::integer /* comment */" },
        new object[] { "now()::timestamp without time zone; DELETE FROM Users" },
        new object[] { "'active'::\"quoted\"" },
        new object[] { "lower(Status)" },
        new object[] { "lower('active'::\"quoted\")" },
        new object[] { "lower('active'::text); DROP TABLE Users" },
        new object[] { "lower(_utf8mb4 Status)" },
    };

    [Theory]
    [MemberData(nameof(AdversarialPayloads))]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
        new object[] { "0xDEADBEEF" },
        new object[] { "0x0" },
        new object[] { "X'DEADBEEF'" },
        new object[] { "x''" },
        new object[] { "B'0'" },
        new object[] { "b'1010'" },
        new object[] { "B'1010'::bit(4)" },
        new object[] { "DATE '2026-06-15'" },
        new object[] { "TIME '12:34:56'" },
        new object[] { "TIME WITH TIME ZONE '12:34:56+02:00'" },
        new object[] { "TIMESTAMP '2026-06-15 12:34:56'" },
        new object[] { "TIMESTAMP WITH TIME ZONE '2026-06-15 12:34:56+02:00'" },
        new object[] { "INTERVAL '1 hour 30 minutes'" },
        new object[] { "NULL" },
        new object[] { "null" },
        new object[] { "TRUE" },
        new object[] { "FALSE" },
        new object[] { "true" },
        new object[] { "false" },
        new object[] { "'hello'" },
        new object[] { "'it''s ok'" },
        new object[] { "N'hello'" },
        new object[] { "n'it''s ok'" },
        new object[] { "''" },
        new object[] { "CURRENT_TIMESTAMP" },
        new object[] { "current_timestamp" },
        new object[] { "CURRENT_TIMESTAMP()" },
        new object[] { "CURRENT_TIMESTAMP(6)" },
        new object[] { "current_timestamp()" },
        new object[] { "CURRENT_DATE" },
        new object[] { "CURRENT_DATE()" },
        new object[] { "CURRENT_TIME" },
        new object[] { "CURRENT_TIME()" },
        new object[] { "CURRENT_TIME(6)" },
        new object[] { "LOCALTIME(6)" },
        new object[] { "LOCALTIMESTAMP(6)" },
        new object[] { "NOW()" },
        new object[] { "NOW(6)" },
        new object[] { "GETDATE()" },
        new object[] { "GETUTCDATE()" },
        new object[] { "NEWID()" },
        new object[] { "NEWSEQUENTIALID()" },
        new object[] { "UUID()" },
        new object[] { "GEN_RANDOM_UUID()" },
        new object[] { "UUID_GENERATE_V4()" },
        new object[] { "SYSDATE()" },
        new object[] { "SYSDATE(6)" },
        new object[] { "SYSDATETIME()" },
        new object[] { "SYSUTCDATETIME()" },
        new object[] { "SYSDATETIMEOFFSET()" },
        new object[] { "UTC_TIMESTAMP()" },
        new object[] { "UTC_TIMESTAMP(6)" },
        new object[] { "CLOCK_TIMESTAMP()" },
        new object[] { "TRANSACTION_TIMESTAMP()" },
        new object[] { "now() AT TIME ZONE 'utc'" },
        new object[] { "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" },
        new object[] { "CURRENT_TIMESTAMP(6) AT TIME ZONE 'utc'::text" },
        new object[] { "timezone('utc', now())" },
        new object[] { "timezone('utc'::text, CURRENT_TIMESTAMP)" },
        new object[] { "nextval('orders_id_seq')" },
        new object[] { "NEXTVAL( 'public.orders_id_seq'::regclass )" },
        new object[] { "'active'::text" },
        new object[] { "'draft'::character varying" },
        new object[] { "'draft'::character varying(32)" },
        new object[] { "'{}'::jsonb" },
        new object[] { "'\\xDEADBEEF'::bytea" },
        new object[] { "'00000000-0000-0000-0000-000000000000'::uuid" },
        new object[] { "42::integer" },
        new object[] { "3.14::numeric(10, 2)" },
        new object[] { "true::boolean" },
        new object[] { "now()::timestamp without time zone" },
        new object[] { "CURRENT_TIMESTAMP::timestamp with time zone" },
        new object[] { "_utf8mb4'active'" },
        new object[] { "lower('NEW')" },
        new object[] { "UPPER(N'pending')" },
        new object[] { "lower('NEW'::text)" },
        new object[] { "upper('pending'::character varying)" },
        new object[] { "lower(_utf8mb4'NEW')" },
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public void SqliteGenerator_AddColumn_AdversarialDefault_Throws(string payload)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", payload));
        Assert.Throws<ArgumentException>(() =>
            new SqliteMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [InlineData("0; DELETE FROM Migrations")]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public void PostgresGenerator_AddColumn_AdversarialDefault_Throws(string payload)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", payload));
        Assert.Throws<ArgumentException>(() =>
            new PostgresMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [InlineData("0; DELETE FROM Migrations")]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public void SqlServerGenerator_AddColumn_AdversarialDefault_Throws(string payload)
    {
        var diff = AddColumnDiff("T", NotNullCol("NewCol", payload));
        Assert.Throws<ArgumentException>(() =>
            new SqlServerMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [InlineData("0; DELETE FROM Migrations")]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public void PostgresGenerator_AlterColumn_AdversarialNewDefault_Throws(string payload)
    {
        var diff = AlterDefaultDiff("T", "Col", typeof(string).FullName!, "'old'", payload);
        Assert.Throws<ArgumentException>(() =>
            new PostgresMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public void PostgresGenerator_AlterColumn_AdversarialOldDefault_ThrowsOnDown(string payload)
    {
        // old default is adversarial — this affects the Down migration.
        var diff = AlterDefaultDiff("T", "Col", typeof(string).FullName!, payload, "'new'");
        Assert.Throws<ArgumentException>(() =>
            new PostgresMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public void MySqlGenerator_AlterColumn_AdversarialNewDefault_Throws(string payload)
    {
        var diff = AlterDefaultDiff("T", "Col", typeof(string).FullName!, "'old'", payload);
        Assert.Throws<ArgumentException>(() =>
            new MySqlMigrationSqlGenerator().GenerateSql(diff));
    }

    [Theory]
    [InlineData("'; DROP TABLE Payload--")]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
    [InlineData("X'DEADBEEF'")]
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
    [InlineData("'\\xDEADBEEF'::bytea")]
    [InlineData("B'1010'::bit(4)")]
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
    [InlineData("0xDEADBEEF")]
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
    [InlineData("0xDEADBEEF")]
    [InlineData("b'1010'")]
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
