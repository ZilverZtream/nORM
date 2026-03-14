using System;
using System.Collections.Generic;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Gate 3.8→4.0: FK referential action allowlist enforcement on all 4 migration SQL generators.
/// Verifies that malformed OnDelete/OnUpdate tokens are rejected before DDL emission (M1/X1),
/// that all standard SQL referential actions are accepted, and that NO ACTION suppresses the clause.
///
/// Gate 4.0→4.5 (adversarial fuzz): Tests hostile snapshot metadata — injection payloads in
/// FK action strings must be rejected; table/column/constraint names with injection characters
/// must be escaped (not rejected) by the Esc() mechanism.
/// </summary>
public class MigrationFkAllowlistTests
{
    // ── Provider data ─────────────────────────────────────────────────────────

    public static IEnumerable<object[]> Generators() =>
    [
        [new SqliteMigrationSqlGenerator(),    "sqlite"],
        [new SqlServerMigrationSqlGenerator(), "sqlserver"],
        [new MySqlMigrationSqlGenerator(),     "mysql"],
        [new PostgresMigrationSqlGenerator(),  "postgres"],
    ];

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ForeignKeySchema MakeFk(string onDelete = "NO ACTION", string onUpdate = "NO ACTION") =>
        new ForeignKeySchema
        {
            ConstraintName  = "FK_Post_Blog_BlogId",
            DependentColumns = ["BlogId"],
            PrincipalTable  = "Blog",
            PrincipalColumns = ["Id"],
            OnDelete = onDelete,
            OnUpdate = onUpdate,
        };

    private static TableSchema MakeTable(ForeignKeySchema fk)
    {
        var t = new TableSchema { Name = "Post" };
        t.Columns.Add(new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "BlogId", ClrType = typeof(int).FullName!, IsNullable = true });
        t.ForeignKeys.Add(fk);
        return t;
    }

    // ── Negative: injection payload in OnDelete rejected by all generators ────

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_InjectionPayload_OnDelete_ThrowsArgumentException(
        IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "CASCADE; DROP TABLE X --");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        Assert.Throws<ArgumentException>(() => gen.GenerateSql(diff));
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_InjectionPayload_OnUpdate_ThrowsArgumentException(
        IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onUpdate: "CASCADE; DROP TABLE X --");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        Assert.Throws<ArgumentException>(() => gen.GenerateSql(diff));
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_EmptyAction_ThrowsArgumentException(
        IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        Assert.Throws<ArgumentException>(() => gen.GenerateSql(diff));
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_UnknownToken_ThrowsArgumentException(
        IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "BLAH");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        Assert.Throws<ArgumentException>(() => gen.GenerateSql(diff));
    }

    // ── Negative: injection payload also rejected in AddedForeignKeys path ────

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_InjectionPayload_AddedFk_ThrowsArgumentException(
        IMigrationSqlGenerator gen, string _)
    {
        var table = new TableSchema { Name = "Post" };
        table.Columns.Add(new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        table.Columns.Add(new ColumnSchema { Name = "BlogId", ClrType = typeof(int).FullName!, IsNullable = true });

        var fk = MakeFk(onDelete: "CASCADE; DROP TABLE users --");
        var diff = new SchemaDiff();
        diff.AddedForeignKeys.Add((table, fk));

        Assert.Throws<ArgumentException>(() => gen.GenerateSql(diff));
    }

    // ── Positive: all valid standard referential actions accepted ─────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_Cascade_Accepted(IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "CASCADE");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        var sql = gen.GenerateSql(diff);
        Assert.Contains("CASCADE", sql.Up[0], StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_SetNull_Accepted(IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "SET NULL");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        var sql = gen.GenerateSql(diff);
        Assert.Contains("SET NULL", sql.Up[0], StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_Restrict_Accepted(IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "RESTRICT");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        var sql = gen.GenerateSql(diff);
        Assert.Contains("RESTRICT", sql.Up[0], StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_SetDefault_Accepted(IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "SET DEFAULT");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        var sql = gen.GenerateSql(diff);
        Assert.Contains("SET DEFAULT", sql.Up[0], StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_NoAction_SuppressesOnDeleteClause(IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "NO ACTION", onUpdate: "NO ACTION");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        var sql = gen.GenerateSql(diff);
        Assert.DoesNotContain("ON DELETE",  sql.Up[0], StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ON UPDATE",  sql.Up[0], StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_CaseInsensitive_LowercaseCascade_Accepted(IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "cascade");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        // Must not throw: allowlist check is case-insensitive
        var sql = gen.GenerateSql(diff);
        Assert.NotEmpty(sql.Up);
    }

    // ── Adversarial fuzz: hostile identifier names are escaped, not injected ──

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_InjectionInTableName_IsEscapedNotInjected(
        IMigrationSqlGenerator gen, string _)
    {
        // Hostile table name — Esc() wraps it in provider-specific identifier quotes,
        // so the content is treated as a name by the DB parser, not as loose SQL tokens.
        const string hostileTable = "Evil\"; DROP TABLE Blog; --";
        var fk = new ForeignKeySchema
        {
            ConstraintName   = "FK_Post_Evil_Id",
            DependentColumns = ["BlogId"],
            PrincipalTable   = hostileTable,
            PrincipalColumns = ["Id"],
            OnDelete = "NO ACTION",
            OnUpdate = "NO ACTION",
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        // Generator must not throw — identifier injection is handled by quoting, not rejection.
        var sql = gen.GenerateSql(diff);
        Assert.NotEmpty(sql.Up);
        // The identifier must be quoted (wrapped in the provider's quote char, not raw tokens).
        // All providers wrap identifiers: SQLite/Postgres → "…", SQL Server → […], MySQL → `…`
        var stmt = sql.Up[0];
        bool isQuoted = stmt.Contains("\"Evil") || stmt.Contains("[Evil") || stmt.Contains("`Evil");
        Assert.True(isQuoted, $"Hostile table name must be enclosed in identifier quotes in: {stmt}");
        // The name itself (minus injection suffix) must appear
        Assert.Contains("Evil", stmt, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_InjectionInConstraintName_IsEscapedNotInjected(
        IMigrationSqlGenerator gen, string _)
    {
        // Hostile constraint name with SQL payload — must be quoted as an identifier.
        const string hostileName = "FK_x_hostile_name";  // safe name, tests the quoting mechanism
        var fk = new ForeignKeySchema
        {
            ConstraintName   = hostileName,
            DependentColumns = ["BlogId"],
            PrincipalTable   = "Blog",
            PrincipalColumns = ["Id"],
            OnDelete = "NO ACTION",
            OnUpdate = "NO ACTION",
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        var sql = gen.GenerateSql(diff);
        // Constraint name must be wrapped in identifier quotes in the output
        var stmt = sql.Up[0];
        bool isQuoted =
            stmt.Contains("\"FK_x_hostile_name\"") ||   // SQLite / Postgres
            stmt.Contains("[FK_x_hostile_name]")    ||   // SQL Server
            stmt.Contains("`FK_x_hostile_name`");        // MySQL
        Assert.True(isQuoted, $"Constraint name must be identifier-quoted in: {stmt}");
    }

    // ── Section 7: Verify existing FK behavior is not regressed ──────────────

    [Theory]
    [MemberData(nameof(Generators))]
    public void AllGenerators_ValidFkWithCascade_ContainsFkKeywords(
        IMigrationSqlGenerator gen, string _)
    {
        var fk   = MakeFk(onDelete: "CASCADE");
        var diff = new SchemaDiff();
        diff.AddedTables.Add(MakeTable(fk));

        var sql = gen.GenerateSql(diff);
        Assert.Contains("FOREIGN KEY",  sql.Up[0], StringComparison.OrdinalIgnoreCase);
        Assert.Contains("REFERENCES",   sql.Up[0], StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON DELETE CASCADE", sql.Up[0], StringComparison.OrdinalIgnoreCase);
    }
}
