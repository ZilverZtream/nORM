using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that migration SQL generators correctly escape metadata names
/// (table names, column names) in all contexts — identifiers and string literals —
/// across all four providers. Tests use adversarial names containing characters
/// that would break SQL syntax if left unescaped.
/// </summary>
public class MigrationMetadataEscapingTests
{
    private static TableSchema BuildTable(string name, params ColumnSchema[] columns)
    {
        var t = new TableSchema { Name = name };
        foreach (var c in columns) t.Columns.Add(c);
        return t;
    }

    private static ColumnSchema Col(string name, string clrType = "System.String", bool nullable = true)
        => new ColumnSchema { Name = name, ClrType = clrType, IsNullable = nullable };

    private static ColumnSchema ColPk(string name)
        => new ColumnSchema { Name = name, ClrType = "System.Int32", IsNullable = false, IsPrimaryKey = true };

    // ── SQL Server: literal-context escaping (M1 / X2) ────────────────────

    /// <summary>
    /// A table name containing a single quote must be escaped as '' inside the
    /// OBJECT_ID('{table}') string literal so the generated DECLARE statement
    /// is syntactically valid SQL.
    /// </summary>
    [Fact]
    public void SqlServer_AlteredDefault_TableNameWithSingleQuote_LiteralEscaped()
    {
        var table = BuildTable("O'Brien", ColPk("Id"));
        var oldCol = Col("Status", "System.Int32", false);
        oldCol = new ColumnSchema { Name = oldCol.Name, ClrType = oldCol.ClrType, IsNullable = false, DefaultValue = "0" };
        var newCol = new ColumnSchema { Name = "Status", ClrType = "System.Int32", IsNullable = false, DefaultValue = "1" };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var declareStmt = sql.Up.First(s => s.StartsWith("DECLARE"));

        // The literal must contain escaped single quote: O''Brien
        Assert.Contains("O''Brien", declareStmt);

        // The literal must not contain a raw unescaped single quote breaking out of the literal
        // e.g., the literal context is OBJECT_ID('...') — count quotes to confirm they are balanced
        AssertSingleQuotesBalancedOutsideBrackets(declareStmt);
    }

    /// <summary>
    /// A column name containing a single quote must be escaped inside the
    /// COL_NAME(...) = '{col}' string literal.
    /// </summary>
    [Fact]
    public void SqlServer_AlteredDefault_ColumnNameWithSingleQuote_LiteralEscaped()
    {
        var table = BuildTable("Products", ColPk("Id"));
        var oldCol = new ColumnSchema { Name = "It's_Status", ClrType = "System.Int32", IsNullable = false, DefaultValue = "0" };
        var newCol = new ColumnSchema { Name = "It's_Status", ClrType = "System.Int32", IsNullable = false, DefaultValue = "1" };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var declareStmt = sql.Up.First(s => s.StartsWith("DECLARE"));

        // The column literal must be escaped: It''s_Status
        Assert.Contains("It''s_Status", declareStmt);
        AssertSingleQuotesBalancedOutsideBrackets(declareStmt);
    }

    /// <summary>
    /// Both table name and column name containing single quotes must be escaped
    /// in the down migration's DECLARE statement as well.
    /// </summary>
    [Fact]
    public void SqlServer_AlteredDefault_DownMigration_BothNamesEscaped()
    {
        var table = BuildTable("O'Brien's", ColPk("Id"));
        var oldCol = new ColumnSchema { Name = "Col'Name", ClrType = "System.Int32", IsNullable = false, DefaultValue = "0" };
        var newCol = new ColumnSchema { Name = "Col'Name", ClrType = "System.Int32", IsNullable = false, DefaultValue = "99" };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var downDeclare = sql.Down.First(s => s.StartsWith("DECLARE"));

        Assert.Contains("O''Brien''s", downDeclare);
        Assert.Contains("Col''Name", downDeclare);
        AssertSingleQuotesBalancedOutsideBrackets(downDeclare);
    }

    /// <summary>
    /// A table name containing a single-quote-injection sequence must have the quote
    /// doubled inside the OBJECT_ID() literal context so the literal cannot be
    /// broken out of. The raw name may appear in the @variable declaration (a T-SQL
    /// syntax error but not an injection vector), but the literal-context occurrences
    /// must be properly escaped.
    /// </summary>
    [Fact]
    public void SqlServer_AlteredDefault_AdversarialTableName_LiteralContextEscaped()
    {
        var table = BuildTable("O'Brien"); // table name with single quote
        var oldCol = new ColumnSchema { Name = "Val", ClrType = "System.Int32", IsNullable = true, DefaultValue = "0" };
        var newCol = new ColumnSchema { Name = "Val", ClrType = "System.Int32", IsNullable = true, DefaultValue = "1" };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var declareStmt = sql.Up.First(s => s.StartsWith("DECLARE"));

        // The OBJECT_ID literal must contain the escaped version: O''Brien
        Assert.Contains("OBJECT_ID('O''Brien')", declareStmt);

        // All single quotes in the statement outside square-bracket identifiers must be balanced
        AssertSingleQuotesBalancedOutsideBrackets(declareStmt);
    }

    // ── 4.5→5.0: Adversarial metadata fuzzing across all four providers ───

    [Theory]
    [InlineData("Table'With'Quotes")]
    [InlineData("Table\"With\"DblQuotes")]
    [InlineData("Table`WithBacktick")]
    [InlineData("Table]WithBracket")]
    [InlineData("Table--Comment")]
    [InlineData("Table; DROP TABLE X; --")]
    [InlineData("Table\nWith\nNewline")]
    [InlineData("TableWith\u0000Null")]
    public void AllProviders_AdversarialTableName_GeneratesWithoutException(string tableName)
    {
        var table = BuildTable(tableName, ColPk("Id"));
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var generators = AllGenerators();

        foreach (var (name, gen) in generators)
        {
            var ex = Record.Exception(() => gen.GenerateSql(diff));
            Assert.True(ex == null, $"{name}: threw {ex?.GetType().Name}: {ex?.Message} for table '{tableName}'");
        }
    }

    [Theory]
    [InlineData("Col'With'Quotes")]
    [InlineData("Col\"DblQuotes")]
    [InlineData("Col`Backtick")]
    [InlineData("Col]Bracket")]
    [InlineData("Col--Injection")]
    [InlineData("'; DROP TABLE T; --")]
    public void AllProviders_AdversarialColumnName_GeneratesWithoutException(string colName)
    {
        var table = BuildTable("SafeTable", ColPk("Id"), Col(colName));
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var generators = AllGenerators();

        foreach (var (name, gen) in generators)
        {
            var ex = Record.Exception(() => gen.GenerateSql(diff));
            Assert.True(ex == null, $"{name}: threw {ex?.GetType().Name}: {ex?.Message} for column '{colName}'");
        }
    }

    /// <summary>
    /// SQL Server CREATE TABLE with adversarial table name must have balanced
    /// square-bracket escaping in the identifier context.
    /// </summary>
    [Theory]
    [InlineData("Table]With]Brackets")]
    [InlineData("A]]B]]C")]
    [InlineData("]Leading")]
    [InlineData("Trailing]")]
    public void SqlServer_AdversarialBracketInName_IdentifierCorrectlyEscaped(string tableName)
    {
        var table = BuildTable(tableName, ColPk("Id"));
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.Contains("CREATE TABLE"));

        // Every ] in the original name should become ]] in the output identifier
        var escaped = tableName.Replace("]", "]]");
        Assert.Contains($"[{escaped}]", createStmt);
    }

    /// <summary>
    /// MySQL CREATE TABLE with adversarial backtick in name must produce
    /// correct double-backtick escaping.
    /// </summary>
    [Theory]
    [InlineData("Table`With`Backtick")]
    [InlineData("``Leading")]
    [InlineData("Trailing``")]
    public void MySql_AdversarialBacktickInName_IdentifierCorrectlyEscaped(string tableName)
    {
        var table = BuildTable(tableName, ColPk("Id"));
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new MySqlMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.Contains("CREATE TABLE"));

        var escaped = tableName.Replace("`", "``");
        Assert.Contains($"`{escaped}`", createStmt);
    }

    /// <summary>
    /// Postgres CREATE TABLE with adversarial double-quote in name must produce
    /// correct double-double-quote escaping.
    /// </summary>
    [Theory]
    [InlineData("Table\"With\"DblQuotes")]
    [InlineData("\"\"Leading")]
    [InlineData("Trailing\"\"")]
    public void Postgres_AdversarialDoubleQuoteInName_IdentifierCorrectlyEscaped(string tableName)
    {
        var table = BuildTable(tableName, ColPk("Id"));
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.Contains("CREATE TABLE"));

        var escaped = tableName.Replace("\"", "\"\"");
        Assert.Contains($"\"{escaped}\"", createStmt);
    }

    /// <summary>
    /// SQLite CREATE TABLE with adversarial double-quote in name must produce
    /// correct double-double-quote escaping.
    /// </summary>
    [Theory]
    [InlineData("Table\"With\"DblQuotes")]
    [InlineData("\"\"Leading")]
    public void Sqlite_AdversarialDoubleQuoteInName_IdentifierCorrectlyEscaped(string tableName)
    {
        var table = BuildTable(tableName, ColPk("Id"));
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createStmt = sql.Up.First(s => s.Contains("CREATE TABLE"));

        var escaped = tableName.Replace("\"", "\"\"");
        Assert.Contains($"\"{escaped}\"", createStmt);
    }

    /// <summary>
    /// ALTER TABLE with adversarial column names must produce correctly escaped
    /// DDL across all four providers.
    /// </summary>
    [Theory]
    [InlineData("Col'Name")]
    [InlineData("Col]Name")]
    [InlineData("Col`Name")]
    [InlineData("Col\"Name")]
    public void AllProviders_AdversarialColumnInAlterColumn_NoException(string colName)
    {
        var table = BuildTable("SafeTable", ColPk("Id"));
        var oldCol = new ColumnSchema { Name = colName, ClrType = "System.String", IsNullable = true };
        var newCol = new ColumnSchema { Name = colName, ClrType = "System.String", IsNullable = false, DefaultValue = "''" };

        // SQL Server throws if NOT NULL without DefaultValue on AddedColumns; use AlteredColumns
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        foreach (var (name, gen) in AllGenerators())
        {
            var ex = Record.Exception(() => gen.GenerateSql(diff));
            Assert.True(ex == null, $"{name}: threw {ex?.GetType().Name}: {ex?.Message} for column '{colName}'");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static IEnumerable<(string Name, IMigrationSqlGenerator Gen)> AllGenerators()
    {
        yield return ("SqlServer", new SqlServerMigrationSqlGenerator());
        yield return ("MySQL",     new MySqlMigrationSqlGenerator());
        yield return ("Postgres",  new PostgresMigrationSqlGenerator());
        yield return ("SQLite",    new SqliteMigrationSqlGenerator());
    }

    /// <summary>
    /// Asserts that single quotes outside of square-bracket-delimited identifiers
    /// are balanced (even count). This detects literal break-outs caused by
    /// unescaped single quotes in string-literal SQL contexts.
    /// </summary>
    private static void AssertSingleQuotesBalancedOutsideBrackets(string sql)
    {
        int quoteCount = 0;
        bool inBracket = false;
        for (int i = 0; i < sql.Length; i++)
        {
            char c = sql[i];
            if (c == '[' && !inBracket) { inBracket = true; continue; }
            if (c == ']' && inBracket)
            {
                // Handle escaped ]] inside brackets
                if (i + 1 < sql.Length && sql[i + 1] == ']') { i++; continue; }
                inBracket = false;
                continue;
            }
            if (!inBracket && c == '\'') quoteCount++;
        }
        Assert.True(quoteCount % 2 == 0,
            $"Unbalanced single quotes ({quoteCount}) in SQL fragment: {sql}");
    }
}
