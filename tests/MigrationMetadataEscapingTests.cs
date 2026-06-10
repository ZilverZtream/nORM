using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that migration SQL generators correctly escape metadata names
/// (table names, column names) in all contexts — identifiers and string literals —
/// across all four providers. Tests use adversarial names containing characters
/// that would break SQL syntax if left unescaped.
/// </summary>
[Xunit.Trait("Category", "Fast")]
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
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

    [Fact]
    public void AllProviders_QualifiedTableName_EscapesEachPartForCreateTable()
    {
        var table = BuildTable("tenant.Users", ColPk("Id"));
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        foreach (var (name, gen) in AllGenerators())
        {
            var createStmt = gen.GenerateSql(diff).Up.First(s => s.StartsWith("CREATE TABLE"));
            var expected = name switch
            {
                "SqlServer" => "CREATE TABLE [tenant].[Users]",
                "MySQL"     => "CREATE TABLE `tenant`.`Users`",
                "Postgres"  => "CREATE TABLE \"tenant\".\"Users\"",
                "SQLite"    => "CREATE TABLE \"tenant\".\"Users\"",
                _           => throw new InvalidOperationException(name)
            };

            Assert.Contains(expected, createStmt);
        }
    }

    [Fact]
    public void SqlServer_QualifiedAddedTable_EnsuresSchemaBeforeCreateTable()
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BuildTable("tenant.Users", ColPk("Id")));
        diff.AddedTables.Add(BuildTable("tenant.Orders", ColPk("Id")));

        var up = new SqlServerMigrationSqlGenerator().GenerateSql(diff).Up;

        Assert.Equal("IF SCHEMA_ID(N'tenant') IS NULL EXEC(N'CREATE SCHEMA [tenant]')", up[0]);
        Assert.Single(up.Where(s => s.StartsWith("IF SCHEMA_ID", StringComparison.Ordinal)));
        Assert.Contains(up.Skip(1), s => s.StartsWith("CREATE TABLE [tenant].[Users]", StringComparison.Ordinal));
        Assert.Contains(up.Skip(1), s => s.StartsWith("CREATE TABLE [tenant].[Orders]", StringComparison.Ordinal));
    }

    [Fact]
    public void SqlServer_QualifiedAddedTable_EnsureSchemaEscapesIdentifierAndLiteral()
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BuildTable("O'Brien].Users", ColPk("Id")));

        var ensureSchema = new SqlServerMigrationSqlGenerator().GenerateSql(diff).Up[0];

        Assert.Contains("SCHEMA_ID(N'O''Brien]')", ensureSchema);
        Assert.Contains("CREATE SCHEMA [O''Brien]]]", ensureSchema);
    }

    [Fact]
    public void Postgres_QualifiedAddedTable_EnsuresSchemaBeforeCreateTable()
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(BuildTable("tenant.Users", ColPk("Id")));

        var up = new PostgresMigrationSqlGenerator().GenerateSql(diff).Up;

        Assert.Equal("CREATE SCHEMA IF NOT EXISTS \"tenant\"", up[0]);
        Assert.Contains(up.Skip(1), s => s.StartsWith("CREATE TABLE \"tenant\".\"Users\"", StringComparison.Ordinal));
    }

    [Fact]
    public void Postgres_QualifiedTableName_QualifiesIndexNames()
    {
        var table = BuildTable(
            "tenant.Users",
            ColPk("Id"),
            new ColumnSchema
            {
                Name = "Email",
                ClrType = "System.String",
                IsNullable = false,
                IndexName = "IX_Users_Email"
            });
        table.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_Users_LowerEmail",
            ExpressionSql = "lower(\"Email\")"
        });

        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var up = new PostgresMigrationSqlGenerator().GenerateSql(diff).Up;

        Assert.Contains(up, s => s.StartsWith("CREATE INDEX \"tenant\".\"IX_Users_Email\" ON \"tenant\".\"Users\"", StringComparison.Ordinal));
        Assert.Contains(up, s => s.StartsWith("CREATE INDEX \"tenant\".\"IX_Users_LowerEmail\" ON \"tenant\".\"Users\"", StringComparison.Ordinal));
    }

    [Fact]
    public void Sqlite_QualifiedTableName_QualifiesIndexNameAndLeavesTargetTableUnqualified()
    {
        var table = BuildTable(
            "tenant.Users",
            ColPk("Id"),
            new ColumnSchema
            {
                Name = "Email",
                ClrType = "System.String",
                IsNullable = false,
                IndexName = "IX_Users_Email"
            });
        table.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_Users_LowerEmail",
            ExpressionSql = "lower(Email)"
        });

        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var up = new SqliteMigrationSqlGenerator().GenerateSql(diff).Up;

        Assert.Contains(up, s => s.StartsWith("CREATE INDEX \"tenant\".\"IX_Users_Email\" ON \"Users\"", StringComparison.Ordinal));
        Assert.Contains(up, s => s.StartsWith("CREATE INDEX \"tenant\".\"IX_Users_LowerEmail\" ON \"Users\"", StringComparison.Ordinal));
        Assert.DoesNotContain(up, s => s.StartsWith("CREATE INDEX", StringComparison.Ordinal) && s.Contains("ON \"tenant\".\"Users\"", StringComparison.Ordinal));
    }

    [Fact]
    public void Sqlite_QualifiedTableWithForeignKeyAndIndexes_GeneratedDdlExecutesInAttachedSchema()
    {
        var users = BuildTable(
            "tenant.Users",
            ColPk("Id"),
            new ColumnSchema
            {
                Name = "Email",
                ClrType = "System.String",
                IsNullable = false,
                IndexName = "IX_Users_Email"
            });
        users.ExpressionIndexes.Add(new ExpressionIndexSchema
        {
            Name = "IX_Users_LowerEmail",
            ExpressionSql = "lower(Email)"
        });

        var orders = BuildTable(
            "tenant.Orders",
            ColPk("Id"),
            new ColumnSchema { Name = "UserId", ClrType = "System.Int32", IsNullable = false });
        orders.ForeignKeys.Add(new ForeignKeySchema
        {
            ConstraintName = "FK_Orders_Users",
            DependentColumns = new[] { "UserId" },
            PrincipalTable = "tenant.Users",
            PrincipalColumns = new[] { "Id" }
        });

        var diff = new SchemaDiff();
        diff.AddedTables.Add(users);
        diff.AddedTables.Add(orders);
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        ExecuteSqlite(connection, "PRAGMA foreign_keys=ON");
        ExecuteSqlite(connection, "ATTACH DATABASE ':memory:' AS tenant");
        foreach (var statement in sql.Up)
            ExecuteSqlite(connection, statement);

        using var fkCommand = connection.CreateCommand();
        fkCommand.CommandText = "PRAGMA tenant.foreign_key_list(Orders)";
        using var fkReader = fkCommand.ExecuteReader();
        Assert.True(fkReader.Read());
        Assert.Equal("Users", fkReader["table"]);

        using var indexCommand = connection.CreateCommand();
        indexCommand.CommandText = "SELECT COUNT(*) FROM tenant.sqlite_master WHERE type = 'index' AND name IN ('IX_Users_Email', 'IX_Users_LowerEmail')";
        Assert.Equal(2L, (long)indexCommand.ExecuteScalar()!);
    }

    [Fact]
    public void Sqlite_AddedColumnWithAddedIndex_DownExecutesAfterTableRecreationDropsIndex()
    {
        var table = BuildTable(
            "Posts",
            ColPk("Id"),
            new ColumnSchema { Name = "Name", ClrType = "System.String", IsNullable = false },
            new ColumnSchema { Name = "Slug", ClrType = "System.String", IsNullable = true, IndexName = "IX_Posts_Slug" });
        var slug = table.Columns.Single(column => column.Name == "Slug");
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, slug));
        diff.AddedIndexes.Add((table, "IX_Posts_Slug", false, new[] { "Slug" }, new[] { false }));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Assert.Contains("DROP INDEX IF EXISTS \"IX_Posts_Slug\"", sql.Down);

        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        ExecuteSqlite(connection, "CREATE TABLE Posts (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)");
        ExecuteSqlite(connection, "INSERT INTO Posts (Id, Name) VALUES (1, 'first')");
        foreach (var statement in sql.Up)
            ExecuteSqlite(connection, statement);
        foreach (var statement in sql.PreTransactionDown ?? Array.Empty<string>())
            ExecuteSqlite(connection, statement);
        foreach (var statement in sql.Down)
            ExecuteSqlite(connection, statement);
        foreach (var statement in sql.PostTransactionDown ?? Array.Empty<string>())
            ExecuteSqlite(connection, statement);

        using var columnCommand = connection.CreateCommand();
        columnCommand.CommandText = "SELECT COUNT(*) FROM pragma_table_info('Posts') WHERE name = 'Slug'";
        Assert.Equal(0L, (long)columnCommand.ExecuteScalar()!);

        using var indexCommand = connection.CreateCommand();
        indexCommand.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'IX_Posts_Slug'";
        Assert.Equal(0L, (long)indexCommand.ExecuteScalar()!);
    }

    [Fact]
    public void SupportedGenerators_QualifiedExpressionIndexDrop_QualifiesIndexName()
    {
        var table = BuildTable("tenant.Users", ColPk("Id"));
        var sqliteExpressionIndex = new ExpressionIndexSchema
        {
            Name = "IX_Users_LowerEmail",
            ExpressionSql = "lower(Email)"
        };
        var postgresExpressionIndex = new ExpressionIndexSchema
        {
            Name = "IX_Users_LowerEmail",
            ExpressionSql = "lower(\"Email\")"
        };

        var sqliteDiff = new SchemaDiff();
        sqliteDiff.AddedExpressionIndexes.Add((table, sqliteExpressionIndex));
        var sqliteSql = new SqliteMigrationSqlGenerator().GenerateSql(sqliteDiff);
        Assert.Contains(sqliteSql.Up, s => s.StartsWith("CREATE INDEX \"tenant\".\"IX_Users_LowerEmail\" ON \"Users\"", StringComparison.Ordinal));
        Assert.Contains("DROP INDEX IF EXISTS \"tenant\".\"IX_Users_LowerEmail\"", sqliteSql.Down);

        var postgresDiff = new SchemaDiff();
        postgresDiff.AddedExpressionIndexes.Add((table, postgresExpressionIndex));
        var postgresSql = new PostgresMigrationSqlGenerator().GenerateSql(postgresDiff);
        Assert.Contains(postgresSql.Up, s => s.StartsWith("CREATE INDEX \"tenant\".\"IX_Users_LowerEmail\" ON \"tenant\".\"Users\"", StringComparison.Ordinal));
        Assert.Contains("DROP INDEX \"tenant\".\"IX_Users_LowerEmail\"", postgresSql.Down);
    }

    [Fact]
    public void AllProviders_QualifiedForeignKeyPrincipalTable_UsesProviderSupportedReference()
    {
        var table = BuildTable(
            "tenant.Orders",
            ColPk("Id"),
            new ColumnSchema { Name = "UserId", ClrType = "System.Int32", IsNullable = false });
        table.ForeignKeys.Add(new ForeignKeySchema
        {
            ConstraintName = "FK_Orders_Users",
            DependentColumns = new[] { "UserId" },
            PrincipalTable = "tenant.Users",
            PrincipalColumns = new[] { "Id" }
        });

        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        foreach (var (name, gen) in AllGenerators())
        {
            var createStmt = gen.GenerateSql(diff).Up.First(s => s.StartsWith("CREATE TABLE"));
            var expected = name switch
            {
                "SqlServer" => "REFERENCES [tenant].[Users]([Id])",
                "MySQL"     => "REFERENCES `tenant`.`Users`(`Id`)",
                "Postgres"  => "REFERENCES \"tenant\".\"Users\"(\"Id\")",
                "SQLite"    => "REFERENCES \"Users\"(\"Id\")",
                _           => throw new InvalidOperationException(name)
            };

            Assert.Contains(expected, createStmt);
            if (name == "SQLite")
                Assert.DoesNotContain("REFERENCES \"tenant\".\"Users\"", createStmt, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AllProviders_DottedColumnName_RemainsSingleIdentifier()
    {
        var table = BuildTable("SafeTable", ColPk("Id"), Col("Audit.CreatedOn"));
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        foreach (var (name, gen) in AllGenerators())
        {
            var createStmt = gen.GenerateSql(diff).Up.First(s => s.StartsWith("CREATE TABLE"));
            var expected = name switch
            {
                "SqlServer" => "[Audit.CreatedOn]",
                "MySQL"     => "`Audit.CreatedOn`",
                "Postgres"  => "\"Audit.CreatedOn\"",
                "SQLite"    => "\"Audit.CreatedOn\"",
                _           => throw new InvalidOperationException(name)
            };

            Assert.Contains(expected, createStmt);
        }
    }

    [Fact]
    public void Sqlite_QualifiedTableName_RecreateUsesSameSchemaTempAndUnqualifiedRenameTarget()
    {
        var table = BuildTable("tenant.Users", ColPk("Id"), Col("Name", "System.String", true));
        var oldCol = new ColumnSchema { Name = "Name", ClrType = "System.String", IsNullable = true };
        var newCol = new ColumnSchema { Name = "Name", ClrType = "System.String", IsNullable = false };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, s => s.StartsWith("DROP TABLE IF EXISTS \"tenant\".\"__temp__Users\""));
        Assert.Contains(sql.Up, s => s.StartsWith("CREATE TABLE \"tenant\".\"__temp__Users\""));
        Assert.Contains(sql.Up, s => s.Contains("FROM \"tenant\".\"Users\""));
        Assert.Contains(sql.Up, s => s == "DROP TABLE \"tenant\".\"Users\"");
        Assert.Contains(sql.Up, s => s == "ALTER TABLE \"tenant\".\"__temp__Users\" RENAME TO \"Users\"");
        Assert.DoesNotContain(sql.Up, s => s.Contains("RENAME TO \"tenant\".\"Users\""));
    }

    private static IEnumerable<(string Name, IMigrationSqlGenerator Gen)> AllGenerators()
    {
        yield return ("SqlServer", new SqlServerMigrationSqlGenerator());
        yield return ("MySQL",     new MySqlMigrationSqlGenerator());
        yield return ("Postgres",  new PostgresMigrationSqlGenerator());
        yield return ("SQLite",    new SqliteMigrationSqlGenerator());
    }

    private static void ExecuteSqlite(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
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
