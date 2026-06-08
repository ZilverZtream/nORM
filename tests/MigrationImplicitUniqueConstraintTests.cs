using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class MigrationImplicitUniqueConstraintTests
{
    public static IEnumerable<object[]> ProviderCreateCases()
    {
        yield return new object[]
        {
            "SqlServer",
            new SqlServerMigrationSqlGenerator(),
            "CONSTRAINT [PK_Users] PRIMARY KEY ([Id])",
            "CREATE UNIQUE INDEX [PK_Users]",
            "CONSTRAINT [UQ_Users_Email] UNIQUE ([Email])",
            "CREATE UNIQUE INDEX [IX_Users_Code] ON [Users] ([Code])",
            "[UQ_Users_Code]"
        };
        yield return new object[]
        {
            "Postgres",
            new PostgresMigrationSqlGenerator(),
            "CONSTRAINT \"PK_Users\" PRIMARY KEY (\"Id\")",
            "CREATE UNIQUE INDEX \"PK_Users\"",
            "CONSTRAINT \"UQ_Users_Email\" UNIQUE (\"Email\")",
            "CREATE UNIQUE INDEX \"IX_Users_Code\" ON \"Users\" (\"Code\")",
            "\"UQ_Users_Code\""
        };
        yield return new object[]
        {
            "MySql",
            new MySqlMigrationSqlGenerator(),
            "CONSTRAINT `PK_Users` PRIMARY KEY (`Id`)",
            "CREATE UNIQUE INDEX `PK_Users`",
            "CONSTRAINT `UQ_Users_Email` UNIQUE (`Email`)",
            "CREATE UNIQUE INDEX `IX_Users_Code` ON `Users` (`Code`)",
            "`UQ_Users_Code`"
        };
        yield return new object[]
        {
            "Sqlite",
            new SqliteMigrationSqlGenerator(),
            "CONSTRAINT \"PK_Users\" PRIMARY KEY (\"Id\")",
            "CREATE UNIQUE INDEX \"PK_Users\"",
            "CONSTRAINT \"UQ_Users_Email\" UNIQUE (\"Email\")",
            "CREATE UNIQUE INDEX \"IX_Users_Code\" ON \"Users\" (\"Code\")",
            "\"UQ_Users_Code\""
        };
    }

    public static IEnumerable<object[]> DirectAlterProviderCases()
    {
        yield return new object[]
        {
            "SqlServer",
            new SqlServerMigrationSqlGenerator(),
            "ALTER TABLE [Users] ADD CONSTRAINT [UQ_Users_Code] UNIQUE ([Code])",
            "ALTER TABLE [Users] DROP CONSTRAINT [UQ_Users_Code]"
        };
        yield return new object[]
        {
            "Postgres",
            new PostgresMigrationSqlGenerator(),
            "ALTER TABLE \"Users\" ADD CONSTRAINT \"UQ_Users_Code\" UNIQUE (\"Code\")",
            "ALTER TABLE \"Users\" DROP CONSTRAINT \"UQ_Users_Code\""
        };
        yield return new object[]
        {
            "MySql",
            new MySqlMigrationSqlGenerator(),
            "ALTER TABLE `Users` ADD CONSTRAINT `UQ_Users_Code` UNIQUE (`Code`)",
            "DROP INDEX `UQ_Users_Code` ON `Users`"
        };
    }

    public static IEnumerable<object[]> DirectPrimaryKeyAlterProviderCases()
    {
        yield return new object[]
        {
            "SqlServer",
            new SqlServerMigrationSqlGenerator(),
            "ALTER TABLE [Users] ADD CONSTRAINT [PK_Users] PRIMARY KEY ([Code])",
            "ALTER TABLE [Users] DROP CONSTRAINT [PK_Users]"
        };
        yield return new object[]
        {
            "Postgres",
            new PostgresMigrationSqlGenerator(),
            "ALTER TABLE \"Users\" ADD CONSTRAINT \"PK_Users\" PRIMARY KEY (\"Code\")",
            "ALTER TABLE \"Users\" DROP CONSTRAINT \"PK_Users\""
        };
        yield return new object[]
        {
            "MySql",
            new MySqlMigrationSqlGenerator(),
            "ALTER TABLE `Users` ADD CONSTRAINT `PK_Users` PRIMARY KEY (`Code`)",
            "ALTER TABLE `Users` DROP PRIMARY KEY"
        };
    }

    [Theory]
    [MemberData(nameof(ProviderCreateCases))]
    public void CreateTable_ImplicitUniqueConstraint_IsNamed_AndExplicitUniqueIndexIsNotDuplicated(
        string _,
        IMigrationSqlGenerator generator,
        string expectedPrimaryKey,
        string unexpectedPrimaryKeyIndex,
        string expectedImplicitUnique,
        string expectedExplicitIndex,
        string unexpectedExplicitUniqueConstraint)
    {
        var code = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true };
        code.Indexes.Add(new ColumnIndexSchema { Name = "IX_Users_Code", IsUnique = true });
        var table = Table("Users",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
            new ColumnSchema { Name = "Email", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true },
            code);
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = generator.GenerateSql(diff);

        var create = Assert.Single(sql.Up.Where(static s => s.StartsWith("CREATE TABLE", StringComparison.Ordinal)));
        Assert.Contains(expectedPrimaryKey, create);
        Assert.Contains(expectedImplicitUnique, create);
        Assert.DoesNotContain(unexpectedExplicitUniqueConstraint, create);
        Assert.DoesNotContain(sql.Up, s => s.Contains(unexpectedPrimaryKeyIndex, StringComparison.Ordinal));
        Assert.Contains(sql.Up, s => s == expectedExplicitIndex);
    }

    [Theory]
    [MemberData(nameof(DirectAlterProviderCases))]
    public void AlteredColumn_ImplicitUniqueGained_AddsNamedConstraintAndDownDropsIt(
        string _,
        IMigrationSqlGenerator generator,
        string expectedAdd,
        string expectedDrop)
    {
        var table = Table("Users",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
            new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true });
        var oldCol = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = false };
        var newCol = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true };
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = generator.GenerateSql(diff);

        Assert.Contains(expectedAdd, sql.Up);
        Assert.Contains(expectedDrop, sql.Down);
    }

    [Theory]
    [MemberData(nameof(DirectAlterProviderCases))]
    public void AlteredColumn_ImplicitUniqueDropped_DropsNamedConstraintAndDownRestoresIt(
        string _,
        IMigrationSqlGenerator generator,
        string expectedAdd,
        string expectedDrop)
    {
        var table = Table("Users",
            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" },
            new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = false });
        var oldCol = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true };
        var newCol = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = false };
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = generator.GenerateSql(diff);

        Assert.Contains(expectedDrop, sql.Up);
        Assert.Contains(expectedAdd, sql.Down);
    }

    [Theory]
    [MemberData(nameof(DirectPrimaryKeyAlterProviderCases))]
    public void AlteredColumn_PrimaryKeyGained_AddsNamedConstraintAndDownDropsIt(
        string _,
        IMigrationSqlGenerator generator,
        string expectedAdd,
        string expectedDrop)
    {
        var table = Table("Users",
            new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" });
        var oldCol = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsPrimaryKey = false, IsUnique = false };
        var newCol = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" };
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = generator.GenerateSql(diff);

        Assert.Contains(expectedAdd, sql.Up);
        Assert.Contains(expectedDrop, sql.Down);
    }

    [Theory]
    [MemberData(nameof(DirectPrimaryKeyAlterProviderCases))]
    public void AlteredColumn_PrimaryKeyDropped_DropsNamedConstraintAndDownRestoresIt(
        string _,
        IMigrationSqlGenerator generator,
        string expectedAdd,
        string expectedDrop)
    {
        var table = Table("Users",
            new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsPrimaryKey = false, IsUnique = false });
        var oldCol = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Users" };
        var newCol = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsPrimaryKey = false, IsUnique = false };
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var sql = generator.GenerateSql(diff);

        Assert.Contains(expectedDrop, sql.Up);
        Assert.Contains(expectedAdd, sql.Down);
    }

    [Theory]
    [MemberData(nameof(DirectAlterProviderCases))]
    public void AddedColumn_ImplicitUnique_AddsNamedConstraintAndDownDropsItBeforeColumn(
        string _,
        IMigrationSqlGenerator generator,
        string expectedAdd,
        string expectedDrop)
    {
        var column = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = true, IsUnique = true };
        var table = Table("Users", column);
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, column));

        var sql = generator.GenerateSql(diff);

        Assert.Contains(expectedAdd, sql.Up);
        AssertBefore(sql.Down, expectedDrop, static s => s.Contains("DROP COLUMN", StringComparison.Ordinal) && s.Contains("Code", StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(DirectAlterProviderCases))]
    public void DroppedColumn_ImplicitUnique_DropsNamedConstraintBeforeColumnAndDownRestoresIt(
        string _,
        IMigrationSqlGenerator generator,
        string expectedAdd,
        string expectedDrop)
    {
        var column = new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = true, IsUnique = true };
        var table = Table("Users", column);
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, column));

        var sql = generator.GenerateSql(diff);

        AssertBefore(sql.Up, expectedDrop, static s => s.Contains("DROP COLUMN", StringComparison.Ordinal) && s.Contains("Code", StringComparison.Ordinal));
        Assert.Contains(expectedAdd, sql.Down);
    }

    [Theory]
    [MemberData(nameof(DirectPrimaryKeyAlterProviderCases))]
    public void AddedColumn_PrimaryKey_AddsNamedConstraintAndDownDropsItBeforeColumn(
        string _,
        IMigrationSqlGenerator generator,
        string expectedAdd,
        string expectedDrop)
    {
        var column = new ColumnSchema
        {
            Name = "Code",
            ClrType = typeof(int).FullName!,
            IsNullable = false,
            DefaultValue = "0",
            IsPrimaryKey = true,
            IsUnique = true,
            IndexName = "PK_Users"
        };
        var table = Table("Users", column);
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, column));

        var sql = generator.GenerateSql(diff);

        Assert.Contains(expectedAdd, sql.Up);
        AssertBefore(sql.Down, expectedDrop, static s => s.Contains("DROP COLUMN", StringComparison.Ordinal) && s.Contains("Code", StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(DirectPrimaryKeyAlterProviderCases))]
    public void DroppedColumn_PrimaryKey_DropsNamedConstraintBeforeColumnAndDownRestoresIt(
        string _,
        IMigrationSqlGenerator generator,
        string expectedAdd,
        string expectedDrop)
    {
        var column = new ColumnSchema
        {
            Name = "Code",
            ClrType = typeof(int).FullName!,
            IsNullable = false,
            DefaultValue = "0",
            IsPrimaryKey = true,
            IsUnique = true,
            IndexName = "PK_Users"
        };
        var table = Table("Users", column);
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, column));

        var sql = generator.GenerateSql(diff);

        AssertBefore(sql.Up, expectedDrop, static s => s.Contains("DROP COLUMN", StringComparison.Ordinal) && s.Contains("Code", StringComparison.Ordinal));
        Assert.Contains(expectedAdd, sql.Down);
    }

    private static TableSchema Table(string name, params ColumnSchema[] columns)
    {
        var table = new TableSchema { Name = name };
        foreach (var column in columns)
            table.Columns.Add(column);
        return table;
    }

    private static void AssertBefore(IReadOnlyList<string> statements, string expectedFirst, Func<string, bool> secondPredicate)
    {
        var first = statements.ToList().FindIndex(s => s == expectedFirst);
        var second = statements.ToList().FindIndex(s => secondPredicate(s));

        Assert.True(first >= 0, $"Expected statement not found: {expectedFirst}");
        Assert.True(second >= 0, "Expected later statement was not found.");
        Assert.True(first < second, $"Expected '{expectedFirst}' before '{statements[second]}'.");
    }
}
