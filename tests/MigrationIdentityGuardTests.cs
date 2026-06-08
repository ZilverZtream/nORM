using System;
using System.Collections.Generic;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class MigrationIdentityGuardTests
{
    public static IEnumerable<object[]> NonSqliteGenerators()
    {
        yield return new object[] { "SQL Server", new SqlServerMigrationSqlGenerator() };
        yield return new object[] { "PostgreSQL", new PostgresMigrationSqlGenerator() };
        yield return new object[] { "MySQL", new MySqlMigrationSqlGenerator() };
    }

    [Theory]
    [MemberData(nameof(NonSqliteGenerators))]
    public void AlteredColumn_IdentityMetadataChanged_ThrowsInsteadOfSilentlyIgnoring(
        string providerName,
        IMigrationSqlGenerator generator)
    {
        var table = Table("Users");
        var oldCol = new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true };
        var newCol = new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsIdentity = true };
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var ex = Assert.Throws<NotSupportedException>(() => generator.GenerateSql(diff));

        Assert.Contains("identity", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(providerName, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(NonSqliteGenerators))]
    public void AddedColumn_IdentityColumnOnExistingTable_ThrowsInsteadOfDroppingIdentityMetadata(
        string providerName,
        IMigrationSqlGenerator generator)
    {
        var table = Table("Users");
        var column = new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsIdentity = true, DefaultValue = "0" };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, column));

        var ex = Assert.Throws<NotSupportedException>(() => generator.GenerateSql(diff));

        Assert.Contains("identity", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(providerName, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(NonSqliteGenerators))]
    public void DroppedColumn_IdentityColumn_ThrowsBecauseDownCannotRestoreIdentityMetadata(
        string providerName,
        IMigrationSqlGenerator generator)
    {
        var table = Table("Users");
        var column = new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsIdentity = true };
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, column));

        var ex = Assert.Throws<NotSupportedException>(() => generator.GenerateSql(diff));

        Assert.Contains("identity", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(providerName, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Sqlite_ValidIntegerPrimaryKeyIdentity_EmitsAutoincrement()
    {
        var table = new TableSchema
        {
            Name = "Users",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsIdentity = true },
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        Assert.Contains(sql.Up, statement => statement.Contains("\"Id\" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT", StringComparison.Ordinal));
        Assert.DoesNotContain(sql.Up, statement => statement.Contains("CONSTRAINT \"PK_Users\" PRIMARY KEY", StringComparison.Ordinal));
    }

    [Fact]
    public void Sqlite_AddedColumnIdentityNonPrimaryKey_ThrowsInsteadOfIgnoringIdentityMetadata()
    {
        var table = Table("Users");
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, new ColumnSchema
        {
            Name = "Sequence",
            ClrType = typeof(int).FullName!,
            IsNullable = false,
            IsIdentity = true,
            DefaultValue = "0"
        }));

        var ex = Assert.Throws<NotSupportedException>(() => new SqliteMigrationSqlGenerator().GenerateSql(diff));

        Assert.Contains("SQLite", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("identity", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("single primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Sqlite_IdentityInCompositePrimaryKey_ThrowsInsteadOfDroppingCompositeKey()
    {
        var table = new TableSchema
        {
            Name = "TenantUsers",
            Columns =
            {
                new ColumnSchema { Name = "TenantId", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true },
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsIdentity = true }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var ex = Assert.Throws<NotSupportedException>(() => new SqliteMigrationSqlGenerator().GenerateSql(diff));

        Assert.Contains("SQLite", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("composite primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Sqlite_NonIntegerIdentityPrimaryKey_ThrowsInsteadOfEmittingInvalidAutoincrement()
    {
        var table = new TableSchema
        {
            Name = "Users",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(string).FullName!, IsNullable = false, IsPrimaryKey = true, IsIdentity = true }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var ex = Assert.Throws<NotSupportedException>(() => new SqliteMigrationSqlGenerator().GenerateSql(diff));

        Assert.Contains("SQLite", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("INTEGER PRIMARY KEY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Sqlite_IdentitySeedOrIncrement_ThrowsBecauseSQLiteCannotRepresentIt()
    {
        var table = new TableSchema
        {
            Name = "Users",
            Columns =
            {
                new ColumnSchema
                {
                    Name = "Id",
                    ClrType = typeof(int).FullName!,
                    IsNullable = false,
                    IsPrimaryKey = true,
                    IsIdentity = true,
                    IdentitySeed = 100,
                    IdentityIncrement = 5
                }
            }
        };
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var ex = Assert.Throws<NotSupportedException>(() => new SqliteMigrationSqlGenerator().GenerateSql(diff));

        Assert.Contains("SQLite", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("seed", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("increment", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static TableSchema Table(string name)
        => new()
        {
            Name = name,
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true }
            }
        };
}
