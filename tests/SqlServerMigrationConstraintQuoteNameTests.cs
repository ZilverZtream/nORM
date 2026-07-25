using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security regression (identifier injection, migration DDL): the SQL Server migration generator drops a
/// column's auto-named DEFAULT constraint via a dynamic EXEC that must build the constraint identifier with
/// QUOTENAME. It previously concatenated <c>'[' + @name + ']'</c>, so a default-constraint name containing a
/// <c>]</c> (a DBA can create one) would break out of the delimited identifier inside the EXEC. QUOTENAME is
/// the T-SQL-native escape (it doubles an embedded <c>]</c>).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class SqlServerMigrationConstraintQuoteNameTests
{
    private static string Up(SchemaDiff diff)
        => string.Join("\n", new SqlServerMigrationSqlGenerator().GenerateSql(diff).Up);

    [Fact]
    public void Drop_column_default_constraint_drop_uses_QuoteName_not_bracket_concat()
    {
        var col = new ColumnSchema { Name = "Bio", ClrType = typeof(string).FullName!, IsNullable = true };
        var table = new TableSchema { Name = "T" };
        table.Columns.Add(col);
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, col));

        var up = Up(diff);
        Assert.Contains("DROP CONSTRAINT '+QUOTENAME(", up);
        Assert.DoesNotContain("DROP CONSTRAINT ['+", up); // the old, injectable bracket concat
    }

    [Fact]
    public void Alter_column_default_rebind_drop_uses_QuoteName_not_bracket_concat()
    {
        var oldCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true, DefaultValue = "'old'" };
        var newCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true, DefaultValue = "'new'" };
        var table = new TableSchema { Name = "T" };
        table.Columns.Add(oldCol);
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        var up = Up(diff);
        Assert.Contains("QUOTENAME(", up);
        Assert.DoesNotContain("['+", up);
    }
}
