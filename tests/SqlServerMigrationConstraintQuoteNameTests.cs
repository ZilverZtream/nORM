using System.Text.RegularExpressions;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security + correctness regression for the SQL Server migration generator's dynamic DEFAULT-constraint
/// drop. SQL Server names default constraints itself, so the generator discovers the name at run time and
/// drops it via dynamic SQL. Two invariants must hold together:
/// <list type="number">
/// <item>the constraint identifier is escaped with <c>QUOTENAME</c> (not <c>'[' + @name + ']'</c>), so a
/// constraint name containing a <c>]</c> — a DBA can create one — cannot break out of the delimited
/// identifier; and</item>
/// <item><c>QUOTENAME</c> is applied while BUILDING a command variable, never inside <c>EXEC(...)</c>. SQL
/// Server's <c>EXEC(&lt;string&gt;)</c> accepts only string literals/variables joined with <c>+</c>, so a
/// function call inside it is a syntax error ("Incorrect syntax near 'QUOTENAME'") that only surfaces against
/// a live server. Every EXEC in the generated batch must therefore execute a variable.</item>
/// </list>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class SqlServerMigrationConstraintQuoteNameTests
{
    private static string Up(SchemaDiff diff)
        => string.Join("\n", new SqlServerMigrationSqlGenerator().GenerateSql(diff).Up);

    private static void AssertQuoteNameEscapedAndOutsideExec(string up)
    {
        // (1) The constraint name is QUOTENAME-escaped; the old injectable bracket concat is gone.
        Assert.Contains("QUOTENAME(", up);
        Assert.DoesNotContain("['+", up);
        // (2) Every EXEC executes a variable — never a literal or a '+'-concatenation — so QUOTENAME
        //     cannot sit inside EXEC(...). This is the T-SQL-validity guard a string-only test can enforce.
        var execArgs = Regex.Matches(up, @"EXEC\s*\(\s*([^)]*)");
        Assert.NotEmpty(execArgs);
        foreach (Match m in execArgs)
            Assert.StartsWith("@", m.Groups[1].Value.Trim());
    }

    [Fact]
    public void Drop_column_default_constraint_drop_uses_QuoteName_outside_exec()
    {
        var col = new ColumnSchema { Name = "Bio", ClrType = typeof(string).FullName!, IsNullable = true };
        var table = new TableSchema { Name = "T" };
        table.Columns.Add(col);
        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((table, col));

        AssertQuoteNameEscapedAndOutsideExec(Up(diff));
    }

    [Fact]
    public void Alter_column_default_rebind_drop_uses_QuoteName_outside_exec()
    {
        var oldCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true, DefaultValue = "'old'" };
        var newCol = new ColumnSchema { Name = "Col", ClrType = typeof(string).FullName!, IsNullable = true, DefaultValue = "'new'" };
        var table = new TableSchema { Name = "T" };
        table.Columns.Add(oldCol);
        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newCol, oldCol));

        AssertQuoteNameEscapedAndOutsideExec(Up(diff));
    }
}
