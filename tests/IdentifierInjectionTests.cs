using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Validates that IsSafeIdentifier rejects injected identifiers and that
/// DbContext.Query throws before executing any SQL when given a malicious table name.
/// </summary>
public class IdentifierInjectionTests
{
    // Bracket-wrapped injection with statement break — previously passed, now rejected
    [Theory]
    [InlineData("[foo]; DROP TABLE Users--")]
    [InlineData("[foo]]; DROP TABLE Users--")]
    [InlineData("\"col\"\"injection\"")]
    [InlineData("`col`injection`")]
    [InlineData("valid]; DROP TABLE x--")]
    [InlineData("[a;b]")]
    [InlineData("a--b")]
    [InlineData("a/*b*/")]
    [InlineData("[a'b]")]
    public void IsSafeIdentifier_ReturnsFalse_ForMaliciousInput(string malicious)
    {
        Assert.False(DbContext.IsSafeIdentifier(malicious),
            $"Expected '{malicious}' to be rejected as unsafe.");
    }

    // Valid identifiers must still pass
    [Theory]
    [InlineData("Users")]
    [InlineData("[Users]")]
    [InlineData("dbo.Users")]
    [InlineData("[dbo].[Users]")]
    [InlineData("My Table")]
    [InlineData("\"Users\"")]
    [InlineData("`Users`")]
    [InlineData("schema.table_name")]
    public void IsSafeIdentifier_ReturnsTrue_ForValidInput(string valid)
    {
        Assert.True(DbContext.IsSafeIdentifier(valid),
            $"Expected '{valid}' to be accepted as safe.");
    }

    // Verify DbContext.Query throws NormUsageException before executing SQL
    [Fact]
    public void Query_ThrowsNormUsageException_ForMaliciousTableName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Should throw before any SQL is executed — no table with this name exists,
        // and the point is that the injection should be rejected at validation time.
        var ex = Assert.Throws<NormUsageException>(() => ctx.Query("[foo]; DROP TABLE Users--"));
        Assert.Contains("Invalid table name", ex.Message);
    }

    [Fact]
    public void Query_ThrowsNormUsageException_ForSemicolonTableName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        Assert.Throws<NormUsageException>(() => ctx.Query("Users; DROP TABLE x"));
    }
}
