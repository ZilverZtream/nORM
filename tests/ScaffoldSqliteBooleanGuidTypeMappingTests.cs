#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SQLite has no dedicated boolean/GUID storage class, so schemas declare these with type names like
/// BOOLEAN / BOOL / BIT and GUID / UNIQUEIDENTIFIER. The scaffolder's store-type mapper had no case for
/// them, so they fell through to the reader-reported CLR type (string): a BOOLEAN column scaffolded as
/// <c>string</c>, and Microsoft.Data.Sqlite's GetString silently coerces the stored 0/1 to "0"/"1" — a
/// query like <c>Where(x =&gt; x.Active == "true")</c> then matches nothing. Map them to bool / Guid
/// (and MONEY to decimal), matching EF Core's reverse-engineering.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ScaffoldSqliteBooleanGuidTypeMappingTests
{
    private static async Task<string> ScaffoldEntityAsync(string createTableSql, string tableName)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = createTableSql;
            cmd.ExecuteNonQuery();
        }
        var dir = Path.Combine(Path.GetTempPath(), "scaffold_types_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "TypeCtx");
            return await File.ReadAllTextAsync(Path.Combine(dir, tableName + ".cs"));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task Boolean_declared_columns_scaffold_as_bool()
    {
        var code = await ScaffoldEntityAsync(
            "CREATE TABLE BoolWidget (Id INTEGER PRIMARY KEY, " +
            "Active BOOLEAN NOT NULL, Enabled BOOL NOT NULL, Flag BIT NOT NULL)",
            "BoolWidget");

        Assert.Contains("public bool Active", code);
        Assert.Contains("public bool Enabled", code);
        Assert.Contains("public bool Flag", code);
        Assert.DoesNotContain("public string Active", code);
    }

    [Fact]
    public async Task Guid_declared_columns_scaffold_as_Guid()
    {
        var code = await ScaffoldEntityAsync(
            "CREATE TABLE GuidWidget (Id INTEGER PRIMARY KEY, " +
            "Token GUID NOT NULL, AltToken UNIQUEIDENTIFIER NOT NULL)",
            "GuidWidget");

        Assert.Contains("public Guid Token", code);
        Assert.Contains("public Guid AltToken", code);
        Assert.DoesNotContain("public string Token", code);
    }

    [Fact]
    public async Task Money_declared_columns_scaffold_as_decimal()
    {
        var code = await ScaffoldEntityAsync(
            "CREATE TABLE MoneyWidget (Id INTEGER PRIMARY KEY, Price MONEY NOT NULL, Petty SMALLMONEY NOT NULL)",
            "MoneyWidget");

        Assert.Contains("public decimal Price", code);
        Assert.Contains("public decimal Petty", code);
    }
}
