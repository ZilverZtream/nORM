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
/// SQLite disables rowid aliasing for a column-constraint <c>INTEGER PRIMARY KEY DESC</c> — such a key is
/// app-assigned, NOT store-generated. The scaffolder flagged any single-column INTEGER PK in a rowid table
/// as <c>[DatabaseGenerated(Identity)]</c>, so the DESC form scaffolded as an identity: nORM then omitted
/// the column from every INSERT and read it back, but SQLite never assigned it (the read-back is NULL) — so
/// SaveChanges through the generated model threw casting DBNull to long. The table-constraint form
/// <c>PRIMARY KEY(col DESC)</c> DOES still alias the rowid and must stay identity.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ScaffoldSqliteIntegerPkDescIdentityTests
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
        var dir = Path.Combine(Path.GetTempPath(), "scaffold_pkdesc_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "PkDescCtx");
            return await File.ReadAllTextAsync(Path.Combine(dir, tableName + ".cs"));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task Column_constraint_integer_pk_desc_is_not_scaffolded_as_identity()
    {
        var code = await ScaffoldEntityAsync(
            "CREATE TABLE IntPkDesc (Id INTEGER PRIMARY KEY DESC, Name TEXT NOT NULL)",
            "IntPkDesc");

        Assert.Contains("public long Id", code);
        Assert.DoesNotContain("DatabaseGenerated(DatabaseGeneratedOption.Identity)", code);
    }

    [Fact]
    public async Task Plain_integer_pk_is_still_scaffolded_as_identity()
    {
        var code = await ScaffoldEntityAsync(
            "CREATE TABLE IntPkPlain (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)",
            "IntPkPlain");

        Assert.Contains("DatabaseGenerated(DatabaseGeneratedOption.Identity)", code);
    }

    [Fact]
    public async Task Table_constraint_pk_desc_still_aliases_rowid_and_stays_identity()
    {
        // PRIMARY KEY(Id DESC) as a TABLE constraint still aliases the rowid in SQLite.
        var code = await ScaffoldEntityAsync(
            "CREATE TABLE IntPkTableDesc (Id INTEGER NOT NULL, Name TEXT NOT NULL, PRIMARY KEY(Id DESC))",
            "IntPkTableDesc");

        Assert.Contains("DatabaseGenerated(DatabaseGeneratedOption.Identity)", code);
    }
}
