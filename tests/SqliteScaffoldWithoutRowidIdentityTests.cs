using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Scaffolding;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A single-column INTEGER PRIMARY KEY aliases the store-generated rowid ONLY in a normal (rowid) table.
/// In a WITHOUT ROWID table there is NO rowid, so even `Id INTEGER PRIMARY KEY` is app-assigned — it must
/// NOT be scaffolded as [DatabaseGenerated(Identity)], or nORM would omit it on insert and read back a
/// nonexistent rowid, corrupting the key.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SqliteScaffoldWithoutRowidIdentityTests
{
    private static bool HasIdentityKey(Type entityType) =>
        entityType.GetProperties().Any(p =>
            p.GetCustomAttribute<DatabaseGeneratedAttribute>()?.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity);

    [Fact]
    public async Task Integer_pk_in_without_rowid_table_is_not_store_generated()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE WrIdRowid (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
                CREATE TABLE WrIdNoRowid (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL) WITHOUT ROWID;
                """;
            cmd.ExecuteNonQuery();
        }

        var rowidType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(cn, "WrIdRowid");
        var noRowidType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(cn, "WrIdNoRowid");

        // Normal rowid table: INTEGER PK IS the rowid alias -> store-generated.
        Assert.True(HasIdentityKey(rowidType));
        // WITHOUT ROWID: no rowid, so the INTEGER PK is app-assigned.
        Assert.False(HasIdentityKey(noRowidType));   // BUG: true — flagged identity despite WITHOUT ROWID
    }
}
