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
/// In SQLite a single-column PK is a store-generated rowid alias ONLY when the declared type is exactly
/// INTEGER. The scaffolder / dynamic schema reader tested the type with Contains("INT"), which also matches
/// BIGINT / INT / SMALLINT / etc. — none of which alias the rowid — so an app-assigned integer key was
/// wrongly flagged store-generated. nORM then omits it on insert and reads back last_insert_rowid, diverging
/// the in-memory key from the stored value (lost updates / duplicate rows). nORM's own migration generator
/// already encodes the correct exact-INTEGER rule.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SqliteScaffoldBigIntKeyIdentityTests
{
    private static bool HasIdentityKey(Type entityType) =>
        entityType.GetProperties().Any(p =>
            p.GetCustomAttribute<DatabaseGeneratedAttribute>()?.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity);

    [Fact]
    public async Task Bigint_primary_key_is_not_store_generated_but_integer_is()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE LedgerBig (EntryId BIGINT PRIMARY KEY, Amount INTEGER NOT NULL);
                CREATE TABLE LedgerRowid (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }

        var bigintType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(cn, "LedgerBig");
        var rowidType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(cn, "LedgerRowid");

        // BIGINT PRIMARY KEY is NOT the rowid alias -> app-assigned, not store-generated.
        Assert.False(HasIdentityKey(bigintType));   // BUG: true — Contains("INT") matched BIGINT
        // INTEGER PRIMARY KEY IS the rowid alias -> store-generated.
        Assert.True(HasIdentityKey(rowidType));
    }
}
