using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for INSERT SQL conditionally appends identity retrieval.
/// </summary>
public class InsertSqlShapeTests
{
    /// <summary>Entity with a DB-generated (auto-increment) primary key.</summary>
    [Table("IdentityItem")]
    private class IdentityItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    /// <summary>Entity with a natural key — user provides the PK value.</summary>
    [Table("NaturalKeyItem")]
    private class NaturalKeyItem
    {
        [Key]
        public string Code { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateIdentityContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE IdentityItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateNaturalKeyContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NaturalKeyItem (Code TEXT PRIMARY KEY, Description TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    /// <summary>
    /// For a natural-key entity the INSERT SQL must NOT contain
    /// identity retrieval (last_insert_rowid or equivalent).
    /// </summary>
    [Fact]
    public void Insert_NaturalKeyEntity_SqlDoesNotContainIdentityRetrieval()
    {
        var (cn, ctx) = CreateNaturalKeyContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var mapping = GetMapping<NaturalKeyItem>(ctx);
        var sql = ctx.Provider.BuildInsert(mapping);

        Assert.DoesNotContain("last_insert_rowid", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SCOPE_IDENTITY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("RETURNING", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// For a DB-generated-key entity the INSERT SQL MUST contain
    /// identity retrieval (last_insert_rowid for SQLite).
    /// </summary>
    [Fact]
    public void Insert_IdentityKeyEntity_SqlContainsIdentityRetrieval()
    {
        var (cn, ctx) = CreateIdentityContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var mapping = GetMapping<IdentityItem>(ctx);
        var sql = ctx.Provider.BuildInsert(mapping);

        // SQLite uses RETURNING clause for identity retrieval (or last_insert_rowid fallback)
        Assert.True(
            sql.Contains("RETURNING", StringComparison.OrdinalIgnoreCase) ||
            sql.Contains("last_insert_rowid", StringComparison.OrdinalIgnoreCase),
            $"Expected identity retrieval SQL, got: {sql}");
    }

    /// <summary>
    /// Insert an entity with a natural key, save, re-query by PK,
    /// verify the original PK value is preserved.
    /// </summary>
    [Fact]
    public async Task Insert_NaturalKeyEntity_SaveSucceeds_PkPreserved()
    {
        var (cn, ctx) = CreateNaturalKeyContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var entity = new NaturalKeyItem { Code = "SKU-001", Description = "Widget" };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();

        // Verify the row exists with the original PK.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Code, Description FROM NaturalKeyItem WHERE Code = 'SKU-001'";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read(), "Row should exist after insert");
        Assert.Equal("SKU-001", reader.GetString(0));
        Assert.Equal("Widget", reader.GetString(1));
    }

    private static nORM.Mapping.TableMapping GetMapping<T>(DbContext ctx) where T : class
    {
        var getMapping = typeof(DbContext)
            .GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        return (nORM.Mapping.TableMapping)getMapping.Invoke(ctx, new object[] { typeof(T) })!;
    }
}

// ── Entities for insert-shape parity tests ────────────────────────────────────

[Table("ShapeIntKey")]
file class ShapeIntKey
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Val { get; set; }
}

[Table("ShapeNoGenKey")]
file class ShapeNoGenKey
{
    [Key]
    public int Id { get; set; }
    public string? Val { get; set; }
}

/// <summary>
/// Cross-provider tests that verify each provider emits the correct SQL idiom
/// for reading back a DB-generated key after an INSERT, and that no spurious
/// identity-retrieval fragment appears for manually-assigned keys.
///
/// SQL Server: <c>OUTPUT INSERTED.[col]</c> (placed before VALUES, not after).
/// MySQL:      <c>; SELECT LAST_INSERT_ID();</c> postfix.
/// PostgreSQL: <c>RETURNING [col]</c> postfix.
/// SQLite:     <c>RETURNING [col]</c> postfix.
/// </summary>
public class InsertIdentityRetrievalShapeTests
{
    [Fact]
    public void SqlServer_Insert_HasOutputInserted_NotScopeIdentity()
    {
        var provider = new SqlServerProvider();
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<ShapeIntKey>(ctx);

        var sql = provider.BuildInsert(mapping);

        Assert.Contains("OUTPUT INSERTED", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SCOPE_IDENTITY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LAST_INSERT_ID", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySQL_Insert_HasLastInsertId()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<ShapeIntKey>(ctx);

        var sql = provider.BuildInsert(mapping);

        Assert.Contains("LAST_INSERT_ID", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Postgres_Insert_HasReturning()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<ShapeIntKey>(ctx);

        var sql = provider.BuildInsert(mapping);

        Assert.Contains("RETURNING", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Sqlite_Insert_HasReturning()
    {
        var provider = new SqliteProvider();
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<ShapeIntKey>(ctx);

        var sql = provider.BuildInsert(mapping);

        Assert.Contains("RETURNING", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AllProviders_ManuallyAssignedKey_NoIdentityRetrievalInSql()
    {
        // When the PK is not DB-generated, no identity retrieval fragment should appear.
        var providers = new DatabaseProvider[]
        {
            new SqliteProvider(),
            new SqlServerProvider(),
            new MySqlProvider(new SqliteParameterFactory()),
            new PostgresProvider(new SqliteParameterFactory()),
        };
        foreach (var p in providers)
        {
            using var ctx = BuildCtx(p);
            var mapping = GetMapping<ShapeNoGenKey>(ctx);
            var sql = p.BuildInsert(mapping);

            Assert.DoesNotContain("RETURNING",       sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("SCOPE_IDENTITY",  sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("LAST_INSERT_ID",  sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("OUTPUT INSERTED", sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    private static DbContext BuildCtx(DatabaseProvider provider)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        return new DbContext(cn, provider);
    }

    private static nORM.Mapping.TableMapping GetMapping<T>(DbContext ctx) where T : class
    {
        var method = typeof(DbContext).GetMethod("GetMapping",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        return (nORM.Mapping.TableMapping)method.Invoke(ctx, new object[] { typeof(T) })!;
    }
}
