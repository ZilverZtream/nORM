using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// Entities used by the DB-generated key propagation tests.
// Using 'file' scope so the names don't leak into the test assembly's type list.

[Table("DbGenGuidKey")]
file class DbGenGuidKey
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public Guid Id { get; set; }
    public string? Name { get; set; }
}

[Table("DbGenStringKey")]
file class DbGenStringKey
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public string? Id { get; set; }
    public string? Name { get; set; }
}

[Table("DbGenIntKey")]
file class DbGenIntKey
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Name { get; set; }
}

/// <summary>
/// Tests that DB-generated primary keys — including non-numeric types such as Guid and string —
/// are correctly propagated back to the entity after an insert, and that each provider emits the
/// right SQL idiom for reading back the generated value.
///
/// Root cause of the original bug: <c>TableMapping.SetPrimaryKey</c> used
/// <c>Convert.ChangeType</c> which fails for Guid, byte[], and string because those types
/// do not implement <c>IConvertible</c>. SQL Server additionally used SCOPE_IDENTITY() which
/// only returns numeric identity values, so Guid/string keys could never be read back.
/// </summary>
public class DbGeneratedKeyPropagationTests
{
    // ── Unit: SetPrimaryKey type-aware conversion ─────────────────────────────

    [Fact]
    public void SetPrimaryKey_Guid_FromString_Succeeds()
    {
        var mapping = BuildMapping<DbGenGuidKey>(new SqliteProvider());
        var entity = new DbGenGuidKey { Name = "test" };
        var guidString = Guid.NewGuid().ToString();

        mapping.SetPrimaryKey(entity, guidString);

        Assert.Equal(Guid.Parse(guidString), entity.Id);
    }

    [Fact]
    public void SetPrimaryKey_Guid_AlreadyGuid_NoConversion()
    {
        var mapping = BuildMapping<DbGenGuidKey>(new SqliteProvider());
        var entity = new DbGenGuidKey { Name = "test" };
        var guid = Guid.NewGuid();

        mapping.SetPrimaryKey(entity, guid);

        Assert.Equal(guid, entity.Id);
    }

    [Fact]
    public void SetPrimaryKey_Guid_FromBytes_Succeeds()
    {
        var mapping = BuildMapping<DbGenGuidKey>(new SqliteProvider());
        var entity = new DbGenGuidKey { Name = "test" };
        var guid = Guid.NewGuid();
        var bytes = guid.ToByteArray();

        mapping.SetPrimaryKey(entity, bytes);

        Assert.Equal(guid, entity.Id);
    }

    [Fact]
    public void SetPrimaryKey_Int_FromLong_Succeeds()
    {
        var mapping = BuildMapping<DbGenIntKey>(new SqliteProvider());
        var entity = new DbGenIntKey { Name = "test" };

        mapping.SetPrimaryKey(entity, 42L); // long → int (SQLite returns long)

        Assert.Equal(42, entity.Id);
    }

    [Fact]
    public void SetPrimaryKey_Int_FromDecimal_Succeeds()
    {
        var mapping = BuildMapping<DbGenIntKey>(new SqliteProvider());
        var entity = new DbGenIntKey { Name = "test" };

        mapping.SetPrimaryKey(entity, 99m); // decimal → int

        Assert.Equal(99, entity.Id);
    }

    // ── SQL Server: OUTPUT INSERTED in generated SQL ──────────────────────────

    [Fact]
    public void SqlServer_BuildInsert_Uses_OutputInserted_For_IntKey()
    {
        var provider = new SqlServerProvider();
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenIntKey>(ctx);

        var sql = provider.BuildInsert(mapping);

        Assert.Contains("OUTPUT INSERTED.", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SCOPE_IDENTITY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_BuildInsert_Uses_OutputInserted_For_GuidKey()
    {
        var provider = new SqlServerProvider();
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenGuidKey>(ctx);

        var sql = provider.BuildInsert(mapping);

        Assert.Contains("OUTPUT INSERTED.", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_GetIdentityRetrievalString_ReturnsEmpty()
    {
        // SQL Server no longer uses a postfix suffix; the generated value is read
        // via OUTPUT INSERTED which appears inline (before VALUES).
        var provider = new SqlServerProvider();
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenIntKey>(ctx);

        var suffix = provider.GetIdentityRetrievalString(mapping);

        Assert.Equal(string.Empty, suffix);
    }

    [Fact]
    public void SqlServer_GetIdentityRetrievalPrefix_ContainsOutputInserted()
    {
        var provider = new SqlServerProvider();
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenIntKey>(ctx);

        var prefix = provider.GetIdentityRetrievalPrefix(mapping);

        Assert.Contains("OUTPUT INSERTED", prefix, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_BuildInsert_OutputInserted_PlacedBeforeValues()
    {
        // OUTPUT INSERTED must appear between the column list and the VALUES keyword,
        // not after VALUES. The SQL Server docs require: INSERT INTO t (cols) OUTPUT ... VALUES (...)
        var provider = new SqlServerProvider();
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenIntKey>(ctx);

        var sql = provider.BuildInsert(mapping);

        var outputPos = sql.IndexOf("OUTPUT INSERTED", StringComparison.OrdinalIgnoreCase);
        var valuesPos = sql.IndexOf("VALUES", StringComparison.OrdinalIgnoreCase);
        Assert.True(outputPos >= 0 && valuesPos >= 0);
        Assert.True(outputPos < valuesPos,
            $"Expected OUTPUT INSERTED before VALUES. SQL: {sql}");
    }

    // ── MySQL: non-numeric generated key → NormConfigurationException ─────────

    [Fact]
    public void MySQL_NonNumericGeneratedKey_GuidKey_ThrowsNormConfigurationException()
    {
        // MySQL AUTO_INCREMENT only supports numeric columns. Guid keys are invalid.
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenGuidKey>(ctx);

        Assert.Throws<NormConfigurationException>(() => provider.GetIdentityRetrievalString(mapping));
    }

    [Fact]
    public void MySQL_NonNumericGeneratedKey_StringKey_ThrowsNormConfigurationException()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenStringKey>(ctx);

        Assert.Throws<NormConfigurationException>(() => provider.GetIdentityRetrievalString(mapping));
    }

    [Fact]
    public void MySQL_NumericGeneratedKey_ReturnsLastInsertId()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenIntKey>(ctx);

        var sql = provider.GetIdentityRetrievalString(mapping);

        Assert.Contains("LAST_INSERT_ID", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── PostgreSQL/SQLite: RETURNING handles any PK type ─────────────────────

    [Fact]
    public void Postgres_BuildInsert_GuidKey_UsesReturning()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenGuidKey>(ctx);

        var sql = provider.BuildInsert(mapping);

        Assert.Contains("RETURNING", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Sqlite_BuildInsert_GuidKey_UsesReturning()
    {
        var provider = new SqliteProvider();
        using var ctx = BuildCtx(provider);
        var mapping = GetMapping<DbGenGuidKey>(ctx);

        var sql = provider.BuildInsert(mapping);

        Assert.Contains("RETURNING", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── Live SQLite: int PK propagated after SaveChangesAsync ────────────────

    [Fact]
    public async Task Sqlite_InsertIntPk_PropagatesAfterSave()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DbGenIntKey (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var entity = new DbGenIntKey { Name = "alice" };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();

        Assert.True(entity.Id > 0, $"Expected DB-generated PK > 0, got {entity.Id}");
    }

    // Note: Guid key propagation is covered by unit tests (SetPrimaryKey_Guid_*).
    // SQLite has no native UUID generation function, so a live Guid insert would
    // require a custom DEFAULT expression in the DDL.

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static nORM.Mapping.TableMapping BuildMapping<T>(DatabaseProvider provider) where T : class
    {
        using var ctx = BuildCtx(provider);
        return GetMapping<T>(ctx);
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
