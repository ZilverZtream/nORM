using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Scaffolding;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies that ComputeSchemaSignature uses a 128-bit (32-char hex) SHA-256 fingerprint
/// and that the signature changes with column type, nullability, and PK changes.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SchemaSignatureTests
{
    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static DynamicEntityTypeGenerator Gen() => new();

    private static string CreateFileDatabase(string tableName)
    {
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_dynamic_scaffold_" + Guid.NewGuid().ToString("N") + ".db");
        using var cn = new SqliteConnection($"Data Source={dbFile}");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"CREATE TABLE {tableName} (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return dbFile;
    }

    [Fact]
    public void Signature_Is_32_Char_Hex()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T1 (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var sig = Gen().ComputeSchemaSignature(cn, "T1");

        // SHA-256 truncated to 16 bytes = 32 hex chars
        Assert.Equal(32, sig.Length);
        Assert.True(Regex.IsMatch(sig, @"^[0-9A-Fa-f]{32}$"), $"Expected 32-char hex, got: {sig}");
    }

    [Fact]
    public void SameSchema_SameSignature()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T2 (Id INTEGER PRIMARY KEY, Value REAL NOT NULL)";
        cmd.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T2");
        var sig2 = gen.ComputeSchemaSignature(cn, "T2");

        Assert.Equal(sig1, sig2);
    }

    [Fact]
    public void DifferentColumnType_DifferentSignature()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T3a (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "CREATE TABLE T3a (Id INTEGER PRIMARY KEY, Amount REAL NOT NULL)";
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T3a");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T3a");

        Assert.NotEqual(sig1, sig2);
    }

    [Fact]
    public void DifferentNullability_DifferentSignature_ForValueTypes()
    {
        // For value types (INTEGER), nullable vs non-nullable produces different CLR types
        // (int? vs int), which the descriptor captures and produces different signatures.
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T4a (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "CREATE TABLE T4a (Id INTEGER PRIMARY KEY, Score INTEGER)";  // nullable int
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T4a");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T4a");

        // The descriptor captures int vs int? (via IsNullableType), so sigs should differ
        Assert.NotEqual(sig1, sig2);
    }

    [Fact]
    public void DifferentNullability_DifferentSignature_ForReferenceTypes()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T4b (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "CREATE TABLE T4b (Id INTEGER PRIMARY KEY, Name TEXT)";
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T4b");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T4b");

        Assert.NotEqual(sig1, sig2);
    }

    [Fact]
    public void SchemaQualifiedLiteralDottedTableName_UsesFirstDotAsSchemaSeparator()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS aux;
            CREATE TABLE "aux"."audit.events" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var type = Gen().GenerateEntityType(cn, "aux.audit.events");
        var table = Assert.Single(type.GetCustomAttributes(typeof(TableAttribute), inherit: false).Cast<TableAttribute>());

        Assert.Equal("audit.events", table.Name);
        Assert.Equal("aux", table.Schema);
        Assert.NotNull(type.GetProperty("Name"));
    }

    [Fact]
    public void AddedColumn_DifferentSignature()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T5a (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "CREATE TABLE T5a (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Extra INTEGER)";
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T5a");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T5a");

        Assert.NotEqual(sig1, sig2);
    }

    [Fact]
    public void DescriptorDelimiterCharactersInColumnNames_AreHandled()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "T5_delim" (
                "Id:Part" INTEGER PRIMARY KEY,
                "Name,Part;Tail" TEXT NOT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var sig = Gen().ComputeSchemaSignature(cn, "T5_delim");

        Assert.Equal(32, sig.Length);
        Assert.True(Regex.IsMatch(sig, @"^[0-9A-Fa-f]{32}$"), $"Expected 32-char hex, got: {sig}");
    }

    [Fact]
    public void ComputedColumn_MetadataAffectsSignatureAndDynamicAttributes()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE T5Computed (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                NameLength INTEGER GENERATED ALWAYS AS (length(Name)) VIRTUAL
            )
            """;
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = """
            CREATE TABLE T5Computed (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                NameLength INTEGER
            )
            """;
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var computedSig = gen.ComputeSchemaSignature(cn, "T5Computed");
        var normalSig = gen.ComputeSchemaSignature(cn2, "T5Computed");
        var type = gen.GenerateEntityType(cn, "T5Computed");
        var generated = type.GetProperty("NameLength")!
            .GetCustomAttributes(typeof(DatabaseGeneratedAttribute), inherit: false)
            .Cast<DatabaseGeneratedAttribute>()
            .SingleOrDefault();

        Assert.NotEqual(normalSig, computedSig);
        Assert.NotNull(generated);
        Assert.Equal(DatabaseGeneratedOption.Computed, generated.DatabaseGeneratedOption);
    }

    [Fact]
    public void DifferentPrimaryKey_DifferentSignature()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T5b (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "CREATE TABLE T5b (Id INTEGER NOT NULL, Name TEXT NOT NULL)";
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T5b");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T5b");

        Assert.NotEqual(sig1, sig2);
    }

    [Fact]
    public void CompositePrimaryKeyOrder_AffectsSignatureAndDynamicKeyPropertyOrder()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE T5CompositeOrder (
                TenantId INTEGER NOT NULL,
                LocalId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (LocalId, TenantId)
            )
            """;
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = """
            CREATE TABLE T5CompositeOrder (
                TenantId INTEGER NOT NULL,
                LocalId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, LocalId)
            )
            """;
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T5CompositeOrder");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T5CompositeOrder");
        var type = gen.GenerateEntityType(cn, "T5CompositeOrder");
        var propertyNames = type.GetProperties().Select(p => p.Name).ToArray();

        Assert.NotEqual(sig1, sig2);
        Assert.Equal("LocalId", propertyNames[0]);
        Assert.Equal("TenantId", propertyNames[1]);
        Assert.Contains(type.GetProperty("LocalId")!.GetCustomAttributes(typeof(KeyAttribute), inherit: false), attr => attr is KeyAttribute);
        Assert.Contains(type.GetProperty("TenantId")!.GetCustomAttributes(typeof(KeyAttribute), inherit: false), attr => attr is KeyAttribute);
    }

    [Fact]
    public void IntegerPrimaryKey_MetadataAffectsSignatureAndDynamicAttributes()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T5c (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "CREATE TABLE T5c (Id INTEGER NOT NULL, Name TEXT NOT NULL)";
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T5c");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T5c");
        var type = gen.GenerateEntityType(cn, "T5c");
        var generated = type.GetProperty("Id")!
            .GetCustomAttributes(typeof(DatabaseGeneratedAttribute), inherit: false)
            .Cast<DatabaseGeneratedAttribute>()
            .SingleOrDefault();

        Assert.NotEqual(sig1, sig2);
        Assert.NotNull(generated);
        Assert.Equal(DatabaseGeneratedOption.Identity, generated.DatabaseGeneratedOption);
        Assert.Equal(typeof(long), type.GetProperty("Id")!.PropertyType);
    }

    [Fact]
    public void GenerateEntityType_WithIdentifierCollisions_GeneratesUniqueProperties()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "dynamic-collision" (
                Id INTEGER PRIMARY KEY,
                "first-name" TEXT NOT NULL,
                "first_name" TEXT NOT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var type = Gen().GenerateEntityType(cn, "dynamic-collision");
        var propertyNames = type.GetProperties().Select(p => p.Name).ToArray();

        Assert.Contains("FirstName", propertyNames);
        Assert.Contains("FirstName2", propertyNames);
    }

    [Fact]
    public void GenerateEntityType_WithNonNullableReferenceColumn_AddsRequiredAttribute()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DynamicRequired (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Notes TEXT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var type = Gen().GenerateEntityType(cn, "DynamicRequired");

        Assert.NotEmpty(type.GetProperty("Name")!.GetCustomAttributes(typeof(RequiredAttribute), inherit: false));
        Assert.Empty(type.GetProperty("Notes")!.GetCustomAttributes(typeof(RequiredAttribute), inherit: false));
    }

    [Fact]
    public void GenerateEntityType_WithSqliteUuidDeclaredType_UsesGuidProperty()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DynamicUuid (
                Id INTEGER PRIMARY KEY,
                ExternalId UUID NOT NULL,
                OptionalExternalId UUID NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var type = Gen().GenerateEntityType(cn, "DynamicUuid");

        Assert.Equal(typeof(Guid), type.GetProperty("ExternalId")!.PropertyType);
        Assert.Equal(typeof(Guid?), type.GetProperty("OptionalExternalId")!.PropertyType);
    }

    [Fact]
    public void GenerateEntityType_WithNoPrimaryKey_MarksTypeReadOnly()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DynamicKeyless (
                ExternalId TEXT NOT NULL,
                Payload TEXT NOT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var type = Gen().GenerateEntityType(cn, "DynamicKeyless");

        Assert.NotNull(type.GetCustomAttributes(typeof(ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
    }

    [Fact]
    public void GenerateEntityType_WithObjectMemberColumnNames_GeneratesUniqueProperties()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "dynamic-object-members" (
                Id INTEGER PRIMARY KEY,
                ToString TEXT NOT NULL,
                Equals TEXT NOT NULL,
                GetHashCode TEXT NOT NULL,
                GetType TEXT NOT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var type = Gen().GenerateEntityType(cn, "dynamic-object-members");
        var propertyNames = type.GetProperties().Select(p => p.Name).ToArray();

        Assert.Contains("ToString2", propertyNames);
        Assert.Contains("Equals2", propertyNames);
        Assert.Contains("GetHashCode2", propertyNames);
        Assert.Contains("GetType2", propertyNames);
    }

    [Fact]
    public void GenerateEntityType_WithQuotedIdentifierCharacters_PreservesOriginalNames()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "dynamic""quote" (
                Id INTEGER PRIMARY KEY,
                "quoted""column" TEXT NOT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var type = Gen().GenerateEntityType(cn, "dynamic\"quote");
        var table = Assert.Single(type.GetCustomAttributes(typeof(TableAttribute), inherit: false).Cast<TableAttribute>());
        var quotedColumn = Assert.Single(type.GetProperties(), p => p.Name == "QuotedColumn");
        var column = Assert.Single(quotedColumn.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());

        Assert.Equal("dynamic\"quote", table.Name);
        Assert.Equal("quoted\"column", column.Name);
    }

    [Fact]
    public void GenerateEntityType_WithLiteralDottedTableName_DoesNotTreatDotAsSchema()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "dynamic.audit" (
                Id INTEGER PRIMARY KEY,
                Message TEXT NOT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var gen = Gen();
        var type = gen.GenerateEntityType(cn, "dynamic.audit");
        var table = Assert.Single(type.GetCustomAttributes(typeof(TableAttribute), inherit: false).Cast<TableAttribute>());
        var signature = gen.ComputeSchemaSignature(cn, "dynamic.audit");

        Assert.Equal("dynamic.audit", table.Name);
        Assert.Null(table.Schema);
        Assert.Contains(type.GetProperties(), p => p.Name == "Message");
        Assert.Equal(32, signature.Length);
    }

    [Fact]
    public void GenerateEntityType_WithLiteralAndSchemaQualifiedDottedCollision_Throws()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS aux;
            CREATE TABLE "aux.orders" (Id INTEGER PRIMARY KEY, Message TEXT NOT NULL);
            CREATE TABLE "aux"."orders" (Id INTEGER PRIMARY KEY, Message TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var ex = Assert.Throws<NormConfigurationException>(() => Gen().GenerateEntityType(cn, "aux.orders"));

        Assert.Contains("ambiguous", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("literal table name", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("schema-qualified", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DynamicScaffolding_ClosedConnection_IsClosedAfterSynchronousUse()
    {
        var dbFile = CreateFileDatabase("LifecycleSync");
        try
        {
            using var cn = new SqliteConnection($"Data Source={dbFile}");
            Assert.Equal(ConnectionState.Closed, cn.State);

            var type = Gen().GenerateEntityType(cn, "LifecycleSync");
            Assert.Equal("LifecycleSync", type.GetCustomAttributes(typeof(TableAttribute), inherit: false).Cast<TableAttribute>().Single().Name);
            Assert.Equal(ConnectionState.Closed, cn.State);

            var signature = Gen().ComputeSchemaSignature(cn, "LifecycleSync");
            Assert.Equal(32, signature.Length);
            Assert.Equal(ConnectionState.Closed, cn.State);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
        }
    }

    [Fact]
    public async Task DynamicScaffolding_ClosedConnection_IsClosedAfterAsyncUse()
    {
        var dbFile = CreateFileDatabase("LifecycleAsync");
        try
        {
            await using var cn = new SqliteConnection($"Data Source={dbFile}");
            Assert.Equal(ConnectionState.Closed, cn.State);

            var type = await Gen().GenerateEntityTypeAsync(cn, "LifecycleAsync");
            Assert.Equal("LifecycleAsync", type.GetCustomAttributes(typeof(TableAttribute), inherit: false).Cast<TableAttribute>().Single().Name);
            Assert.Equal(ConnectionState.Closed, cn.State);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
        }
    }
}
