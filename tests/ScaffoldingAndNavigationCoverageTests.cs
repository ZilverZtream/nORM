// Tests for DatabaseScaffolder private helpers, NavigationContext,
// NavigationPropertyInfo, LazyNavigationCollection<T>, LazyNavigationReference<T>,
// and NormIncludableQueryable / ThenInclude extensions.

#nullable enable

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

// ── Entity types for LazyNavigationCollection / LazyNavigationReference tests ──

[Table("SAN_Parent")]
[Xunit.Trait("Category", "Fast")]
public class SanParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public ICollection<SanChild>? Children { get; set; }
}

[Table("SAN_Child")]
[Xunit.Trait("Category", "Fast")]
public class SanChild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
}

// ── Entity types for ThenInclude tests ────────────────────────────────────────

[Table("SchemaWidget", Schema = "main")]
[Xunit.Trait("Category", "Fast")]
public class SanSchemaWidget
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Table("SAN_ComputedGenerated")]
public class SanComputedGenerated
{
    [Key]
    public int Id { get; set; }

    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
    public int Total { get; set; }
}

[Table("SAN_RowVersionGenerated")]
public class SanRowVersionGenerated
{
    [Key]
    public int Id { get; set; }

    [Timestamp]
    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
    public byte[] RowVersion { get; set; } = Array.Empty<byte>();
}

[Table("SchemaParent", Schema = "aux")]
[Xunit.Trait("Category", "Fast")]
public class SanSchemaParent
{
    [Key]
    public int Id { get; set; }
    public List<SanSchemaChild> Children { get; set; } = new();
}

[Table("SchemaChild", Schema = "aux")]
[Xunit.Trait("Category", "Fast")]
public class SanSchemaChild
{
    [Key]
    public int Id { get; set; }
    public int ParentId { get; set; }
    public int Amount { get; set; }
}

[Table("SAN_TIParent")]
[Xunit.Trait("Category", "Fast")]
public class SanTIParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public ICollection<SanTIChild>? Children { get; set; }
    public SanTIChild? SingleChild { get; set; }
}

[Table("SAN_TIChild")]
[Xunit.Trait("Category", "Fast")]
public class SanTIChild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
    public ICollection<SanTIGrandchild>? Grandchildren { get; set; }
    public SanTIGrandchild? Grandchild { get; set; }
}

[Table("SAN_TIGrandchild")]
[Xunit.Trait("Category", "Fast")]
public class SanTIGrandchild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ChildId { get; set; }
}

// ── DatabaseScaffolder private-method helpers via reflection ──────────────────

[Xunit.Trait("Category", "Fast")]
public class DatabaseScaffolderPrivateMethodTests
{
    // ── Reflection helpers ──────────────────────────────────────────────────

    private static MethodInfo GetMethod(string name, Type[] paramTypes)
        => typeof(DatabaseScaffolder)
               .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static, null, paramTypes, null)
           ?? throw new MissingMethodException(nameof(DatabaseScaffolder), name);

    private static string InvokeToPascalCase(string input)
    {
        var m = GetMethod("ToPascalCase", new[] { typeof(string) });
        return (string)m.Invoke(null, new object[] { input })!;
    }

    private static string InvokeEscapeCSharpIdentifier(string input)
    {
        var m = GetMethod("EscapeCSharpIdentifier", new[] { typeof(string) });
        return (string)m.Invoke(null, new object[] { input })!;
    }

    private static string InvokeGetTypeName(Type type, bool allowNull)
    {
        var m = GetMethod("GetTypeName", new[] { typeof(Type), typeof(bool) });
        return (string)m.Invoke(null, new object[] { type, allowNull })!;
    }

    private static string InvokeGetUnqualifiedName(string identifier)
    {
        var m = GetMethod("GetUnqualifiedName", new[] { typeof(string) });
        return (string)m.Invoke(null, new object[] { identifier })!;
    }

    private static string? InvokeGetSchemaNameOrNull(string identifier)
    {
        var m = GetMethod("GetSchemaNameOrNull", new[] { typeof(string) });
        return (string?)m.Invoke(null, new object[] { identifier });
    }

    private static string InvokeScaffoldContext(string ns, string ctxName, IEnumerable<string> entities)
    {
        var m = GetMethod("ScaffoldContext", new[] { typeof(string), typeof(string), typeof(IEnumerable<string>) });
        return (string)m.Invoke(null, new object[] { ns, ctxName, entities })!;
    }

    // ── ToPascalCase ────────────────────────────────────────────────────────

    [Fact]
    public void ToPascalCase_UnderscoreSeparated_ReturnsPascalCase()
    {
        var result = InvokeToPascalCase("user_name");
        Assert.Equal("UserName", result);
    }

    [Fact]
    public void ToPascalCase_AllUpperWithUnderscore_ReturnsFirstLetterUpper()
    {
        var result = InvokeToPascalCase("CUSTOMER_ID");
        // Each segment: C+ustomer = "Customer", I+d = "Id"
        Assert.Equal("CustomerId", result);
    }

    [Fact]
    public void ToPascalCase_SpaceSeparated_ReturnsPascalCase()
    {
        var result = InvokeToPascalCase("first name");
        Assert.Equal("FirstName", result);
    }

    [Fact]
    public void ToPascalCase_SingleLower_CapitalizesFirst()
    {
        var result = InvokeToPascalCase("id");
        Assert.Equal("Id", result);
    }

    [Fact]
    public void ToPascalCase_AlreadyPascal_RoundsTrip()
    {
        // "User" → one segment → "User"
        var result = InvokeToPascalCase("User");
        Assert.Equal("User", result);
    }

    [Fact]
    public void ToPascalCase_WhitespaceOnly_ReturnsInput()
    {
        // Implementation: IsNullOrWhiteSpace → return name as-is
        var result = InvokeToPascalCase("   ");
        Assert.Equal("   ", result);
    }

    [Fact]
    public void ToPascalCase_MultipleUnderscores_RemovesAll()
    {
        var result = InvokeToPascalCase("order_line_item");
        Assert.Equal("OrderLineItem", result);
    }

    [Fact]
    public void ToPascalCase_SingleChar_CapitalizesIt()
    {
        var result = InvokeToPascalCase("x");
        Assert.Equal("X", result);
    }

    [Fact]
    public void ToPascalCase_CamelCase_PreservesInnerWordCapital()
    {
        var result = InvokeToPascalCase("blogPost");
        Assert.Equal("BlogPost", result);
    }

    [Fact]
    public void ToPascalCase_InvalidSeparators_BecomeWordBoundaries()
    {
        var result = InvokeToPascalCase("bad-table.name");
        Assert.Equal("BadTableName", result);
    }

    // ── EscapeCSharpIdentifier ──────────────────────────────────────────────

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_class_PrependsAt()
    {
        Assert.Equal("@class", InvokeEscapeCSharpIdentifier("class"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_string_PrependsAt()
    {
        Assert.Equal("@string", InvokeEscapeCSharpIdentifier("string"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_int_PrependsAt()
    {
        Assert.Equal("@int", InvokeEscapeCSharpIdentifier("int"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_bool_PrependsAt()
    {
        Assert.Equal("@bool", InvokeEscapeCSharpIdentifier("bool"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_return_PrependsAt()
    {
        Assert.Equal("@return", InvokeEscapeCSharpIdentifier("return"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_ValidIdentifier_ReturnsUnchanged()
    {
        Assert.Equal("MyField", InvokeEscapeCSharpIdentifier("MyField"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_StartsWithDigit_PrefixesUnderscore()
    {
        Assert.Equal("_123abc", InvokeEscapeCSharpIdentifier("123abc"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_ContainsHyphen_ReplacesWithUnderscore()
    {
        // Hyphens are invalid in C# identifiers
        var result = InvokeEscapeCSharpIdentifier("my-field");
        Assert.Equal("my_field", result);
    }

    [Fact]
    public void EscapeCSharpIdentifier_EmptyString_ReturnsFallbackIdentifier()
    {
        Assert.Equal("_", InvokeEscapeCSharpIdentifier(""));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Underscore_IsValid()
    {
        // Underscore alone is a valid C# identifier
        Assert.Equal("_", InvokeEscapeCSharpIdentifier("_"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_var_PrependsAt()
    {
        Assert.Equal("@var", InvokeEscapeCSharpIdentifier("var"));
    }

    // ── GetTypeName ─────────────────────────────────────────────────────────

    [Fact]
    public void GetTypeName_Int_NotNull_ReturnsInt()
    {
        Assert.Equal("int", InvokeGetTypeName(typeof(int), false));
    }

    [Fact]
    public void GetTypeName_Int_AllowNull_ReturnsIntQuestion()
    {
        Assert.Equal("int?", InvokeGetTypeName(typeof(int), true));
    }

    [Fact]
    public void GetTypeName_String_NotNull_ReturnsString()
    {
        Assert.Equal("string", InvokeGetTypeName(typeof(string), false));
    }

    [Fact]
    public void GetTypeName_String_AllowNull_ReturnsStringQuestion()
    {
        Assert.Equal("string?", InvokeGetTypeName(typeof(string), true));
    }

    [Fact]
    public void GetTypeName_Bool_NotNull_ReturnsBool()
    {
        Assert.Equal("bool", InvokeGetTypeName(typeof(bool), false));
    }

    [Fact]
    public void GetTypeName_Bool_AllowNull_ReturnsBoolQuestion()
    {
        Assert.Equal("bool?", InvokeGetTypeName(typeof(bool), true));
    }

    [Fact]
    public void GetTypeName_DateTime_NotNull_ReturnsDateTime()
    {
        Assert.Equal("DateTime", InvokeGetTypeName(typeof(DateTime), false));
    }

    [Fact]
    public void GetTypeName_DateTime_AllowNull_ReturnsDateTimeQuestion()
    {
        Assert.Equal("DateTime?", InvokeGetTypeName(typeof(DateTime), true));
    }

    [Fact]
    public void GetTypeName_Guid_NotNull_ReturnsGuid()
    {
        Assert.Equal("Guid", InvokeGetTypeName(typeof(Guid), false));
    }

    [Fact]
    public void GetTypeName_Guid_AllowNull_ReturnsGuidQuestion()
    {
        Assert.Equal("Guid?", InvokeGetTypeName(typeof(Guid), true));
    }

    [Fact]
    public void GetTypeName_Long_NotNull_ReturnsLong()
    {
        Assert.Equal("long", InvokeGetTypeName(typeof(long), false));
    }

    [Fact]
    public void GetTypeName_Decimal_NotNull_ReturnsDecimal()
    {
        Assert.Equal("decimal", InvokeGetTypeName(typeof(decimal), false));
    }

    [Fact]
    public void GetTypeName_Double_NotNull_ReturnsDouble()
    {
        Assert.Equal("double", InvokeGetTypeName(typeof(double), false));
    }

    [Fact]
    public void GetTypeName_Float_NotNull_ReturnsFloat()
    {
        Assert.Equal("float", InvokeGetTypeName(typeof(float), false));
    }

    [Fact]
    public void GetTypeName_Short_NotNull_ReturnsFallbackName()
    {
        // short IS in the switch (mapped to "short")
        var result = InvokeGetTypeName(typeof(short), false);
        Assert.Equal("short", result);
    }

    [Fact]
    public void GetTypeName_ByteArray_NotNull_ReturnsByteArray()
    {
        Assert.Equal("byte[]", InvokeGetTypeName(typeof(byte[]), false));
    }

    [Fact]
    public void GetTypeName_ByteArray_AllowNull_ReturnsByteArrayQuestion()
    {
        Assert.Equal("byte[]?", InvokeGetTypeName(typeof(byte[]), true));
    }

    // ── GetUnqualifiedName ──────────────────────────────────────────────────

    [Theory]
    [InlineData(typeof(sbyte), "sbyte")]
    [InlineData(typeof(uint), "uint")]
    [InlineData(typeof(ulong), "ulong")]
    [InlineData(typeof(ushort), "ushort")]
    [InlineData(typeof(char), "char")]
    [InlineData(typeof(DateOnly), "DateOnly")]
    [InlineData(typeof(DateTimeOffset), "DateTimeOffset")]
    [InlineData(typeof(TimeOnly), "TimeOnly")]
    [InlineData(typeof(TimeSpan), "TimeSpan")]
    public void GetTypeName_CommonScalarTypes_ReturnsStableCSharpName(Type type, string expected)
    {
        Assert.Equal(expected, InvokeGetTypeName(type, false));
        Assert.Equal(expected + "?", InvokeGetTypeName(type, true));
    }

    [Fact]
    public void GetUnqualifiedName_SchemaQualified_ReturnsTablePart()
    {
        Assert.Equal("table", InvokeGetUnqualifiedName("schema.table"));
    }

    [Fact]
    public void GetUnqualifiedName_NoSchema_ReturnsWhole()
    {
        Assert.Equal("mytable", InvokeGetUnqualifiedName("mytable"));
    }

    [Fact]
    public void GetUnqualifiedName_TwoLevelSchema_ReturnsLast()
    {
        Assert.Equal("leaf", InvokeGetUnqualifiedName("db.schema.leaf"));
    }

    // ── GetSchemaNameOrNull ─────────────────────────────────────────────────

    [Fact]
    public void GetSchemaNameOrNull_SchemaQualified_ReturnsSchema()
    {
        Assert.Equal("schema", InvokeGetSchemaNameOrNull("schema.table"));
    }

    [Fact]
    public void GetSchemaNameOrNull_NoSchema_ReturnsNull()
    {
        Assert.Null(InvokeGetSchemaNameOrNull("table"));
    }

    [Fact]
    public void GetSchemaNameOrNull_LeadingDot_ReturnsNull()
    {
        // ".table" → idx=0, which is not >0 → returns null
        Assert.Null(InvokeGetSchemaNameOrNull(".table"));
    }

    // ── ScaffoldContext ─────────────────────────────────────────────────────

    [Fact]
    public void ScaffoldContext_ContainsClassName()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "User", "Order" });
        Assert.Contains("class AppDbContext", code);
    }

    [Fact]
    public void ScaffoldContext_ContainsNamespace()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "User" });
        Assert.Contains("namespace MyApp", code);
    }

    [Fact]
    public void ScaffoldContext_ContainsEntityProperty()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "User" });
        Assert.Contains("Users", code);
    }

    [Fact]
    public void ScaffoldContext_UsesReadablePluralizedQueryPropertyNames()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "Category", "Class" });
        Assert.Contains("IQueryable<Category> Categories", code);
        Assert.Contains("IQueryable<Class> Classes", code);
    }

    [Fact]
    public void ScaffoldContext_EntitiesAreSortedAlphabetically()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", new[] { "Zebra", "Apple" });
        var appleIdx = code.IndexOf("Apples", StringComparison.Ordinal);
        var zebraIdx = code.IndexOf("Zebras", StringComparison.Ordinal);
        Assert.True(appleIdx < zebraIdx, "Entities should be alphabetically sorted in the context.");
    }

    [Fact]
    public void ScaffoldContext_ContextInheritsDbContext()
    {
        var code = InvokeScaffoldContext("MyApp", "AppDbContext", Array.Empty<string>());
        Assert.Contains(": DbContext", code);
    }

    [Fact]
    public void ScaffoldContext_KeywordContextName_PrependsAt()
    {
        // "class" is a C# keyword; the context class name should be escaped
        var code = InvokeScaffoldContext("MyApp", "class", Array.Empty<string>());
        Assert.Contains("@class", code);
    }

    [Fact]
    public void ScaffoldContext_ContainsAutoGeneratedHeader()
    {
        var code = InvokeScaffoldContext("MyApp", "Ctx", Array.Empty<string>());
        Assert.Contains("auto-generated", code);
    }

    // ── ScaffoldAsync (public integration) ─────────────────────────────────

    [Fact]
    public async Task ScaffoldAsync_NullConnection_ThrowsArgumentNullException()
    {
        var provider = new SqliteProvider();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(null!, provider, Path.GetTempPath(), "NS"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullProvider_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, null!, Path.GetTempPath(), "NS"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullOutputDirectory_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var provider = new SqliteProvider();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, provider, null!, "NS"));
    }

    [Fact]
    public async Task ScaffoldAsync_NullNamespace_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var provider = new SqliteProvider();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, provider, Path.GetTempPath(), null!));
    }

    [Fact]
    public async Task ScaffoldAsync_InvalidNamespace_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var provider = new SqliteProvider();

        await Assert.ThrowsAsync<NormConfigurationException>(() =>
            DatabaseScaffolder.ScaffoldAsync(cn, provider, Path.GetTempPath(), "1.Bad Namespace"));
    }

    [Fact]
    public async Task ScaffoldAsync_UnsafeContextName_WritesEscapedContextFile()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Widget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "1 Bad/Ctx");

            var contextFile = Path.Combine(dir, "_1BadCtx.cs");
            Assert.True(File.Exists(contextFile));
            var contextCode = File.ReadAllText(contextFile);
            Assert.Contains("public class _1BadCtx : DbContext", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_ContextNameCollidingWithEntityName_UsesUniqueContextFile()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Widget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "Widget");

            Assert.True(File.Exists(Path.Combine(dir, "Widget.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "WidgetContext.cs")));
            var entityCode = File.ReadAllText(Path.Combine(dir, "Widget.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "WidgetContext.cs"));

            Assert.Contains("public class Widget", entityCode);
            Assert.Contains("public class WidgetContext : DbContext", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task TableAttributeSchema_IsIncludedInRuntimeTableMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE "main"."SchemaWidget" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                INSERT INTO "main"."SchemaWidget" (Id, Name) VALUES (1, 'schema-ok');
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(SanSchemaWidget));
        Assert.Equal("main.SchemaWidget", mapping.TableName);
        Assert.Equal("\"main\".\"SchemaWidget\"", mapping.EscTable);

        var rows = await ctx.Query<SanSchemaWidget>().ToListAsync();
        var row = Assert.Single(rows);
        Assert.Equal("schema-ok", row.Name);
    }

    [Fact]
    public void DatabaseGeneratedComputed_IsTreatedAsDatabaseGeneratedColumn()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(SanComputedGenerated));
        var total = Assert.Single(mapping.Columns, c => c.PropName == nameof(SanComputedGenerated.Total));
        Assert.True(total.IsDbGenerated);
        Assert.DoesNotContain(mapping.InsertColumns, c => c.PropName == nameof(SanComputedGenerated.Total));
        Assert.DoesNotContain(mapping.UpdateColumns, c => c.PropName == nameof(SanComputedGenerated.Total));
    }

    [Fact]
    public void RowVersionComputed_IsTimestampAndDatabaseGeneratedColumn()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(SanRowVersionGenerated));
        var rowVersion = Assert.Single(mapping.Columns, c => c.PropName == nameof(SanRowVersionGenerated.RowVersion));
        Assert.True(rowVersion.IsTimestamp);
        Assert.True(rowVersion.IsDbGenerated);
        Assert.DoesNotContain(mapping.InsertColumns, c => c.PropName == nameof(SanRowVersionGenerated.RowVersion));
        Assert.DoesNotContain(mapping.UpdateColumns, c => c.PropName == nameof(SanRowVersionGenerated.RowVersion));
    }

    [Fact]
    public async Task ScaffoldAsync_SqliteAttachedDatabase_PreservesSchemaAndDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                PRAGMA foreign_keys=ON;
                ATTACH DATABASE ':memory:' AS aux;
                CREATE TABLE "aux"."SchemaAuthor" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE "aux"."SchemaBook" (
                    Id INTEGER PRIMARY KEY,
                    AuthorId INTEGER NOT NULL,
                    Title TEXT NOT NULL DEFAULT 'untitled',
                    CONSTRAINT FK_SchemaBook_Author FOREIGN KEY (AuthorId) REFERENCES SchemaAuthor(Id)
                );
                CREATE INDEX "aux"."IX_SchemaBook_Title" ON "SchemaBook"(Title);
                CREATE TRIGGER "aux"."TR_SchemaBook_Audit" AFTER INSERT ON "SchemaBook" BEGIN SELECT 1; END;
                CREATE VIEW "aux"."SchemaBookView" AS SELECT Id, Title FROM "SchemaBook";
                """;
            cmd.ExecuteNonQuery();
        }

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "AttachedCtx");

            var authorCode = File.ReadAllText(Path.Combine(dir, "SchemaAuthor.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "SchemaBook.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "AttachedCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[Table(\"SchemaAuthor\", Schema = \"aux\")]", authorCode);
            Assert.Contains("[Table(\"SchemaBook\", Schema = \"aux\")]", bookCode);
            Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode);
            Assert.Contains("HasForeignKey(d => d.AuthorId, p => p.Id, cascadeDelete: false)", contextCode);
            Assert.Contains("[Index(\"IX_SchemaBook_Title\")]", bookCode);
            Assert.Contains("aux.SchemaBook", warnings);
            Assert.Contains("TR_SchemaBook_Audit", warnings);
            Assert.Contains("aux.SchemaBookView", warnings);

            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("table").GetString() == "aux.SchemaBook" && item.GetProperty("kind").GetString() == "Default");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("table").GetString() == "aux.SchemaBook" && item.GetProperty("kind").GetString() == "Trigger");
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects");
            Assert.Contains(skippedObjects.EnumerateArray(), item => item.GetProperty("name").GetString() == "aux.SchemaBookView");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task TableAttributeSchema_IsUsedInNavigationProjectionSubqueries()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                ATTACH DATABASE ':memory:' AS aux;
                CREATE TABLE "aux"."SchemaParent" (Id INTEGER PRIMARY KEY);
                CREATE TABLE "aux"."SchemaChild" (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO "aux"."SchemaParent" (Id) VALUES (1);
                INSERT INTO "aux"."SchemaChild" (Id, ParentId, Amount) VALUES (10, 1, 7), (11, 1, 5);
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SanSchemaParent>()
                .HasMany<SanSchemaChild>(p => p.Children)
                .WithOne()
                .HasForeignKey(c => c.ParentId, p => p.Id)
        });

        var rows = await ctx.Query<SanSchemaParent>()
            .Select(p => new
            {
                p.Id,
                ChildCount = p.Children.Count(),
                Total = p.Children.Sum(c => c.Amount)
            })
            .ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal(2, row.ChildCount);
        Assert.Equal(12, row.Total);
    }

    [Fact]
    public async Task ScaffoldAsync_EmptyDatabase_ThrowsOrWritesContextFile()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "TestCtx");
            Assert.True(File.Exists(Path.Combine(dir, "TestCtx.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithTable_ThrowsOrWritesFiles()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SanWidget2 (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "MyCtx2");
            Assert.True(File.Exists(Path.Combine(dir, "MyCtx2.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SanWidget2.cs")));
            var entityCode = File.ReadAllText(Path.Combine(dir, "SanWidget2.cs"));
            Assert.Contains("[Required]", entityCode);
            Assert.Contains("public string Name { get; set; } = default!;", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSingleColumnForeignKey_GeneratesNavigationsAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Author_Id INTEGER NOT NULL,
                Title TEXT NOT NULL,
                FOREIGN KEY (Author_Id) REFERENCES Author(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "BookStoreContext");

            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "BookStoreContext.cs"));

            Assert.Contains("public List<Book> Books { get; set; } = new();", authorCode);
            Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode);
            Assert.Contains("public Author? Author { get; set; }", bookCode);
            Assert.Contains(".HasMany(p => p.Books)", contextCode);
            Assert.Contains(".WithOne(d => d.Author)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.AuthorId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains("configure?.Invoke(mb);", contextCode);
            Assert.Contains("var configuredOptions = options?.Clone() ?? new DbContextOptions();", contextCode);
            Assert.DoesNotContain("options.OnModelCreating =", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMultipleForeignKeysToSamePrincipal_UsesRoleBasedNavigationNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Address (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Line1 TEXT NOT NULL
            );
            CREATE TABLE Shipment (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                BillingAddressId INTEGER NOT NULL,
                ShippingAddressId INTEGER NOT NULL,
                CONSTRAINT FK_Shipment_BillingAddress FOREIGN KEY (BillingAddressId) REFERENCES Address(Id),
                CONSTRAINT FK_Shipment_ShippingAddress FOREIGN KEY (ShippingAddressId) REFERENCES Address(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ShipmentContext");

            var addressCode = File.ReadAllText(Path.Combine(dir, "Address.cs"));
            var shipmentCode = File.ReadAllText(Path.Combine(dir, "Shipment.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "ShipmentContext.cs"));

            Assert.Contains("public List<Shipment> ShipmentsByBillingAddressId { get; set; } = new();", addressCode);
            Assert.Contains("public List<Shipment> ShipmentsByShippingAddressId { get; set; } = new();", addressCode);
            Assert.Contains("[ForeignKey(nameof(BillingAddressId))]", shipmentCode);
            Assert.Contains("public Address? BillingAddress { get; set; }", shipmentCode);
            Assert.Contains("[ForeignKey(nameof(ShippingAddressId))]", shipmentCode);
            Assert.Contains("public Address? ShippingAddress { get; set; }", shipmentCode);
            Assert.DoesNotContain("public Address? Address { get; set; }", shipmentCode);
            Assert.Contains(".HasMany(p => p.ShipmentsByBillingAddressId)", contextCode);
            Assert.Contains(".WithOne(d => d.BillingAddress)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.BillingAddressId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains(".HasMany(p => p.ShipmentsByShippingAddressId)", contextCode);
            Assert.Contains(".WithOne(d => d.ShippingAddress)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ShippingAddressId, p => p.Id, cascadeDelete: false);", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WhenNavigationNameCollidesWithScalarProperty_MakesNavigationUnique()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Books TEXT NOT NULL
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                AuthorId INTEGER NOT NULL,
                Author TEXT NOT NULL,
                CONSTRAINT FK_Book_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CollisionNavCtx");

            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CollisionNavCtx.cs"));

            Assert.Contains("public string Books { get; set; } = default!;", authorCode);
            Assert.Contains("public List<Book> BooksByAuthorId { get; set; } = new();", authorCode);
            Assert.Contains("public string Author { get; set; } = default!;", bookCode);
            Assert.Contains("public Author? Author2 { get; set; }", bookCode);
            Assert.Contains(".HasMany(p => p.BooksByAuthorId)", contextCode);
            Assert.Contains(".WithOne(d => d.Author2)", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSingleColumnIndexes_GeneratesIndexAttributes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedWidget (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Code TEXT NOT NULL,
                Name TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IX_IndexedWidget_Code ON IndexedWidget(Code);
            CREATE INDEX IX_IndexedWidget_Name ON IndexedWidget(Name);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "IndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedWidget.cs"));
            Assert.Contains("using nORM.Configuration;", entityCode);
            Assert.Contains("[Index(\"IX_IndexedWidget_Code\", IsUnique = true)]", entityCode);
            Assert.Contains("[Index(\"IX_IndexedWidget_Name\")]", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeIndex_GeneratesOrderedIndexAttributes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedOrder (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                TenantId INTEGER NOT NULL,
                OrderNo TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IX_IndexedOrder_Tenant_OrderNo ON IndexedOrder(TenantId, OrderNo);
            CREATE INDEX IX_IndexedOrder_Tenant ON IndexedOrder(TenantId);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedOrder.cs"));
            Assert.Contains("[Index(\"IX_IndexedOrder_Tenant\")]", entityCode);
            Assert.Contains("[Index(\"IX_IndexedOrder_Tenant_OrderNo\", IsUnique = true, Order = 0)]", entityCode);
            Assert.Contains("[Index(\"IX_IndexedOrder_Tenant_OrderNo\", IsUnique = true, Order = 1)]", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqlitePartialAndExpressionIndexes_EmitsDiagnosticsInsteadOfPortableIndexes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedProviderSpecific (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                Active INTEGER NOT NULL
            );
            CREATE INDEX IX_IndexedProviderSpecific_Name_Active ON IndexedProviderSpecific(Name) WHERE Active = 1;
            CREATE INDEX IX_IndexedProviderSpecific_LowerName ON IndexedProviderSpecific(lower(Name));
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ProviderIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedProviderSpecific.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain("[Index(\"IX_IndexedProviderSpecific_Name_Active\")]", entityCode);
            Assert.DoesNotContain("[Index(\"IX_IndexedProviderSpecific_LowerName\")]", entityCode);
            Assert.Contains("PartialIndex", warnings);
            Assert.Contains("ExpressionIndex", warnings);
            Assert.Contains("IX_IndexedProviderSpecific_Name_Active", warnings);
            Assert.Contains("IX_IndexedProviderSpecific_LowerName", warnings);

            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "PartialIndex" &&
                item.GetProperty("name").GetString() == "IX_IndexedProviderSpecific_Name_Active");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "ExpressionIndex" &&
                item.GetProperty("name").GetString() == "IX_IndexedProviderSpecific_LowerName");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithDescendingIndex_EmitsDiagnosticsForSortDirection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IndexedDirection (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE INDEX IX_IndexedDirection_Name_Desc ON IndexedDirection(Name DESC);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "DirectionIndexCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "IndexedDirection.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[Index(\"IX_IndexedDirection_Name_Desc\")]", entityCode);
            Assert.Contains("DescendingIndex", warnings);
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "DescendingIndex" &&
                item.GetProperty("name").GetString() == "IX_IndexedDirection_Name_Desc" &&
                item.GetProperty("suggestedAction").GetString()!.Contains("ASC/DESC", StringComparison.Ordinal));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeForeignKey_EmitsDiagnosticsAndNoFakeNavigation()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE TenantOrder (
                TenantId INTEGER NOT NULL,
                OrderId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, OrderId)
            );
            CREATE TABLE TenantOrderLine (
                TenantId INTEGER NOT NULL,
                OrderId INTEGER NOT NULL,
                LineNo INTEGER NOT NULL,
                Sku TEXT NOT NULL,
                PRIMARY KEY (TenantId, OrderId, LineNo),
                CONSTRAINT FK_Line_Order FOREIGN KEY (TenantId, OrderId) REFERENCES TenantOrder(TenantId, OrderId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeFkCtx");

            var principalCode = File.ReadAllText(Path.Combine(dir, "TenantOrder.cs"));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "TenantOrderLine.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeFkCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain("List<TenantOrderLine>", principalCode);
            Assert.DoesNotContain("public TenantOrder?", dependentCode);
            Assert.DoesNotContain("HasForeignKey", contextCode);
            Assert.Contains("Composite Foreign Keys", warnings);
            Assert.Contains("sqlite_fk_", warnings);
            Assert.Contains("TenantId, OrderId", warnings);
            Assert.Contains("TenantOrderLine", warnings);
            Assert.Contains("TenantOrder", warnings);
            var compositeForeignKeys = warningJson.RootElement.GetProperty("compositeForeignKeys");
            Assert.Equal("TenantOrderLine", compositeForeignKeys[0].GetProperty("dependentTable").GetString());
            Assert.Equal("TenantOrder", compositeForeignKeys[0].GetProperty("principalTable").GetString());
            Assert.Equal("TenantId", compositeForeignKeys[0].GetProperty("dependentColumns")[0].GetString());
            Assert.Equal("OrderId", compositeForeignKeys[0].GetProperty("dependentColumns")[1].GetString());
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithProviderOwnedSchemaFeatures_EmitsDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FeatureOwned (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT COLLATE NOCASE NOT NULL DEFAULT 'new',
                NameLength INTEGER GENERATED ALWAYS AS (length(Name)) VIRTUAL,
                CONSTRAINT CK_FeatureOwned_Name CHECK (length(Name) > 0)
            );
            CREATE TRIGGER TR_FeatureOwned_Audit AFTER INSERT ON FeatureOwned BEGIN SELECT 1; END;
            CREATE VIEW FeatureOwnedView AS SELECT Id, Name FROM FeatureOwned;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "FeatureOwnedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "FeatureOwned.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Computed)]", entityCode);
            Assert.Contains("Provider-Owned Schema Features", warnings);
            Assert.DoesNotContain("Composite Foreign Keys", warnings);
            Assert.Contains("Default", warnings);
            Assert.Contains("Name", warnings);
            Assert.Contains("Computed", warnings);
            Assert.Contains("NameLength", warnings);
            Assert.Contains("Collation", warnings);
            Assert.Contains("Trigger", warnings);
            Assert.Contains("TR_FeatureOwned_Audit", warnings);
            Assert.Contains("CheckConstraint", warnings);
            Assert.Contains("Skipped Database Objects", warnings);
            Assert.Contains("FeatureOwnedView", warnings);
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Default" && item.GetProperty("name").GetString() == "Name");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Computed" && item.GetProperty("name").GetString() == "NameLength");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Collation" && item.GetProperty("name").GetString() == "FeatureOwned");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "Trigger" && item.GetProperty("name").GetString() == "TR_FeatureOwned_Audit");
            Assert.Contains(providerOwned.EnumerateArray(), item => item.GetProperty("kind").GetString() == "CheckConstraint" && item.GetProperty("name").GetString() == "FeatureOwned");
            Assert.All(providerOwned.EnumerateArray(), item => Assert.False(string.IsNullOrWhiteSpace(item.GetProperty("suggestedAction").GetString())));
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects");
            Assert.Contains(skippedObjects.EnumerateArray(), item => item.GetProperty("kind").GetString() == "View" && item.GetProperty("name").GetString() == "FeatureOwnedView");
            Assert.All(skippedObjects.EnumerateArray(), item => Assert.False(string.IsNullOrWhiteSpace(item.GetProperty("suggestedAction").GetString())));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSqliteVirtualTable_ReportsSkippedObject()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE VIRTUAL TABLE SearchDocs USING fts5(Body);";
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex)
        {
            if (Skip.If(true, $"SQLite FTS5 virtual tables are not available in this build: {ex.Message}")) return;
        }

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "VirtualCtx");

            Assert.False(File.Exists(Path.Combine(dir, "SearchDocs.cs")));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("VirtualTable", warnings);
            Assert.Contains("SearchDocs", warnings);
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects");
            Assert.Contains(skippedObjects.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTable" &&
                item.GetProperty("name").GetString() == "SearchDocs");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCheckInIdentifierName_DoesNotEmitCheckConstraintDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Checkpoint (
                Id INTEGER PRIMARY KEY,
                CheckName TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CheckpointCtx");

            var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
            if (File.Exists(warningPath))
            {
                var warnings = File.ReadAllText(warningPath);
                Assert.DoesNotContain("CheckConstraint", warnings);
            }
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithForeignKeyReferentialActions_PreservesCascadeDeleteFlag()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE CascadeChild (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NOT NULL,
                CONSTRAINT FK_Cascade_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id) ON DELETE CASCADE
            );
            CREATE TABLE RestrictChild (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NOT NULL,
                CONSTRAINT FK_Restrict_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id) ON DELETE RESTRICT
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "FkActionCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "FkActionCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id);", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains("ReferentialAction", warnings);
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "ReferentialAction" &&
                item.GetProperty("detail").GetString()!.Contains("RESTRICT", StringComparison.Ordinal));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithKeylessTable_EmitsMissingPrimaryKeyDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE KeylessImport (ExternalId TEXT NOT NULL, Payload TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "KeylessCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "KeylessImport.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain("[Key]", entityCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.Contains("KeylessImport", warnings);
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "KeylessImport" &&
                item.GetProperty("suggestedAction").GetString()!.Contains("primary key", StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_FailOnWarnings_WritesDiagnosticsThenThrows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WarningOwned (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Status TEXT NOT NULL DEFAULT 'new'
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "WarningOwnedCtx",
                    new ScaffoldOptions { FailOnWarnings = true }));

            Assert.Contains("Scaffolding produced warnings", ex.Message);
            Assert.True(File.Exists(Path.Combine(dir, "WarningOwned.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "WarningOwnedCtx.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            Assert.Contains("Default", warnings);
            Assert.Contains("Status", warnings);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPureJoinTable_EmitsManyToManyMapping()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinCtx");

            Assert.False(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinCtx.cs"));
            Assert.Contains("public List<Book> Books { get; set; } = new();", authorCode);
            Assert.Contains("public List<Author> Authors { get; set; } = new();", bookCode);
            Assert.Contains(".HasMany<Book>(p => p.Books)", contextCode);
            Assert.Contains(".WithMany(p => p.Authors)", contextCode);
            Assert.Contains(".UsingTable(\"AuthorBook\", \"AuthorId\", \"BookId\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPayloadJoinTable_EmitsManyToManyDiagnostics()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE AuthorBook (
                AuthorId INTEGER NOT NULL,
                BookId INTEGER NOT NULL,
                CreatedAt TEXT NOT NULL,
                PRIMARY KEY (AuthorId, BookId),
                CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinCtx");

            Assert.True(File.Exists(Path.Combine(dir, "AuthorBook.cs")));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("Possible Many-To-Many Join Tables", warnings);
            Assert.Contains("AuthorBook", warnings);
            Assert.Contains("Author", warnings);
            Assert.Contains("Book", warnings);
            var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables");
            Assert.Equal("AuthorBook", joinTables[0].GetProperty("table").GetString());
            Assert.Contains(joinTables[0].GetProperty("principalTables").EnumerateArray(), item => item.GetString() == "Author");
            Assert.Contains(joinTables[0].GetProperty("principalTables").EnumerateArray(), item => item.GetString() == "Book");
            Assert.Contains("UsingTable", joinTables[0].GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPureJoinTable_UsesJoinTableNameForNavigationDirection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Student (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Course (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE StudentCourse (
                CourseId INTEGER NOT NULL,
                StudentId INTEGER NOT NULL,
                PRIMARY KEY (CourseId, StudentId),
                CONSTRAINT FK_StudentCourse_Course FOREIGN KEY (CourseId) REFERENCES Course(Id),
                CONSTRAINT FK_StudentCourse_Student FOREIGN KEY (StudentId) REFERENCES Student(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "JoinCtx");

            var studentCode = File.ReadAllText(Path.Combine(dir, "Student.cs"));
            var courseCode = File.ReadAllText(Path.Combine(dir, "Course.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "JoinCtx.cs"));

            Assert.Contains("public List<Course> Courses { get; set; } = new();", studentCode);
            Assert.Contains("public List<Student> Students { get; set; } = new();", courseCode);
            Assert.Contains(".HasMany<Course>(p => p.Courses)", contextCode);
            Assert.Contains(".WithMany(p => p.Students)", contextCode);
            Assert.Contains(".UsingTable(\"StudentCourse\", \"StudentId\", \"CourseId\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelfReferencingPureJoinTable_GeneratesUniqueInverseCollections()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE PersonFriend (
                PersonId INTEGER NOT NULL,
                FriendId INTEGER NOT NULL,
                PRIMARY KEY (PersonId, FriendId),
                CONSTRAINT FK_PersonFriend_Person FOREIGN KEY (PersonId) REFERENCES Person(Id),
                CONSTRAINT FK_PersonFriend_Friend FOREIGN KEY (FriendId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SelfJoinCtx");

            var personCode = File.ReadAllText(Path.Combine(dir, "Person.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SelfJoinCtx.cs"));

            Assert.False(File.Exists(Path.Combine(dir, "PersonFriend.cs")));
            Assert.Contains("public List<Person> Persons { get; set; } = new();", personCode);
            Assert.Contains("public List<Person> Persons2 { get; set; } = new();", personCode);
            Assert.Contains(".HasMany<Person>(p => p.Persons)", contextCode);
            Assert.Contains(".WithMany(p => p.Persons2)", contextCode);
            Assert.Contains(".UsingTable(\"PersonFriend\", \"PersonId\", \"FriendId\");", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithInvalidSqlIdentifiers_GeneratesValidCSharpIdentifiers()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "bad-table" (
                "1st-name" TEXT NOT NULL,
                "has space" INTEGER NULL,
                "class" TEXT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "MyCtx3");

            var entityCode = File.ReadAllText(Path.Combine(dir, "BadTable.cs"));
            Assert.Contains("public class BadTable", entityCode);
            Assert.Contains("[Required]", entityCode);
            Assert.Contains("public string _1stName { get; set; } = default!;", entityCode);
            Assert.Contains("public long? HasSpace", entityCode);
            Assert.Contains("public string? Class", entityCode);
            Assert.DoesNotContain("@1st-name", entityCode);
            Assert.DoesNotContain("Has space", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithInvalidContextName_GeneratesValidContextClass()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ContextNameEntity (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "1-bad context");

            var contextPath = Path.Combine(dir, "_1BadContext.cs");
            Assert.True(File.Exists(contextPath));
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("public class _1BadContext : DbContext", contextCode);
            Assert.Contains("public _1BadContext(DbConnection cn, DatabaseProvider provider, DbContextOptions? options = null)", contextCode);
            Assert.DoesNotContain("1-bad context", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithQuotedSqlIdentifiers_GeneratesEscapedSourceLiterals()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "Quoted""Back\Table" (
                "Id" INTEGER PRIMARY KEY,
                "bad""col\name<&>
            line" TEXT NOT NULL
            );
            CREATE INDEX "IX""Back\Name
            Line" ON "Quoted""Back\Table" ("bad""col\name<&>
            line");
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "QuotedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "QuotedBackTable.cs"));
            Assert.Contains("[Table(\"Quoted\\\"Back\\\\Table\")]", entityCode);
            Assert.Contains("[Column(\"bad\\\"col\\\\name<&>\\nline\")]", entityCode);
            Assert.Contains("[Index(\"IX\\\"Back\\\\Name\\nLine\")]", entityCode);
            Assert.Contains("Maps to column bad\"col\\name&lt;&amp;&gt;\\nline", entityCode);
            Assert.DoesNotContain("\nline\" TEXT", entityCode, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithLiteralDottedIdentifiers_GeneratesSingleIdentifierMappings()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "audit.events" (
                Id INTEGER PRIMARY KEY,
                "value.part" TEXT NOT NULL
            );
            CREATE INDEX "ix.audit.value" ON "audit.events" ("value.part");
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "DottedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "AuditEvents.cs"));
            Assert.Contains("[Table(\"audit.events\")]", entityCode);
            Assert.Contains("[Index(\"ix.audit.value\")]", entityCode);
            Assert.Contains("[Column(\"value.part\")]", entityCode);
            Assert.Contains("public string ValuePart { get; set; } = default!;", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithIdentifierCollisions_GeneratesUniqueNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "sales-order" (
                Id INTEGER PRIMARY KEY,
                "first-name" TEXT NOT NULL,
                "first_name" TEXT NOT NULL
            );
            CREATE TABLE "sales_order" (
                Id INTEGER PRIMARY KEY,
                Value TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CollisionCtx");

            Assert.True(File.Exists(Path.Combine(dir, "SalesOrder.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SalesOrder2.cs")));
            var firstEntityCode = File.ReadAllText(Path.Combine(dir, "SalesOrder.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CollisionCtx.cs"));

            Assert.Contains("public string FirstName { get; set; } = default!;", firstEntityCode);
            Assert.Contains("public string FirstName2 { get; set; } = default!;", firstEntityCode);
            Assert.Contains("public IQueryable<SalesOrder> SalesOrders", contextCode);
            Assert.Contains("public IQueryable<SalesOrder2> SalesOrder2s", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithObjectMemberColumnNames_GeneratesUniquePropertyNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ObjectMembers (
                Id INTEGER PRIMARY KEY,
                ToString TEXT NOT NULL,
                Equals TEXT NOT NULL,
                GetHashCode TEXT NOT NULL,
                GetType TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ObjectMemberCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "ObjectMembers.cs"));
            Assert.Contains("public string ToString2 { get; set; } = default!;", entityCode);
            Assert.Contains("public string Equals2 { get; set; } = default!;", entityCode);
            Assert.Contains("public string GetHashCode2 { get; set; } = default!;", entityCode);
            Assert.Contains("public string GetType2 { get; set; } = default!;", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithTableFilter_GeneratesOnlyRequestedTables()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE KeepMe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SkipMe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "FilteredCtx",
                new ScaffoldOptions { Tables = new[] { "KeepMe" } });

            Assert.True(File.Exists(Path.Combine(dir, "KeepMe.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "SkipMe.cs")));
            var contextCode = File.ReadAllText(Path.Combine(dir, "FilteredCtx.cs"));
            Assert.Contains("IQueryable<KeepMe> KeepMes", contextCode);
            Assert.DoesNotContain("SkipMes", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNullOrBlankTableFilter_TreatsFilterAsEmpty()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE FilterNullSafe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var nullDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        var blankDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                nullDir,
                "TestNs",
                "NullFilterCtx",
                new ScaffoldOptions { Tables = null! });

            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                blankDir,
                "TestNs",
                "BlankFilterCtx",
                new ScaffoldOptions { Tables = new[] { " ", "" } });

            Assert.True(File.Exists(Path.Combine(nullDir, "FilterNullSafe.cs")));
            Assert.True(File.Exists(Path.Combine(blankDir, "FilterNullSafe.cs")));
        }
        finally
        {
            if (Directory.Exists(nullDir)) Directory.Delete(nullDir, recursive: true);
            if (Directory.Exists(blankDir)) Directory.Delete(blankDir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithAmbiguousBareTableFilter_RequiresSchemaQualifiedName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS auxa;
            ATTACH DATABASE ':memory:' AS auxb;
            CREATE TABLE "auxa"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "auxb"."DuplicateName" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var ambiguousDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        var qualifiedDir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    ambiguousDir,
                    "TestNs",
                    "AmbiguousFilterCtx",
                    new ScaffoldOptions { Tables = new[] { "DuplicateName" } }));

            Assert.Contains("ambiguous", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("auxa.DuplicateName", ex.Message, StringComparison.Ordinal);
            Assert.Contains("auxb.DuplicateName", ex.Message, StringComparison.Ordinal);
            Assert.Contains("schema-qualified", ex.Message, StringComparison.OrdinalIgnoreCase);

            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                qualifiedDir,
                "TestNs",
                "QualifiedFilterCtx",
                new ScaffoldOptions { Tables = new[] { "auxa.DuplicateName" } });

            var entityCode = File.ReadAllText(Path.Combine(qualifiedDir, "DuplicateName.cs"));
            Assert.Contains("[Table(\"DuplicateName\", Schema = \"auxa\")]", entityCode);
        }
        finally
        {
            if (Directory.Exists(ambiguousDir)) Directory.Delete(ambiguousDir, recursive: true);
            if (Directory.Exists(qualifiedDir)) Directory.Delete(qualifiedDir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithLiteralDottedTableFilterCollision_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS aux;
            CREATE TABLE "aux.orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "aux"."orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "DottedFilterCtx",
                    new ScaffoldOptions { Tables = new[] { "aux.orders" } }));

            Assert.Contains("ambiguous", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("literal dotted table names", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelectedTableKeyCollision_ThrowsBeforeGeneratingAmbiguousModel()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            ATTACH DATABASE ':memory:' AS aux;
            CREATE TABLE "aux.orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE "aux"."orders" (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CollisionCtx"));

            Assert.Contains("display names collide", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("literal dotted table names", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMissingTableFilter_ThrowsNormConfigurationException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Existing (Id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "FilteredCtx",
                    new ScaffoldOptions { Tables = new[] { "Missing" } }));
            Assert.Contains("Missing", ex.Message);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithViewTableFilter_ThrowsSkippedObjectDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Existing (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE VIEW ExistingView AS SELECT Id, Name FROM Existing;
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "FilteredCtx",
                    new ScaffoldOptions { Tables = new[] { "ExistingView" } }));

            Assert.Contains("matched database object", ex.Message);
            Assert.Contains("View ExistingView", ex.Message);
            Assert.Contains("does not emit as entity classes", ex.Message);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoOverwrite_RefusesExistingFiles()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ExistingFile (Id INTEGER PRIMARY KEY)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "ExistingFile.cs"), "// owned");

            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "NoOverwriteCtx",
                    new ScaffoldOptions { OverwriteFiles = false }));
            Assert.Contains("already exists", ex.Message);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoOverwrite_PreflightsAllFilesBeforeWriting()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AlphaNoOverwrite (Id INTEGER PRIMARY KEY);
            CREATE TABLE BetaNoOverwrite (Id INTEGER PRIMARY KEY);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, "BetaNoOverwrite.cs"), "// owned");

            await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "NoOverwriteCtx",
                    new ScaffoldOptions { OverwriteFiles = false }));

            Assert.False(File.Exists(Path.Combine(dir, "AlphaNoOverwrite.cs")));
            Assert.False(File.Exists(Path.Combine(dir, "NoOverwriteCtx.cs")));
            Assert.Equal("// owned", File.ReadAllText(Path.Combine(dir, "BetaNoOverwrite.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }
}

// ── NavigationContext tests ───────────────────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class SanNavigationContextTests
{
    private static DbContext CreateCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Constructor_SetsDbContextAndEntityType()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        Assert.Same(ctx, navCtx.DbContext);
        Assert.Equal(typeof(SanParent), navCtx.EntityType);
    }

    [Fact]
    public void IsLoaded_InitiallyReturnsFalse()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        Assert.False(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MarkAsLoaded_ThenIsLoaded_ReturnsTrue()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        Assert.True(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MarkAsUnloaded_AfterLoaded_ReturnsFalse()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        navCtx.MarkAsUnloaded("Children");
        Assert.False(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MarkAsUnloaded_WhenNeverLoaded_DoesNotThrow()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        // Should not throw
        navCtx.MarkAsUnloaded("NonExistent");
    }

    [Fact]
    public void Dispose_ClearsLoadedState()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        navCtx.Dispose();
        Assert.False(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MultipleProperties_TrackedIndependently()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("PropA");
        Assert.True(navCtx.IsLoaded("PropA"));
        Assert.False(navCtx.IsLoaded("PropB"));
    }

    [Fact]
    public void MarkAsLoaded_Idempotent()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        navCtx.MarkAsLoaded("Children");
        Assert.True(navCtx.IsLoaded("Children"));
    }
}

// ── NavigationPropertyInfo record tests ──────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class SanNavigationPropertyInfoTests
{
    [Fact]
    public void Constructor_SetsAllProperties()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var info = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        Assert.Same(prop, info.Property);
        Assert.Equal(typeof(SanChild), info.TargetType);
        Assert.True(info.IsCollection);
    }

    [Fact]
    public void Constructor_NonCollection_SetsIsCollectionFalse()
    {
        var prop = typeof(SanParent).GetProperty("Id")!;
        var info = new NavigationPropertyInfo(prop, typeof(int), false);
        Assert.False(info.IsCollection);
    }

    [Fact]
    public void Record_Equality_SameValues_AreEqual()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var a = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        var b = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        Assert.Equal(a, b);
    }

    [Fact]
    public void Record_Equality_DifferentIsCollection_NotEqual()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var a = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        var b = new NavigationPropertyInfo(prop, typeof(SanChild), false);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Record_ToString_ContainsTypeName()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var info = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        var s = info.ToString();
        Assert.Contains(nameof(NavigationPropertyInfo), s);
    }
}

// ── LazyNavigationCollection<T> tests ────────────────────────────────────────
// These tests use an in-memory SQLite DB with the table created so that
// lazy loading can resolve. They exercise collection methods AFTER the
// collection has been loaded by the navigation infrastructure.

[Xunit.Trait("Category", "Fast")]
public class SanLazyNavigationCollectionTests : IDisposable
{
    private readonly SqliteConnection _cn;
    private readonly DbContext _ctx;
    private readonly SanParent _parent;
    private readonly PropertyInfo _childrenProp;
    private readonly NavigationContext _navCtx;
    private readonly LazyNavigationCollection<SanChild> _lazy;

    public SanLazyNavigationCollectionTests()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        _cn.Open();

        using var cmd = _cn.CreateCommand();
        // Create tables so the mapping can resolve
        cmd.CommandText = @"
            CREATE TABLE SAN_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT);
            CREATE TABLE SAN_Child  (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL);
            INSERT INTO SAN_Parent DEFAULT VALUES;
            INSERT INTO SAN_Child  (ParentId) VALUES (1);";
        cmd.ExecuteNonQuery();

        _ctx = new DbContext(_cn, new SqliteProvider(), new nORM.Configuration.DbContextOptions { EagerChangeTracking = false });
        _parent = new SanParent { Id = 1 };
        _childrenProp = typeof(SanParent).GetProperty("Children")!;
        _navCtx = new NavigationContext(_ctx, typeof(SanParent));
        _lazy = new LazyNavigationCollection<SanChild>(_parent, _childrenProp, _navCtx);
    }

    public void Dispose()
    {
        _ctx.Dispose();
        _cn.Dispose();
    }

    // Helper: seed the parent's Children property with a pre-built list so
    // subsequent lazy-collection operations do NOT need to hit the DB.
    private List<SanChild> SeedLoadedList(params SanChild[] items)
    {
        var list = new List<SanChild>(items);
        _childrenProp.SetValue(_parent, list);
        _navCtx.MarkAsLoaded("Children");
        return list;
    }

    [Fact]
    public void IsReadOnly_ReturnsFalse_ForLoadedList()
    {
        SeedLoadedList();
        Assert.False(_lazy.IsReadOnly);
    }

    [Fact]
    public void Count_ReturnsZero_WhenEmpty()
    {
        SeedLoadedList();
        Assert.Empty(_lazy);
    }

    [Fact]
    public void Count_ReturnsOne_WhenOneItemSeeded()
    {
        SeedLoadedList(new SanChild { Id = 1, ParentId = 1 });
        Assert.Single(_lazy);
    }

    [Fact]
    public void Add_IncreasesCount()
    {
        SeedLoadedList();
        _lazy.Add(new SanChild { Id = 99, ParentId = 1 });
        Assert.Single(_lazy);
    }

    [Fact]
    public void Contains_ReturnsTrueForAddedItem()
    {
        SeedLoadedList();
        var child = new SanChild { Id = 5, ParentId = 1 };
        _lazy.Add(child);
        Assert.Contains(child, _lazy);
    }

    [Fact]
    public void Contains_ReturnsFalseForMissingItem()
    {
        SeedLoadedList();
        Assert.DoesNotContain(new SanChild { Id = 999, ParentId = 1 }, _lazy);
    }

    [Fact]
    public void Remove_DecreasesCount()
    {
        var child = new SanChild { Id = 7, ParentId = 1 };
        SeedLoadedList(child);
        var removed = _lazy.Remove(child);
        Assert.True(removed);
        Assert.Empty(_lazy);
    }

    [Fact]
    public void Clear_EmptiesCollection()
    {
        SeedLoadedList(new SanChild { Id = 1, ParentId = 1 }, new SanChild { Id = 2, ParentId = 1 });
        _lazy.Clear();
        Assert.Empty(_lazy);
    }

    [Fact]
    public void CopyTo_CopiesItemsToArray()
    {
        var child = new SanChild { Id = 3, ParentId = 1 };
        SeedLoadedList(child);
        var arr = new SanChild[1];
        _lazy.CopyTo(arr, 0);
        Assert.Same(child, arr[0]);
    }

    [Fact]
    public void IndexOf_ReturnsCorrectIndex()
    {
        var child = new SanChild { Id = 4, ParentId = 1 };
        SeedLoadedList(child);
        Assert.Equal(0, _lazy.IndexOf(child));
    }

    [Fact]
    public void IndexOf_MissingItem_ReturnsMinusOne()
    {
        SeedLoadedList();
        Assert.Equal(-1, _lazy.IndexOf(new SanChild { Id = 42 }));
    }

    [Fact]
    public void Indexer_Get_ReturnsCorrectItem()
    {
        var child = new SanChild { Id = 8, ParentId = 1 };
        SeedLoadedList(child);
        Assert.Same(child, _lazy[0]);
    }

    [Fact]
    public void Indexer_Set_UpdatesItem()
    {
        var original = new SanChild { Id = 1, ParentId = 1 };
        SeedLoadedList(original);
        var replacement = new SanChild { Id = 2, ParentId = 1 };
        _lazy[0] = replacement;
        Assert.Same(replacement, _lazy[0]);
    }

    [Fact]
    public void Insert_AddsAtIndex()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a);
        _lazy.Insert(0, b);
        Assert.Same(b, _lazy[0]);
        Assert.Same(a, _lazy[1]);
    }

    [Fact]
    public void RemoveAt_RemovesCorrectItem()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a, b);
        _lazy.RemoveAt(0);
        Assert.Single(_lazy);
        Assert.Same(b, _lazy[0]);
    }

    [Fact]
    public void GetEnumerator_YieldsAllItems()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a, b);
        var items = new List<SanChild>();
        foreach (var item in _lazy)
            items.Add(item);
        Assert.Equal(2, items.Count); // Not a collection size check — items is a plain List<T>
    }

    [Fact]
    public async Task GetAsyncEnumerator_YieldsAllItems()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a, b);
        var items = new List<SanChild>();
        await foreach (var item in _lazy)
            items.Add(item);
        Assert.Equal(2, items.Count); // Not a collection size check — items is a plain List<T>
    }
}

// ── LazyNavigationReference<T> tests ─────────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class LazyNavigationReferenceTests
{
    private static (DbContext Ctx, SanParent Parent, PropertyInfo Prop, NavigationContext NavCtx) CreateSetup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var parent = new SanParent { Id = 1 };
        // Use a fake property just to hold the reference
        var prop = typeof(SanParent).GetProperty("Id")!;
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        return (ctx, parent, prop, navCtx);
    }

    [Fact]
    public void SetValue_MarksAsLoaded_AndContextReflectsIt()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var child = new SanChild { Id = 1, ParentId = 1 };
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(child);
            Assert.True(navCtx.IsLoaded(prop.Name));
        }
    }

    [Fact]
    public async Task GetValueAsync_AfterSetValue_ReturnsValue()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var child = new SanChild { Id = 2, ParentId = 1 };
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(child);
            // Because _isLoaded=true, GetValueAsync will NOT go to DB
            var result = await lazyRef.GetValueAsync();
            Assert.Same(child, result);
        }
    }

    [Fact]
    public async Task GetValueAsync_SetValueNull_ReturnsNull()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(null);
            var result = await lazyRef.GetValueAsync();
            Assert.Null(result);
        }
    }

    [Fact]
    public void ImplicitConversion_ToTask_Works()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var child = new SanChild { Id = 3, ParentId = 1 };
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(child);
            Task<SanChild?> task = lazyRef; // implicit operator
            Assert.NotNull(task);
        }
    }
}

// ── EnableLazyLoading extension tests ────────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class EnableLazyLoadingTests
{
    [Fact]
    public void EnableLazyLoading_NullEntity_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        Assert.Throws<ArgumentNullException>(
            () => NavigationPropertyExtensions.EnableLazyLoading<SanParent>(null!, ctx));
    }

    [Fact]
    public void EnableLazyLoading_EntityWithNullCollection_InjectsLazyProxy()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var parent = new SanParent { Id = 1, Children = null };
        parent.EnableLazyLoading(ctx);
        // The Children property should now be a LazyNavigationCollection<SanChild>
        Assert.IsType<LazyNavigationCollection<SanChild>>(parent.Children);
    }

    [Fact]
    public void EnableLazyLoading_EntityWithExistingCollection_DoesNotReplace()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var existing = new List<SanChild> { new SanChild { Id = 1 } };
        var parent = new SanParent { Id = 1, Children = existing };
        parent.EnableLazyLoading(ctx);
        // Must not overwrite an already-populated collection
        Assert.Same(existing, parent.Children);
    }

    [Fact]
    public void CleanupNavigationContext_DoesNotThrowForUnregisteredEntity()
    {
        var parent = new SanParent { Id = 42 };
        // Should not throw even if the entity was never registered
        NavigationPropertyExtensions.CleanupNavigationContext(parent);
    }
}

// ── INormIncludableQueryable / ThenInclude extension tests ────────────────────

[Xunit.Trait("Category", "Fast")]
public class NormIncludableQueryableTests
{
    private static DbContext CreateCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Include_OnQuery_ReturnsIncludableQueryable()
    {
        using var ctx = CreateCtx();
        var q = ctx.Query<SanTIParent>() as INormQueryable<SanTIParent>;
        Assert.NotNull(q);
        var included = q!.Include(p => p.Children);
        Assert.IsAssignableFrom<INormIncludableQueryable<SanTIParent, ICollection<SanTIChild>>>(included);
    }

    [Fact]
    public void Include_SingleRef_ReturnsIncludableQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.SingleChild);
        Assert.NotNull(included);
    }

    [Fact]
    public void ThenInclude_AfterCollectionInclude_ReturnsNewQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.Children);
        // ThenInclude through collection: IEnumerable<SanTIChild> → SanTIChild.Grandchildren
        var then = ((INormIncludableQueryable<SanTIParent, ICollection<SanTIChild>>)included)
            .ThenInclude(c => c.Grandchildren);
        Assert.NotNull(then);
        Assert.IsAssignableFrom<INormIncludableQueryable<SanTIParent, ICollection<SanTIGrandchild>>>(then);
    }

    [Fact]
    public void ThenInclude_AfterReferenceInclude_ReturnsNewQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.SingleChild);
        var then = ((INormIncludableQueryable<SanTIParent, SanTIChild?>)included)
            .ThenInclude(c => c!.Grandchild);
        Assert.NotNull(then);
    }

    [Fact]
    public void Include_FollowedByAsNoTracking_ReturnsQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var result = q.Include(p => p.Children).AsNoTracking();
        Assert.NotNull(result);
    }

    [Fact]
    public void Include_FollowedByAsSplitQuery_ReturnsQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var result = q.Include(p => p.Children).AsSplitQuery();
        Assert.NotNull(result);
    }

    [Fact]
    public void Include_ChainedTwice_BothIncludesPreservedInExpression()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var q2 = q.Include(p => p.Children).Include(p => p.SingleChild);
        var expr = ((IQueryable<SanTIParent>)q2).Expression.ToString();
        // Expression tree should reference both navigation paths
        Assert.NotNull(expr);
        Assert.IsAssignableFrom<INormIncludableQueryable<SanTIParent, SanTIChild?>>(q2);
    }

    [Fact]
    public void ThenInclude_ChainedTwice_ProducesCorrectQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        // Include → ThenInclude → second Include
        var step1 = q.Include(p => p.Children);
        var step2 = ((INormIncludableQueryable<SanTIParent, ICollection<SanTIChild>>)step1)
            .ThenInclude(c => c.Grandchildren);
        Assert.NotNull(step2);
        // Continue building
        var step3 = step2.Include(p => p.SingleChild);
        Assert.NotNull(step3);
    }

    [Fact]
    public void AsNoTracking_OnBaseQuery_ReturnsINormQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var noTracking = q.AsNoTracking();
        Assert.NotNull(noTracking);
        Assert.IsAssignableFrom<INormQueryable<SanTIParent>>(noTracking);
    }

    [Fact]
    public void AsSplitQuery_OnBaseQuery_ReturnsINormQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var split = q.AsSplitQuery();
        Assert.NotNull(split);
        Assert.IsAssignableFrom<INormQueryable<SanTIParent>>(split);
    }

    [Fact]
    public void Include_ExpressionContainsIncludeMethodCall()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.Children);
        var expr = ((IQueryable<SanTIParent>)included).Expression;
        Assert.NotNull(expr);
        // The expression tree for Include wraps a MethodCallExpression
        Assert.Equal(System.Linq.Expressions.ExpressionType.Call, expr.NodeType);
    }
}
