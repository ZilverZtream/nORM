using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using nORM.Mapping;
using nORM.SourceGeneration;
using Xunit;

#nullable enable

namespace nORM.Tests;

// SG1: [CompileTimeQuery] must not crash with KeyNotFoundException when entity lacks [GenerateMaterializer].
//      GetCompiledQueryMaterializer must fall back to runtime materialization or throw descriptively.
// SG2: Generated methods must respect MaterializerFactory guards (fluent rename / converter / OwnsOne).

// ── Entities WITHOUT [GenerateMaterializer] (SG1) ────────────────────────────
// Must be internal (not private-nested) so the source generator can reference them.

[Table("ctmg_unmapped")]
internal sealed class CtmgUnmappedEntity
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Value { get; set; } = string.Empty;
}

// Different type to avoid shared-static-cache cross-contamination
[Table("ctmg_unmapped2")]
internal sealed class CtmgUnmappedEntity2
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Value { get; set; } = string.Empty;
}

// ── Entities WITH [GenerateMaterializer] (SG2) ───────────────────────────────
// Must be internal so the source generator can emit a materializer for them.

[Table("ctmg_renamed")]
[GenerateMaterializer]
internal sealed class CtmgRenamedEntity
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    // No [Column] attr — source-gen uses CLR name "Value"
    // fluent HasColumnName("display_value") renames it at runtime
    public string Value { get; set; } = string.Empty;
}

[Table("ctmg_converted")]
[GenerateMaterializer]
internal sealed class CtmgConvertedEntity
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Payload { get; set; } = string.Empty;
}

// ── ValueConverter for SG2-2 ─────────────────────────────────────────────────

internal sealed class CtmgUpperCasePayloadConverter : ValueConverter<string, string>
{
    // ConvertToProvider: store as lowercase; ConvertFromProvider: return as uppercase
    public override object? ConvertToProvider(string value) => value?.ToLowerInvariant();
    public override object? ConvertFromProvider(string value) => (value as string)?.ToUpperInvariant() ?? value;
}

public class CompileTimeMaterializerGuardTests
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection OpenDb(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    // ── SG1-1: Mapped entity without [GenerateMaterializer] falls back to runtime ──────────────

    [Fact]
    public async Task SG1_MappedEntity_NoGenerateMaterializer_FallsBackToRuntime()
    {
        using var cn = OpenDb(
            "CREATE TABLE ctmg_unmapped (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value TEXT NOT NULL);" +
            "INSERT INTO ctmg_unmapped (Value) VALUES ('hello');");

        await using var ctx = new DbContext(cn, new SqliteProvider());
        // Touch the entity via Query so IsMapped returns true
        _ = await ctx.Query<CtmgUnmappedEntity>().ToListAsync();

        // Must not throw KeyNotFoundException — falls back to runtime materializer
        var materializer = ctx.GetCompiledQueryMaterializer<CtmgUnmappedEntity>("ctmg_unmapped");
        Assert.NotNull(materializer);

        await using var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Value FROM ctmg_unmapped";
        var results = await ctx.ExecuteCompiledQueryListAsync(cmd, materializer);
        Assert.Single(results);
        Assert.Equal("hello", results[0].Value);
    }

    // ── SG1-2: Unmapped entity, no compiled materializer → descriptive error ─────────────────

    [Fact]
    public async Task SG1_UnmappedEntity_NoCompiledMaterializer_ThrowsDescriptively()
    {
        using var cn = OpenDb("CREATE TABLE ctmg_unmapped2 (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value TEXT NOT NULL);");
        // Fresh context, entity never queried → IsMapped = false; no [GenerateMaterializer] → no compiled mat
        await using var ctx = new DbContext(cn, new SqliteProvider());

        var ex = Assert.Throws<InvalidOperationException>(
            () => ctx.GetCompiledQueryMaterializer<CtmgUnmappedEntity2>("ctmg_unmapped2"));

        // Must reference the table name and suggest [GenerateMaterializer]
        Assert.Contains("ctmg_unmapped2", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GenerateMaterializer", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ── SG2-1: Fluent HasColumnName bypasses compiled materializer (wrong GetOrdinal) ──────────

    [Fact]
    public async Task SG2_FluentColumnRename_GetCompiledQueryMaterializer_ReturnsCorrectValues()
    {
        // DB column is "display_value"; CLR property is "Value"; source-gen materializer would
        // call GetOrdinal("Value") → IndexOutOfRangeException. Runtime materializer knows "display_value".
        using var cn = OpenDb(
            "CREATE TABLE ctmg_renamed (Id INTEGER PRIMARY KEY AUTOINCREMENT, display_value TEXT NOT NULL);" +
            "INSERT INTO ctmg_renamed (display_value) VALUES ('correct_value');");

        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CtmgRenamedEntity>()
                .Property(x => x.Value).HasColumnName("display_value")
        });

        // With the fluent rename guard, GetCompiledQueryMaterializer must use runtime materializer.
        var materializer = ctx.GetCompiledQueryMaterializer<CtmgRenamedEntity>("ctmg_renamed");

        await using var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, display_value FROM ctmg_renamed";
        var results = await ctx.ExecuteCompiledQueryListAsync(cmd, materializer);
        Assert.Single(results);
        // If compiled materializer were used, this would either crash or return empty string.
        Assert.Equal("correct_value", results[0].Value);
    }

    // ── SG2-2: HasConversion bypasses compiled materializer (raw value, no converter applied) ──

    [Fact]
    public async Task SG2_ValueConverter_GetCompiledQueryMaterializer_AppliesConverter()
    {
        using var cn = OpenDb(
            "CREATE TABLE ctmg_converted (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL);" +
            "INSERT INTO ctmg_converted (Payload) VALUES ('hello');");

        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            // Converter: ConvertFromProvider returns uppercase → materializer must invoke it
            OnModelCreating = mb => mb.Entity<CtmgConvertedEntity>()
                .Property(x => x.Payload)
                .HasConversion(new CtmgUpperCasePayloadConverter())
        });

        // Compiled materializer would return "hello" (raw, no converter applied).
        // Runtime materializer applies the converter → returns "HELLO".
        var materializer = ctx.GetCompiledQueryMaterializer<CtmgConvertedEntity>("ctmg_converted");

        await using var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Payload FROM ctmg_converted";
        var results = await ctx.ExecuteCompiledQueryListAsync(cmd, materializer);
        Assert.Single(results);
        Assert.Equal("HELLO", results[0].Payload);
    }

    // ── SG2-3: No unsafe conditions → returns a working materializer ──────────────────────────

    [Fact]
    public async Task SG2_NoUnsafeConditions_GetCompiledQueryMaterializer_ReturnsWorkingMaterializer()
    {
        // Standard mapping: CLR prop name = DB column name, no converter, no owned navigations.
        using var cn = OpenDb(
            "CREATE TABLE ctmg_converted (Id INTEGER PRIMARY KEY AUTOINCREMENT, Payload TEXT NOT NULL);" +
            "INSERT INTO ctmg_converted (Payload) VALUES ('standard');");

        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CtmgConvertedEntity>()
        });

        var materializer = ctx.GetCompiledQueryMaterializer<CtmgConvertedEntity>("ctmg_converted");
        Assert.NotNull(materializer);

        await using var cmd = await ctx.CreateCompiledQueryCommandAsync();
        cmd.CommandText = "SELECT Id, Payload FROM ctmg_converted";
        var results = await ctx.ExecuteCompiledQueryListAsync(cmd, materializer);
        Assert.Single(results);
        Assert.Equal("standard", results[0].Payload);
    }
}
