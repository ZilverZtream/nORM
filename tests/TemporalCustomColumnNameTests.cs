using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// M1 regression tests: temporal history-table DDL and trigger SQL must use the mapped
/// DB column name (Column.Name) not the CLR property name (Column.PropName) when they differ.
///
/// Root cause: All four providers build liveMap keyed by LiveColumnInfo.Name (DB column name)
/// but previously looked up using c.PropName (CLR property name), causing a miss whenever
/// [Column("custom")] or fluent HasColumnName("custom") was in use. Result: live-schema type
/// and nullability info was silently ignored and CLR defaults were used instead, causing drift.
///
/// Fix: liveMap lookup now uses c.Name (DB column name) in all four providers.
/// DDL and trigger column references also switched from c.PropName → c.Name.
/// </summary>
public class TemporalCustomColumnNameTests
{
    // ── Test entity with [Column] overrides so PropName ≠ DB column name ────────

    [Table("CustomColEntity")]
    private class CustomColEntity
    {
        [Key]
        [Column("entity_id")]
        public int Id { get; set; }

        [Column("entity_name")]
        public string Name { get; set; } = "";

        [Column("entity_score")]
        public decimal Score { get; set; }
    }

    /// <summary>Entity that uses default column names (PropName == Name) — regression baseline.</summary>
    [Table("DefaultColEntity")]
    private class DefaultColEntity
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private static nORM.Mapping.TableMapping GetMapping<T>() where T : class
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());
        return ctx.GetMapping(typeof(T));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SQLite provider — GenerateCreateHistoryTableSql
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Sqlite_HistoryDdl_UsesDbColumnName_NotPropName()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping<CustomColEntity>();
        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

        // DDL must reference DB column names
        Assert.Contains("entity_id", ddl);
        Assert.Contains("entity_name", ddl);
        Assert.Contains("entity_score", ddl);

        // Must NOT contain raw CLR property names when they differ from DB column names
        Assert.DoesNotContain("\"Id\"", ddl);
        Assert.DoesNotContain("\"Name\"", ddl);
        Assert.DoesNotContain("\"Score\"", ddl);
    }

    [Fact]
    public void Sqlite_HistoryDdl_LiveColumnLookup_ByDbName_NotPropName()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping<CustomColEntity>();

        // Live columns keyed by DB column name ("entity_id" not "Id")
        var liveColumns = new List<DatabaseProvider.LiveColumnInfo>
        {
            new("entity_id",    "INTEGER",  false),
            new("entity_name",  "TEXT",     false),
            new("entity_score", "REAL",     false),
        };

        var ddl = provider.GenerateCreateHistoryTableSql(mapping, liveColumns);

        // Live types must be used (not CLR fallbacks) — live lookup succeeded
        Assert.Contains("INTEGER", ddl, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("TEXT",    ddl, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("REAL",    ddl, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Sqlite_HistoryDdl_LiveColumnLookup_MismatchOnPropName_PreviouslyBroken()
    {
        // This test documents the bug: live columns using the DB name would NOT be found
        // if the lookup used c.PropName. After the fix, the lookup uses c.Name and the
        // live types ARE used even when PropName ≠ Name.
        var provider = new SqliteProvider();
        var mapping = GetMapping<CustomColEntity>();

        // Provide a live column for "entity_score" with a distinctive custom type
        var liveColumns = new List<DatabaseProvider.LiveColumnInfo>
        {
            new("entity_id",    "INTEGER",          false),
            new("entity_name",  "TEXT",             false),
            new("entity_score", "NUMERIC(18,4)",    false),  // custom type
        };

        var ddl = provider.GenerateCreateHistoryTableSql(mapping, liveColumns);

        // After fix: live type is used for "entity_score"
        Assert.Contains("NUMERIC(18,4)", ddl, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Sqlite_TriggerSql_UsesDbColumnName_NotPropName()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping<CustomColEntity>();
        var sql = provider.GenerateTemporalTriggersSql(mapping);

        Assert.Contains("entity_id",    sql);
        Assert.Contains("entity_name",  sql);
        Assert.Contains("entity_score", sql);

        // CLR property names must NOT appear as bare column references
        // (they might appear inside "CustomColEntity" table name, so only check for quoted property names)
        Assert.DoesNotContain("\"Id\"",    sql);
        Assert.DoesNotContain("\"Name\"",  sql);
        Assert.DoesNotContain("\"Score\"", sql);
    }

    [Fact]
    public void Sqlite_TriggerSql_KeyCondition_UsesDbColumnName()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping<CustomColEntity>();
        var sql = provider.GenerateTemporalTriggersSql(mapping);

        // Key condition in WHERE clause must use "entity_id" not "Id"
        Assert.Contains("entity_id", sql);
        // The condition should appear in an UPDATE ... WHERE clause
        Assert.Contains("WHERE", sql, System.StringComparison.OrdinalIgnoreCase);
    }

    // ── Regression: default-column-name entities are unaffected ─────────────────

    [Fact]
    public void Sqlite_DefaultColumnNames_HistoryDdl_Unchanged()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping<DefaultColEntity>();
        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

        // When PropName == Name, DDL should work exactly as before
        Assert.Contains("Id",   ddl);
        Assert.Contains("Name", ddl);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SQL Server provider
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void SqlServer_HistoryDdl_UsesDbColumnName_NotPropName()
    {
        var provider = new SqlServerProvider();
        var mapping = GetMapping<CustomColEntity>();
        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

        Assert.Contains("entity_id",    ddl);
        Assert.Contains("entity_name",  ddl);
        Assert.Contains("entity_score", ddl);

        Assert.DoesNotContain("[Id]",    ddl);
        Assert.DoesNotContain("[Name]",  ddl);
        Assert.DoesNotContain("[Score]", ddl);
    }

    [Fact]
    public void SqlServer_HistoryDdl_LiveColumnLookup_ByDbName()
    {
        var provider = new SqlServerProvider();
        var mapping = GetMapping<CustomColEntity>();

        var liveColumns = new List<DatabaseProvider.LiveColumnInfo>
        {
            new("entity_id",    "int",          false),
            new("entity_name",  "nvarchar(200)", false),
            new("entity_score", "decimal(18,4)", false),
        };

        var ddl = provider.GenerateCreateHistoryTableSql(mapping, liveColumns);

        Assert.Contains("nvarchar(200)",  ddl, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("decimal(18,4)",  ddl, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_TriggerSql_UsesDbColumnName_NotPropName()
    {
        var provider = new SqlServerProvider();
        var mapping = GetMapping<CustomColEntity>();
        var sql = provider.GenerateTemporalTriggersSql(mapping);

        Assert.Contains("entity_id",    sql);
        Assert.Contains("entity_name",  sql);
        Assert.Contains("entity_score", sql);

        Assert.DoesNotContain("[Id]",    sql);
        Assert.DoesNotContain("[Name]",  sql);
        Assert.DoesNotContain("[Score]", sql);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MySQL provider
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void MySql_HistoryDdl_UsesDbColumnName_NotPropName()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var mapping = GetMapping<CustomColEntity>();
        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

        Assert.Contains("entity_id",    ddl);
        Assert.Contains("entity_name",  ddl);
        Assert.Contains("entity_score", ddl);

        Assert.DoesNotContain("`Id`",    ddl);
        Assert.DoesNotContain("`Name`",  ddl);
        Assert.DoesNotContain("`Score`", ddl);
    }

    [Fact]
    public void MySql_HistoryDdl_LiveColumnLookup_ByDbName()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var mapping = GetMapping<CustomColEntity>();

        var liveColumns = new List<DatabaseProvider.LiveColumnInfo>
        {
            new("entity_id",    "int",              false),
            new("entity_name",  "varchar(200)",     false),
            new("entity_score", "decimal(18,4)",    false),
        };

        var ddl = provider.GenerateCreateHistoryTableSql(mapping, liveColumns);
        Assert.Contains("varchar(200)",  ddl, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("decimal(18,4)", ddl, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySql_TriggerSql_UsesDbColumnName_NotPropName()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var mapping = GetMapping<CustomColEntity>();
        var sql = provider.GenerateTemporalTriggersSql(mapping);

        Assert.Contains("entity_id",    sql);
        Assert.Contains("entity_name",  sql);
        Assert.Contains("entity_score", sql);

        Assert.DoesNotContain("`Id`",    sql);
        Assert.DoesNotContain("`Name`",  sql);
        Assert.DoesNotContain("`Score`", sql);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PostgreSQL provider
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void Postgres_HistoryDdl_UsesDbColumnName_NotPropName()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var mapping = GetMapping<CustomColEntity>();
        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

        Assert.Contains("entity_id",    ddl);
        Assert.Contains("entity_name",  ddl);
        Assert.Contains("entity_score", ddl);

        Assert.DoesNotContain("\"Id\"",    ddl);
        Assert.DoesNotContain("\"Name\"",  ddl);
        Assert.DoesNotContain("\"Score\"", ddl);
    }

    [Fact]
    public void Postgres_HistoryDdl_LiveColumnLookup_ByDbName()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var mapping = GetMapping<CustomColEntity>();

        var liveColumns = new List<DatabaseProvider.LiveColumnInfo>
        {
            new("entity_id",    "integer",          false),
            new("entity_name",  "varchar(200)",     false),
            new("entity_score", "numeric(18,4)",    false),
        };

        var ddl = provider.GenerateCreateHistoryTableSql(mapping, liveColumns);
        Assert.Contains("varchar(200)",  ddl, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("numeric(18,4)", ddl, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Postgres_TriggerSql_UsesDbColumnName_NotPropName()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var mapping = GetMapping<CustomColEntity>();
        var sql = provider.GenerateTemporalTriggersSql(mapping);

        Assert.Contains("entity_id",    sql);
        Assert.Contains("entity_name",  sql);
        Assert.Contains("entity_score", sql);

        Assert.DoesNotContain("\"Id\"",    sql);
        Assert.DoesNotContain("\"Name\"",  sql);
        Assert.DoesNotContain("\"Score\"", sql);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Cross-provider parity: all providers agree on DB column names
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void AllProviders_HistoryDdl_ContainSameDbColumnNames()
    {
        var mapping = GetMapping<CustomColEntity>();

        var providers = new DatabaseProvider[]
        {
            new SqliteProvider(),
            new SqlServerProvider(),
            new MySqlProvider(new SqliteParameterFactory()),
            new PostgresProvider(new SqliteParameterFactory()),
        };

        foreach (var provider in providers)
        {
            var ddl = provider.GenerateCreateHistoryTableSql(mapping);
            var providerName = provider.GetType().Name;

            Assert.True(ddl.Contains("entity_id"),
                $"{providerName} DDL missing 'entity_id': {ddl}");
            Assert.True(ddl.Contains("entity_name"),
                $"{providerName} DDL missing 'entity_name': {ddl}");
            Assert.True(ddl.Contains("entity_score"),
                $"{providerName} DDL missing 'entity_score': {ddl}");
        }
    }

    [Fact]
    public void AllProviders_TriggerSql_ContainSameDbColumnNames()
    {
        var mapping = GetMapping<CustomColEntity>();

        var providers = new DatabaseProvider[]
        {
            new SqliteProvider(),
            new SqlServerProvider(),
            new MySqlProvider(new SqliteParameterFactory()),
            new PostgresProvider(new SqliteParameterFactory()),
        };

        foreach (var provider in providers)
        {
            var sql = provider.GenerateTemporalTriggersSql(mapping);
            var providerName = provider.GetType().Name;

            Assert.True(sql.Contains("entity_id"),
                $"{providerName} trigger SQL missing 'entity_id': {sql}");
            Assert.True(sql.Contains("entity_name"),
                $"{providerName} trigger SQL missing 'entity_name': {sql}");
            Assert.True(sql.Contains("entity_score"),
                $"{providerName} trigger SQL missing 'entity_score': {sql}");
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Fluent HasColumnName — same fix applies to fluent-configured column names
    // ══════════════════════════════════════════════════════════════════════════

    [Table("FluentColEntity")]
    private class FluentColEntity
    {
        [Key] public int Id { get; set; }
        public string FirstName { get; set; } = "";
    }

    [Fact]
    public void Sqlite_FluentColumnName_HistoryDdl_UsesFluentDbName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var provider = new SqliteProvider();
        using var ctx = new DbContext(cn, provider, new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<FluentColEntity>()
                .Property(e => e.FirstName)
                .HasColumnName("first_name")
        });

        var mapping = ctx.GetMapping(typeof(FluentColEntity));
        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

        Assert.Contains("first_name", ddl);
        Assert.DoesNotContain("\"FirstName\"", ddl);
    }
}
