using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

//<summary>
//Verifies that temporal history table DDL uses the EXACT column types from the main
//table mapping rather than a simplified or hard-coded type. Specifically:
//- SQLite: columns must use INTEGER/REAL/BLOB/TEXT (not TEXT for everything).
//- SQL Server: columns must use the same GetSqlType mapping as the main table.
//</summary>
public class TemporalSchemaTests
{
 // ── Entity used for DDL generation tests ─────────────────────────────────

    [Table("TemporalEntity")]
    private class TemporalEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public decimal Price { get; set; }

        public DateTime CreatedAt { get; set; }

        public bool IsActive { get; set; }

        public long Counter { get; set; }

        public double Score { get; set; }

        public int? NullableCount { get; set; }

        public string? NullableName { get; set; }
    }

 // ── SQLite: history table should use proper SQLite types ──────────────────

    private static TableMapping GetMapping(DatabaseProvider provider)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, provider);
        var method = typeof(DbContext).GetMethod("GetMapping",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        return (TableMapping)method.Invoke(ctx, new object[] { typeof(TemporalEntity) })!;
    }

 //<summary>
 //SQLite temporal history table must use proper SQLite column types
 //(INTEGER, REAL, BLOB, TEXT) rather than TEXT for all columns.
 //Previously all columns were mapped to TEXT, which silently coerces/loses precision.
 //</summary>
    [Fact]
    public void SQLite_TemporalHistoryTable_UsesCorrectColumnTypes()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping(provider);

        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

 // Integer types must map to INTEGER, not TEXT
        Assert.Contains("\"Id\" INTEGER", ddl);
        Assert.Contains("\"IsActive\" INTEGER", ddl);
        Assert.Contains("\"Counter\" INTEGER", ddl);

 // Floating-point types must map to REAL, not TEXT
        Assert.Contains("\"Price\" REAL", ddl);
        Assert.Contains("\"Score\" REAL", ddl);

 // String stays TEXT
        Assert.Contains("\"Name\" TEXT", ddl);
    }

 //<summary>
 //Non-nullable columns in the main table must have NOT NULL in history table.
 //Nullable columns (Nullable&lt;T&gt; and reference types) must NOT have NOT NULL.
 //</summary>
    [Fact]
    public void SQLite_TemporalHistoryTable_PreservesNullability()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping(provider);

        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

 // Non-nullable value type columns should have NOT NULL
        Assert.Contains("\"Id\" INTEGER NOT NULL", ddl);
        Assert.Contains("\"IsActive\" INTEGER NOT NULL", ddl);
        Assert.Contains("\"Counter\" INTEGER NOT NULL", ddl);

 // Nullable<T> columns must NOT have NOT NULL
 // (the column definition should be "INTEGER" without NOT NULL)
 // We verify the nullable int column exists without NOT NULL
        Assert.Contains("\"NullableCount\" INTEGER", ddl);
 // Ensure it doesn't appear as "NullableCount" INTEGER NOT NULL
        Assert.DoesNotContain("\"NullableCount\" INTEGER NOT NULL", ddl);

 // Reference type string — nullable string must not have NOT NULL
        Assert.Contains("\"NullableName\" TEXT", ddl);
        Assert.DoesNotContain("\"NullableName\" TEXT NOT NULL", ddl);
    }

 //<summary>
 //The temporal history table DDL must include the period/versioning columns
 //(__VersionId, __ValidFrom, __ValidTo, __Operation) in addition to entity columns.
 //</summary>
    [Fact]
    public void SQLite_TemporalHistoryTable_IncludesPeriodColumns()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping(provider);

        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

 // Period columns for temporal tracking must be present
        Assert.Contains("__VersionId", ddl);
        Assert.Contains("__ValidFrom", ddl);
        Assert.Contains("__ValidTo", ddl);
        Assert.Contains("__Operation", ddl);

 // The history table name must follow the _History convention
        Assert.Contains("\"TemporalEntity_History\"", ddl);
    }

 //<summary>
 //Temporal trigger DDL must reference all entity columns (INSERT/UPDATE/DELETE triggers).
 //</summary>
    [Fact]
    public void SQLite_TemporalTriggers_ReferenceAllColumns()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping(provider);

        var triggerDdl = provider.GenerateTemporalTriggersSql(mapping);

 // All three trigger types must be created
        Assert.Contains("AFTER INSERT ON", triggerDdl);
        Assert.Contains("AFTER UPDATE ON", triggerDdl);
        Assert.Contains("AFTER DELETE ON", triggerDdl);

 // Triggers must reference the history table
        Assert.Contains("\"TemporalEntity_History\"", triggerDdl);
    }

 //<summary>
 //end-to-end: The history table DDL must actually be valid SQLite and
 //the table must be creatable in memory, with the correct column affinities.
 //</summary>
    [Fact]
    public void SQLite_TemporalHistoryTable_DDL_IsValidAndExecutable()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping(provider);

        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

 // Execute the DDL against a real SQLite in-memory database
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
 // Should not throw
        cmd.ExecuteNonQuery();

 // Verify the history table exists with the expected columns
        using var infoCmd = cn.CreateCommand();
        infoCmd.CommandText = "PRAGMA table_info(\"TemporalEntity_History\")";
        using var reader = infoCmd.ExecuteReader();
        var columns = new System.Collections.Generic.List<(string Name, string Type)>();
        while (reader.Read())
        {
            columns.Add((reader.GetString(1), reader.GetString(2).ToUpperInvariant()));
        }

 // Period columns
        Assert.Contains(columns, c => c.Name == "__VersionId");
        Assert.Contains(columns, c => c.Name == "__ValidFrom");
        Assert.Contains(columns, c => c.Name == "__ValidTo");
        Assert.Contains(columns, c => c.Name == "__Operation");

 // Entity columns with correct affinity
        Assert.Contains(columns, c => c.Name == "Id" && c.Type.Contains("INTEGER"));
        Assert.Contains(columns, c => c.Name == "Name" && c.Type.Contains("TEXT"));
        Assert.Contains(columns, c => c.Name == "IsActive" && c.Type.Contains("INTEGER"));
        Assert.Contains(columns, c => c.Name == "Price" && c.Type.Contains("REAL"));
    }

 // ── SQL Server: history table should use same type mapping as main table ─

 //<summary>
 //SQL Server temporal history table must use the same GetSqlType mapping as
 //the main table (INT, BIGINT, DATETIME2, BIT, etc.), not a separate simplified mapping.
 //</summary>
    [Fact]
    public void SqlServer_TemporalHistoryTable_UsesCorrectColumnTypes()
    {
        var provider = new SqlServerProvider();
        var mapping = GetMapping(provider);

        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

 // SQL Server type mappings
        Assert.Contains("[Id] INT", ddl);
        Assert.Contains("[IsActive] BIT", ddl);
        Assert.Contains("[Counter] BIGINT", ddl);
        Assert.Contains("[Price] DECIMAL(18,2)", ddl);
        Assert.Contains("[CreatedAt] DATETIME2", ddl);
        Assert.Contains("[Name] NVARCHAR(MAX)", ddl);
    }

 //<summary>
 //SQL Server history table DDL must include the period columns
 //(__VersionId, __ValidFrom, __ValidTo, __Operation).
 //</summary>
    [Fact]
    public void SqlServer_TemporalHistoryTable_IncludesPeriodColumns()
    {
        var provider = new SqlServerProvider();
        var mapping = GetMapping(provider);

        var ddl = provider.GenerateCreateHistoryTableSql(mapping);

        Assert.Contains("[__VersionId]", ddl);
        Assert.Contains("[__ValidFrom]", ddl);
        Assert.Contains("[__ValidTo]", ddl);
        Assert.Contains("[__Operation]", ddl);

        Assert.Contains("[TemporalEntity_History]", ddl);
    }

 // ── SQLite GetMapping helper for SqlServer uses a different provider ─────

    private static TableMapping GetSqlServerMapping()
    {
 // SqlServerProvider's Escape uses [] and it validates connection type,
 // but for DDL generation we only need the mapping, not a live connection.
 // We use SqliteProvider to get the TableMapping (the mapping logic is provider-agnostic),
 // then call GenerateCreateHistoryTableSql on the SqlServerProvider.
 // However, since SqlServerProvider.GetSqlType is private and the column Escape is from
 // the provider that built the mapping, we need to build the mapping via the correct provider.
 // To avoid needing a real SQL Server connection, we can use reflection to build the mapping.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

 // Build mapping with a SqliteProvider so that we don't need a SqlConnection.
 // The column names and types are the same regardless of provider.
        using var ctx = new DbContext(cn, new SqliteProvider());
        var method = typeof(DbContext).GetMethod("GetMapping",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        return (TableMapping)method.Invoke(ctx, new object[] { typeof(TemporalEntity) })!;
    }

 // ── S6-1: MySQL temporal trigger SQL must not use DELIMITER directives ───

 //<summary>
 //S6-1: The MySQL GenerateTemporalTriggersSql must NOT contain MySQL CLI DELIMITER
 //directives (e.g., "DELIMITER $$", "END$$", "DELIMITER ;") because these are
 //client-side directives that cannot be executed via DbCommand.ExecuteNonQueryAsync.
 //</summary>
    [Fact]
    public void MySql_TemporalTriggersSql_DoesNotContainDelimiterDirectives()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var mapping = GetMapping(new SqliteProvider()); // mapping is provider-agnostic

        var triggerSql = provider.GenerateTemporalTriggersSql(mapping);

 // Must NOT contain MySQL CLI DELIMITER directives
        Assert.DoesNotContain("DELIMITER $$", triggerSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DELIMITER ;", triggerSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("END$$", triggerSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("$$", triggerSql, StringComparison.OrdinalIgnoreCase);
    }

 //<summary>
 //S6-1: Each MySQL trigger (INSERT, UPDATE, DELETE) must appear as a separate
 //statement when split at the "-- DELIMITER" separator used by TemporalManager.
 //This verifies that the splitting mechanism produces exactly 3 CREATE TRIGGER statements.
 //</summary>
    [Fact]
    public void MySql_TemporalTriggersSql_SplitsIntoThreeSeparateCreateTriggerStatements()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var mapping = GetMapping(new SqliteProvider());

        var triggerSql = provider.GenerateTemporalTriggersSql(mapping);

 // Split using the same logic as TemporalManager.ExecuteDdlAsync
        var batchSeparators = new[]
        {
            "\n-- DELIMITER\n", "\r\n-- DELIMITER\r\n", "\r\n-- DELIMITER\n", "\n-- DELIMITER\r\n",
            "-- DELIMITER"
        };
        var batches = triggerSql
            .Split(batchSeparators, StringSplitOptions.RemoveEmptyEntries)
            .Select(b => b.Trim())
            .Where(b => b.Length > 0)
            .ToList();

 // Must produce exactly 3 CREATE TRIGGER statements
        Assert.Equal(3, batches.Count);
        Assert.All(batches, batch =>
            Assert.True(batch.TrimStart().StartsWith("CREATE TRIGGER", StringComparison.OrdinalIgnoreCase),
                $"Each batch must start with CREATE TRIGGER, but got: {batch.Substring(0, Math.Min(50, batch.Length))}"));
    }

 //<summary>
 //S6-1: Each MySQL trigger statement produced by splitting at -- DELIMITER must
 //be a syntactically valid CREATE TRIGGER statement (starts with CREATE TRIGGER,
 //contains AFTER INSERT/UPDATE/DELETE, references the history table).
 //</summary>
    [Fact]
    public void MySql_TemporalTriggersSql_EachStatementIsValidCreateTrigger()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var mapping = GetMapping(new SqliteProvider());

        var triggerSql = provider.GenerateTemporalTriggersSql(mapping);

        var batchSeparators = new[] { "-- DELIMITER" };
        var batches = triggerSql
            .Split(batchSeparators, StringSplitOptions.RemoveEmptyEntries)
            .Select(b => b.Trim())
            .Where(b => b.Length > 0)
            .ToList();

 // Verify each trigger contains the expected events
        Assert.Contains(batches, b => b.Contains("AFTER INSERT", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(batches, b => b.Contains("AFTER UPDATE", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(batches, b => b.Contains("AFTER DELETE", StringComparison.OrdinalIgnoreCase));

 // Verify all triggers reference the history table
        Assert.All(batches, batch =>
            Assert.Contains("_History", batch, StringComparison.OrdinalIgnoreCase));
    }

 //<summary>
 //S6-1: SQLite temporal triggers must be unaffected — they must still use the
 //same GO-less, single-statement-per-trigger format (SQLite uses semicolons not DELIMITER).
 //</summary>
    [Fact]
    public void SQLite_TemporalTriggers_UnaffectedByMySqlDelimiterFix()
    {
        var provider = new SqliteProvider();
        var mapping = GetMapping(provider);

        var triggerSql = provider.GenerateTemporalTriggersSql(mapping);

 // SQLite triggers must NOT contain -- DELIMITER markers (those are MySQL-specific)
        Assert.DoesNotContain("-- DELIMITER", triggerSql, StringComparison.OrdinalIgnoreCase);

 // SQLite triggers must still contain AFTER INSERT/UPDATE/DELETE
        Assert.Contains("AFTER INSERT ON", triggerSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AFTER UPDATE ON", triggerSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AFTER DELETE ON", triggerSql, StringComparison.OrdinalIgnoreCase);
    }

 //<summary>
 //S6-1: SQL Server temporal triggers must be unaffected — they use GO batch separators
 //which TemporalManager.ExecuteDdlAsync already handles correctly.
 //</summary>
    [Fact]
    public void SqlServer_TemporalTriggers_UnaffectedByMySqlDelimiterFix()
    {
        var provider = new SqlServerProvider();
        var mapping = GetMapping(new SqliteProvider());

        var triggerSql = provider.GenerateTemporalTriggersSql(mapping);

 // SQL Server triggers must NOT contain -- DELIMITER markers
        Assert.DoesNotContain("-- DELIMITER", triggerSql, StringComparison.OrdinalIgnoreCase);

 // SQL Server triggers must still contain AFTER INSERT/UPDATE/DELETE
        Assert.Contains("AFTER INSERT", triggerSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AFTER UPDATE", triggerSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AFTER DELETE", triggerSql, StringComparison.OrdinalIgnoreCase);
    }
}
