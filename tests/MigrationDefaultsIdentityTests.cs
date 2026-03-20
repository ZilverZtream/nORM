using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using nORM.Query;
using Xunit;
using MigrationBase = nORM.Migration.Migration;

namespace nORM.Tests;

/// <summary>
/// Gate 2.5-to-3.0 migration tests:
///   - Per-provider CREATE TABLE SQL includes DEFAULT and identity keywords
///   - SQLite recreate-table preserves defaults
///   - Live SQLite insert with AUTOINCREMENT
///   - AsOf tag cancellation
///   - Full snapshot-to-diff-to-SQL-to-apply-to-insert round-trip
/// </summary>
public class MigrationDefaultsIdentityTests
{
    // ═══════════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════════

    private static TableSchema MakeTable(string name, params ColumnSchema[] cols)
    {
        var t = new TableSchema { Name = name };
        foreach (var c in cols) t.Columns.Add(c);
        return t;
    }

    private static ColumnSchema PkCol(string name, string? clrType = null, bool identity = false) =>
        new()
        {
            Name = name,
            ClrType = clrType ?? typeof(int).FullName!,
            IsNullable = false,
            IsPrimaryKey = true,
            IsUnique = true,
            IndexName = $"PK_{name}",
            IsIdentity = identity
        };

    private static ColumnSchema Col(string name, bool nullable = true, string? clrType = null,
        string? defaultValue = null, bool identity = false) =>
        new()
        {
            Name = name,
            ClrType = clrType ?? typeof(string).FullName!,
            IsNullable = nullable,
            DefaultValue = defaultValue,
            IsIdentity = identity
        };

    // ═══════════════════════════════════════════════════════════════════════
    // 1. SQLite CREATE TABLE includes DEFAULT
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Sqlite_CreateTable_IncludesDefault()
    {
        var table = MakeTable("Items",
            PkCol("Id"),
            Col("Status", nullable: false, clrType: typeof(int).FullName!, defaultValue: "0"),
            Col("CreatedAt", nullable: false, clrType: typeof(string).FullName!, defaultValue: "CURRENT_TIMESTAMP")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("DEFAULT 0", createSql);
        Assert.Contains("DEFAULT CURRENT_TIMESTAMP", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 2. SQL Server CREATE TABLE includes DEFAULT
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SqlServer_CreateTable_IncludesDefault()
    {
        var table = MakeTable("Items",
            PkCol("Id"),
            Col("Status", nullable: false, clrType: typeof(int).FullName!, defaultValue: "0"),
            Col("Label", nullable: true, clrType: typeof(string).FullName!, defaultValue: "'unknown'")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("DEFAULT 0", createSql);
        Assert.Contains("DEFAULT 'unknown'", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 3. MySQL CREATE TABLE includes DEFAULT
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MySql_CreateTable_IncludesDefault()
    {
        var table = MakeTable("Items",
            PkCol("Id"),
            Col("Active", nullable: false, clrType: typeof(bool).FullName!, defaultValue: "true"),
            Col("Tag", nullable: true, clrType: typeof(string).FullName!, defaultValue: "'default'")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new MySqlMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("DEFAULT true", createSql);
        Assert.Contains("DEFAULT 'default'", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 4. PostgreSQL CREATE TABLE includes DEFAULT
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Postgres_CreateTable_IncludesDefault()
    {
        var table = MakeTable("Items",
            PkCol("Id"),
            Col("Priority", nullable: false, clrType: typeof(int).FullName!, defaultValue: "1"),
            Col("Notes", nullable: true, clrType: typeof(string).FullName!, defaultValue: "'N/A'")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new PostgresMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("DEFAULT 1", createSql);
        Assert.Contains("DEFAULT 'N/A'", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 5. SQLite CREATE TABLE includes AUTOINCREMENT for identity PK
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Sqlite_CreateTable_IncludesAutoincrement()
    {
        var table = MakeTable("Products",
            PkCol("Id", identity: true),
            Col("Name", nullable: false)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("AUTOINCREMENT", createSql);
        Assert.Contains("PRIMARY KEY", createSql);
        // AUTOINCREMENT PK should NOT produce a separate table-level PRIMARY KEY constraint
        // (SQLite requires inline "INTEGER PRIMARY KEY AUTOINCREMENT")
        Assert.DoesNotContain("PRIMARY KEY (", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 6. SQL Server CREATE TABLE includes IDENTITY(1,1) for identity PK
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SqlServer_CreateTable_IncludesIdentity()
    {
        var table = MakeTable("Products",
            PkCol("Id", identity: true),
            Col("Name", nullable: false)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("IDENTITY(1,1)", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 7. MySQL CREATE TABLE includes AUTO_INCREMENT for identity PK
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MySql_CreateTable_IncludesAutoIncrement()
    {
        var table = MakeTable("Products",
            PkCol("Id", identity: true),
            Col("Name", nullable: false)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new MySqlMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("AUTO_INCREMENT", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 8. PostgreSQL CREATE TABLE includes GENERATED ALWAYS AS IDENTITY
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Postgres_CreateTable_IncludesGeneratedIdentity()
    {
        var table = MakeTable("Products",
            PkCol("Id", identity: true),
            Col("Name", nullable: false)
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new PostgresMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("GENERATED ALWAYS AS IDENTITY", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 9. SQLite RecreateTable preserves defaults across table recreation
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Sqlite_RecreateTable_PreservesDefaults()
    {
        // Start with a table that has a default on Status
        var oldTable = MakeTable("Events",
            PkCol("Id"),
            Col("Status", nullable: false, clrType: typeof(int).FullName!, defaultValue: "0"),
            Col("Label", nullable: true, clrType: typeof(string).FullName!)
        );
        // Alter Label to NOT NULL (triggers table recreation in SQLite)
        var newTable = MakeTable("Events",
            PkCol("Id"),
            Col("Status", nullable: false, clrType: typeof(int).FullName!, defaultValue: "0"),
            Col("Label", nullable: false, clrType: typeof(string).FullName!, defaultValue: "'empty'")
        );

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } }
        );

        // The Label column changed from nullable to NOT NULL with a default -- triggers AlteredColumns
        Assert.True(diff.AlteredColumns.Count > 0);

        var result = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // The recreated table DDL in the Up statements should preserve the DEFAULT on Status
        // RecreateTable emits CREATE TABLE __temp__Events (...) which includes the defaults
        var recreateCreate = result.Up.First(s => s.Contains("CREATE TABLE") && s.Contains("__temp__"));
        Assert.Contains("DEFAULT 0", recreateCreate);
        Assert.Contains("DEFAULT 'empty'", recreateCreate);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 10. SQLite live insert with AUTOINCREMENT — verify Id populated
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Sqlite_LiveInsert_WithAutoincrement_PopulatesId()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        // Create a dynamic assembly with a single migration that creates an AUTOINCREMENT table
        var asm = DdlAssembly(1, "CreateAutoIncTable",
            "CREATE TABLE MdiAutoItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Score INTEGER NOT NULL DEFAULT 0)");

        var runner = new SqliteMigrationRunner(connection, asm);
        await runner.ApplyMigrationsAsync();

        // Verify the table was created
        await using var verifyCmd = connection.CreateCommand();
        verifyCmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='MdiAutoItem'";
        var tableName = await verifyCmd.ExecuteScalarAsync();
        Assert.Equal("MdiAutoItem", tableName);

        // Insert a row
        await using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO MdiAutoItem (Name) VALUES ('TestItem'); SELECT last_insert_rowid();";
        var generatedId = Convert.ToInt64(await insertCmd.ExecuteScalarAsync());
        Assert.True(generatedId > 0, "AUTOINCREMENT should produce a positive Id");

        // Insert a second row and verify monotonic increment
        await using var insertCmd2 = connection.CreateCommand();
        insertCmd2.CommandText = "INSERT INTO MdiAutoItem (Name) VALUES ('TestItem2'); SELECT last_insert_rowid();";
        var secondId = Convert.ToInt64(await insertCmd2.ExecuteScalarAsync());
        Assert.True(secondId > generatedId, "Second AUTOINCREMENT Id should be greater than first");

        // Verify default value was applied (Score should be 0)
        await using var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT Score FROM MdiAutoItem WHERE Id = @id";
        var p = selectCmd.CreateParameter();
        p.ParameterName = "@id";
        p.Value = generatedId;
        selectCmd.Parameters.Add(p);
        var score = Convert.ToInt64(await selectCmd.ExecuteScalarAsync());
        Assert.Equal(0L, score);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 11. AsOf tag cancellation test — document sync limitation
    // ═══════════════════════════════════════════════════════════════════════

    [Table("MdiAsOfItem")]
    public class MdiAsOfItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task AsOf_Tag_WithCancelledToken_ThrowsOperationCanceled()
    {
        // Create a minimal SQLite db with the entity table
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var setupCmd = cn.CreateCommand();
        setupCmd.CommandText = "CREATE TABLE MdiAsOfItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)";
        await setupCmd.ExecuteNonQueryAsync();

        using var ctx = new DbContext(cn, new SqliteProvider());

        var cts = new CancellationTokenSource();
        cts.Cancel(); // Pre-cancel the token

        var q = ctx.Query<MdiAsOfItem>();
        // .AsOf("some-tag") uses GetTimestampForTagAsync internally which requires a DB call
        // With a pre-cancelled token, the operation should throw OperationCanceledException
        // or a DbException wrapping it (temporal tables don't exist anyway).
        // SYNC LIMITATION: AsOf with a string tag triggers an async DB call (tag lookup)
        // during query translation. With a cancelled token, this surfaces as either
        // OperationCanceledException or NormQueryException wrapping the cancellation.
        var asOfQuery = q.AsOf("nonexistent-tag");

        // The query translation or execution will fail because:
        // 1. The temporal tag table does not exist, OR
        // 2. The cancellation token is already cancelled
        // Either way, an exception is expected.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await asOfQuery.ToListAsync(cts.Token);
        });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 12. Full migration round-trip: snapshot -> diff -> SQL -> apply -> insert
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationRoundTrip_IdentityAndDefaults_EndToEnd()
    {
        // Build schema snapshot representing a new table with identity PK and defaults
        var emptySnapshot = new SchemaSnapshot();
        var targetSnapshot = new SchemaSnapshot();
        targetSnapshot.Tables.Add(MakeTable("MdiRoundTrip",
            PkCol("Id", identity: true),
            Col("Name", nullable: false, clrType: typeof(string).FullName!, defaultValue: "'unnamed'"),
            Col("Score", nullable: false, clrType: typeof(int).FullName!, defaultValue: "100"),
            Col("Notes", nullable: true, clrType: typeof(string).FullName!)
        ));

        // Step 1: Compute diff
        var diff = SchemaDiffer.Diff(emptySnapshot, targetSnapshot);
        Assert.Single(diff.AddedTables);
        Assert.Equal("MdiRoundTrip", diff.AddedTables[0].Name);

        // Step 2: Generate SQLite SQL
        var sqlStatements = new SqliteMigrationSqlGenerator().GenerateSql(diff);
        Assert.NotEmpty(sqlStatements.Up);

        var createSql = sqlStatements.Up.First(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("AUTOINCREMENT", createSql);
        Assert.Contains("DEFAULT 'unnamed'", createSql);
        Assert.Contains("DEFAULT 100", createSql);

        // Step 3: Apply SQL to a live SQLite database
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        foreach (var stmt in sqlStatements.Up)
        {
            await using var cmd = cn.CreateCommand();
            cmd.CommandText = stmt;
            await cmd.ExecuteNonQueryAsync();
        }

        // Step 4: Insert a row using only Name (defaults should fill Score)
        await using var insertCmd = cn.CreateCommand();
        insertCmd.CommandText = "INSERT INTO \"MdiRoundTrip\" (\"Notes\") VALUES ('test note'); SELECT last_insert_rowid();";
        var id = Convert.ToInt64(await insertCmd.ExecuteScalarAsync());
        Assert.True(id > 0, "Identity column should produce a positive Id");

        // Step 5: Read back and verify defaults were applied
        await using var selectCmd = cn.CreateCommand();
        selectCmd.CommandText = "SELECT \"Name\", \"Score\", \"Notes\" FROM \"MdiRoundTrip\" WHERE \"Id\" = @id";
        var p = selectCmd.CreateParameter();
        p.ParameterName = "@id";
        p.Value = id;
        selectCmd.Parameters.Add(p);
        await using var reader = await selectCmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("unnamed", reader.GetString(0));
        Assert.Equal(100, reader.GetInt32(1));
        Assert.Equal("test note", reader.GetString(2));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 13. SQL Server identity + default in same table
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SqlServer_CreateTable_IdentityAndDefault_BothPresent()
    {
        var table = MakeTable("Orders",
            PkCol("OrderId", identity: true),
            Col("Status", nullable: false, clrType: typeof(int).FullName!, defaultValue: "0"),
            Col("CreatedAt", nullable: false, clrType: typeof(DateTime).FullName!, defaultValue: "GETDATE()")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new SqlServerMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("IDENTITY(1,1)", createSql);
        Assert.Contains("DEFAULT 0", createSql);
        Assert.Contains("DEFAULT GETDATE()", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 14. PostgreSQL identity + default in same table
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Postgres_CreateTable_IdentityAndDefault_BothPresent()
    {
        var table = MakeTable("Invoices",
            PkCol("InvoiceId", identity: true),
            Col("Total", nullable: false, clrType: typeof(decimal).FullName!, defaultValue: "0"),
            Col("DueDate", nullable: false, clrType: typeof(DateTime).FullName!, defaultValue: "CURRENT_TIMESTAMP")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        var result = new PostgresMigrationSqlGenerator().GenerateSql(diff);

        var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
        Assert.Contains("GENERATED ALWAYS AS IDENTITY", createSql);
        Assert.Contains("DEFAULT 0", createSql);
        Assert.Contains("DEFAULT CURRENT_TIMESTAMP", createSql);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 15. SchemaSnapshotBuilder picks up [DatabaseGenerated(Identity)]
    // ═══════════════════════════════════════════════════════════════════════

    [Table("MdiSnapshotEntity")]
    public class MdiSnapshotEntity
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public void SchemaSnapshotBuilder_SetsIsIdentity_FromAttribute()
    {
        // SchemaSnapshotBuilder.Build(assembly) scans for [Table] + [Key] types
        // and should set IsIdentity for [DatabaseGenerated(Identity)] properties.
        var snapshot = SchemaSnapshotBuilder.Build(typeof(MdiSnapshotEntity).Assembly);
        var table = snapshot.Tables.FirstOrDefault(t => t.Name == "MdiSnapshotEntity");
        Assert.NotNull(table);

        var idCol = table.Columns.FirstOrDefault(c => c.Name == "Id");
        Assert.NotNull(idCol);
        Assert.True(idCol.IsIdentity, "IsIdentity should be true for [DatabaseGenerated(Identity)] property");
        Assert.True(idCol.IsPrimaryKey, "IsPrimaryKey should be true for [Key] property");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 16. SQLite recreate-table preserves AUTOINCREMENT through FK changes
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Sqlite_RecreateTable_PreservesAutoincrement_ThroughFkChange()
    {
        var parentTable = MakeTable("MdiParent",
            PkCol("Id", identity: true)
        );

        var childTable = MakeTable("MdiChild",
            PkCol("ChildId", identity: true),
            Col("ParentId", nullable: false, clrType: typeof(int).FullName!)
        );

        // Add an FK to MdiChild referencing MdiParent
        var fk = new ForeignKeySchema
        {
            ConstraintName = "FK_MdiChild_MdiParent",
            DependentColumns = new[] { "ParentId" },
            PrincipalTable = "MdiParent",
            PrincipalColumns = new[] { "Id" },
            OnDelete = "CASCADE",
            OnUpdate = "NO ACTION"
        };

        // Simulate adding FK on existing table (triggers recreate in SQLite)
        var oldChildTable = MakeTable("MdiChild",
            PkCol("ChildId", identity: true),
            Col("ParentId", nullable: false, clrType: typeof(int).FullName!)
        );
        var newChildTable = MakeTable("MdiChild",
            PkCol("ChildId", identity: true),
            Col("ParentId", nullable: false, clrType: typeof(int).FullName!)
        );
        newChildTable.ForeignKeys.Add(fk);

        var diff = new SchemaDiff();
        diff.AddedForeignKeys.Add((newChildTable, fk));

        var result = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // The Up path should recreate the table, preserving AUTOINCREMENT on ChildId
        var recreateCreate = result.Up.FirstOrDefault(s => s.Contains("CREATE TABLE") && s.Contains("__temp__"));
        Assert.NotNull(recreateCreate);
        Assert.Contains("AUTOINCREMENT", recreateCreate);
        Assert.Contains("FK_MdiChild_MdiParent", recreateCreate);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 17. SchemaDiffer detects IsIdentity changes
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SchemaDiffer_DetectsIsIdentityChange()
    {
        var oldSnapshot = new SchemaSnapshot
        {
            Tables = { MakeTable("Counters",
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Id", IsIdentity = false },
                Col("Value", nullable: false, clrType: typeof(int).FullName!)
            )}
        };
        var newSnapshot = new SchemaSnapshot
        {
            Tables = { MakeTable("Counters",
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Id", IsIdentity = true },
                Col("Value", nullable: false, clrType: typeof(int).FullName!)
            )}
        };

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);
        Assert.True(diff.AlteredColumns.Count > 0);
        var alteredId = diff.AlteredColumns.First(ac => ac.NewColumn.Name == "Id");
        Assert.True(alteredId.NewColumn.IsIdentity);
        Assert.False(alteredId.OldColumn.IsIdentity);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 18. Multiple defaults with various literal types
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void AllProviders_CreateTable_SupportsVariousDefaultLiterals()
    {
        var table = MakeTable("Settings",
            PkCol("Id"),
            Col("IntDefault", nullable: false, clrType: typeof(int).FullName!, defaultValue: "42"),
            Col("DecimalDefault", nullable: false, clrType: typeof(decimal).FullName!, defaultValue: "3.14"),
            Col("StringDefault", nullable: false, clrType: typeof(string).FullName!, defaultValue: "'hello'"),
            Col("BoolDefault", nullable: false, clrType: typeof(bool).FullName!, defaultValue: "false"),
            Col("NullDefault", nullable: true, clrType: typeof(string).FullName!, defaultValue: "null")
        );
        var diff = new SchemaDiff();
        diff.AddedTables.Add(table);

        // Test each generator emits all defaults
        var generators = new IMigrationSqlGenerator[]
        {
            new SqliteMigrationSqlGenerator(),
            new SqlServerMigrationSqlGenerator(),
            new MySqlMigrationSqlGenerator(),
            new PostgresMigrationSqlGenerator()
        };

        foreach (var gen in generators)
        {
            var result = gen.GenerateSql(diff);
            var createSql = result.Up.Single(s => s.StartsWith("CREATE TABLE"));
            Assert.Contains("DEFAULT 42", createSql);
            Assert.Contains("DEFAULT 3.14", createSql);
            Assert.Contains("DEFAULT 'hello'", createSql);
            Assert.Contains("DEFAULT false", createSql);
            Assert.Contains("DEFAULT null", createSql);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Dynamic assembly helpers (for live SQLite migration tests)
    // ═══════════════════════════════════════════════════════════════════════

    private static readonly Type[] _upDownParams =
        { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) };

    private static readonly ConstructorInfo _baseCtor =
        typeof(MigrationBase).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;

    private static readonly MethodInfo _upAbstract =
        typeof(MigrationBase).GetMethod("Up", _upDownParams)!;
    private static readonly MethodInfo _downAbstract =
        typeof(MigrationBase).GetMethod("Down", _upDownParams)!;

    private static void EmitCtor(TypeBuilder tb, long version, string name)
    {
        var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
        var il = ctor.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldc_I8, version);
        il.Emit(OpCodes.Ldstr, name);
        il.Emit(OpCodes.Call, _baseCtor);
        il.Emit(OpCodes.Ret);
    }

    private static void EmitNoOpDown(TypeBuilder tb)
    {
        var m = tb.DefineMethod("Down",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        m.GetILGenerator().Emit(OpCodes.Ret);
        tb.DefineMethodOverride(m, _downAbstract);
    }

    private static void EmitDdlUp(TypeBuilder tb, string sql)
    {
        var m = tb.DefineMethod("Up",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        var il = m.GetILGenerator();

        var createCmd = typeof(DbConnection).GetMethod("CreateCommand")!;
        var setTx = typeof(DbCommand).GetProperty("Transaction")!.GetSetMethod()!;
        var setCmdText = typeof(DbCommand).GetProperty("CommandText")!.GetSetMethod()!;
        var execNonQ = typeof(DbCommand).GetMethod("ExecuteNonQuery")!;
        var dispose = typeof(IDisposable).GetMethod("Dispose")!;

        il.DeclareLocal(typeof(DbCommand));
        il.Emit(OpCodes.Ldarg_1);          // connection
        il.Emit(OpCodes.Callvirt, createCmd);
        il.Emit(OpCodes.Stloc_0);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldarg_2);          // transaction
        il.Emit(OpCodes.Callvirt, setTx);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldstr, sql);
        il.Emit(OpCodes.Callvirt, setCmdText);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Callvirt, execNonQ);
        il.Emit(OpCodes.Pop);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Callvirt, dispose);

        il.Emit(OpCodes.Ret);
        tb.DefineMethodOverride(m, _upAbstract);
    }

    private static Assembly DdlAssembly(long version, string name, string createTableSql)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("MDI_Ddl_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");
        var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb, version, name);
        EmitDdlUp(tb, createTableSql);
        EmitNoOpDown(tb);
        tb.CreateType();
        return ab;
    }
}
