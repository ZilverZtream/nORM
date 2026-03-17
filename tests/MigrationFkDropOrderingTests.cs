using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies that migration SQL generators emit FK constraint drops BEFORE column/table
/// drops in both the Up and Down migration scripts.
///
/// Root cause: the original generators processed DroppedColumns before DroppedForeignKeys,
/// so any ALTER TABLE DROP COLUMN that had a FK constraint referencing it would fail at
/// runtime because the constraint still existed.
///
/// Fix: GenerateSql() now uses independent up/down passes with correct dependency ordering:
///   UP:   DroppedFK → DroppedTables → DroppedColumns → Alters → AddedTables → AddedColumns → AddedFK
///   DOWN: AddedFK(drop) → AddedColumns(drop) → AddedTables(drop) → Alters → DroppedColumns(add) → DroppedTables(recreate) → DroppedFK(add)
///
/// Also validates SQLite concurrent migration serialization (CCD-1).
/// </summary>
public class MigrationFkDropOrderingTests
{
    // ── Shared diff builders ───────────────────────────────────────────────────

    private static TableSchema MakeOrdersTable() => new TableSchema
    {
        Name = "Orders",
        Columns =
        {
            new ColumnSchema { Name = "Id",         ClrType = typeof(int).FullName!,     IsNullable = false, IsPrimaryKey = true  },
            new ColumnSchema { Name = "CustomerId", ClrType = typeof(int).FullName!,     IsNullable = false },
            new ColumnSchema { Name = "Amount",     ClrType = typeof(decimal).FullName!, IsNullable = false },
        },
        ForeignKeys =
        {
            new ForeignKeySchema
            {
                ConstraintName   = "FK_Orders_Customers_CustomerId",
                DependentColumns = new[] { "CustomerId" },
                PrincipalTable   = "Customers",
                PrincipalColumns = new[] { "Id" },
                OnDelete = "NO ACTION",
                OnUpdate = "NO ACTION",
            }
        }
    };

    /// <summary>
    /// A diff that drops a FK constraint AND the FK column — the ordering-critical scenario.
    /// DROP CONSTRAINT must precede DROP COLUMN.
    /// </summary>
    private static SchemaDiff MakeDiffWithDropFkAndDropColumn()
    {
        var table = MakeOrdersTable();
        var diff  = new SchemaDiff();
        diff.DroppedColumns.Add((table, new ColumnSchema { Name = "CustomerId", ClrType = typeof(int).FullName!, IsNullable = false }));
        diff.DroppedForeignKeys.Add((table, new ForeignKeySchema
        {
            ConstraintName   = "FK_Orders_Customers_CustomerId",
            DependentColumns = new[] { "CustomerId" },
            PrincipalTable   = "Customers",
            PrincipalColumns = new[] { "Id" },
            OnDelete = "NO ACTION",
            OnUpdate = "NO ACTION",
        }));
        return diff;
    }

    /// <summary>
    /// A diff that adds a column AND a FK on that column — DOWN migration must drop FK before column.
    /// </summary>
    private static SchemaDiff MakeDiffWithAddFkAndAddColumn()
    {
        var table = new TableSchema
        {
            Name = "Items",
            Columns =
            {
                new ColumnSchema { Name = "Id",      ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true },
                new ColumnSchema { Name = "OrderId", ClrType = typeof(int).FullName!, IsNullable = true  },
            }
        };
        var diff = new SchemaDiff();
        diff.AddedColumns.Add((table, new ColumnSchema { Name = "OrderId", ClrType = typeof(int).FullName!, IsNullable = true }));
        diff.AddedForeignKeys.Add((table, new ForeignKeySchema
        {
            ConstraintName   = "FK_Items_Orders_OrderId",
            DependentColumns = new[] { "OrderId" },
            PrincipalTable   = "Orders",
            PrincipalColumns = new[] { "Id" },
            OnDelete = "NO ACTION",
            OnUpdate = "NO ACTION",
        }));
        return diff;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-1: SQL Server — UP list: DROP CONSTRAINT before DROP COLUMN
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFKO_1_SqlServer_Up_DropConstraintBeforeDropColumn()
    {
        var stmts = new SqlServerMigrationSqlGenerator().GenerateSql(MakeDiffWithDropFkAndDropColumn());
        var up    = stmts.Up.ToList();

        var dropConstraintIdx = up.FindIndex(s => s.Contains("DROP CONSTRAINT", StringComparison.OrdinalIgnoreCase));
        var dropColumnIdx     = up.FindIndex(s => s.Contains("DROP COLUMN",     StringComparison.OrdinalIgnoreCase));

        Assert.True(dropConstraintIdx >= 0, "UP must contain DROP CONSTRAINT");
        Assert.True(dropColumnIdx     >= 0, "UP must contain DROP COLUMN");
        Assert.True(dropConstraintIdx < dropColumnIdx,
            $"UP: DROP CONSTRAINT (idx {dropConstraintIdx}) must precede DROP COLUMN (idx {dropColumnIdx}). " +
            "Dropping a column with an active FK constraint fails at runtime.");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-2: MySQL — UP list: DROP FOREIGN KEY before DROP COLUMN
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFKO_2_MySql_Up_DropForeignKeyBeforeDropColumn()
    {
        var stmts = new MySqlMigrationSqlGenerator().GenerateSql(MakeDiffWithDropFkAndDropColumn());
        var up    = stmts.Up.ToList();

        var dropFkIdx     = up.FindIndex(s => s.Contains("DROP FOREIGN KEY", StringComparison.OrdinalIgnoreCase));
        var dropColumnIdx = up.FindIndex(s => s.Contains("DROP COLUMN",      StringComparison.OrdinalIgnoreCase));

        Assert.True(dropFkIdx     >= 0, "UP must contain DROP FOREIGN KEY");
        Assert.True(dropColumnIdx >= 0, "UP must contain DROP COLUMN");
        Assert.True(dropFkIdx < dropColumnIdx,
            $"UP: DROP FOREIGN KEY (idx {dropFkIdx}) must precede DROP COLUMN (idx {dropColumnIdx}).");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-3: Postgres — UP list: DROP CONSTRAINT before DROP COLUMN
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFKO_3_Postgres_Up_DropConstraintBeforeDropColumn()
    {
        var stmts = new PostgresMigrationSqlGenerator().GenerateSql(MakeDiffWithDropFkAndDropColumn());
        var up    = stmts.Up.ToList();

        var dropConstraintIdx = up.FindIndex(s => s.Contains("DROP CONSTRAINT", StringComparison.OrdinalIgnoreCase));
        var dropColumnIdx     = up.FindIndex(s => s.Contains("DROP COLUMN",     StringComparison.OrdinalIgnoreCase));

        Assert.True(dropConstraintIdx >= 0, "UP must contain DROP CONSTRAINT");
        Assert.True(dropColumnIdx     >= 0, "UP must contain DROP COLUMN");
        Assert.True(dropConstraintIdx < dropColumnIdx,
            $"UP: DROP CONSTRAINT (idx {dropConstraintIdx}) must precede DROP COLUMN (idx {dropColumnIdx}).");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-4: SQL Server DOWN — DROP CONSTRAINT before DROP COLUMN
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFKO_4_SqlServer_Down_DropConstraintBeforeDropColumn()
    {
        var stmts = new SqlServerMigrationSqlGenerator().GenerateSql(MakeDiffWithAddFkAndAddColumn());
        var down  = stmts.Down.ToList();

        var dropConstraintIdx = down.FindIndex(s => s.Contains("DROP CONSTRAINT", StringComparison.OrdinalIgnoreCase));
        var dropColumnIdx     = down.FindIndex(s => s.Contains("DROP COLUMN",     StringComparison.OrdinalIgnoreCase));

        Assert.True(dropConstraintIdx >= 0, "DOWN must contain DROP CONSTRAINT");
        Assert.True(dropColumnIdx     >= 0, "DOWN must contain DROP COLUMN");
        Assert.True(dropConstraintIdx < dropColumnIdx,
            $"DOWN: DROP CONSTRAINT (idx {dropConstraintIdx}) must precede DROP COLUMN (idx {dropColumnIdx}). " +
            "Rolling back a migration that added a FK+column requires dropping the FK first.");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-5: MySQL DOWN — DROP FOREIGN KEY before DROP COLUMN
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFKO_5_MySql_Down_DropForeignKeyBeforeDropColumn()
    {
        var stmts = new MySqlMigrationSqlGenerator().GenerateSql(MakeDiffWithAddFkAndAddColumn());
        var down  = stmts.Down.ToList();

        var dropFkIdx     = down.FindIndex(s => s.Contains("DROP FOREIGN KEY", StringComparison.OrdinalIgnoreCase));
        var dropColumnIdx = down.FindIndex(s => s.Contains("DROP COLUMN",       StringComparison.OrdinalIgnoreCase));

        Assert.True(dropFkIdx     >= 0, "DOWN must contain DROP FOREIGN KEY");
        Assert.True(dropColumnIdx >= 0, "DOWN must contain DROP COLUMN");
        Assert.True(dropFkIdx < dropColumnIdx,
            $"DOWN: DROP FOREIGN KEY (idx {dropFkIdx}) must precede DROP COLUMN (idx {dropColumnIdx}).");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-6: Postgres DOWN — DROP CONSTRAINT before DROP COLUMN
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFKO_6_Postgres_Down_DropConstraintBeforeDropColumn()
    {
        var stmts = new PostgresMigrationSqlGenerator().GenerateSql(MakeDiffWithAddFkAndAddColumn());
        var down  = stmts.Down.ToList();

        var dropConstraintIdx = down.FindIndex(s => s.Contains("DROP CONSTRAINT", StringComparison.OrdinalIgnoreCase));
        var dropColumnIdx     = down.FindIndex(s => s.Contains("DROP COLUMN",     StringComparison.OrdinalIgnoreCase));

        Assert.True(dropConstraintIdx >= 0, "DOWN must contain DROP CONSTRAINT");
        Assert.True(dropColumnIdx     >= 0, "DOWN must contain DROP COLUMN");
        Assert.True(dropConstraintIdx < dropColumnIdx,
            $"DOWN: DROP CONSTRAINT (idx {dropConstraintIdx}) must precede DROP COLUMN (idx {dropColumnIdx}).");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-7: All generators — ADD COLUMN must precede ADD CONSTRAINT in UP
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void MFKO_7_AllGenerators_Up_AddColumnBeforeAddConstraint(string provider)
    {
        IMigrationSqlGenerator gen = provider switch
        {
            "sqlserver" => new SqlServerMigrationSqlGenerator(),
            "mysql"     => new MySqlMigrationSqlGenerator(),
            "postgres"  => new PostgresMigrationSqlGenerator(),
            _           => throw new ArgumentException(provider)
        };

        var stmts = gen.GenerateSql(MakeDiffWithAddFkAndAddColumn());
        var up    = stmts.Up.ToList();

        // ADD COLUMN statement (contains ADD but not CONSTRAINT/FOREIGN KEY keywords)
        var addColumnIdx = up.FindIndex(s =>
            (s.Contains("ADD COLUMN", StringComparison.OrdinalIgnoreCase) || (s.Contains(" ADD ", StringComparison.OrdinalIgnoreCase))) &&
            !s.Contains("CONSTRAINT",   StringComparison.OrdinalIgnoreCase) &&
            !s.Contains("FOREIGN KEY",  StringComparison.OrdinalIgnoreCase));

        // ADD CONSTRAINT / ADD ... FOREIGN KEY statement
        var addFkIdx = up.FindIndex(s =>
            s.Contains("ADD ", StringComparison.OrdinalIgnoreCase) &&
            (s.Contains("CONSTRAINT", StringComparison.OrdinalIgnoreCase) ||
             s.Contains("FOREIGN KEY", StringComparison.OrdinalIgnoreCase)));

        Assert.True(addColumnIdx >= 0, $"[{provider}] UP must contain ADD COLUMN");
        Assert.True(addFkIdx     >= 0, $"[{provider}] UP must contain ADD CONSTRAINT/FOREIGN KEY");
        Assert.True(addColumnIdx < addFkIdx,
            $"[{provider}] ADD COLUMN (idx {addColumnIdx}) must precede ADD CONSTRAINT (idx {addFkIdx}).");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-8: All generators — DroppedFK is the FIRST UP statement when only
    //         FK+column drops are present (no table drops to precede it)
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void MFKO_8_AllGenerators_DroppedFk_IsFirstStatementInUp(string provider)
    {
        IMigrationSqlGenerator gen = provider switch
        {
            "sqlserver" => new SqlServerMigrationSqlGenerator(),
            "mysql"     => new MySqlMigrationSqlGenerator(),
            "postgres"  => new PostgresMigrationSqlGenerator(),
            _           => throw new ArgumentException(provider)
        };

        var stmts   = gen.GenerateSql(MakeDiffWithDropFkAndDropColumn());
        var firstUp = stmts.Up.First();

        var isDropFk = firstUp.Contains("DROP CONSTRAINT",  StringComparison.OrdinalIgnoreCase)
                    || firstUp.Contains("DROP FOREIGN KEY", StringComparison.OrdinalIgnoreCase);
        Assert.True(isDropFk,
            $"[{provider}] first UP statement must be a FK drop, got: {firstUp}");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-9: SQLite concurrent runner — two concurrent ApplyMigrationsAsync
    //         calls on the same file apply migrations exactly once (CCD-1).
    //
    //         Uses the test assembly's existing migrations (CreateBlogTable v1,
    //         AddPostsTable v2) on a fresh temporary database so no new
    //         Migration subclass needs to be added to the test assembly.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MFKO_9_SqliteRunner_ConcurrentApply_HistoryHasExactlyOneEntryPerMigration()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"norm_mfko9_{Guid.NewGuid():N}.db");
        try
        {
            // Use the test assembly — contains the two real migrations (v1 and v2).
            var asm = typeof(SqliteMigrationRunnerTests).Assembly;

            await using var cn1 = new SqliteConnection($"Data Source={dbPath}");
            await using var cn2 = new SqliteConnection($"Data Source={dbPath}");
            await cn1.OpenAsync();
            await cn2.OpenAsync();

            var runner1 = new SqliteMigrationRunner(cn1, asm);
            var runner2 = new SqliteMigrationRunner(cn2, asm);

            // Fire both concurrently — exclusive lock serializes them.
            await Task.WhenAll(runner1.ApplyMigrationsAsync(), runner2.ApplyMigrationsAsync());

            // Verify each migration version appears at most once.
            await using var cmd = cn1.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
            var total = Convert.ToInt64(await cmd.ExecuteScalarAsync());

            // Total rows == number of distinct migrations (not double the count).
            await using var cmd2 = cn1.CreateCommand();
            cmd2.CommandText = "SELECT COUNT(DISTINCT \"Version\") FROM \"__NormMigrationsHistory\"";
            var distinct = Convert.ToInt64(await cmd2.ExecuteScalarAsync());

            Assert.Equal(distinct, total);
            Assert.True(total > 0, "At least one migration must have been applied.");
        }
        finally
        {
            foreach (var ext in new[] { "", "-wal", "-shm" })
                try { File.Delete(dbPath + ext); } catch { }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-10: After concurrent apply, HasPendingMigrationsAsync returns false
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MFKO_10_SqliteRunner_AfterConcurrentApply_NoPendingMigrations()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"norm_mfko10_{Guid.NewGuid():N}.db");
        try
        {
            var asm = typeof(SqliteMigrationRunnerTests).Assembly;

            await using var cn1 = new SqliteConnection($"Data Source={dbPath}");
            await using var cn2 = new SqliteConnection($"Data Source={dbPath}");
            await using var cn3 = new SqliteConnection($"Data Source={dbPath}");
            await cn1.OpenAsync();
            await cn2.OpenAsync();
            await cn3.OpenAsync();

            await Task.WhenAll(
                new SqliteMigrationRunner(cn1, asm).ApplyMigrationsAsync(),
                new SqliteMigrationRunner(cn2, asm).ApplyMigrationsAsync());

            var hasPending = await new SqliteMigrationRunner(cn3, asm).HasPendingMigrationsAsync();
            Assert.False(hasPending, "After concurrent apply, no migrations should remain pending.");
        }
        finally
        {
            foreach (var ext in new[] { "", "-wal", "-shm" })
                try { File.Delete(dbPath + ext); } catch { }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MFKO-11: SQLite generator — DroppedColumn with FK uses table-recreation,
    //          PRAGMA foreign_keys=off appears in pre-transaction segment
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFKO_11_SqliteMigrationSqlGenerator_DropColumnWithFk_UsesPragmaNotInlineDrop()
    {
        var table = MakeOrdersTable();
        var diff  = new SchemaDiff();
        diff.DroppedColumns.Add((table, new ColumnSchema { Name = "CustomerId", ClrType = typeof(int).FullName!, IsNullable = false }));
        diff.DroppedForeignKeys.Add((table, new ForeignKeySchema
        {
            ConstraintName   = "FK_Orders_Customers_CustomerId",
            DependentColumns = new[] { "CustomerId" },
            PrincipalTable   = "Customers",
            PrincipalColumns = new[] { "Id" },
            OnDelete = "NO ACTION",
            OnUpdate = "NO ACTION",
        }));

        var stmts = new SqliteMigrationSqlGenerator().GenerateSql(diff);

        // SQLite uses table-recreation — no direct ALTER TABLE DROP COLUMN.
        Assert.DoesNotContain(stmts.Up, s => s.Contains("DROP COLUMN", StringComparison.OrdinalIgnoreCase));

        // PRAGMA must be in the pre-transaction segment, not inline.
        Assert.NotNull(stmts.PreTransactionUp);
        Assert.Contains(stmts.PreTransactionUp!, s => s.Contains("foreign_keys=off", StringComparison.OrdinalIgnoreCase));
    }
}
