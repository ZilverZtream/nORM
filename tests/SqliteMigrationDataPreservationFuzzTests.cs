using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Seeded oracle machine for migration data preservation. SQLite alters run as
/// full table recreates (rebuild + INSERT..SELECT), so every migration is a
/// chance to silently lose or corrupt rows. Each case builds a random baseline
/// schema, seeds random rows (including NULLs), applies a random mutation set
/// (add / drop / rename / alter nullability) through SchemaDiffer +
/// SqliteMigrationSqlGenerator, and verifies after Up AND after Down that the
/// live schema matches expectations and every surviving column still holds the
/// exact pre-migration value. Dropped-column data is expected gone (re-added
/// columns come back NULL); added columns must read NULL until written.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SqliteMigrationDataPreservationFuzzTests
{
    private static readonly (Type Clr, Func<Random, object> Value)[] ColumnTypePool =
    {
        (typeof(int), rng => rng.Next(-1000, 1000)),
        (typeof(long), rng => (long)rng.Next(-100000, 100000) * 7919L),
        (typeof(string), rng => "s" + rng.Next(0, 100000).ToString(CultureInfo.InvariantCulture)),
        (typeof(double), rng => Math.Round(rng.NextDouble() * 500 - 250, 6)),
        (typeof(decimal), rng => (decimal)rng.Next(-100000, 100000) / 100m),
        (typeof(bool), rng => rng.Next(2) == 1),
        (typeof(DateTime), rng => new DateTime(2020, 1, 1).AddSeconds(rng.Next(0, 200_000_000))),
    };

    private sealed class ColSpec
    {
        public required string Name;
        public required Type Clr;
        public required int TypeIndex;
        public bool Nullable;
    }

    [Theory]
    [InlineData(20260714)]
    [InlineData(4242)]
    [InlineData(910_777_313)]
    public void Random_migrations_preserve_data_up_and_down(int seed)
    {
        var rng = new Random(seed);
        for (var caseIndex = 0; caseIndex < 60; caseIndex++)
        {
            RunCase(rng, seed, caseIndex);
        }
    }

    private static void RunCase(Random rng, int seed, int caseIndex)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // ── Baseline schema ────────────────────────────────────────────────
        var colCount = rng.Next(3, 7);
        var baseline = new List<ColSpec>();
        for (var c = 0; c < colCount; c++)
        {
            var typeIndex = rng.Next(ColumnTypePool.Length);
            baseline.Add(new ColSpec
            {
                Name = $"C{c}",
                Clr = ColumnTypePool[typeIndex].Clr,
                TypeIndex = typeIndex,
                Nullable = rng.Next(3) != 0,
            });
        }

        // ── Optional indexes (must survive recreates verbatim) ────────────
        var indexedColumns = new List<string>();
        var indexCount = rng.Next(3) == 0 ? rng.Next(1, 3) : 0;
        for (var ix = 0; ix < indexCount && ix < baseline.Count; ix++)
        {
            var col = baseline[rng.Next(baseline.Count)];
            if (indexedColumns.Contains(col.Name)) continue;
            indexedColumns.Add(col.Name);
        }

        // ── Optional FK child table (parent recreates must not orphan it) ──
        var withChild = rng.Next(2) == 0;

        var baselineTable = BuildTable(baseline, indexedColumns: indexedColumns);
        var childTable = withChild ? BuildChildTable() : null;
        var gen = new SqliteMigrationSqlGenerator();
        var createDiff = new SchemaDiff { AddedTables = { baselineTable } };
        if (childTable != null) createDiff.AddedTables.Add(childTable);
        foreach (var s in gen.GenerateSql(createDiff).Up)
            Exec(cn, s);

        // ── Seed rows ──────────────────────────────────────────────────────
        var rowCount = rng.Next(2, 7);
        var seededNulls = new HashSet<string>();
        for (var r = 0; r < rowCount; r++)
        {
            using var insert = cn.CreateCommand();
            var names = string.Join(", ", baseline.Select(c => c.Name));
            var slots = string.Join(", ", baseline.Select((c, idx) => "@v" + idx));
            insert.CommandText = $"INSERT INTO FuzzMig ({names}) VALUES ({slots})";
            for (var idx = 0; idx < baseline.Count; idx++)
            {
                var col = baseline[idx];
                object value;
                if (col.Nullable && rng.Next(4) == 0)
                {
                    value = DBNull.Value;
                    seededNulls.Add(col.Name);
                }
                else
                {
                    value = ColumnTypePool[col.TypeIndex].Value(rng);
                }
                insert.Parameters.AddWithValue("@v" + idx, value);
            }
            insert.ExecuteNonQuery();
        }

        // Seed child rows referencing existing parent identities (1..rowCount).
        var childBefore = new Dictionary<long, Dictionary<string, object>>();
        if (withChild)
        {
            var childRows = rng.Next(2, 6);
            for (var r = 0; r < childRows; r++)
            {
                using var insert = cn.CreateCommand();
                insert.CommandText = "INSERT INTO FuzzMigChild (ParentId, Val) VALUES (@p, @v)";
                insert.Parameters.AddWithValue("@p", rng.Next(1, rowCount + 1));
                insert.Parameters.AddWithValue("@v", "c" + rng.Next(1000));
                insert.ExecuteNonQuery();
            }
            childBefore = SnapshotTable(cn, "FuzzMigChild", new[] { "ParentId", "Val" });
        }

        var before = Snapshot(cn, baseline.Select(c => c.Name));

        // ── Random mutations (at most one per column) ─────────────────────
        var target = baseline.Select(c => new ColSpec
        {
            Name = c.Name, Clr = c.Clr, TypeIndex = c.TypeIndex, Nullable = c.Nullable
        }).ToList();
        var renames = new Dictionary<string, string>(StringComparer.Ordinal); // old -> new
        var dropped = new List<ColSpec>();
        var added = new List<string>();
        // Indexed columns keep their definitions this round — the indexes
        // themselves must ride through any recreate untouched.
        var touched = new HashSet<string>(indexedColumns, StringComparer.Ordinal);
        var previousNames = new Dictionary<string, string>(StringComparer.Ordinal); // target col -> PreviousName

        var mutations = rng.Next(1, 4);
        for (var m = 0; m < mutations; m++)
        {
            switch (rng.Next(4))
            {
                case 0:
                {
                    var name = $"A{m}_{rng.Next(1000)}";
                    var typeIndex = rng.Next(ColumnTypePool.Length);
                    target.Add(new ColSpec { Name = name, Clr = ColumnTypePool[typeIndex].Clr, TypeIndex = typeIndex, Nullable = true });
                    added.Add(name);
                    touched.Add(name);
                    break;
                }
                case 1:
                {
                    var candidates = target.Where(c => !touched.Contains(c.Name) && !added.Contains(c.Name)).ToList();
                    if (candidates.Count <= 1) break;
                    var victim = candidates[rng.Next(candidates.Count)];
                    target.Remove(victim);
                    dropped.Add(victim);
                    touched.Add(victim.Name);
                    break;
                }
                case 2:
                {
                    var candidates = target.Where(c => !touched.Contains(c.Name) && !added.Contains(c.Name)).ToList();
                    if (candidates.Count == 0) break;
                    var victim = candidates[rng.Next(candidates.Count)];
                    var newName = victim.Name + "_r" + rng.Next(100);
                    renames[victim.Name] = newName;
                    previousNames[newName] = victim.Name;
                    touched.Add(victim.Name);
                    touched.Add(newName);
                    victim.Name = newName;
                    break;
                }
                default:
                {
                    var candidates = target.Where(c => !touched.Contains(c.Name) && !added.Contains(c.Name)
                        && (!c.Nullable || !seededNulls.Contains(c.Name))).ToList();
                    if (candidates.Count == 0) break;
                    var victim = candidates[rng.Next(candidates.Count)];
                    victim.Nullable = !victim.Nullable;
                    touched.Add(victim.Name);
                    break;
                }
            }
        }

        if (added.Count == 0 && dropped.Count == 0 && renames.Count == 0 && !TargetDiffers(baseline, target, previousNames))
            return; // no-op case

        var targetTable = BuildTable(target, previousNames, indexedColumns);
        var oldSnapshot = new SchemaSnapshot { Tables = { baselineTable } };
        var newSnapshot = new SchemaSnapshot { Tables = { targetTable } };
        if (childTable != null)
        {
            oldSnapshot.Tables.Add(childTable);
            newSnapshot.Tables.Add(BuildChildTable());
        }
        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);
        var sql = gen.GenerateSql(diff);

        var detail = $"seed={seed} case={caseIndex} baseline=[{Describe(baseline)}] target=[{Describe(target)}] " +
                     $"drops=[{string.Join(",", dropped.Select(d => d.Name))}] renames=[{string.Join(",", renames.Select(kv => kv.Key + ">" + kv.Value))}]";

        // ── Up ─────────────────────────────────────────────────────────────
        try
        {
            Apply(cn, sql.PreTransactionUp, sql.Up, sql.PostTransactionUp);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Up failed: {detail}", ex);
        }

        AssertSchema(cn, target, detail + " (after Up)");
        AssertIndexes(cn, indexedColumns, detail + " (after Up)");
        if (withChild) AssertChildIntegrity(cn, childBefore, detail + " (after Up)");
        var afterUp = Snapshot(cn, target.Select(c => c.Name));
        Assert.True(before.Count == afterUp.Count, $"row count changed on Up: {detail} — {before.Count} -> {afterUp.Count}");
        foreach (var col in target)
        {
            var sourceName = previousNames.TryGetValue(col.Name, out var prev) ? prev : col.Name;
            foreach (var id in before.Keys)
            {
                var expected = added.Contains(col.Name) ? DBNull.Value : before[id][sourceName];
                var actualVal = afterUp[id][col.Name];
                Assert.True(StorageEquals(expected, actualVal),
                    $"value drift on Up col={col.Name} row={id}: {detail} — expected {Render(expected)} got {Render(actualVal)}");
            }
        }

        // ── Down ───────────────────────────────────────────────────────────
        try
        {
            Apply(cn, sql.PreTransactionDown, sql.Down, sql.PostTransactionDown);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Down failed: {detail}", ex);
        }

        AssertSchema(cn, baseline, detail + " (after Down)");
        AssertIndexes(cn, indexedColumns, detail + " (after Down)");
        if (withChild) AssertChildIntegrity(cn, childBefore, detail + " (after Down)");
        var afterDown = Snapshot(cn, baseline.Select(c => c.Name));
        Assert.True(before.Count == afterDown.Count, $"row count changed on Down: {detail} — {before.Count} -> {afterDown.Count}");
        var droppedByName = dropped.ToDictionary(d => d.Name, StringComparer.Ordinal);
        foreach (var col in baseline)
        {
            foreach (var id in before.Keys)
            {
                // A column dropped on Up comes back on Down with its data gone:
                // NULL when nullable, the type-appropriate zero backfill otherwise
                // (the restored definition itself stays default-free).
                var expected = droppedByName.ContainsKey(col.Name)
                    ? RestoredBackfillValue(col)
                    : before[id][col.Name];
                var actualVal = afterDown[id][col.Name];
                Assert.True(StorageEquals(expected, actualVal),
                    $"value drift on Down col={col.Name} row={id}: {detail} — expected {Render(expected)} got {Render(actualVal)}");
            }
        }
    }

    private static bool TargetDiffers(List<ColSpec> baseline, List<ColSpec> target, Dictionary<string, string> previousNames)
    {
        if (baseline.Count != target.Count) return true;
        foreach (var t in target)
        {
            var sourceName = previousNames.TryGetValue(t.Name, out var prev) ? prev : t.Name;
            var b = baseline.FirstOrDefault(c => c.Name == sourceName);
            if (b == null || b.Nullable != t.Nullable || b.TypeIndex != t.TypeIndex || sourceName != t.Name) return true;
        }
        return false;
    }

    private static TableSchema BuildTable(List<ColSpec> cols, Dictionary<string, string>? previousNames = null,
        List<string>? indexedColumns = null)
    {
        var t = new TableSchema { Name = "FuzzMig" };
        t.Columns.Add(new ColumnSchema
        {
            Name = "Id",
            ClrType = typeof(int).FullName!,
            IsPrimaryKey = true,
            IsIdentity = true,
        });
        foreach (var c in cols)
        {
            var schema = new ColumnSchema
            {
                Name = c.Name,
                ClrType = c.Clr.FullName!,
                IsNullable = c.Nullable,
            };
            if (previousNames != null && previousNames.TryGetValue(c.Name, out var prev))
                schema.PreviousName = prev;
            if (indexedColumns != null && indexedColumns.Contains(c.Name))
                schema.Indexes.Add(new ColumnIndexSchema { Name = IndexName(c.Name), IsUnique = false, Order = 0 });
            t.Columns.Add(schema);
        }
        return t;
    }

    private static string IndexName(string columnName) => $"IX_FuzzMig_{columnName}";

    private static TableSchema BuildChildTable()
    {
        var t = new TableSchema { Name = "FuzzMigChild" };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        t.Columns.Add(new ColumnSchema { Name = "ParentId", ClrType = typeof(int).FullName! });
        t.Columns.Add(new ColumnSchema { Name = "Val", ClrType = typeof(string).FullName!, IsNullable = true });
        t.ForeignKeys.Add(new ForeignKeySchema
        {
            ConstraintName = "FK_FuzzMigChild_FuzzMig_ParentId",
            DependentColumns = new[] { "ParentId" },
            PrincipalTable = "FuzzMig",
            PrincipalColumns = new[] { "Id" },
        });
        return t;
    }

    private static void AssertIndexes(SqliteConnection cn, List<string> indexedColumns, string detail)
    {
        var live = new List<string>();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='FuzzMig' AND name NOT LIKE 'sqlite_%'";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                live.Add(reader.GetString(0));
        }
        var expected = indexedColumns.Select(IndexName).OrderBy(x => x, StringComparer.Ordinal).ToList();
        var actual = live.OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"index drift {detail}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
    }

    private static void AssertChildIntegrity(SqliteConnection cn, Dictionary<long, Dictionary<string, object>> childBefore, string detail)
    {
        // Child rows survive the parent's recreate untouched.
        var childNow = SnapshotTable(cn, "FuzzMigChild", new[] { "ParentId", "Val" });
        Assert.True(childBefore.Count == childNow.Count,
            $"child row count changed {detail}: {childBefore.Count} -> {childNow.Count}");
        foreach (var id in childBefore.Keys)
        {
            foreach (var col in childBefore[id].Keys)
            {
                Assert.True(StorageEquals(childBefore[id][col], childNow[id][col]),
                    $"child value drift {detail} col={col} row={id}: expected {Render(childBefore[id][col])} got {Render(childNow[id][col])}");
            }
        }

        // No orphans slipped through the recreate.
        Exec(cn, "PRAGMA foreign_keys=ON");
        using (var check = cn.CreateCommand())
        {
            check.CommandText = "PRAGMA foreign_key_check";
            using var reader = check.ExecuteReader();
            Assert.False(reader.Read(), $"foreign_key_check reported violations {detail}");
        }

        // The FK constraint is still ENFORCED (not silently lost by the rebuild).
        using (var orphan = cn.CreateCommand())
        {
            orphan.CommandText = "INSERT INTO FuzzMigChild (ParentId, Val) VALUES (999999, 'orphan')";
            var threw = false;
            try { orphan.ExecuteNonQuery(); }
            catch (SqliteException) { threw = true; }
            Assert.True(threw, $"FK constraint no longer enforced {detail} — orphan insert succeeded");
        }
    }

    private static void AssertSchema(SqliteConnection cn, List<ColSpec> expected, string detail)
    {
        var live = new Dictionary<string, int>(StringComparer.Ordinal);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT name, \"notnull\" FROM pragma_table_info('FuzzMig')";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                live[reader.GetString(0)] = reader.GetInt32(1);
        }

        var expectedNames = new[] { "Id" }.Concat(expected.Select(c => c.Name)).OrderBy(x => x, StringComparer.Ordinal).ToList();
        var liveNames = live.Keys.OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.True(expectedNames.SequenceEqual(liveNames),
            $"schema mismatch {detail}: expected [{string.Join(",", expectedNames)}] got [{string.Join(",", liveNames)}]");
        foreach (var col in expected)
        {
            var expectNotNull = col.Nullable ? 0 : 1;
            Assert.True(live[col.Name] == expectNotNull,
                $"nullability mismatch {detail} col={col.Name}: expected notnull={expectNotNull} got {live[col.Name]}");
        }
    }

    private static Dictionary<long, Dictionary<string, object>> Snapshot(SqliteConnection cn, IEnumerable<string> columns)
        => SnapshotTable(cn, "FuzzMig", columns);

    private static Dictionary<long, Dictionary<string, object>> SnapshotTable(SqliteConnection cn, string table, IEnumerable<string> columns)
    {
        var cols = columns.ToList();
        var result = new Dictionary<long, Dictionary<string, object>>();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Id{(cols.Count > 0 ? ", " + string.Join(", ", cols) : "")} FROM {table}";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var id = reader.GetInt64(0);
            var row = new Dictionary<string, object>(StringComparer.Ordinal);
            for (var i = 0; i < cols.Count; i++)
                row[cols[i]] = reader.IsDBNull(i + 1) ? DBNull.Value : reader.GetValue(i + 1);
            result[id] = row;
        }
        return result;
    }

    private static object RestoredBackfillValue(ColSpec col)
    {
        if (col.Nullable)
            return DBNull.Value;
        // TEXT-affinity columns backfill '', numeric ones 0 — mirrors the
        // generator's restored-column insert expression.
        if (col.Clr == typeof(string) || col.Clr == typeof(decimal) || col.Clr == typeof(DateTime))
            return "";
        if (col.Clr == typeof(double))
            return 0d;
        return 0L;
    }

    private static bool StorageEquals(object expected, object actual)
    {
        if (expected is DBNull && actual is DBNull) return true;
        if (expected is DBNull || actual is DBNull) return false;
        if (expected.GetType() == actual.GetType()) return Equals(expected, actual);
        // The recreate INSERT..SELECT copies storage verbatim; compare invariant
        // text as the storage-form fallback for provider-level type affinities.
        return string.Equals(
            Convert.ToString(expected, CultureInfo.InvariantCulture),
            Convert.ToString(actual, CultureInfo.InvariantCulture),
            StringComparison.Ordinal);
    }

    private static string Render(object value)
        => value is DBNull ? "<null>" : $"{value} ({value.GetType().Name})";

    private static string Describe(IEnumerable<ColSpec> cols)
        => string.Join(",", cols.Select(c => $"{c.Name}:{c.Clr.Name}{(c.Nullable ? "?" : "")}"));

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static void Apply(SqliteConnection cn, IEnumerable<string>? pre, IReadOnlyList<string> body, IEnumerable<string>? post)
    {
        foreach (var s in pre ?? Enumerable.Empty<string>()) Exec(cn, s);
        foreach (var s in body) Exec(cn, s);
        foreach (var s in post ?? Enumerable.Empty<string>()) Exec(cn, s);
    }
}
