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
        public string? DefaultValue;
    }

    // Type changes whose value coercion round-trips losslessly through SQLite
    // affinities in BOTH directions (Up coerce, Down coerce back).
    private static readonly Dictionary<Type, Type[]> SafeConversions = new()
    {
        [typeof(int)] = new[] { typeof(long), typeof(double), typeof(string) },
        [typeof(long)] = new[] { typeof(string) },
        [typeof(double)] = new[] { typeof(string) },
        [typeof(bool)] = new[] { typeof(int) },
    };

    /// <summary>
    /// Environment-directed seed sweep for building the release dry window: set
    /// NORM_MIGRATION_FUZZ_SWEEP to "start:count" to run that seed range through
    /// the up/down preservation machine (60 cases per seed). Unset, this fact is
    /// a no-op so the fixed seeds stay the baseline.
    /// </summary>
    [Fact]
    public void Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_MIGRATION_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], CultureInfo.InvariantCulture);
        for (var s = start; s < start + count; s++)
            Random_migrations_preserve_data_up_and_down(s);
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

        // ── Optional UNIQUE-indexed column (enforcement must survive) ─────
        var withUnique = rng.Next(3) == 0;
        if (withUnique)
        {
            baseline.Add(new ColSpec { Name = "U0", Clr = typeof(int), TypeIndex = 0, Nullable = false });
        }

        // ── Optional CHECK-constrained column (enforcement must survive) ──
        var withCheck = rng.Next(4) == 0;
        if (withCheck)
        {
            baseline.Add(new ColSpec { Name = "Ck0", Clr = typeof(int), TypeIndex = 0, Nullable = false });
        }

        // ── Optional DEFAULT-carrying column (default must survive; a dropped
        // defaulted column restores with the default backfilled) ───────────
        var withDefault = rng.Next(4) == 0;
        var dropDefaultCol = withDefault && rng.Next(2) == 0;
        if (withDefault)
        {
            baseline.Add(new ColSpec { Name = "D0", Clr = typeof(int), TypeIndex = 0, Nullable = false, DefaultValue = "42" });
        }

        // ── Optional FK child table (parent recreates must not orphan it) ──
        var withChild = rng.Next(2) == 0;
        // Sometimes the CHILD mutates in the same migration (Val tightens to
        // NOT NULL, forcing a child recreate alongside the parent's) — both
        // rebuilds share one FK-off window and the child's FK must survive.
        var mutateChild = withChild && rng.Next(2) == 0;

        // Index NAMES stay keyed to the baseline column so a rename keeps the
        // index identity while pointing it at the renamed column.
        var baselineIndexes = indexedColumns.ToDictionary(c => c, IndexName, StringComparer.Ordinal);
        var baselineTable = BuildTable(baseline, indexNamesByColumn: baselineIndexes, uniqueColumn: withUnique ? "U0" : null, withCheck: withCheck);
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
                if (col.Name == "U0")
                {
                    value = r * 131 + 7; // distinct per row — unique index must accept the seed
                }
                else if (col.Name == "Ck0")
                {
                    value = rng.Next(1, 500); // strictly positive — satisfies CHECK (Ck0 > 0)
                }
                else if (col.Name == "D0")
                {
                    value = rng.Next(100, 999); // explicit values distinct from the DEFAULT 42
                }
                else if (col.Nullable && rng.Next(4) == 0)
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
            Name = c.Name, Clr = c.Clr, TypeIndex = c.TypeIndex, Nullable = c.Nullable, DefaultValue = c.DefaultValue
        }).ToList();
        var renames = new Dictionary<string, string>(StringComparer.Ordinal); // old -> new
        var dropped = new List<ColSpec>();
        var added = new List<string>();
        // Indexed columns participate in mutations: dropping one must drop its
        // index (a recreate re-emitting an index over a missing column is broken
        // SQL), and renaming one must carry the index to the new column name.
        var touched = new HashSet<string>(StringComparer.Ordinal) { "U0", "Ck0", "D0" };
        var typeChanged = new HashSet<string>(StringComparer.Ordinal);
        var previousNames = new Dictionary<string, string>(StringComparer.Ordinal); // target col -> PreviousName

        var mutations = rng.Next(1, 4);
        for (var m = 0; m < mutations; m++)
        {
            switch (rng.Next(5))
            {
                case 4:
                {
                    // Safe type conversions only: the recreate INSERT..SELECT coerces
                    // stored values through the new column's affinity, and these
                    // round-trip losslessly through the Down recreate.
                    var candidates = target.Where(c => !touched.Contains(c.Name) && !added.Contains(c.Name)
                        && SafeConversions.ContainsKey(c.Clr)).ToList();
                    if (candidates.Count == 0) break;
                    var victim = candidates[rng.Next(candidates.Count)];
                    var options = SafeConversions[victim.Clr];
                    var newClr = options[rng.Next(options.Length)];
                    victim.Clr = newClr;
                    victim.TypeIndex = Array.FindIndex(ColumnTypePool, p => p.Clr == newClr);
                    typeChanged.Add(victim.Name);
                    touched.Add(victim.Name);
                    break;
                }
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

        if (dropDefaultCol)
        {
            var d0 = target.Single(c => c.Name == "D0");
            target.Remove(d0);
            dropped.Add(d0);
        }

        if (added.Count == 0 && dropped.Count == 0 && renames.Count == 0 && !mutateChild
            && !TargetDiffers(baseline, target, previousNames))
            return; // no-op case

        // Post-mutation index map: dropped columns lose their index; renamed
        // columns keep the ORIGINAL index name on the NEW column name.
        var droppedSet = new HashSet<string>(dropped.Select(d => d.Name), StringComparer.Ordinal);
        var targetIndexes = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (col, ixName) in baselineIndexes)
        {
            if (droppedSet.Contains(col)) continue;
            targetIndexes[renames.TryGetValue(col, out var renamed) ? renamed : col] = ixName;
        }

        var targetTable = BuildTable(target, previousNames, targetIndexes, withUnique ? "U0" : null, withCheck);
        var oldSnapshot = new SchemaSnapshot { Tables = { baselineTable } };
        var newSnapshot = new SchemaSnapshot { Tables = { targetTable } };
        if (childTable != null)
        {
            oldSnapshot.Tables.Add(childTable);
            newSnapshot.Tables.Add(BuildChildTable(valNullable: !mutateChild));
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
        AssertIndexes(cn, targetIndexes, withUnique, detail + " (after Up)");
        if (withCheck) AssertCheckEnforced(cn, detail + " (after Up)");
        if (withChild) AssertChildIntegrity(cn, childBefore, detail + " (after Up)");
        if (mutateChild) AssertChildValNullability(cn, expectNotNull: true, detail + " (after Up)");
        if (withDefault && !dropDefaultCol) AssertDefaultApplied(cn, detail + " (after Up)");
        var afterUp = Snapshot(cn, target.Select(c => c.Name));
        Assert.True(before.Count == afterUp.Count, $"row count changed on Up: {detail} — {before.Count} -> {afterUp.Count}");
        foreach (var col in target)
        {
            var sourceName = previousNames.TryGetValue(col.Name, out var prev) ? prev : col.Name;
            foreach (var id in before.Keys)
            {
                var expected = added.Contains(col.Name) ? DBNull.Value : before[id][sourceName];
                var actualVal = afterUp[id][col.Name];
                // A type-changed column reads back in the NEW affinity's storage
                // form (5 -> '5.0' etc.); compare numerically where possible.
                var ok = typeChanged.Contains(col.Name)
                    ? CoercedEquals(expected, actualVal)
                    : StorageEquals(expected, actualVal);
                Assert.True(ok,
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
        AssertIndexes(cn, baselineIndexes, withUnique, detail + " (after Down)");
        if (withCheck) AssertCheckEnforced(cn, detail + " (after Down)");
        if (withChild) AssertChildIntegrity(cn, childBefore, detail + " (after Down)");
        if (mutateChild) AssertChildValNullability(cn, expectNotNull: false, detail + " (after Down)");
        if (withDefault) AssertDefaultApplied(cn, detail + " (after Down)");
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
        Dictionary<string, string>? indexNamesByColumn = null, string? uniqueColumn = null, bool withCheck = false)
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
                DefaultValue = c.DefaultValue,
            };
            if (previousNames != null && previousNames.TryGetValue(c.Name, out var prev))
                schema.PreviousName = prev;
            if (indexNamesByColumn != null && indexNamesByColumn.TryGetValue(c.Name, out var ixName))
                schema.Indexes.Add(new ColumnIndexSchema { Name = ixName, IsUnique = false, Order = 0 });
            if (uniqueColumn != null && c.Name == uniqueColumn)
                schema.Indexes.Add(new ColumnIndexSchema { Name = IndexName(c.Name), IsUnique = true, Order = 0 });
            t.Columns.Add(schema);
        }
        if (withCheck)
            t.CheckConstraints.Add(new CheckConstraintSchema { ConstraintName = "CK_FuzzMig_Ck0", Sql = "Ck0 > 0" });
        return t;
    }

    private static string IndexName(string columnName) => $"IX_FuzzMig_{columnName}";

    private static TableSchema BuildChildTable(bool valNullable = true)
    {
        var t = new TableSchema { Name = "FuzzMigChild" };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        t.Columns.Add(new ColumnSchema { Name = "ParentId", ClrType = typeof(int).FullName! });
        t.Columns.Add(new ColumnSchema { Name = "Val", ClrType = typeof(string).FullName!, IsNullable = valNullable });
        t.ForeignKeys.Add(new ForeignKeySchema
        {
            ConstraintName = "FK_FuzzMigChild_FuzzMig_ParentId",
            DependentColumns = new[] { "ParentId" },
            PrincipalTable = "FuzzMig",
            PrincipalColumns = new[] { "Id" },
        });
        return t;
    }

    private static void AssertIndexes(SqliteConnection cn, Dictionary<string, string> indexNamesByColumn, bool withUnique, string detail)
    {
        var live = new Dictionary<string, bool>(StringComparer.Ordinal);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT name, \"unique\" FROM pragma_index_list('FuzzMig') WHERE origin = 'c'";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                live[reader.GetString(0)] = reader.GetInt32(1) == 1;
        }
        var expected = indexNamesByColumn.Values
            .Concat(withUnique ? new[] { IndexName("U0") } : Array.Empty<string>())
            .OrderBy(x => x, StringComparer.Ordinal).ToList();
        var actual = live.Keys.OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.True(expected.SequenceEqual(actual),
            $"index drift {detail}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");

        // Each surviving index must cover the column it is expected to cover —
        // a rename that leaves the index behind (or points it at the wrong
        // column) is silent index loss.
        foreach (var (colName, ixName) in indexNamesByColumn)
        {
            using var info = cn.CreateCommand();
            info.CommandText = $"SELECT name FROM pragma_index_info('{ixName}')";
            var liveCol = info.ExecuteScalar() as string;
            Assert.True(string.Equals(liveCol, colName, StringComparison.Ordinal),
                $"index column drift {detail} index={ixName}: expected {colName} got {liveCol ?? "<missing>"}");
        }
        if (withUnique)
        {
            Assert.True(live[IndexName("U0")],
                $"unique index came back NON-unique {detail}");
            // The rebuilt index must still ENFORCE uniqueness, not merely report it.
            // Duplicate an EXISTING row wholesale (minus Id) so every NOT NULL
            // column is satisfied and the only violation left is the unique one.
            var insertCols = new List<string>();
            using (var info = cn.CreateCommand())
            {
                info.CommandText = "SELECT name FROM pragma_table_info('FuzzMig') WHERE name <> 'Id'";
                using var reader = info.ExecuteReader();
                while (reader.Read())
                    insertCols.Add(reader.GetString(0));
            }
            var colList = string.Join(", ", insertCols);
            using var insert = cn.CreateCommand();
            insert.CommandText = $"INSERT INTO FuzzMig ({colList}) SELECT {colList} FROM FuzzMig LIMIT 1";
            var uniqueViolation = false;
            try { insert.ExecuteNonQuery(); }
            catch (SqliteException ex)
            {
                uniqueViolation = ex.Message.Contains("UNIQUE", StringComparison.OrdinalIgnoreCase);
            }
            Assert.True(uniqueViolation, $"unique index no longer enforced {detail} — duplicate U0 accepted");
        }
        foreach (var ixName in indexNamesByColumn.Values)
        {
            Assert.False(live[ixName],
                $"non-unique index came back UNIQUE {detail} index={ixName}");
        }
    }

    private static void AssertCheckEnforced(SqliteConnection cn, string detail)
    {
        // Copy an existing row wholesale but violate the CHECK column (and
        // freshen the unique column when present) so the CHECK is the only
        // constraint that can fire — a rebuild that silently dropped it would
        // accept the row.
        var insertCols = new List<string>();
        using (var info = cn.CreateCommand())
        {
            info.CommandText = "SELECT name FROM pragma_table_info('FuzzMig') WHERE name <> 'Id'";
            using var reader = info.ExecuteReader();
            while (reader.Read())
                insertCols.Add(reader.GetString(0));
        }
        var selectList = string.Join(", ", insertCols.Select(c =>
            c == "Ck0" ? "-1" : c == "U0" ? "987654321" : c));
        using var insert = cn.CreateCommand();
        insert.CommandText = $"INSERT INTO FuzzMig ({string.Join(", ", insertCols)}) SELECT {selectList} FROM FuzzMig LIMIT 1";
        var checkViolation = false;
        try { insert.ExecuteNonQuery(); }
        catch (SqliteException ex)
        {
            checkViolation = ex.Message.Contains("CHECK", StringComparison.OrdinalIgnoreCase);
        }
        Assert.True(checkViolation, $"CHECK constraint no longer enforced {detail} — violating insert accepted");
    }

    private static void AssertChildValNullability(SqliteConnection cn, bool expectNotNull, string detail)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT \"notnull\" FROM pragma_table_info('FuzzMigChild') WHERE name = 'Val'";
        var notNull = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.True(notNull == (expectNotNull ? 1 : 0),
            $"child Val nullability wrong {detail}: expected notnull={(expectNotNull ? 1 : 0)} got {notNull}");
    }

    private static void AssertDefaultApplied(SqliteConnection cn, string detail)
    {
        // Insert a row OMITTING the defaulted column (copying every other column
        // from an existing row, freshening the unique column) — the declared
        // DEFAULT must fill it. A rebuild that lost the DEFAULT clause fails the
        // NOT NULL instead, and one that mangled it yields the wrong value.
        var insertCols = new List<string>();
        using (var info = cn.CreateCommand())
        {
            info.CommandText = "SELECT name FROM pragma_table_info('FuzzMig') WHERE name NOT IN ('Id', 'D0')";
            using var reader = info.ExecuteReader();
            while (reader.Read())
                insertCols.Add(reader.GetString(0));
        }
        var selectList = string.Join(", ", insertCols.Select(c => c == "U0" ? "987654322" : c));
        long probeId;
        using (var insert = cn.CreateCommand())
        {
            insert.CommandText =
                $"INSERT INTO FuzzMig ({string.Join(", ", insertCols)}) SELECT {selectList} FROM FuzzMig LIMIT 1; " +
                "SELECT last_insert_rowid()";
            try
            {
                probeId = Convert.ToInt64(insert.ExecuteScalar(), CultureInfo.InvariantCulture);
            }
            catch (SqliteException ex)
            {
                using var master = cn.CreateCommand();
                master.CommandText = "SELECT sql FROM sqlite_master WHERE type='table' AND name='FuzzMig'";
                throw new InvalidOperationException(
                    $"default probe insert failed {detail}; live DDL: {master.ExecuteScalar()}", ex);
            }
        }
        try
        {
            using var read = cn.CreateCommand();
            read.CommandText = $"SELECT D0 FROM FuzzMig WHERE Id = {probeId}";
            var applied = Convert.ToInt64(read.ExecuteScalar(), CultureInfo.InvariantCulture);
            Assert.True(applied == 42, $"DEFAULT not applied {detail}: expected 42 got {applied}");
        }
        finally
        {
            Exec(cn, $"DELETE FROM FuzzMig WHERE Id = {probeId}");
        }
    }

    // Numeric-aware equality for type-changed columns: the new affinity's
    // storage form may render differently ('5.0' vs 5) while the value is intact.
    private static bool CoercedEquals(object expected, object actual)
    {
        if (expected is DBNull && actual is DBNull) return true;
        if (expected is DBNull || actual is DBNull) return false;
        var es = Convert.ToString(expected, CultureInfo.InvariantCulture);
        var os = Convert.ToString(actual, CultureInfo.InvariantCulture);
        if (double.TryParse(es, NumberStyles.Any, CultureInfo.InvariantCulture, out var ed)
            && double.TryParse(os, NumberStyles.Any, CultureInfo.InvariantCulture, out var od))
            return Math.Abs(ed - od) < 1e-9;
        return string.Equals(es, os, StringComparison.Ordinal);
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
        // A restored column with a declared DEFAULT backfills that default.
        if (col.DefaultValue != null)
            return long.Parse(col.DefaultValue, CultureInfo.InvariantCulture);
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
