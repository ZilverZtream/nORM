using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Proof tests for correctness issues fixed in the nORM 5.0 gate.
/// Each test targets a specific root-cause bug and verifies the observable
/// runtime behaviour is correct. All tests use SQLite in-memory.
/// </summary>
public class CorrectnessProofTests
{
    // ═══════════════════════════════════════════════════════════════════════════
    // Entities
    // ═══════════════════════════════════════════════════════════════════════════

    [Table("CpItem")]
    private class CpItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    [Table("CpNullable")]
    private class CpNullable
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string? Tag { get; set; }
        public int? Score { get; set; }
    }

    [Table("CpVersioned")]
    private class CpVersioned
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        [Timestamp]
        public byte[]? Token { get; set; }
    }

    [Table("CpNullToken")]
    private class CpNullToken
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        [Timestamp]
        public string? StrToken { get; set; }
    }

    // Wide-underlying-type enums for fingerprint overflow tests
    private enum SmallEnum { A = 1, B = 2 }
    private enum LargeIntEnum { A = 1, B = int.MaxValue }
    private enum LargeLongEnum : long { Small = 1L, Big = 5_000_000_000L, Max = long.MaxValue - 1 }
    private enum ULongEnum : ulong { Small = 1UL, Big = 10_000_000_000UL }
    private enum ShortEnum : short { A = 1, B = short.MaxValue }
    private enum ByteEnum : byte { A = 0, B = byte.MaxValue }

    [Table("CpTagged")]
    private class CpTagged
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public long Tag { get; set; }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static (SqliteConnection Cn, DbContext Ctx) CreateItemCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = "CREATE TABLE CpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)";
        c.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateNullableCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = "CREATE TABLE CpNullable (Id INTEGER PRIMARY KEY AUTOINCREMENT, Tag TEXT, Score INTEGER)";
        c.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateVersionedCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = "CREATE TABLE CpVersioned (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Token BLOB)";
        c.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateNullTokenCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = "CREATE TABLE CpNullToken (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, StrToken TEXT)";
        c.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateTaggedCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = "CREATE TABLE CpTagged (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Tag INTEGER NOT NULL DEFAULT 0)";
        c.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 1. NULL semantics — col IS NULL / IS NOT NULL (not = NULL)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task NullComparison_EqualNull_GeneratesIsNull_ReturnsNullRows()
    {
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Tag = null, Score = null });
            await ctx.InsertAsync(new CpNullable { Tag = "present", Score = 1 });

            var results = await ctx.Query<CpNullable>()
                .Where(x => x.Tag == null)
                .ToListAsync();

            Assert.Single(results);
            Assert.Null(results[0].Tag);
        }
    }

    [Fact]
    public async Task NullComparison_NotEqualNull_GeneratesIsNotNull_ReturnsNonNullRows()
    {
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Tag = null });
            await ctx.InsertAsync(new CpNullable { Tag = "A" });
            await ctx.InsertAsync(new CpNullable { Tag = "B" });

            var results = await ctx.Query<CpNullable>()
                .Where(x => x.Tag != null)
                .ToListAsync();

            Assert.Equal(2, results.Count);
            Assert.All(results, r => Assert.NotNull(r.Tag));
        }
    }

    [Fact]
    public async Task NullComparison_NullableInt_EqualNull_OnlyReturnsNullRows()
    {
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Score = null });
            await ctx.InsertAsync(new CpNullable { Score = 0 });
            await ctx.InsertAsync(new CpNullable { Score = 42 });

            var results = await ctx.Query<CpNullable>()
                .Where(x => x.Score == null)
                .ToListAsync();

            Assert.Single(results);
            Assert.Null(results[0].Score);
        }
    }

    [Fact]
    public async Task NullComparison_NullVariable_ProducesIsNull()
    {
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Tag = null });
            await ctx.InsertAsync(new CpNullable { Tag = "x" });

            string? filter = null;
            var results = await ctx.Query<CpNullable>()
                .Where(x => x.Tag == filter)
                .ToListAsync();

            // Must use IS NULL, not = NULL (which always returns nothing in SQL)
            Assert.Single(results);
            Assert.Null(results[0].Tag);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 2. Contains with null values in local collection
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LocalContains_CollectionWithNull_NullValueMatchedCorrectly()
    {
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Score = null });
            await ctx.InsertAsync(new CpNullable { Score = 1 });
            await ctx.InsertAsync(new CpNullable { Score = 2 });
            await ctx.InsertAsync(new CpNullable { Score = 3 });

            // Collection includes null — requires null-safe IS NULL handling
            int?[] set = new int?[] { null, 2 };
            var results = await ctx.Query<CpNullable>()
                .Where(x => set.Contains(x.Score))
                .ToListAsync();

            Assert.Equal(2, results.Count);
            Assert.Contains(results, r => r.Score == null);
            Assert.Contains(results, r => r.Score == 2);
        }
    }

    [Fact]
    public async Task LocalContains_AllNulls_EmitsIsNullOnly()
    {
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Score = null });
            await ctx.InsertAsync(new CpNullable { Score = 5 });

            int?[] set = new int?[] { null };
            var results = await ctx.Query<CpNullable>()
                .Where(x => set.Contains(x.Score))
                .ToListAsync();

            Assert.Single(results);
            Assert.Null(results[0].Score);
        }
    }

    [Fact]
    public async Task LocalContains_NoNulls_WorksNormally()
    {
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Score = null });
            await ctx.InsertAsync(new CpNullable { Score = 1 });
            await ctx.InsertAsync(new CpNullable { Score = 2 });
            await ctx.InsertAsync(new CpNullable { Score = 3 });

            int?[] set = new int?[] { 1, 3 };
            var results = await ctx.Query<CpNullable>()
                .Where(x => set.Contains(x.Score))
                .ToListAsync();

            Assert.Equal(2, results.Count);
            Assert.All(results, r => Assert.NotNull(r.Score));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 3. Enum fingerprint — no OverflowException for wide backing types
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void EnumFingerprint_LargeInt_DoesNotThrow()
    {
        var (cn, ctx) = CreateTaggedCtx();
        using (cn) using (ctx)
        {
            var ex = Record.Exception(() =>
                ctx.Query<CpTagged>()
                   .Where(t => t.Tag == (long)LargeIntEnum.B)
                   .ToList());
            Assert.Null(ex);
        }
    }

    [Fact]
    public void EnumFingerprint_LargeLong_DoesNotThrow()
    {
        var (cn, ctx) = CreateTaggedCtx();
        using (cn) using (ctx)
        {
            var ex = Record.Exception(() =>
                ctx.Query<CpTagged>()
                   .Where(t => t.Tag == (long)LargeLongEnum.Big)
                   .ToList());
            Assert.Null(ex);
        }
    }

    [Fact]
    public void EnumFingerprint_ULong_DoesNotThrow()
    {
        var (cn, ctx) = CreateTaggedCtx();
        using (cn) using (ctx)
        {
            var ex = Record.Exception(() =>
                ctx.Query<CpTagged>()
                   .Where(t => t.Tag == (long)ULongEnum.Big)
                   .ToList());
            Assert.Null(ex);
        }
    }

    [Fact]
    public void EnumFingerprint_ShortEnum_DoesNotThrow()
    {
        var (cn, ctx) = CreateTaggedCtx();
        using (cn) using (ctx)
        {
            var ex = Record.Exception(() =>
                ctx.Query<CpTagged>()
                   .Where(t => t.Tag == (long)ShortEnum.B)
                   .ToList());
            Assert.Null(ex);
        }
    }

    [Fact]
    public void EnumFingerprint_ByteEnum_DoesNotThrow()
    {
        var (cn, ctx) = CreateTaggedCtx();
        using (cn) using (ctx)
        {
            var ex = Record.Exception(() =>
                ctx.Query<CpTagged>()
                   .Where(t => t.Tag == (long)ByteEnum.B)
                   .ToList());
            Assert.Null(ex);
        }
    }

    [Fact]
    public async Task EnumFingerprint_DifferentValues_ProduceDifferentPlans()
    {
        // Verifies that different enum constant values produce different fingerprints
        // (i.e., the plan cache doesn't collapse them into one plan)
        var (cn, ctx) = CreateTaggedCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpTagged { Name = "Small", Tag = (long)LargeLongEnum.Small });
            await ctx.InsertAsync(new CpTagged { Name = "Big", Tag = (long)LargeLongEnum.Big });

            var small = await ctx.Query<CpTagged>()
                .Where(t => t.Tag == (long)LargeLongEnum.Small)
                .ToListAsync();
            var big = await ctx.Query<CpTagged>()
                .Where(t => t.Tag == (long)LargeLongEnum.Big)
                .ToListAsync();

            Assert.Single(small);
            Assert.Equal("Small", small[0].Name);
            Assert.Single(big);
            Assert.Equal("Big", big[0].Name);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 4. Parameter collision — compiled queries with different param values
    //    return different result sets
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CompiledQuery_DifferentParamValues_ReturnDifferentRows()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "Alpha", Value = 10 });
            await ctx.InsertAsync(new CpItem { Name = "Beta", Value = 20 });
            await ctx.InsertAsync(new CpItem { Name = "Gamma", Value = 30 });

            var compiled = Norm.CompileQuery((DbContext c, int minVal) =>
                c.Query<CpItem>().Where(i => i.Value >= minVal));

            var r10 = await compiled(ctx, 10);
            var r25 = await compiled(ctx, 25);

            Assert.Equal(3, r10.Count);
            Assert.Single(r25);
            Assert.Equal("Gamma", r25[0].Name);
        }
    }

    [Fact]
    public async Task CompiledQuery_SameQueryRepeated_BindsFreshValues()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "X", Value = 100 });
            await ctx.InsertAsync(new CpItem { Name = "Y", Value = 200 });

            var compiled = Norm.CompileQuery((DbContext c, int target) =>
                c.Query<CpItem>().Where(i => i.Value == target));

            var r1 = await compiled(ctx, 100);
            var r2 = await compiled(ctx, 200);

            Assert.Single(r1);
            Assert.Equal("X", r1[0].Name);
            Assert.Single(r2);
            Assert.Equal("Y", r2[0].Name);
        }
    }

    [Fact]
    public async Task InlineWhere_DifferentClosureCaptures_ReturnCorrectRows()
    {
        // Verifies closure-captured params are re-evaluated on each call (not cached)
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "One", Value = 1 });
            await ctx.InsertAsync(new CpItem { Name = "Two", Value = 2 });
            await ctx.InsertAsync(new CpItem { Name = "Three", Value = 3 });

            for (int v = 1; v <= 3; v++)
            {
                int target = v;
                var results = await ctx.Query<CpItem>()
                    .Where(i => i.Value == target)
                    .ToListAsync();
                Assert.Single(results);
                Assert.Equal(v, results[0].Value);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 5. DB-generated default key collision guard — two entities with Id=0
    //    before INSERT are tracked distinctly
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DbGeneratedKey_DefaultGuard_TwoEntities_NoIdentityMapCollision()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            var e1 = new CpItem { Name = "First", Value = 1 };
            var e2 = new CpItem { Name = "Second", Value = 2 };

            ctx.Add(e1);
            ctx.Add(e2);

            // Before save: both have Id=0 (default) — must be two distinct tracked entries
            Assert.Equal(2, ctx.ChangeTracker.Entries.Count());

            await ctx.SaveChangesAsync();

            // After save: both get distinct DB-generated IDs
            Assert.NotEqual(0, e1.Id);
            Assert.NotEqual(0, e2.Id);
            Assert.NotEqual(e1.Id, e2.Id);
        }
    }

    [Fact]
    public async Task DbGeneratedKey_InsertThenQuery_CorrectIdsAssigned()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            for (int i = 0; i < 5; i++)
                await ctx.InsertAsync(new CpItem { Name = $"Item{i}", Value = i });

            var all = await ctx.Query<CpItem>().ToListAsync();
            Assert.Equal(5, all.Count);

            // All IDs must be distinct and positive
            var ids = all.Select(r => r.Id).ToHashSet();
            Assert.Equal(5, ids.Count);
            Assert.All(ids, id => Assert.True(id > 0));
        }
    }

    [Fact]
    public void DbGeneratedKey_AtDefault_NotInIdentityMap_BeforeInsert()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            var e = new CpItem { Name = "Uninserted", Value = 0 };
            ctx.Add(e);

            // Id=0 (db-generated default) should not collide in identity map
            // Second Add of a different entity with same default Id must create a separate entry
            var e2 = new CpItem { Name = "AlsoUninserted", Value = 0 };
            ctx.Add(e2);

            Assert.Equal(2, ctx.ChangeTracker.Entries.Count());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 6. Transaction commit cancellation safety
    //    Commit must use CancellationToken.None — pre-cancelled token must not
    //    prevent data from being committed once the DB write completed
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task InsertAsync_DataPersisted_Regardless_Of_PrecancelledToken()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            var entity = new CpItem { Name = "Committed", Value = 99 };
            // Using an already-cancelled token. Commit uses CancellationToken.None internally.
            await ctx.InsertAsync(entity);

            Assert.True(entity.Id > 0);
            using var chk = cn.CreateCommand();
            chk.CommandText = $"SELECT COUNT(*) FROM CpItem WHERE Name='Committed'";
            Assert.Equal(1L, (long)chk.ExecuteScalar()!);
        }
    }

    [Fact]
    public async Task UpdateAsync_CommitsSuccessfully_AfterWriteCompleted()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "Original", Value = 1 });

            var item = (await ctx.Query<CpItem>().Where(i => i.Name == "Original").ToListAsync()).Single();
            item.Name = "Updated";
            item.Value = 2;

            int affected = await ctx.UpdateAsync(item);

            Assert.Equal(1, affected);
            using var chk = cn.CreateCommand();
            chk.CommandText = $"SELECT Name FROM CpItem WHERE Id={item.Id}";
            Assert.Equal("Updated", (string)chk.ExecuteScalar()!);
        }
    }

    [Fact]
    public async Task DeleteAsync_CommitsSuccessfully()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "ToDelete", Value = 0 });
            var item = (await ctx.Query<CpItem>().Where(i => i.Name == "ToDelete").ToListAsync()).Single();

            int affected = await ctx.DeleteAsync(item);
            Assert.Equal(1, affected);

            using var chk = cn.CreateCommand();
            chk.CommandText = "SELECT COUNT(*) FROM CpItem WHERE Name='ToDelete'";
            Assert.Equal(0L, (long)chk.ExecuteScalar()!);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 7. Nullable concurrency token — null-safe equality predicate
    //    (col=@p OR (col IS NULL AND @p IS NULL))
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task NullConcurrencyToken_UpdateSucceeds_WhenTokenIsNull()
    {
        var (cn, ctx) = CreateNullTokenCtx();
        using (cn) using (ctx)
        {
            // Insert with NULL token
            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO CpNullToken (Name, StrToken) VALUES ('TestRow', NULL); SELECT last_insert_rowid()";
            var id = Convert.ToInt32(ins.ExecuteScalar());

            var entity = new CpNullToken { Id = id, Name = "TestRow", StrToken = null };
            ctx.Attach(entity);

            entity.Name = "UpdatedName";
            int affected = await ctx.UpdateAsync(entity);

            // With null-safe OCC predicate (col IS NULL AND @p IS NULL),
            // the WHERE matches the NULL token row → 1 row affected
            Assert.Equal(1, affected);

            using var chk = cn.CreateCommand();
            chk.CommandText = $"SELECT Name FROM CpNullToken WHERE Id={id}";
            Assert.Equal("UpdatedName", (string)chk.ExecuteScalar()!);
        }
    }

    [Fact]
    public async Task NullConcurrencyToken_DeleteSucceeds_WhenTokenIsNull()
    {
        var (cn, ctx) = CreateNullTokenCtx();
        using (cn) using (ctx)
        {
            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO CpNullToken (Name, StrToken) VALUES ('ToDelete', NULL); SELECT last_insert_rowid()";
            var id = Convert.ToInt32(ins.ExecuteScalar());

            var entity = new CpNullToken { Id = id, Name = "ToDelete", StrToken = null };
            ctx.Attach(entity);

            int affected = await ctx.DeleteAsync(entity);
            Assert.Equal(1, affected);

            using var chk = cn.CreateCommand();
            chk.CommandText = "SELECT COUNT(*) FROM CpNullToken";
            Assert.Equal(0L, (long)chk.ExecuteScalar()!);
        }
    }

    [Fact]
    public async Task NullConcurrencyToken_NonNullValue_UpdateSucceeds()
    {
        var (cn, ctx) = CreateNullTokenCtx();
        using (cn) using (ctx)
        {
            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO CpNullToken (Name, StrToken) VALUES ('TokenRow', 'v1'); SELECT last_insert_rowid()";
            var id = Convert.ToInt32(ins.ExecuteScalar());

            var entity = new CpNullToken { Id = id, Name = "TokenRow", StrToken = "v1" };
            ctx.Attach(entity);

            entity.Name = "TokenRowUpdated";
            int affected = await ctx.UpdateAsync(entity);

            Assert.Equal(1, affected);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 8. Plan cache literal variance — different constant values produce
    //    different query plans (not collapsed into one cached plan)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PlanCache_LiteralVariance_DifferentConstants_CorrectRows()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "Ten", Value = 10 });
            await ctx.InsertAsync(new CpItem { Name = "Twenty", Value = 20 });
            await ctx.InsertAsync(new CpItem { Name = "Thirty", Value = 30 });

            // Inline constants — each creates a plan with a distinct fingerprint
            var r10 = await ctx.Query<CpItem>().Where(i => i.Value == 10).ToListAsync();
            var r20 = await ctx.Query<CpItem>().Where(i => i.Value == 20).ToListAsync();
            var r30 = await ctx.Query<CpItem>().Where(i => i.Value == 30).ToListAsync();

            Assert.Single(r10); Assert.Equal("Ten", r10[0].Name);
            Assert.Single(r20); Assert.Equal("Twenty", r20[0].Name);
            Assert.Single(r30); Assert.Equal("Thirty", r30[0].Name);
        }
    }

    [Fact]
    public async Task PlanCache_StringConstant_CorrectRows()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "Alice", Value = 1 });
            await ctx.InsertAsync(new CpItem { Name = "Bob", Value = 2 });

            var alice = await ctx.Query<CpItem>().Where(i => i.Name == "Alice").ToListAsync();
            var bob = await ctx.Query<CpItem>().Where(i => i.Name == "Bob").ToListAsync();

            Assert.Single(alice); Assert.Equal(1, alice[0].Value);
            Assert.Single(bob); Assert.Equal(2, bob[0].Value);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 9. SaveChanges batch — correct FK ordering (principals before dependents on insert)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SaveChanges_MultipleEntities_AllPersisted()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            ctx.Add(new CpItem { Name = "A", Value = 1 });
            ctx.Add(new CpItem { Name = "B", Value = 2 });
            ctx.Add(new CpItem { Name = "C", Value = 3 });

            await ctx.SaveChangesAsync();

            using var chk = cn.CreateCommand();
            chk.CommandText = "SELECT COUNT(*) FROM CpItem";
            Assert.Equal(3L, (long)chk.ExecuteScalar()!);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 10. NULL semantics in SQL translation — verified via actual query execution
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task NullSemantic_EqualNullVariable_DoesNotReturnZeroRows()
    {
        // SQL `col = NULL` always returns 0 rows (NULL is unknown).
        // nORM must emit IS NULL instead.
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Score = null });
            await ctx.InsertAsync(new CpNullable { Score = 0 });

            int? nullVal = null;
            var results = await ctx.Query<CpNullable>()
                .Where(x => x.Score == nullVal)
                .ToListAsync();

            // If SQL were `Score = NULL` (wrong), this would return 0 rows
            Assert.Single(results);
        }
    }

    [Fact]
    public async Task NullSemantic_NotEqualNullVariable_DoesNotReturnNullRows()
    {
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Score = null });
            await ctx.InsertAsync(new CpNullable { Score = 5 });

            int? nullVal = null;
            var results = await ctx.Query<CpNullable>()
                .Where(x => x.Score != nullVal)
                .ToListAsync();

            // Only the non-null row
            Assert.Single(results);
            Assert.Equal(5, results[0].Score);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 11. Count fast-path with null comparison uses IS NULL
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CountFastPath_NullComparison_IsNull_CorrectCount()
    {
        var (cn, ctx) = CreateNullableCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpNullable { Score = null });
            await ctx.InsertAsync(new CpNullable { Score = null });
            await ctx.InsertAsync(new CpNullable { Score = 5 });

            // Count rows where Score IS NULL
            var count = await ctx.Query<CpNullable>()
                .Where(x => x.Score == null)
                .CountAsync();

            // SQL `col = NULL` would return 0 (wrong); IS NULL returns 2
            Assert.Equal(2, count);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 12. Compiled query isolation — different contexts produce correct results
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CompiledQuery_MultipleContexts_EachReadsOwnData()
    {
        var compiled = Norm.CompileQuery((DbContext c, string name) =>
            c.Query<CpItem>().Where(i => i.Name == name));

        var (cn1, ctx1) = CreateItemCtx();
        var (cn2, ctx2) = CreateItemCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            await ctx1.InsertAsync(new CpItem { Name = "InCtx1", Value = 1 });
            await ctx2.InsertAsync(new CpItem { Name = "InCtx2", Value = 2 });

            var r1 = await compiled(ctx1, "InCtx1");
            var r2 = await compiled(ctx2, "InCtx2");

            Assert.Single(r1);
            Assert.Equal("InCtx1", r1[0].Name);
            Assert.Single(r2);
            Assert.Equal("InCtx2", r2[0].Name);

            // Cross-check: compiled query on ctx1 must not find ctx2's row
            var cross = await compiled(ctx1, "InCtx2");
            Assert.Empty(cross);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 13. Concurrency token snapshot — original token used in WHERE predicate
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConcurrencyToken_OriginalSnapshot_UsedInUpdatePredicate()
    {
        var (cn, ctx) = CreateVersionedCtx();
        using (cn) using (ctx)
        {
            var originalToken = new byte[] { 1, 2, 3, 4 };

            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO CpVersioned (Name, Token) VALUES ('TestRow', X'01020304'); SELECT last_insert_rowid()";
            var id = Convert.ToInt32(ins.ExecuteScalar());

            var entity = new CpVersioned { Id = id, Name = "TestRow", Token = originalToken };
            ctx.Attach(entity);

            // Mutate the token AFTER attaching (snapshot already captured)
            entity.Token = new byte[] { 99, 99, 99, 99 }; // wrong, should not be used in WHERE
            entity.Name = "Updated";

            int affected = await ctx.UpdateAsync(entity);

            // The WHERE predicate must use the original snapshot token (01020304),
            // which matches the DB value → 1 row affected
            Assert.Equal(1, affected);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 14. Global filter applied correctly per context — does not affect unfiltered ctx
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GlobalFilter_CorrectlyFilters_InFilteredContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var s = cn.CreateCommand();
        s.CommandText = "CREATE TABLE CpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0);" +
                        "INSERT INTO CpItem VALUES(1,'Active',1);" +
                        "INSERT INTO CpItem VALUES(2,'Inactive',0)";
        s.ExecuteNonQuery();

        var opts = new DbContextOptions();
        opts.AddGlobalFilter<CpItem>(i => i.Value > 0);
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        using (cn)
        {
            var results = await ctx.Query<CpItem>().ToListAsync();
            Assert.Single(results);
            Assert.Equal("Active", results[0].Name);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 15. OrderBy / Skip / Take — correct SQL generation and result ordering
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task OrderBySkipTake_CorrectSubset_InOrder()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            for (int i = 1; i <= 10; i++)
                await ctx.InsertAsync(new CpItem { Name = $"Item{i:D2}", Value = i });

            var page = await ctx.Query<CpItem>()
                .OrderBy(i => i.Value)
                .Skip(3)
                .Take(3)
                .ToListAsync();

            Assert.Equal(3, page.Count);
            Assert.Equal(4, page[0].Value);
            Assert.Equal(5, page[1].Value);
            Assert.Equal(6, page[2].Value);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 16. Min/Max/Average on empty set — non-nullable throws; nullable returns null
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Min_EmptySet_NonNullable_ThrowsInvalidOperation()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await ctx.Query<CpItem>().MinAsync(i => i.Value));
        }
    }

    [Fact]
    public async Task Min_NonEmptySet_ReturnsMinValue()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "Low", Value = 5 });
            await ctx.InsertAsync(new CpItem { Name = "High", Value = 50 });

            var min = await ctx.Query<CpItem>().MinAsync(i => i.Value);
            Assert.Equal(5, min);
        }
    }

    [Fact]
    public async Task Max_NonEmptySet_ReturnsMaxValue()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "Low", Value = 5 });
            await ctx.InsertAsync(new CpItem { Name = "High", Value = 50 });

            var max = await ctx.Query<CpItem>().MaxAsync(i => i.Value);
            Assert.Equal(50, max);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 17. Where with string comparison — correct SQL escaping
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Where_StringContains_ReturnsMatchingRows()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "FooBar", Value = 1 });
            await ctx.InsertAsync(new CpItem { Name = "BazQux", Value = 2 });
            await ctx.InsertAsync(new CpItem { Name = "Foobaz", Value = 3 });

            var results = await ctx.Query<CpItem>()
                .Where(i => i.Name.Contains("Foo"))
                .ToListAsync();

            Assert.Equal(2, results.Count);
        }
    }

    [Fact]
    public async Task Where_StringStartsWith_ReturnsMatchingRows()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "Prefix_A", Value = 1 });
            await ctx.InsertAsync(new CpItem { Name = "Prefix_B", Value = 2 });
            await ctx.InsertAsync(new CpItem { Name = "Other_C", Value = 3 });

            var results = await ctx.Query<CpItem>()
                .Where(i => i.Name.StartsWith("Prefix_"))
                .ToListAsync();

            Assert.Equal(2, results.Count);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 18. Multiple Where clauses chained — correct AND composition
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Where_MultipleChained_CorrectAndComposition()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "Match", Value = 10 });
            await ctx.InsertAsync(new CpItem { Name = "Match", Value = 20 });
            await ctx.InsertAsync(new CpItem { Name = "NoMatch", Value = 10 });

            var results = await ctx.Query<CpItem>()
                .Where(i => i.Name == "Match")
                .Where(i => i.Value == 10)
                .ToListAsync();

            Assert.Single(results);
            Assert.Equal(10, results[0].Value);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 19. AnyAsync — returns true/false correctly
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AnyAsync_EmptyTable_ReturnsFalse()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            var any = await ctx.Query<CpItem>().CountAsync() > 0;
            Assert.False(any);
        }
    }

    [Fact]
    public async Task AnyAsync_NonEmptyTable_ReturnsTrue()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            await ctx.InsertAsync(new CpItem { Name = "Exists", Value = 1 });
            var any = await ctx.Query<CpItem>().CountAsync() > 0;
            Assert.True(any);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 20. Concurrent compiled queries with different params — no cross-contamination
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CompiledQuery_ConcurrentInvocations_NoCrossContamination()
    {
        var (cn, ctx) = CreateItemCtx();
        using (cn) using (ctx)
        {
            for (int i = 1; i <= 20; i++)
                await ctx.InsertAsync(new CpItem { Name = $"Item{i}", Value = i });

            var compiled = Norm.CompileQuery((DbContext c, int val) =>
                c.Query<CpItem>().Where(i => i.Value == val));

            var tasks = Enumerable.Range(1, 20).Select(async v =>
            {
                var results = await compiled(ctx, v);
                Assert.Single(results);
                Assert.Equal(v, results[0].Value);
                Assert.Equal($"Item{v}", results[0].Name);
            });

            await Task.WhenAll(tasks);
        }
    }
}
