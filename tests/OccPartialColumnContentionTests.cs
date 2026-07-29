using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial OCC probes for the PARTIAL-COLUMN UPDATE path combined with a [Timestamp] token.
/// Every existing OCC test entity has exactly ONE mutable non-token column, so the partial-column
/// UPDATE (ComputeUpdateSetColumns → a SET clause narrower than the full mutable set) has never been
/// exercised together with optimistic concurrency. The worst-class risk: a stale writer that changes
/// a DIFFERENT column than the winner is silently applied, because the narrow partial UPDATE still
/// "matches" the row. These tests prove the token WHERE predicate is enforced on partial updates and
/// that the batch rolls back atomically — verified by RAW row reads, never through nORM.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class OccPartialColumnContentionTests
{
    [Table("OccMultiCol")]
    public class OccMultiCol
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Balance { get; set; }
        [Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    [Table("OccExplicitKey")]
    public class OccExplicitKey
    {
        [Key] public int Id { get; set; }            // convention key: an explicit non-zero value is honored on SQLite
        public string Payload { get; set; } = "";
        [Timestamp] public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    [Table("OccMultiCol")]  // same table shape, but NO [Timestamp] — the token-less contrast
    public class NoTokenMultiCol
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Balance { get; set; }
    }

    private static (SqliteConnection keeper, string cs) SharedDb(string ddl)
    {
        var cs = $"Data Source=file:occpc_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return (keeper, cs);
    }

    private static DbContext Open(string cs)
    {
        var cn = new SqliteConnection(cs);
        cn.Open();
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });
    }

    private static (string Name, long Balance, byte[] Token) RowState(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name, Balance, Token FROM OccMultiCol WHERE Id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read(), $"row {id} vanished");
        var token = (byte[])r["Token"];
        return (r.GetString(0), r.GetInt64(1), token);
    }

    private const string MultiColDdl =
        "CREATE TABLE OccMultiCol (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Balance INTEGER NOT NULL, Token BLOB NOT NULL);";

    // ── 1. Partial update preserves the untouched column and advances the token ──────────────

    [Fact]
    public async Task Partial_update_changing_one_column_preserves_the_other_and_advances_token()
    {
        var (keeper, cs) = SharedDb(MultiColDdl +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (1, 'alice', 100, X'01');");
        using var _keeper = keeper;
        using var ctx = Open(cs);

        var row = await ctx.Query<OccMultiCol>().Where(r => r.Id == 1).FirstAsync();

        // Change ONLY Balance → a partial UPDATE (SET Balance, Token WHERE ...), Name must be untouched.
        row.Balance = 150;
        await ctx.SaveChangesAsync();

        var afterFirst = RowState(keeper, 1);
        Assert.Equal("alice", afterFirst.Name);                       // untouched column preserved
        Assert.Equal(150L, afterFirst.Balance);
        Assert.False(afterFirst.Token.SequenceEqual(new byte[] { 1 }), "token must advance on a partial update");

        // Change ONLY Name → must match the advanced token (no false conflict), Balance preserved.
        row.Name = "bob";
        await ctx.SaveChangesAsync();

        var afterSecond = RowState(keeper, 1);
        Assert.Equal("bob", afterSecond.Name);
        Assert.Equal(150L, afterSecond.Balance);                     // Balance from the first save survives
    }

    // ── 2. THE silent-loss probe: a stale writer changing a DIFFERENT column is rejected ─────

    [Fact]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public async Task Partial_update_stale_writer_on_a_different_column_is_rejected_not_silently_applied()
    {
        var (keeper, cs) = SharedDb(MultiColDdl +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (1, 'orig-name', 100, X'01');");
        using var _keeper = keeper;
        using var ctxA = Open(cs);
        using var ctxB = Open(cs);

        var a = await ctxA.Query<OccMultiCol>().Where(r => r.Id == 1).FirstAsync();
        var b = await ctxB.Query<OccMultiCol>().Where(r => r.Id == 1).FirstAsync();

        // Writer A changes only Name and commits first — the row token advances.
        a.Name = "A-name";
        await ctxA.SaveChangesAsync();

        // Writer B is now stale and changes only Balance (a DIFFERENT column). The partial UPDATE
        // touches only Balance, but the token WHERE must still reject it — otherwise B's Balance write
        // silently lands on top of A's committed row (a lost update masked by the non-overlapping column).
        b.Balance = 999;
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctxB.SaveChangesAsync());

        var final = RowState(keeper, 1);
        Assert.Equal("A-name", final.Name);       // A's change survived
        Assert.Equal(100L, final.Balance);        // B's stale Balance write was NOT applied
    }

    // ── 3. Multi-entity partial-column batch, one stale token → atomic rollback of ALL ───────

    [Fact]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public async Task Partial_update_batch_with_one_stale_token_rolls_back_every_row()
    {
        var (keeper, cs) = SharedDb(MultiColDdl +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (1, 'n1', 10, X'01');" +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (2, 'n2', 20, X'02');" +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (3, 'n3', 30, X'03');");
        using var _keeper = keeper;
        using var ctx = Open(cs);

        var rows = (await ctx.Query<OccMultiCol>().ToListAsync()).OrderBy(r => r.Id).ToList();

        // A concurrent writer bumps row 2's token AFTER this context loaded it → row 2 is now stale.
        using (var bump = keeper.CreateCommand())
        {
            bump.CommandText = "UPDATE OccMultiCol SET Token = X'FF' WHERE Id = 2";
            bump.ExecuteNonQuery();
        }

        // Each row gets a DIFFERENT single column changed → three distinct partial UPDATEs in one batch.
        rows[0].Name = "n1-v2";      // partial: Name only
        rows[1].Balance = 222;       // partial: Balance only (this is the stale row)
        rows[2].Name = "n3-v2";      // partial: Name only

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());

        // Atomic rollback: NONE of the three partial updates may have landed.
        var s1 = RowState(keeper, 1);
        var s3 = RowState(keeper, 3);
        Assert.Equal("n1", s1.Name);      // row 1's partial Name update rolled back
        Assert.Equal(10L, s1.Balance);
        Assert.Equal("n3", s3.Name);      // row 3's partial Name update rolled back
        Assert.Equal(30L, s3.Balance);
        // Row 2 keeps the concurrent writer's token; its Balance was never applied.
        Assert.Equal(20L, RowState(keeper, 2).Balance);
    }

    // ── 4. Recovery after a partial-column batch conflict does not poison the context ────────

    [Fact]
    public async Task After_a_partial_batch_conflict_reloading_the_stale_row_lets_the_save_land()
    {
        var (keeper, cs) = SharedDb(MultiColDdl +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (1, 'n1', 10, X'01');" +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (2, 'n2', 20, X'02');");
        using var _keeper = keeper;
        using var ctx = Open(cs);

        var rows = (await ctx.Query<OccMultiCol>().ToListAsync()).OrderBy(r => r.Id).ToList();

        using (var bump = keeper.CreateCommand())
        {
            bump.CommandText = "UPDATE OccMultiCol SET Token = X'FF', Balance = 200 WHERE Id = 2";
            bump.ExecuteNonQuery();
        }

        rows[0].Name = "n1-v2";
        rows[1].Balance = 222;
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());

        // Nothing landed. Reload row 2 fresh (picks up the concurrent token + Balance=200) and re-apply
        // both edits. row 1's OriginalToken must NOT have been advanced by the rolled-back attempt, or its
        // re-save would false-conflict.
        ctx.ChangeTracker.Clear();
        var reload = (await ctx.Query<OccMultiCol>().ToListAsync()).OrderBy(r => r.Id).ToList();
        reload[0].Name = "n1-v2";
        reload[1].Balance = 222;
        await ctx.SaveChangesAsync();

        Assert.Equal("n1-v2", RowState(keeper, 1).Name);
        Assert.Equal(222L, RowState(keeper, 2).Balance);
    }

    // ── 5. Direct active-record UpdateAsync with a multi-column OCC entity rejects a stale writer ──

    [Fact]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public async Task Direct_UpdateAsync_multicolumn_stale_writer_is_rejected()
    {
        var (keeper, cs) = SharedDb(MultiColDdl +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (1, 'orig', 100, X'01');");
        using var _keeper = keeper;
        using var ctxA = Open(cs);
        using var ctxB = Open(cs);

        var a = await ctxA.Query<OccMultiCol>().Where(r => r.Id == 1).FirstAsync();
        var b = await ctxB.Query<OccMultiCol>().Where(r => r.Id == 1).FirstAsync();

        a.Name = "A-name";
        await ctxA.UpdateAsync(a);

        b.Balance = 777;
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctxB.UpdateAsync(b));

        var final = RowState(keeper, 1);
        Assert.Equal("A-name", final.Name);
        Assert.Equal(100L, final.Balance);
    }

    // ── 6. Interleaved INSERT of the same explicit PK is fail-loud, not a silent overwrite ───

    [Fact]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public async Task Interleaved_insert_of_same_explicit_pk_one_wins_the_other_fails_loud()
    {
        var (keeper, cs) = SharedDb(
            "CREATE TABLE OccExplicitKey (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Token BLOB NOT NULL);");
        using var _keeper = keeper;
        using var ctxA = Open(cs);
        using var ctxB = Open(cs);

        ctxA.Add(new OccExplicitKey { Id = 5, Payload = "A", Token = new byte[] { 1 } });
        await ctxA.SaveChangesAsync();

        // Second context inserts the SAME primary key. It must fail loudly (UNIQUE violation),
        // never silently overwrite A's committed row via an INSERT-OR-REPLACE style write.
        ctxB.Add(new OccExplicitKey { Id = 5, Payload = "B", Token = new byte[] { 2 } });
        var ex = await Record.ExceptionAsync(() => ctxB.SaveChangesAsync());
        Assert.NotNull(ex);
        Assert.IsNotType<DbConcurrencyException>(ex);   // it's a constraint violation, not an OCC conflict

        using var read = keeper.CreateCommand();
        read.CommandText = "SELECT Payload FROM OccExplicitKey WHERE Id = 5";
        Assert.Equal("A", (string)read.ExecuteScalar()!);
    }

    // ── 7. Explicit PropertyEntry.IsModified partial UPDATE + OCC rejects a stale writer ─────

    [Fact]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public async Task Explicit_IsModified_partial_update_still_enforces_the_token()
    {
        var (keeper, cs) = SharedDb(MultiColDdl +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (1, 'orig-name', 100, X'01');");
        using var _keeper = keeper;
        using var ctxA = Open(cs);
        using var ctxB = Open(cs);

        var a = await ctxA.Query<OccMultiCol>().Where(r => r.Id == 1).FirstAsync();
        var b = await ctxB.Query<OccMultiCol>().Where(r => r.Id == 1).FirstAsync();

        // Writer A commits a partial update via explicit column marking.
        a.Name = "A-name";
        ctxA.Entry(a).Property(nameof(OccMultiCol.Name)).IsModified = true;
        await ctxA.SaveChangesAsync();

        // Writer B force-marks Balance modified via the explicit API (its VALUE is even unchanged) — the
        // partial UPDATE it emits must still carry the token WHERE and be rejected as stale.
        ctxB.Entry(b).Property(nameof(OccMultiCol.Balance)).IsModified = true;
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctxB.SaveChangesAsync());

        var final = RowState(keeper, 1);
        Assert.Equal("A-name", final.Name);
        Assert.Equal(100L, final.Balance);
    }

    // ── 8. Token advances correctly through a long run of alternating partial updates ────────

    [Fact]
    public async Task Alternating_partial_column_updates_advance_the_token_without_false_conflicts()
    {
        var (keeper, cs) = SharedDb(MultiColDdl +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (1, 'name0', 0, X'01');");
        using var _keeper = keeper;
        using var ctx = Open(cs);

        var row = await ctx.Query<OccMultiCol>().Where(r => r.Id == 1).FirstAsync();

        // Alternate which single column changes each save; each is a partial UPDATE that must match the
        // token stamped by the previous save. A stale OriginalToken snapshot would throw a false conflict.
        for (int i = 1; i <= 8; i++)
        {
            if (i % 2 == 1) row.Name = "name" + i;
            else row.Balance = i;
            await ctx.SaveChangesAsync();
        }

        var final = RowState(keeper, 1);
        Assert.Equal("name7", final.Name);
        Assert.Equal(8L, final.Balance);
    }

    // ── 9. Teeth-check contrast: WITHOUT a token, the same partial writes both land ──────────

    /// <summary>
    /// Proves the token-enforcement probe (test 2) is not vacuous. The IDENTICAL table and the IDENTICAL
    /// two-writer / different-column scenario, but with the [Timestamp] column removed, lets BOTH partial
    /// updates land (last-writer-wins, no conflict) — so the DbConcurrencyException and the Balance=100
    /// assertion in test 2 are caused specifically by the token WHERE predicate, not by the partial-column
    /// mechanics. If nORM ever stopped checking the token on a partial update, test 2's Balance would read
    /// 999 exactly like this contrast — which its raw-row assertion would catch.
    /// </summary>
    [Fact]
    public async Task Contrast_without_token_partial_updates_on_different_columns_both_land()
    {
        var (keeper, cs) = SharedDb(MultiColDdl +
            "INSERT INTO OccMultiCol (Id, Name, Balance, Token) VALUES (1, 'orig-name', 100, X'01');");
        using var _keeper = keeper;
        using var ctxA = Open(cs);
        using var ctxB = Open(cs);

        var a = await ctxA.Query<NoTokenMultiCol>().Where(r => r.Id == 1).FirstAsync();
        var b = await ctxB.Query<NoTokenMultiCol>().Where(r => r.Id == 1).FirstAsync();

        a.Name = "A-name";
        await ctxA.SaveChangesAsync();

        b.Balance = 999;
        await ctxB.SaveChangesAsync();   // no token → no conflict; the partial Balance update lands

        var final = RowState(keeper, 1);
        Assert.Equal("A-name", final.Name);   // A's partial Name change
        Assert.Equal(999L, final.Balance);    // B's partial Balance change — both survive without a token
    }
}
