using System;
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
/// Guards against SILENT-WRONG read results from KEY/VALUE equality comparing BOXED CLR values of
/// DIFFERENT runtime types on the CLIENT-SIDE SET OPERATION path (Union/Intersect/Except combining a DB
/// scalar projection with a LOCAL in-memory sequence via LINQ-to-Objects EqualityComparer&lt;object&gt;.Default).
/// If the DB arm materialized a narrow-width column (short/byte) as SQLite's Int64 box while the local
/// sequence boxed as the model width, object.Equals((long)10,(short)10)==false would silently corrupt the
/// set op. These confirm the DB arm materializes to the MODEL width, so the boxes match and results are correct.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ClientSetOperationKeyWidthTests
{
    [Table("BkxWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public short Code { get; set; }   // short column (narrower than SQLite Int64)
        public byte Bucket { get; set; }  // byte column
    }

    private static DbContext NewCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE BkxWidget (Id INTEGER PRIMARY KEY, Code INTEGER NOT NULL, Bucket INTEGER NOT NULL);" +
                "INSERT INTO BkxWidget VALUES (1,10,100),(2,20,101),(3,30,102);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    // ---------------------------------------------------------------------------------------------
    // 1. CONTROL: prove the DB arm alone materializes the 3 short values correctly (so any set-op
    //    failure below is the client set-op equality, not the projection).
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public void Short_scalar_projection_materializes_all_rows()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;
        var all = ctx.Query<Widget>().Select(w => w.Code).OrderBy(x => x).ToList();
        Assert.Equal(new short[] { 10, 20, 30 }, all.ToArray());
    }

    // ---------------------------------------------------------------------------------------------
    // 2. Client Intersect of a short DB projection with a local short[]. Expected {10,20}.
    //    If the DB arm boxes long and the local boxes short, object.Equals misses -> EMPTY.
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public void Client_intersect_short_projection_with_local_short_array()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;
        var local = new short[] { 10, 20 };
        var result = ctx.Query<Widget>().Select(w => w.Code).Intersect(local).ToList();
        Assert.Equal(new short[] { 10, 20 }, result.OrderBy(x => x).ToArray());
    }

    // ---------------------------------------------------------------------------------------------
    // 3. Client Except of a short DB projection with a local short[]. Expected {30}.
    //    If box types mismatch, NOTHING is excluded -> {10,20,30}.
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public void Client_except_short_projection_with_local_short_array()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;
        var local = new short[] { 10, 20 };
        var result = ctx.Query<Widget>().Select(w => w.Code).Except(local).ToList();
        Assert.Equal(new short[] { 30 }, result.OrderBy(x => x).ToArray());
    }

    // ---------------------------------------------------------------------------------------------
    // 4. Client Union of a short DB projection with a local short[] that overlaps. Expected the
    //    union deduped: {10,20,30}. If box types mismatch, 10 is NOT deduped -> 4 items.
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public void Client_union_short_projection_dedups_against_local_short_array()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;
        var local = new short[] { 10 };
        var result = ctx.Query<Widget>().Select(w => w.Code).Union(local).ToList();
        Assert.Equal(new short[] { 10, 20, 30 }, result.OrderBy(x => x).ToArray());
    }

    // ---------------------------------------------------------------------------------------------
    // 5. Same as (2) but a byte column vs a local byte[]. Expected {100,101}.
    // ---------------------------------------------------------------------------------------------
    [Fact]
    public void Client_intersect_byte_projection_with_local_byte_array()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;
        var local = new byte[] { 100, 101 };
        var result = ctx.Query<Widget>().Select(w => w.Bucket).Intersect(local).ToList();
        Assert.Equal(new byte[] { 100, 101 }, result.OrderBy(x => x).ToArray());
    }
}
