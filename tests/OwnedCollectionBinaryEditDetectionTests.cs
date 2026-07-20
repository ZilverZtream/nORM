using System;
using System.Collections.Generic;
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
/// Owned children are not tracked as independent entries; the only change detection is a per-child content
/// signature. Building that signature from <c>col.Getter(item)?.ToString()</c> is blind to any column whose
/// CLR ToString() is not value-distinguishing: a byte[] renders as "System.Byte[]" for every value, and a
/// DateTime's default format drops sub-second precision. A content-only edit to such a column then leaves the
/// signature unchanged, the owner Unchanged, and the owned-collection sync never runs — silently dropping the
/// edit. The signature must be value-faithful.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedCollectionBinaryEditDetectionTests
{
    [Table("OcbPost")]
    public class Post
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Blob> Blobs { get; set; } = new();
    }

    public class Blob
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public byte[] Data { get; set; } = Array.Empty<byte>();
        public DateTime Stamp { get; set; }
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:ocb_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OcbPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE OcbBlob (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Data BLOB NOT NULL, Stamp TEXT NOT NULL);
                INSERT INTO OcbPost VALUES (1, 'p');
                INSERT INTO OcbBlob (PostId, Data, Stamp) VALUES (1, X'010203', '2020-01-01 00:00:00.000');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Blob>(p => p.Blobs, tableName: "OcbBlob", foreignKey: "PostId")
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static byte[] ReadData(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT Data FROM OcbBlob WHERE PostId = 1";
        return (byte[])cmd.ExecuteScalar()!;
    }

    private static string ReadStamp(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT Stamp FROM OcbBlob WHERE PostId = 1";
        return (string)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task Editing_owned_child_byte_array_only_persists()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Blobs).ToList().Single();
        Assert.Single(post.Blobs);

        post.Blobs[0].Data = new byte[] { 9, 9, 9 };   // was {1,2,3}
        await ctx.SaveChangesAsync();

        Assert.Equal(new byte[] { 9, 9, 9 }, ReadData(keeper));  // BUG: still {1,2,3} - edit undetected
    }

    [Fact]
    public async Task Editing_owned_child_subsecond_datetime_only_persists()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Blobs).ToList().Single();
        // Same second, different milliseconds: default DateTime.ToString() ("G") drops the fractional part,
        // so a signature built from ToString() would miss this edit entirely.
        post.Blobs[0].Stamp = new DateTime(2020, 1, 1, 0, 0, 0, 500);
        await ctx.SaveChangesAsync();

        // Reload through a fresh context (format-agnostic): the sub-second edit must have persisted.
        await using var verify = make();
        var reloaded = ((INormQueryable<Post>)verify.Query<Post>()).Include(p => p.Blobs).ToList().Single();
        Assert.Equal(new DateTime(2020, 1, 1, 0, 0, 0, 500), reloaded.Blobs[0].Stamp);  // BUG: was .000
    }
}
