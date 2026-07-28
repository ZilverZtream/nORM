using System;
using System.Collections.Generic;
using System.ComponentModel;
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
/// An entity that implements INotifyPropertyChanged is switched to notification-based change tracking and
/// excluded from the snapshot scan. Notifications fire only for its own scalar setters, so a pure
/// owned-collection edit (add/remove) — which raises no PropertyChanged on the owner — was never detected:
/// the owner stayed Unchanged, SaveChanges collected nothing, and the edit was silently dropped. A POCO owner
/// in the identical scenario persists correctly. An INPC owner with owned/m2m navigations must still be
/// scanned for association/owned edits.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NotifyingOwnerOwnedEditDetectionTests
{
    [Table("NooPost")]
    public class Post : INotifyPropertyChanged
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        private string _title = "";
        public string Title
        {
            get => _title;
            set { if (_title != value) { _title = value; PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Title))); } }
        }
        public List<Blob> Blobs { get; set; } = new();
        public event PropertyChangedEventHandler? PropertyChanged;
    }

    public class Blob
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Data { get; set; } = "";
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:noo_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NooPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE NooBlob (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Data TEXT NOT NULL);
                INSERT INTO NooPost VALUES (1, 'p');
                INSERT INTO NooBlob (PostId, Data) VALUES (1, 'first');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Blob>(p => p.Blobs, tableName: "NooBlob", foreignKey: "PostId")
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static long BlobCount(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM NooBlob WHERE PostId = 1";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Notifying_owner_owned_collection_add_is_persisted()
    {
        var (keeper, make) = Setup();
        using var _keeper = keeper;

        await using (var ctx = make())
        {
            var post = ctx.Query<Post>().Include(p => p.Blobs).ToList().Single();
            post.Blobs.Add(new Blob { Data = "added" });   // owned-collection-only edit, no scalar change
            await ctx.SaveChangesAsync();
        }

        Assert.Equal(2, BlobCount(keeper));   // the added child must be persisted, not silently dropped
    }

    [Fact]
    public async Task Notifying_owner_owned_collection_clear_is_persisted()
    {
        var (keeper, make) = Setup();
        using var _keeper = keeper;

        await using (var ctx = make())
        {
            var post = ctx.Query<Post>().Include(p => p.Blobs).ToList().Single();
            post.Blobs.Clear();   // owned-collection removal, no scalar change
            await ctx.SaveChangesAsync();
        }

        Assert.Equal(0, BlobCount(keeper));
    }

    [Fact]
    public async Task Notifying_owner_scalar_edit_still_tracked()
    {
        var (keeper, make) = Setup();
        using var _keeper = keeper;

        await using (var ctx = make())
        {
            var post = ctx.Query<Post>().Include(p => p.Blobs).ToList().Single();
            post.Title = "changed";
            await ctx.SaveChangesAsync();
        }

        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT Title FROM NooPost WHERE Id = 1";
        Assert.Equal("changed", (string)cmd.ExecuteScalar()!);
    }
}

/// <summary>
/// The owned-REFERENCE variant of the same silent-lost-update: a mutation inside an owned value object
/// (order.Subtotal.Amount = 250) raises no PropertyChanged on the INPC owner, so without scanning it the
/// flattened owned column change is never detected and the write is silently dropped.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NotifyingOwnerOwnedReferenceEditDetectionTests
{
    public class Money
    {
        public decimal Amount { get; set; }
        public string Currency { get; set; } = "";
    }

    [Table("NorOrder")]
    public class Order : INotifyPropertyChanged
    {
        [Key] public int Id { get; set; }
        private string _note = "";
        public string Note
        {
            get => _note;
            set { if (_note != value) { _note = value; PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Note))); } }
        }
        public Money Subtotal { get; set; } = new();
        public event PropertyChangedEventHandler? PropertyChanged;
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NorOrder (Id INTEGER PRIMARY KEY, Note TEXT NOT NULL, " +
                "Subtotal_Amount TEXT NOT NULL, Subtotal_Currency TEXT NOT NULL);" +
                "INSERT INTO NorOrder VALUES (1, 'n', '100', 'USD');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsOne(o => o.Subtotal);
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Notifying_owner_owned_reference_edit_is_persisted()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; await using var _ctx = ctx;

        var order = ctx.Query<Order>().First();
        order.Subtotal.Amount = 250m;   // owned-reference sub-property edit, no scalar edit on Order
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Subtotal_Amount FROM NorOrder WHERE Id = 1";
        Assert.Equal(250m, Convert.ToDecimal(cmd.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture));
    }
}
