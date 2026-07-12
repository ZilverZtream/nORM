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
/// Two foreign keys from one entity to the SAME principal type — the classic
/// Message { SenderId, ReceiverId } → User shape. Relationship discovery and
/// eager loading must keep the two relationships distinct: a user's SentMessages
/// must be correlated on SenderId and ReceivedMessages on ReceiverId, never mixed.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DualForeignKeySamePrincipalTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DfkUser (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE DfkMessage (Id INTEGER PRIMARY KEY, SenderId INTEGER NOT NULL, ReceiverId INTEGER NOT NULL, Body TEXT NOT NULL);
            INSERT INTO DfkUser VALUES (1,'Alice'),(2,'Bob');
            -- Alice(1) -> Bob(2): "hi"; Bob(2) -> Alice(1): "hello"; Alice(1) -> Bob(2): "again"
            INSERT INTO DfkMessage VALUES
              (10, 1, 2, 'hi'),
              (11, 2, 1, 'hello'),
              (12, 1, 2, 'again');
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<DfkUser>().HasKey(u => u.Id);
                mb.Entity<DfkMessage>().HasKey(m => m.Id);
                mb.Entity<DfkUser>()
                    .HasMany(u => u.SentMessages).WithOne(m => m.Sender).HasForeignKey(m => m.SenderId, u => u.Id);
                mb.Entity<DfkUser>()
                    .HasMany(u => u.ReceivedMessages).WithOne(m => m.Receiver).HasForeignKey(m => m.ReceiverId, u => u.Id);
            }
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), opts);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Include_keeps_sent_and_received_collections_distinct()
    {
        var users = (await ((INormQueryable<DfkUser>)_ctx.Query<DfkUser>())
            .Include(u => u.SentMessages)
            .Include(u => u.ReceivedMessages)
            .AsSplitQuery()
            .OrderBy(u => u.Id)
            .ToListAsync())
            .ToDictionary(u => u.Id);

        // Alice sent "hi" and "again"; received "hello".
        Assert.Equal(new[] { "again", "hi" }, users[1].SentMessages.Select(m => m.Body).OrderBy(b => b));
        Assert.Equal(new[] { "hello" }, users[1].ReceivedMessages.Select(m => m.Body));

        // Bob sent "hello"; received "hi" and "again".
        Assert.Equal(new[] { "hello" }, users[2].SentMessages.Select(m => m.Body));
        Assert.Equal(new[] { "again", "hi" }, users[2].ReceivedMessages.Select(m => m.Body).OrderBy(b => b));
    }

    [Table("DfkUser")]
    public sealed class DfkUser
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<DfkMessage> SentMessages { get; set; } = new();
        public List<DfkMessage> ReceivedMessages { get; set; } = new();
    }

    [Table("DfkMessage")]
    public sealed class DfkMessage
    {
        [Key] public int Id { get; set; }
        public int SenderId { get; set; }
        public int ReceiverId { get; set; }
        public string Body { get; set; } = "";
        public DfkUser Sender { get; set; } = null!;
        public DfkUser Receiver { get; set; } = null!;
    }
}
