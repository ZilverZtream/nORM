using System;
using System.ComponentModel;
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
/// Partial-column UPDATEs for an <see cref="INotifyPropertyChanged"/> entity: these are tracked without
/// snapshot diffing — the change handler flags each changed column in <c>_changedProperties</c> incrementally
/// rather than via DetectChanges. The partial-update path reads those flags, so only the notified column is
/// written and a concurrent write to an un-notified column survives.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NotifyPartialColumnUpdateTests
{
    [Table("NpuWidget")]
    private class NotifyWidget : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler? PropertyChanged;
        private void Set(ref int field, int value, string name)
        {
            if (field != value) { field = value; PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name)); }
        }
        private int _id, _a, _b, _c;
        public int Id { get => _id; set => Set(ref _id, value, nameof(Id)); }
        public int A { get => _a; set => Set(ref _a, value, nameof(A)); }
        public int B { get => _b; set => Set(ref _b, value, nameof(B)); }
        public int C { get => _c; set => Set(ref _c, value, nameof(C)); }
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<NotifyWidget>().HasKey(w => w.Id)
    }, ownsConnection: false);

    [Fact]
    public async Task Partial_update_of_a_notifying_entity_writes_only_the_notified_column()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NpuWidget (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, C INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        await using (var seed = Ctx(cn))
        {
            seed.Add(new NotifyWidget { Id = 1, A = 10, B = 20, C = 30 });
            await seed.SaveChangesAsync();
        }

        await using var ctx = Ctx(cn);
        var w = (await ctx.Query<NotifyWidget>().Where(x => x.Id == 1).ToListAsync()).Single();
        w.A = 11;   // raises PropertyChanged("A") -> flags only column A

        // Concurrent write to the un-notified column B must survive the partial update.
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = "UPDATE NpuWidget SET B = 99 WHERE Id = 1"; cmd.ExecuteNonQuery(); }

        await ctx.SaveChangesAsync();

        using var read = cn.CreateCommand();
        read.CommandText = "SELECT A, B, C FROM NpuWidget WHERE Id = 1";
        using var r = read.ExecuteReader(); r.Read();
        Assert.Equal(11, r.GetInt32(0));   // notified change applied
        Assert.Equal(99, r.GetInt32(1));   // concurrent change to the un-notified column SURVIVED
        Assert.Equal(30, r.GetInt32(2));
    }
}
