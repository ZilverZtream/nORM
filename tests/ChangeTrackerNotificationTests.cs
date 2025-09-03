using System.ComponentModel;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class ChangeTrackerNotificationTests
{
    private class NotifyingEntity : INotifyPropertyChanged
    {
        private string _name = string.Empty;
        public int Id { get; set; }
        public string Name
        {
            get => _name;
            set
            {
                if (_name != value)
                {
                    _name = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Name)));
                }
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;
    }

    [Fact]
    public void PropertyChanged_sets_state_modified()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var entity = new NotifyingEntity { Name = "A" };
        ctx.Attach(entity);
        var entry = ctx.ChangeTracker.Entries.Single();
        Assert.Equal(EntityState.Unchanged, entry.State);

        entity.Name = "B";
        Assert.Equal(EntityState.Modified, entry.State);
    }

    [Fact]
    public void AcceptChanges_resets_notification()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var entity = new NotifyingEntity { Name = "A" };
        ctx.Attach(entity);
        var entry = ctx.ChangeTracker.Entries.Single();

        entity.Name = "B";
        Assert.Equal(EntityState.Modified, entry.State);

        entry.AcceptChanges();
        Assert.Equal(EntityState.Unchanged, entry.State);

        entity.Name = "C";
        Assert.Equal(EntityState.Modified, entry.State);
    }
}
