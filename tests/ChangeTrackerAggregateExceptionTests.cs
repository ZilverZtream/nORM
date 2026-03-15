using System;
using System.Linq;
using System.Reflection;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>DetectChanges must throw AggregateException when a property getter fails.</summary>
public class ChangeTrackerAggregateExceptionTests
{
    private class FaultyEntity
    {
        public int Id { get; set; } = 1;
        public bool ShouldThrow { get; set; }
        private string _name = "value";
        // Has a setter so nORM includes it in the column mapping
        public string Name
        {
            get => ShouldThrow
                ? throw new InvalidOperationException("Getter failed")
                : _name;
            set => _name = value;
        }
    }

    [Fact]
    public void DetectAllChanges_ThrowsAggregateException_WhenPropertyGetterFails()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        // EagerChangeTracking ensures snapshot is taken immediately on Attach (before ShouldThrow=true)
        using var ctx = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { EagerChangeTracking = true });

        var entity = new FaultyEntity { Id = 1, ShouldThrow = false };
        ctx.Attach(entity); // Snapshot taken now: Name="value"

        entity.ShouldThrow = true; // Next access to Name will throw

        var detectAllChanges = typeof(ChangeTracker).GetMethod("DetectAllChanges",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

        var ex = Assert.Throws<TargetInvocationException>(() =>
            detectAllChanges.Invoke(ctx.ChangeTracker, null));

        Assert.IsType<AggregateException>(ex.InnerException);
        var agg = (AggregateException)ex.InnerException!;
        Assert.NotEmpty(agg.InnerExceptions);
        Assert.IsType<InvalidOperationException>(agg.InnerExceptions[0]);
        Assert.Equal("Getter failed", agg.InnerExceptions[0].Message);
    }

    [Fact]
    public void DetectChanges_ThrowsAggregateException_WhenPropertyGetterFails()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { EagerChangeTracking = true });

        var entity = new FaultyEntity { Id = 1, ShouldThrow = false };
        ctx.Attach(entity); // Snapshot taken now: Name="value"

        entity.ShouldThrow = true;

        // MarkDirty moves the entry to the dirty set
        var entry = ctx.ChangeTracker.Entries.Single();
        var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        markDirty.Invoke(ctx.ChangeTracker, new object[] { entry });

        var detectChanges = typeof(ChangeTracker).GetMethod("DetectChanges",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

        var ex = Assert.Throws<TargetInvocationException>(() =>
            detectChanges.Invoke(ctx.ChangeTracker, null));

        Assert.IsType<AggregateException>(ex.InnerException);
        var agg = (AggregateException)ex.InnerException!;
        Assert.NotEmpty(agg.InnerExceptions);
        Assert.IsType<InvalidOperationException>(agg.InnerExceptions[0]);
    }
}
