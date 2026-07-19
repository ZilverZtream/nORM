using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The generic (converter-aware) materializer path must FAIL LOUD when a value cannot be converted or
/// assigned, rather than silently leaving the property at its CLR default (silent-wrong / silent data loss).
/// The optimized materializer path already throws on a type mismatch; this pins consistency. Uses a
/// converter whose ConvertFromProvider throws to force the converter-aware path and a conversion failure.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MaterializerConversionFailLoudTests
{
    [Table("FlWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }

    private sealed class ThrowingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value;
        public override object? ConvertFromProvider(int value) => throw new InvalidCastException("simulated bad stored value");
    }

    [Fact]
    public void Materializer_throws_with_column_context_when_conversion_fails()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FlWidget (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL); INSERT INTO FlWidget VALUES (1, 5);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property<int>(w => w.Score).HasConversion(new ThrowingConverter());
            }
        }, ownsConnection: false);

        // Previously the failed conversion was swallowed and Score stayed 0 (silent-wrong); now it throws
        // with the column/property in the message.
        var ex = Assert.Throws<InvalidOperationException>(() => ctx.Query<Widget>().ToList());
        Assert.Contains("Score", ex.Message);
    }
}
