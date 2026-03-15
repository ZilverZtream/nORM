using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>Ensures SaveChanges throws when modifying/deleting a keyless entity.</summary>
public class NoKeyEntityTests
{
    [Table("NoKeyTable")]
    private class NoKeyEntity
    {
        // No Id, no [Key] — nORM will detect zero key columns
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    [Fact]
    public async Task SaveChanges_ThrowsNormConfigurationException_ForKeylessModifiedEntity()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var entity = new NoKeyEntity { Name = "Test", Value = 42 };
        ctx.Update(entity);

        await Assert.ThrowsAsync<NormConfigurationException>(() => ctx.SaveChangesAsync(detectChanges: false));
    }

    [Fact]
    public async Task SaveChanges_ThrowsNormConfigurationException_ForKeylessDeletedEntity()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var entity = new NoKeyEntity { Name = "Test", Value = 42 };
        ctx.Remove(entity);

        await Assert.ThrowsAsync<NormConfigurationException>(() => ctx.SaveChangesAsync(detectChanges: false));
    }
}
