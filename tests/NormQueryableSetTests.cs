using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies the EF Core-style <c>Set&lt;T&gt;()</c> entry point behaves as an alias for
/// <c>Query&lt;T&gt;()</c> - same rows, same composability.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public sealed class NormQueryableSetTests
{
    [Table("SetProbe")]
    private sealed class SetProbe
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static SqliteConnection NewSeededConnection()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText =
            "CREATE TABLE SetProbe (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "INSERT INTO SetProbe (Name) VALUES ('Ada'), ('Grace');";
        command.ExecuteNonQuery();
        return connection;
    }

    [Fact]
    public async Task Set_returns_the_same_rows_as_Query()
    {
        var connection = NewSeededConnection();
        using var context = new DbContext(connection, new SqliteProvider());

        var viaSet = await context.Set<SetProbe>().OrderBy(p => p.Id).ToListAsync();
        var viaQuery = await context.Query<SetProbe>().OrderBy(p => p.Id).ToListAsync();

        Assert.Equal(2, viaSet.Count);
        Assert.Equal(viaQuery.Select(p => p.Name), viaSet.Select(p => p.Name));
        Assert.Equal(new[] { "Ada", "Grace" }, viaSet.Select(p => p.Name).ToArray());
    }

    [Fact]
    public async Task Set_composes_with_where_like_Query()
    {
        var connection = NewSeededConnection();
        using var context = new DbContext(connection, new SqliteProvider());

        var match = await context.Set<SetProbe>().Where(p => p.Name == "Grace").ToListAsync();

        Assert.Single(match);
        Assert.Equal("Grace", match[0].Name);
    }
}
