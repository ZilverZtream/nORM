using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A top-level scalar Max/Min over an int-stored enum column must return the enum value. The scalar-result
/// converter had per-type fast branches (int/long/DateTime/Guid/TimeSpan/…) but no enum branch, so an enum
/// TResult fell to Convert.ChangeType(&lt;Int64&gt;, enumType) which throws InvalidCastException. The materializer
/// path (grouped aggregates, correlated subqueries) already round-trips enums; only the top-level scalar
/// aggregate / projection crashed.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class EnumScalarAggregateTests
{
    public enum Status { Pending = 0, Active = 1, Closed = 2, Zed = 3 }

    [Table("EsaRow")]
    public sealed class Row { [Key] public int Id { get; set; } public Status State { get; set; } }

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE EsaRow (Id INTEGER PRIMARY KEY, State INTEGER NOT NULL);" +
                          "INSERT INTO EsaRow VALUES (1,0),(2,1),(3,2),(4,1),(5,3);";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact] public void Max_over_int_enum_returns_enum() { using var c = Ctx(); Assert.Equal(Status.Zed, c.Query<Row>().Max(r => r.State)); }
    [Fact] public void Min_over_int_enum_returns_enum() { using var c = Ctx(); Assert.Equal(Status.Pending, c.Query<Row>().Min(r => r.State)); }
    [Fact] public void Select_then_Max_over_int_enum() { using var c = Ctx(); Assert.Equal(Status.Zed, c.Query<Row>().Select(r => r.State).Max()); }
    [Fact] public async Task MaxAsync_over_int_enum() { using var c = Ctx(); Assert.Equal(Status.Zed, await c.Query<Row>().MaxAsync(r => r.State)); }
    [Fact] public void Max_over_nullable_int_enum() { using var c = Ctx(); Assert.Equal((Status?)Status.Zed, c.Query<Row>().Max(r => (Status?)r.State)); }
}
