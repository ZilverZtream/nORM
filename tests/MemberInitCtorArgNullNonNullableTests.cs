using System;
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
/// A `new Dto(x.Id) { Age = x.Age }` projection (constructor arg + member init) must treat a NULL read for a
/// non-nullable value-type member the same way the entity read and the plain `new Dto { Age = x.Age }`
/// projection do: fail loud, not silently substitute default(int)=0. The MemberInit-with-ctor-args
/// materializer unconditionally wrapped every column in Condition(isDbNull, default, read), so a NULL in a
/// non-nullable member silently became 0 — a silent-wrong divergence from every sibling path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MemberInitCtorArgNullNonNullableTests
{
    [Table("MicnSrc")]
    private class Src
    {
        [Key] public int Id { get; set; }
        public int Age { get; set; }    // NON-nullable model property; the DB column is nullable with a NULL row
    }

    private class Dto
    {
        public Dto(int id) => Id = id;
        public int Id { get; }
        public int Age { get; set; }     // NON-nullable member — a NULL read must not silently become 0
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE MicnSrc (Id INTEGER PRIMARY KEY, Age INTEGER NULL);" +
                "INSERT INTO MicnSrc VALUES (1, NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void MemberInit_ctor_arg_projection_does_not_silently_default_null_nonnullable_member()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Age is NULL in the DB; the non-nullable Dto.Age member must not silently read back 0.
        // The entity read and `new Dto { Age = x.Age }` both throw on this, so this must too.
        Assert.ThrowsAny<Exception>(() =>
            ctx.Query<Src>().Select(x => new Dto(x.Id) { Age = x.Age }).ToList());
    }

    [Fact]
    public void MemberInit_ctor_arg_projection_reads_present_values()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE MicnSrc (Id INTEGER PRIMARY KEY, Age INTEGER NULL);" +
                "INSERT INTO MicnSrc VALUES (2, 42);";
            cmd.ExecuteNonQuery();
        }
        using var _cn = cn;
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions());

        // A present (non-null) value must still project correctly.
        var dto = ctx.Query<Src>().Select(x => new Dto(x.Id) { Age = x.Age }).Single();
        Assert.Equal(2, dto.Id);
        Assert.Equal(42, dto.Age);
    }
}
