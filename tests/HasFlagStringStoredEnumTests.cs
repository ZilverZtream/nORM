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
/// A [Flags] enum persisted via a value converter as its NAME string cannot be bit-tested server-side:
/// <c>('Read, Write' &amp; 2)</c> coerces the text to 0, so HasFlag silently returned NO rows. A bitwise flag
/// test over a name string is not reliably translatable (and EF can't do it either), so the query must fail
/// loud with an actionable message instead of silently matching nothing.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class HasFlagStringStoredEnumTests
{
    [Flags]
    private enum Perm { None = 0, Read = 1, Write = 2, Admin = 4 }

    private sealed class PermStringConverter : ValueConverter<Perm, string>
    {
        public override object? ConvertToProvider(Perm value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<Perm>(value);
    }

    [Table("HfRow")]
    private sealed class Row
    {
        [Key] public int Id { get; set; }
        public Perm Perms { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE HfRow (Id INTEGER PRIMARY KEY, Perms TEXT NOT NULL);" +
                "INSERT INTO HfRow (Id, Perms) VALUES (1, 'Read, Write'), (2, 'Read'), (3, 'Admin');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().Property(r => r.Perms).HasConversion(new PermStringConverter())
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public void HasFlag_on_string_stored_flags_enum_fails_loud_instead_of_matching_nothing()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Would silently return [] before; must throw an actionable error instead.
        var ex = Record.Exception(() =>
            ctx.Query<Row>().Where(r => r.Perms.HasFlag(Perm.Write)).Select(r => r.Id).ToList());

        Assert.NotNull(ex);
        Assert.Contains("integer", ex!.Message, StringComparison.OrdinalIgnoreCase);
    }
}
