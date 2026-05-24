using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Probes <c>string + int</c> auto-coercion in projection -- the C#
/// compiler lowers <c>p.First + p.Age</c> as
/// <c>string.Concat(p.First, (object)p.Age)</c> (boxing the int). The
/// translator must either:
///   * Cast the numeric operand to TEXT and concat via provider concat SQL
///     so projection returns e.g. "Ada42" -- best.
///   * Throw NormUnsupportedFeatureException pointing at an explicit
///     <c>p.Age.ToString()</c> rewrite -- acceptable.
///
/// Silent-wrongness shape: translator drops or mis-routes the boxed int,
/// returning just "Ada" / null / a numeric coercion artifact.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringPlusIntCoercionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PspiPerson (Id INTEGER PRIMARY KEY, First TEXT NOT NULL, Age INTEGER NOT NULL);
            INSERT INTO PspiPerson VALUES
                (1, 'Ada', 42),
                (2, 'Grace', 85),
                (3, 'Alan', 41);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PspiPerson>().HasKey(p => p.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_with_string_plus_int_concatenates_via_provider_concat()
    {
        // p.First + p.Age compiles to BinaryExpression(Add) with node.Right
        // type == object (boxed int). The 76f04af GetConcatSql fix triggers
        // because node.Left.Type == string, and SQLite's `||` operator
        // implicitly converts the integer column to its text representation
        // during concatenation -> "Ada42", "Grace85", "Alan41". Pin this
        // working behavior so a future fix that "tightens" the string-detect
        // to require BOTH sides string doesn't regress this common shape.
        var result = await _ctx.Query<PspiPerson>()
            .OrderBy(p => p.Id)
            .Select(p => p.First + p.Age)
            .ToListAsync();
        Assert.Equal(new[] { "Ada42", "Grace85", "Alan41" }, result.ToArray());
    }

    [Fact]
    public async Task Select_with_int_ToString_in_projection_concatenates_via_CAST_AS_TEXT()
    {
        // Companion of the implicit pin above. Originally documented the
        // throw as the current contract -- subsequently fixed by extending
        // TranslatabilityAnalyzer + SelectClauseVisitor with no-arg ToString()
        // -> provider CAST AS TEXT. Both implicit (p.First + p.Age) and
        // explicit (p.First + p.Age.ToString()) now produce identical
        // results.
        var result = await _ctx.Query<PspiPerson>()
            .OrderBy(p => p.Id)
            .Select(p => p.First + p.Age.ToString())
            .ToListAsync();
        Assert.Equal(new[] { "Ada42", "Grace85", "Alan41" }, result.ToArray());
    }

    [Table("PspiPerson")]
    public sealed class PspiPerson
    {
        [Key] public int Id { get; set; }
        public string First { get; set; } = string.Empty;
        public int Age { get; set; }
    }
}
