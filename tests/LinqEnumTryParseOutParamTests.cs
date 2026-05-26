using System;
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
/// Pins <c>Enum.TryParse&lt;T&gt;(col, out _)</c> as a WHERE predicate.
/// Detects rows whose text column contains a name that maps to a member
/// of <typeparamref name="T"/>. Lowers to a column-IN-enum-names check
/// (server-side), so callers can filter out malformed rows without
/// downloading them.
///
/// The richer two-step usage
/// <c>TryParse(col, out var s) &amp;&amp; s == X</c> is equivalent to
/// <c>col == nameof(X)</c>; nORM already translates that. This probe
/// covers the "is parseable" check shape that has no obvious rewrite.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqEnumTryParseOutParamTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EtpRow (Id INTEGER PRIMARY KEY, StatusText TEXT NOT NULL);
            INSERT INTO EtpRow VALUES
                (1, 'Active'),
                (2, 'Inactive'),
                (3, 'NotAStatus'),
                (4, 'Pending');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Enum_TryParse_predicate_filters_to_rows_whose_text_is_a_valid_enum_name()
    {
        // Expression trees can't contain `out _` — must use a named out var.
        // The variable is unused but its presence in the tree drives the
        // ByRef-arg detection path in the translator.
        Status sink;
        var rows = await _ctx.Query<EtpRow>()
            .Where(r => Enum.TryParse<Status>(r.StatusText, out sink))
            .OrderBy(r => r.Id)
            .ToListAsync();
        // Active, Inactive, Pending — defined; NotAStatus is not.
        Assert.Equal(new[] { 1, 2, 4 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Enum_TryParse_with_specific_value_compare_still_works_via_string_equals()
    {
        // The richer "TryParse(col, out s) && s == X" shape isn't pattern-
        // matched by the TryParse translator (the out-var carries state across
        // the && which would need data-flow rewriting). Equivalent and
        // already-translatable: just compare the column string directly.
        var rows = await _ctx.Query<EtpRow>()
            .Where(r => r.StatusText == nameof(Status.Active))
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1 }, rows.Select(r => r.Id).ToArray());
    }

    public enum Status { Active, Inactive, Pending }

    [Table("EtpRow")]
    public sealed class EtpRow
    {
        [Key] public int Id { get; set; }
        public string StatusText { get; set; } = "";
    }
}
