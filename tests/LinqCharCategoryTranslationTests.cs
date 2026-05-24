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
/// Pins server-side translation of <c>char.IsDigit</c>, <c>char.IsLetter</c>,
/// and <c>char.IsWhiteSpace</c> when applied to an indexed string char (the
/// shape unlocked by 86d7ac1). Each predicate lowers to a portable BETWEEN /
/// IN range comparison wrapped around the existing SUBSTR(col, i+1, 1) emit:
/// <list type="bullet">
///   <item>IsDigit: <c>x BETWEEN '0' AND '9'</c></item>
///   <item>IsLetter: <c>(x BETWEEN 'A' AND 'Z' OR x BETWEEN 'a' AND 'z')</c></item>
///   <item>IsWhiteSpace: <c>x IN (' ', CHAR(9), CHAR(10), CHAR(13))</c></item>
/// </list>
/// (ASCII-only — matches the most common use case of validating user input.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCharCategoryTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CcRow (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            -- Codes that start with: digit / uppercase / lowercase / whitespace / symbol
            INSERT INTO CcRow VALUES
                (1, '7alpha'),
                (2, 'Banana'),
                (3, 'cherry'),
                (4, ' lead-space'),
                (5, '!warn');
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
    public async Task IsDigit_on_indexed_first_char_filters_digit_prefixed_rows()
    {
        // Only row 1 ('7alpha') starts with a digit.
        var rows = await _ctx.Query<CcRow>().Where(r => char.IsDigit(r.Code[0])).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
    }

    [Fact]
    public async Task IsLetter_on_indexed_first_char_filters_letter_prefixed_rows()
    {
        // Rows 2 ('Banana') and 3 ('cherry') start with a letter (Banana=upper, cherry=lower).
        var rows = (await _ctx.Query<CcRow>().Where(r => char.IsLetter(r.Code[0])).ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2, rows[0].Id);
        Assert.Equal(3, rows[1].Id);
    }

    [Fact]
    public async Task IsWhiteSpace_on_indexed_first_char_filters_space_prefixed_rows()
    {
        // Only row 4 (' lead-space') starts with whitespace.
        var rows = await _ctx.Query<CcRow>().Where(r => char.IsWhiteSpace(r.Code[0])).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(4, rows[0].Id);
    }

    [Table("CcRow")]
    public sealed class CcRow
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
