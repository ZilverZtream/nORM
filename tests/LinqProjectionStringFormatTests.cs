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
/// Probe + strict pin for <c>string.Format("...", arg0, arg1, ...)</c> in
/// projection. .NET 6+ overloads with up to 3 positional args + the params
/// variant. Translates to a composed <c>provider.GetConcatSql(...)</c> by
/// splitting the format string on '{N}' placeholders and interleaving the
/// arg SQL fragments with quoted literals. Only positional placeholders
/// without format specifiers are supported (e.g. <c>{0}</c>); a placeholder
/// with a format like <c>{0:N2}</c> falls through to the existing
/// throw-or-correct behavior since no portable SQL implements .NET format
/// strings.
///
/// Silent-wrongness shapes:
///   * Placeholder swap -> '{0}-{1}' becomes '{1}-{0}' if visit order is
///     wrong; visibly broken in rendered output.
///   * Literal text dropped -> 'Id={0}' becomes just the int value.
///   * Numeric / null arg coerced to empty string -> values missing where
///     .NET would substitute "0" / "".
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringFormatTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsfItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL);
            INSERT INTO PsfItem VALUES
                (1, 'Alpha', 10),
                (2, 'Beta',  20),
                (3, 'Gamma', 30);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsfItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Format_with_single_placeholder_interpolates_column()
    {
        var result = await _ctx.Query<PsfItem>()
            .OrderBy(p => p.Id)
            .Select(p => string.Format("Name={0}", p.Name))
            .ToListAsync();
        Assert.Equal(new[] { "Name=Alpha", "Name=Beta", "Name=Gamma" }, result.ToArray());
    }

    [Fact]
    public async Task Select_string_Format_with_two_placeholders_interleaves_both_columns()
    {
        var result = await _ctx.Query<PsfItem>()
            .OrderBy(p => p.Id)
            .Select(p => string.Format("{0}/{1}", p.Name, p.Score))
            .ToListAsync();
        Assert.Equal(new[] { "Alpha/10", "Beta/20", "Gamma/30" }, result.ToArray());
    }

    [Fact]
    public async Task Select_string_Format_three_placeholders_with_literal_prefix_and_suffix()
    {
        var result = await _ctx.Query<PsfItem>()
            .OrderBy(p => p.Id)
            .Select(p => string.Format("[{0}] {1} = {2}", p.Id, p.Name, p.Score))
            .ToListAsync();
        Assert.Equal(new[] { "[1] Alpha = 10", "[2] Beta = 20", "[3] Gamma = 30" }, result.ToArray());
    }

    [Table("PsfItem")]
    public sealed class PsfItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Score { get; set; }
    }
}
