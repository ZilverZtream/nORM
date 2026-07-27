using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The parameterless Trim()/TrimStart()/TrimEnd() and IsNullOrWhiteSpace strip ALL Unicode whitespace in C#,
/// but SQLite's one-argument TRIM/LTRIM/RTRIM remove only U+0020 (ASCII space). A predicate like
/// <c>d.Name.Trim() == "Acme"</c> therefore silently dropped rows whose value had a trailing tab/newline. The
/// SQLite provider must pass the full C# whitespace set to TRIM so the result matches LINQ-to-Objects.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SqliteTrimUnicodeWhitespaceTests
{
    [Table("TrimDoc")]
    private sealed class Doc
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Ids: 1 trailing tab, 2 leading newline, 3 trailing NBSP, 4 clean, 5 all-whitespace, 6 empty.
            cmd.CommandText =
                "CREATE TABLE TrimDoc (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "INSERT INTO TrimDoc (Id, Name) VALUES " +
                "(1, 'Acme' || char(9)), (2, char(10) || 'Acme'), (3, 'Acme' || char(160)), " +
                "(4, 'Acme'), (5, char(9) || char(10) || ' '), (6, '');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Trim_matches_dotnet_over_unicode_whitespace()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var ids = ctx.Query<Doc>().Where(d => d.Name.Trim() == "Acme").Select(d => d.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 2, 3, 4 }, ids);   // BUG: only { 4 } — tab/newline/NBSP not trimmed
    }

    [Fact]
    public void TrimEnd_matches_dotnet_over_unicode_whitespace()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var ids = ctx.Query<Doc>().Where(d => d.Name.TrimEnd() == "Acme").Select(d => d.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 3, 4 }, ids);       // trailing whitespace stripped; id 2 (leading NL) stays "\nAcme"
    }

    [Fact]
    public void IsNullOrWhiteSpace_matches_dotnet_over_unicode_whitespace()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var ids = ctx.Query<Doc>().Where(d => string.IsNullOrWhiteSpace(d.Name)).Select(d => d.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 5, 6 }, ids);          // BUG: only { 6 } — the all-whitespace row 5 missed
    }
}
