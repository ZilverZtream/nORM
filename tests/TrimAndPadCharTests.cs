using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for string Trim/Pad with an explicit char argument, previously broken:
/// <list type="bullet">
/// <item><c>TrimEnd('x')</c> etc. (the single-char overload added in .NET Core) fell through to the
/// generic route and emitted the method name — <c>no such function: TRIMEND</c>. Only the char[] overload
/// was handled.</item>
/// <item><c>PadRight(10, '.')</c> inlined the pad char as a bare unquoted token — <c>near "." syntax
/// error</c> — because the projection visitor's constant handler had no char case and fell to a raw
/// ToString.</item>
/// </list>
/// The char case in the projection constant handler also fixes any inlined char literal (e.g. concat with
/// a char). Each case runs the identical LINQ expression against nORM (SQLite) and LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class TrimAndPadCharTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("TpcRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static readonly string[] Names = { "xxAlphaxx", "__beta__", "Zulu", "9nine9", "**mango" };
    private static readonly Row[] Rows = Names.Select((n, i) => new Row { Id = i + 1, Name = n }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TpcRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        foreach (var r in Rows) cmd.CommandText += $"INSERT INTO TpcRow VALUES ({r.Id},'{r.Name}');";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    private static void Assert_<T>(Func<IQueryable<Row>, IEnumerable<T>> q)
    {
        var expected = q(Rows.AsQueryable()).ToList();
        using var ctx = Ctx();
        var actual = q(ctx.Query<Row>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    // Trim single-char overloads
    [Fact] public void TrimEnd_char() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.TrimEnd('x')));
    [Fact] public void TrimStart_char() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.TrimStart('x')));
    [Fact] public void Trim_char() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Trim('x')));
    [Fact] public void TrimEnd_digit() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.TrimEnd('9')));
    [Fact] public void TrimStart_char_length() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.TrimStart('_').Length));
    [Fact] public void TrimEnd_char_predicate() => Assert_(q => q.Where(r => r.Name.TrimEnd('x') == "xxAlpha").OrderBy(r => r.Id).Select(r => r.Id));

    [Fact]
    public void TrimEnd_closure_char()
    {
        char c = 'x';
        Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.TrimEnd(c)));
    }

    // char[] overload no-regression
    [Fact] public void TrimEnd_char_array() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.TrimEnd('x', '9')));
    [Fact] public void Trim_char_array() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.Trim('*', '_')));

    // Pad with a char
    [Fact] public void PadLeft_char() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.PadLeft(10, '.')));
    [Fact] public void PadRight_char() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.PadRight(10, '.')));
    [Fact] public void PadLeft_default_space() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name.PadLeft(10)));

    // Inlined char literal in a projection (the broader constant-handler fix)
    [Fact] public void Concat_with_char_literal() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name + '!'));
}
