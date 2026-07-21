using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for string concatenation / interpolation of CULTURE-SAFE operands — integer
/// and string members, arithmetic results, and null members (C# concat treats null as empty). Integer
/// default formatting has no grouping/separator so it is culture-independent and matches nORM's invariant
/// lowering.
///
/// NOTE deliberately excluded: interpolating/concatenating a <c>decimal</c> or <c>double</c> via
/// <c>ToString()</c> diverges — nORM emits the INVARIANT representation (dot separator, provider decimal
/// scale) while C#'s parameterless <c>ToString()</c> uses the CURRENT CULTURE (e.g. a comma separator and
/// scale-preserving "2.50"). Matching arbitrary client culture in server SQL is infeasible (invariant is
/// the correct server choice); this is the same documented divergence class as string ORDER BY. Likewise,
/// interpolating a <c>bool</c> renders as 0/1 rather than "True"/"False" (a separate documented divergence).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class StringConcatInterpolationTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("SciRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int V { get; set; }
        public long L { get; set; }
        public int? NV { get; set; }
        public string Name { get; set; } = "";
    }

    private static readonly Row[] Seed = Enumerable.Range(1, 8).Select(i => new Row
    { Id = i, V = i * 3, L = i * 1000000L, NV = (i % 3 == 0) ? (int?)null : i * 7, Name = "n" + i }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SciRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, L INTEGER NOT NULL, NV INTEGER NULL, Name TEXT NOT NULL);";
        foreach (var r in Seed)
            cmd.CommandText += $"INSERT INTO SciRow VALUES ({r.Id},{r.V},{r.L},{(r.NV.HasValue ? r.NV.Value.ToString(CultureInfo.InvariantCulture) : "NULL")},'{r.Name}');";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    private static void Assert_<T>(Func<IQueryable<Row>, IEnumerable<T>> q)
    {
        var expected = q(Seed.AsQueryable()).ToList();
        using var ctx = Ctx();
        var actual = q(ctx.Query<Row>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public void String_plus_int_concat() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name + ":" + r.V));
    [Fact] public void Int_interpolation() => Assert_(q => q.OrderBy(r => r.Id).Select(r => $"{r.Id}-{r.V}"));
    [Fact] public void Long_interpolation() => Assert_(q => q.OrderBy(r => r.Id).Select(r => $"L={r.L}"));
    [Fact] public void Int_ToString() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.V.ToString()));
    [Fact] public void Int_ToString_plus_string() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Id.ToString() + r.Name));
    [Fact] public void Nullable_int_concat_null_as_empty() => Assert_(q => q.OrderBy(r => r.Id).Select(r => "nv:" + r.NV));
    [Fact] public void Nullable_int_interpolation() => Assert_(q => q.OrderBy(r => r.Id).Select(r => $"[{r.NV}]"));
    [Fact] public void Multi_part_interpolation() => Assert_(q => q.OrderBy(r => r.Id).Select(r => $"{r.Id}/{r.Name}/{r.V}"));
    [Fact] public void Concat_static_with_numbers() => Assert_(q => q.OrderBy(r => r.Id).Select(r => string.Concat(r.Id, r.Name, r.V)));
    [Fact] public void Arithmetic_then_concat() => Assert_(q => q.OrderBy(r => r.Id).Select(r => "sum=" + (r.V + r.Id)));
    [Fact] public void Concat_in_predicate() => Assert_(q => q.Where(r => (r.Name + "!") == "n3!").OrderBy(r => r.Id).Select(r => r.Id));
}
