using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Property fuzz for decimal predicates over <b>non-canonical</b> decimal TEXT storage on SQLite.
///
/// <para>Why this fuzzer exists — the harness lesson. The general LINQ-parity fuzzer seeds every row
/// through nORM's own write path, which canonicalizes a decimal to a single scale ("24.50m" is stored
/// as <c>"24.5"</c>). A raw <c>col = @p</c> fast-path comparison therefore never diverges from the
/// numeric answer on nORM-written data, so a whole class of storage-format bugs — equality that is
/// scale-sensitive, ranges/orderings that are lexical — is structurally invisible to a fuzzer that only
/// ever sees canonical text. The class only bites decimals written by <i>other</i> clients (raw SQL,
/// another ORM, a migration) with a different trailing-zero scale — exactly the live-DB scenario nORM
/// supports. This fuzzer closes that blind spot: it seeds via raw SQL with a random trailing-zero scale
/// and checks every read fast path (filtered-ordered list, Count, ordering) against a numeric
/// LINQ-to-objects oracle.</para>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DecimalStorageFormatFuzzTests
{
    [Table("FuzzDec_Test")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public decimal Amount { get; set; }
        public bool Flag { get; set; }
    }

    private static readonly decimal[] Pool =
    {
        0m, 0.5m, 1m, 1.25m, 2m, 2.5m, 9.9m, 10m, 24.5m, 79.99m, 100m, 100.5m, 429m, -3.25m, -10m
    };

    private static readonly ExpressionType[] Ops =
    {
        ExpressionType.Equal, ExpressionType.LessThan, ExpressionType.LessThanOrEqual,
        ExpressionType.GreaterThan, ExpressionType.GreaterThanOrEqual
    };

    // Render a decimal as TEXT with `extra` trailing fraction zeros beyond its natural scale, so the
    // stored text is numerically equal but lexically distinct from the canonical form other clients would
    // never guarantee (e.g. 24.5m with extra=3 -> "24.5000", 100m with extra=2 -> "100.00").
    private static string NonCanonicalText(decimal v, int extra)
    {
        var text = v.ToString(CultureInfo.InvariantCulture);
        if (extra == 0)
            return text;
        if (!text.Contains('.'))
            text += ".";
        return text + new string('0', extra);
    }

    private static Expression<Func<Row, bool>> BuildPredicate(ExpressionType op, decimal value)
    {
        var p = Expression.Parameter(typeof(Row), "r");
        var body = Expression.MakeBinary(op, Expression.Property(p, nameof(Row.Amount)), Expression.Constant(value));
        return Expression.Lambda<Func<Row, bool>>(body, p);
    }

    [Theory]
    [InlineData(6000, 24)]
    [InlineData(6001, 24)]
    [InlineData(6002, 24)]
    [InlineData(6003, 24)]
    [InlineData(6004, 24)]
    [InlineData(6005, 24)]
    [InlineData(6006, 24)]
    [InlineData(6007, 24)]
    public async Task Decimal_predicates_over_noncanonical_storage_match_numeric_oracle(int seed, int cases)
    {
        var rng = new Random(seed);

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE FuzzDec_Test (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL, Flag INTEGER NOT NULL);";
            c.ExecuteNonQuery();
        }

        var oracle = new List<Row>();
        var rowCount = 20 + rng.Next(12);
        for (var id = 1; id <= rowCount; id++)
        {
            var v = Pool[rng.Next(Pool.Length)];
            var flag = rng.Next(2) == 0;
            var text = NonCanonicalText(v, rng.Next(0, 4));
            using (var c = cn.CreateCommand())
            {
                c.CommandText = "INSERT INTO FuzzDec_Test (Id, Amount, Flag) VALUES ($id, $a, $f);";
                c.Parameters.AddWithValue("$id", id);
                c.Parameters.AddWithValue("$a", text);            // raw TEXT, non-canonical scale
                c.Parameters.AddWithValue("$f", flag ? 1 : 0);
                c.ExecuteNonQuery();
            }
            oracle.Add(new Row { Id = id, Amount = v, Flag = flag });
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        for (var i = 0; i < cases; i++)
        {
            var op = Ops[rng.Next(Ops.Length)];
            var value = Pool[rng.Next(Pool.Length)];
            var predicate = BuildPredicate(op, value);
            var compiled = predicate.Compile();
            var expectedIds = oracle.Where(compiled).Select(r => r.Id).OrderBy(x => x).ToList();

            // Filtered-ordered list fast path (async -> hits the fast path when engaged).
            var listed = await ctx.Query<Row>().Where(predicate).OrderBy(r => r.Id).ToListAsync();
            Assert.Equal(expectedIds, listed.Select(r => r.Id).OrderBy(x => x).ToList());

            // Count fast path (== hits the direct-count path; ranges defer — both must be numeric).
            var counted = await ctx.Query<Row>().CountAsync(predicate);
            Assert.Equal(expectedIds.Count, counted);
        }

        // Ordering fast path: OrderBy on the decimal column must be numerically non-decreasing even
        // though the stored TEXT sorts lexically ("100.00" < "24.5000" as strings).
        var asc = await ctx.Query<Row>().Where(r => r.Id > 0).OrderBy(r => r.Amount).ToListAsync();
        for (var j = 1; j < asc.Count; j++)
            Assert.True(asc[j - 1].Amount <= asc[j].Amount, $"seed {seed}: ORDER BY Amount not numeric at index {j}");
    }
}
