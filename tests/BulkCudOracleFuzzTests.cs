using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Set-based write fuzzer: seeded ExecuteUpdateAsync / ExecuteDeleteAsync shapes
/// (generated predicates with closure captures, literal and computed setters,
/// multi-setter chains) interleave with SaveChanges inserts, verified against an
/// in-memory committed model. Every predicate/setter pair is applied to BOTH the
/// database and the model: a mistranslated WHERE updates or deletes the wrong
/// rows, and a mistranslated setter corrupts values — silently. Each operation
/// uses a fresh context (set-based writes bypass change tracking by contract),
/// and the affected-row count must equal the model's match count.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class BulkCudOracleFuzzTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("BulkCud_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public string Name { get; set; } = "";
        public bool Flag { get; set; }
    }

    [Theory]
    [InlineData(20260714)]
    [InlineData(42)]
    [InlineData(987654)]
    [InlineData(31337)]
    public async Task Set_based_writes_match_the_committed_model(int seed)
    {
        var dbName = $"bulkcud_{seed}_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BulkCud_Test (Id INTEGER PRIMARY KEY, IntVal INTEGER NOT NULL, Name TEXT NOT NULL, Flag INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        DbContext OpenCtx()
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return new DbContext(cn, new SqliteProvider());
        }

        await RunBulkCudMachineAsync(OpenCtx, seed, steps: 150);
    }

    /// <summary>Set-based write machine body, shared with the live-provider variant.</summary>
    internal static async Task RunBulkCudMachineAsync(Func<DbContext> openCtx, int seed, int steps)
    {
        var rng = new Random(seed);
        var model = new Dictionary<int, Row>();
        var nextKey = 1;
        var trace = new List<string>();
        string Tail() => "\nops:\n" + string.Join("\n", trace.TakeLast(40));

        async Task VerifyAsync(string context)
        {
            using var verifyCtx = openCtx();
            var rows = (await verifyCtx.Query<Row>().ToListAsync()).OrderBy(r => r.Id).ToList();
            var expected = model.Values.OrderBy(r => r.Id).ToList();
            Assert.True(rows.Count == expected.Count,
                $"row count mismatch {context}: db={rows.Count} model={expected.Count}\n" +
                $"db: [{string.Join(",", rows.Select(r => r.Id))}]\nmodel: [{string.Join(",", expected.Select(r => r.Id))}]{Tail()}");
            for (var i = 0; i < rows.Count; i++)
            {
                Assert.True(rows[i].Id == expected[i].Id
                        && rows[i].IntVal == expected[i].IntVal
                        && rows[i].Name == expected[i].Name
                        && rows[i].Flag == expected[i].Flag,
                    $"row mismatch {context} at Id={expected[i].Id}: " +
                    $"db=({rows[i].Id},{rows[i].IntVal},\"{rows[i].Name}\",{rows[i].Flag}) " +
                    $"model=({expected[i].Id},{expected[i].IntVal},\"{expected[i].Name}\",{expected[i].Flag}){Tail()}");
            }
        }

        (Expression<Func<Row, bool>> Db, Func<Row, bool> Model, string Desc) GeneratePredicate()
        {
            var k = rng.Next(-60, 60);
            var m = rng.Next(2, 6);
            var r = rng.Next(0, m);
            var digit = (char)('0' + rng.Next(10));
            var needle = digit.ToString();
            Expression<Func<Row, bool>> pred = rng.Next(7) switch
            {
                0 => x => x.IntVal > k,
                1 => x => x.IntVal <= k && x.Flag,
                2 => x => x.Id % m == r,
                3 => x => x.Name.Contains(needle),
                4 => x => !x.Flag || x.IntVal * 2 < k,
                5 => x => x.IntVal >= k && x.IntVal < k + 25,
                _ => x => x.Flag == (x.Id % 2 == 0),
            };
            return (pred, pred.Compile(), $"k={k} m={m} r={r} needle={needle}: {pred.Body}");
        }

        async Task SeedRowsAsync(int count)
        {
            using var ctx = openCtx();
            for (var j = 0; j < count; j++)
            {
                var row = new Row
                {
                    Id = nextKey++,
                    IntVal = rng.Next(-80, 80),
                    Name = $"n{rng.Next(1000)}",
                    Flag = rng.Next(2) == 0,
                };
                ctx.Add(row);
                model[row.Id] = new Row { Id = row.Id, IntVal = row.IntVal, Name = row.Name, Flag = row.Flag };
            }
            await ctx.SaveChangesAsync();
        }

        await SeedRowsAsync(12);
        await VerifyAsync($"seed={seed} (initial)");

        for (var step = 0; step < steps; step++)
        {
            switch (rng.Next(10))
            {
                case 0 or 1 or 2: // bulk update, one of several setter shapes
                {
                    var (dbPred, modelPred, desc) = GeneratePredicate();
                    var k = rng.Next(-20, 20);
                    var suffix = rng.Next(10).ToString();
                    var flagVal = rng.Next(2) == 0;
                    var shape = rng.Next(4);
                    int affected;
                    try
                    {
                        using var ctx = openCtx();
                        var target = ctx.Query<Row>().Where(dbPred);
                        affected = shape switch
                        {
                            0 => await target.ExecuteUpdateAsync(s => s.SetProperty(x => x.IntVal, k)),
                            1 => await target.ExecuteUpdateAsync(s => s.SetProperty(x => x.IntVal, x => x.IntVal + k)),
                            2 => await target.ExecuteUpdateAsync(s => s.SetProperty(x => x.Name, x => x.Name + suffix)),
                            _ => await target.ExecuteUpdateAsync(s => s
                                .SetProperty(x => x.IntVal, x => x.IntVal * 2 - x.Id)
                                .SetProperty(x => x.Flag, flagVal)),
                        };
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                    {
                        throw new Xunit.Sdk.XunitException(
                            $"bulk update threw {ex.GetType().Name} seed={seed} step={step} shape={shape} pred[{desc}]: {ex.Message}{Tail()}", ex);
                    }
                    var matched = 0;
                    foreach (var row in model.Values.Where(modelPred).ToList())
                    {
                        matched++;
                        switch (shape)
                        {
                            case 0: row.IntVal = k; break;
                            case 1: row.IntVal += k; break;
                            case 2: row.Name += suffix; break;
                            default: row.IntVal = row.IntVal * 2 - row.Id; row.Flag = flagVal; break;
                        }
                    }
                    trace.Add($"{step}: update shape={shape} k={k} suffix={suffix} flag={flagVal} pred[{desc}] matched={matched} affected={affected}");
                    Assert.True(affected == matched,
                        $"update affected mismatch seed={seed} step={step}: db={affected} model={matched} pred[{desc}]{Tail()}");
                    await VerifyAsync($"seed={seed} step={step} (after update)");
                    break;
                }
                case 3 or 4: // bulk delete
                {
                    var (dbPred, modelPred, desc) = GeneratePredicate();
                    int affected;
                    try
                    {
                        using var ctx = openCtx();
                        affected = await ctx.Query<Row>().Where(dbPred).ExecuteDeleteAsync();
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                    {
                        throw new Xunit.Sdk.XunitException(
                            $"bulk delete threw {ex.GetType().Name} seed={seed} step={step} pred[{desc}]: {ex.Message}{Tail()}", ex);
                    }
                    var matchedKeys = model.Values.Where(modelPred).Select(r => r.Id).ToList();
                    foreach (var key in matchedKeys)
                        model.Remove(key);
                    trace.Add($"{step}: delete pred[{desc}] matched={matchedKeys.Count} affected={affected}");
                    Assert.True(affected == matchedKeys.Count,
                        $"delete affected mismatch seed={seed} step={step}: db={affected} model={matchedKeys.Count} pred[{desc}]{Tail()}");
                    await VerifyAsync($"seed={seed} step={step} (after delete)");
                    break;
                }
                case 5 or 6: // replenish through the tracked write path
                {
                    var count = 1 + rng.Next(4);
                    await SeedRowsAsync(count);
                    trace.Add($"{step}: add {count} rows");
                    await VerifyAsync($"seed={seed} step={step} (after adds)");
                    break;
                }
                case 7: // key-subset delete via a local Contains list (parameter expansion)
                {
                    var victims = model.Keys.Where(_ => rng.Next(4) == 0).ToList();
                    int affected;
                    using (var ctx = openCtx())
                    {
                        affected = await ctx.Query<Row>().Where(x => victims.Contains(x.Id)).ExecuteDeleteAsync();
                    }
                    foreach (var key in victims)
                        model.Remove(key);
                    trace.Add($"{step}: delete keys [{string.Join(",", victims)}] affected={affected}");
                    Assert.True(affected == victims.Count,
                        $"subset delete affected mismatch seed={seed} step={step}: db={affected} model={victims.Count}{Tail()}");
                    await VerifyAsync($"seed={seed} step={step} (after subset delete)");
                    break;
                }
                default: // no-match update: a predicate selecting nothing must touch nothing
                {
                    int affected;
                    using (var ctx = openCtx())
                    {
                        affected = await ctx.Query<Row>().Where(x => x.Id < 0)
                            .ExecuteUpdateAsync(s => s.SetProperty(x => x.IntVal, 0));
                    }
                    trace.Add($"{step}: no-match update affected={affected}");
                    Assert.True(affected == 0,
                        $"no-match update affected {affected} rows seed={seed} step={step}{Tail()}");
                    await VerifyAsync($"seed={seed} step={step} (after no-match update)");
                    break;
                }
            }
        }
    }
}
