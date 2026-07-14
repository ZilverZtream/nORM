using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-driven fuzz over one-to-many collection writes: each round loads every parent with
/// its children (so the collections are snapshotted), then randomly removes children (sever →
/// FK null) and moves children between parents (reparent → FK repointed), and after SaveChanges
/// verifies every child's FK in the database matches an in-memory oracle. Guards the severance
/// and reparent logic added this cycle against accumulated multi-round state.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CollectionRemovalReparentFuzzTests
{
    [Table("CrrDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Emp> Employees { get; set; } = new();
    }

    [Table("CrrEmp")]
    public class Emp
    {
        [Key] public int Id { get; set; }
        public int? DeptId { get; set; }
        public string Name { get; set; } = "";
    }

    private const int DeptCount = 4;
    private const int EmpCount = 14;

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup(int seed)
    {
        var rng = new Random(seed);
        var keeper = new SqliteConnection($"Data Source=file:crr_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CrrDept (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CrrEmp (Id INTEGER PRIMARY KEY, DeptId INTEGER NULL, Name TEXT NOT NULL);
                """;
            cmd.ExecuteNonQuery();
            for (var d = 1; d <= DeptCount; d++)
            {
                using var ins = keeper.CreateCommand();
                ins.CommandText = $"INSERT INTO CrrDept VALUES ({d}, 'D{d}')";
                ins.ExecuteNonQuery();
            }
            for (var e = 1; e <= EmpCount; e++)
            {
                var dept = rng.Next(0, DeptCount + 1); // 0 => null
                using var ins = keeper.CreateCommand();
                ins.CommandText = $"INSERT INTO CrrEmp VALUES ({e}, {(dept == 0 ? "NULL" : dept.ToString())}, 'E{e}')";
                ins.ExecuteNonQuery();
            }
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    mb.Entity<Dept>().HasKey(d => d.Id);
                    mb.Entity<Emp>().HasKey(e => e.Id);
                    mb.Entity<Dept>().HasMany(d => d.Employees).WithOne().HasForeignKey(e => e.DeptId!, d => d.Id);
                }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static Dictionary<int, int?> ReadOracleFromDb(SqliteConnection k)
    {
        var map = new Dictionary<int, int?>();
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT Id, DeptId FROM CrrEmp";
        using var r = cmd.ExecuteReader();
        while (r.Read())
            map[r.GetInt32(0)] = r.IsDBNull(1) ? (int?)null : r.GetInt32(1);
        return map;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(42)]
    [InlineData(20260714)]
    public async Task Random_collection_removals_and_reparents_match_the_oracle(int seed)
    {
        var (keeper, make) = Setup(seed);
        using var _ = keeper;
        var rng = new Random(seed ^ 0x5f3759df);
        var oracle = ReadOracleFromDb(keeper);

        for (var round = 0; round < 10; round++)
        {
            await using var ctx = make();
            var depts = ((INormQueryable<Dept>)ctx.Query<Dept>()).Include(d => d.Employees)
                .OrderBy(d => d.Id).ToList();
            var byId = depts.ToDictionary(d => d.Id);

            // A few random ops this round.
            var ops = rng.Next(1, 5);
            for (var o = 0; o < ops; o++)
            {
                // Candidate emps: those currently assigned to some loaded dept.
                var assigned = depts.SelectMany(d => d.Employees.Select(e => (Dept: d, Emp: e))).ToList();
                if (assigned.Count == 0) break;
                var pick = assigned[rng.Next(assigned.Count)];

                if (rng.Next(2) == 0)
                {
                    // SEVER: remove from its dept's collection → FK null.
                    pick.Dept.Employees.Remove(pick.Emp);
                    oracle[pick.Emp.Id] = null;
                }
                else
                {
                    // REPARENT: move to a different dept's collection → FK repointed.
                    var target = byId[rng.Next(1, DeptCount + 1)];
                    if (ReferenceEquals(target, pick.Dept)) continue;
                    pick.Dept.Employees.Remove(pick.Emp);
                    target.Employees.Add(pick.Emp);
                    oracle[pick.Emp.Id] = target.Id;
                }
            }

            await ctx.SaveChangesAsync();

            var dbState = ReadOracleFromDb(keeper);
            foreach (var kvp in oracle)
                Assert.True(dbState[kvp.Key] == kvp.Value,
                    $"seed={seed} round={round} emp={kvp.Key}: expected DeptId={kvp.Value?.ToString() ?? "null"} got {dbState[kvp.Key]?.ToString() ?? "null"}");
        }
    }
}
