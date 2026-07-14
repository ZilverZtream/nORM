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
/// Removing the root of a three-level tracked graph (Company → Dept → Employee) with every
/// navigation populated must cascade the delete through all levels — a one-level cascade
/// would silently leave FK-dangling grandchildren.
/// </summary>
[Trait("Category", "Fast")]
public class MultiLevelCascadeDeleteTests
{
    [Table("MlcCompany")]
    public class Company
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Dept> Depts { get; set; } = new();
    }

    [Table("MlcDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public int CompanyId { get; set; }
        public List<Employee> Employees { get; set; } = new();
    }

    [Table("MlcEmployee")]
    public class Employee
    {
        [Key] public int Id { get; set; }
        public int DeptId { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:mlc_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MlcCompany (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE MlcDept (Id INTEGER PRIMARY KEY, CompanyId INTEGER NOT NULL);
                CREATE TABLE MlcEmployee (Id INTEGER PRIMARY KEY, DeptId INTEGER NOT NULL, Name TEXT NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    mb.Entity<Company>().HasKey(c => c.Id);
                    mb.Entity<Dept>().HasKey(d => d.Id);
                    mb.Entity<Employee>().HasKey(e => e.Id);
                    mb.Entity<Company>().HasMany(c => c.Depts).WithOne().HasForeignKey(d => d.CompanyId, c => c.Id);
                    mb.Entity<Dept>().HasMany(d => d.Employees).WithOne().HasForeignKey(e => e.DeptId, d => d.Id);
                }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static int Count(SqliteConnection k, string table)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Deleting_root_cascades_through_three_levels()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var company = new Company { Id = 1, Name = "Acme" };
        var d1 = new Dept { Id = 10 };
        var d2 = new Dept { Id = 11 };
        d1.Employees.Add(new Employee { Id = 100, Name = "a" });
        d1.Employees.Add(new Employee { Id = 101, Name = "b" });
        d2.Employees.Add(new Employee { Id = 102, Name = "c" });
        company.Depts.Add(d1);
        company.Depts.Add(d2);
        ctx.Add(company);
        await ctx.SaveChangesAsync();
        Assert.Equal(1, Count(keeper, "MlcCompany"));
        Assert.Equal(2, Count(keeper, "MlcDept"));
        Assert.Equal(3, Count(keeper, "MlcEmployee"));

        // The full graph is tracked with populated navigations at every level. Removing the
        // root must cascade through Depts AND Employees — a one-level cascade would orphan
        // the employees (silent FK-dangling rows).
        ctx.Remove(company);
        await ctx.SaveChangesAsync();

        Assert.Equal(0, Count(keeper, "MlcCompany"));
        Assert.Equal(0, Count(keeper, "MlcDept"));
        Assert.Equal(0, Count(keeper, "MlcEmployee")); // grandchildren gone too
    }
}
