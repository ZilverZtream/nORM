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

[Trait("Category", "Fast")]
public class MultiLevelGeneratedKeyInsertTests
{
    [Table("MgkCompany")]
    public class Company
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Dept> Depts { get; set; } = new();
    }

    [Table("MgkDept")]
    public class Dept
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int CompanyId { get; set; }
        public string Name { get; set; } = "";
        public List<Employee> Employees { get; set; } = new();
    }

    [Table("MgkEmployee")]
    public class Employee
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int DeptId { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:mgk_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MgkCompany (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                CREATE TABLE MgkDept (Id INTEGER PRIMARY KEY AUTOINCREMENT, CompanyId INTEGER NOT NULL, Name TEXT NOT NULL);
                CREATE TABLE MgkEmployee (Id INTEGER PRIMARY KEY AUTOINCREMENT, DeptId INTEGER NOT NULL, Name TEXT NOT NULL);
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

    [Fact]
    public async Task Generated_keys_propagate_through_three_levels_on_insert()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        // Build the full graph with NO keys set — every level's key is DB-generated.
        var company = new Company { Name = "Acme" };
        var d1 = new Dept { Name = "Eng" };
        var d2 = new Dept { Name = "Ops" };
        d1.Employees.Add(new Employee { Name = "a" });
        d1.Employees.Add(new Employee { Name = "b" });
        d2.Employees.Add(new Employee { Name = "c" });
        company.Depts.Add(d1);
        company.Depts.Add(d2);
        ctx.Add(company);
        await ctx.SaveChangesAsync();

        // Every FK must chain to the correct generated key — no dangling FK=0.
        Assert.True(company.Id > 0);
        Assert.True(d1.Id > 0 && d2.Id > 0);
        Assert.Equal(company.Id, d1.CompanyId);
        Assert.Equal(company.Id, d2.CompanyId);
        foreach (var e in d1.Employees) Assert.Equal(d1.Id, e.DeptId);
        foreach (var e in d2.Employees) Assert.Equal(d2.Id, e.DeptId);

        // Verify persisted linkage from the database directly.
        int CountEmpUnderCompany(int companyId)
        {
            using var cmd = keeper.CreateCommand();
            cmd.CommandText = $@"SELECT COUNT(*) FROM MgkEmployee e
                JOIN MgkDept d ON e.DeptId = d.Id
                JOIN MgkCompany c ON d.CompanyId = c.Id
                WHERE c.Id = {companyId}";
            return Convert.ToInt32(cmd.ExecuteScalar());
        }
        Assert.Equal(3, CountEmpUnderCompany(company.Id)); // all 3 employees chain to the company
    }
}
