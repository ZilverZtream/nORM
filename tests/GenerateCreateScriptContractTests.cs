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
/// Pins <c>Database.GenerateCreateScript()</c> (EF-parity): it emits the create-table DDL for the mapped
/// model as a string, ordering a principal before its dependents, and it EXECUTES nothing — the tables do
/// not appear until the returned script is run. The generated DDL is valid and round-trips into a working
/// schema.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GenerateCreateScriptContractTests
{
    [Table("GcsDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Emp> Emps { get; set; } = new();
    }

    [Table("GcsEmp")]
    public class Emp
    {
        [Key] public int Id { get; set; }
        public int DeptId { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext Make(SqliteConnection cn)
    {
        // Tables are intentionally NOT created here — GenerateCreateScript must produce the DDL itself.
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Dept>().HasMany(d => d.Emps).WithOne().HasForeignKey(e => e.DeptId, d => d.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static bool TableExists(SqliteConnection cn, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name=$n";
        cmd.Parameters.AddWithValue("$n", name);
        return cmd.ExecuteScalar() != null;
    }

    [Fact]
    public void Generates_create_table_ddl_with_principal_before_dependent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Make(cn);

        var script = ctx.Database.GenerateCreateScript();

        Assert.Contains("CREATE TABLE", script);
        Assert.Contains("GcsDept", script);
        Assert.Contains("GcsEmp", script);
        // FK ordering: the principal's CREATE precedes the dependent's, so its name appears first.
        Assert.True(script.IndexOf("GcsDept", StringComparison.Ordinal) < script.IndexOf("GcsEmp", StringComparison.Ordinal));
    }

    [Fact]
    public void Generates_without_executing_and_the_ddl_round_trips()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Make(cn);

        Assert.False(TableExists(cn, "GcsDept"));   // nothing created yet

        var script = ctx.Database.GenerateCreateScript();

        // Generation executed nothing — the tables still do not exist.
        Assert.False(TableExists(cn, "GcsDept"));
        Assert.False(TableExists(cn, "GcsEmp"));

        // Running the generated script creates a working schema.
        using (var run = cn.CreateCommand())
        {
            run.CommandText = script;
            run.ExecuteNonQuery();
        }
        Assert.True(TableExists(cn, "GcsDept"));
        Assert.True(TableExists(cn, "GcsEmp"));
    }

    [Fact]
    public async Task Generated_schema_supports_inserts_and_queries()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Make(cn);

        using (var run = cn.CreateCommand())
        {
            run.CommandText = ctx.Database.GenerateCreateScript();
            run.ExecuteNonQuery();
        }

        ctx.Add(new Dept { Id = 1, Name = "eng" });
        ctx.Add(new Emp { Id = 1, DeptId = 1, Name = "ada" });
        await ctx.SaveChangesAsync();

        var emps = await ctx.Query<Emp>().Where(e => e.DeptId == 1).ToListAsync();
        Assert.Equal(new[] { "ada" }, emps.Select(e => e.Name).ToArray());
    }
}
