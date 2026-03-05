using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SG-1: Verifies that a compiled query delegate recomputes the plan for a new context
/// rather than permanently reusing the first context's plan.
/// </summary>
public class CompiledQueryContextIsolationTests
{
    [Table("Person")]
    public class PersonSG
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateCtx(string tableName)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                $"CREATE TABLE \"{tableName}\"(Id INTEGER PRIMARY KEY, Name TEXT);" +
                $"INSERT INTO \"{tableName}\" VALUES(1,'FromTable_{tableName}');";
            cmd.ExecuteNonQuery();
        }

        DbContext ctx;
        if (tableName == "Person")
        {
            ctx = new DbContext(cn, new SqliteProvider());
        }
        else
        {
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<PersonSG>().ToTable(tableName) };
            ctx = new DbContext(cn, new SqliteProvider(), opts);
        }
        return (cn, ctx);
    }

    [Fact]
    public async Task CompiledQuery_recomputes_plan_for_different_table_mapping()
    {
        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<PersonSG>().Where(p => p.Id == id));

        // First context uses default "Person" table
        var (cn1, ctx1) = CreateCtx("Person");
        using (cn1) using (ctx1)
        {
            var r1 = await compiled(ctx1, 1);
            Assert.Single(r1);
            Assert.Equal("FromTable_Person", r1[0].Name);
        }

        // Second context maps the same entity to "Persons" table
        var (cn2, ctx2) = CreateCtx("Persons");
        using (cn2) using (ctx2)
        {
            var r2 = await compiled(ctx2, 1);
            Assert.Single(r2);
            // Must come from the "Persons" table, not the stale "Person" plan
            Assert.Equal("FromTable_Persons", r2[0].Name);
        }
    }

    [Fact]
    public async Task CompiledQuery_same_mapping_reuses_plan_correctly()
    {
        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<PersonSG>().Where(p => p.Id == id));

        // Both contexts use the default "Person" table — plan should be reused and still correct
        var (cn1, ctx1) = CreateCtx("Person");
        var (cn2, ctx2) = CreateCtx("Person");
        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var r1 = await compiled(ctx1, 1);
            var r2 = await compiled(ctx2, 1);
            Assert.Single(r1);
            Assert.Single(r2);
            Assert.Equal(r1[0].Name, r2[0].Name);
        }
    }
}
