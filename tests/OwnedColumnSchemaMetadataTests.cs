using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The fluent schema verbs HasComment / HasDefaultValue configured on an OWNED collection element
/// (OwnsMany) reach the owned child table's generated schema, not just top-level entities. Verified
/// end-to-end through EnsureCreated and the verbatim CREATE text preserved in sqlite_master.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedColumnSchemaMetadataTests
{
    [Table("OcsOwner")]
    public class Owner
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    public class Line
    {
        public string Note { get; set; } = "";
        public int Qty { get; set; }
    }

    [Fact]
    public void HasComment_and_HasDefaultValue_on_owned_columns_reach_the_child_table_schema()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Owner>().HasKey(o => o.Id);
                mb.Entity<Owner>().OwnsMany<Line>(o => o.Lines, tableName: "OcsLine", foreignKey: "OwnerId",
                    buildAction: b =>
                    {
                        b.Property<string>(l => l.Note).HasComment("line note");
                        b.Property<int>(l => l.Qty).HasDefaultValue(7);
                    });
            }
        }, ownsConnection: false);

        ctx.Database.EnsureCreated();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT sql FROM sqlite_master WHERE name='OcsLine'";
        var ddl = (string)cmd.ExecuteScalar()!;

        Assert.Contains("/* line note */", ddl);   // owned-column comment reached the child table
        Assert.Contains("DEFAULT 7", ddl);          // owned-column literal default reached the child table
    }
}
