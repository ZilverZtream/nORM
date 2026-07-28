using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The precompiled IL fast materializer reads reader ordinal i into the i-th public property, assuming the
/// reader columns are exactly the public properties in order. An entity with a [NotMapped] (or navigation,
/// or read-only) property breaks that alignment: the SELECT emits only mapped columns, so the ordinals shift
/// and a later property reads the wrong column (silent-wrong under a wide reader) or reads past the end
/// (crash). Using the fast materializer must be gated on the properties provably matching the mapped columns;
/// otherwise fall back to the reflection materializer, which reads by the mapping.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PrecompiledILMaterializerAlignmentTests
{
    [Table("IlmOrder")]
    public class IlmOrder
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Total { get; set; }
        [NotMapped] public string Note { get; set; } = "";  // extra writable public property, not a column
    }

    [Fact]
    public void Precompiled_entity_with_notmapped_property_still_materializes_correctly()
    {
        // Opt into the IL fast path for this type (populates the static fast-materializer cache).
        MaterializerFactory.PrecompileCommonPatterns<IlmOrder>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE IlmOrder (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Total INTEGER NOT NULL);" +
                "INSERT INTO IlmOrder VALUES (1, 'alpha', 42);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions());

        var row = ctx.Query<IlmOrder>().Single();

        Assert.Equal(1, row.Id);
        Assert.Equal("alpha", row.Name);
        Assert.Equal(42, row.Total);
    }
}
