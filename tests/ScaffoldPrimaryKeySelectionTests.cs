#nullable enable

using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// The entity key must follow the declared PRIMARY KEY constraint, not the schema table's IsKey flag.
/// SqlDataReader.GetSchemaTable reports the "best" unique row identifier, so for a table whose unique
/// clustered index differs from a nonclustered primary key (e.g. ASP.NET's aspnet_Users: PK on UserId,
/// unique clustered index on ApplicationId + LoweredUserName) it flags the clustered index columns as
/// the key. Marking those as [Key] silently mis-keys the entity, breaking writes and any relationship
/// that targets the real key. The discovered primary-key columns must win.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ScaffoldPrimaryKeySelectionTests
{
    [Fact]
    public void Declared_primary_key_wins_over_schema_is_key_flag()
    {
        var primaryKey = new[] { "UserId" };

        // The declared key column is the key even though the schema flag says otherwise...
        Assert.True(ScaffoldEntitySourceBuilder.IsDeclaredPrimaryKeyColumn(primaryKey, "UserId", schemaIsKeyFallback: false));

        // ...and clustered-index columns the schema flag reports are NOT part of the key.
        Assert.False(ScaffoldEntitySourceBuilder.IsDeclaredPrimaryKeyColumn(primaryKey, "ApplicationId", schemaIsKeyFallback: true));
        Assert.False(ScaffoldEntitySourceBuilder.IsDeclaredPrimaryKeyColumn(primaryKey, "LoweredUserName", schemaIsKeyFallback: true));
    }

    [Fact]
    public void Composite_declared_primary_key_matches_all_its_columns()
    {
        var primaryKey = new[] { "AuthorId", "BookId" };

        Assert.True(ScaffoldEntitySourceBuilder.IsDeclaredPrimaryKeyColumn(primaryKey, "AuthorId", schemaIsKeyFallback: false));
        Assert.True(ScaffoldEntitySourceBuilder.IsDeclaredPrimaryKeyColumn(primaryKey, "BookId", schemaIsKeyFallback: false));
        Assert.False(ScaffoldEntitySourceBuilder.IsDeclaredPrimaryKeyColumn(primaryKey, "AddedOn", schemaIsKeyFallback: false));
    }

    [Fact]
    public void Falls_back_to_the_schema_flag_for_keyless_tables_and_views()
    {
        // No primary key discovered (view / keyless table): the schema IsKey flag is honored as-is.
        Assert.True(ScaffoldEntitySourceBuilder.IsDeclaredPrimaryKeyColumn(null, "Anything", schemaIsKeyFallback: true));
        Assert.False(ScaffoldEntitySourceBuilder.IsDeclaredPrimaryKeyColumn(null, "Anything", schemaIsKeyFallback: false));
        Assert.True(ScaffoldEntitySourceBuilder.IsDeclaredPrimaryKeyColumn(new string[0], "Anything", schemaIsKeyFallback: true));
    }
}
