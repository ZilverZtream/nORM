#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// A database view (and some stored-procedure result sets) may legally expose the same column name
/// more than once, e.g. <c>SELECT l.val, r.val FROM ...</c>. The scaffolder's column-to-property map
/// is keyed by column name, so those duplicates collapse to a single entry; without a per-entity
/// uniqueness guard every physical occurrence would emit the same C# property name and the generated
/// entity would not compile (CS0102 "already contains a definition"). Each physical column must
/// receive a distinct property name while the first occurrence keeps its mapped name unchanged.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ScaffoldDuplicateViewColumnTests
{
    [Fact]
    public void Repeated_column_names_get_distinct_property_names()
    {
        // The map is keyed by column name, so a view exposing "val" twice yields a single entry.
        var columnPropertyNames = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["id"] = "Id",
            ["val"] = "Val",
        };

        var names = ScaffoldEntitySourceBuilder.AssignUniqueColumnPropertyNames(
            "DupPriceView",
            columnPropertyNames,
            new[] { "id", "val", "val" });

        Assert.Equal(3, names.Count);
        Assert.Equal(names.Count, names.Distinct(StringComparer.Ordinal).Count());
        Assert.Equal("Id", names[0]);
        Assert.Equal("Val", names[1]); // first occurrence keeps the mapped name verbatim
        Assert.NotEqual(names[1], names[2]);
    }

    [Fact]
    public void Names_are_unchanged_when_no_column_name_repeats()
    {
        var columnPropertyNames = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["id"] = "Id",
            ["price"] = "Price",
            ["name"] = "Name",
        };

        var names = ScaffoldEntitySourceBuilder.AssignUniqueColumnPropertyNames(
            "Product",
            columnPropertyNames,
            new[] { "id", "price", "name" });

        Assert.Equal(new[] { "Id", "Price", "Name" }, names);
    }

    [Fact]
    public void Reserved_member_and_entity_names_never_collide()
    {
        // A column mapping that would shadow the entity name or an object member must still be renamed.
        var columnPropertyNames = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["a"] = "A",
            ["a_again"] = "A",
        };

        var names = ScaffoldEntitySourceBuilder.AssignUniqueColumnPropertyNames(
            "Sample",
            columnPropertyNames,
            new[] { "a", "a_again" });

        Assert.Equal(names.Count, names.Distinct(StringComparer.Ordinal).Count());
    }
}
