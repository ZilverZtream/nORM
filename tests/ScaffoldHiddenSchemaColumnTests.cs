#nullable enable

using System.Data;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// GetSchemaTable(KeyInfo) pads a view's metadata with the base tables' key columns so keys can be
/// read back. Those columns are marked IsHidden and are not part of the view's projection, so emitting
/// them as entity properties makes every read fail with "invalid column name". The scaffolder must skip
/// hidden schema-table rows while keeping every visible (projected) column.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ScaffoldHiddenSchemaColumnTests
{
    [Fact]
    public void Hidden_key_info_columns_are_excluded_and_visible_columns_are_kept()
    {
        var schema = new DataTable();
        schema.Columns.Add("ColumnName", typeof(string));
        schema.Columns.Add("IsHidden", typeof(bool));

        var projected = schema.NewRow();
        projected["ColumnName"] = "eh_namn";
        projected["IsHidden"] = false;
        schema.Rows.Add(projected);

        var keyInfoPadding = schema.NewRow();
        keyInfoPadding["ColumnName"] = "eh_id"; // a base table's key surfaced only by KeyInfo
        keyInfoPadding["IsHidden"] = true;
        schema.Rows.Add(keyInfoPadding);

        Assert.False(ScaffoldEntitySourceBuilder.IsHiddenSchemaColumn(projected));
        Assert.True(ScaffoldEntitySourceBuilder.IsHiddenSchemaColumn(keyInfoPadding));
    }

    [Fact]
    public void A_schema_table_without_an_is_hidden_column_treats_every_column_as_visible()
    {
        var schema = new DataTable();
        schema.Columns.Add("ColumnName", typeof(string));
        var row = schema.NewRow();
        row["ColumnName"] = "Id";
        schema.Rows.Add(row);

        Assert.False(ScaffoldEntitySourceBuilder.IsHiddenSchemaColumn(row));
    }
}
