using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public partial class ScaffoldingContractDocTests
{
    [Fact]
    public void Routine_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetRevenue",
            "Routine",
            "SQL Server stored procedure; parameters=2; outputParameters=1; parameterModes=@tenantId:IN:int,@total:OUT:decimal; resultColumns=Id:int:0|Name:nvarchar(40):1",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("SQL Server", metadata["provider"]);
        Assert.Equal("stored procedure", metadata["routineType"]);
        Assert.Equal(2, metadata["parameterCount"]);
        Assert.Equal(1, metadata["outputParameterCount"]);
        Assert.Equal("@tenantId", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("@total", parameters[1]["name"]);
        Assert.Equal("OUT", parameters[1]["mode"]);
        Assert.Equal("decimal", parameters[1]["dataType"]);
        Assert.Equal("Id", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
        Assert.Equal("Name", resultColumns[1]["name"]);
        Assert.Equal("nvarchar(40)", resultColumns[1]["dataType"]);
        Assert.Equal(true, resultColumns[1]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_colon_characters_in_provider_names()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant:id:IN:int; resultColumns=Order:Id:int:0|Line:Name:nvarchar(40):1",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("@tenant:id", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("Order:Id", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
        Assert.Equal("Line:Name", resultColumns[1]["name"]);
        Assert.Equal("nvarchar(40)", resultColumns[1]["dataType"]);
        Assert.Equal(true, resultColumns[1]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_mode_like_text_inside_parameter_names()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant:IN:name:IN:int; resultColumns=",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;

        Assert.Equal("@tenant:IN:name", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_semicolon_characters_inside_values()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant;id:IN:int; resultColumns=Order;Id:int:0|Line;Name:nvarchar(40):1",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("@tenant;id", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("Order;Id", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
        Assert.Equal("Line;Name", resultColumns[1]["name"]);
        Assert.Equal("nvarchar(40)", resultColumns[1]["dataType"]);
        Assert.Equal(true, resultColumns[1]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_splits_lists_after_complete_entries()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=2; outputParameters=0; parameterModes=@tenant,id:IN:int,@search,text:IN:nvarchar(40); resultColumns=Order|Id:int:0|Line|Name:nvarchar(40):1",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("@tenant,id", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("@search,text", parameters[1]["name"]);
        Assert.Equal("IN", parameters[1]["mode"]);
        Assert.Equal("nvarchar(40)", parameters[1]["dataType"]);
        Assert.Equal("Order|Id", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
        Assert.Equal("Line|Name", resultColumns[1]["name"]);
        Assert.Equal("nvarchar(40)", resultColumns[1]["dataType"]);
        Assert.Equal(true, resultColumns[1]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_unknown_key_like_text_inside_values()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "GetOddNames",
            "Routine",
            "SQL Server stored procedure; parameters=1; outputParameters=0; parameterModes=@tenant; note=retained:IN:int; resultColumns=Order; note=retained:int:0",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;
        var resultColumns = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["resultColumns"]!;

        Assert.Equal("@tenant; note=retained", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("int", parameters[0]["dataType"]);
        Assert.Equal("Order; note=retained", resultColumns[0]["name"]);
        Assert.Equal("int", resultColumns[0]["dataType"]);
        Assert.Equal(false, resultColumns[0]["nullable"]);
    }

    [Fact]
    public void Routine_warning_metadata_preserves_allowlisted_key_text_inside_values()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var routine = Activator.CreateInstance(
            skippedObjectType,
            "public",
            "calculate_odd",
            "Routine",
            "PostgreSQL function; parameters=1; outputParameters=0; parameterModes=tenant; dataType=retained:IN:integer; dataType=integer",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { routine })!;
        var parameters = (IReadOnlyList<IReadOnlyDictionary<string, object?>>)metadata["parameters"]!;

        Assert.Equal("tenant; dataType=retained", parameters[0]["name"]);
        Assert.Equal("IN", parameters[0]["mode"]);
        Assert.Equal("integer", parameters[0]["dataType"]);
        Assert.Equal("integer", metadata["dataType"]);
        Assert.Equal("scalar-function", metadata["callShape"]);
    }

    [Fact]
    public void Sequence_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var sequence = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "OrderNo",
            "Sequence",
            "SQL Server sequence; dataType=bigint",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { sequence })!;

        Assert.Equal("SQL Server", metadata["provider"]);
        Assert.Equal("bigint", metadata["dataType"]);
        Assert.Equal("long", metadata["clrType"]);
        Assert.Equal(true, metadata["stubSupported"]);
    }

    [Fact]
    public void Synonym_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var synonym = Activator.CreateInstance(
            skippedObjectType,
            "dbo",
            "OrderAlias",
            "Synonym",
            "SQL Server synonym; baseObject=[dbo].[Orders; note=retained]; baseType=U",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { synonym })!;

        Assert.Equal("SQL Server", metadata["provider"]);
        Assert.Equal("[dbo].[Orders; note=retained]", metadata["baseObject"]);
        Assert.Equal("U", metadata["baseType"]);
        Assert.Equal("Table", metadata["targetKind"]);
        Assert.Equal(true, metadata["queryArtifactSupported"]);
    }

    [Fact]
    public void Event_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var scheduledEvent = Activator.CreateInstance(
            skippedObjectType,
            null,
            "RefreshLedger",
            "Event",
            "MySQL event; eventType=RECURRING; status=ENABLED; intervalValue=1; intervalField=DAY; starts=2026-01-01 00:00:00",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { scheduledEvent })!;

        Assert.Equal("MySQL", metadata["provider"]);
        Assert.Equal("RECURRING", metadata["eventType"]);
        Assert.Equal("ENABLED", metadata["status"]);
        Assert.Equal("1", metadata["intervalValue"]);
        Assert.Equal("DAY", metadata["intervalField"]);
        Assert.Equal("2026-01-01 00:00:00", metadata["starts"]);
    }

    [Fact]
    public void Query_object_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var view = Activator.CreateInstance(
            skippedObjectType,
            "reporting",
            "OpenOrders",
            "View",
            "PostgreSQL view",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { view })!;

        Assert.Equal("PostgreSQL", metadata["provider"]);
        Assert.Equal("View", metadata["targetKind"]);
        Assert.Equal(true, metadata["queryArtifactSupported"]);
    }

    [Fact]
    public void Virtual_table_shadow_warning_metadata_includes_owner()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var shadow = Activator.CreateInstance(
            skippedObjectType,
            "main",
            "SearchDocs_content",
            "VirtualTableShadow",
            "SQLite virtual table shadow table",
            null)!;
        var method = scaffolder.GetMethod("BuildSkippedObjectMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        var metadata = (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { shadow })!;

        Assert.Equal("SQLite", metadata["provider"]);
        Assert.Equal("VirtualTableShadow", metadata["targetKind"]);
        Assert.Equal(false, metadata["queryArtifactSupported"]);
        Assert.Equal("SearchDocs", metadata["shadowOf"]);
    }

    [Fact]
    public void Provider_owned_feature_warning_metadata_is_structured_for_ci_inventory()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = scaffolder.GetMethod("BuildUnsupportedFeatureMetadata", BindingFlags.NonPublic | BindingFlags.Static)!;

        IReadOnlyDictionary<string, object?> Metadata(string kind, string name, string detail, string tableKey = "dbo.Orders")
        {
            var feature = Activator.CreateInstance(featureType, tableKey, kind, name, detail)!;
            return (IReadOnlyDictionary<string, object?>)method.Invoke(null, new[] { feature })!;
        }

        var missingPrimaryKey = Metadata("MissingPrimaryKey", "OrderReport", "Table has no primary key.", "dbo.OrderReport");
        Assert.Equal(true, missingPrimaryKey["readOnlyEntity"]);
        Assert.Equal(false, missingPrimaryKey["generatedWritesSupported"]);
        Assert.Equal(false, missingPrimaryKey["generatedNavigationSupported"]);
        Assert.Equal("dbo.OrderReport", missingPrimaryKey["table"]);
        Assert.Equal("missing-primary-key", missingPrimaryKey["reason"]);

        var principalKey = Metadata("RelationshipPrincipalKey", "FK_Order_Customer", "FK references crm.Customer.(TenantId, Code); generated principal key is Id");
        Assert.Equal(true, principalKey["navigationSuppressed"]);
        Assert.Equal("principal-key-not-scaffoldable", principalKey["reason"]);
        Assert.Equal("dbo.Orders", principalKey["dependentTable"]);
        Assert.Equal("crm.Customer", principalKey["principalTable"]);
        Assert.Equal(new[] { "TenantId", "Code" }, (string[])principalKey["principalColumns"]!);

        var dependentKey = Metadata("RelationshipDependentKey", "FK_Report_Order", "Dependent table has no generated primary key.", "report.OrderReport");
        Assert.Equal(true, dependentKey["navigationSuppressed"]);
        Assert.Equal("dependent-table-keyless", dependentKey["reason"]);
        Assert.Equal(false, dependentKey["generatedNavigationSupported"]);
        Assert.Equal("report.OrderReport", dependentKey["dependentTable"]);

        var referentialAction = Metadata("ReferentialAction", "FK_Order_Customer", "ON DELETE SET DEFAULT; ON UPDATE NO ACTION");
        Assert.Equal(true, referentialAction["navigationSuppressed"]);
        Assert.Equal(false, referentialAction["generatedNavigationSupported"]);
        Assert.Equal("referential-action-not-scaffoldable", referentialAction["reason"]);
        Assert.Equal("SET DEFAULT", referentialAction["onDelete"]);
        Assert.Equal("NO ACTION", referentialAction["onUpdate"]);

        var precisionScale = Metadata("PrecisionScale", "Amount", "decimal(18,2)");
        Assert.Equal(18, precisionScale["precision"]);
        Assert.Equal(2, precisionScale["scale"]);

        var precisionOnly = Metadata("PrecisionScale", "Amount", "numeric(10)");
        Assert.Equal(10, precisionOnly["precision"]);
        Assert.Null(precisionOnly["scale"]);

        var computed = Metadata("Computed", "Total", "(Price * Quantity) STORED");
        Assert.Equal("Price * Quantity", computed["computedSql"]);
        Assert.Equal(true, computed["stored"]);

        var rowVersion = Metadata("RowVersion", "Version", "rowversion");
        Assert.Equal("dbo.Orders", rowVersion["table"]);
        Assert.Equal("rowversion", rowVersion["providerType"]);
        Assert.Equal(true, rowVersion["providerOwnedDdl"]);
        Assert.Equal(true, rowVersion["generatedModelConfigurationSupported"]);
        Assert.Equal(true, rowVersion["concurrencyToken"]);
        Assert.Equal(true, rowVersion["databaseGenerated"]);
        Assert.Equal(false, rowVersion["readOnlyEntity"]);
        Assert.Equal(true, rowVersion["generatedWritesSupported"]);
        Assert.Equal("provider-managed-rowversion", rowVersion["reason"]);

        var identity = Metadata("IdentityStrategy", "Id", "IDENTITY(100,10)");
        Assert.Equal("IDENTITY(100,10)", identity["identityStrategy"]);
        Assert.Equal(true, identity["readOnlyEntity"]);
        Assert.Equal(false, identity["generatedWritesSupported"]);
        Assert.Equal("provider-specific-identity-strategy", identity["reason"]);
        Assert.Equal(100L, identity["seed"]);
        Assert.Equal(10L, identity["increment"]);

        var providerSpecificColumn = Metadata("ProviderSpecificColumnType", "Shape", "geometry");
        Assert.Equal("geometry", providerSpecificColumn["providerType"]);
        Assert.Equal(true, providerSpecificColumn["readOnlyEntity"]);
        Assert.Equal(false, providerSpecificColumn["generatedWritesSupported"]);
        Assert.Equal("provider-specific-column-type", providerSpecificColumn["reason"]);
        Assert.Equal("Latin1_General_CI_AS", Metadata("Collation", "Name", "Latin1_General_CI_AS")["collation"]);
        var defaultSql = Metadata("Default", "CreatedAt", "now()");
        Assert.Equal("now()", defaultSql["defaultSql"]);
        Assert.Equal(true, defaultSql["readOnlyEntity"]);
        Assert.Equal(false, defaultSql["generatedWritesSupported"]);
        Assert.Equal("provider-specific-default", defaultSql["reason"]);
        Assert.Equal("Amount > 0", Metadata("CheckConstraint", "CK_Orders_Amount", "CHECK (Amount > 0)")["checkSql"]);
        var partialIndex = Metadata("PartialIndex", "IX_Orders_Open", "CREATE INDEX IX_Orders_Open ON Orders (Status) WHERE Status = 'Open'");
        Assert.Equal(true, partialIndex["filtered"]);
        Assert.Equal("Status = 'Open'", partialIndex["filterSql"]);
        Assert.Equal(false, partialIndex["isUnique"]);

        var expressionIndex = Metadata("ExpressionIndex", "IX_Orders_LowerName", "CREATE UNIQUE INDEX IX ON Orders (lower(Name)) WHERE Name IS NOT NULL");
        Assert.Equal(true, expressionIndex["expressionBased"]);
        Assert.Equal("lower(Name)", expressionIndex["expressionSql"]);
        Assert.Equal("lower(Name)", expressionIndex["keySql"]);
        Assert.Equal("Name IS NOT NULL", expressionIndex["filterSql"]);
        Assert.Equal(true, expressionIndex["isUnique"]);
        Assert.StartsWith("CREATE UNIQUE INDEX", Assert.IsType<string>(expressionIndex["indexSql"]));

        var mySqlExpression = Metadata("ExpressionIndex", "IX_Orders_LowerName", "MySQL expression index; expression=LOWER(`Name`)");
        Assert.Equal("MySQL", mySqlExpression["provider"]);
        Assert.Equal("LOWER(`Name`)", mySqlExpression["expressionSql"]);
        Assert.Equal(true, Metadata("IncludedColumnIndex", "IX_Orders_Customer", "SQL Server index with included columns")["includedColumns"]);
        Assert.Equal(true, Metadata("DescendingIndex", "IX_Orders_Created", "SQL Server descending index key")["descending"]);
        var prefixIndex = Metadata("PrefixIndex", "IX_Orders_Code", "MySQL prefix index; prefixColumns=Code:8/80");
        Assert.Equal(true, prefixIndex["prefixIndex"]);
        var prefixColumns = Assert.IsAssignableFrom<IReadOnlyList<IReadOnlyDictionary<string, object?>>>(prefixIndex["prefixColumns"]);
        var prefixColumn = Assert.Single(prefixColumns);
        Assert.Equal("Code", prefixColumn["name"]);
        Assert.Equal(8, prefixColumn["prefixLength"]);
        Assert.Equal(80, prefixColumn["declaredLength"]);
        var sqlServerProviderIndex = Metadata("ProviderSpecificIndex", "IX_Orders_Search", "SQL Server provider-specific index; indexType=NONCLUSTERED COLUMNSTORE");
        Assert.Equal(true, sqlServerProviderIndex["providerSpecific"]);
        Assert.Equal("SQL Server", sqlServerProviderIndex["provider"]);
        Assert.Equal("NONCLUSTERED COLUMNSTORE", sqlServerProviderIndex["indexType"]);

        var mySqlProviderIndex = Metadata("ProviderSpecificIndex", "IX_Orders_Search", "MySQL provider-specific index; indexType=FULLTEXT");
        Assert.Equal("MySQL", mySqlProviderIndex["provider"]);
        Assert.Equal("FULLTEXT", mySqlProviderIndex["indexType"]);

        var postgresProviderIndex = Metadata(
            "ProviderSpecificIndex",
            "IX_Orders_Search",
            "PostgreSQL btree index with provider-specific key options; accessMethod=btree; hasNullsNotDistinct=true; hasNonDefaultOperatorClass=false; hasIndexCollation=true; hasNonDefaultNullOrdering=true; indexSql=CREATE UNIQUE INDEX IX ON Orders (Name NULLS FIRST) NULLS NOT DISTINCT");
        Assert.Equal(true, postgresProviderIndex["providerSpecific"]);
        Assert.Equal("PostgreSQL", postgresProviderIndex["provider"]);
        Assert.Equal("btree", postgresProviderIndex["accessMethod"]);
        Assert.Equal(true, postgresProviderIndex["hasNullsNotDistinct"]);
        Assert.Equal(false, postgresProviderIndex["hasNonDefaultOperatorClass"]);
        Assert.Equal(true, postgresProviderIndex["hasIndexCollation"]);
        Assert.Equal(true, postgresProviderIndex["hasNonDefaultNullOrdering"]);
        Assert.Equal(true, postgresProviderIndex["isUnique"]);
        Assert.Equal("Name NULLS FIRST", postgresProviderIndex["keySql"]);

        var postgresQuotedProviderIndex = Metadata(
            "ProviderSpecificIndex",
            "IX_Orders_Quoted",
            "PostgreSQL btree index with provider-specific key options; accessMethod=btree; hasNullsNotDistinct=false; hasNonDefaultOperatorClass=false; hasIndexCollation=false; hasNonDefaultNullOrdering=true; indexSql=CREATE INDEX IX ON Orders (regexp_replace(Name, $tag$; accessMethod=gin; hasIndexCollation=true; indexSql=wrong$tag$, '', 'g') NULLS FIRST)");
        Assert.Equal("btree", postgresQuotedProviderIndex["accessMethod"]);
        Assert.Equal(false, postgresQuotedProviderIndex["hasNullsNotDistinct"]);
        Assert.Equal(false, postgresQuotedProviderIndex["hasIndexCollation"]);
        Assert.Equal(true, postgresQuotedProviderIndex["hasNonDefaultNullOrdering"]);
        Assert.Equal(
            "regexp_replace(Name, $tag$; accessMethod=gin; hasIndexCollation=true; indexSql=wrong$tag$, '', 'g') NULLS FIRST",
            postgresQuotedProviderIndex["keySql"]);
        Assert.Contains(
            "$tag$; accessMethod=gin; hasIndexCollation=true; indexSql=wrong$tag$",
            Assert.IsType<string>(postgresQuotedProviderIndex["indexSql"]));

        var trigger = Metadata("Trigger", "TR_Orders_Audit", "PostgreSQL trigger; timing=BEFORE; event=INSERT; orientation=ROW");
        Assert.Equal("PostgreSQL", trigger["provider"]);
        Assert.Equal("Trigger", trigger["providerObjectKind"]);
        Assert.Equal("dbo.Orders", trigger["table"]);
        Assert.Equal("TR_Orders_Audit", trigger["triggerName"]);
        Assert.Equal(true, trigger["providerOwnedDdl"]);
        Assert.Equal(false, trigger["generatedModelConfigurationSupported"]);
        Assert.Equal(true, trigger["readOnlyEntity"]);
        Assert.Equal(false, trigger["generatedWritesSupported"]);
        Assert.Equal("provider-owned-trigger", trigger["reason"]);
        Assert.Equal(false, trigger["definitionAvailable"]);
        Assert.Equal("BEFORE", trigger["timing"]);
        Assert.Equal("INSERT", trigger["event"]);
        Assert.Equal("ROW", trigger["orientation"]);

        var sqlServerTrigger = Metadata("Trigger", "TR_Orders_Block", "SQL Server trigger; timing=INSTEAD OF; isDisabled=true; isInsteadOf=true");
        Assert.Equal("SQL Server", sqlServerTrigger["provider"]);
        Assert.Equal(true, sqlServerTrigger["isDisabled"]);
        Assert.Equal(true, sqlServerTrigger["isInsteadOf"]);

        var temporal = Metadata("TemporalTable", "Orders", "SQL Server system-versioned temporal table; temporalType=system-versioned; historyTable=dbo.OrdersHistory");
        Assert.Equal("SQL Server", temporal["provider"]);
        Assert.Equal("TemporalTable", temporal["providerObjectKind"]);
        Assert.Equal("dbo.Orders", temporal["table"]);
        Assert.Equal(true, temporal["providerNativeTemporal"]);
        Assert.Equal(false, temporal["generatedTemporalConfigurationSupported"]);
        Assert.Equal(true, temporal["readOnlyEntity"]);
        Assert.Equal(false, temporal["generatedWritesSupported"]);
        Assert.Equal("provider-native-temporal", temporal["reason"]);
        Assert.Equal("system-versioned", temporal["temporalType"]);
        Assert.Equal("dbo.OrdersHistory", temporal["historyTable"]);
    }

    [Fact]
    public void Precision_scale_warning_uses_stable_code_and_action()
    {
        var scaffolder = typeof(nORM.Scaffolding.DatabaseScaffolder);
        var foreignKeyType = scaffolder.GetNestedType("ScaffoldForeignKey", BindingFlags.NonPublic)!;
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var skippedObjectType = scaffolder.GetNestedType("ScaffoldSkippedObject", BindingFlags.NonPublic)!;
        var indexType = scaffolder.GetNestedType("ScaffoldIndex", BindingFlags.NonPublic)!;
        var features = Array.CreateInstance(featureType, 1);
        features.SetValue(
            Activator.CreateInstance(
                featureType,
                "dbo.Orders",
                "PrecisionScale",
                "Amount",
                "decimal(provider)")!,
            0);
        var method = scaffolder.GetMethod("ScaffoldDiagnosticsJson", BindingFlags.NonPublic | BindingFlags.Static)!;
        var json = (string)method.Invoke(
            null,
            new object[]
            {
                Array.CreateInstance(foreignKeyType, 0),
                features,
                Array.CreateInstance(skippedObjectType, 0),
                new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase),
                Array.CreateInstance(indexType, 0),
                new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase),
                new HashSet<string>(StringComparer.OrdinalIgnoreCase),
                null!
            })!;

        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var diagnostic = Assert.Single(doc.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray());
        Assert.Equal("SCF105", diagnostic.GetProperty("code").GetString());
        Assert.Equal("schema-feature", diagnostic.GetProperty("category").GetString());
        Assert.Equal("PrecisionScale", diagnostic.GetProperty("kind").GetString());
        Assert.Contains("HasPrecision", diagnostic.GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, doc.RootElement.GetProperty("summary").GetProperty("codes").GetProperty("SCF105").GetInt32());
    }

}
