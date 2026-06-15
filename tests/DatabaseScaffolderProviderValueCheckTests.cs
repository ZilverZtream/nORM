#nullable enable

using System;
using System.Collections.Generic;
using System.Reflection;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Theory]
    [InlineData(@"enum('draft','it\'s','comma,value')", "Status", "Enum", "Status IN ('draft', 'it''s', 'comma,value')")]
    [InlineData(@"set('read\'write','admin')", "Flags", "Set", "Flags IN ('', 'read''write', 'admin', 'read''write,admin')")]
    public void TryBuildProviderValueCheckSql_EscapesMySqlQuotedValues(string detail, string column, string expectedKind, string expectedSql)
    {
        Assert.True(ScaffoldFeatureConfigurationBuilder.TryBuildProviderValueCheckSql(column, detail, out var checkKind, out var sql));
        Assert.Equal(expectedKind, checkKind);
        Assert.Equal(expectedSql, sql);
    }

    [Fact]
    public void TryBuildProviderValueCheckSql_RejectsMySqlSetValuesWithEscapedCommas()
    {
        Assert.False(ScaffoldFeatureConfigurationBuilder.TryBuildProviderValueCheckSql(
            "Flags",
            @"set('read\,write','admin')",
            out var checkKind,
            out var sql));
        Assert.Equal(string.Empty, checkKind);
        Assert.Equal(string.Empty, sql);
    }

    [Fact]
    public void BuildEnumCheckConstraintConfigurations_EmitsCheckForPostgresDomainEnum()
    {
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Customers"] = "Customer"
        };
        var propertiesByTable = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Customers"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Status"] = "Status"
            }
        };
        var features = new[]
        {
            new ScaffoldFeatureInput(
                0,
                new ScaffoldUnsupportedFeatureInfo(
                    "public.Customers",
                    "ProviderSpecificColumnType",
                    "Status",
                    "DOMAIN (public.customer_status_domain -> ENUM (public.customer_status: 'draft','active','archived'))"))
        };

        var result = ScaffoldFeatureConfigurationBuilder.BuildEnumCheckConstraintConfigurations(
            entityByTable,
            propertiesByTable,
            features);

        var check = Assert.Single(result);
        Assert.Equal("public.Customers", check.TableKey);
        Assert.Equal("Customer", check.EntityName);
        Assert.Equal("CK_Customer_Status_Enum", check.Name);
        Assert.Equal("Status IN ('draft', 'active', 'archived')", check.Sql);
    }

    [Fact]
    public void BuildCheckConstraintConfigurations_ReplacesSyntheticConstraintNameWithStableName()
    {
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["dbo.Orders"] = "Order"
        };
        var features = new[]
        {
            new ScaffoldFeatureInput(
                0,
                new ScaffoldUnsupportedFeatureInfo(
                    "dbo.Orders",
                    "CheckConstraint",
                    "CK__Orders__Amount__12345678",
                    "([Amount]>(0))")
                {
                    Metadata = new Dictionary<string, object?> { ["isSyntheticName"] = true }
                })
        };

        var result = ScaffoldFeatureConfigurationBuilder.BuildCheckConstraintConfigurations(entityByTable, features);

        var check = Assert.Single(result);
        var name = check.Name;
        Assert.StartsWith("CK_Order_", name, StringComparison.Ordinal);
        Assert.DoesNotContain("CK__Orders__", name, StringComparison.Ordinal);
        Assert.Equal("[Amount]>(0)", check.Sql);
    }

    [Fact]
    public void BuildFeatureConfigurations_KeepsMySqlOnUpdateTimestampAsProviderOwnedDefault()
    {
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Orders"] = "Order"
        };
        var propertiesByTable = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Orders"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["UpdatedAt"] = "UpdatedAt"
            }
        };
        var features = new[]
        {
            new ScaffoldFeatureInput(
                0,
                new ScaffoldUnsupportedFeatureInfo(
                    "Orders",
                    "Default",
                    "UpdatedAt",
                    "CURRENT_TIMESTAMP DEFAULT_GENERATED on update CURRENT_TIMESTAMP"))
        };

        var configurations = ScaffoldFeatureConfigurationBuilder.BuildFeatureConfigurations(
            features,
            entityByTable,
            propertiesByTable,
            new Dictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase));

        Assert.Empty(configurations.DefaultValuesByTable);
        Assert.Contains("Orders", configurations.ProviderSpecificDefaultTableKeys);
        Assert.Contains("Orders", configurations.ProviderOwnedWriteBlockedTableKeys);
        Assert.Empty(configurations.GeneratedFeatureIndexes);
    }

    [Fact]
    public void BuildFeatureConfigurations_PromotesSafePostgresNextvalDefaultAndKeepsTypeWritable()
    {
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Orders"] = "Order"
        };
        var propertiesByTable = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Orders"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["OrderNumber"] = "OrderNumber"
            }
        };
        var features = new[]
        {
            new ScaffoldFeatureInput(
                0,
                new ScaffoldUnsupportedFeatureInfo(
                    "public.Orders",
                    "Default",
                    "OrderNumber",
                    "nextval('public.order_numbers_seq'::regclass)"))
        };

        var configurations = ScaffoldFeatureConfigurationBuilder.BuildFeatureConfigurations(
            features,
            entityByTable,
            propertiesByTable,
            new Dictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase));

        Assert.Equal("nextval('public.order_numbers_seq'::regclass)", configurations.DefaultValuesByTable["public.Orders"]["OrderNumber"]);
        Assert.DoesNotContain("public.Orders", configurations.ProviderSpecificDefaultTableKeys);
        Assert.DoesNotContain("public.Orders", configurations.ProviderOwnedWriteBlockedTableKeys);
        Assert.Equal(new[] { 0 }, configurations.GeneratedFeatureIndexes);
    }

    [Theory]
    [InlineData("character varying(320)", "character varying(320)")]
    [InlineData("varchar(64)", "character varying(64)")]
    [InlineData("character(12)", "character(12)")]
    [InlineData("char(8)", "character(8)")]
    [InlineData("numeric(10,2)", "numeric(10,2)")]
    [InlineData("decimal(18, 4)", "numeric(18,4)")]
    [InlineData("numeric(18,,2)", "text")]
    [InlineData("numeric(18,)", "text")]
    [InlineData("numeric(,2)", "text")]
    [InlineData("varchar()", "text")]
    [InlineData("USER-DEFINED (citext)", "citext")]
    [InlineData("USER-DEFINED (uuid)", "uuid")]
    [InlineData("ARRAY (_int4)", "integer[]")]
    [InlineData("ARRAY (_text)", "text[]")]
    [InlineData("ARRAY (_bytea)", "bytea[]")]
    [InlineData("ARRAY (_timestamptz)", "timestamp with time zone[]")]
    [InlineData("ARRAY (_timetz)", "time with time zone[]")]
    [InlineData("ARRAY (varchar(64))", "character varying[]")]
    [InlineData("ARRAY (numeric(10,2))", "numeric[]")]
    [InlineData("integer[]", "integer[]")]
    [InlineData("int4[]", "integer[]")]
    [InlineData("varchar[]", "character varying[]")]
    [InlineData("bpchar[]", "character[]")]
    [InlineData("character varying(64)[]", "character varying[]")]
    [InlineData("numeric(10,2)[]", "numeric[]")]
    [InlineData("uuid[]", "uuid[]")]
    [InlineData("time with time zone[]", "time with time zone[]")]
    [InlineData("timetz[]", "time with time zone[]")]
    public void NormalizePostgresDomainProbeCastType_StaticAndDynamic_NormalizesSafeFacetsAndTextCastsMalformedTypes(string typeText, string expected)
    {
        var dynamicMethod = typeof(DynamicEntityTypeGenerator)
            .GetMethod("NormalizePostgresDomainProbeCastType", BindingFlags.NonPublic | BindingFlags.Static, null, new[] { typeof(string) }, null)
            ?? throw new MissingMethodException(nameof(DynamicEntityTypeGenerator), "NormalizePostgresDomainProbeCastType");

        Assert.Equal(expected, ScaffoldProviderSpecificTypeClassifier.NormalizePostgresDomainProbeCastType(typeText));
        Assert.Equal(expected, (string)dynamicMethod.Invoke(null, new object[] { typeText })!);
    }

    [Theory]
    [InlineData("USER-DEFINED (citext)", "citext")]
    [InlineData("USER-DEFINED (uuid)", "uuid")]
    [InlineData("USER-DEFINED (custom_payload)", "text")]
    public void TryGetPostgresSchemaProbeCastType_Static_PreservesSafeUdtsAndTextCastsUnsafe(string detail, string expected)
    {
        Assert.True(ScaffoldProviderSpecificTypeClassifier.TryGetPostgresSchemaProbeCastType(detail, out var castType));
        Assert.Equal(expected, castType);
    }
}
