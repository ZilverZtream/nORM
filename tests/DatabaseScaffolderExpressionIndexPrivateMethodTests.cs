#nullable enable

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void BuildExpressionIndexConfigurations_SuppressesProviderSpecificAccessMethods()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Documents"] = "Document"
        };

        var ordinaryExpressionFeatures = Array.CreateInstance(featureType, 1);
        ordinaryExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LowerName",
            "CREATE INDEX \"IX_Documents_LowerName\" ON public.\"Documents\" USING btree (lower(\"Name\"))")!, 0);

        var ordinaryResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, ordinaryExpressionFeatures })!;

        var providerSpecificExpressionFeatures = Array.CreateInstance(featureType, 2);
        providerSpecificExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_Search",
            "CREATE INDEX \"IX_Documents_Search\" ON public.\"Documents\" USING gin (to_tsvector('simple'::regconfig, \"Name\"))")!, 0);
        providerSpecificExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ProviderSpecificIndex",
            "IX_Documents_Search",
            "gin index")!, 1);

        var providerSpecificResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, providerSpecificExpressionFeatures })!;

        var includedExpressionFeatures = Array.CreateInstance(featureType, 2);
        includedExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LowerName_Include",
            "CREATE INDEX \"IX_Documents_LowerName_Include\" ON public.\"Documents\" USING btree (lower(\"Name\")) INCLUDE (\"Score\")")!, 0);
        includedExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "IncludedColumnIndex",
            "IX_Documents_LowerName_Include",
            "PostgreSQL index with included columns")!, 1);

        var includedExpressionResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, includedExpressionFeatures })!;

        var descendingExpressionFeatures = Array.CreateInstance(featureType, 2);
        descendingExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LowerName_Desc",
            "CREATE INDEX \"IX_Documents_LowerName_Desc\" ON public.\"Documents\" USING btree (lower(\"Name\") DESC)")!, 0);
        descendingExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "DescendingIndex",
            "IX_Documents_LowerName_Desc",
            "PostgreSQL descending expression index key")!, 1);

        var descendingExpressionResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, descendingExpressionFeatures })!;

        var mySqlExpressionFeatures = Array.CreateInstance(featureType, 1);
        mySqlExpressionFeatures.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_MySqlExpression",
            "MySQL expression index; expression=(LOWER(`Name`)), `Score`; isUnique=true")!, 0);

        var mySqlExpressionResult = (System.Collections.ICollection)method.Invoke(
            null,
            new object[] { entityByTable, mySqlExpressionFeatures })!;

        Assert.Single((System.Collections.IEnumerable)ordinaryResult);
        Assert.Empty((System.Collections.IEnumerable)providerSpecificResult);
        Assert.Empty((System.Collections.IEnumerable)includedExpressionResult);
        var descendingExpression = Assert.Single((System.Collections.IEnumerable)descendingExpressionResult);
        Assert.NotNull(descendingExpression);
        Assert.Equal(
            "lower(\"Name\") DESC",
            descendingExpression.GetType().GetProperty("ExpressionSql")!.GetValue(descendingExpression));
        var mySqlExpression = Assert.Single((System.Collections.IEnumerable)mySqlExpressionResult);
        Assert.NotNull(mySqlExpression);
        Assert.Equal(
            "(LOWER(`Name`)), `Score`",
            mySqlExpression.GetType().GetProperty("ExpressionSql")!.GetValue(mySqlExpression));
        Assert.Equal(
            true,
            mySqlExpression.GetType().GetProperty("IsUnique")!.GetValue(mySqlExpression));
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ExtractsFilterAfterExpressionBody()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Documents"] = "Document"
        };
        var features = Array.CreateInstance(featureType, 2);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LiteralWhere",
            "CREATE INDEX \"IX_Documents_LiteralWhere\" ON public.\"Documents\" USING btree (strpos(\"Name\", ' WHERE '))")!, 0);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_LiteralWhere_Filtered",
            "CREATE INDEX \"IX_Documents_LiteralWhere_Filtered\" ON public.\"Documents\" USING btree (strpos(\"Name\", ' WHERE ')) WHERE \"Name\" IS NOT NULL")!, 1);

        var result = ((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, features })!)
            .Cast<object>()
            .ToDictionary(
                item => (string)item.GetType().GetProperty("Name")!.GetValue(item)!,
                StringComparer.Ordinal);

        Assert.Equal(2, result.Count);
        Assert.Equal(
            "strpos(\"Name\", ' WHERE ')",
            result["IX_Documents_LiteralWhere"].GetType().GetProperty("ExpressionSql")!.GetValue(result["IX_Documents_LiteralWhere"]));
        Assert.Null(result["IX_Documents_LiteralWhere"].GetType().GetProperty("FilterSql")!.GetValue(result["IX_Documents_LiteralWhere"]));
        Assert.Equal(
            "strpos(\"Name\", ' WHERE ')",
            result["IX_Documents_LiteralWhere_Filtered"].GetType().GetProperty("ExpressionSql")!.GetValue(result["IX_Documents_LiteralWhere_Filtered"]));
        Assert.Equal(
            "\"Name\" IS NOT NULL",
            result["IX_Documents_LiteralWhere_Filtered"].GetType().GetProperty("FilterSql")!.GetValue(result["IX_Documents_LiteralWhere_Filtered"]));
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ExtractsFilterAcrossWhitespace()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["main.Documents"] = "Document"
        };
        var features = Array.CreateInstance(featureType, 1);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "main.Documents",
            "ExpressionIndex",
            "IX_Documents_LowerName_Filtered",
            "CREATE INDEX \"IX_Documents_LowerName_Filtered\" ON \"Documents\" (lower(\"Name\"))\r\nWHERE \"Name\" IS NOT NULL")!, 0);

        var result = Assert.Single(((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, features })!)
            .Cast<object>());

        Assert.Equal(
            "lower(\"Name\")",
            result.GetType().GetProperty("ExpressionSql")!.GetValue(result));
        Assert.Equal(
            "\"Name\" IS NOT NULL",
            result.GetType().GetProperty("FilterSql")!.GetValue(result));
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_DetectsUniqueOnlyFromCreateIndexPrefix()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Documents"] = "Document"
        };
        var features = Array.CreateInstance(featureType, 2);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "IX_Documents_CreateUniqueLiteral",
            "CREATE INDEX \"IX_Documents_CreateUniqueLiteral\" ON public.\"Documents\" USING btree (strpos(\"Name\", 'CREATE UNIQUE'))")!, 0);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents",
            "ExpressionIndex",
            "UX_Documents_LowerName",
            "CREATE UNIQUE INDEX \"UX_Documents_LowerName\" ON public.\"Documents\" USING btree (lower(\"Name\"))")!, 1);

        var result = ((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, features })!)
            .Cast<object>()
            .ToDictionary(
                item => (string)item.GetType().GetProperty("Name")!.GetValue(item)!,
                StringComparer.Ordinal);

        Assert.False((bool)result["IX_Documents_CreateUniqueLiteral"].GetType().GetProperty("IsUnique")!.GetValue(result["IX_Documents_CreateUniqueLiteral"])!);
        Assert.True((bool)result["UX_Documents_LowerName"].GetType().GetProperty("IsUnique")!.GetValue(result["UX_Documents_LowerName"])!);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ParsesKeyListAfterQuotedTableName()
    {
        var scaffolder = typeof(DatabaseScaffolder);
        var featureType = scaffolder.GetNestedType("ScaffoldUnsupportedFeature", BindingFlags.NonPublic)!;
        var method = GetMethod(
            "BuildExpressionIndexConfigurations",
            new[] { typeof(IReadOnlyDictionary<string, string>), typeof(IEnumerable<>).MakeGenericType(featureType) });
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["public.Documents(Archive)"] = "DocumentArchive"
        };
        var features = Array.CreateInstance(featureType, 1);
        features.SetValue(Activator.CreateInstance(
            featureType,
            "public.Documents(Archive)",
            "ExpressionIndex",
            "IX_Documents_Archive_LowerName",
            "CREATE INDEX \"IX_Documents_Archive_LowerName\" ON public.\"Documents(Archive)\" USING btree (lower(\"Name\")) WHERE \"Name\" IS NOT NULL")!, 0);

        var result = Assert.Single(((System.Collections.IEnumerable)method.Invoke(
                null,
                new object[] { entityByTable, features })!)
            .Cast<object>());

        Assert.Equal(
            "lower(\"Name\")",
            result.GetType().GetProperty("ExpressionSql")!.GetValue(result));
        Assert.Equal(
            "\"Name\" IS NOT NULL",
            result.GetType().GetProperty("FilterSql")!.GetValue(result));
    }

}
