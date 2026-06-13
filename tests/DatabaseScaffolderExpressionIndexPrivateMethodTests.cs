#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void BuildExpressionIndexConfigurations_SuppressesProviderSpecificAccessMethods()
    {
        var entityByTable = EntityMap();

        var ordinaryResult = BuildExpressionIndexes(
            entityByTable,
            ExpressionFeature(
                "IX_Documents_LowerName",
                "CREATE INDEX \"IX_Documents_LowerName\" ON public.\"Documents\" USING btree (lower(\"Name\"))"));

        var providerSpecificResult = BuildExpressionIndexes(
            entityByTable,
            ExpressionFeature(
                "IX_Documents_Search",
                "CREATE INDEX \"IX_Documents_Search\" ON public.\"Documents\" USING gin (to_tsvector('simple'::regconfig, \"Name\"))"),
            Feature("ProviderSpecificIndex", "IX_Documents_Search", "gin index"));

        var includedExpressionResult = BuildExpressionIndexes(
            entityByTable,
            ExpressionFeature(
                "IX_Documents_LowerName_Include",
                "CREATE INDEX \"IX_Documents_LowerName_Include\" ON public.\"Documents\" USING btree (lower(\"Name\")) INCLUDE (\"Score\")"),
            Feature("IncludedColumnIndex", "IX_Documents_LowerName_Include", "PostgreSQL index with included columns"));

        var postgresNullSemanticsResult = BuildExpressionIndexes(
            entityByTable,
            ExpressionFeature(
                "UX_Documents_LowerName_Nulls",
                "CREATE UNIQUE INDEX \"UX_Documents_LowerName_Nulls\" ON public.\"Documents\" USING btree (lower(\"Name\") NULLS FIRST) NULLS NOT DISTINCT"),
            Feature(
                "ProviderSpecificIndex",
                "UX_Documents_LowerName_Nulls",
                "PostgreSQL btree index with provider-specific key options; accessMethod=btree; hasNullsNotDistinct=true; hasNonDefaultOperatorClass=false; hasIndexCollation=false; hasNonDefaultNullOrdering=true; indexSql=CREATE UNIQUE INDEX \"UX_Documents_LowerName_Nulls\" ON public.\"Documents\" USING btree (lower(\"Name\") NULLS FIRST) NULLS NOT DISTINCT"));

        var descendingExpressionResult = BuildExpressionIndexes(
            entityByTable,
            ExpressionFeature(
                "IX_Documents_LowerName_Desc",
                "CREATE INDEX \"IX_Documents_LowerName_Desc\" ON public.\"Documents\" USING btree (lower(\"Name\") DESC)"),
            Feature("DescendingIndex", "IX_Documents_LowerName_Desc", "PostgreSQL descending expression index key"));

        var mySqlExpressionResult = BuildExpressionIndexes(
            entityByTable,
            ExpressionFeature(
                "IX_Documents_MySqlExpression",
                "MySQL expression index; expression=(LOWER(`Name`)), `Score`; isUnique=true"));

        Assert.Single(ordinaryResult);
        Assert.Empty(providerSpecificResult);
        Assert.Equal(new[] { "Score" }, Assert.Single(includedExpressionResult).IncludedColumnNames);

        var postgresNullSemantics = Assert.Single(postgresNullSemanticsResult);
        Assert.Equal("lower(\"Name\")", postgresNullSemantics.ExpressionSql);
        Assert.Equal(IndexNullSortOrder.First, postgresNullSemantics.NullSortOrder);
        Assert.True(postgresNullSemantics.NullsNotDistinct);

        Assert.Equal("lower(\"Name\") DESC", Assert.Single(descendingExpressionResult).ExpressionSql);

        var mySqlExpression = Assert.Single(mySqlExpressionResult);
        Assert.Equal("(LOWER(`Name`)), `Score`", mySqlExpression.ExpressionSql);
        Assert.True(mySqlExpression.IsUnique);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ExtractsFilterAfterExpressionBody()
    {
        var result = BuildExpressionIndexes(
                EntityMap(),
                ExpressionFeature(
                    "IX_Documents_LiteralWhere",
                    "CREATE INDEX \"IX_Documents_LiteralWhere\" ON public.\"Documents\" USING btree (strpos(\"Name\", ' WHERE '))"),
                ExpressionFeature(
                    "IX_Documents_LiteralWhere_Filtered",
                    "CREATE INDEX \"IX_Documents_LiteralWhere_Filtered\" ON public.\"Documents\" USING btree (strpos(\"Name\", ' WHERE ')) WHERE \"Name\" IS NOT NULL"))
            .ToDictionary(item => item.Name, StringComparer.Ordinal);

        Assert.Equal(2, result.Count);
        Assert.Equal("strpos(\"Name\", ' WHERE ')", result["IX_Documents_LiteralWhere"].ExpressionSql);
        Assert.Null(result["IX_Documents_LiteralWhere"].FilterSql);
        Assert.Equal("strpos(\"Name\", ' WHERE ')", result["IX_Documents_LiteralWhere_Filtered"].ExpressionSql);
        Assert.Equal("\"Name\" IS NOT NULL", result["IX_Documents_LiteralWhere_Filtered"].FilterSql);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_IgnoresPostgresDollarQuotedKeywordsAndParentheses()
    {
        var result = Assert.Single(BuildExpressionIndexes(
            EntityMap(),
            ExpressionFeature(
                "IX_Documents_DollarQuoted_Filtered",
                "CREATE INDEX \"IX_Documents_DollarQuoted_Filtered\" ON public.\"Documents\" USING btree (regexp_replace(\"Name\", $tag$) WHERE ($tag$, '', 'g')) WHERE \"Name\" IS NOT NULL")));

        Assert.Equal("regexp_replace(\"Name\", $tag$) WHERE ($tag$, '', 'g')", result.ExpressionSql);
        Assert.Equal("\"Name\" IS NOT NULL", result.FilterSql);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ExtractsFilterAcrossWhitespace()
    {
        var result = Assert.Single(BuildExpressionIndexes(
            EntityMap("main.Documents"),
            new DatabaseScaffolder.ScaffoldUnsupportedFeature(
                "main.Documents",
                "ExpressionIndex",
                "IX_Documents_LowerName_Filtered",
                "CREATE INDEX \"IX_Documents_LowerName_Filtered\" ON \"Documents\" (lower(\"Name\"))\r\nWHERE \"Name\" IS NOT NULL")));

        Assert.Equal("lower(\"Name\")", result.ExpressionSql);
        Assert.Equal("\"Name\" IS NOT NULL", result.FilterSql);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_TrimsFilterStatementTerminator()
    {
        var result = Assert.Single(BuildExpressionIndexes(
            EntityMap(),
            ExpressionFeature(
                "IX_Documents_LiteralTerminator_Filtered",
                "CREATE INDEX \"IX_Documents_LiteralTerminator_Filtered\" ON public.\"Documents\" USING btree (strpos(\"Name\", ';')) WHERE \"Name\" <> ';';")));

        Assert.Equal("strpos(\"Name\", ';')", result.ExpressionSql);
        Assert.Equal("\"Name\" <> ';'", result.FilterSql);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ParsesIndexFacetsAcrossProviderWhitespace()
    {
        const string indexSql = "CREATE INDEX \"IX_Documents_LowerName_Include\"\r\nON\tpublic.\"Documents\"\r\nUSING btree\r\n(lower(\"Name\"))\r\nINCLUDE\r\n(\"Score\", \"Rank\")\r\nWHERE \"Name\" IS NOT NULL";
        var result = Assert.Single(BuildExpressionIndexes(
            EntityMap(),
            ExpressionFeature("IX_Documents_LowerName_Include", indexSql),
            Feature("IncludedColumnIndex", "IX_Documents_LowerName_Include", indexSql)));

        Assert.Equal("lower(\"Name\")", result.ExpressionSql);
        Assert.Equal("\"Name\" IS NOT NULL", result.FilterSql);
        Assert.Equal(new[] { "Score", "Rank" }, result.IncludedColumnNames);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_DetectsUniqueOnlyFromCreateIndexPrefix()
    {
        var result = BuildExpressionIndexes(
                EntityMap(),
                ExpressionFeature(
                    "IX_Documents_CreateUniqueLiteral",
                    "CREATE INDEX \"IX_Documents_CreateUniqueLiteral\" ON public.\"Documents\" USING btree (strpos(\"Name\", 'CREATE UNIQUE'))"),
                ExpressionFeature(
                    "UX_Documents_LowerName",
                    "CREATE UNIQUE INDEX \"UX_Documents_LowerName\" ON public.\"Documents\" USING btree (lower(\"Name\"))"))
            .ToDictionary(item => item.Name, StringComparer.Ordinal);

        Assert.False(result["IX_Documents_CreateUniqueLiteral"].IsUnique);
        Assert.True(result["UX_Documents_LowerName"].IsUnique);
    }

    [Fact]
    public void BuildExpressionIndexConfigurations_ParsesKeyListAfterQuotedTableName()
    {
        var result = Assert.Single(BuildExpressionIndexes(
            EntityMap("public.Documents(Archive)", "DocumentArchive"),
            new DatabaseScaffolder.ScaffoldUnsupportedFeature(
                "public.Documents(Archive)",
                "ExpressionIndex",
                "IX_Documents_Archive_LowerName",
                "CREATE INDEX \"IX_Documents_Archive_LowerName\" ON public.\"Documents(Archive)\" USING btree (lower(\"Name\")) WHERE \"Name\" IS NOT NULL")));

        Assert.Equal("lower(\"Name\")", result.ExpressionSql);
        Assert.Equal("\"Name\" IS NOT NULL", result.FilterSql);
    }

    private static IReadOnlyDictionary<string, string> EntityMap(
        string tableKey = "public.Documents",
        string entityName = "Document")
        => new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [tableKey] = entityName
        };

    private static DatabaseScaffolder.ScaffoldUnsupportedFeature ExpressionFeature(string name, string detail)
        => Feature("ExpressionIndex", name, detail);

    private static DatabaseScaffolder.ScaffoldUnsupportedFeature Feature(string kind, string name, string detail)
        => new("public.Documents", kind, name, detail);

    private static IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> BuildExpressionIndexes(
        IReadOnlyDictionary<string, string> entityByTable,
        params DatabaseScaffolder.ScaffoldUnsupportedFeature[] features)
        => ScaffoldFeatureConfigurationAdapter.BuildExpressionIndexConfigurations(entityByTable, features);
}
