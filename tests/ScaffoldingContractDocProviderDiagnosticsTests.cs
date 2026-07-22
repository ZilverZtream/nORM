using System;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public partial class ScaffoldingContractDocTests
{
    [Fact]
    public void Doc_and_source_pin_provider_owned_temporal_and_view_diagnostics()
    {
        var doc = ReadDoc();
        var source = ReadStaticEntityScaffoldSource();
        var joinDiagnosticBuilderSource = ReadJoinTableDiagnosticSource();
        var postgresUnsupportedSource = ReadPostgresUnsupportedFeatureSource();
        var sqlServerUnsupportedSource = ReadSqlServerUnsupportedFeatureSource();
        var mySqlUnsupportedSource = ReadMySqlUnsupportedFeatureSource();
        var skippedDiscoverySource = ReadSkippedObjectDiscoverySource();
        var skippedMetadataSource = string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.Events.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.QueryObjects.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.Sequences.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.Synonyms.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldSkippedObjectMetadataBuilder.Values.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineMetadataBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineMetadataBuilder.Parameters.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldRoutineMetadataBuilder.Results.cs"));
        var columnDiscoverySource = ReadColumnDiscoverySource();
        var diagnosticsWriterSource = ReadDiagnosticsWriterSource();
        var routineStubWriterSource = ReadRoutineStubWriterSource();
        var parserSource = ReadSemicolonParserSource();
        var sqlMetadataParserSource = ReadSqlMetadataParserSource();
        var unsupportedMetadataSource = ReadUnsupportedFeatureMetadataSource();
        var providerTypeClassifierSource = ReadProviderSpecificTypeClassifierSource();
        var normalizedDoc = NormalizeDocWhitespace(doc);
        Assert.Contains("SQL Server provider-native temporal tables", doc, StringComparison.Ordinal);
        Assert.Contains("views", doc, StringComparison.Ordinal);
        Assert.Contains("virtual tables", doc, StringComparison.Ordinal);
        Assert.Contains("shadow tables", doc, StringComparison.Ordinal);
        Assert.Contains("routines", doc, StringComparison.Ordinal);
        Assert.Contains("sequences", doc, StringComparison.Ordinal);
        Assert.Contains("synonyms", doc, StringComparison.Ordinal);
        Assert.Contains("materialized views", doc, StringComparison.Ordinal);
        Assert.Contains("events", doc, StringComparison.Ordinal);
        Assert.Contains("skipped", doc, StringComparison.Ordinal);
        Assert.Contains("tables without primary keys", doc, StringComparison.Ordinal);
        Assert.Contains("HasCollation", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific column types", doc, StringComparison.Ordinal);
        Assert.Contains("decimal precision/scale preservation", doc, StringComparison.Ordinal);
        Assert.Contains("Column(TypeName", doc, StringComparison.Ordinal);
        Assert.Contains("schema snapshot precision parsing", doc, StringComparison.Ordinal);
        Assert.Contains("domain-wrapped numeric type text", doc, StringComparison.Ordinal);
        Assert.Contains("provider metadata-backed identity", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL identity/serial", doc, StringComparison.Ordinal);
        Assert.Contains("owned sequences", doc, StringComparison.Ordinal);
        Assert.Contains("treated as identity metadata", doc, StringComparison.Ordinal);
        Assert.Contains("Independent PostgreSQL `nextval('sequence'::regclass)` defaults", doc, StringComparison.Ordinal);
        Assert.Contains("dependent or principal table is intentionally filtered out", doc, StringComparison.Ordinal);
        Assert.Contains("FilterForeignKeysToScaffoldedTables", source, StringComparison.Ordinal);
        Assert.Contains("rowversion/timestamp", doc, StringComparison.Ordinal);
        Assert.Contains("identity seed/increment", doc, StringComparison.Ordinal);
        Assert.Contains("SQL Server `PERSISTED`", normalizedDoc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL stored generated", normalizedDoc, StringComparison.Ordinal);
        Assert.Contains("MySQL `VIRTUAL GENERATED`/`STORED", normalizedDoc, StringComparison.Ordinal);
        Assert.Contains("unrecognized FK referential actions", doc, StringComparison.Ordinal);
        Assert.Contains("relationships that do not target the generated principal primary key or an", doc, StringComparison.Ordinal);
        Assert.Contains("exact ordered unfiltered unique index", doc, StringComparison.Ordinal);
        Assert.Contains("HasNonNullableColumns", source, StringComparison.Ordinal);
        Assert.Contains("ReferencesScaffoldablePrincipalKey", source, StringComparison.Ordinal);
        Assert.Contains("ReferencesUniqueIndex", source, StringComparison.Ordinal);
        Assert.Contains("ReferencesUniqueIndex(foreignKeyGroup, primaryKeyColumnsByTable, indexes)", source, StringComparison.Ordinal);
        Assert.Contains("HasExactUniqueColumnSet(indexes, tableKey, foreignKeyColumnSet)", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("HasExactOrderedUniqueIndex(indexes, principalKey, principalColumns)", joinDiagnosticBuilderSource, StringComparison.Ordinal);
        Assert.Contains("GetIdentityColumnNamesAsync", source, StringComparison.Ordinal);
        Assert.Contains("auto_increment", columnDiscoverySource, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("nextval(%", columnDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("pg_get_serial_sequence(format('%I.%I', c.table_schema, c.table_name), c.column_name)", columnDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("dependency.deptype IN ('a', 'i')", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("pg_get_serial_sequence(format('%I.%I', c.table_schema, c.table_name), c.column_name)", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("temporal_type <> 0", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("skippedDatabaseObjects", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("MissingPrimaryKey", source, StringComparison.Ordinal);
        Assert.Contains("Collation", source, StringComparison.Ordinal);
        Assert.Contains("ProviderSpecificColumnType", source, StringComparison.Ordinal);
        Assert.Contains("PrecisionScale", routineStubWriterSource, StringComparison.Ordinal);
        Assert.Contains("EndsWithDelimitedTypeName", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("RowVersion", source, StringComparison.Ordinal);
        Assert.Contains("IdentityStrategy", source, StringComparison.Ordinal);
        Assert.Contains("TryTrimTrailingComputedStorageToken", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("quote == '[' ? ']'", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("TryConsumeSqlKeyword(candidate, ref keywordIndex, \"CHECK\")", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("TryConsumeSqlKeyword(detail, ref keywordIndex, \"IDENTITY\")", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("cc.is_persisted", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("generation_expression || ' STORED'", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("stored generated", mySqlUnsupportedSource, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("virtual generated", mySqlUnsupportedSource, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ReferentialAction", source, StringComparison.Ordinal);
        Assert.Contains("RelationshipPrincipalKey", source, StringComparison.Ordinal);
        Assert.Contains("'Routine'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("outputParameters", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("parameterModes", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("pa.is_output = 1 THEN 'OUT'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("decimal precision/scale", doc, StringComparison.Ordinal);
        Assert.Contains("decimal precision and optional scale", doc, StringComparison.Ordinal);
        Assert.Contains("exclude provider metadata return rows from callable input counts", doc, StringComparison.Ordinal);
        Assert.Contains("p.parameter_mode IS NOT NULL", skippedDiscoverySource, StringComparison.Ordinal);
        // Parameter modes are aggregated in declaration order via a compat-safe FOR XML PATH
        // subquery (STRING_AGG ... WITHIN GROUP requires database compatibility level >= 110, so
        // it cannot be used to scaffold SQL Server databases running at 2008/2012 compat).
        Assert.Contains("ORDER BY pa.parameter_id", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("FOR XML PATH('')", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("ty.name", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("base_ty.name", sqlServerUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("p.data_type", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("domain_name", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("domain_schema", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("column_type", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("unsigned", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("legacy display widths", doc, StringComparison.Ordinal);
        Assert.Contains("NormalizeMySqlUnsignedTypeDetail", providerTypeClassifierSource, StringComparison.Ordinal);
        Assert.Contains("BuildSkippedObjectMetadata", source, StringComparison.Ordinal);
        Assert.Contains("BuildSequenceMetadata", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("BuildSynonymMetadata", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("BuildEventMetadata", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("BuildQueryObjectMetadata", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("BuildUnsupportedFeatureMetadata", source, StringComparison.Ordinal);
        Assert.Contains("ScaffoldUnsupportedFeatureMetadataBuilder.BuildMetadata", source, StringComparison.Ordinal);
        Assert.Contains("TryParseRelationshipPrincipalKeyDetail", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("TryParseReferentialActionDetail", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("InferSqliteVirtualTableShadowOwner", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("ParseRoutineSemicolonValues", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldSemicolonParser.ParseRoutine", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("SelectBestOrderedSemicolonValueMarkers", parserSource, StringComparison.Ordinal);
        Assert.Contains("SelectOrderedSemicolonValueMarkers", parserSource, StringComparison.Ordinal);
        Assert.Contains("GetRoutineSemicolonValueKeyOrders", parserSource, StringComparison.Ordinal);
        Assert.Contains("IsKnownSemicolonValueKey", parserSource, StringComparison.Ordinal);
        Assert.Contains("ParseRoutineParameters", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("TryParseRoutineParameterMode", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("trimmed.Length - 1", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("SplitRoutineResultColumns", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("TryParseRoutineResultColumnParts", skippedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("information_schema.parameters", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("specific_name", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("RIGHT(r.specific_name, LENGTH(routine_proc.oid::text) + 1)", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("routine_proc.prokind IN ('f', 'p')", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("extension_dependency.refclassid = 'pg_catalog.pg_extension'::regclass", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("extension_dependency.deptype = 'e'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'Sequence'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'VirtualTable'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'VirtualTableShadow'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'Synonym'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'MaterializedView'", skippedDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("'Event'", skippedDiscoverySource, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_source_pin_provider_specific_index_diagnostics()
    {
        var doc = ReadDoc();
        var source = ReadStaticEntityScaffoldSource();
        var diagnosticsWriterSource = ReadDiagnosticsWriterSource();
        var expressionIndexConfigurationBuilderSource = string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldExpressionIndexConfigurationBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldExpressionIndexConfigurationBuilder.Planning.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldExpressionIndexConfigurationBuilder.Facets.cs"));
        var indexDiscoverySource = ReadIndexDiscoverySource();
        var sqlMetadataParserSource = ReadSqlMetadataParserSource();
        var postgresUnsupportedSource = ReadPostgresUnsupportedFeatureSource();
        var mySqlUnsupportedSource = ReadMySqlUnsupportedFeatureSource();
        var sqliteUnsupportedSource = ReadSqliteUnsupportedFeatureSource();
        var unsupportedMetadataSource = ReadUnsupportedFeatureMetadataSource();

        Assert.Contains("filtered/partial", doc, StringComparison.Ordinal);
        Assert.Contains("filtered/partial predicates for the same", doc, StringComparison.Ordinal);
        Assert.Contains("expression", doc, StringComparison.Ordinal);
        Assert.Contains("included-column", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite attached-schema partial index predicates", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL prefix indexes", doc, StringComparison.Ordinal);
        Assert.Contains("descending", doc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PartialIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("ExpressionIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("IncludedColumnIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("DescendingIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("PrefixIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("ProviderSpecificIndex", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("supported FK referential actions are emitted", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("unrecognized/provider-specific FK referential action tokens", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("Ordinary SQL Server/PostgreSQL included-column indexes are emitted with IndexAttribute.IsIncluded", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("Ordinary SQL Server/PostgreSQL/SQLite filtered and partial column indexes are emitted with IndexAttribute.FilterSql", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("supported expression-index predicates are emitted with HasExpressionIndex filter metadata", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("SQLite, ordinary PostgreSQL B-tree, and MySQL expression indexes are emitted with HasExpressionIndex", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("including supported filters, included columns, PostgreSQL null ordering, and NULLS NOT DISTINCT uniqueness", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("v1 scaffolding emits only key-column index metadata", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("v1 scaffolding emits only provider-neutral column indexes", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("v1 scaffolding emits portable B-tree/rowstore column-index metadata only", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.DoesNotContain("non-default FK referential actions", diagnosticsWriterSource, StringComparison.Ordinal);
        Assert.Contains("ExtractCreateIndexWhereClause(feature.Detail)", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("IsCreateIndexUnique(feature.Detail)", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("ExtractCreateIndexIncludedColumnNames", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("TryApplyProviderSpecificExpressionIndexFacets", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("providerSpecificIndexes", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("includedColumnIndexes", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("FindCreateIndexKeyListOpen", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("FindSqlKeywordOutsideQuotes", sqlMetadataParserSource, StringComparison.Ordinal);
        Assert.Contains("GetSqliteIndexFilterSqlAsync(connection, provider, table.Schema, name)", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("index_xinfo", sqliteUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("\"PartialIndex\"", sqliteUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("\"ExpressionIndex\"", sqliteUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("\"DescendingIndex\"", sqliteUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("i.type IN (1, 2)", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("am.amname = 'btree'", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("ix.indoption[key.ord - 1] & 1", indexDiscoverySource, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_get_indexdef(ix.indexrelid) ILIKE '% DESC%'", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("UPPER(COALESCE(NULLIF(s.index_type, ''), 'BTREE')) = 'BTREE'", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("bad.sub_part IS NOT NULL", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("bad.sub_part < bad_col.character_maximum_length", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("sub_part IS NOT NULL", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("SHOW INDEX FROM", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("ReaderHasColumn(reader, \"Expression\")", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("MySQL expression index", mySqlUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("expressionIndexKeys", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("!string.IsNullOrWhiteSpace(index.FilterSql)", source, StringComparison.Ordinal);
        Assert.Contains("Mixed functional indexes are not partially emitted", doc, StringComparison.Ordinal);
        Assert.Contains("ordinary PostgreSQL B-tree expression indexes", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL expression indexes exposed by `SHOW INDEX`", doc, StringComparison.Ordinal);
        Assert.Contains("provider-specific access methods, non-default operator classes, or index collations", doc, StringComparison.Ordinal);
        Assert.Contains("`NULLS NOT DISTINCT` unique column indexes are preserved", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL B-tree expression indexes with DDL-exposed", doc, StringComparison.Ordinal);
        Assert.Contains("including descending expression keys, filtered/partial predicates", doc, StringComparison.Ordinal);
        Assert.Contains("`NULLS NOT DISTINCT` uniqueness are emitted with expanded", doc, StringComparison.Ordinal);
        Assert.Contains("includedColumnNames", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
        Assert.Contains("pg_get_indexdef(ix.indexrelid)::text", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("provider-specific B-tree operator classes/collations", doc, StringComparison.Ordinal);
        Assert.Contains("expression indexes with non-default operator classes or index collations", doc, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN pg_attribute option_att", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("option_att.attnum IS NOT NULL", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("option_opclass.opcdefault = false", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("ix.indcollation[option_key.ord - 1] <> option_att.attcollation", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL B-tree indexes with non-default", doc, StringComparison.Ordinal);
        Assert.Contains("with non-default `NULLS FIRST/LAST` ordering", doc, StringComparison.Ordinal);
        Assert.Contains("indnullsnotdistinct", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("representable PostgreSQL B-tree expression-index null ordering and `NULLS NOT DISTINCT` uniqueness are emitted through `HasExpressionIndex`", doc, StringComparison.Ordinal);
        Assert.Contains("hasNullsNotDistinct", postgresUnsupportedSource, StringComparison.Ordinal);
        Assert.Contains("NullsNotDistinct", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("IndexNullSortOrder", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("ParseIndexNullSortOrder", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("NullSortOrder", indexDiscoverySource, StringComparison.Ordinal);
        Assert.Contains("hasNonDefaultOperatorClass", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("hasIndexCollation", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.Contains("hasNonDefaultNullOrdering", unsupportedMetadataSource, StringComparison.Ordinal);
        Assert.DoesNotContain("unrepresentableExpressionIndexes", expressionIndexConfigurationBuilderSource, StringComparison.Ordinal);
    }
}
