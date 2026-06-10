#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace nORM.Scaffolding
{
    internal static class ScaffoldDiagnosticsWriter
    {
        public static void AppendMarkdown(
            StringBuilder sb,
            IReadOnlyList<ScaffoldCompositeForeignKeyDiagnosticInfo> compositeForeignKeys,
            IReadOnlyList<ScaffoldPossibleJoinTableDiagnosticInfo> possibleJoinTables,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects)
        {
            sb.AppendLine("# nORM Scaffold Warnings");
            sb.AppendLine();
            sb.AppendLine("The scaffolder detected database features that were not converted into runnable nORM model code.");
            sb.AppendLine("Review these items before using the generated model for migrations or navigation queries.");

            AppendCompositeForeignKeys(sb, compositeForeignKeys);
            AppendPossibleJoinTables(sb, possibleJoinTables);
            AppendUnsupportedFeatures(sb, unsupportedFeatures);
            AppendSkippedObjects(sb, skippedObjects);
        }

        public static string WriteJson(
            IReadOnlyList<ScaffoldCompositeForeignKeyDiagnosticInfo> compositeForeignKeys,
            IReadOnlyList<ScaffoldPossibleJoinTableDiagnosticInfo> possibleJoinTables,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects)
        {
            var compositeForeignKeyItems = compositeForeignKeys
                .Select(foreignKey => new
                {
                    code = CodeForCompositeForeignKey(),
                    severity = Severity(),
                    category = CategoryForCompositeForeignKey(),
                    constraint = foreignKey.ConstraintName,
                    dependentTable = foreignKey.DependentTable,
                    dependentColumns = foreignKey.DependentColumns,
                    principalTable = foreignKey.PrincipalTable,
                    principalColumns = foreignKey.PrincipalColumns,
                    metadata = foreignKey.Metadata,
                    suggestedAction = SuggestedActionForCompositeForeignKey()
                })
                .ToArray();

            var possibleJoinTableItems = possibleJoinTables
                .Select(table => new
                {
                    code = CodeForPossibleJoinTable(),
                    severity = Severity(),
                    category = CategoryForPossibleJoinTable(),
                    table = table.TableKey,
                    principalTables = table.PrincipalTables,
                    constraints = table.ConstraintNames,
                    reasons = table.Reasons,
                    metadata = table.Metadata,
                    suggestedAction = SuggestedActionForPossibleJoinTable()
                })
                .ToArray();

            var providerOwnedSchemaFeatures = unsupportedFeatures
                .OrderBy(f => f.TableKey, StringComparer.Ordinal)
                .ThenBy(f => f.Kind, StringComparer.Ordinal)
                .ThenBy(f => f.Name, StringComparer.Ordinal)
                .Select(feature => new
                {
                    code = CodeForUnsupportedFeature(feature.Kind),
                    severity = Severity(),
                    category = CategoryForUnsupportedFeature(feature.Kind),
                    kind = feature.Kind,
                    table = feature.TableKey,
                    name = feature.Name,
                    detail = feature.Detail,
                    metadata = feature.Metadata ?? new Dictionary<string, object?>(0, StringComparer.Ordinal),
                    suggestedAction = SuggestedActionForUnsupportedFeature(feature.Kind)
                })
                .ToArray();

            var skippedDatabaseObjects = skippedObjects
                .OrderBy(o => TableKey(o.Schema, o.Name), StringComparer.Ordinal)
                .ThenBy(o => o.Kind, StringComparer.Ordinal)
                .Select(obj => new
                {
                    code = CodeForSkippedObject(obj.Kind),
                    severity = Severity(),
                    category = CategoryForSkippedObject(obj.Kind),
                    kind = obj.Kind,
                    name = TableKey(obj.Schema, obj.Name),
                    detail = obj.Detail,
                    metadata = obj.Metadata ?? new Dictionary<string, object?>(0, StringComparer.Ordinal),
                    suggestedAction = SuggestedActionForSkippedObject(obj.Kind)
                })
                .ToArray();

            var diagnosticCodes = compositeForeignKeyItems
                .Select(item => item.code)
                .Concat(possibleJoinTableItems.Select(item => item.code))
                .Concat(providerOwnedSchemaFeatures.Select(item => item.code))
                .Concat(skippedDatabaseObjects.Select(item => item.code))
                .GroupBy(code => code, StringComparer.Ordinal)
                .OrderBy(group => group.Key, StringComparer.Ordinal)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

            var diagnosticCategories = compositeForeignKeyItems
                .Select(item => item.category)
                .Concat(possibleJoinTableItems.Select(item => item.category))
                .Concat(providerOwnedSchemaFeatures.Select(item => item.category))
                .Concat(skippedDatabaseObjects.Select(item => item.category))
                .GroupBy(category => category, StringComparer.Ordinal)
                .OrderBy(group => group.Key, StringComparer.Ordinal)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

            return JsonSerializer.Serialize(
                new
                {
                    version = 1,
                    summary = new
                    {
                        totalWarnings = compositeForeignKeyItems.Length
                            + possibleJoinTableItems.Length
                            + providerOwnedSchemaFeatures.Length
                            + skippedDatabaseObjects.Length,
                        sectionCounts = new
                        {
                            compositeForeignKeys = compositeForeignKeyItems.Length,
                            possibleManyToManyJoinTables = possibleJoinTableItems.Length,
                            providerOwnedSchemaFeatures = providerOwnedSchemaFeatures.Length,
                            skippedDatabaseObjects = skippedDatabaseObjects.Length
                        },
                        codes = diagnosticCodes,
                        categories = diagnosticCategories
                    },
                    compositeForeignKeys = compositeForeignKeyItems,
                    possibleManyToManyJoinTables = possibleJoinTableItems,
                    providerOwnedSchemaFeatures,
                    skippedDatabaseObjects
                },
                new JsonSerializerOptions { WriteIndented = true });
        }

        public static string Severity()
            => "Warning";

        public static string CodeForCompositeForeignKey()
            => "SCF001";

        public static string CategoryForCompositeForeignKey()
            => "relationship";

        public static string CodeForPossibleJoinTable()
            => "SCF002";

        public static string CategoryForPossibleJoinTable()
            => "many-to-many";

        public static string CodeForUnsupportedFeature(string kind)
            => kind switch
            {
                "Default" => "SCF100",
                "Computed" => "SCF101",
                "CheckConstraint" => "SCF102",
                "Collation" => "SCF103",
                "ProviderSpecificColumnType" => "SCF104",
                "ReferentialAction" => "SCF106",
                "RelationshipPrincipalKey" => "SCF107",
                "RelationshipDependentKey" => "SCF118",
                "RowVersion" => "SCF108",
                "IdentityStrategy" => "SCF109",
                "Trigger" => "SCF110",
                "PartialIndex" => "SCF111",
                "ExpressionIndex" => "SCF112",
                "IncludedColumnIndex" => "SCF113",
                "DescendingIndex" => "SCF114",
                "TemporalTable" => "SCF115",
                "MissingPrimaryKey" => "SCF116",
                "PrefixIndex" => "SCF117",
                "ProviderSpecificIndex" => "SCF119",
                _ => "SCF199"
            };

        public static string CategoryForUnsupportedFeature(string kind)
            => kind switch
            {
                "ReferentialAction" or "RelationshipPrincipalKey" or "RelationshipDependentKey" => "relationship",
                "PartialIndex" or "ExpressionIndex" or "IncludedColumnIndex" or "DescendingIndex" or "PrefixIndex" or "ProviderSpecificIndex" => "index",
                "Trigger" or "TemporalTable" => "database-object",
                "MissingPrimaryKey" => "table-shape",
                _ => "schema-feature"
            };

        public static string CodeForSkippedObject(string kind)
            => kind switch
            {
                "View" => "SCF200",
                "Routine" => "SCF201",
                "Sequence" => "SCF202",
                "Synonym" => "SCF203",
                "MaterializedView" => "SCF204",
                "Event" => "SCF205",
                "VirtualTable" => "SCF206",
                "VirtualTableShadow" => "SCF207",
                _ => "SCF299"
            };

        public static string CategoryForSkippedObject(string kind)
            => kind switch
            {
                "View" or "MaterializedView" => "query-object",
                "Routine" or "Event" => "routine",
                "Sequence" => "key-generation",
                "VirtualTable" or "VirtualTableShadow" => "virtual-table",
                _ => "database-object"
            };

        public static string SuggestedActionForCompositeForeignKey()
            => "Keep scalar columns and add the composite relationship manually, or simplify the relationship to a single-column surrogate key before relying on generated navigations.";

        public static string SuggestedActionForPossibleJoinTable()
            => "If this is a safe pure join table, verify all FK columns are NOT NULL, both FKs target generated primary keys or exact ordered unfiltered unique indexes, and the bridge uses either an FK-column primary key or a generated surrogate primary key plus an exact unfiltered unique index over the FK columns; then use the generated UsingTable mapping. Keep payload, duplicate-pair, or domain-behavior bridges as explicit join entities.";

        public static string SuggestedActionForUnsupportedFeature(string kind)
            => kind switch
            {
                "Default" => "Generated code marks this type with [ReadOnlyEntity]. Move default semantics into application/model configuration, convert the default to emitted HasDefaultValueSql metadata, or keep provider DDL in migrations with a hand-modeled writable type.",
                "Computed" => "Use explicit HasComputedColumnSql model configuration or keep the provider-specific generated expression in migrations.",
                "CheckConstraint" => "Use explicit HasCheckConstraint model configuration or keep the provider-specific CHECK predicate in migrations.",
                "Collation" => "Keep collation-sensitive behavior in provider migrations and add explicit application/query tests before relying on generated code for comparisons or ordering.",
                "ProviderSpecificColumnType" => "Keep this provider-specific type behind explicit provider migrations/converters or remodel it to a portable CLR/database shape before claiming provider mobility.",
                "ReferentialAction" => "Generated navigations are suppressed for this FK. Review the provider-specific referential action token and add explicit relationship configuration only after preserving its semantics.",
                "RelationshipPrincipalKey" => "Add a primary key or exact ordered unfiltered unique index for the referenced principal columns, or configure the relationship manually before relying on generated navigations.",
                "RelationshipDependentKey" => "Add a primary key to the dependent table before relying on generated navigations/includes, or keep the keyless type read-only and configure explicit query projections.",
                "RowVersion" => "Keep provider-managed rowversion/timestamp semantics in migrations; scaffolded code marks the column as [Timestamp] and database-generated but cannot recreate provider DDL.",
                "IdentityStrategy" => "Parsed SQL Server IDENTITY(seed, increment) metadata is scaffolded into HasIdentityOptions; unparsed provider-specific identity strategies make the generated type [ReadOnlyEntity] until key generation is hand-modeled.",
                "Trigger" => "Generated code marks this type with [ReadOnlyEntity]. Keep the trigger in provider migrations and add an explicit hand-modeled writable type only after testing side effects nORM cannot infer.",
                "PartialIndex" => "Keep the filtered/partial index in provider migrations; v1 scaffolding emits only provider-neutral column indexes.",
                "ExpressionIndex" => "Keep the expression index in provider migrations or replace it with a provider-neutral persisted column plus a normal index.",
                "IncludedColumnIndex" => "Ordinary SQL Server/PostgreSQL included-column indexes are emitted with IndexAttribute.IsIncluded; keep this diagnostic in provider migrations only when the included-column facet cannot be attached safely to generated column-index metadata.",
                "DescendingIndex" => "Review this descending index shape; ordinary column-key descending indexes are generated, but this one was not safe to map as provider-neutral index metadata.",
                "PrefixIndex" => "Keep the MySQL prefix index in provider migrations; prefix uniqueness is not full-column uniqueness and is not used for generated alternate-key relationships.",
                "ProviderSpecificIndex" => "Keep this provider-specific index implementation in migrations; v1 scaffolding emits portable B-tree/rowstore column-index metadata only.",
                "TemporalTable" => "Choose provider-native temporal intentionally or migrate to nORM-managed temporal history; do not assume scaffolding round-trips native temporal DDL.",
                "MissingPrimaryKey" => "Generated code marks this type with [ReadOnlyEntity] so query materialization works but nORM writes are rejected; add a primary key before using generated writes or navigations.",
                _ => "Review the provider-owned object and add explicit model configuration or migration code for the intended behavior."
            };

        public static string SuggestedActionForSkippedObject(string kind)
            => kind switch
            {
                "View" => "Ordinary views scaffold as read-oriented query artifacts by default. Review this provider-specific skipped view metadata and keep it behind explicit provider-bound query code if it cannot be represented as a read-only entity.",
                "Routine" => "Keep routine calls behind explicit raw SQL/stored-procedure code and document the provider-bound contract.",
                "Sequence" => "Configure generated-key behavior explicitly or keep sequence DDL in provider migrations.",
                "Synonym" => "Select local table/view synonyms explicitly with --table/--schema, use --emit-query-artifacts, resolve the synonym to a supported base table, or keep non-query/remote synonyms behind provider-bound integration code.",
                "MaterializedView" => "Materialized views scaffold as read-oriented query artifacts by default. Keep refresh behavior provider-bound and review any provider-specific skipped metadata manually.",
                "Event" => "Keep scheduled event behavior in provider operations/migrations; v1 scaffolding emits table models only.",
                "VirtualTable" => "Select the virtual table explicitly with --table/--schema or use --emit-query-artifacts/ScaffoldOptions.EmitQueryArtifacts for a read-oriented query artifact, or keep the virtual table behind provider-bound query/index code.",
                "VirtualTableShadow" => "Do not map SQLite virtual-table shadow storage as domain entities; keep it provider-owned with the virtual table.",
                _ => "Keep this database object in provider migrations or hand-written integration code."
            };

        private static void AppendCompositeForeignKeys(
            StringBuilder sb,
            IReadOnlyList<ScaffoldCompositeForeignKeyDiagnosticInfo> compositeForeignKeys)
        {
            if (compositeForeignKeys.Count == 0)
                return;

            sb.AppendLine();
            sb.AppendLine("## Composite Foreign Keys");
            sb.AppendLine();
            sb.AppendLine("These composite foreign keys do not target the generated principal primary key or an exact ordered unfiltered unique index, so v1 scaffolding keeps them diagnostic.");
            sb.AppendLine("The generated entity classes keep the scalar columns, and no relationship navigation is emitted for these constraints.");
            sb.AppendLine();
            sb.AppendLine("| Code | Severity | Category | Constraint | Dependent | Columns | Principal | Principal Columns | Suggested Action |");
            sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
            foreach (var foreignKey in compositeForeignKeys)
            {
                sb.AppendLine(
                    $"| {CodeForCompositeForeignKey()} | {Severity()} | {CategoryForCompositeForeignKey()} | {EscapeMarkdown(foreignKey.ConstraintName)} | {EscapeMarkdown(foreignKey.DependentTable)} | {EscapeMarkdown(string.Join(", ", foreignKey.DependentColumns))} | {EscapeMarkdown(foreignKey.PrincipalTable)} | {EscapeMarkdown(string.Join(", ", foreignKey.PrincipalColumns))} | {EscapeMarkdown(SuggestedActionForCompositeForeignKey())} |");
            }
        }

        private static void AppendPossibleJoinTables(
            StringBuilder sb,
            IReadOnlyList<ScaffoldPossibleJoinTableDiagnosticInfo> possibleJoinTables)
        {
            if (possibleJoinTables.Count == 0)
                return;

            sb.AppendLine();
            sb.AppendLine("## Possible Many-To-Many Join Tables");
            sb.AppendLine();
            sb.AppendLine("These tables look like join-table candidates but were scaffolded as normal entities because at least one safe `UsingTable` requirement was not met.");
            sb.AppendLine();
            sb.AppendLine("| Code | Severity | Category | Table | Principal Tables | Constraints | Reason Codes | Suggested Action |");
            sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- |");
            foreach (var table in possibleJoinTables)
            {
                sb.AppendLine(
                    $"| {CodeForPossibleJoinTable()} | {Severity()} | {CategoryForPossibleJoinTable()} | {EscapeMarkdown(table.TableKey)} | {EscapeMarkdown(string.Join(", ", table.PrincipalTables))} | {EscapeMarkdown(string.Join(", ", table.ConstraintNames))} | {EscapeMarkdown(string.Join(", ", table.Reasons))} | {EscapeMarkdown(SuggestedActionForPossibleJoinTable())} |");
            }
        }

        private static void AppendUnsupportedFeatures(
            StringBuilder sb,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures)
        {
            if (unsupportedFeatures.Count == 0)
                return;

            sb.AppendLine();
            sb.AppendLine("## Provider-Owned Schema Features");
            sb.AppendLine();
            sb.AppendLine("Defaults, ordinary table CHECK constraints, and computed/generated column expressions are emitted as migration metadata when possible. Collations, scaffoldable citext/JSON/XML/UUID scalar storage, parsed SQL Server identity seed/increment settings, and supported FK referential actions are emitted when a generated property or relationship can safely own them. Remaining provider-specific column types, rowversion/timestamp columns, unparsed identity strategies, unrecognized/provider-specific FK referential action tokens, relationships that do not target the generated principal primary key or an exact ordered unfiltered unique index, triggers, provider-native temporal tables, and tables without primary keys are discovered for review, but are not emitted as complete provider-neutral nORM model code. Unmodeled defaults, unparsed identity strategies, unsafe provider-specific column types, triggers, temporal tables, and keyless tables make generated types read-only so write paths fail closed.");
            sb.AppendLine();
            sb.AppendLine("| Code | Severity | Category | Kind | Table | Object | Detail | Suggested Action |");
            sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- |");
            foreach (var feature in unsupportedFeatures
                .OrderBy(f => f.TableKey, StringComparer.Ordinal)
                .ThenBy(f => f.Kind, StringComparer.Ordinal)
                .ThenBy(f => f.Name, StringComparer.Ordinal))
            {
                sb.AppendLine(
                    $"| {CodeForUnsupportedFeature(feature.Kind)} | {Severity()} | {CategoryForUnsupportedFeature(feature.Kind)} | {EscapeMarkdown(feature.Kind)} | {EscapeMarkdown(feature.TableKey)} | {EscapeMarkdown(feature.Name)} | {EscapeMarkdown(feature.Detail)} | {EscapeMarkdown(SuggestedActionForUnsupportedFeature(feature.Kind))} |");
            }
        }

        private static void AppendSkippedObjects(
            StringBuilder sb,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects)
        {
            if (skippedObjects.Count == 0)
                return;

            sb.AppendLine();
            sb.AppendLine("## Skipped Database Objects");
            sb.AppendLine();
            sb.AppendLine("Ordinary views/materialized views are emitted as read-only query artifacts by default; SQLite virtual tables and local table/view synonyms can be emitted when explicitly selected by table/schema filters or when query-artifact emission is enabled. Routines can be emitted as opt-in provider-bound stubs, and sequences/non-query synonyms/events remain provider-owned review items.");
            sb.AppendLine();
            sb.AppendLine("| Code | Severity | Category | Kind | Name | Detail | Suggested Action |");
            sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- |");
            foreach (var obj in skippedObjects
                .OrderBy(o => TableKey(o.Schema, o.Name), StringComparer.Ordinal)
                .ThenBy(o => o.Kind, StringComparer.Ordinal))
            {
                sb.AppendLine(
                    $"| {CodeForSkippedObject(obj.Kind)} | {Severity()} | {CategoryForSkippedObject(obj.Kind)} | {EscapeMarkdown(obj.Kind)} | {EscapeMarkdown(TableKey(obj.Schema, obj.Name))} | {EscapeMarkdown(obj.Detail)} | {EscapeMarkdown(SuggestedActionForSkippedObject(obj.Kind))} |");
            }
        }

        private static string EscapeMarkdown(string value)
            => value
                .Replace("\\", "\\\\")
                .Replace("|", "\\|")
                .Replace("\r", "\\r")
                .Replace("\n", "\\n");

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;
    }
}
