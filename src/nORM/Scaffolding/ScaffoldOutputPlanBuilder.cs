#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.ObjectPool;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldOutputPlanBuilder
    {
        public static async Task<ScaffoldOutputPlan> BuildAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string outputDirectory,
            string contextOutputDirectory,
            string namespaceName,
            string contextNamespace,
            string safeContextName,
            ScaffoldModelDiscoveryResult discovery,
            ScaffoldModelComposition composition,
            ScaffoldOptions options,
            ObjectPool<StringBuilder> stringBuilderPool)
        {
            var entityFiles = await ScaffoldEntityFileAdapter.BuildScaffoldEntityFilesAsync(
                connection,
                provider,
                outputDirectory,
                namespaceName,
                discovery.Tables,
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                discovery.PrimaryKeyColumnsByTable,
                discovery.NonNullableColumnsByTable,
                discovery.SqliteDeclaredTypesByTable,
                discovery.ColumnStoreTypesByTable,
                discovery.StringBinaryFacetsByTable,
                discovery.CommentsByTable,
                discovery.IdentityColumnsByTable,
                discovery.Indexes,
                composition.Relationships,
                composition.ManyToManyJoins,
                composition.ManyToManyJoinTableKeys,
                discovery.QueryArtifactTableKeys,
                discovery.FeatureConfigurations,
                options).ConfigureAwait(false);
            var generatedFiles = entityFiles.GeneratedFiles.ToList();

            var routineStubs = options.EmitRoutineStubs
                ? discovery.SkippedObjects.Where(static obj => string.Equals(obj.Kind, "Routine", StringComparison.OrdinalIgnoreCase)).ToArray()
                : Array.Empty<DatabaseScaffolder.ScaffoldSkippedObject>();
            var sequenceStubs = options.EmitSequenceStubs
                ? discovery.SkippedObjects.Where(static obj => string.Equals(obj.Kind, "Sequence", StringComparison.OrdinalIgnoreCase)).ToArray()
                : Array.Empty<DatabaseScaffolder.ScaffoldSkippedObject>();
            var ctxCode = ScaffoldContextAdapter.Write(
                contextNamespace,
                safeContextName,
                entityFiles.EntityNames,
                composition.Relationships,
                composition.ManyToManyJoins,
                routineStubs,
                composition.CompositePrimaryKeys,
                composition.DefaultValueConfigurations,
                composition.CheckConstraints,
                composition.ComputedColumnConfigurations,
                composition.ExpressionIndexConfigurations,
                composition.CollationConfigurations,
                sequenceStubs,
                composition.IdentityOptionConfigurations,
                composition.PrecisionConfigurations,
                composition.ColumnFacetConfigurations,
                options.UsePluralizer,
                options.UseNullableReferenceTypes,
                namespaceName,
                options.UseDatabaseNames);
            generatedFiles.Add((Path.Combine(contextOutputDirectory, safeContextName + ".cs"), ctxCode));

            var computedColumnsByTable = discovery.FeatureConfigurations.ComputedColumnsByTable;
            var providerOwnedWriteBlockedTableKeys = discovery.FeatureConfigurations.ProviderOwnedWriteBlockedTableKeys;
            var diagnostics = ScaffoldDiagnosticsAdapter.ScaffoldDiagnostics(
                discovery.ForeignKeys,
                discovery.UnsupportedFeatures,
                discovery.SkippedObjects,
                discovery.PrimaryKeyColumnsByTable,
                discovery.Indexes,
                discovery.ColumnPropertiesByTable,
                discovery.NonNullableColumnsByTable,
                computedColumnsByTable,
                discovery.IdentityColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                composition.ManyToManyJoinTableKeys,
                stringBuilderPool);
            var diagnosticsJson = string.IsNullOrWhiteSpace(diagnostics)
                ? null
                : ScaffoldDiagnosticsAdapter.ScaffoldDiagnosticsJson(
                    discovery.ForeignKeys,
                    discovery.UnsupportedFeatures,
                    discovery.SkippedObjects,
                    discovery.PrimaryKeyColumnsByTable,
                    discovery.Indexes,
                    discovery.ColumnPropertiesByTable,
                    discovery.NonNullableColumnsByTable,
                    computedColumnsByTable,
                    discovery.IdentityColumnsByTable,
                    providerOwnedWriteBlockedTableKeys,
                    composition.ManyToManyJoinTableKeys);

            return new ScaffoldOutputPlan(generatedFiles, diagnostics, diagnosticsJson);
        }
    }

    internal sealed record ScaffoldOutputPlan(
        List<(string Path, string Content)> GeneratedFiles,
        string Diagnostics,
        string? DiagnosticsJson);
}
