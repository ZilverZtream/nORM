#nullable enable
using System;
using System.IO;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldOutputPlanBuilder
    {
        private static (string Path, string Content) BuildContextFile(
            ScaffoldOutputPlanRequest request,
            ScaffoldEntityFileSet entityFiles)
        {
            var options = request.Options;
            var composition = request.Composition;
            var routineStubs = SelectSkippedObjectStubs(
                request.Discovery,
                options.EmitRoutineStubs,
                "Routine");
            var sequenceStubs = SelectSkippedObjectStubs(
                request.Discovery,
                options.EmitSequenceStubs,
                "Sequence");
            var ctxCode = ScaffoldContextAdapter.Write(
                request.ContextNamespace,
                request.SafeContextName,
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
                request.NamespaceName,
                options.UseDatabaseNames);

            return (Path.Combine(request.ContextOutputDirectory, request.SafeContextName + ".cs"), ctxCode);
        }

        private static DatabaseScaffolder.ScaffoldSkippedObject[] SelectSkippedObjectStubs(
            ScaffoldModelDiscoveryResult discovery,
            bool emit,
            string kind)
            => emit
                ? discovery.SkippedObjects
                    .Where(obj => string.Equals(obj.Kind, kind, StringComparison.OrdinalIgnoreCase))
                    .ToArray()
                : Array.Empty<DatabaseScaffolder.ScaffoldSkippedObject>();
    }
}
