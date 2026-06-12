#nullable enable
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        public static string Write(ScaffoldContextInfo context)
        {
            var emitRoutineStubs = context.RoutineStubs.Count > 0;
            var hasSequenceStubs = context.SequenceStubs.Count > 0;
            var needsSystemUsing = context.Relationships.Count > 0
                                   || context.ManyToManyJoins.Count > 0
                                   || context.CompositePrimaryKeys.Count > 0
                                   || emitRoutineStubs;
            var sb = new StringBuilder();
            AppendScaffoldContextHeader(
                sb,
                context.NamespaceName,
                context.ContextName,
                emitRoutineStubs,
                hasSequenceStubs,
                needsSystemUsing,
                context.UseNullableReferenceTypes,
                context.EntityNamespaceName);
            var queryPropertyNames = AppendScaffoldContextConstructorsAndQueries(
                sb,
                context.ContextName,
                context.EntityNames,
                context.UsePluralizer,
                context.UseNullableReferenceTypes);
            if (emitRoutineStubs)
                ScaffoldRoutineStubWriter.AppendRoutineStubs(sb, context.RoutineStubs, queryPropertyNames, context.UseNullableReferenceTypes, context.UseDatabaseNames);
            if (hasSequenceStubs)
                AppendSequenceStubs(sb, context.SequenceStubs, queryPropertyNames, context.UseDatabaseNames);
            AppendConfigureOptionsStart(sb, context.UseNullableReferenceTypes);
            AppendModelConfiguration(sb, context);
            AppendScaffoldContextFooter(sb);
            return sb.ToString();
        }
    }
}
