#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSemicolonParser
    {
        private static bool IsKnownSemicolonValueKey(string key)
            => key.Equals("parameters", StringComparison.OrdinalIgnoreCase)
               || key.Equals("outputParameters", StringComparison.OrdinalIgnoreCase)
               || key.Equals("parameterModes", StringComparison.OrdinalIgnoreCase)
               || key.Equals("resultColumns", StringComparison.OrdinalIgnoreCase)
               || key.Equals("callShape", StringComparison.OrdinalIgnoreCase)
               || key.Equals("dataType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("baseObject", StringComparison.OrdinalIgnoreCase)
               || key.Equals("baseType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("eventType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("status", StringComparison.OrdinalIgnoreCase)
               || key.Equals("intervalValue", StringComparison.OrdinalIgnoreCase)
               || key.Equals("intervalField", StringComparison.OrdinalIgnoreCase)
               || key.Equals("executeAt", StringComparison.OrdinalIgnoreCase)
               || key.Equals("starts", StringComparison.OrdinalIgnoreCase)
               || key.Equals("ends", StringComparison.OrdinalIgnoreCase)
               || key.Equals("expression", StringComparison.OrdinalIgnoreCase)
               || key.Equals("isUnique", StringComparison.OrdinalIgnoreCase)
               || key.Equals("filterSql", StringComparison.OrdinalIgnoreCase)
               || key.Equals("indexSql", StringComparison.OrdinalIgnoreCase)
               || key.Equals("indexType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("accessMethod", StringComparison.OrdinalIgnoreCase)
               || key.Equals("prefixColumns", StringComparison.OrdinalIgnoreCase)
               || key.Equals("hasNullsNotDistinct", StringComparison.OrdinalIgnoreCase)
               || key.Equals("hasNonDefaultOperatorClass", StringComparison.OrdinalIgnoreCase)
               || key.Equals("hasIndexCollation", StringComparison.OrdinalIgnoreCase)
               || key.Equals("hasNonDefaultNullOrdering", StringComparison.OrdinalIgnoreCase)
               || key.Equals("timing", StringComparison.OrdinalIgnoreCase)
               || key.Equals("event", StringComparison.OrdinalIgnoreCase)
               || key.Equals("orientation", StringComparison.OrdinalIgnoreCase)
               || key.Equals("triggerSql", StringComparison.OrdinalIgnoreCase)
               || key.Equals("isDisabled", StringComparison.OrdinalIgnoreCase)
               || key.Equals("isInsteadOf", StringComparison.OrdinalIgnoreCase)
               || key.Equals("temporalType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("historyTable", StringComparison.OrdinalIgnoreCase);
    }
}
