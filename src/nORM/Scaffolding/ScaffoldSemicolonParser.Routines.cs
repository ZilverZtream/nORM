#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSemicolonParser
    {
        private static IReadOnlyList<IReadOnlyList<string>> GetRoutineSemicolonValueKeyOrders(string detail)
        {
            var firstMarker = detail.IndexOf(';');
            var header = firstMarker < 0 ? detail.Trim() : detail[..firstMarker].Trim();
            if (header.StartsWith("SQL Server stored procedure", StringComparison.OrdinalIgnoreCase))
            {
                return new[]
                {
                    new[] { "parameters", "outputParameters", "parameterModes", "resultColumns" }
                };
            }

            if (header.StartsWith("SQL Server ", StringComparison.OrdinalIgnoreCase))
            {
                return new[]
                {
                    new[] { "parameters", "outputParameters", "callShape", "parameterModes", "dataType", "resultColumns" },
                    new[] { "parameters", "outputParameters", "parameterModes", "callShape", "dataType", "resultColumns" }
                };
            }

            if (header.StartsWith("PostgreSQL ", StringComparison.OrdinalIgnoreCase)
                || header.StartsWith("MySQL ", StringComparison.OrdinalIgnoreCase))
            {
                return new[]
                {
                    new[] { "parameters", "outputParameters", "parameterModes", "callShape", "dataType" },
                    new[] { "parameters", "outputParameters", "callShape", "parameterModes", "dataType" }
                };
            }

            return Array.Empty<IReadOnlyList<string>>();
        }
    }
}
