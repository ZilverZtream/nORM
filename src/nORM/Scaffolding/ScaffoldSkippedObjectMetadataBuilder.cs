#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSkippedObjectMetadataBuilder
    {
        public static IReadOnlyDictionary<string, object?> BuildMetadata(ScaffoldSkippedObjectInfo obj)
        {
            if (string.Equals(obj.Kind, "Sequence", StringComparison.OrdinalIgnoreCase))
                return BuildSequenceMetadata(obj);

            if (string.Equals(obj.Kind, "Synonym", StringComparison.OrdinalIgnoreCase))
                return BuildSynonymMetadata(obj);

            if (string.Equals(obj.Kind, "Event", StringComparison.OrdinalIgnoreCase))
                return BuildEventMetadata(obj);

            if (string.Equals(obj.Kind, "View", StringComparison.OrdinalIgnoreCase)
                || string.Equals(obj.Kind, "MaterializedView", StringComparison.OrdinalIgnoreCase)
                || string.Equals(obj.Kind, "VirtualTable", StringComparison.OrdinalIgnoreCase)
                || string.Equals(obj.Kind, "VirtualTableShadow", StringComparison.OrdinalIgnoreCase))
            {
                return BuildQueryObjectMetadata(obj);
            }

            if (!string.Equals(obj.Kind, "Routine", StringComparison.OrdinalIgnoreCase))
                return new Dictionary<string, object?>(0, StringComparer.Ordinal);

            var values = ParseRoutineSemicolonValues(obj.Detail, out var header);
            var provider = ParseRoutineProvider(header);
            var routineType = ParseRoutineType(header);
            values.TryGetValue("dataType", out var dataType);
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = provider,
                ["routineType"] = routineType,
                ["parameterCount"] = ParseNullableInt(values.TryGetValue("parameters", out var parameterCount) ? parameterCount : null),
                ["outputParameterCount"] = ParseNullableInt(values.TryGetValue("outputParameters", out var outputParameterCount) ? outputParameterCount : null)
            };

            if (!string.IsNullOrWhiteSpace(dataType))
                metadata["dataType"] = dataType;

            if (values.TryGetValue("callShape", out var callShape) && !string.IsNullOrWhiteSpace(callShape))
                metadata["callShape"] = callShape;
            else
            {
                var inferredCallShape = InferRoutineCallShape(provider, routineType, dataType);
                if (!string.IsNullOrWhiteSpace(inferredCallShape))
                    metadata["callShape"] = inferredCallShape;
            }

            if (values.TryGetValue("parameterModes", out var parameterModes))
                metadata["parameters"] = ParseRoutineParameters(parameterModes);

            if (values.TryGetValue("resultColumns", out var resultColumns))
                metadata["resultColumns"] = ParseRoutineResultColumns(resultColumns);

            return metadata;
        }

        public static bool IsTableLikeSqlServerSynonym(string detail)
        {
            if (!detail.StartsWith("SQL Server synonym", StringComparison.OrdinalIgnoreCase))
                return false;

            var baseType = ParseSemicolonValue(detail, "baseType");
            return string.Equals(baseType, "U", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(baseType, "V", StringComparison.OrdinalIgnoreCase);
        }

        public static string ParseSkippedObjectProvider(string detail)
        {
            if (detail.StartsWith("SQL Server", StringComparison.OrdinalIgnoreCase)) return "SQL Server";
            if (detail.StartsWith("PostgreSQL", StringComparison.OrdinalIgnoreCase)) return "PostgreSQL";
            if (detail.StartsWith("MySQL", StringComparison.OrdinalIgnoreCase)) return "MySQL";
            if (detail.StartsWith("SQLite", StringComparison.OrdinalIgnoreCase)) return "SQLite";
            var space = detail.IndexOf(' ');
            return space > 0 ? detail[..space] : detail;
        }

        private static IReadOnlyDictionary<string, object?> BuildSequenceMetadata(ScaffoldSkippedObjectInfo obj)
        {
            var provider = ParseSequenceProvider(obj.Detail);
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = FormatSequenceProvider(provider),
                ["stubSupported"] = provider is "sqlserver" or "postgres",
                ["clrType"] = ScaffoldTypeNameHelper.GetTypeName(MapSequenceValueType(obj.Detail), allowNull: false)
            };

            var dataType = ParseSemicolonValue(obj.Detail, "dataType");
            if (!string.IsNullOrWhiteSpace(dataType))
                metadata["dataType"] = dataType;

            return metadata;
        }

        private static IReadOnlyDictionary<string, object?> BuildSynonymMetadata(ScaffoldSkippedObjectInfo obj)
        {
            if (!obj.Detail.StartsWith("SQL Server synonym", StringComparison.OrdinalIgnoreCase))
                return new Dictionary<string, object?>(0, StringComparer.Ordinal);

            var baseObject = ParseSemicolonValue(obj.Detail, "baseObject");
            var baseType = ParseSemicolonValue(obj.Detail, "baseType");
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = "SQL Server",
                ["queryArtifactSupported"] = IsTableLikeSqlServerSynonym(obj.Detail)
            };

            if (!string.IsNullOrWhiteSpace(baseObject))
                metadata["baseObject"] = baseObject;
            if (!string.IsNullOrWhiteSpace(baseType))
            {
                metadata["baseType"] = baseType;
                metadata["targetKind"] = baseType.ToUpperInvariant() switch
                {
                    "U" => "Table",
                    "V" => "View",
                    "P" => "Procedure",
                    "FN" or "IF" or "TF" => "Function",
                    _ => baseType
                };
            }

            return metadata;
        }

        private static IReadOnlyDictionary<string, object?> BuildEventMetadata(ScaffoldSkippedObjectInfo obj)
        {
            if (!obj.Detail.StartsWith("MySQL event", StringComparison.OrdinalIgnoreCase))
                return new Dictionary<string, object?>(0, StringComparer.Ordinal);

            var values = ScaffoldSemicolonParser.Parse(obj.Detail, out _);
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = "MySQL"
            };

            AddMetadataValue(metadata, values, "eventType");
            AddMetadataValue(metadata, values, "status");
            AddMetadataValue(metadata, values, "intervalValue");
            AddMetadataValue(metadata, values, "intervalField");
            AddMetadataValue(metadata, values, "executeAt");
            AddMetadataValue(metadata, values, "starts");
            AddMetadataValue(metadata, values, "ends");
            return metadata;
        }

        private static IReadOnlyDictionary<string, object?> BuildQueryObjectMetadata(ScaffoldSkippedObjectInfo obj)
        {
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = ParseSkippedObjectProvider(obj.Detail),
                ["targetKind"] = obj.Kind,
                ["queryArtifactSupported"] = IsQueryArtifactObject(obj)
            };

            if (string.Equals(obj.Kind, "VirtualTableShadow", StringComparison.OrdinalIgnoreCase))
            {
                var owner = InferSqliteVirtualTableShadowOwner(obj.Name);
                if (!string.IsNullOrWhiteSpace(owner))
                    metadata["shadowOf"] = owner;
            }

            return metadata;
        }

        private static bool IsQueryArtifactObject(ScaffoldSkippedObjectInfo obj)
            => string.Equals(obj.Kind, "View", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "MaterializedView", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "VirtualTable", StringComparison.OrdinalIgnoreCase)
               || (string.Equals(obj.Kind, "Synonym", StringComparison.OrdinalIgnoreCase) && IsTableLikeSqlServerSynonym(obj.Detail));

        private static string InferSqliteVirtualTableShadowOwner(string tableName)
        {
            var suffixes = new[]
            {
                "_content",
                "_data",
                "_idx",
                "_docsize",
                "_config",
                "_segments",
                "_segdir",
                "_stat",
                "_node",
                "_parent",
                "_rowid"
            };

            foreach (var suffix in suffixes)
            {
                if (tableName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase)
                    && tableName.Length > suffix.Length)
                {
                    return tableName[..^suffix.Length];
                }
            }

            return string.Empty;
        }

        private static string FormatSequenceProvider(string provider)
            => provider switch
            {
                "sqlserver" => "SQL Server",
                "postgres" => "PostgreSQL",
                _ => provider
            };

        private static string ParseSequenceProvider(string detail)
        {
            if (detail.StartsWith("SQL Server", StringComparison.OrdinalIgnoreCase))
                return "sqlserver";
            if (detail.StartsWith("PostgreSQL", StringComparison.OrdinalIgnoreCase))
                return "postgres";
            return string.Empty;
        }

        private static Type MapSequenceValueType(string detail)
        {
            var dataType = ParseSemicolonValue(detail, "dataType");
            var normalized = dataType.Split('(', 2)[0].Trim().ToLowerInvariant();
            return normalized switch
            {
                "tinyint" => typeof(byte),
                "smallint" => typeof(short),
                "int" or "integer" => typeof(int),
                "bigint" => typeof(long),
                "decimal" or "numeric" => typeof(decimal),
                _ => typeof(long)
            };
        }

        private static string ParseSemicolonValue(string detail, string key)
        {
            var values = ScaffoldSemicolonParser.Parse(detail, out _);
            return values.TryGetValue(key, out var value) ? value : string.Empty;
        }

        private static string InferRoutineCallShape(string provider, string routineType, string? dataType)
        {
            if (!routineType.Contains("function", StringComparison.OrdinalIgnoreCase))
                return string.Empty;

            if (provider.Equals("PostgreSQL", StringComparison.OrdinalIgnoreCase)
                && (string.Equals(dataType, "record", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(dataType, "table", StringComparison.OrdinalIgnoreCase)))
            {
                return "table-valued-function";
            }

            return "scalar-function";
        }

        private static string ParseRoutineProvider(string header)
        {
            if (header.StartsWith("SQL Server ", StringComparison.OrdinalIgnoreCase)) return "SQL Server";
            if (header.StartsWith("PostgreSQL ", StringComparison.OrdinalIgnoreCase)) return "PostgreSQL";
            if (header.StartsWith("MySQL ", StringComparison.OrdinalIgnoreCase)) return "MySQL";
            var space = header.IndexOf(' ');
            return space > 0 ? header[..space] : header;
        }

        private static string ParseRoutineType(string header)
        {
            foreach (var provider in new[] { "SQL Server ", "PostgreSQL ", "MySQL " })
            {
                if (header.StartsWith(provider, StringComparison.OrdinalIgnoreCase))
                    return header[provider.Length..];
            }

            var space = header.IndexOf(' ');
            return space > 0 ? header[(space + 1)..] : header;
        }

        private static IReadOnlyDictionary<string, string> ParseRoutineSemicolonValues(string detail, out string header)
            => ScaffoldSemicolonParser.ParseRoutine(detail, out header, IsCompleteSemicolonValue);

        private static bool IsCompleteSemicolonValue(string key, string value)
        {
            if (key.Equals("parameters", StringComparison.OrdinalIgnoreCase)
                || key.Equals("outputParameters", StringComparison.OrdinalIgnoreCase))
            {
                return int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out _);
            }

            if (key.Equals("parameterModes", StringComparison.OrdinalIgnoreCase))
                return IsCompleteRoutineParameterModes(value);

            if (key.Equals("resultColumns", StringComparison.OrdinalIgnoreCase))
                return IsCompleteRoutineResultColumns(value);

            return true;
        }

        private static bool IsCompleteRoutineParameterModes(string parameterModes)
        {
            if (string.IsNullOrWhiteSpace(parameterModes))
                return true;

            var parts = SplitRoutineParameterModes(parameterModes);
            return parts.Count > 0 && parts.All(part => TryParseRoutineParameterMode(part, out _, out _, out _));
        }

        private static bool IsCompleteRoutineResultColumns(string resultColumns)
        {
            if (string.IsNullOrWhiteSpace(resultColumns))
                return true;

            var parts = SplitRoutineResultColumns(resultColumns);
            return parts.Count > 0 && parts.All(part => TryParseRoutineResultColumnParts(part, out _, out _, out _));
        }

        private static int? ParseNullableInt(string? value)
            => int.TryParse(value, out var parsed) ? parsed : null;

        private static IReadOnlyList<IReadOnlyDictionary<string, object?>> ParseRoutineParameters(string parameterModes)
        {
            if (string.IsNullOrWhiteSpace(parameterModes))
                return Array.Empty<IReadOnlyDictionary<string, object?>>();

            return SplitRoutineParameterModes(parameterModes)
                .Select(ParseRoutineParameter)
                .ToArray();
        }

        private static IReadOnlyDictionary<string, object?> ParseRoutineParameter(string raw)
        {
            if (!TryParseRoutineParameterMode(raw, out var name, out var mode, out var dataType))
            {
                var parts = raw.Split(':', 3);
                name = parts.Length > 0 ? parts[0].Trim() : string.Empty;
                mode = parts.Length > 1 ? parts[1].Trim() : string.Empty;
                dataType = parts.Length > 2 ? parts[2].Trim() : string.Empty;
            }

            var parameter = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["name"] = name,
                ["mode"] = mode
            };
            if (!string.IsNullOrWhiteSpace(dataType))
            {
                parameter["dataType"] = dataType;
                parameter["clrType"] = ScaffoldRoutineTypeMapper.GetRoutineParameterTypeName(dataType);
                parameter["dbType"] = ScaffoldRoutineTypeMapper.GetRoutineParameterDbTypeName(dataType);
            }

            return parameter;
        }

        private static bool TryParseRoutineParameterMode(
            string raw,
            out string name,
            out string mode,
            out string dataType)
        {
            name = string.Empty;
            mode = string.Empty;
            dataType = string.Empty;

            var trimmed = raw.Trim();
            if (trimmed.Length == 0)
                return false;

            for (var separator = trimmed.Length - 1; separator >= 0; separator--)
            {
                if (trimmed[separator] != ':')
                    continue;

                var modeStart = separator + 1;
                var modeEnd = trimmed.IndexOf(':', modeStart);
                var candidate = modeEnd >= 0
                    ? trimmed.Substring(modeStart, modeEnd - modeStart).Trim()
                    : trimmed[modeStart..].Trim();
                if (!IsRoutineParameterMode(candidate))
                    continue;

                name = trimmed[..separator].Trim();
                mode = candidate;
                dataType = modeEnd >= 0 ? trimmed[(modeEnd + 1)..].Trim() : string.Empty;
                return true;
            }

            return false;
        }

        private static bool IsRoutineParameterMode(string mode)
            => mode.Equals("IN", StringComparison.OrdinalIgnoreCase)
               || mode.Equals("OUT", StringComparison.OrdinalIgnoreCase)
               || mode.Equals("INOUT", StringComparison.OrdinalIgnoreCase)
               || mode.Equals("RETURN", StringComparison.OrdinalIgnoreCase);

        private static IReadOnlyList<IReadOnlyDictionary<string, object?>> ParseRoutineResultColumns(string resultColumns)
        {
            if (string.IsNullOrWhiteSpace(resultColumns))
                return Array.Empty<IReadOnlyDictionary<string, object?>>();

            return SplitRoutineResultColumns(resultColumns)
                .Select(ParseRoutineResultColumn)
                .ToArray();
        }

        private static IReadOnlyDictionary<string, object?> ParseRoutineResultColumn(string raw)
        {
            string name;
            string dataType;
            bool? isNullable = null;
            if (TryParseRoutineResultColumnParts(raw, out name, out dataType, out var nullable))
            {
                isNullable = nullable;
            }
            else
            {
                var parts = raw.Split(':', 3);
                name = parts.Length > 0 ? parts[0].Trim() : string.Empty;
                dataType = parts.Length > 1 ? parts[1].Trim() : string.Empty;
                if (parts.Length > 2 && int.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedNullable))
                    isNullable = parsedNullable != 0;
            }

            var column = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["name"] = name,
                ["dataType"] = dataType
            };
            if (isNullable.HasValue)
                column["nullable"] = isNullable.Value;
            return column;
        }

        private static bool TryParseRoutineResultColumnParts(
            string raw,
            out string name,
            out string dataType,
            out bool nullable)
        {
            name = string.Empty;
            dataType = string.Empty;
            nullable = false;

            var trimmed = raw.Trim();
            var nullableSeparator = trimmed.LastIndexOf(':');
            if (nullableSeparator <= 0
                || !int.TryParse(trimmed[(nullableSeparator + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedNullable))
            {
                return false;
            }

            var typeSeparator = trimmed.LastIndexOf(':', nullableSeparator - 1);
            if (typeSeparator < 0)
                return false;

            name = trimmed[..typeSeparator].Trim();
            dataType = trimmed.Substring(typeSeparator + 1, nullableSeparator - typeSeparator - 1).Trim();
            nullable = parsedNullable != 0;
            return true;
        }

        private static IReadOnlyList<string> SplitRoutineParameterModes(string parameterModes)
        {
            var parts = new List<string>();
            var start = 0;
            var depth = 0;
            for (var i = 0; i < parameterModes.Length; i++)
            {
                var ch = parameterModes[i];
                if (ch == '(')
                {
                    depth++;
                }
                else if (ch == ')' && depth > 0)
                {
                    depth--;
                }
                else if (ch == ',' && depth == 0)
                {
                    var candidate = parameterModes[start..i];
                    if (TryParseRoutineParameterMode(candidate, out _, out _, out _))
                    {
                        AddRoutineDelimitedPart(parts, candidate);
                        start = i + 1;
                    }
                }
            }

            AddRoutineDelimitedPart(parts, parameterModes[start..]);
            return parts;
        }

        private static IReadOnlyList<string> SplitRoutineResultColumns(string resultColumns)
        {
            var parts = new List<string>();
            var start = 0;
            for (var i = 0; i < resultColumns.Length; i++)
            {
                if (resultColumns[i] != '|')
                    continue;

                var candidate = resultColumns[start..i];
                if (TryParseRoutineResultColumnParts(candidate, out _, out _, out _))
                {
                    AddRoutineDelimitedPart(parts, candidate);
                    start = i + 1;
                }
            }

            AddRoutineDelimitedPart(parts, resultColumns[start..]);
            return parts;
        }

        private static void AddRoutineDelimitedPart(List<string> parts, string value)
        {
            var trimmed = value.Trim();
            if (!string.IsNullOrWhiteSpace(trimmed))
                parts.Add(trimmed);
        }

        private static void AddMetadataValue(
            IDictionary<string, object?> metadata,
            IReadOnlyDictionary<string, string> values,
            string key)
        {
            if (values.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
                metadata[key] = value;
        }
    }
}
