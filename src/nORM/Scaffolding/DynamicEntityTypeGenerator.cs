using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Reflection.Emit;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Generates entity types at runtime based on database schema information.
    /// Reuses a single shared <see cref="ModuleBuilder"/> to prevent unloadable assembly
    /// accumulation when types are evicted from cache.
    /// </summary>
    [RequiresDynamicCode("Dynamic scaffolding emits CLR types at runtime and is not supported under NativeAOT or runtimes where dynamic code is disabled.")]
    [RequiresUnreferencedCode("Dynamic scaffolding reflects database schema into runtime-generated entity types and is not trim-safe.")]
    public class DynamicEntityTypeGenerator
    {
        private sealed record ColumnInfo(string ColumnName, string PropertyName, Type PropertyType, bool AllowsNull, bool IsKey, bool IsAuto, bool IsComputed, int? MaxLength);

        /// <summary>Namespace prefix used for all dynamically generated entity types.</summary>
        private const string DynamicTypeNamespace = "nORM.Dynamic";

        /// <summary>Name of the shared dynamic assembly that hosts all generated entity types.</summary>
        private const string DynamicAssemblyName = "nORM.Dynamic.Entities";

        /// <summary>Name of the dynamic module within the shared assembly.</summary>
        private const string DynamicModuleName = "MainModule";

        /// <summary>
        /// Number of leading bytes from the SHA-256 hash used as the schema signature.
        /// 16 bytes yields a 32-character hex string with negligible collision probability.
        /// </summary>
        private const int SchemaSignatureTruncationBytes = 16;

        // Shared static AssemblyBuilder and ModuleBuilder for all generated types,
        // preventing unloadable assembly accumulation when types are evicted from cache.
        private static readonly AssemblyBuilder _sharedAssembly;
        private static readonly ModuleBuilder _sharedModule;
        private static readonly object _moduleBuilderLock = new();
        private static long _typeCounter;

        static DynamicEntityTypeGenerator()
        {
            var assemblyName = new AssemblyName(DynamicAssemblyName);
            _sharedAssembly = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
            _sharedModule = _sharedAssembly.DefineDynamicModule(DynamicModuleName);
        }

        /// <summary>
        /// Generates a CLR type representing the specified table asynchronously.
        /// Uses the shared <see cref="ModuleBuilder"/> to avoid per-type assembly allocation.
        /// </summary>
        /// <param name="connection">Database connection. Will be opened if not already open.</param>
        /// <param name="tableName">Name of the table to generate. May include schema (schema.table).</param>
        /// <returns>The generated <see cref="Type"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or whitespace.</exception>
        [RequiresDynamicCode("Dynamic scaffolding emits CLR types at runtime and is not supported under NativeAOT or runtimes where dynamic code is disabled.")]
        [RequiresUnreferencedCode("Dynamic scaffolding reflects database schema into runtime-generated entity types and is not trim-safe.")]
        public async Task<Type> GenerateEntityTypeAsync(DbConnection connection, string tableName)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                await connection.OpenAsync().ConfigureAwait(false);

            try
            {
                var (schemaName, bareTable, columns) = ResolveTableSchema(connection, tableName);
                // Materialize columns eagerly so the reader is closed before type building begins.
                return BuildDynamicType(schemaName, bareTable, columns);
            }
            finally
            {
                if (!connectionWasOpen)
                    await connection.CloseAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Generates a CLR type representing the specified table synchronously.
        /// Uses the shared <see cref="ModuleBuilder"/> to avoid per-type assembly allocation.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="tableName">Name of the table to generate. May include schema (schema.table).</param>
        /// <returns>The generated <see cref="Type"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or whitespace.</exception>
        [RequiresDynamicCode("Dynamic scaffolding emits CLR types at runtime and is not supported under NativeAOT or runtimes where dynamic code is disabled.")]
        [RequiresUnreferencedCode("Dynamic scaffolding reflects database schema into runtime-generated entity types and is not trim-safe.")]
        public Type GenerateEntityType(DbConnection connection, string tableName)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                connection.Open();

            try
            {
                var (schemaName, bareTable, columns) = ResolveTableSchema(connection, tableName);
                // Materialize columns eagerly so the reader is closed before type building begins.
                return BuildDynamicType(schemaName, bareTable, columns);
            }
            finally
            {
                if (!connectionWasOpen)
                    connection.Close();
            }
        }

        /// <summary>
        /// Builds a dynamic CLR type from the given column descriptors using the shared <see cref="ModuleBuilder"/>.
        /// Each invocation generates a uniquely-named type to prevent conflicts when the same table
        /// is regenerated after a schema change.
        /// </summary>
        private static Type BuildDynamicType(string? schemaName, string tableName, IReadOnlyList<ColumnInfo> columns)
        {
            lock (_moduleBuilderLock)
            {
                var className = EscapeCSharpIdentifier(ToPascalCase(tableName));

                // Generate unique type name to avoid conflicts when same table is regenerated
                var typeId = Interlocked.Increment(ref _typeCounter);
                var uniqueTypeName = $"{DynamicTypeNamespace}.{className}_{typeId}";

                // Create type using shared module
                var typeBuilder = _sharedModule.DefineType(
                    uniqueTypeName,
                    TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.Sealed,
                    typeof(object));

                // Add parameterless constructor
                typeBuilder.DefineDefaultConstructor(
                    MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName);

                var tableAttrCtor = typeof(TableAttribute).GetConstructor(new[] { typeof(string) })!;
                if (schemaName is null)
                {
                    typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(tableAttrCtor, new object[] { tableName }));
                }
                else
                {
                    var schemaProperty = typeof(TableAttribute).GetProperty(nameof(TableAttribute.Schema))!;
                    typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(
                        tableAttrCtor,
                        new object[] { tableName },
                        new[] { schemaProperty },
                        new object[] { schemaName }));
                }

                // Add properties for each column
                foreach (var col in columns)
                {
                    var propertyType = col.PropertyType;
                    var fieldBuilder = typeBuilder.DefineField($"_{col.PropertyName}", propertyType, FieldAttributes.Private);
                    var propertyBuilder = typeBuilder.DefineProperty(col.PropertyName, PropertyAttributes.HasDefault, propertyType, null);

                    // Add [Column] attribute mapping to the original database column name
                    var columnAttrCtor = typeof(ColumnAttribute).GetConstructor(new[] { typeof(string) })!;
                    var columnAttr = new CustomAttributeBuilder(columnAttrCtor, new object[] { col.ColumnName });
                    propertyBuilder.SetCustomAttribute(columnAttr);

                    // Add [Key] attribute for primary key columns
                    if (col.IsKey)
                    {
                        var keyAttrCtor = typeof(KeyAttribute).GetConstructor(Type.EmptyTypes)!;
                        var keyAttr = new CustomAttributeBuilder(keyAttrCtor, Array.Empty<object>());
                        propertyBuilder.SetCustomAttribute(keyAttr);
                    }

                    // Add database-generated attribute for identity/computed columns.
                    if (col.IsAuto || col.IsComputed)
                    {
                        var dbGenAttrCtor = typeof(DatabaseGeneratedAttribute).GetConstructor(new[] { typeof(DatabaseGeneratedOption) })!;
                        var option = col.IsAuto ? DatabaseGeneratedOption.Identity : DatabaseGeneratedOption.Computed;
                        var dbGenAttr = new CustomAttributeBuilder(dbGenAttrCtor, new object[] { option });
                        propertyBuilder.SetCustomAttribute(dbGenAttr);
                    }

                    // Add [MaxLength] attribute for string columns with a known size
                    if (col.MaxLength.HasValue)
                    {
                        var maxLenAttrCtor = typeof(MaxLengthAttribute).GetConstructor(new[] { typeof(int) })!;
                        var maxLenAttr = new CustomAttributeBuilder(maxLenAttrCtor, new object[] { col.MaxLength.Value });
                        propertyBuilder.SetCustomAttribute(maxLenAttr);
                    }

                    // Define getter
                    var getMethod = typeBuilder.DefineMethod(
                        $"get_{col.PropertyName}",
                        MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                        propertyType,
                        Type.EmptyTypes);

                    var getIl = getMethod.GetILGenerator();
                    getIl.Emit(OpCodes.Ldarg_0);
                    getIl.Emit(OpCodes.Ldfld, fieldBuilder);
                    getIl.Emit(OpCodes.Ret);

                    // Define setter
                    var setMethod = typeBuilder.DefineMethod(
                        $"set_{col.PropertyName}",
                        MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                        null,
                        new[] { propertyType });

                    var setIl = setMethod.GetILGenerator();
                    setIl.Emit(OpCodes.Ldarg_0);
                    setIl.Emit(OpCodes.Ldarg_1);
                    setIl.Emit(OpCodes.Stfld, fieldBuilder);
                    setIl.Emit(OpCodes.Ret);

                    propertyBuilder.SetGetMethod(getMethod);
                    propertyBuilder.SetSetMethod(setMethod);
                }

                return typeBuilder.CreateType()!;
            }
        }

        /// <summary>
        /// Computes a stable hash string that represents the schema of the specified table.
        /// The signature is derived from ordered column names, their CLR types, primary-key status,
        /// nullability, identity, and length metadata. Including this in the dynamic-type cache key ensures
        /// that schema changes (added columns, changed types, generated attributes) produce a new cache entry
        /// rather than returning a stale type.
        /// Uses SHA-256 truncated to <see cref="SchemaSignatureTruncationBytes"/> bytes (32-char hex)
        /// for negligible collision probability.
        /// </summary>
        /// <param name="connection">Open database connection used to probe the schema.</param>
        /// <param name="tableName">Possibly schema-qualified table name.</param>
        /// <returns>A hex string fingerprint of the column descriptors in ordinal order.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is null or whitespace.</exception>
        public string ComputeSchemaSignature(DbConnection connection, string tableName)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                connection.Open();
            try
            {
                var (_, _, columns) = ResolveTableSchema(connection, tableName);
                // Include every metadata field that changes the emitted CLR surface. Omitting one can return
                // a stale dynamic type after a schema change even though the generated C# would differ.
                var descriptor = BuildSchemaDescriptor(columns);
                var hash = SHA256.HashData(Encoding.UTF8.GetBytes(descriptor));
                return Convert.ToHexString(hash[..SchemaSignatureTruncationBytes]);
            }
            finally
            {
                if (!connectionWasOpen)
                    connection.Close();
            }
        }

        private static string BuildSchemaDescriptor(IReadOnlyList<ColumnInfo> columns)
        {
            var sb = new StringBuilder();
            foreach (var column in columns)
            {
                AppendDescriptorPart(sb, column.ColumnName);
                AppendDescriptorPart(sb, column.PropertyType.FullName ?? string.Empty);
                AppendDescriptorPart(sb, column.IsKey ? "PK" : "C");
                AppendDescriptorPart(sb, column.AllowsNull ? "N" : "NN");
                AppendDescriptorPart(sb, column.IsAuto ? "AI" : "NA");
                AppendDescriptorPart(sb, column.IsComputed ? "CMP" : "NCMP");
                AppendDescriptorPart(sb, column.MaxLength?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "-");
            }

            return sb.ToString();
        }

        private static void AppendDescriptorPart(StringBuilder builder, string value)
        {
            builder.Append(value.Length.ToString(System.Globalization.CultureInfo.InvariantCulture));
            builder.Append(':');
            builder.Append(value);
            builder.Append(';');
        }

        private static IEnumerable<ColumnInfo> GetTableSchema(DbConnection connection, string? schemaName, string tableName)
        {
            var qualified = EscapeQualified(connection, schemaName, tableName);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {qualified} WHERE 1=0";
            using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
            var schema = reader.GetSchemaTable();
            if (schema is null)
                yield break;
            var existingPropertyNames = CreateReservedMemberNameSet();
            var sqliteComputedColumns = GetSqliteComputedColumns(connection, schemaName, tableName);
            foreach (DataRow row in schema.Rows)
            {
                var colName = row["ColumnName"]?.ToString();
                if (string.IsNullOrEmpty(colName))
                    continue;
                var propName = MakeUnique(EscapeCSharpIdentifier(ToPascalCase(colName)), existingPropertyNames);
                if (row["DataType"] is not Type clrType)
                    continue;
                var allowNull = row["AllowDBNull"] is bool b && b;

                var propertyType = GetPropertyType(clrType, allowNull);

                var isKey = schema.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
                var isAuto = schema.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai;
                var isComputed = (schema.Columns.Contains("IsExpression") && row["IsExpression"] is bool expression && expression)
                    || sqliteComputedColumns.Contains(colName);

                int? maxLength = null;
                if (clrType == typeof(string) && schema.Columns.Contains("ColumnSize") && row["ColumnSize"] != DBNull.Value)
                {
                    if (int.TryParse(row["ColumnSize"]?.ToString(), out var size) && size > 0)
                        maxLength = size;
                }

                yield return new ColumnInfo(colName, propName, propertyType, allowNull, isKey, isAuto, isComputed, maxLength);
            }
        }

        private static IReadOnlySet<string> GetSqliteComputedColumns(DbConnection connection, string? schemaName, string tableName)
        {
            if (!connection.GetType().Name.Contains("Sqlite", StringComparison.OrdinalIgnoreCase))
                return new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                ? string.Empty
                : EscapeIdentifier(connection, schemaName!) + ".";
            cmd.CommandText = $"PRAGMA {schemaPrefix}table_xinfo({EscapeIdentifier(connection, tableName)})";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (!ReaderHasColumn(reader, "hidden")
                    || !ReaderHasColumn(reader, "name")
                    || Convert.ToInt32(reader["hidden"], System.Globalization.CultureInfo.InvariantCulture) is not (2 or 3))
                {
                    continue;
                }

                var name = Convert.ToString(reader["name"]);
                if (!string.IsNullOrWhiteSpace(name))
                    result.Add(name);
            }

            return result;
        }

        private static (string? schema, string table) SplitSchema(string identifier)
        {
            var idx = identifier.LastIndexOf('.');
            if (idx > 0 && idx < identifier.Length - 1)
                return (identifier[..idx], identifier[(idx + 1)..]);
            return (null, identifier);
        }

        private static (string? schema, string table, List<ColumnInfo> columns) ResolveTableSchema(DbConnection connection, string tableName)
        {
            if (tableName.Contains('.', StringComparison.Ordinal))
            {
                var exactFound = TryGetTableSchema(connection, null, tableName, out var exactColumns);
                var (schemaName, bareTable) = SplitSchema(tableName);
                var schemaFound = TryGetTableSchema(connection, schemaName, bareTable, out var schemaColumns);

                if (exactFound && schemaFound)
                {
                    throw new NormConfigurationException(
                        $"Dynamic scaffolding table name '{tableName}' is ambiguous: it matches both a literal table name and a schema-qualified table. " +
                        "Use a typed model or remove the naming collision before using Query(string).");
                }

                if (exactFound)
                    return (null, tableName, exactColumns);

                if (schemaFound)
                    return (schemaName, bareTable, schemaColumns);
            }

            var (fallbackSchemaName, fallbackBareTable) = SplitSchema(tableName);
            var columns = GetTableSchema(connection, fallbackSchemaName, fallbackBareTable).ToList();
            return (fallbackSchemaName, fallbackBareTable, columns);
        }

        private static bool TryGetTableSchema(DbConnection connection, string? schemaName, string tableName, out List<ColumnInfo> columns)
        {
            try
            {
                columns = GetTableSchema(connection, schemaName, tableName).ToList();
                return true;
            }
            catch (DbException)
            {
                columns = new List<ColumnInfo>();
                return false;
            }
        }

        private static string EscapeQualified(DbConnection connection, string? schema, string table)
        {
            return string.IsNullOrEmpty(schema)
                ? EscapeIdentifier(connection, table)
                : $"{EscapeIdentifier(connection, schema!)}.{EscapeIdentifier(connection, table)}";
        }

        private static bool ReaderHasColumn(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Wraps a raw SQL identifier in the appropriate quoting characters for the provider.
        /// Escapes embedded quoting characters so legitimate identifiers are preserved
        /// without allowing crafted table or schema names to break out of the quote.
        /// </summary>
        private static string EscapeIdentifier(DbConnection connection, string identifier)
        {
            var name = connection.GetType().Name.ToLowerInvariant();
            return name switch
            {
                var n when n.Contains("sqlconnection") => $"[{identifier.Replace("]", "]]")}]",
                var n when n.Contains("sqlite") => $"\"{identifier.Replace("\"", "\"\"")}\"",
                var n when n.Contains("npgsql") => $"\"{identifier.Replace("\"", "\"\"")}\"",
                var n when n.Contains("mysql") => $"`{identifier.Replace("`", "``")}`",
                _ => $"\"{identifier.Replace("\"", "\"\"")}\""
            };
        }

        /// <summary>
        /// Maps a CLR type and its nullability to the property type for the generated entity.
        /// Value types that allow null are wrapped in <see cref="Nullable{T}"/>; reference types
        /// are returned as-is since nullability is implicit.
        /// </summary>
        private static Type GetPropertyType(Type type, bool allowNull)
        {
            if (!type.IsValueType)
                return type;

            if (allowNull)
                return typeof(Nullable<>).MakeGenericType(type);

            return type;
        }

        private static string ToPascalCase(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return name;

            var sb = new StringBuilder(name.Length);
            var segmentStart = 0;
            for (var i = 0; i <= name.Length; i++)
            {
                if (i < name.Length && char.IsLetterOrDigit(name[i]))
                    continue;

                AppendPascalSegment(sb, name.AsSpan(segmentStart, i - segmentStart));
                segmentStart = i + 1;
            }

            return sb.ToString();
        }

        private static string MakeUnique(string baseName, HashSet<string> existingNames)
        {
            var candidate = string.IsNullOrWhiteSpace(baseName) ? "_" : baseName;
            var unique = candidate;
            var suffix = 2;
            while (existingNames.Contains(unique))
            {
                unique = candidate + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);
                suffix++;
            }

            existingNames.Add(unique);
            return unique;
        }

        private static HashSet<string> CreateReservedMemberNameSet()
            => typeof(object)
                .GetMembers(BindingFlags.Instance | BindingFlags.Public)
                .Select(member => member.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static void AppendPascalSegment(StringBuilder sb, ReadOnlySpan<char> segment)
        {
            if (segment.IsEmpty)
                return;

            var hasLower = false;
            for (var i = 0; i < segment.Length; i++)
            {
                if (char.IsLower(segment[i]))
                {
                    hasLower = true;
                    break;
                }
            }

            sb.Append(char.ToUpperInvariant(segment[0]));
            for (var i = 1; i < segment.Length; i++)
            {
                sb.Append(hasLower ? segment[i] : char.ToLowerInvariant(segment[i]));
            }
        }

        /// <summary>
        /// Escapes a candidate C# identifier so it is valid syntax. Reserved keywords and common
        /// contextual keywords are prefixed with <c>@</c>; other invalid characters are replaced
        /// with underscores.
        /// </summary>
        private static string EscapeCSharpIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier)) return "_";

            var sb = new StringBuilder(identifier.Length + 1);
            for (var i = 0; i < identifier.Length; i++)
            {
                var ch = identifier[i];
                var valid = i == 0
                    ? char.IsLetter(ch) || ch == '_'
                    : char.IsLetterOrDigit(ch) || ch == '_';

                if (valid)
                    sb.Append(ch);
                else if (i == 0 && char.IsDigit(ch))
                    sb.Append('_').Append(ch);
                else
                    sb.Append('_');
            }

            if (sb.Length == 0)
                sb.Append('_');

            var escaped = sb.ToString();
            return _csharpKeywords.Contains(escaped) ? "@" + escaped : escaped;
        }

        /// <summary>
        /// Set of C# reserved keywords and common contextual keywords that require escaping
        /// with the <c>@</c> verbatim prefix when used as identifiers.
        /// </summary>
        private static readonly HashSet<string> _csharpKeywords = new(StringComparer.Ordinal)
        {
            // Reserved keywords
            "abstract","as","base","bool","break","byte","case","catch","char","checked","class","const",
            "continue","decimal","default","delegate","do","double","else","enum","event","explicit","extern",
            "false","finally","fixed","float","for","foreach","goto","if","implicit","in","int","interface",
            "internal","is","lock","long","namespace","new","null","object","operator","out","override","params",
            "private","protected","public","readonly","ref","return","sbyte","sealed","short","sizeof","stackalloc",
            "static","string","struct","switch","this","throw","true","try","typeof","uint","ulong","unchecked",
            "unsafe","ushort","using","virtual","void","volatile","while",
            // Contextual keywords commonly used as identifiers in database column names
            "record","partial","var","dynamic","async","await","nameof","when","and","or","not","with",
            "init","required","file","scoped","global","managed","unmanaged","nint","nuint","value","yield"
        };
    }
}
