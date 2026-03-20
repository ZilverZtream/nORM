using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Generates entity types at runtime based on database schema information.
    /// Reuses a single shared <see cref="ModuleBuilder"/> to prevent unloadable assembly
    /// accumulation when types are evicted from cache.
    /// </summary>
    public class DynamicEntityTypeGenerator
    {
        private sealed record ColumnInfo(string ColumnName, string PropertyName, Type PropertyType, bool IsKey, bool IsAuto, int? MaxLength);

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
        public async Task<Type> GenerateEntityTypeAsync(DbConnection connection, string tableName)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync().ConfigureAwait(false);

            var (schemaName, bareTable) = SplitSchema(tableName);
            // Materialize columns eagerly so the reader is closed before type building begins.
            var columns = GetTableSchema(connection, schemaName, bareTable).ToList();

            return BuildDynamicType(tableName, columns);
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
        public Type GenerateEntityType(DbConnection connection, string tableName)
        {
            ArgumentNullException.ThrowIfNull(connection);
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            if (connection.State != ConnectionState.Open)
                connection.Open();

            var (schemaName, bareTable) = SplitSchema(tableName);
            // Materialize columns eagerly so the reader is closed before type building begins.
            var columns = GetTableSchema(connection, schemaName, bareTable).ToList();

            return BuildDynamicType(tableName, columns);
        }

        /// <summary>
        /// Builds a dynamic CLR type from the given column descriptors using the shared <see cref="ModuleBuilder"/>.
        /// Each invocation generates a uniquely-named type to prevent conflicts when the same table
        /// is regenerated after a schema change.
        /// </summary>
        private static Type BuildDynamicType(string tableName, IReadOnlyList<ColumnInfo> columns)
        {
            var className = EscapeCSharpIdentifier(ToPascalCase(GetUnqualifiedName(tableName)));

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

                // Add [DatabaseGenerated(Identity)] attribute for auto-increment columns
                if (col.IsAuto)
                {
                    var dbGenAttrCtor = typeof(DatabaseGeneratedAttribute).GetConstructor(new[] { typeof(DatabaseGeneratedOption) })!;
                    var dbGenAttr = new CustomAttributeBuilder(dbGenAttrCtor, new object[] { DatabaseGeneratedOption.Identity });
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

        /// <summary>
        /// Computes a stable hash string that represents the schema of the specified table.
        /// The signature is derived from ordered column names, their CLR types, primary-key status,
        /// and nullability. Including this in the dynamic-type cache key ensures that schema changes
        /// (added columns, changed types) produce a new cache entry rather than returning a stale type.
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
            if (connection.State != ConnectionState.Open)
                connection.Open();
            var (schemaName, bareTable) = SplitSchema(tableName);
            var columns = GetTableSchema(connection, schemaName, bareTable).ToList();
            // Include IsNullable and IsPrimaryKey in descriptor so different nullability/key
            // configs produce different signatures.
            var descriptor = string.Join(",", columns.Select(c =>
                $"{c.ColumnName}:{c.PropertyType.FullName}:{(c.IsKey ? "PK" : "C")}:{(IsNullableType(c.PropertyType) ? "N" : "NN")}"));
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(descriptor));
            return Convert.ToHexString(hash[..SchemaSignatureTruncationBytes]);
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
            foreach (DataRow row in schema.Rows)
            {
                var colName = row["ColumnName"]?.ToString();
                if (string.IsNullOrEmpty(colName))
                    continue;
                var propName = EscapeCSharpIdentifier(ToPascalCase(colName));
                if (row["DataType"] is not Type clrType)
                    continue;
                var allowNull = row["AllowDBNull"] is bool b && b;

                var propertyType = GetPropertyType(clrType, allowNull);

                var isKey = schema.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
                var isAuto = schema.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai;

                int? maxLength = null;
                if (clrType == typeof(string) && schema.Columns.Contains("ColumnSize") && row["ColumnSize"] != DBNull.Value)
                {
                    if (int.TryParse(row["ColumnSize"]?.ToString(), out var size) && size > 0)
                        maxLength = size;
                }

                yield return new ColumnInfo(colName, propName, propertyType, isKey, isAuto, maxLength);
            }
        }

        private static (string? schema, string table) SplitSchema(string identifier)
        {
            var idx = identifier.LastIndexOf('.');
            if (idx > 0 && idx < identifier.Length - 1)
                return (identifier[..idx], identifier[(idx + 1)..]);
            return (null, identifier);
        }

        private static string EscapeQualified(DbConnection connection, string? schema, string table)
        {
            return string.IsNullOrEmpty(schema)
                ? EscapeIdentifier(connection, table)
                : $"{EscapeIdentifier(connection, schema!)}.{EscapeIdentifier(connection, table)}";
        }

        /// <summary>
        /// Wraps a raw SQL identifier in the appropriate quoting characters for the provider.
        /// Strips the quoting character from within the value to prevent SQL injection via
        /// crafted table or schema names.
        /// </summary>
        private static string EscapeIdentifier(DbConnection connection, string identifier)
        {
            var name = connection.GetType().Name.ToLowerInvariant();
            return name switch
            {
                var n when n.Contains("sqlconnection") => $"[{identifier.Replace("]", "")}]",
                var n when n.Contains("sqlite") => $"\"{identifier.Replace("\"", "")}\"",
                var n when n.Contains("npgsql") => $"\"{identifier.Replace("\"", "")}\"",
                var n when n.Contains("mysql") => $"`{identifier.Replace("`", "")}`",
                _ => identifier
            };
        }

        private static string GetUnqualifiedName(string identifier)
        {
            var idx = identifier.LastIndexOf('.');
            return idx >= 0 ? identifier[(idx + 1)..] : identifier;
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

        private static bool IsNullableType(Type t) =>
            !t.IsValueType || (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>));

        private static string ToPascalCase(string name)
        {
            var parts = name.Split(new[] { '_', ' ' }, StringSplitOptions.RemoveEmptyEntries);
            var sb = new StringBuilder(name.Length);
            foreach (var part in parts)
            {
                if (part.Length > 0)
                {
                    sb.Append(char.ToUpperInvariant(part[0]));
                    if (part.Length > 1)
                        sb.Append(part[1..].ToLowerInvariant());
                }
            }
            return sb.ToString();
        }

        /// <summary>
        /// Escapes a candidate C# identifier so it is valid syntax. Reserved keywords and common
        /// contextual keywords are prefixed with <c>@</c>. Identifiers that start with a digit or
        /// contain non-alphanumeric characters are also prefixed. An empty or null input returns
        /// <c>"_"</c> as a safe fallback since an empty string is not a valid C# identifier.
        /// </summary>
        private static string EscapeCSharpIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier)) return "_";
            bool needsAt = _csharpKeywords.Contains(identifier)
                           || !(char.IsLetter(identifier[0]) || identifier[0] == '_')
                           || identifier.Any(ch => !(char.IsLetterOrDigit(ch) || ch == '_'));
            return needsAt ? "@" + identifier : identifier;
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
