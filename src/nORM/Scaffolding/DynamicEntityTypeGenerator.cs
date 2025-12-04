using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Generates entity types at runtime based on database schema information.
    /// MEMORY LEAK FIX: Reuses a single ModuleBuilder instead of creating new assemblies per type.
    /// </summary>
    public class DynamicEntityTypeGenerator
    {
        private sealed record ColumnInfo(string ColumnName, string PropertyName, Type PropertyType, bool IsKey, bool IsAuto, int? MaxLength);

        // MEMORY LEAK FIX: Shared static AssemblyBuilder and ModuleBuilder for all generated types
        // This prevents unloadable assembly accumulation when types are evicted from cache
        private static readonly AssemblyBuilder _sharedAssembly;
        private static readonly ModuleBuilder _sharedModule;
        private static int _typeCounter = 0;

        static DynamicEntityTypeGenerator()
        {
            // Initialize shared assembly and module once for all dynamic types
            var assemblyName = new AssemblyName("nORM.Dynamic.Entities");
            _sharedAssembly = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
            _sharedModule = _sharedAssembly.DefineDynamicModule("MainModule");
        }

        /// <summary>
        /// Generates a CLR type representing the specified table asynchronously.
        /// MEMORY LEAK FIX: Uses shared ModuleBuilder instead of creating new assemblies.
        /// </summary>
        /// <param name="connection">Database connection.</param>
        /// <param name="tableName">Name of the table to generate. May include schema (schema.table).</param>
        /// <returns>The generated <see cref="Type"/>.</returns>
        public async Task<Type> GenerateEntityTypeAsync(DbConnection connection, string tableName)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync().ConfigureAwait(false);

            var (schemaName, bareTable) = SplitSchema(tableName);
            var columns = GetTableSchema(connection, schemaName, bareTable);

            return BuildDynamicType(tableName, columns);
        }

        /// <summary>
        /// Generates a CLR type representing the specified table.
        /// MEMORY LEAK FIX: Uses shared ModuleBuilder instead of creating new assemblies.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="tableName">Name of the table to generate. May include schema (schema.table).</param>
        /// <returns>The generated <see cref="Type"/>.</returns>
        public Type GenerateEntityType(DbConnection connection, string tableName)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            if (connection.State != ConnectionState.Open)
                connection.Open();

            var (schemaName, bareTable) = SplitSchema(tableName);
            var columns = GetTableSchema(connection, schemaName, bareTable);

            return BuildDynamicType(tableName, columns);
        }

        /// <summary>
        /// MEMORY LEAK FIX: Builds a dynamic type using the shared ModuleBuilder.
        /// All generated types reuse the same assembly and module, preventing memory leaks.
        /// </summary>
        private static Type BuildDynamicType(string tableName, IEnumerable<ColumnInfo> columns)
        {
            var className = EscapeCSharpIdentifier(ToPascalCase(GetUnqualifiedName(tableName)));

            // Generate unique type name to avoid conflicts when same table is regenerated
            var typeId = Interlocked.Increment(ref _typeCounter);
            var uniqueTypeName = $"nORM.Dynamic.{className}_{typeId}";

            // Create type using shared module
            var typeBuilder = _sharedModule.DefineType(
                uniqueTypeName,
                TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.Sealed,
                typeof(object));

            // Add parameterless constructor
            var ctor = typeBuilder.DefineDefaultConstructor(
                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName);

            // Add properties for each column
            foreach (var col in columns)
            {
                var propertyType = col.PropertyType;
                var fieldBuilder = typeBuilder.DefineField($"_{col.PropertyName}", propertyType, FieldAttributes.Private);
                var propertyBuilder = typeBuilder.DefineProperty(col.PropertyName, PropertyAttributes.HasDefault, propertyType, null);

                // Add [Column] attribute
                var columnAttrCtor = typeof(ColumnAttribute).GetConstructor(new[] { typeof(string) })!;
                var columnAttr = new CustomAttributeBuilder(columnAttrCtor, new object[] { col.ColumnName });
                propertyBuilder.SetCustomAttribute(columnAttr);

                // Add [Key] attribute if needed
                if (col.IsKey)
                {
                    var keyAttrCtor = typeof(KeyAttribute).GetConstructor(Type.EmptyTypes)!;
                    var keyAttr = new CustomAttributeBuilder(keyAttrCtor, Array.Empty<object>());
                    propertyBuilder.SetCustomAttribute(keyAttr);
                }

                // Add [DatabaseGenerated] attribute if needed
                if (col.IsAuto)
                {
                    var dbGenAttrCtor = typeof(DatabaseGeneratedAttribute).GetConstructor(new[] { typeof(DatabaseGeneratedOption) })!;
                    var dbGenAttr = new CustomAttributeBuilder(dbGenAttrCtor, new object[] { DatabaseGeneratedOption.Identity });
                    propertyBuilder.SetCustomAttribute(dbGenAttr);
                }

                // Add [MaxLength] attribute if needed
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

        private static IEnumerable<ColumnInfo> GetTableSchema(DbConnection connection, string? schemaName, string tableName)
        {
            var qualified = EscapeQualified(connection, schemaName, tableName);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {qualified} WHERE 1=0";
            using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
            var schema = reader.GetSchemaTable()!;
            foreach (DataRow row in schema.Rows)
            {
                var colName = row["ColumnName"]!.ToString()!;
                var propName = EscapeCSharpIdentifier(ToPascalCase(colName));
                var clrType = (Type)row["DataType"]!;
                var allowNull = row["AllowDBNull"] is bool b && b;

                // MEMORY LEAK FIX: Return actual Type instead of string
                var propertyType = GetPropertyType(clrType, allowNull);

                var isKey = schema.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
                var isAuto = schema.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai;

                int? maxLength = null;
                if (clrType == typeof(string) && schema.Columns.Contains("ColumnSize") && row["ColumnSize"] != DBNull.Value)
                {
                    if (int.TryParse(row["ColumnSize"]!.ToString(), out var size) && size > 0)
                        maxLength = size;
                }

                yield return new ColumnInfo(colName, propName, propertyType, isKey, isAuto, maxLength);
            }
        }

        private static (string? schema, string table) SplitSchema(string identifier)
        {
            var idx = identifier.LastIndexOf('.');
            if (idx > 0)
                return (identifier[..idx], identifier[(idx + 1)..]);
            return (null, identifier);
        }

        private static string EscapeQualified(DbConnection connection, string? schema, string table)
        {
            return string.IsNullOrEmpty(schema)
                ? EscapeIdentifier(connection, table)
                : $"{EscapeIdentifier(connection, schema!)}.{EscapeIdentifier(connection, table)}";
        }

        private static string EscapeIdentifier(DbConnection connection, string identifier)
        {
            // Support schema-qualified parts by escaping each component if needed.
            var name = connection.GetType().Name.ToLowerInvariant();
            return name switch
            {
                var n when n.Contains("sqlconnection") => $"[{identifier}]",
                var n when n.Contains("sqlite") => $"\"{identifier}\"",
                var n when n.Contains("npgsql") => $"\"{identifier}\"",
                var n when n.Contains("mysql") => $"`{identifier}`",
                _ => identifier
            };
        }

        private static string GetUnqualifiedName(string identifier)
        {
            var idx = identifier.LastIndexOf('.');
            return idx >= 0 ? identifier[(idx + 1)..] : identifier;
        }

        /// <summary>
        /// MEMORY LEAK FIX: Returns actual Type for properties instead of string representation.
        /// This allows TypeBuilder to work with actual types.
        /// </summary>
        private static Type GetPropertyType(Type type, bool allowNull)
        {
            // For reference types (including string), nullability is implicit
            if (!type.IsValueType)
            {
                return type;
            }

            // For value types that allow null, wrap in Nullable<T>
            if (allowNull)
            {
                return typeof(Nullable<>).MakeGenericType(type);
            }

            return type;
        }

        private static string ToPascalCase(string name)
        {
            var parts = name.Split(new[] { '_', ' ' }, StringSplitOptions.RemoveEmptyEntries);
            var result = string.Empty;
            foreach (var part in parts)
            {
                if (part.Length > 0)
                {
                    result += char.ToUpperInvariant(part[0]);
                    if (part.Length > 1)
                        result += part[1..].ToLowerInvariant();
                }
            }
            return result;
        }

        private static string EscapeCSharpIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier)) return identifier;
            bool needsAt = _csharpKeywords.Contains(identifier)
                           || !(char.IsLetter(identifier[0]) || identifier[0] == '_')
                           || identifier.Any(ch => !(char.IsLetterOrDigit(ch) || ch == '_'));
            return needsAt ? "@" + identifier : identifier;
        }

        private static readonly HashSet<string> _csharpKeywords = new(StringComparer.Ordinal)
        {
            "abstract","as","base","bool","break","byte","case","catch","char","checked","class","const",
            "continue","decimal","default","delegate","do","double","else","enum","event","explicit","extern",
            "false","finally","fixed","float","for","foreach","goto","if","implicit","in","int","interface",
            "internal","is","lock","long","namespace","new","null","object","operator","out","override","params",
            "private","protected","public","readonly","ref","return","sbyte","sealed","short","sizeof","stackalloc",
            "static","string","struct","switch","this","throw","true","try","typeof","uint","ulong","unchecked",
            "unsafe","ushort","using","virtual","void","volatile","while","record","partial","var","dynamic"
        };
    }
}
