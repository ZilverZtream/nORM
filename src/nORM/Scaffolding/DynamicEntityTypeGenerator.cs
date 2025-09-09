using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Generates entity types at runtime based on database schema information.
    /// </summary>
    public class DynamicEntityTypeGenerator
    {
        private sealed record ColumnInfo(string ColumnName, string PropertyName, string TypeName, bool IsKey, bool IsAuto, int? MaxLength);
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        /// <summary>
        /// Generates a CLR type representing the specified table.
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

            var className = EscapeCSharpIdentifier(ToPascalCase(GetUnqualifiedName(tableName)));
            var (schemaName, bareTable) = SplitSchema(tableName);
            var columns = GetTableSchema(connection, schemaName, bareTable);

            var sb = _stringBuilderPool.Get();
            try
            {
                sb.AppendLine("using System;");
                sb.AppendLine("using System.ComponentModel.DataAnnotations;");
                sb.AppendLine("using System.ComponentModel.DataAnnotations.Schema;");
                sb.AppendLine($"namespace nORM.Dynamic {{ public class {className} {{");

                foreach (var col in columns)
                {
                    if (col.IsKey) sb.AppendLine("    [Key]");
                    if (col.IsAuto) sb.AppendLine("    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]");
                    if (col.MaxLength.HasValue) sb.AppendLine($"    [MaxLength({col.MaxLength.Value})]");
                    sb.AppendLine($"    [Column(\"{col.ColumnName}\")]");
                    sb.AppendLine($"    public {col.TypeName} {col.PropertyName} {{ get; set; }}");
                }

                sb.AppendLine("} }");
                var syntaxTree = CSharpSyntaxTree.ParseText(sb.ToString());

                var references = AppDomain.CurrentDomain.GetAssemblies()
                    .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
                    .Select(a => MetadataReference.CreateFromFile(a.Location));

                var compilation = CSharpCompilation.Create(
                    "nORM.Dynamic.Entities",
                    new[] { syntaxTree },
                    references,
                    new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

                using var ms = new MemoryStream();
                var result = compilation.Emit(ms);
                if (!result.Success)
                {
                    var errors = string.Join(Environment.NewLine, result.Diagnostics
                        .Where(d => d.Severity == DiagnosticSeverity.Error)
                        .Select(d => d.ToString()));
                    throw new InvalidOperationException($"Failed to generate dynamic entity type. {errors}");
                }
                ms.Seek(0, SeekOrigin.Begin);
                var assembly = Assembly.Load(ms.ToArray());
                return assembly.GetType($"nORM.Dynamic.{className}")!;
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
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
                var typeName = GetTypeName(clrType, allowNull);

                var isKey = schema.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
                var isAuto = schema.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai;

                int? maxLength = null;
                if (clrType == typeof(string) && schema.Columns.Contains("ColumnSize") && row["ColumnSize"] != DBNull.Value)
                {
                    if (int.TryParse(row["ColumnSize"]!.ToString(), out var size) && size > 0)
                        maxLength = size;
                }

                yield return new ColumnInfo(colName, propName, typeName, isKey, isAuto, maxLength);
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

        private static string GetTypeName(Type type, bool allowNull)
        {
            string name = type == typeof(byte[]) ? "byte[]" : type switch
            {
                var t when t == typeof(int) => "int",
                var t when t == typeof(long) => "long",
                var t when t == typeof(short) => "short",
                var t when t == typeof(byte) => "byte",
                var t when t == typeof(bool) => "bool",
                var t when t == typeof(string) => "string",
                var t when t == typeof(DateTime) => "DateTime",
                var t when t == typeof(decimal) => "decimal",
                var t when t == typeof(double) => "double",
                var t when t == typeof(float) => "float",
                var t when t == typeof(Guid) => "Guid",
                _ => type.FullName ?? type.Name
            };
            if (allowNull)
            {
                if (name.EndsWith("[]", StringComparison.Ordinal))
                    name += "?";
                else if (type.IsValueType || type == typeof(string))
                    name += "?";
                else
                    name += "?";
            }
            return name;
        }

        private static string ToPascalCase(string name)
        {
            var parts = name.Split(new[] { '_', ' ' }, StringSplitOptions.RemoveEmptyEntries);
            var sb = _stringBuilderPool.Get();
            try
            {
                foreach (var part in parts)
                {
                    sb.Append(char.ToUpperInvariant(part[0]));
                    if (part.Length > 1)
                        sb.Append(part[1..].ToLowerInvariant());
                }
                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
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
