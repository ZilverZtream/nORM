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

namespace nORM.Scaffolding
{
    /// <summary>
    /// Generates entity types at runtime based on database schema information.
    /// </summary>
    public class DynamicEntityTypeGenerator
    {
        private sealed record ColumnInfo(string PropertyName, string TypeName);

        /// <summary>
        /// Generates a CLR type representing the specified table.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="tableName">Name of the table to generate.</param>
        /// <returns>The generated <see cref="Type"/>.</returns>
        public Type GenerateEntityType(DbConnection connection, string tableName)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(tableName));
            if (connection.State != ConnectionState.Open)
                connection.Open();

            var className = ToPascalCase(tableName);
            var columns = GetTableSchema(connection, tableName);

            var sb = new StringBuilder();
            sb.AppendLine("using System;");
            sb.AppendLine("using System.ComponentModel.DataAnnotations;");
            sb.AppendLine($"namespace nORM.Dynamic {{ public class {className} {{");
            foreach (var col in columns)
            {
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

        private static IEnumerable<ColumnInfo> GetTableSchema(DbConnection connection, string tableName)
        {
            var escaped = EscapeIdentifier(connection, tableName);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {escaped} WHERE 1=0";
            using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
            var schema = reader.GetSchemaTable()!;
            foreach (DataRow row in schema.Rows)
            {
                var columnName = row["ColumnName"]!.ToString()!;
                var propName = ToPascalCase(columnName);
                var clrType = (Type)row["DataType"]!;
                var allowNull = row["AllowDBNull"] is bool b && b;
                var typeName = GetTypeName(clrType, allowNull && clrType.IsValueType);
                yield return new ColumnInfo(propName, typeName);
            }
        }

        private static string EscapeIdentifier(DbConnection connection, string identifier)
        {
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

        private static string ToPascalCase(string name)
        {
            var parts = name.Split(new[] { '_', ' ' }, StringSplitOptions.RemoveEmptyEntries);
            var sb = new StringBuilder();
            foreach (var part in parts)
            {
                if (part.Length == 0) continue;
                sb.Append(char.ToUpperInvariant(part[0]));
                if (part.Length > 1)
                    sb.Append(part[1..].ToLowerInvariant());
            }
            return sb.ToString();
        }

        private static string GetTypeName(Type type, bool nullable)
        {
            var name = type switch
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
            if (nullable && name != "string")
                name += "?";
            return name;
        }
    }
}
