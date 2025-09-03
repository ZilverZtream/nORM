using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Provides reverse-engineering utilities that can scaffold entity classes and a DbContext
    /// from an existing database schema.
    /// </summary>
    public static class DatabaseScaffolder
    {
        /// <summary>
        /// Generates entity classes and a DbContext based on the current database schema.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="provider">Database provider implementation.</param>
        /// <param name="outputDirectory">Directory where files will be generated.</param>
        /// <param name="namespaceName">Namespace for the generated classes.</param>
        /// <param name="contextName">Name of the generated DbContext.</param>
        public static async Task ScaffoldAsync(DbConnection connection, DatabaseProvider provider, string outputDirectory, string namespaceName, string contextName = "AppDbContext")
        {
            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync();

            Directory.CreateDirectory(outputDirectory);
            var tables = connection.GetSchema("Tables");
            var entityNames = new List<string>();

            foreach (DataRow table in tables.Rows)
            {
                var tableType = table.Table.Columns.Contains("TABLE_TYPE") ? table["TABLE_TYPE"]?.ToString() : null;
                if (tableType != null && !string.Equals(tableType, "TABLE", StringComparison.OrdinalIgnoreCase))
                    continue;

                var tableName = table["TABLE_NAME"]!.ToString()!;
                var entityName = ToPascalCase(tableName);
                entityNames.Add(entityName);

                var entityCode = await ScaffoldEntityAsync(connection, provider, tableName, entityName, namespaceName);
                File.WriteAllText(Path.Combine(outputDirectory, entityName + ".cs"), entityCode);
            }

            var ctxCode = ScaffoldContext(namespaceName, contextName, entityNames);
            File.WriteAllText(Path.Combine(outputDirectory, contextName + ".cs"), ctxCode);
        }

        private static async Task<string> ScaffoldEntityAsync(DbConnection connection, DatabaseProvider provider, string tableName, string entityName, string namespaceName)
        {
            var sb = new StringBuilder();
            sb.AppendLine("using System;");
            sb.AppendLine("using System.ComponentModel.DataAnnotations;");
            sb.AppendLine("using System.ComponentModel.DataAnnotations.Schema;");
            sb.AppendLine();
            sb.AppendLine($"namespace {namespaceName};");
            sb.AppendLine();
            sb.AppendLine($"[Table(\"{tableName}\")]" );
            sb.AppendLine($"public class {entityName}");
            sb.AppendLine("{");

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {provider.Escape(tableName)} WHERE 1=0";
            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
            var schema = reader.GetSchemaTable()!;
            foreach (DataRow row in schema.Rows)
            {
                var colName = row["ColumnName"]!.ToString()!;
                var propName = ToPascalCase(colName);
                var clrType = (Type)row["DataType"]!;
                var allowNull = row["AllowDBNull"] is bool b && b;
                var isNullable = allowNull && clrType.IsValueType;
                var typeName = GetTypeName(clrType, isNullable);
                var isKey = row.Table.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
                var isAuto = row.Table.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai;

                sb.AppendLine("    /// <summary>");
                sb.AppendLine($"    /// Maps to column {colName}");
                sb.AppendLine("    /// </summary>");
                if (isKey)
                    sb.AppendLine("    [Key]");
                if (isAuto)
                    sb.AppendLine("    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]");
                sb.AppendLine($"    [Column(\"{colName}\")]\n    public {typeName} {propName} {{ get; set; }}\n");
            }

            sb.AppendLine("}");
            return sb.ToString();
        }

        private static string ScaffoldContext(string namespaceName, string contextName, IEnumerable<string> entities)
        {
            var sb = new StringBuilder();
            sb.AppendLine("using System.Data.Common;");
            sb.AppendLine("using nORM.Core;");
            sb.AppendLine("using nORM.Configuration;");
            sb.AppendLine("using nORM.Providers;");
            sb.AppendLine();
            sb.AppendLine($"namespace {namespaceName};");
            sb.AppendLine();
            sb.AppendLine($"public class {contextName} : DbContext");
            sb.AppendLine("{");
            sb.AppendLine($"    public {contextName}(DbConnection cn, DatabaseProvider provider, DbContextOptions? options = null) : base(cn, provider, options) {{ }}\n");
            foreach (var entity in entities.OrderBy(e => e))
                sb.AppendLine($"    public INormQueryable<{entity}> {entity}s => this.Query<{entity}>();");
            sb.AppendLine("}");
            return sb.ToString();
        }

        private static string GetTypeName(Type type, bool nullable)
        {
            string name = type switch
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
    }
}
