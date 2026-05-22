using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading;
using nORM.Migration;

namespace nORM.Cli;

public static class MigrationCodeWriter
{
    public static string WriteMigrationSource(string className, long version, string name, MigrationSqlStatements sql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(className);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(sql);

        var sb = new StringBuilder();
        sb.AppendLine("using System.Data.Common;");
        sb.AppendLine("using System.Threading;");
        sb.AppendLine("using nORM.Migration;");
        sb.AppendLine();
        sb.AppendLine($"public class {className} : Migration");
        sb.AppendLine("{");
        sb.AppendLine($"    public {className}() : base({version}, {ToCSharpStringLiteral(name)}) {{ }}");
        AppendMethod("Up", sql.PreTransactionUp, sql.Up, sql.PostTransactionUp, sb);
        AppendMethod("Down", sql.PreTransactionDown, sql.Down, sql.PostTransactionDown, sb);
        sb.AppendLine("}");
        return sb.ToString();
    }

    public static string ToCSharpStringLiteral(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var builder = new StringBuilder(value.Length + 2);
        builder.Append('"');
        foreach (var ch in value)
        {
            builder.Append(ch switch
            {
                '\\' => "\\\\",
                '"' => "\\\"",
                '\0' => "\\0",
                '\a' => "\\a",
                '\b' => "\\b",
                '\f' => "\\f",
                '\n' => "\\n",
                '\r' => "\\r",
                '\t' => "\\t",
                '\v' => "\\v",
                _ when char.IsControl(ch) => "\\u" + ((int)ch).ToString("x4"),
                _ => ch.ToString()
            });
        }
        builder.Append('"');
        return builder.ToString();
    }

    private static void AppendMethod(
        string methodName,
        IReadOnlyList<string>? preStatements,
        IReadOnlyList<string> statements,
        IReadOnlyList<string>? postStatements,
        StringBuilder sb)
    {
        sb.AppendLine($"    public override void {methodName}(DbConnection connection, DbTransaction transaction, CancellationToken ct = default)");
        sb.AppendLine("    {");

        if (preStatements != null && preStatements.Count > 0)
        {
            sb.AppendLine("        // Pre-transaction statements: execute outside the transaction.");
            foreach (var pre in preStatements)
            {
                sb.AppendLine("        ct.ThrowIfCancellationRequested();");
                sb.AppendLine($"        using (var preCmd = connection.CreateCommand()) {{ preCmd.CommandText = {ToCSharpStringLiteral(pre)}; preCmd.ExecuteNonQuery(); }}");
            }
        }

        if (statements.Count > 0)
        {
            sb.AppendLine("        foreach (var sql in new[] {");
            for (var i = 0; i < statements.Count; i++)
            {
                var comma = i < statements.Count - 1 ? "," : string.Empty;
                sb.AppendLine($"            {ToCSharpStringLiteral(statements[i])}{comma}");
            }
            sb.AppendLine("        })");
            sb.AppendLine("        {");
            sb.AppendLine("            ct.ThrowIfCancellationRequested();");
            sb.AppendLine("            using var cmd = connection.CreateCommand();");
            sb.AppendLine("            cmd.Transaction = transaction;");
            sb.AppendLine("            cmd.CommandText = sql;");
            sb.AppendLine("            cmd.ExecuteNonQuery();");
            sb.AppendLine("        }");
        }

        if (postStatements != null && postStatements.Count > 0)
        {
            sb.AppendLine("        // Post-transaction statements: execute after the transaction commits.");
            foreach (var post in postStatements)
            {
                sb.AppendLine("        ct.ThrowIfCancellationRequested();");
                sb.AppendLine($"        using (var postCmd = connection.CreateCommand()) {{ postCmd.CommandText = {ToCSharpStringLiteral(post)}; postCmd.ExecuteNonQuery(); }}");
            }
        }

        sb.AppendLine("    }");
    }
}
