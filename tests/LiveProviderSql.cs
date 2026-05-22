using System;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;

#nullable enable

namespace nORM.Tests;

internal static class LiveProviderSql
{
    private static readonly string[] Identifiers =
    {
        "__NormMigrationsHistory",
        "CP_TenantRow", "CP_BatchItem", "CP_MetaChar",
        "G40_TenantItem", "G40_OccItem", "G40_OccStr", "G40_Item", "G40_SP", "G40_NoKey",
        "LPM_TenantItem", "LPM_OccItem", "LPM_Item",
        "LS_TenantItem", "LS_OccItem", "LS_Item",
        "PS_Customer", "PS_Order",
        "SP_Item", "SpLiveMigTarget", "SpLiveMigTarget2",
        "NullableNum", "RowVersion", "TenantId", "AppliedOn",
        "CustomerId", "IsActive", "Payload", "Version", "Active",
        "Amount", "Secret", "Total", "Email", "Label", "Score",
        "Value", "Token", "Name", "Tag", "Id"
    };

    public static bool IsPostgres(DbConnection connection)
        => string.Equals(connection.GetType().FullName, "Npgsql.NpgsqlConnection", StringComparison.Ordinal);

    public static bool IsMySql(DbConnection connection)
        => string.Equals(connection.GetType().FullName, "MySqlConnector.MySqlConnection", StringComparison.Ordinal);

    public static string Identifier(DbConnection connection, string identifier)
        => IsPostgres(connection) ? Quote(identifier) :
           IsMySql(connection) ? QuoteMySql(identifier) :
           identifier;

    public static string Normalize(DbConnection connection, string sql)
    {
        var isPostgres = IsPostgres(connection);
        var isMySql = IsMySql(connection);
        if (!isPostgres && !isMySql)
            return sql;

        foreach (var identifier in Identifiers.OrderByDescending(static x => x.Length))
        {
            var quote = isPostgres ? Quote(identifier) : QuoteMySql(identifier);
            sql = sql.Replace(Quote(identifier), quote, StringComparison.Ordinal);

            var pattern = $@"(?<![`""\w]){Regex.Escape(identifier)}(?![`""\w])";
            sql = Regex.Replace(sql, pattern, quote, RegexOptions.CultureInvariant);
        }

        return sql;
    }

    public static string Quote(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    public static string QuoteMySql(string identifier)
        => "`" + identifier.Replace("`", "``", StringComparison.Ordinal) + "`";
}
