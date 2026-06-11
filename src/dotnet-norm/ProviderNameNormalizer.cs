using System;

namespace nORM.Cli;

internal static class ProviderNameNormalizer
{
    public static string Normalize(string providerName)
        => providerName.Trim().ToLowerInvariant() switch
        {
            "microsoft.entityframeworkcore.sqlserver" or "sqlserver" or "mssql" => "sqlserver",
            "microsoft.entityframeworkcore.sqlite" or "sqlite" => "sqlite",
            "npgsql.entityframeworkcore.postgresql" or "npgsql" or "postgres" or "postgresql" => "postgres",
            "pomelo.entityframeworkcore.mysql" or "mysql.entityframeworkcore" or "mysql" or "mariadb" => "mysql",
            var normalized => normalized
        };
}
