#nullable enable
using System;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldProviderKind
    {
        public static bool IsSqlite(DatabaseProvider provider)
            => provider is SqliteProvider;

        public static bool IsSqlServer(DatabaseProvider provider)
            => provider is SqlServerProvider
               || ProviderNameContains(provider, "SqlServer");

        public static bool IsPostgres(DatabaseProvider provider)
            => provider is PostgresProvider
               || ProviderNameContains(provider, "Postgres");

        public static bool IsMySql(DatabaseProvider provider)
            => provider is MySqlProvider
               || ProviderNameContains(provider, "MySql");

        private static bool ProviderNameContains(DatabaseProvider provider, string value)
            => provider.GetType().Name.Contains(value, StringComparison.OrdinalIgnoreCase);
    }
}
