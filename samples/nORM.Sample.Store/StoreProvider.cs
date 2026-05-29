namespace nORM.Sample.Store;

public sealed record StoreProvider(StoreProviderKind Kind, string Name)
{
    public static StoreProvider? Parse(string value) => value.ToLowerInvariant() switch
    {
        "sqlite" => new StoreProvider(StoreProviderKind.Sqlite, "sqlite"),
        "sqlserver" or "mssql" => new StoreProvider(StoreProviderKind.SqlServer, "sqlserver"),
        "postgres" or "postgresql" => new StoreProvider(StoreProviderKind.Postgres, "postgres"),
        "mysql" or "mariadb" => new StoreProvider(StoreProviderKind.MySql, "mysql"),
        _ => null
    };
}

public enum StoreProviderKind
{
    Sqlite,
    SqlServer,
    Postgres,
    MySql
}
