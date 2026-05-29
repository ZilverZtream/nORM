namespace nORM.Sample.Store;

internal static class StoreProviderConnectionStrings
{
    public static string? Get(StoreProvider provider) => provider.Kind switch
    {
        StoreProviderKind.SqlServer => First("NORM_SAMPLE_SQLSERVER", "NORM_TEST_SQLSERVER"),
        StoreProviderKind.Postgres => First("NORM_SAMPLE_POSTGRES", "NORM_TEST_POSTGRES"),
        StoreProviderKind.MySql => First("NORM_SAMPLE_MYSQL", "NORM_TEST_MYSQL"),
        StoreProviderKind.Sqlite => null,
        _ => throw new ArgumentOutOfRangeException(nameof(provider))
    };

    private static string? First(params string[] names)
    {
        foreach (var name in names)
        {
            var value = Environment.GetEnvironmentVariable(name);
            if (!string.IsNullOrWhiteSpace(value))
                return value;

            var alias = Environment.GetEnvironmentVariable(name + "_CS");
            if (!string.IsNullOrWhiteSpace(alias))
                return alias;
        }

        return null;
    }
}
