namespace nORM.Scaffolding
{
    internal static partial class ScaffoldMySqlUnsupportedFeatureDiscovery
    {
        private static readonly string MySqlUnsupportedFeatureSql =
            MySqlColumnFeatureSql + "\nUNION ALL\n" +
            MySqlObjectFeatureSql + "\nUNION ALL\n" +
            MySqlIndexFeatureSql;
    }
}
