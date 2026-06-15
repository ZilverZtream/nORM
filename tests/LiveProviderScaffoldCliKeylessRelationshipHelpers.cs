#nullable enable

using System.Data.Common;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static void SetupKeylessPrincipalRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string principalTable,
        string childTable,
        string uniqueName,
        string fkName)
    {
        CleanupKeylessPrincipalRelationship(connection, provider, principalTable, childTable);

        var principal = provider.Escape(principalTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var externalId = provider.Escape("ExternalId");
        var principalExternalId = provider.Escape("PrincipalExternalId");
        var payload = provider.Escape("Payload");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {principal} ({externalId} {text} NOT NULL, {payload} {text} NOT NULL, CONSTRAINT {provider.Escape(uniqueName)} UNIQUE ({externalId}))",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {principalExternalId} {text} NOT NULL, {payload} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({principalExternalId}) REFERENCES {principal} ({externalId}))");
    }

    private static void CleanupKeylessPrincipalRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string principalTable,
        string childTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(principalTable)}");
    }
}
