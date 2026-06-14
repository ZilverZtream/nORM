#nullable enable

using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private const string OutputOptionsTable = "ScaffoldLiveOutputOption";

    private static async Task SetupOutputOptionsTableAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        await TeardownOutputOptionsTableAsync(connection, provider, kind);

        var table = provider.Escape(OutputOptionsTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL, {notes} {TextType(kind, 80)} NULL)");
    }

    private static async Task TeardownOutputOptionsTableAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, OutputOptionsTable, provider.Escape(OutputOptionsTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
