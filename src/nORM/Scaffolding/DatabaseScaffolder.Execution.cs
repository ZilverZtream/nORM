#nullable enable
using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static async Task ScaffoldCoreAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string outputDirectory,
            string namespaceName,
            string contextName,
            ScaffoldOptions? options)
        {
            if (connection is null) throw new ArgumentNullException(nameof(connection));
            if (provider is null) throw new ArgumentNullException(nameof(provider));
            if (outputDirectory is null) throw new ArgumentNullException(nameof(outputDirectory));
            if (namespaceName is null) throw new ArgumentNullException(nameof(namespaceName));
            if (string.IsNullOrWhiteSpace(contextName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(contextName));
            if (!ScaffoldNameHelper.IsValidNamespaceName(namespaceName))
                throw new NormConfigurationException(
                    $"Scaffold namespace '{namespaceName}' is not a valid C# namespace. " +
                    "Use a dot-separated namespace such as 'MyApp.Data'.");

            options ??= new ScaffoldOptions();
            var contextNamespace = ScaffoldOutputManager.NormalizeContextNamespace(namespaceName, options.ContextNamespace);
            var contextOutputDirectory = ScaffoldOutputManager.ResolveContextOutputDirectory(outputDirectory, options.ContextDirectory, options.ContextOutputDirectory);
            var safeContextName = ScaffoldNameHelper.EscapeCSharpIdentifier(ScaffoldNameHelper.ToPascalCase(contextName));

            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                await connection.OpenAsync().ConfigureAwait(false);

            try
            {
                if (!options.DryRun)
                {
                    Directory.CreateDirectory(outputDirectory);
                    Directory.CreateDirectory(contextOutputDirectory);
                }

                var discovery = await ScaffoldModelDiscovery.BuildAsync(connection, provider, options).ConfigureAwait(false);
                safeContextName = ScaffoldNameHelper.MakeUniqueContextName(safeContextName, discovery.EntityByTable.Values);
                var composition = ScaffoldModelCompositionBuilder.Build(discovery, options.NoRelationships);
                var outputPlan = await ScaffoldOutputPlanBuilder.BuildAsync(new ScaffoldOutputPlanRequest(
                    connection,
                    provider,
                    outputDirectory,
                    contextOutputDirectory,
                    namespaceName,
                    contextNamespace,
                    safeContextName,
                    discovery,
                    composition,
                    options,
                    _stringBuilderPool)).ConfigureAwait(false);
                await ScaffoldOutputManager.EmitAsync(
                    outputDirectory,
                    outputPlan.GeneratedFiles,
                    outputPlan.Diagnostics,
                    outputPlan.DiagnosticsJson,
                    options).ConfigureAwait(false);
            }
            finally
            {
                if (!connectionWasOpen)
                    await connection.CloseAsync().ConfigureAwait(false);
            }
        }
    }
}
