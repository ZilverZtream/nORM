#nullable enable
using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Provides reverse-engineering utilities that can scaffold entity classes and a DbContext
    /// from an existing database schema.
    /// </summary>
    public static partial class DatabaseScaffolder
    {
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        /// <summary>
        /// Generates entity classes and a DbContext based on the current database schema.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="provider">Database provider implementation.</param>
        /// <param name="outputDirectory">Directory to write generated files into.</param>
        /// <param name="namespaceName">Namespace for the generated classes.</param>
        /// <param name="contextName">Name of the generated DbContext.</param>
        public static async Task ScaffoldAsync(DbConnection connection, DatabaseProvider provider, string outputDirectory, string namespaceName, string contextName = "AppDbContext")
            => await ScaffoldAsync(connection, provider, outputDirectory, namespaceName, contextName, options: null).ConfigureAwait(false);

        /// <summary>
        /// Generates entity classes and a DbContext based on the current database schema.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="provider">Database provider implementation.</param>
        /// <param name="outputDirectory">Directory to write generated files into.</param>
        /// <param name="namespaceName">Namespace for the generated classes.</param>
        /// <param name="options">Scaffolding options.</param>
        public static Task ScaffoldAsync(DbConnection connection, DatabaseProvider provider, string outputDirectory, string namespaceName, ScaffoldOptions options)
            => ScaffoldAsync(connection, provider, outputDirectory, namespaceName, "AppDbContext", options);

        /// <summary>
        /// Generates entity classes and a DbContext based on the current database schema.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="provider">Database provider implementation.</param>
        /// <param name="outputDirectory">Directory to write generated files into.</param>
        /// <param name="namespaceName">Namespace for the generated classes.</param>
        /// <param name="contextName">Name of the generated DbContext.</param>
        /// <param name="options">Scaffolding options.</param>
        public static async Task ScaffoldAsync(DbConnection connection, DatabaseProvider provider, string outputDirectory, string namespaceName, string contextName, ScaffoldOptions? options)
        {
            if (connection is null) throw new ArgumentNullException(nameof(connection));
            if (provider is null) throw new ArgumentNullException(nameof(provider));
            if (outputDirectory is null) throw new ArgumentNullException(nameof(outputDirectory));
            if (namespaceName is null) throw new ArgumentNullException(nameof(namespaceName));
            if (string.IsNullOrWhiteSpace(contextName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(contextName));
            if (!IsValidNamespaceName(namespaceName))
                throw new NormConfigurationException(
                    $"Scaffold namespace '{namespaceName}' is not a valid C# namespace. " +
                    "Use a dot-separated namespace such as 'MyApp.Data'.");
            options ??= new ScaffoldOptions();
            var contextNamespace = ScaffoldOutputManager.NormalizeContextNamespace(namespaceName, options.ContextNamespace);
            var contextOutputDirectory = ScaffoldOutputManager.ResolveContextOutputDirectory(outputDirectory, options.ContextDirectory, options.ContextOutputDirectory);
            var safeContextName = EscapeCSharpIdentifier(ToPascalCase(contextName));

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
                safeContextName = MakeUniqueContextName(safeContextName, discovery.EntityByTable.Values);
                var composition = ScaffoldModelCompositionBuilder.Build(discovery);
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
                await EmitScaffoldOutputAsync(
                    outputDirectory,
                    outputPlan.GeneratedFiles,
                    outputPlan.Diagnostics,
                    outputPlan.DiagnosticsJson,
                    options).ConfigureAwait(false);
            }
            finally
            {
                // Only close the connection if we opened it
                if (!connectionWasOpen)
                    await connection.CloseAsync().ConfigureAwait(false);
            }
        }

    }
}
