using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Scaffolding;

partial class Program
{
    private static async Task<int> ExecuteScaffoldCommandAsync(
        ScaffoldCommandRequest request,
        bool jsonOutput,
        CancellationToken cancellationToken)
    {
        using var connection = CreateConnection(request.ProviderName, request.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        var provider = CreateProvider(request.ProviderName);

        string? dryRunTempOutput = null;
        var scaffoldOutput = request.Output;
        var scaffoldOptions = request.Options;
        if (request.Options.DryRun)
        {
            dryRunTempOutput = Path.Combine(Path.GetTempPath(), "norm_scaffold_dryrun_" + Guid.NewGuid().ToString("N"));
            scaffoldOutput = dryRunTempOutput;
            scaffoldOptions = CreateDryRunScaffoldOptions(request.Options, dryRunTempOutput);
        }

        try
        {
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(connection, provider, scaffoldOutput, request.Namespace, request.ContextName, scaffoldOptions);
            }
            catch (NormConfigurationException ex) when (IsScaffoldWarningsFailure(ex, scaffoldOutput))
            {
                if (jsonOutput)
                    WriteScaffoldResultJson("failed", request.Output, request.Options.DryRun, reportsWritten: !request.Options.DryRun, ReadScaffoldWarningSummary(scaffoldOutput), ex);
                else
                    PrintScaffoldWarningSummary(scaffoldOutput);
                return jsonOutput ? 1 : Fail(ex);
            }

            var warningSummary = ReadScaffoldWarningSummary(scaffoldOutput);
            if (jsonOutput)
            {
                WriteScaffoldResultJson("succeeded", request.Output, request.Options.DryRun, reportsWritten: !request.Options.DryRun && warningSummary.TotalWarnings > 0, warningSummary);
            }
            else if (request.Options.DryRun)
            {
                if (warningSummary.TotalWarnings > 0 || warningSummary.Error is not null)
                    PrintScaffoldWarningSummary(scaffoldOutput);
                Console.WriteLine($"Scaffolding dry run completed. No files were written to {request.Output}.");
            }
            else
            {
                Console.WriteLine($"Scaffolding completed. Files written to {request.Output}.");
            }

            if (!jsonOutput && !request.Options.DryRun && warningSummary.TotalWarnings > 0)
            {
                Console.WriteLine("Scaffolding warnings were written to nORM.ScaffoldWarnings.md and nORM.ScaffoldWarnings.json.");
                PrintScaffoldWarningSummary(request.Output);
            }
        }
        finally
        {
            if (dryRunTempOutput is not null)
                TryDeleteDirectory(dryRunTempOutput);
        }

        return 0;
    }

    private static ScaffoldOptions CreateDryRunScaffoldOptions(ScaffoldOptions options, string dryRunTempOutput)
        => new()
        {
            Schemas = options.Schemas,
            Tables = options.Tables,
            PluralizeQueryProperties = options.PluralizeQueryProperties,
            UseDatabaseNames = options.UseDatabaseNames,
            UseNullableReferenceTypes = options.UseNullableReferenceTypes,
            ContextDirectory = options.ContextDirectory,
            ContextOutputDirectory = options.ContextOutputDirectory is null
                ? null
                : Path.Combine(dryRunTempOutput, "__context"),
            ContextNamespace = options.ContextNamespace,
            OverwriteFiles = true,
            DryRun = false,
            FailOnWarnings = options.FailOnWarnings,
            EmitRoutineStubs = options.EmitRoutineStubs,
            EmitSequenceStubs = options.EmitSequenceStubs,
            EmitViewEntities = options.EmitViewEntities,
            EmitQueryArtifacts = options.EmitQueryArtifacts
        };
}
