using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Threading;
using System.Threading.Tasks;

partial class Program
{
    private static async Task<int> RunScaffoldCommandAsync(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        CancellationToken cancellationToken)
    {
        var jsonOutput = result.GetValue(bindings.JsonOption);
        var outputForJson = result.GetValue(bindings.OutputOption) ?? ".";
        var dryRunForJson = result.GetValue(bindings.DryRunOption);
        var requestResolved = false;

        try
        {
            if (!IsScaffoldOptionExplicit(result, bindings.JsonOption))
                jsonOutput = ReadEfToolConfigJsonDefault() ?? false;

            var request = ResolveScaffoldCommandRequest(result, bindings);
            outputForJson = request.Output;
            dryRunForJson = request.Options.DryRun;
            requestResolved = true;
            return await ExecuteScaffoldCommandAsync(request, jsonOutput, cancellationToken);
        }
        catch (Exception ex)
        {
            if (jsonOutput)
            {
                if (!requestResolved)
                    (outputForJson, dryRunForJson) = ResolveScaffoldJsonFailureContext(result, bindings, outputForJson, dryRunForJson);

                WriteScaffoldResultJson("failed", outputForJson, dryRunForJson, reportsWritten: false, ScaffoldWarningSummary.Empty, ex);
                return 1;
            }

            return Fail(ex);
        }
    }

    private static (string Output, bool DryRun) ResolveScaffoldJsonFailureContext(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        string outputFallback,
        bool dryRunFallback)
    {
        try
        {
            var efToolConfig = LoadEfToolConfig();
            var dryRun = IsScaffoldOptionExplicit(result, bindings.DryRunOption)
                ? result.GetValue(bindings.DryRunOption)
                : efToolConfig?.DryRun ?? dryRunFallback;
            var output = IsScaffoldOptionExplicit(result, bindings.OutputOption) || efToolConfig?.OutputDir is null
                ? result.GetValue(bindings.OutputOption) ?? outputFallback
                : efToolConfig.OutputDir;
            var projectInfo = ResolveScaffoldProject(
                FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ProjectOption, "--project"), efToolConfig?.Project),
                inferCurrentDirectory: true);
            return (ResolveScaffoldOutputPath(output, projectInfo), dryRun);
        }
        catch (Exception)
        {
            return (outputFallback, dryRunFallback);
        }
    }
}
