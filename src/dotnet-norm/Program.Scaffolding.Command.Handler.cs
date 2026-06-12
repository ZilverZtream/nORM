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

        try
        {
            var request = ResolveScaffoldCommandRequest(result, bindings);
            outputForJson = request.Output;
            return await ExecuteScaffoldCommandAsync(request, jsonOutput, cancellationToken);
        }
        catch (Exception ex)
        {
            if (jsonOutput)
            {
                WriteScaffoldResultJson("failed", outputForJson, dryRunForJson, reportsWritten: false, ScaffoldWarningSummary.Empty, ex);
                return 1;
            }

            return Fail(ex);
        }
    }
}
