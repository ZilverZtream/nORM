using System.CommandLine;
using System.CommandLine.Parsing;
using nORM.Security;

partial class Program
{
    private static ScaffoldCommandRequest ResolveScaffoldCommandRequest(ParseResult result, ScaffoldCommandBindings bindings)
    {
        ValidateScaffoldUnmatchedTokens(result);
        var input = ResolveScaffoldConnectionProviderInput(result, bindings);
        var projectOption = GetOptionalNonBlankScaffoldOption(result, bindings.ProjectOption, "--project");
        var startupProjectOption = GetOptionalNonBlankScaffoldOption(result, bindings.StartupProjectOption, "--startup-project");
        var efToolConfig = LoadEfToolConfig();
        ReadScaffoldCompatibilityOptions(result, bindings, efToolConfig);

        var projectInfo = ResolveScaffoldProject(FirstNonBlank(projectOption, efToolConfig?.Project), inferCurrentDirectory: true);
        var startupProjectInfo = IsNamedConnectionReference(input.ConnectionReference)
            ? ResolveScaffoldProject(FirstNonBlank(startupProjectOption, efToolConfig?.StartupProject))
            : null;
        var scaffoldEnvironment = GetScaffoldPassThroughEnvironment();
        var connectionString = ResolveScaffoldConnectionString(input.ConnectionReference, projectInfo, startupProjectInfo, scaffoldEnvironment);
        var validated = ConnectionStringValidator.Validate(connectionString, input.ProviderName);
        var output = ResolveScaffoldOutputPath(GetRequiredNonBlankScaffoldOption(result, bindings.OutputOption, "--output", efToolConfig?.OutputDir), projectInfo);
        var naming = ResolveScaffoldOutputNaming(result, bindings, efToolConfig, projectInfo, output, validated.ConnectionString, input);
        var filters = ResolveScaffoldFilters(result, bindings, efToolConfig);
        var forceOverwrite = ResolveScaffoldOverwrite(result, bindings, efToolConfig);
        var options = CreateScaffoldOptions(result, bindings, efToolConfig, projectInfo, naming, filters, forceOverwrite);

        return new ScaffoldCommandRequest(input.ProviderName, validated.ConnectionString, output, naming.Namespace, naming.ContextName, options);
    }
}
