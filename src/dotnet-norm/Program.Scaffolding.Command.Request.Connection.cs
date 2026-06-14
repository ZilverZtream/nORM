using System.CommandLine;
using System.CommandLine.Parsing;
using nORM.Core;

partial class Program
{
    private static ScaffoldConnectionProviderInput ResolveScaffoldConnectionProviderInput(
        ParseResult result,
        ScaffoldCommandBindings bindings)
    {
        var connectionOption = GetOptionalNonBlankScaffoldOption(result, bindings.ConnectionOption, "--connection");
        var providerOption = GetOptionalNonBlankScaffoldOption(result, bindings.ProviderOption, "--provider");
        var connectionPosition = NullIfWhiteSpace(result.GetValue(bindings.ConnectionArgument));
        var providerPosition = NullIfWhiteSpace(result.GetValue(bindings.ProviderArgument));
        if (connectionOption is not null && providerOption is null && providerPosition is null && connectionPosition is not null)
        {
            providerPosition = connectionPosition;
            connectionPosition = null;
        }

        var connectionReference = FirstNonBlank(connectionOption, connectionPosition)
            ?? throw new NormConfigurationException("Scaffold requires a database connection string. Pass --connection <CONNECTION> or the EF-style positional <CONNECTION> argument.");
        var providerName = FirstNonBlank(providerOption, providerPosition)
            ?? throw new NormConfigurationException("Scaffold requires a database provider. Pass --provider <PROVIDER> or the EF-style positional <PROVIDER> argument.");

        return new ScaffoldConnectionProviderInput(connectionReference, NormalizeProviderName(providerName));
    }
}
