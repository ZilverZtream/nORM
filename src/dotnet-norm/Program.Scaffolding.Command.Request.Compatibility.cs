using System.CommandLine;
using System.CommandLine.Parsing;

partial class Program
{
    private static void ReadScaffoldCompatibilityOptions(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        EfToolConfig? efToolConfig)
    {
        _ = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.FrameworkOption, "--framework"), efToolConfig?.Framework);
        _ = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ConfigurationOption, "--configuration"), efToolConfig?.Configuration);
        _ = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.RuntimeOption, "--runtime"), efToolConfig?.Runtime);
        _ = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.MsbuildProjectExtensionsPathOption, "--msbuildprojectextensionspath"), efToolConfig?.MsbuildProjectExtensionsPath);
        _ = GetScaffoldBoolOptionOrConfig(result, bindings.NoBuildOption, efToolConfig?.NoBuild);
        _ = result.GetValue(bindings.VerboseOption) || efToolConfig?.Verbose == true;
        _ = result.GetValue(bindings.NoColorOption) || efToolConfig?.NoColor == true;
        _ = result.GetValue(bindings.PrefixOutputOption) || efToolConfig?.PrefixOutput == true;
        _ = GetScaffoldBoolOptionOrConfig(result, bindings.NoOnConfiguringOption, efToolConfig?.NoOnConfiguring);
        _ = GetScaffoldBoolOptionOrConfig(result, bindings.DataAnnotationsOption, efToolConfig?.DataAnnotations);
    }
}
