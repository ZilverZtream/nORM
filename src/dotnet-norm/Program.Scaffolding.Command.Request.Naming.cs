using System.CommandLine;
using System.CommandLine.Parsing;

partial class Program
{
    private static ScaffoldOutputNaming ResolveScaffoldOutputNaming(
        ParseResult result,
        ScaffoldCommandBindings bindings,
        EfToolConfig? efToolConfig,
        ScaffoldProjectInfo? projectInfo,
        string output,
        string connectionString,
        ScaffoldConnectionProviderInput input)
    {
        var explicitNamespace = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.NamespaceOption, "--namespace"), efToolConfig?.Namespace);
        var ns = ValidateScaffoldNamespaceName(ResolveScaffoldNamespace(explicitNamespace, projectInfo, output), "--namespace");
        var contextDirectory = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextDirectoryOption, "--context-dir"), efToolConfig?.ContextDir);
        var contextOutputDirectory = ResolveScaffoldContextOutputDirectory(contextDirectory, projectInfo);
        var explicitContextName = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextOption, "--context"), efToolConfig?.Context);
        var contextName = explicitContextName
            ?? InferScaffoldContextName(connectionString, input.ConnectionReference, input.ProviderName);
        var explicitContextNamespace = FirstNonBlank(GetOptionalNonBlankScaffoldOption(result, bindings.ContextNamespaceOption, "--context-namespace"), efToolConfig?.ContextNamespace);
        var (ctx, contextNamespace) = ResolveScaffoldContextNameAndNamespace(contextName, explicitContextNamespace, ns, contextDirectory, explicitNamespace, projectInfo);
        if (explicitContextName is not null)
            ctx = ValidateScaffoldContextClassName(ctx);
        if (contextNamespace is not null)
            contextNamespace = ValidateScaffoldNamespaceName(contextNamespace, explicitContextNamespace is null ? "context namespace" : "--context-namespace");

        return new ScaffoldOutputNaming(ns, ctx, contextNamespace, contextOutputDirectory);
    }
}
