using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public partial class ScaffoldingContractDocTests
{
    [Fact]
    public void Public_api_policy_lists_every_scaffold_options_member()
    {
        var shippedApi = ReadRepoFile("tests", "PublicApi.Shipped.txt");
        var publicApiPolicy = ReadRepoFile("docs", "public-api-policy.md");
        var scaffoldOptionMembers = shippedApi
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Where(static line => line.StartsWith("P:nORM.Scaffolding.ScaffoldOptions.", StringComparison.Ordinal))
            .Select(static line => line[2..].Split(':')[0])
            .OrderBy(static member => member, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(scaffoldOptionMembers);
        foreach (var member in scaffoldOptionMembers)
            Assert.Contains(member, publicApiPolicy, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_and_cli_pin_repeatable_table_filter_for_literal_commas()
    {
        var doc = ReadDoc();
        var cliSource = string.Concat(
            ReadRepoFile("src", "dotnet-norm", "Program.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Bindings.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Execution.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Handler.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Options.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Options.Compatibility.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Options.Connection.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Options.Filters.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Options.Generation.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Options.ProjectNaming.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Symbols.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Request.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Request.Compatibility.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Request.Connection.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Request.Helpers.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Command.Request.Naming.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.EfToolConfig.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.EfToolConfig.JsonDefault.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.EfToolConfig.Model.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.EfToolConfig.Values.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Project.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Names.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.NamespacePaths.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.ContextNamespace.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.ContextNaming.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.ProjectMetadata.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Configuration.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Configuration.Json.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Configuration.Sources.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.Filters.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Scaffolding.FiltersAndResults.cs"),
            ReadRepoFile("src", "dotnet-norm", "ProviderNameNormalizer.cs"),
            ReadRepoFile("src", "dotnet-norm", "Program.Shared.cs"));
        var scaffolderSource = string.Concat(
            ReadDatabaseScaffolderSource(),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldOutputPlanBuilder.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldOutputPlanBuilder.Context.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldOutputPlanBuilder.Diagnostics.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldOutputPlanRequest.cs"));
        var tableFilterSource = string.Concat(
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Collisions.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Requests.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Matching.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Casing.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Identifiers.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.QueryArtifacts.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Validation.cs"),
            ReadRepoFile("src", "nORM", "Scaffolding", "ScaffoldTableFilter.Validation.Selectable.cs"));
        var contextWriterSource = ReadContextWriterSource();
        var cliReadme = ReadRepoFile("src", "dotnet-norm", "README.md");
        var rootReadme = ReadRepoFile("README.md");
        var publicApiPolicy = ReadRepoFile("docs", "public-api-policy.md");

        Assert.Contains("repeatable CLI `--table`", doc, StringComparison.Ordinal);
        Assert.Contains("repeatable CLI `--schema`", doc, StringComparison.Ordinal);
        Assert.Contains("multi-value `--table First Second`", doc, StringComparison.Ordinal);
        Assert.Contains("multi-value `--schema Accounting Sales`", doc, StringComparison.Ordinal);
        Assert.Contains("`schema.table`", doc, StringComparison.Ordinal);
        Assert.Contains("`schema.view`", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL catalog-qualified", doc, StringComparison.Ordinal);
        Assert.Contains("table and query-artifact filters", doc, StringComparison.Ordinal);
        Assert.Contains("current catalog can still be used in table/query-artifact filters", doc, StringComparison.Ordinal);
        Assert.Contains("table filters can also select matching routine or sequence stubs", doc, StringComparison.Ordinal);
        Assert.Contains("more than one selectable table, query artifact, routine, or sequence", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.Schemas", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.UsePluralizer", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.UseDatabaseNames", doc, StringComparison.Ordinal);
        Assert.Contains("nORM.Scaffolding.ScaffoldOptions.UseDatabaseNames", publicApiPolicy, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_preserves_database_names_on_live_provider", publicApiPolicy, StringComparison.Ordinal);
        Assert.Contains("routine result-column names", doc, StringComparison.Ordinal);
        Assert.Contains("Provider-bound routine and sequence stubs follow the same rule", doc, StringComparison.Ordinal);
        Assert.Contains("Synthetic navigation", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style CLI aliases", doc, StringComparison.Ordinal);
        Assert.Contains("`--data-annotations`/`-d`", doc, StringComparison.Ordinal);
        Assert.Contains("`--force`/`-f`", doc, StringComparison.Ordinal);
        Assert.Contains("CLI refuses to overwrite existing generated files", doc, StringComparison.Ordinal);
        Assert.Contains("Passing both `--force` and `--no-overwrite` is rejected", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style positional CLI arguments", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style namespace-qualified context names", doc, StringComparison.Ordinal);
        Assert.Contains("validated as C# namespaces before generation", doc, StringComparison.Ordinal);
        Assert.Contains("class-name segments are validated as C# type", doc, StringComparison.Ordinal);
        Assert.Contains("Blank explicit CLI string values", doc, StringComparison.Ordinal);
        Assert.Contains("When CLI `--context` is omitted", doc, StringComparison.Ordinal);
        Assert.Contains("norm scaffold <connection> <provider>", doc, StringComparison.Ordinal);
        Assert.Contains("norm dbcontext scaffold <connection> <provider>", doc, StringComparison.Ordinal);
        Assert.Contains("advertises `dbcontext` as an EF-style alias group", doc, StringComparison.Ordinal);
        Assert.Contains("positional value after `--connection`", doc, StringComparison.Ordinal);
        Assert.Contains("EF Core provider package names", doc, StringComparison.Ordinal);
        Assert.Contains("positional scaffold provider", doc, StringComparison.Ordinal);
        Assert.Contains("explicit `--provider` option", doc, StringComparison.Ordinal);
        Assert.Contains("Microsoft.EntityFrameworkCore.Sqlite", doc, StringComparison.Ordinal);
        Assert.Contains("Npgsql.EntityFrameworkCore.PostgreSQL", doc, StringComparison.Ordinal);
        Assert.Contains("Name=ConnectionStrings:AppDb", doc, StringComparison.Ordinal);
        Assert.Contains("ConnectionStrings__AppDb", doc, StringComparison.Ordinal);
        Assert.Contains("environment variables first", doc, StringComparison.Ordinal);
        Assert.Contains("UserSecretsId", doc, StringComparison.Ordinal);
        Assert.Contains("user secrets", doc, StringComparison.Ordinal);
        Assert.Contains("does not execute startup code", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style project targeting", doc, StringComparison.Ordinal);
        Assert.Contains("paths are resolved below that project directory", doc, StringComparison.Ordinal);
        Assert.Contains("plus sanitized output directory", doc, StringComparison.Ordinal);
        Assert.Contains("contains exactly one `.csproj`", doc, StringComparison.Ordinal);
        Assert.Contains("project-relative placement", doc, StringComparison.Ordinal);
        Assert.Contains("project's root namespace plus", doc, StringComparison.Ordinal);
        Assert.Contains("nullable-reference defaults", doc, StringComparison.Ordinal);
        Assert.Contains("`Directory.Build.props`", doc, StringComparison.Ordinal);
        Assert.Contains("`--startup-project`/`-s`,", doc, StringComparison.Ordinal);
        Assert.Contains("`--framework`/`--target-framework`", doc, StringComparison.Ordinal);
        Assert.Contains("`--msbuildprojectextensionspath`", doc, StringComparison.Ordinal);
        Assert.Contains("invoke MSBuild", doc, StringComparison.Ordinal);
        Assert.Contains("application arguments after `--`", doc, StringComparison.Ordinal);
        Assert.Contains("`-- --environment Production`", doc, StringComparison.Ordinal);
        Assert.Contains("`appsettings.Production.json`", doc, StringComparison.Ordinal);
        Assert.Contains("`ASPNETCORE_ENVIRONMENT`", doc, StringComparison.Ordinal);
        Assert.Contains("`DOTNET_ENVIRONMENT`", doc, StringComparison.Ordinal);
        Assert.Contains("blank `--environment` value is rejected", doc, StringComparison.Ordinal);
        Assert.Contains("typos are not swallowed", doc, StringComparison.Ordinal);
        Assert.Contains("`.config/dotnet-ef.json` defaults", doc, StringComparison.Ordinal);
        Assert.Contains("`outputDir`/`output`", doc, StringComparison.Ordinal);
        Assert.Contains("`schema`/`schemas`", doc, StringComparison.Ordinal);
        Assert.Contains("`table`/`tables`", doc, StringComparison.Ordinal);
        Assert.Contains("`framework`/`targetFramework`, `configuration`, `runtime`,", doc, StringComparison.Ordinal);
        Assert.Contains("`msbuildProjectExtensionsPath`", doc, StringComparison.Ordinal);
        Assert.Contains("`verbose`, `noColor`", doc, StringComparison.Ordinal);
        Assert.Contains("`prefixOutput`", doc, StringComparison.Ordinal);
        Assert.Contains("`noPluralize`, `useDatabaseNames`", doc, StringComparison.Ordinal);
        Assert.Contains("`force`, `noOverwrite`", doc, StringComparison.Ordinal);
        Assert.Contains("`dryRun`, `failOnWarnings`, `emitRoutineStubs`", doc, StringComparison.Ordinal);
        Assert.Contains("`emitSequenceStubs`", doc, StringComparison.Ordinal);
        Assert.Contains("`emitViewEntities`", doc, StringComparison.Ordinal);
        Assert.Contains("`emitQueryArtifacts`", doc, StringComparison.Ordinal);
        Assert.Contains("array table/schema defaults are accepted", doc, StringComparison.Ordinal);
        Assert.Contains("present string properties must", doc, StringComparison.Ordinal);
        Assert.Contains("be non-blank", doc, StringComparison.Ordinal);
        Assert.Contains("expand the explicit selection", doc, StringComparison.Ordinal);
        Assert.Contains("override the opposite config default", doc, StringComparison.Ordinal);
        Assert.Contains("take precedence over configuration file", doc, StringComparison.Ordinal);
        Assert.Contains("EF common output switches", doc, StringComparison.Ordinal);
        Assert.Contains("`--json` or config `json` emits a machine-readable", doc, StringComparison.Ordinal);
        Assert.Contains("`--no-onconfiguring` is accepted", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.ContextDirectory", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.ContextNamespace", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldOptions.ContextOutputDirectory", doc, StringComparison.Ordinal);
        Assert.Contains("matching supported query", doc, StringComparison.Ordinal);
        Assert.Contains("scaffolded by default as read-only query artifacts", doc, StringComparison.Ordinal);
        Assert.Contains("explicitly selected by table/schema filters", doc, StringComparison.Ordinal);
        Assert.Contains("opt-in routine or", doc, StringComparison.Ordinal);
        Assert.Contains("sequence stubs in the selected schemas", doc, StringComparison.Ordinal);
        Assert.Contains("ShouldEmitQueryArtifactObject", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("IsDefaultQueryArtifactObject", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("IsDefaultMySqlCatalogQualifiedFilter", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("MatchesTableFilter(provider, table, request, filterCatalog)", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("MatchesSkippedObjectFilter(provider, obj, request, filterCatalog)", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("MatchesSelectableSkippedObjectFilter", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("GetSelectableTableFilterMatches", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("DisplaySkippedObjectMatch", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("same-schema object-kind collisions", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("options.EmitRoutineStubs", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("options.EmitSequenceStubs", tableFilterSource, StringComparison.Ordinal);
        Assert.Contains("unioned with explicit table filters", doc, StringComparison.Ordinal);
        Assert.Contains("database object names are singularized for entity classes", doc, StringComparison.Ordinal);
        Assert.Contains("output-relative API placement", doc, StringComparison.Ordinal);
        Assert.Contains("EF-style project-relative placement", doc, StringComparison.Ordinal);
        Assert.Contains("CLI `--context-dir` rejects absolute", doc, StringComparison.Ordinal);
        Assert.Contains("literal table names that contain commas", doc, StringComparison.Ordinal);
        Assert.Contains("blank CLI filters are rejected", doc, StringComparison.Ordinal);
        Assert.Contains("--table \"Keep,Me\"", doc, StringComparison.Ordinal);
        Assert.Contains("Option<string>(\"--output\", \"-o\", \"--output-dir\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--namespace\", \"-n\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--context\", \"-c\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--project\", \"-p\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--startup-project\", \"-s\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--framework\", \"--target-framework\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--msbuildprojectextensionspath\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("MsbuildProjectExtensionsPathOption = symbols.MsbuildProjectExtensionsPathOption", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--no-build\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--json\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--verbose\", \"-v\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--no-color\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--prefix-output\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("TreatUnmatchedTokensAsErrors = false", cliSource, StringComparison.Ordinal);
        Assert.Contains("ValidateScaffoldUnmatchedTokens", cliSource, StringComparison.Ordinal);
        Assert.Contains("AreEfPassThroughTokens", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetScaffoldPassThroughEnvironment", cliSource, StringComparison.Ordinal);
        Assert.Contains("RequireScaffoldPassThroughEnvironmentValue", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetEfPassThroughTokens", cliSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeEfStyleCommandArgs", cliSource, StringComparison.Ordinal);
        Assert.Contains("new Command(\"dbcontext\"", cliSource, StringComparison.Ordinal);
        Assert.Contains("EF-style DbContext command aliases", cliSource, StringComparison.Ordinal);
        Assert.Contains("IsHelpToken", cliSource, StringComparison.Ordinal);
        Assert.Contains("WriteScaffoldResultJson", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadScaffoldWarningSummary", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldConnectionString", cliSource, StringComparison.Ordinal);
        Assert.Contains("TryResolveScaffoldNamedConnection", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadProjectUserSecretsId", cliSource, StringComparison.Ordinal);
        Assert.Contains("BuildScaffoldConfigurationSources", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetUserSecretsFilePath", cliSource, StringComparison.Ordinal);
        Assert.Contains("appsettings.{environment}.json", cliSource, StringComparison.Ordinal);
        Assert.Contains("ASPNETCORE_ENVIRONMENT", cliSource, StringComparison.Ordinal);
        Assert.Contains("DOTNET_ENVIRONMENT", cliSource, StringComparison.Ordinal);
        Assert.Contains("LoadEfToolConfig", cliSource, StringComparison.Ordinal);
        Assert.Contains("FindEfToolConfigPath", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadFirstEfToolConfigString", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadEfToolConfigStringList", cliSource, StringComparison.Ordinal);
        Assert.Contains("AddEfToolConfigListValue", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadEfToolConfigBool", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadFirstEfToolConfigString(document.RootElement, \"framework\", \"targetFramework\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadEfToolConfigString(document.RootElement, \"msbuildProjectExtensionsPath\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("must be a string or string array", cliSource, StringComparison.Ordinal);
        Assert.Contains("must be a boolean", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveEfToolConfigPath", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldProject", cliSource, StringComparison.Ordinal);
        Assert.Contains("TryResolveCurrentDirectoryScaffoldProject", cliSource, StringComparison.Ordinal);
        Assert.Contains("CreateScaffoldProjectInfo", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldOutputPath", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("ValidateScaffoldNamespaceName", cliSource, StringComparison.Ordinal);
        Assert.Contains("ValidateScaffoldContextClassName", cliSource, StringComparison.Ordinal);
        Assert.Contains("IsValidScaffoldIdentifier", cliSource, StringComparison.Ordinal);
        Assert.Contains("IsValidScaffoldNamespaceName", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldContextNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldContextOutputDirectory", cliSource, StringComparison.Ordinal);
        Assert.Contains("Scaffold --context-dir must be a relative path", cliSource, StringComparison.Ordinal);
        Assert.Contains("ResolveScaffoldContextNameAndNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("SplitScaffoldContextName", cliSource, StringComparison.Ordinal);
        Assert.Contains("InferScaffoldContextName", cliSource, StringComparison.Ordinal);
        Assert.Contains("TryGetScaffoldDatabaseName", cliSource, StringComparison.Ordinal);
        Assert.Contains("AppendNamespacePath", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadProjectDefaultNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadProjectUseNullableReferenceTypes", cliSource, StringComparison.Ordinal);
        Assert.Contains("ReadNearestDirectoryBuildPropsProperty", cliSource, StringComparison.Ordinal);
        Assert.Contains("IsNullableReferenceTypesEnabled", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--schemas\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string[]>(\"--schema\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string[]>(\"--table\", \"-t\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("symbols.SchemaOption.AllowMultipleArgumentsPerToken = true", cliSource, StringComparison.Ordinal);
        Assert.Contains("symbols.TableOption.AllowMultipleArgumentsPerToken = true", cliSource, StringComparison.Ordinal);
        Assert.Contains("Argument<string?>(\"connection\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Argument<string?>(\"provider\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetOptionalNonBlankScaffoldOption", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetRequiredNonBlankScaffoldOption", cliSource, StringComparison.Ordinal);
        Assert.Contains("Scaffold {optionName} must not be blank", cliSource, StringComparison.Ordinal);
        Assert.Contains("providerPosition = connectionPosition;", cliSource, StringComparison.Ordinal);
        Assert.Contains("FirstNonBlank(connectionOption, connectionPosition)", cliSource, StringComparison.Ordinal);
        Assert.Contains("FirstNonBlank(providerOption, providerPosition)", cliSource, StringComparison.Ordinal);
        Assert.Contains("NormalizeProviderName(providerName)", cliSource, StringComparison.Ordinal);
        Assert.Contains("\"microsoft.entityframeworkcore.sqlite\"", cliSource, StringComparison.Ordinal);
        Assert.Contains("\"npgsql.entityframeworkcore.postgresql\"", cliSource, StringComparison.Ordinal);
        Assert.Contains("\"pomelo.entityframeworkcore.mysql\"", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--no-pluralize\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--use-database-names\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("sequence, routine, column, and routine result-column names as generated CLR names", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--no-onconfiguring\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--data-annotations\", \"-d\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<bool>(\"--force\", \"-f\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("By default, scaffold output conflicts are refused", cliSource, StringComparison.Ordinal);
        Assert.Contains("OverwriteFiles = forceOverwrite", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--context-dir\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("Option<string?>(\"--context-namespace\")", cliSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldCommandBindings", cliSource, StringComparison.Ordinal);
        Assert.Contains("RunScaffoldCommandAsync", cliSource, StringComparison.Ordinal);
        Assert.Contains("GetScaffoldBoolOptionOrConfig", cliSource, StringComparison.Ordinal);
        Assert.Contains("IsScaffoldOptionExplicit", cliSource, StringComparison.Ordinal);
        Assert.Contains("hasExplicitCliFilters", cliSource, StringComparison.Ordinal);
        Assert.Contains("forceExplicit", cliSource, StringComparison.Ordinal);
        Assert.Contains("noOverwriteExplicit", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.OutputDir", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.NoPluralize", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.UseDatabaseNames", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.Force", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.NoOverwrite", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.DryRun", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.FailOnWarnings", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.EmitRoutineStubs", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.EmitSequenceStubs", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.EmitViewEntities", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.EmitQueryArtifacts", cliSource, StringComparison.Ordinal);
        Assert.Contains("Compatibility alias for --emit-query-artifacts", cliSource, StringComparison.Ordinal);
        Assert.Contains("Generate bounded read-oriented query artifacts", cliSource, StringComparison.Ordinal);
        Assert.Contains("efToolConfig?.MsbuildProjectExtensionsPath", cliSource, StringComparison.Ordinal);
        Assert.Contains("UsePluralizer = !GetScaffoldBoolOptionOrConfig(result, bindings.NoPluralizeOption, efToolConfig?.NoPluralize)", cliSource, StringComparison.Ordinal);
        Assert.Contains("UseDatabaseNames = GetScaffoldBoolOptionOrConfig(result, bindings.UseDatabaseNamesOption, efToolConfig?.UseDatabaseNames)", cliSource, StringComparison.Ordinal);
        Assert.Contains("DryRun = GetScaffoldBoolOptionOrConfig(result, bindings.DryRunOption, efToolConfig?.DryRun)", cliSource, StringComparison.Ordinal);
        Assert.Contains("UseNullableReferenceTypes = projectInfo?.UseNullableReferenceTypes ?? true", cliSource, StringComparison.Ordinal);
        Assert.Contains("options.UseDatabaseNames", scaffolderSource, StringComparison.Ordinal);
        Assert.Contains("ScaffoldRoutineStubWriter.AppendRoutineStubs(sb, context.RoutineStubs, queryPropertyNames, context.UseNullableReferenceTypes, context.UseDatabaseNames)", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("AppendSequenceStubs(sb, context.SequenceStubs, queryPropertyNames, context.UseDatabaseNames)", contextWriterSource, StringComparison.Ordinal);
        Assert.Contains("ContextOutputDirectory = naming.ContextOutputDirectory", cliSource, StringComparison.Ordinal);
        Assert.Contains("ContextNamespace = naming.ContextNamespace", cliSource, StringComparison.Ordinal);
        Assert.Contains("ParseSchemaFilters", cliSource, StringComparison.Ordinal);
        Assert.Contains("ParseTableFilters", cliSource, StringComparison.Ordinal);
        Assert.Contains("ParseCliCsvList", cliSource, StringComparison.Ordinal);
        Assert.Contains("Use repeatable `--table`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("routine result-column names", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Blank CLI table/schema filters are rejected", cliReadme, StringComparison.Ordinal);
        Assert.Contains("MySQL catalog-qualified", cliReadme, StringComparison.Ordinal);
        Assert.Contains("table and query-artifact filters", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`schema.view` filters", cliReadme, StringComparison.Ordinal);
        Assert.Contains("select matching provider routines or sequences", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Ambiguous explicit filters", cliReadme, StringComparison.Ordinal);
        Assert.Contains("schema-prefixed generated", cliReadme, StringComparison.Ordinal);
        Assert.Contains("repeatable `--schema`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("multi-value `--table First Second`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("multi-value", rootReadme, StringComparison.Ordinal);
        Assert.Contains("routine result-column names", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Blank CLI table/schema filters are rejected", rootReadme, StringComparison.Ordinal);
        Assert.Contains("catalog-qualified table and query-artifact filters", rootReadme, StringComparison.Ordinal);
        Assert.Contains("table and query-artifact filters", rootReadme, StringComparison.Ordinal);
        Assert.Contains("select matching provider routines or sequences", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Ambiguous explicit filters", rootReadme, StringComparison.Ordinal);
        Assert.Contains("schema-prefixed generated", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--project`/`-p`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("plus sanitized output directory", cliReadme, StringComparison.Ordinal);
        Assert.Contains("nullable-reference output", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`#nullable disable`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("contains exactly one `.csproj`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("EF-style placement", cliReadme, StringComparison.Ordinal);
        Assert.Contains("namespace-qualified names such as", cliReadme, StringComparison.Ordinal);
        Assert.Contains("When `--context` is omitted", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--startup-project`/`-s`, `--framework`/`--target-framework`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--msbuildprojectextensionspath`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("application arguments after `--`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`-- --environment Production`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("blank `--environment` value is rejected", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`.config/dotnet-ef.json` defaults", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`outputDir`/`output`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`schema`/`schemas`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`table`/`tables`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`msbuildProjectExtensionsPath`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`prefixOutput`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`noPluralize`, `useDatabaseNames`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`force`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`noOverwrite`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`dryRun`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`failOnWarnings`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`emitRoutineStubs`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`emitSequenceStubs`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`emitViewEntities`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`emitQueryArtifacts`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("present string properties must be non-blank", cliReadme, StringComparison.Ordinal);
        Assert.Contains("cannot expand the explicit", cliReadme, StringComparison.Ordinal);
        Assert.Contains("opposite config default", cliReadme, StringComparison.Ordinal);
        Assert.Contains("matching supported query artifacts", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--json` or config `json` emits a machine-readable", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--verbose`/`-v`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--no-pluralize`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--use-database-names`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--no-onconfiguring`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--output-dir`/`-o`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--data-annotations`/`-d`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Unfiltered ordinary views", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--force`/`-f`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("refuses output conflicts by default", cliReadme, StringComparison.Ordinal);
        Assert.Contains("norm scaffold <connection> <provider>", cliReadme, StringComparison.Ordinal);
        Assert.Contains("norm dbcontext scaffold <connection> <provider>", cliReadme, StringComparison.Ordinal);
        Assert.Contains("advertises `dbcontext` as an EF-style alias group", cliReadme, StringComparison.Ordinal);
        Assert.Contains("positional value after `--connection`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Microsoft.EntityFrameworkCore.Sqlite", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Npgsql.EntityFrameworkCore.PostgreSQL", cliReadme, StringComparison.Ordinal);
        Assert.Contains("Name=ConnectionStrings:AppDb", cliReadme, StringComparison.Ordinal);
        Assert.Contains("UserSecretsId", cliReadme, StringComparison.Ordinal);
        Assert.Contains("startup-project and target-project user secrets", cliReadme, StringComparison.Ordinal);
        Assert.Contains("startup-project", cliReadme, StringComparison.Ordinal);
        Assert.Contains("environment files searched before target-project environment files", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--context-dir`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("`--context-namespace`", cliReadme, StringComparison.Ordinal);
        Assert.Contains("norm scaffold <connection> <provider>", rootReadme, StringComparison.Ordinal);
        Assert.Contains("norm dbcontext scaffold <connection> <provider>", rootReadme, StringComparison.Ordinal);
        Assert.Contains("advertises `dbcontext` as an EF-style alias group", rootReadme, StringComparison.Ordinal);
        Assert.Contains("refuses existing output conflicts by default", rootReadme, StringComparison.Ordinal);
        Assert.Contains("positional value after `--connection`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Microsoft.EntityFrameworkCore.Sqlite", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Npgsql.EntityFrameworkCore.PostgreSQL", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Name=ConnectionStrings:AppDb", rootReadme, StringComparison.Ordinal);
        Assert.Contains("UserSecretsId", rootReadme, StringComparison.Ordinal);
        Assert.Contains("startup-project and target-project user secrets", rootReadme, StringComparison.Ordinal);
        Assert.Contains("startup-project", rootReadme, StringComparison.Ordinal);
        Assert.Contains("environment files searched before target-project environment files", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--project`/`-p` targets", rootReadme, StringComparison.Ordinal);
        Assert.Contains("plus sanitized", rootReadme, StringComparison.Ordinal);
        Assert.Contains("project-aware nullable output", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`Directory.Build.props`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("contains exactly one `.csproj`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("project-relative paths", rootReadme, StringComparison.Ordinal);
        Assert.Contains("MyApp.Data.AppDbContext", rootReadme, StringComparison.Ordinal);
        Assert.Contains("When `--context` is omitted", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--startup-project`/`-s`, `--framework`/`--target-framework`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--msbuildprojectextensionspath`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("application arguments after `--`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`-- --environment Production`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("blank `--environment` value is rejected", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`.config/dotnet-ef.json` defaults", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`outputDir`/`output`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`schema`/`schemas`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`table`/`tables`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`msbuildProjectExtensionsPath`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`prefixOutput`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`noPluralize`, `useDatabaseNames`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`force`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`noOverwrite`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`dryRun`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`failOnWarnings`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`emitRoutineStubs`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`emitSequenceStubs`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`emitViewEntities`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`emitQueryArtifacts`", rootReadme, StringComparison.Ordinal);
        Assert.Contains("present string properties must be non-blank", rootReadme, StringComparison.Ordinal);
        Assert.Contains("cannot expand the explicit", rootReadme, StringComparison.Ordinal);
        Assert.Contains("opposite config default", rootReadme, StringComparison.Ordinal);
        Assert.Contains("matching supported query artifacts", rootReadme, StringComparison.Ordinal);
        Assert.Contains("Unfiltered ordinary views", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--json` emits a machine-readable", rootReadme, StringComparison.Ordinal);
        Assert.Contains("`--verbose`/`-v`", rootReadme, StringComparison.Ordinal);
    }

}
