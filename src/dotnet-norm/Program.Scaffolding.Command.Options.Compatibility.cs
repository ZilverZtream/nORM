using System.CommandLine;

partial class Program
{
    private static ScaffoldCompatibilityCommandSymbols CreateScaffoldCompatibilityCommandSymbols()
        => new(
            new Option<string?>("--framework", "--target-framework") { Description = "Accepted for EF Core scaffold compatibility and nORM design-time CLI consistency; nORM scaffold does not build the target project." },
            new Option<string?>("--configuration") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold does not build the target project." },
            new Option<string?>("--runtime") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold does not restore runtime-specific assets." },
            new Option<string?>("--msbuildprojectextensionspath") { Description = "Accepted for legacy EF-style scaffold compatibility; nORM scaffold does not invoke MSBuild." },
            new Option<bool>("--no-build") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold never builds the target project." },
            new Option<bool>("--verbose", "-v") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold output is already explicit." },
            new Option<bool>("--no-color") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold output is plain text." },
            new Option<bool>("--prefix-output") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffold output is not severity-prefixed." },
            new Option<bool>("--no-onconfiguring") { Description = "Accepted for EF Core scaffold compatibility; nORM generated contexts never emit OnConfiguring." },
            new Option<bool>("--data-annotations", "-d") { Description = "Accepted for EF Core scaffold compatibility; nORM scaffolding already emits data annotations where supported." });

    private static void AddScaffoldCompatibilityCommandSymbols(Command scaffold, ScaffoldCommandSymbols symbols)
    {
        scaffold.Add(symbols.FrameworkOption);
        scaffold.Add(symbols.ConfigurationOption);
        scaffold.Add(symbols.RuntimeOption);
        scaffold.Add(symbols.MsbuildProjectExtensionsPathOption);
        scaffold.Add(symbols.NoBuildOption);
        scaffold.Add(symbols.VerboseOption);
        scaffold.Add(symbols.NoColorOption);
        scaffold.Add(symbols.PrefixOutputOption);
        scaffold.Add(symbols.NoOnConfiguringOption);
        scaffold.Add(symbols.DataAnnotationsOption);
    }
}
