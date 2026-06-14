using System;
using System.CommandLine;

partial class Program
{
    static Command CreateScaffoldCommand()
    {
        var scaffold = CreateScaffoldRootCommand();
        var symbols = CreateScaffoldCommandSymbols();
        AddScaffoldCommandSymbols(scaffold, symbols);
        var bindings = CreateScaffoldCommandBindings(symbols);
        scaffold.SetAction((result, cancellationToken) => RunScaffoldCommandAsync(result, bindings, cancellationToken));
        return scaffold;
    }

    private static Command CreateScaffoldRootCommand()
    {
        var scaffold = new Command("scaffold", "Scaffold a bounded v1 nORM model from base tables. Provider-owned schema features are written to nORM.ScaffoldWarnings.md/json for review.\nExamples:\n  norm scaffold --connection \"Server=.;Database=AppDb;Trusted_Connection=True;\" --provider sqlserver --output Models\n  norm scaffold \"Data Source=app.db\" sqlite --output-dir Models");
        scaffold.TreatUnmatchedTokensAsErrors = false;
        return scaffold;
    }
}
