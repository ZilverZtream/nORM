using System.CommandLine;

partial class Program
{
    private static ScaffoldConnectionCommandSymbols CreateScaffoldConnectionCommandSymbols()
        => new(
            new Argument<string?>("connection") { Arity = ArgumentArity.ZeroOrOne },
            new Argument<string?>("provider") { Arity = ArgumentArity.ZeroOrOne },
            new Option<string?>("--connection") { Description = "Database connection string. e.g. 'Server=.;Database=AppDb;Trusted_Connection=True;'" },
            new Option<string?>("--provider") { Description = "Database provider (sqlserver, sqlite, postgres, mysql) or matching EF Core provider package name." });

    private static void AddScaffoldConnectionCommandSymbols(Command scaffold, ScaffoldCommandSymbols symbols)
    {
        scaffold.Add(symbols.ConnectionArgument);
        scaffold.Add(symbols.ProviderArgument);
        scaffold.Add(symbols.ConnectionOption);
        scaffold.Add(symbols.ProviderOption);
    }
}
