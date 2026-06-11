using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;

var root = new RootCommand("Command line tools for the nORM ORM framework");

root.Add(CreateScaffoldCommand());

var dbcontext = new Command("dbcontext", "EF-style DbContext command aliases. Use 'norm dbcontext scaffold ...' to run bounded nORM scaffolding.");
root.Add(dbcontext);

root.Add(CreateDatabaseCommand());

root.Add(CreatePortabilityCommand());
root.Add(CreateMigrationsCommand());

return await root.Parse(NormalizeEfStyleCommandArgs(args)).InvokeAsync(new InvocationConfiguration());

static string[] NormalizeEfStyleCommandArgs(string[] args)
{
    if (args.Length == 0 || !string.Equals(args[0], "dbcontext", StringComparison.OrdinalIgnoreCase))
        return args;

    if (args.Length == 1)
        return new[] { "scaffold", "--help" };

    if (IsHelpToken(args[1]))
        return new[] { "scaffold" }.Concat(args.Skip(1)).ToArray();

    if (string.Equals(args[1], "scaffold", StringComparison.OrdinalIgnoreCase))
    {
        return new[] { "scaffold" }.Concat(args.Skip(2)).ToArray();
    }

    return args;
}
static bool IsHelpToken(string token)
    => token is "--help" or "-h" or "/?";
