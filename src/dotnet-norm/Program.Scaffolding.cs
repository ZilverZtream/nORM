using System;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Collections.Generic;
using System.Linq;
using nORM.Core;

partial class Program
{
    static string? GetOptionalNonBlankScaffoldOption(ParseResult result, Option<string?> option, string optionName)
    {
        var value = result.GetValue(option);
        if (result.GetResult(option) is not null && string.IsNullOrWhiteSpace(value))
            throw new NormConfigurationException($"Scaffold {optionName} must not be blank.");

        return NullIfWhiteSpace(value);
    }

    static string GetRequiredNonBlankScaffoldOption(ParseResult result, Option<string> option, string optionName)
    {
        var value = result.GetValue(option);
        if (string.IsNullOrWhiteSpace(value))
            throw new NormConfigurationException($"Scaffold {optionName} must not be blank.");

        return value;
    }

    static void ValidateScaffoldUnmatchedTokens(ParseResult result)
    {
        if (result.UnmatchedTokens.Count == 0)
            return;

        var commandLineArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
        if (AreEfPassThroughTokens(result.UnmatchedTokens, commandLineArgs))
            return;

        throw new NormConfigurationException(
            "Unrecognized scaffold argument(s): " + string.Join(" ", result.UnmatchedTokens) +
            ". EF-style application arguments are accepted only after '--'; '--environment' is used for named-connection appsettings lookup and other application arguments are ignored because nORM scaffold does not execute startup code.");
    }

    static bool AreEfPassThroughTokens(IReadOnlyList<string> unmatchedTokens, IReadOnlyList<string> commandLineArgs)
    {
        var passThroughTokens = GetEfPassThroughTokens(commandLineArgs);
        return passThroughTokens.Count > 0 &&
            unmatchedTokens.SequenceEqual(passThroughTokens, StringComparer.Ordinal);
    }

    static string? GetScaffoldPassThroughEnvironment()
    {
        var passThroughTokens = GetEfPassThroughTokens(Environment.GetCommandLineArgs().Skip(1).ToArray());
        for (var i = 0; i < passThroughTokens.Count; i++)
        {
            var token = passThroughTokens[i];
            if (string.Equals(token, "--environment", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= passThroughTokens.Count || passThroughTokens[i + 1].StartsWith("--", StringComparison.Ordinal))
                    throw new NormConfigurationException("EF-style application argument '--environment' requires a value.");

                return NullIfWhiteSpace(passThroughTokens[i + 1]);
            }

            const string environmentPrefix = "--environment=";
            if (token.StartsWith(environmentPrefix, StringComparison.OrdinalIgnoreCase))
                return NullIfWhiteSpace(token[environmentPrefix.Length..]);
        }

        return null;
    }

    static List<string> GetEfPassThroughTokens(IReadOnlyList<string> commandLineArgs)
    {
        var separatorIndex = -1;
        for (var i = 0; i < commandLineArgs.Count; i++)
        {
            if (commandLineArgs[i] == "--")
            {
                separatorIndex = i;
                break;
            }
        }

        return separatorIndex < 0
            ? new List<string>()
            : commandLineArgs.Skip(separatorIndex + 1).ToList();
    }
}
