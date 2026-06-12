using System;
using System.Collections.Generic;
using System.IO;

namespace nORM.Cli;

public static partial class ProviderMobilitySourceScanner
{
    private static void ScanFile(string root, string file, List<ProviderMobilityFinding> findings)
    {
        if (IsProjectFile(file) && TryScanProjectFile(root, file, findings))
            return;

        var rules = RulesForFile(file);
        var lines = File.ReadAllLines(file);
        var inBlockComment = false;
        for (var i = 0; i < lines.Length; i++)
        {
            var line = StripBlockComments(lines[i], ref inBlockComment);
            var trimmed = line.TrimStart();
            if (trimmed.StartsWith("//", StringComparison.Ordinal) || trimmed.StartsWith("--", StringComparison.Ordinal))
                continue;

            foreach (var rule in rules)
            {
                if (!line.Contains(rule.Pattern, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (rule.Pattern == "ProviderNative" && line.Contains("TemporalStorageMode.ProviderNative", StringComparison.Ordinal))
                    continue;

                AddFinding(root, file, i + 1, rule, findings);
            }
        }
    }

    private static (string Pattern, string Kind, string SuggestedFix)[] RulesForFile(string file)
    {
        if (file.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
            return SqlRules;
        if (IsProjectFile(file))
            return ProjectRules;

        return CSharpRules;
    }

    private static bool IsProjectFile(string file)
        => file.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase) ||
           file.EndsWith(".props", StringComparison.OrdinalIgnoreCase) ||
           file.EndsWith(".targets", StringComparison.OrdinalIgnoreCase);

    private static string StripBlockComments(string line, ref bool inBlockComment)
    {
        var current = line;
        while (true)
        {
            if (inBlockComment)
            {
                var end = current.IndexOf("*/", StringComparison.Ordinal);
                if (end < 0)
                    return string.Empty;

                current = current[(end + 2)..];
                inBlockComment = false;
            }

            var start = current.IndexOf("/*", StringComparison.Ordinal);
            if (start < 0)
                return current;

            var close = current.IndexOf("*/", start + 2, StringComparison.Ordinal);
            if (close < 0)
            {
                inBlockComment = true;
                return current[..start];
            }

            current = current.Remove(start, close - start + 2);
        }
    }
}
