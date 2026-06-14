#nullable enable
using System;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal static class ScaffoldReferentialAction
    {
        private const string NoAction = "NO ACTION";

        public static string Normalize(string? action)
            => string.IsNullOrWhiteSpace(action)
                ? NoAction
                : action.Replace('_', ' ').Trim().ToUpperInvariant();

        public static bool IsScaffoldable(string? action)
            => TryParse(action, out _);

        public static bool IsDefault(string? action)
            => string.Equals(Normalize(action), NoAction, StringComparison.OrdinalIgnoreCase);

        public static bool TryParse(string? action, out ReferentialAction referentialAction)
        {
            switch (Normalize(action))
            {
                case "CASCADE":
                    referentialAction = ReferentialAction.Cascade;
                    return true;
                case "SET NULL":
                    referentialAction = ReferentialAction.SetNull;
                    return true;
                case "RESTRICT":
                    referentialAction = ReferentialAction.Restrict;
                    return true;
                case "SET DEFAULT":
                    referentialAction = ReferentialAction.SetDefault;
                    return true;
                case NoAction:
                    referentialAction = ReferentialAction.NoAction;
                    return true;
                default:
                    referentialAction = ReferentialAction.NoAction;
                    return false;
            }
        }

        public static string FormatModelBuilderLiteral(string? action)
            => Normalize(action) switch
            {
                "CASCADE" => "ReferentialAction.Cascade",
                "SET NULL" => "ReferentialAction.SetNull",
                "RESTRICT" => "ReferentialAction.Restrict",
                "SET DEFAULT" => "ReferentialAction.SetDefault",
                _ => "ReferentialAction.NoAction"
            };
    }
}
