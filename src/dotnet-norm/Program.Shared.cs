using System;
using System.Linq;
using System.Text;
using nORM.Cli;
using nORM.Security;

partial class Program
{
    static string? NullIfWhiteSpace(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    static string? FirstNonBlank(params string?[] values)
        => values.FirstOrDefault(value => !string.IsNullOrWhiteSpace(value))?.Trim();

    static string NormalizeProviderName(string providerName)
        => ProviderNameNormalizer.Normalize(providerName);

    /// <summary>
    /// Replaces any recognizable connection string segments containing sensitive key=value pairs
    /// with [REDACTED] so that passwords and secrets are not printed to stderr on error.
    /// </summary>
    static string RedactConnectionStrings(string message)
        => ConnectionStringRedactor.RedactMessage(message);

    static string ToCSharpIdentifier(string value)
    {
        var builder = new StringBuilder(value.Length);
        for (var i = 0; i < value.Length; i++)
        {
            var ch = value[i];
            builder.Append(i == 0
                ? (char.IsLetter(ch) || ch == '_' ? ch : '_')
                : (char.IsLetterOrDigit(ch) || ch == '_' ? ch : '_'));
        }

        return builder.Length == 0 ? "_" : builder.ToString();
    }
}
