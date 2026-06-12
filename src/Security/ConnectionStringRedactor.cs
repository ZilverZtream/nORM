using System;
using System.Data.Common;
using System.Text;

namespace nORM.Security;

internal static class ConnectionStringRedactor
{
    private const string DiagnosticRedactedValue = "[REDACTED]";
    private const string ConnectionStringRedactedValue = "***";

    private static readonly string[] SensitiveKeyPatterns =
    {
        "shared access signature",
        "sharedaccesssignature",
        "shared access key",
        "sharedaccesskey",
        "user password",
        "access token",
        "accesstoken",
        "client secret",
        "clientsecret",
        "application secret",
        "applicationsecret",
        "private key",
        "privatekey",
        "account key",
        "accountkey",
        "sas token",
        "sastoken",
        "api key",
        "apikey",
        "user id",
        "userid",
        "password",
        "secret",
        "token",
        "pwd",
        "uid"
    };

    internal static string RedactMessage(string message)
    {
        if (string.IsNullOrEmpty(message))
            return message;

        StringBuilder? builder = null;
        var lastCopyStart = 0;
        for (var index = 0; index < message.Length; index++)
        {
            if (!TryMatchSensitiveAssignment(message, index, out var valueStart, out var valueEnd))
                continue;

            builder ??= new StringBuilder(message.Length);
            builder.Append(message, lastCopyStart, valueStart - lastCopyStart);
            builder.Append(DiagnosticRedactedValue);
            lastCopyStart = valueEnd;
            index = valueEnd - 1;
        }

        if (builder is null)
            return message;

        builder.Append(message, lastCopyStart, message.Length - lastCopyStart);
        return builder.ToString();
    }

    internal static string RedactBuilder(DbConnectionStringBuilder builder)
    {
        var redacted = new DbConnectionStringBuilder();
        foreach (string key in builder.Keys)
        {
            redacted[key] = IsSensitiveKey(key) ? ConnectionStringRedactedValue : builder[key];
        }

        return redacted.ConnectionString;
    }

    internal static bool IsSensitiveKey(string key)
    {
        var normalized = NormalizeKey(key);
        foreach (var sensitiveKey in SensitiveKeyPatterns)
        {
            if (normalized.Equals(NormalizeKey(sensitiveKey), StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static bool TryMatchSensitiveAssignment(string message, int index, out int valueStart, out int valueEnd)
    {
        valueStart = 0;
        valueEnd = 0;
        if (!HasAssignmentBoundary(message, index))
            return false;

        foreach (var key in SensitiveKeyPatterns)
        {
            if (!StartsWith(message, index, key))
                continue;

            var equalsIndex = index + key.Length;
            while (equalsIndex < message.Length && char.IsWhiteSpace(message[equalsIndex]))
                equalsIndex++;

            if (equalsIndex >= message.Length || message[equalsIndex] != '=')
                continue;

            valueStart = equalsIndex + 1;
            while (valueStart < message.Length && char.IsWhiteSpace(message[valueStart]))
                valueStart++;

            valueEnd = FindValueEnd(message, valueStart);
            return true;
        }

        return false;
    }

    private static bool StartsWith(string value, int index, string candidate)
    {
        if (index + candidate.Length > value.Length)
            return false;

        return string.Compare(value, index, candidate, 0, candidate.Length, StringComparison.OrdinalIgnoreCase) == 0;
    }

    private static bool HasAssignmentBoundary(string message, int index)
    {
        if (index == 0)
            return true;

        var previous = message[index - 1];
        return previous is ';' or ',' or '(' or '[' or '{' or '"' or '\'' || char.IsWhiteSpace(previous);
    }

    private static int FindValueEnd(string message, int valueStart)
    {
        if (valueStart >= message.Length)
            return valueStart;

        return message[valueStart] switch
        {
            '"' => FindDelimitedValueEnd(message, valueStart + 1, '"'),
            '\'' => FindDelimitedValueEnd(message, valueStart + 1, '\''),
            '{' => FindDelimitedValueEnd(message, valueStart + 1, '}'),
            _ => FindUnquotedValueEnd(message, valueStart)
        };
    }

    private static int FindDelimitedValueEnd(string message, int searchStart, char terminator)
    {
        for (var index = searchStart; index < message.Length; index++)
        {
            if (message[index] != terminator)
                continue;

            if (terminator is '"' or '\'' &&
                index + 1 < message.Length &&
                message[index + 1] == terminator)
            {
                index++;
                continue;
            }

            return index + 1;
        }

        return message.Length;
    }

    private static int FindUnquotedValueEnd(string message, int valueStart)
    {
        for (var index = valueStart; index < message.Length; index++)
        {
            if (message[index] is ';' or '\r' or '\n')
                return index;
        }

        return message.Length;
    }

    private static string NormalizeKey(string key)
    {
        var builder = new StringBuilder(key.Length);
        foreach (var ch in key)
        {
            if (!char.IsWhiteSpace(ch) && ch != '_' && ch != '-')
                builder.Append(char.ToLowerInvariant(ch));
        }

        return builder.ToString();
    }
}
