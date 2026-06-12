using System;
using nORM.Security;
using Xunit;

namespace nORM.Tests;

[Trait("Category", "Fast")]
public sealed class ConnectionStringRedactorTests
{
    [Theory]
    [InlineData("Password")]
    [InlineData("Pwd")]
    [InlineData("User ID")]
    [InlineData("Uid")]
    [InlineData("Access Token")]
    [InlineData("Account Key")]
    [InlineData("Shared Access Signature")]
    [InlineData("Client Secret")]
    [InlineData("ApiKey")]
    [InlineData("Private Key")]
    public void RedactMessage_redacts_connection_string_secret_keys_used_by_cli_diagnostics(string key)
    {
        var secret = "leak-me-" + key.Replace(" ", "", StringComparison.Ordinal);
        var message = $"Scaffold failed while opening Server=.;Database=App;{key}={secret};Encrypt=True";

        var redacted = ConnectionStringRedactor.RedactMessage(message);

        Assert.DoesNotContain(secret, redacted, StringComparison.Ordinal);
        Assert.Contains($"{key}=[REDACTED]", redacted, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Encrypt=True", redacted, StringComparison.Ordinal);
    }

    [Fact]
    public void RedactMessage_redacts_quoted_secret_values_with_semicolons()
    {
        const string secret = "alpha;beta;gam\"\"ma";
        var message = $"Scaffold JSON failure: Data Source=app.db;Password=\"{secret}\";Mode=ReadWrite";

        var redacted = ConnectionStringRedactor.RedactMessage(message);

        Assert.DoesNotContain("alpha", redacted, StringComparison.Ordinal);
        Assert.DoesNotContain("beta", redacted, StringComparison.Ordinal);
        Assert.DoesNotContain("gam", redacted, StringComparison.Ordinal);
        Assert.Contains("Password=[REDACTED];Mode=ReadWrite", redacted, StringComparison.Ordinal);
    }

    [Fact]
    public void RedactMessage_does_not_redact_substrings_that_are_not_connection_string_keys()
    {
        const string message = "The dbpassword=value token is an ordinary identifier in this diagnostic.";

        var redacted = ConnectionStringRedactor.RedactMessage(message);

        Assert.Equal(message, redacted);
    }

    [Fact]
    public void ConnectionStringValidator_redacts_the_same_sensitive_key_family()
    {
        const string password = "db-secret";
        const string accountKey = "storage-secret";
        const string user = "operator-login";
        var connection = $"Server=.;Database=App;User ID={user};Password={password};Account Key={accountKey};Encrypt=True";

        var redacted = ConnectionStringValidator.Redact(connection);

        Assert.DoesNotContain(password, redacted, StringComparison.Ordinal);
        Assert.DoesNotContain(accountKey, redacted, StringComparison.Ordinal);
        Assert.DoesNotContain(user, redacted, StringComparison.Ordinal);
        Assert.Contains("Encrypt=True", redacted, StringComparison.OrdinalIgnoreCase);
    }
}
