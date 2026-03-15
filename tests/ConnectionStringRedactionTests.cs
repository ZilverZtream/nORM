using System;
using System.Reflection;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that NormalizeConnectionStringForCacheKey strips sensitive
/// keys (Password, Pwd, Access Token, Token, Secret) before producing the
/// cache key so that credentials never appear in cache key material.
/// </summary>
public class ConnectionStringRedactionTests
{
    private static readonly MethodInfo _normalize =
        typeof(NormQueryProvider)
            .GetMethod("NormalizeConnectionStringForCacheKey",
                BindingFlags.NonPublic | BindingFlags.Static)!;

    private static string Normalize(string? cs) =>
        (string)_normalize.Invoke(null, new object?[] { cs })!;

 // ─── Sensitive key values are stripped from normalized result ────────

    [Fact]
    public void Password_NotInNormalizedKey()
    {
        var cs = "Server=myserver;Database=mydb;Password=secret123";
        var result = Normalize(cs);

        Assert.DoesNotContain("secret123", result, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Password", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Pwd_NotInNormalizedKey()
    {
        var cs = "Server=myserver;Database=mydb;Pwd=hunter2";
        var result = Normalize(cs);

        Assert.DoesNotContain("hunter2", result, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Pwd", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AccessToken_NotInNormalizedKey()
    {
        var cs = "Server=myserver;Database=mydb;Access Token=abc123token";
        var result = Normalize(cs);

        Assert.DoesNotContain("abc123token", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Token_NotInNormalizedKey()
    {
        var cs = "Server=myserver;Database=mydb;Token=eyJhbGci";
        var result = Normalize(cs);

        Assert.DoesNotContain("eyJhbGci", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Secret_NotInNormalizedKey()
    {
        var cs = "Server=myserver;Database=mydb;Secret=topsecret";
        var result = Normalize(cs);

        Assert.DoesNotContain("topsecret", result, StringComparison.OrdinalIgnoreCase);
    }

 // ─── Strings differing only by password → same normalized key ───────

    [Fact]
    public void DifferentPasswords_ProduceSameNormalizedKey()
    {
 // Two connection strings differing only in the password must produce
 // the same cache key (credentials are stripped before hashing).
        var cs1 = "Server=myserver;Database=mydb;Password=pass1";
        var cs2 = "Server=myserver;Database=mydb;Password=pass2";

        Assert.Equal(Normalize(cs1), Normalize(cs2));
    }

 // ─── Non-sensitive keys remain in the normalized key ────────────────

    [Fact]
    public void DifferentServers_ProduceDifferentNormalizedKeys()
    {
 // Cache isolation must still work: different Server= values → different keys.
        var cs1 = "Server=server1;Database=mydb;Password=secret";
        var cs2 = "Server=server2;Database=mydb;Password=secret";

        Assert.NotEqual(Normalize(cs1), Normalize(cs2));
    }

    [Fact]
    public void ServerKey_RemainsInNormalizedKey()
    {
        var cs = "Server=myserver;Database=mydb;Password=secret";
        var result = Normalize(cs);

        Assert.Contains("Server=myserver", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Database=mydb", result, StringComparison.OrdinalIgnoreCase);
    }

 // ─── Edge cases ─────────────────────────────────────────────────────

    [Fact]
    public void NullConnectionString_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, Normalize(null));
    }

    [Fact]
    public void EmptyConnectionString_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, Normalize(string.Empty));
    }

    [Fact]
    public void PasswordKeyIsCaseInsensitive()
    {
        var cs1 = "Server=s;PASSWORD=secret";
        var cs2 = "Server=s;password=secret";
        var cs3 = "Server=s;Password=secret";

 // All should strip password and produce the same key.
        Assert.Equal(Normalize(cs1), Normalize(cs2));
        Assert.Equal(Normalize(cs2), Normalize(cs3));
        Assert.DoesNotContain("secret", Normalize(cs1), StringComparison.OrdinalIgnoreCase);
    }
}
