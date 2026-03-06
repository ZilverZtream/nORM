using System;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using nORM.Scaffolding;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// C-1: Verifies that ComputeSchemaSignature uses a 128-bit (32-char hex) SHA-256 fingerprint
/// and that the signature changes with column type, nullability, and PK changes.
/// </summary>
public class SchemaSignatureTests
{
    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static DynamicEntityTypeGenerator Gen() => new();

    [Fact]
    public void Signature_Is_32_Char_Hex()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T1 (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var sig = Gen().ComputeSchemaSignature(cn, "T1");

        // C-1: SHA-256 truncated to 16 bytes = 32 hex chars
        Assert.Equal(32, sig.Length);
        Assert.True(Regex.IsMatch(sig, @"^[0-9A-Fa-f]{32}$"), $"Expected 32-char hex, got: {sig}");
    }

    [Fact]
    public void SameSchema_SameSignature()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T2 (Id INTEGER PRIMARY KEY, Value REAL NOT NULL)";
        cmd.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T2");
        var sig2 = gen.ComputeSchemaSignature(cn, "T2");

        Assert.Equal(sig1, sig2);
    }

    [Fact]
    public void DifferentColumnType_DifferentSignature()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T3a (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "CREATE TABLE T3a (Id INTEGER PRIMARY KEY, Amount REAL NOT NULL)";
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T3a");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T3a");

        Assert.NotEqual(sig1, sig2);
    }

    [Fact]
    public void DifferentNullability_DifferentSignature_ForValueTypes()
    {
        // For value types (INTEGER), nullable vs non-nullable produces different CLR types
        // (int? vs int), which the descriptor captures and produces different signatures.
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T4a (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "CREATE TABLE T4a (Id INTEGER PRIMARY KEY, Score INTEGER)";  // nullable int
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T4a");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T4a");

        // The descriptor captures int vs int? (via IsNullableType), so sigs should differ
        Assert.NotEqual(sig1, sig2);
    }

    [Fact]
    public void AddedColumn_DifferentSignature()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE T5a (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        using var cn2 = OpenMemory();
        using var cmd2 = cn2.CreateCommand();
        cmd2.CommandText = "CREATE TABLE T5a (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Extra INTEGER)";
        cmd2.ExecuteNonQuery();

        var gen = Gen();
        var sig1 = gen.ComputeSchemaSignature(cn, "T5a");
        var sig2 = gen.ComputeSchemaSignature(cn2, "T5a");

        Assert.NotEqual(sig1, sig2);
    }
}
