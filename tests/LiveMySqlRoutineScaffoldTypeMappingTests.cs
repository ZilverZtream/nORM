#nullable enable

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

// End-to-end verification that the MySQL routine discovery + shared type mapper produce data-lossless CLR
// parameter types for provider-ambiguous integer types. MySQL TINYINT is signed (-128..127) — mapping it to
// byte silently corrupts negatives — while SQL Server TINYINT is unsigned; the discovery must therefore emit
// a MySQL-distinct token ("tinyint signed") that the shared mapper resolves to sbyte. A MySQL BIT(M>1) bit
// field must not collapse to a single bool. Live-only: needs a real MySQL server (NORM_TEST_MYSQL).
public class LiveMySqlRoutineScaffoldTypeMappingTests
{
    [Fact]
    public void MySqlRoutineDiscovery_MapsSignedTinyintAndWideBitToLosslessClrTypes()
    {
        var cs = LiveProviderEnvironment.GetConnectionString("mysql");
        if (string.IsNullOrEmpty(cs)) return; // NORM_TEST_MYSQL not set → skip.

        using var cn = OpenMySql(cs);
        const string proc = "norm_scaffold_typemap_proc";

        try
        {
            Exec(cn, $"DROP PROCEDURE IF EXISTS `{proc}`");
            // signed TINYINT, TINYINT UNSIGNED, a wide BIT field, and BIT(1).
            Exec(cn, $"CREATE PROCEDURE `{proc}`(IN a TINYINT, IN b TINYINT UNSIGNED, IN c BIT(8), IN d BIT(1)) BEGIN SELECT a; END");

            var detail = GetRoutineDetail(cn, proc);
            Assert.NotNull(detail);

            // Drive the real discovery → metadata → CLR-type chain.
            var metadata = ScaffoldRoutineMetadataBuilder.BuildMetadata(detail!);
            var parameters = ScaffoldRoutineMetadataReader.GetRoutineInputParameters(metadata, useNullableReferenceTypes: true);
            var byName = parameters.ToDictionary(p => p.Name.TrimStart('@'), StringComparer.OrdinalIgnoreCase);

            // BUG (before the fix): signed TINYINT → byte? (loses -128..-1), BIT(8) → bool? (loses the field).
            Assert.Equal("sbyte?", byName["a"].TypeName);
            Assert.Equal("byte?", byName["b"].TypeName);
            Assert.Equal("ulong?", byName["c"].TypeName);
            Assert.Equal("bool?", byName["d"].TypeName);
        }
        finally
        {
            try { Exec(cn, $"DROP PROCEDURE IF EXISTS `{proc}`"); } catch { }
        }
    }

    private static string? GetRoutineDetail(DbConnection cn, string routineName)
    {
        var sql = (string)typeof(ScaffoldMySqlSkippedObjectDiscovery)
            .GetMethod("GetSkippedObjectSql", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, null)!;

        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var kind = Convert.ToString(reader.GetValue(2));
            var name = Convert.ToString(reader.GetValue(1));
            if (string.Equals(kind, "Routine", StringComparison.Ordinal)
                && string.Equals(name, routineName, StringComparison.OrdinalIgnoreCase))
                return Convert.ToString(reader.GetValue(3));
        }

        return null;
    }

    private static DbConnection OpenMySql(string cs)
    {
        var type = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")
            ?? throw new InvalidOperationException("Cannot load MySqlConnector.MySqlConnection.");
        var cn = (DbConnection)Activator.CreateInstance(type, cs)!;
        cn.Open();
        return cn;
    }

    private static void Exec(DbConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
