using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Cross-provider companion to <see cref="TextEncodingAdversarialFidelityTests"/> (A+ dimension C). The
/// SQLite sweep proved nORM preserves adversarial text locally; the higher silent-corruption risk is on
/// the SERVER engines, where the wrong column type mangles Unicode: SQL Server VARCHAR (vs NVARCHAR)
/// drops non-ASCII, and MySQL utf8 (3-byte, vs utf8mb4) cannot hold astral-plane code points. This
/// asserts astral emoji, BiDi/zero-width controls, a mid-string BOM, and a decomposed grapheme all
/// round-trip ordinally exact through the nORM write path on every configured live server — a silent
/// truncation or normalization would be the exact data loss the project bars.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class TextEncodingAdversarialCrossProviderLiveTests
{
    [Table("TextAdvXP_Test")]
    private sealed class T
    {
        [Key] public int Id { get; set; }
        public string S { get; set; } = "";
    }

    // Build a string from raw code points (BMP or astral) — pure-ASCII source, no escape sequence to
    // corrupt. ConvertFromUtf32 emits the correct surrogate pair for astral code points.
    private static string Cp(params int[] codes) => string.Concat(codes.Select(char.ConvertFromUtf32));

    private const int ZWJ = 0x200D, ZWNJ = 0x200C, RLO = 0x202E, PDF = 0x202C, BOM = 0xFEFF, ACUTE = 0x0301;
    private const int GRIN = 0x1F600, ROBOT = 0x1F916; // astral (surrogate pairs in UTF-16)

    // NUL is deliberately excluded: Postgres rejects NUL in text at the DB level, so it needs a separate
    // round-trip-or-throw invariant rather than an exact-match assertion. These payloads every engine must
    // preserve verbatim given a Unicode-capable column.
    private static readonly (int Id, string S, string Label)[] Rows =
    {
        (1, Cp('c', 'a', 'f', 0x00E9, ' ', GRIN, ' ', ROBOT), "BMP accent + astral emoji"),
        (2, Cp('a', ZWJ, 'b', ZWNJ, 'c'), "zero-width joiner/non-joiner"),
        (3, Cp(RLO, 'a', 'b', 'c', PDF), "RTL override + pop"),
        (4, Cp('x', BOM, 'y'), "mid-string BOM"),
        (5, Cp('e', ACUTE), "decomposed grapheme (must stay 2 code units)"),
    };

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_MYSQL not set");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), null);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static DbConnection Open(Type connectionType, string cs)
    {
        var cn = (DbConnection)Activator.CreateInstance(connectionType, cs)!;
        cn.Open();
        return cn;
    }

    private static void Exec(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Adversarial_unicode_round_trips_exactly_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var table = kind == "postgres" ? "\"TextAdvXP_Test\"" : "TextAdvXP_Test";
        var idCol = kind == "postgres" ? "\"Id\" INT PRIMARY KEY" : "Id INT PRIMARY KEY";
        var sCol = kind switch
        {
            "sqlserver" => "S NVARCHAR(200) NOT NULL",
            "mysql" => "S VARCHAR(200) CHARACTER SET utf8mb4 NOT NULL",
            _ => "\"S\" VARCHAR(200) NOT NULL", // postgres, UTF-8 by default
        };

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {sCol})");
        try
        {
            using (var ctx = new DbContext(factory!(), provider!))
            {
                foreach (var (id, s, _) in Rows)
                    await ctx.InsertAsync(new T { Id = id, S = s });
            }

            using (var ctx = new DbContext(factory!(), provider!))
            {
                var back = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking().OrderBy(t => t.Id).ToList();
                Assert.Equal(Rows.Length, back.Count);
                for (int i = 0; i < Rows.Length; i++)
                    Assert.True(string.Equals(Rows[i].S, back[i].S, StringComparison.Ordinal),
                        $"[{kind}] payload '{Rows[i].Label}' corrupted: wrote {Describe(Rows[i].S)} read {Describe(back[i].S)}");

                // The decomposed grapheme must not have been NFC-normalized by the server.
                Assert.Equal(2, back.Single(r => r.Id == 5).S.Length);
            }
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Embedded_nul_round_trips_exactly_or_is_rejected_never_silently_truncated(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        // "a\0b" — a native C-string terminator can truncate this to "a". Providers legitimately differ:
        // SQLite/MySQL/SQL Server can store an embedded NUL; Postgres rejects NUL in text. The invariant
        // nORM must uphold on EVERY engine is that there is NO SILENT TRUNCATION — the value either
        // round-trips byte-exact or the write fails loudly. A wrote-"a\0b"-read-"a" is the data-loss bug.
        var nul = Cp('a', 0x0000, 'b');

        var table = kind == "postgres" ? "\"TextAdvXPNul_Test\"" : "TextAdvXPNul_Test";
        var idCol = kind == "postgres" ? "\"Id\" INT PRIMARY KEY" : "Id INT PRIMARY KEY";
        var sCol = kind switch
        {
            "sqlserver" => "S NVARCHAR(200) NOT NULL",
            "mysql" => "S VARCHAR(200) CHARACTER SET utf8mb4 NOT NULL",
            _ => "\"S\" VARCHAR(200) NOT NULL",
        };

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {sCol})");
        try
        {
            Exception? writeError = null;
            try
            {
                using var ctx = new DbContext(factory!(), provider!);
                await ctx.InsertAsync(new T { Id = 1, S = nul });
            }
            catch (Exception ex)
            {
                writeError = ex; // a loud rejection (e.g. Postgres refusing NUL in text) is acceptable.
            }

            if (writeError == null)
            {
                using var ctx = new DbContext(factory!(), provider!);
                var back = ((INormQueryable<T>)ctx.Query<T>()).AsNoTracking().Single();
                Assert.True(string.Equals(nul, back.S, StringComparison.Ordinal),
                    $"[{kind}] embedded NUL SILENTLY TRUNCATED (data loss): wrote {Describe(nul)} read {Describe(back.S)}");
            }
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }

    private static string Describe(string s) =>
        $"[{s.Length}] " + string.Join(" ", s.Select(ch => ((int)ch).ToString("X4")));
}
