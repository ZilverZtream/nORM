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
/// drops non-ASCII, and MySQL utf8 (3-byte, vs utf8mb4) cannot hold astral-plane code points. Asserts
/// adversarial payloads round-trip ordinally exact through the nORM write path (insert AND tracked
/// update) on every configured live server, and that an embedded NUL is never silently truncated.
///
/// Each test uses a DISTINCT entity + table so the cases are isolated on the shared live databases: the
/// raw DDL table name and the entity's [Table] mapping must agree, or InsertAsync/Query would target a
/// different table than the DDL created.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class TextEncodingAdversarialCrossProviderLiveTests
{
    [Table("TextAdvXPRt_Test")]
    private sealed class TRt { [Key] public int Id { get; set; } public string S { get; set; } = ""; }

    [Table("TextAdvXPNul_Test")]
    private sealed class TNul { [Key] public int Id { get; set; } public string S { get; set; } = ""; }

    [Table("TextAdvXPUpd_Test")]
    private sealed class TUpd { [Key] public int Id { get; set; } public string S { get; set; } = ""; }

    // Build a string from raw code points (BMP or astral) — pure-ASCII source, no escape sequence to
    // corrupt. ConvertFromUtf32 emits the correct surrogate pair for astral code points.
    private static string Cp(params int[] codes) => string.Concat(codes.Select(char.ConvertFromUtf32));

    private const int ZWJ = 0x200D, ZWNJ = 0x200C, RLO = 0x202E, PDF = 0x202C, BOM = 0xFEFF, ACUTE = 0x0301;
    private const int GRIN = 0x1F600, ROBOT = 0x1F916; // astral (surrogate pairs in UTF-16)

    // NUL is excluded from the exact-match set (Postgres rejects NUL in text); it gets its own
    // round-trip-or-throw invariant below. These payloads every engine must preserve verbatim.
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

    // Provider-specific DDL for a { Id INT PK, S <unicode text> } table with the given (already
    // provider-cased) table name. Postgres quotes+preserves identifier case; NVARCHAR / utf8mb4 make the
    // column Unicode-capable so a failure means a real corruption rather than a column-type artifact.
    private static (string Table, string Ddl) Schema(string kind, string baseName)
    {
        var table = kind == "postgres" ? $"\"{baseName}\"" : baseName;
        var idCol = kind == "postgres" ? "\"Id\" INT PRIMARY KEY" : "Id INT PRIMARY KEY";
        var sCol = kind switch
        {
            "sqlserver" => "S NVARCHAR(200) NOT NULL",
            "mysql" => "S VARCHAR(200) CHARACTER SET utf8mb4 NOT NULL",
            _ => "\"S\" VARCHAR(200) NOT NULL",
        };
        return (table, $"CREATE TABLE {table} ({idCol}, {sCol})");
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Adversarial_unicode_round_trips_exactly_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var (table, ddl) = Schema(kind, "TextAdvXPRt_Test");
        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, ddl);
        try
        {
            using (var ctx = new DbContext(factory!(), provider!))
                foreach (var (id, s, _) in Rows)
                    await ctx.InsertAsync(new TRt { Id = id, S = s });

            using (var ctx = new DbContext(factory!(), provider!))
            {
                var back = ((INormQueryable<TRt>)ctx.Query<TRt>()).AsNoTracking().OrderBy(t => t.Id).ToList();
                Assert.Equal(Rows.Length, back.Count);
                for (int i = 0; i < Rows.Length; i++)
                    Assert.True(string.Equals(Rows[i].S, back[i].S, StringComparison.Ordinal),
                        $"[{kind}] payload '{Rows[i].Label}' corrupted: wrote {Describe(Rows[i].S)} read {Describe(back[i].S)}");
                Assert.Equal(2, back.Single(r => r.Id == 5).S.Length); // decomposed grapheme not NFC-normalized
            }
        }
        finally { Exec(factory!, $"DROP TABLE IF EXISTS {table}"); }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Adversarial_unicode_survives_a_tracked_update_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        // The server UPDATE path infers the parameter type independently of INSERT, so it is a distinct
        // truncation/normalization surface. A row written benign then changed to an adversarial value
        // must persist byte-exact.
        var (table, ddl) = Schema(kind, "TextAdvXPUpd_Test");
        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, ddl);
        try
        {
            using (var ctx = new DbContext(factory!(), provider!))
                foreach (var (id, _, _) in Rows)
                    await ctx.InsertAsync(new TUpd { Id = id, S = "benign" });

            using (var ctx = new DbContext(factory!(), provider!))
            {
                var tracked = ((INormQueryable<TUpd>)ctx.Query<TUpd>()).OrderBy(t => t.Id).ToList();
                for (int i = 0; i < tracked.Count; i++) tracked[i].S = Rows[i].S;
                await ctx.SaveChangesAsync();
            }

            using (var ctx = new DbContext(factory!(), provider!))
            {
                var back = ((INormQueryable<TUpd>)ctx.Query<TUpd>()).AsNoTracking().OrderBy(t => t.Id).ToList();
                Assert.Equal(Rows.Length, back.Count);
                for (int i = 0; i < Rows.Length; i++)
                    Assert.True(string.Equals(Rows[i].S, back[i].S, StringComparison.Ordinal),
                        $"[{kind}] payload '{Rows[i].Label}' corrupted on UPDATE: wrote {Describe(Rows[i].S)} read {Describe(back[i].S)}");
            }
        }
        finally { Exec(factory!, $"DROP TABLE IF EXISTS {table}"); }
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
        // nORM must uphold on EVERY engine is NO SILENT TRUNCATION — the value either round-trips
        // byte-exact or the write fails loudly. A wrote-"a\0b"-read-"a" is the data-loss bug.
        var nul = Cp('a', 0x0000, 'b');

        var (table, ddl) = Schema(kind, "TextAdvXPNul_Test");
        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, ddl);
        try
        {
            Exception? writeError = null;
            try
            {
                using var ctx = new DbContext(factory!(), provider!);
                await ctx.InsertAsync(new TNul { Id = 1, S = nul });
            }
            catch (Exception ex)
            {
                writeError = ex; // a loud rejection (e.g. Postgres refusing NUL in text) is acceptable.
            }

            if (writeError == null)
            {
                using var ctx = new DbContext(factory!(), provider!);
                var back = ((INormQueryable<TNul>)ctx.Query<TNul>()).AsNoTracking().Single();
                Assert.True(string.Equals(nul, back.S, StringComparison.Ordinal),
                    $"[{kind}] embedded NUL SILENTLY TRUNCATED (data loss): wrote {Describe(nul)} read {Describe(back.S)}");
            }
        }
        finally { Exec(factory!, $"DROP TABLE IF EXISTS {table}"); }
    }

    private static string Describe(string s) =>
        $"[{s.Length}] " + string.Join(" ", s.Select(ch => ((int)ch).ToString("X4")));
}
