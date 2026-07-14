using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The rename-before-index/alter ordering fix, executed against real servers:
/// each provider's generated Up must rename an indexed column before rebuilding
/// its index (and before altering it), and the generated Down must rename back
/// before restoring pre-rename index and column definitions — with seeded rows
/// surviving both directions.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderMigrationRenameOrderingTests
{
    private static (Func<DbConnection>?, Func<SchemaDiff, MigrationSqlStatements>?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_MYSQL not set");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), d => new MySqlMigrationSqlGenerator().GenerateSql(d), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), d => new PostgresMigrationSqlGenerator().GenerateSql(d), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), d => new SqlServerMigrationSqlGenerator().GenerateSql(d), null);
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

    private static void Exec(Func<DbConnection> factory, IEnumerable<string> statements)
    {
        using var cn = factory();
        foreach (var sql in statements)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
    }

    private static List<long> ReadColumn(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        var values = new List<long>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            values.Add(Convert.ToInt64(reader.GetValue(0)));
        return values;
    }

    private static TableSchema Table(string valName, string? valPreviousName, bool valNullable, bool withExtra)
    {
        var t = new TableSchema { Name = "RenameOrd_Test" };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        var val = new ColumnSchema { Name = valName, ClrType = typeof(long).FullName!, IsNullable = valNullable };
        if (valPreviousName != null)
            val.PreviousName = valPreviousName;
        val.Indexes.Add(new ColumnIndexSchema { Name = "IX_RenameOrd_Val", IsUnique = false, Order = 0 });
        t.Columns.Add(val);
        if (withExtra)
            t.Columns.Add(new ColumnSchema { Name = "Extra", ClrType = typeof(long).FullName!, IsNullable = true });
        return t;
    }

    private static void DropIfExists(Func<DbConnection> factory, string kind)
    {
        var drop = kind == "sqlserver"
            ? "IF OBJECT_ID('RenameOrd_Test') IS NOT NULL DROP TABLE RenameOrd_Test"
            : kind == "postgres" ? "DROP TABLE IF EXISTS \"RenameOrd_Test\"" : "DROP TABLE IF EXISTS RenameOrd_Test";
        Exec(factory, new[] { drop });
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Renamed_indexed_column_migrates_up_and_down_with_data(string kind)
    {
        var (factory, generate, skip) = OpenLive(kind);
        if (skip != null) return;

        DropIfExists(factory!, kind);
        try
        {
            var baseline = Table("Val", null, valNullable: true, withExtra: false);
            Exec(factory!, generate!(new SchemaDiff { AddedTables = { baseline } }).Up);

            var quotedInsert = kind == "postgres"
                ? "INSERT INTO \"RenameOrd_Test\" (\"Id\", \"Val\") VALUES (1, 10), (2, 20), (3, 30)"
                : "INSERT INTO RenameOrd_Test (Id, Val) VALUES (1, 10), (2, 20), (3, 30)";
            Exec(factory!, new[] { quotedInsert });

            // Rename the indexed column AND add a column in one migration — the
            // shape whose old ordering rebuilt the index before the rename ran.
            var target = Table("ValX", "Val", valNullable: true, withExtra: true);
            var diff = SchemaDiffer.Diff(
                new SchemaSnapshot { Tables = { baseline } },
                new SchemaSnapshot { Tables = { target } });
            var sql = generate!(diff);

            Exec(factory!, sql.Up);
            var afterUp = ReadColumn(factory!, kind == "postgres"
                ? "SELECT \"ValX\" FROM \"RenameOrd_Test\" ORDER BY \"Id\""
                : "SELECT ValX FROM RenameOrd_Test ORDER BY Id");
            Assert.Equal(new long[] { 10, 20, 30 }, afterUp);

            Exec(factory!, sql.Down);
            var afterDown = ReadColumn(factory!, kind == "postgres"
                ? "SELECT \"Val\" FROM \"RenameOrd_Test\" ORDER BY \"Id\""
                : "SELECT Val FROM RenameOrd_Test ORDER BY Id");
            Assert.Equal(new long[] { 10, 20, 30 }, afterDown);
        }
        finally
        {
            DropIfExists(factory!, kind);
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Renamed_and_tightened_column_migrates_up_and_down_with_data(string kind)
    {
        var (factory, generate, skip) = OpenLive(kind);
        if (skip != null) return;

        DropIfExists(factory!, kind);
        try
        {
            var baseline = Table("Val", null, valNullable: true, withExtra: false);
            Exec(factory!, generate!(new SchemaDiff { AddedTables = { baseline } }).Up);

            var quotedInsert = kind == "postgres"
                ? "INSERT INTO \"RenameOrd_Test\" (\"Id\", \"Val\") VALUES (1, 10), (2, 20)"
                : "INSERT INTO RenameOrd_Test (Id, Val) VALUES (1, 10), (2, 20)";
            Exec(factory!, new[] { quotedInsert });

            // Rename AND tighten nullability on the SAME column — the alter
            // statement references the post-rename name, so it must run after
            // the rename on Up (and revert before the rename-back on Down).
            var target = Table("ValX", "Val", valNullable: false, withExtra: false);
            var diff = SchemaDiffer.Diff(
                new SchemaSnapshot { Tables = { baseline } },
                new SchemaSnapshot { Tables = { target } });
            var sql = generate!(diff);

            Exec(factory!, sql.Up);
            var afterUp = ReadColumn(factory!, kind == "postgres"
                ? "SELECT \"ValX\" FROM \"RenameOrd_Test\" ORDER BY \"Id\""
                : "SELECT ValX FROM RenameOrd_Test ORDER BY Id");
            Assert.Equal(new long[] { 10, 20 }, afterUp);

            Exec(factory!, sql.Down);
            var afterDown = ReadColumn(factory!, kind == "postgres"
                ? "SELECT \"Val\" FROM \"RenameOrd_Test\" ORDER BY \"Id\""
                : "SELECT Val FROM RenameOrd_Test ORDER BY Id");
            Assert.Equal(new long[] { 10, 20 }, afterDown);
        }
        finally
        {
            DropIfExists(factory!, kind);
        }
    }
}
