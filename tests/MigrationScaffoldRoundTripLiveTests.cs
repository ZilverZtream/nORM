using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using nORM.Migration;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A model migrated by nORM and then scaffolded back from the live database must
/// come back with the same CLR property types — otherwise the migrate/scaffold
/// pair silently drifts the model (e.g. a byte column widening to a signed type,
/// or a decimal collapsing to double). Exercises the MySQL migration type map
/// (DATETIME(6)/TIME(6)/TINYINT UNSIGNED/LONGTEXT) end to end against a real
/// server. Types MySQL cannot represent distinctly are asserted at their
/// documented round-trip type instead: TimeOnly and TimeSpan share TIME, and
/// DateTimeOffset has no offset-carrying type.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class MigrationScaffoldRoundTripLiveTests
{
    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenMySql()
    {
        var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
        if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_MYSQL not set");
        var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
        return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), null);
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

    [Fact]
    public async Task Migrated_scalar_types_scaffold_back_to_the_original_clr_types_on_live_mysql()
    {
        var (factory, provider, skip) = OpenMySql();
        if (skip != null) return;

        var scalars = new (string Name, Type Clr, string ExpectedScaffoldType)[]
        {
            ("IntCol", typeof(int), "int"),
            ("LongCol", typeof(long), "long"),
            ("ShortCol", typeof(short), "short"),
            ("ByteCol", typeof(byte), "byte"),
            ("BoolCol", typeof(bool), "bool"),
            ("StringCol", typeof(string), "string"),
            ("DateTimeCol", typeof(DateTime), "DateTime"),
            ("DecimalCol", typeof(decimal), "decimal"),
            ("DoubleCol", typeof(double), "double"),
            ("FloatCol", typeof(float), "float"),
            ("GuidCol", typeof(Guid), "Guid"),
            ("BlobCol", typeof(byte[]), "byte[]"),
            ("DateOnlyCol", typeof(DateOnly), "DateOnly"),
            ("SByteCol", typeof(sbyte), "sbyte"),
            ("UShortCol", typeof(ushort), "ushort"),
            ("UIntCol", typeof(uint), "uint"),
            ("ULongCol", typeof(ulong), "ulong"),
            // MySQL TIME serves both TimeSpan and TimeOnly; the scaffolder's
            // documented choice for TIME is TimeSpan.
            ("TimeSpanCol", typeof(TimeSpan), "TimeSpan"),
            ("TimeOnlyCol", typeof(TimeOnly), "TimeSpan"),
            // MySQL has no offset-carrying type; DATETIME(6) scaffolds as DateTime.
            ("DtoCol", typeof(DateTimeOffset), "DateTime"),
        };

        var diff = new SchemaDiff();
        var table = new TableSchema
        {
            Name = "MigScaffoldRoundTrip_Test",
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false },
            }
        };
        foreach (var (name, clr, _) in scalars)
            table.Columns.Add(new ColumnSchema { Name = name, ClrType = clr.FullName!, IsNullable = false });
        diff.AddedTables.Add(table);

        var statements = new MySqlMigrationSqlGenerator().GenerateSql(diff);

        var dir = Path.Combine(Path.GetTempPath(), "norm_scaffold_rt_" + Guid.NewGuid().ToString("N"));
        Exec(factory!, "DROP TABLE IF EXISTS MigScaffoldRoundTrip_Test");
        try
        {
            foreach (var sql in statements.Up)
                Exec(factory!, sql);

            // Scope the scaffold to this test's table: the live database is shared and
            // concurrent tests create/drop their own tables mid-run, so scaffolding
            // everything races against their cleanup.
            var options = new ScaffoldOptions { Tables = new[] { "MigScaffoldRoundTrip_Test" } };
            using (var cn = factory!())
                await DatabaseScaffolder.ScaffoldAsync(cn, provider!, dir, "RoundTripNs", "RoundTripCtx", options);

            var entityFile = Directory.GetFiles(dir, "*.cs")
                .SingleOrDefault(f => Path.GetFileNameWithoutExtension(f).StartsWith("Migscaffoldroundtrip", StringComparison.OrdinalIgnoreCase));
            if (entityFile == null)
                Assert.Fail($"entity file not found; generated: {string.Join(", ", Directory.GetFiles(dir).Select(Path.GetFileName))}");
            var entityCode = File.ReadAllText(entityFile!);
            foreach (var (name, _, expected) in scalars)
            {
                Assert.Contains($"public {expected} {name}", entityCode);
            }
        }
        finally
        {
            Exec(factory!, "DROP TABLE IF EXISTS MigScaffoldRoundTrip_Test");
            try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }
}
