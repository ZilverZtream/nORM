using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Bulk ExecuteUpdate / ExecuteDelete against a value-converter column must bind
/// the PROVIDER representation on both the WHERE predicate and the SetProperty
/// value. A converter storing an enum as a string, compared or assigned as the
/// raw CLR value, silently matches the wrong rows or writes an int into a text
/// column.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class BulkCudConverterColumnTests
{
    public enum Status { Draft = 0, Open = 1, Closed = 2 }

    [Table("BccRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public Status Status { get; set; }
        public int V { get; set; }
    }

    private sealed class StatusConverter : IValueConverter
    {
        public Type ModelType => typeof(Status);
        public Type ProviderType => typeof(string);
        public object ConvertToProvider(object? value) => ((Status)value!).ToString();
        public object ConvertFromProvider(object? value) => Enum.Parse<Status>((string)value!);
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb()
    {
        var cs = $"Data Source=file:bcc_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE BccRow_Test (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL, V INTEGER NOT NULL);
                INSERT INTO BccRow_Test VALUES
                    (1, 'Draft', 10), (2, 'Open', 20), (3, 'Open', 30), (4, 'Closed', 40);
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().Property(r => r.Status).HasConversion(new StatusConverter())
        };
        return (keeper, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static List<(int Id, string Status, int V)> ReadRaw(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT Id, Status, V FROM BccRow_Test ORDER BY Id";
        using var reader = cmd.ExecuteReader();
        var rows = new List<(int, string, int)>();
        while (reader.Read())
            rows.Add((reader.GetInt32(0), reader.GetString(1), reader.GetInt32(2)));
        return rows;
    }

    [Fact]
    public async Task Bulk_update_where_on_converter_column_targets_correct_rows()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Update only the Open rows — the WHERE compares the converter column
        // against a captured enum, which must bind the stored string 'Open'.
        var status = Status.Open;
        await ctx.Query<Row>().Where(r => r.Status == status)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.V, 999));

        var raw = ReadRaw(keeper);
        Assert.Equal(10, raw.Single(r => r.Id == 1).V);   // Draft untouched
        Assert.Equal(999, raw.Single(r => r.Id == 2).V);  // Open updated
        Assert.Equal(999, raw.Single(r => r.Id == 3).V);  // Open updated
        Assert.Equal(40, raw.Single(r => r.Id == 4).V);   // Closed untouched
    }

    [Fact]
    public async Task Bulk_update_set_converter_column_stores_provider_value()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Assign a captured enum to the converter column — must store 'Closed'.
        var newStatus = Status.Closed;
        await ctx.Query<Row>().Where(r => r.V <= 20)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.Status, newStatus));

        var raw = ReadRaw(keeper);
        Assert.Equal("Closed", raw.Single(r => r.Id == 1).Status); // V=10 → set
        Assert.Equal("Closed", raw.Single(r => r.Id == 2).Status); // V=20 → set
        Assert.Equal("Open", raw.Single(r => r.Id == 3).Status);   // V=30 → untouched
        Assert.Equal("Closed", raw.Single(r => r.Id == 4).Status); // already Closed
    }

    [Fact]
    public async Task Bulk_delete_where_on_converter_column_targets_correct_rows()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        var status = Status.Open;
        await ctx.Query<Row>().Where(r => r.Status == status).ExecuteDeleteAsync();

        var raw = ReadRaw(keeper);
        Assert.Equal(new[] { 1, 4 }, raw.Select(r => r.Id).OrderBy(i => i).ToArray());
        Assert.DoesNotContain(raw, r => r.Status == "Open");
    }

    [Fact]
    public async Task Bulk_update_converter_where_and_set_rebind_across_cached_plans()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // ONE shared call site → shared cached plan. Both the WHERE match and the
        // SET value are captured converter enums that churn per call.
        static async Task Apply(DbContext c, Status match, Status set)
            => await c.Query<Row>().Where(r => r.Status == match)
                .ExecuteUpdateAsync(s => s.SetProperty(r => r.Status, set));

        await Apply(ctx, Status.Draft, Status.Open);   // row 1 Draft → Open
        await Apply(ctx, Status.Closed, Status.Draft); // row 4 Closed → Draft

        var raw = ReadRaw(keeper);
        Assert.Equal("Open", raw.Single(r => r.Id == 1).Status);
        Assert.Equal("Open", raw.Single(r => r.Id == 2).Status);
        Assert.Equal("Open", raw.Single(r => r.Id == 3).Status);
        Assert.Equal("Draft", raw.Single(r => r.Id == 4).Status);
    }
}
