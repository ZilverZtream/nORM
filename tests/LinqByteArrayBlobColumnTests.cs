using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins materializer + WHERE round-trip on a <c>byte[]</c> BLOB column. The
/// materializer must return a byte[] with the same bytes (length and content)
/// for each row, and equality predicates against a constant byte[] must match
/// — covers a real shape for entity classes with binary payloads
/// (file hashes, encrypted tokens, serialized state).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqByteArrayBlobColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE BlobRow (Id INTEGER PRIMARY KEY, Payload BLOB NOT NULL);
            INSERT INTO BlobRow VALUES
                (1, x'00'),
                (2, x'DEADBEEF'),
                (3, x'CAFEBABE'),
                (4, x'');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task ByteArray_column_round_trips_through_materializer()
    {
        var rows = (await _ctx.Query<BlobRow>().OrderBy(r => r.Id).ToListAsync()).ToArray();
        Assert.Equal(4, rows.Length);
        Assert.Equal(new byte[] { 0x00 },                         rows[0].Payload);
        Assert.Equal(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF },       rows[1].Payload);
        Assert.Equal(new byte[] { 0xCA, 0xFE, 0xBA, 0xBE },       rows[2].Payload);
        Assert.Equal(Array.Empty<byte>(),                         rows[3].Payload);
    }

    [Fact]
    public async Task ByteArray_equality_filter_matches_blob_with_same_bytes()
    {
        var target = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var rows = await _ctx.Query<BlobRow>().Where(r => r.Payload == target).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(2, rows[0].Id);
    }

    [Table("BlobRow")]
    public sealed class BlobRow
    {
        [Key] public int Id { get; set; }
        public byte[] Payload { get; set; } = Array.Empty<byte>();
    }
}
