using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A primary-key-less table (an append-only log, a staging table, a view) is scaffolded as a
/// [ReadOnlyEntity]. If such an entity has a column named "Id", the key convention manufactured a spurious
/// primary key for it, so read-path identity resolution collapsed rows that legitimately share that value —
/// the second row's data silently vanished from the result. A read-only entity with no explicitly configured
/// key must be treated as keyless (no convention key, no identity resolution), matching EF Core's keyless
/// query type for a PK-less table.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ReadOnlyKeylessEntityRowCollapseTests
{
    [ReadOnlyEntity]
    [Table("KolLog_Test")]
    public class LogEntry
    {
        public int Id { get; set; }
        public string Message { get; set; } = "";
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // No PRIMARY KEY — a duplicate Id is legal and both rows must survive the read.
            cmd.CommandText =
                "CREATE TABLE KolLog_Test (Id INTEGER, Message TEXT NOT NULL);" +
                "INSERT INTO KolLog_Test VALUES (1, 'a'), (1, 'b'), (2, 'c');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void Read_only_keyless_entity_does_not_collapse_duplicate_id_rows()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var messages = ctx.Query<LogEntry>()
            .OrderBy(e => e.Message)
            .ToList()
            .Select(e => e.Message)
            .ToArray();

        // All three physical rows must come back — the two Id=1 rows must not merge into one.
        Assert.Equal(new[] { "a", "b", "c" }, messages);
    }
}
