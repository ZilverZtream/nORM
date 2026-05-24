using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Strict pin verifying the VisitNew constant-folding work (2d6b915) covers
/// inline <c>new Guid("...")</c> on the RHS of a Where comparison. This is
/// the most common shape for an externally-supplied entity key written
/// inline in a query.
///
/// Before the fold landed any inline NewExpression in Where -- DateTime,
/// TimeSpan, Guid -- silently produced unbound parameter markers in SQL
/// and broke with 'SQLite Error 1: near "@p<N>": syntax error'. The fold
/// is type-generic (excludes only typeof(string)), so Guid should round-
/// trip; this file pins that fact.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereInlineGuidConstructorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;
    private static readonly Guid GuidA = new("11111111-1111-1111-1111-111111111111");
    private static readonly Guid GuidB = new("22222222-2222-2222-2222-222222222222");
    private static readonly Guid GuidC = new("33333333-3333-3333-3333-333333333333");

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WigItem (Id INTEGER PRIMARY KEY, Token TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var rows = new (int id, Guid token)[]
        {
            (1, GuidA),
            (2, GuidB),
            (3, GuidC),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO WigItem (Id, Token) VALUES ($id, $t)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var pt = insert.CreateParameter(); pt.ParameterName = "$t"; insert.Parameters.Add(pt);
        foreach (var (id, tok) in rows)
        {
            pid.Value = id;
            pt.Value = tok.ToString();
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WigItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_inline_Guid_string_constructor_matches_row()
    {
        // The 1-arg Guid(string) constructor is the most common inline form.
        // Without the VisitNew fold this previously emitted broken SQL with
        // an unbound parameter; the fold evaluates the constructor at
        // translation time and binds the Guid as a single parameter.
        var result = await _ctx.Query<WigItem>()
            .Where(i => i.Token == new Guid("22222222-2222-2222-2222-222222222222"))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_inline_Guid_string_constructor_in_negated_compare()
    {
        var result = await _ctx.Query<WigItem>()
            .Where(i => i.Token != new Guid("11111111-1111-1111-1111-111111111111"))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WigItem")]
    public sealed class WigItem
    {
        [Key] public int Id { get; set; }
        public Guid Token { get; set; }
    }
}
