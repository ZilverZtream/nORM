using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Sharper hunt: Skip(a).Take(n).Skip(b) composition where the FIRST Skip is a
/// captured (parameter) offset. Oracle = LINQ-to-Objects. A dropped first offset
/// silently returns the wrong window with no exception.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PaginationOrderingCompositeTests : IDisposable
{
    private readonly ITestOutputHelper _out;
    private readonly SqliteConnection _cn;
    private readonly DbContext _ctx;
    private readonly List<PcpRow> _oracle;

    public PaginationOrderingCompositeTests(ITestOutputHelper o)
    {
        _out = o;
        _cn = new SqliteConnection("Data Source=:memory:");
        _cn.Open();
        using (var cmd = _cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PcpRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL);" +
                "INSERT INTO PcpRow (Id,A) VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);";
            cmd.ExecuteNonQuery();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);
        _oracle = Enumerable.Range(1, 12).Select(i => new PcpRow { Id = i, A = i }).ToList();
    }

    public void Dispose() { _ctx.Dispose(); _cn.Dispose(); }

    private int[] O(Func<IEnumerable<PcpRow>, IEnumerable<PcpRow>> shape) => shape(_oracle).Select(r => r.Id).ToArray();
    private int[] N(Func<IQueryable<PcpRow>, IQueryable<PcpRow>> shape) => shape(_ctx.Query<PcpRow>()).ToList().Select(r => r.Id).ToArray();

    [Fact]
    public void Skip_param_Take_lit_Skip_lit()
    {
        int a = 2; // captured => parameter
        var oracle = O(q => q.OrderBy(r => r.Id).Skip(a).Take(6).Skip(2));
        var norm = N(q => q.OrderBy(r => r.Id).Skip(a).Take(6).Skip(2));
        _out.WriteLine($"skip(param a).take(6).skip(2)  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Skip_lit_Take_lit_Skip_param()
    {
        int b = 2; // captured => parameter
        var oracle = O(q => q.OrderBy(r => r.Id).Skip(2).Take(6).Skip(b));
        var norm = N(q => q.OrderBy(r => r.Id).Skip(2).Take(6).Skip(b));
        _out.WriteLine($"skip(2).take(6).skip(param b)  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Skip_param_Take_lit_Skip_param()
    {
        int a = 2, b = 2; // both captured => parameters
        var oracle = O(q => q.OrderBy(r => r.Id).Skip(a).Take(6).Skip(b));
        var norm = N(q => q.OrderBy(r => r.Id).Skip(a).Take(6).Skip(b));
        _out.WriteLine($"skip(param a).take(6).skip(param b)  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Skip_param_Take_param_Skip_lit()
    {
        int a = 2, n = 6; // captured
        var oracle = O(q => q.OrderBy(r => r.Id).Skip(a).Take(n).Skip(2));
        var norm = N(q => q.OrderBy(r => r.Id).Skip(a).Take(n).Skip(2));
        _out.WriteLine($"skip(param a).take(param n).skip(2)  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    // Regular-query equivalent of the compiled Skip(pageIndex*size) bug: captured locals
    // are constant-folded, so the computed offset works. Confirms the drop is compiled-only.
    [Fact]
    public void Regular_Skip_computed_captured_offset_is_folded()
    {
        int pageIndex = 2, pageSize = 4;
        var oracle = O(q => q.OrderBy(r => r.Id).Skip(pageIndex * pageSize).Take(pageSize));
        var norm = N(q => q.OrderBy(r => r.Id).Skip(pageIndex * pageSize).Take(pageSize));
        _out.WriteLine($"regular Skip(pageIndex*size)  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    // Control: the all-literal case is known-good (existing PagingCompositionTests).
    [Fact]
    public void Skip_lit_Take_lit_Skip_lit_control()
    {
        var oracle = O(q => q.OrderBy(r => r.Id).Skip(2).Take(6).Skip(2));
        var norm = N(q => q.OrderBy(r => r.Id).Skip(2).Take(6).Skip(2));
        _out.WriteLine($"skip(2).take(6).skip(2) control  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    [Table("PcpRow")]
    public sealed class PcpRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
    }
}
