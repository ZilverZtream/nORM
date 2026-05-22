using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class CompiledQuerySqlShapeParityTests
{
    [Table("SqlShapeUser")]
    public class SqlShapeUser
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool IsActive { get; set; }
        public int Age { get; set; }
        public string City { get; set; } = "";
    }

    [Table("SqlShapeOrder")]
    public class SqlShapeOrder
    {
        [Key]
        public int Id { get; set; }
        public int UserId { get; set; }
        public decimal Amount { get; set; }
        public string ProductName { get; set; } = "";
    }

    public sealed record SqlShapeJoinRow(string Name, decimal Amount, string ProductName);

    [Fact]
    public async Task SimpleQuery_CompiledAndRuntime_UseSameSqlShape()
    {
        using var cn = CreateConnection();
        var interceptor = new SqlCaptureInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var take = 2;
        var runtime = await ctx.Query<SqlShapeUser>()
            .Where(u => u.IsActive == true)
            .OrderBy(u => u.Id)
            .Take(take)
            .ToListAsync();
        var runtimeSql = interceptor.LastReaderSql;

        var compiled = Norm.CompileQuery((DbContext c, int t) =>
            c.Query<SqlShapeUser>()
                .Where(u => u.IsActive == true)
                .OrderBy(u => u.Id)
                .Take(t));

        interceptor.LastReaderSql = null;
        var compiledRows = await compiled(ctx, take);

        Assert.Equal(runtime.Select(u => u.Id), compiledRows.Select(u => u.Id));
        AssertSqlShapeEqual(runtimeSql, interceptor.LastReaderSql);
    }

    [Fact]
    public async Task ComplexQuery_CompiledAndRuntime_UseSameSqlShape()
    {
        using var cn = CreateConnection();
        var interceptor = new SqlCaptureInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var minAge = 25;
        var city = "New York";
        var runtime = await ctx.Query<SqlShapeUser>()
            .Where(u => u.IsActive == true && u.Age > minAge && u.City == city)
            .OrderBy(u => u.Name)
            .Skip(1)
            .Take(2)
            .ToListAsync();
        var runtimeSql = interceptor.LastReaderSql;

        var compiled = Norm.CompileQuery((DbContext c, (int MinAge, string City) p) =>
            c.Query<SqlShapeUser>()
                .Where(u => u.IsActive == true && u.Age > p.MinAge && u.City == p.City)
                .OrderBy(u => u.Name)
                .Skip(1)
                .Take(2));

        interceptor.LastReaderSql = null;
        var compiledRows = await compiled(ctx, (minAge, city));

        Assert.Equal(runtime.Select(u => u.Id), compiledRows.Select(u => u.Id));
        AssertSqlShapeEqual(runtimeSql, interceptor.LastReaderSql);
    }

    [Fact]
    public async Task JoinQuery_CompiledAndRuntime_UseSameSqlShape()
    {
        using var cn = CreateConnection();
        var interceptor = new SqlCaptureInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var minAmount = 100m;
        var runtime = await ctx.Query<SqlShapeUser>()
            .Join(ctx.Query<SqlShapeOrder>(), u => u.Id, o => o.UserId,
                (u, o) => new { u.Name, o.Amount, o.ProductName })
            .Where(x => x.Amount > minAmount)
            .Take(10)
            .ToListAsync();
        var runtimeSql = interceptor.LastReaderSql;

        var compiled = Norm.CompileQuery<DbContext, decimal, object>((c, amount) =>
            c.Query<SqlShapeUser>()
                .Join(c.Query<SqlShapeOrder>(), u => u.Id, o => o.UserId,
                    (u, o) => new { u.Name, o.Amount, o.ProductName })
                .Where(x => x.Amount > amount)
                .Take(10));

        interceptor.LastReaderSql = null;
        var compiledRows = await compiled(ctx, minAmount);

        Assert.Equal(runtime.Count, compiledRows.Count);
        AssertSqlShapeEqual(runtimeSql, interceptor.LastReaderSql);
    }

    [Fact]
    public async Task JoinQuery_WithConstructorDtoProjection_CanFilterProjectedMembers()
    {
        using var cn = CreateConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var runtime = await ctx.Query<SqlShapeUser>()
            .Join(ctx.Query<SqlShapeOrder>(), u => u.Id, o => o.UserId,
                (u, o) => new SqlShapeJoinRow(u.Name, o.Amount, o.ProductName))
            .Where(x => x.Amount > 100m)
            .Take(10)
            .ToListAsync();

        var compiled = Norm.CompileQuery<DbContext, decimal, SqlShapeJoinRow>((c, amount) =>
            c.Query<SqlShapeUser>()
                .Join(c.Query<SqlShapeOrder>(), u => u.Id, o => o.UserId,
                    (u, o) => new SqlShapeJoinRow(u.Name, o.Amount, o.ProductName))
                .Where(x => x.Amount > amount)
                .Take(10));

        var compiledRows = await compiled(ctx, 100m);

        Assert.Equal(new[]
        {
            new SqlShapeJoinRow("Ada", 150m, "Keyboard"),
            new SqlShapeJoinRow("Ada", 225m, "Monitor")
        }, runtime);
        Assert.Equal(runtime, compiledRows);
    }

    private static SqliteConnection CreateConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SqlShapeUser (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                IsActive INTEGER NOT NULL,
                Age INTEGER NOT NULL,
                City TEXT NOT NULL
            );
            CREATE TABLE SqlShapeOrder (
                Id INTEGER PRIMARY KEY,
                UserId INTEGER NOT NULL,
                Amount REAL NOT NULL,
                ProductName TEXT NOT NULL
            );
            INSERT INTO SqlShapeUser VALUES (1, 'Ada', 1, 30, 'New York');
            INSERT INTO SqlShapeUser VALUES (2, 'Ben', 1, 26, 'New York');
            INSERT INTO SqlShapeUser VALUES (3, 'Cid', 0, 40, 'London');
            INSERT INTO SqlShapeOrder VALUES (1, 1, 150.0, 'Keyboard');
            INSERT INTO SqlShapeOrder VALUES (2, 2, 75.0, 'Mouse');
            INSERT INTO SqlShapeOrder VALUES (3, 1, 225.0, 'Monitor');
            """;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static void AssertSqlShapeEqual(string? expected, string? actual)
    {
        Assert.False(string.IsNullOrWhiteSpace(expected));
        Assert.False(string.IsNullOrWhiteSpace(actual));
        var expectedShape = NormalizeSqlShape(expected!);
        var actualShape = NormalizeSqlShape(actual!);
        Assert.True(
            expectedShape == actualShape,
            $"Expected SQL:{Environment.NewLine}{expectedShape}{Environment.NewLine}Actual SQL:{Environment.NewLine}{actualShape}");
    }

    private static string NormalizeSqlShape(string sql)
    {
        var normalized = Regex.Replace(sql, @"@\w+", "@p");
        normalized = Regex.Replace(normalized, @"\b(LIMIT|OFFSET)\s+(?:@p|\d+)\b", "$1 #", RegexOptions.IgnoreCase);
        return Regex.Replace(normalized, @"\s+", " ").Trim();
    }

    private sealed class SqlCaptureInterceptor : IDbCommandInterceptor
    {
        public string? LastReaderSql { get; set; }

        public InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        {
            LastReaderSql = command.CommandText;
            return InterceptionResult<DbDataReader>.Continue();
        }

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            LastReaderSql = command.CommandText;
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct) =>
            Task.FromResult(InterceptionResult<int>.Continue());
        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken ct) =>
            Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct) =>
            Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken ct) =>
            Task.CompletedTask;
        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken ct) =>
            Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken ct) =>
            Task.CompletedTask;
    }
}
