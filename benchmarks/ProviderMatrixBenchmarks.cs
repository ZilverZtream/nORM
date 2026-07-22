using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Npgsql;
using SqlClient = Microsoft.Data.SqlClient;
using EfDbContext = Microsoft.EntityFrameworkCore.DbContext;
using EfQueryableExtensions = Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions;
using NormAsyncExtensions = nORM.Core.NormAsyncExtensions;
using static Microsoft.EntityFrameworkCore.EF;

namespace nORM.Benchmarks;

public sealed class ProviderMatrixEfContext : EfDbContext
{
    private readonly string _provider;
    private readonly string _connectionString;

    public ProviderMatrixEfContext(string provider, string connectionString)
    {
        _provider = provider;
        _connectionString = connectionString;
    }

    // Fully qualified: nORM.Core now also defines a DbSet<T>, so a bare DbSet<> here is ambiguous.
    public Microsoft.EntityFrameworkCore.DbSet<BenchmarkUser> Users { get; set; } = null!;
    public Microsoft.EntityFrameworkCore.DbSet<BenchmarkOrder> Orders { get; set; } = null!;

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        _ = _provider switch
        {
            "Sqlite" => optionsBuilder.UseSqlite(_connectionString),
            "SqlServer" => optionsBuilder.UseSqlServer(_connectionString),
            "Postgres" => optionsBuilder.UseNpgsql(_connectionString),
            "MySql" => optionsBuilder.UseMySql(_connectionString, ServerVersion.AutoDetect(_connectionString)),
            _ => throw new InvalidOperationException($"Unsupported benchmark provider '{_provider}'.")
        };
    }

    protected override void OnModelCreating(Microsoft.EntityFrameworkCore.ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BenchmarkUser>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).ValueGeneratedOnAdd();
            if (_provider == "Postgres")
                entity.Property(e => e.CreatedAt).HasColumnType("timestamp without time zone");
            else if (_provider == "MySql")
                entity.Property(e => e.CreatedAt).HasColumnType("datetime(6)");
            entity.ToTable("BenchmarkUser");
            entity.HasIndex(e => e.IsActive);
            entity.HasIndex(e => e.Age);
            entity.HasIndex(e => e.City);
        });

        modelBuilder.Entity<BenchmarkOrder>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).ValueGeneratedOnAdd();
            if (_provider == "Postgres")
                entity.Property(e => e.OrderDate).HasColumnType("timestamp without time zone");
            else if (_provider == "MySql")
                entity.Property(e => e.OrderDate).HasColumnType("datetime(6)");
            entity.ToTable("BenchmarkOrder");
            entity.HasIndex(e => e.UserId);
        });
    }
}

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
[SimpleJob(RuntimeMoniker.Net80, launchCount: 3, warmupCount: 3, iterationCount: 20)]
public class ProviderMatrixBenchmarks
{
    private static readonly string[] AllProviders = ["Sqlite", "SqlServer", "Postgres", "MySql"];
    private const int UserCount = 12_000;
    private const int OrderCount = 2000;
    private const int ParallelThroughputWorkers = 8;
    private readonly List<BenchmarkUser> _testUsers = new();
    private readonly List<BenchmarkOrder> _testOrders = new();

    public static IReadOnlyList<string> SelectedProviders { get; set; } = AllProviders;

    public static IEnumerable<string> Providers() => SelectedProviders;

    [ParamsSource(nameof(Providers))]
    public string Provider { get; set; } = "Sqlite";

    private string _connectionString = string.Empty;
    private ProviderMatrixEfContext? _efContext;
    private DbConnection? _normConnection;
    private nORM.Core.DbContext? _normContext;
    private DbConnection? _adoConnection;

    private DbCommand? _adoSimplePrepared;
    private DbParameter? _adoSimpleTakeParam;
    private DbCommand? _adoComplexPrepared;
    private DbParameter? _adoComplexAgeParam;
    private DbParameter? _adoComplexCityParam;
    private DbCommand? _adoJoinPrepared;
    private DbParameter? _adoJoinAmountParam;

    private static readonly Func<ProviderMatrixEfContext, int, IAsyncEnumerable<BenchmarkUser>> s_efSimpleCompiled
        = CompileAsyncQuery((ProviderMatrixEfContext ctx, int take)
            => EfQueryableExtensions.AsNoTracking(ctx.Users).Where(u => u.IsActive).Take(take));

    private static readonly Func<ProviderMatrixEfContext, int, string, IAsyncEnumerable<BenchmarkUser>> s_efComplexCompiled
        = CompileAsyncQuery((ProviderMatrixEfContext ctx, int age, string city)
            => EfQueryableExtensions.AsNoTracking(ctx.Users)
                .Where(u => u.IsActive && u.Age > age && u.City == city)
                .OrderBy(u => u.Name)
                .Skip(5)
                .Take(20));

    private static readonly Func<nORM.Core.DbContext, int, Task<List<BenchmarkUser>>> s_normSimpleCompiled
        = Norm.CompileQuery<nORM.Core.DbContext, int, BenchmarkUser>(
            (c, take) => c.Query<BenchmarkUser>().Where(u => u.IsActive == true).Take(take));

    private static readonly Func<nORM.Core.DbContext, (int, string), Task<List<BenchmarkUser>>> s_normComplexCompiled
        = Norm.CompileQuery<nORM.Core.DbContext, (int, string), BenchmarkUser>(
            (c, p) => c.Query<BenchmarkUser>()
                .Where(u => u.IsActive == true && u.Age > p.Item1 && u.City == p.Item2)
                
                .OrderBy(u => u.Name)
                .Skip(5)
                .Take(20));

    private static readonly Func<nORM.Core.DbContext, int, Task<List<BenchmarkJoinRow>>> s_normJoinCompiled
        = Norm.CompileQuery<nORM.Core.DbContext, int, BenchmarkJoinRow>((ctx, amount) =>
            ctx.Query<BenchmarkUser>()
                .Join(ctx.Query<BenchmarkOrder>(), u => u.Id, o => o.UserId,
                    (u, o) => new BenchmarkJoinRow(u.Name, o.Amount, o.ProductName))
                
                .Where(x => x.Amount > amount)
                .Take(50));

    [GlobalSetup]
    public async Task Setup()
    {
        _connectionString = GetConnectionString(Provider);
        SeedDeterministicData();

        await EnsureProviderDatabaseAsync();
        await ResetSchemaAsync();
        await SeedDatabaseAsync();

        _efContext = new ProviderMatrixEfContext(Provider, _connectionString);
        await _efContext.Database.OpenConnectionAsync();
        await ApplyBenchmarkConnectionSettingsAsync(Provider, _efContext.Database.GetDbConnection());
        _efContext.ChangeTracker.QueryTrackingBehavior = Microsoft.EntityFrameworkCore.QueryTrackingBehavior.NoTracking;

        _normConnection = await OpenBenchmarkConnectionAsync(Provider, _connectionString);
        _normContext = new nORM.Core.DbContext(_normConnection, CreateNormProvider(Provider), new nORM.Configuration.DbContextOptions
        {
            BulkBatchSize = 50,
            DefaultTrackingBehavior = nORM.Core.QueryTrackingBehavior.NoTracking,
            TimeoutConfiguration = { BaseTimeout = TimeSpan.FromSeconds(30) }
        });

        _adoConnection = await OpenBenchmarkConnectionAsync(Provider, _connectionString);
        PrepareCommands();
    }

    private void SeedDeterministicData()
    {
        _testUsers.Clear();
        _testOrders.Clear();

        var random = new Random(42);
        for (var i = 1; i <= UserCount; i++)
        {
            _testUsers.Add(new BenchmarkUser
            {
                Name = $"User {i}",
                Email = $"user{i}@example.com",
                CreatedAt = BenchmarkNow().AddDays(-random.Next(365)),
                IsActive = random.Next(10) > 2,
                Age = random.Next(18, 80),
                City = GetRandomCity(random),
                Department = GetRandomDepartment(random),
                Salary = random.Next(40_000, 100_000)
            });
        }

        for (var i = 1; i <= OrderCount; i++)
        {
            _testOrders.Add(new BenchmarkOrder
            {
                UserId = random.Next(1, UserCount + 1),
                Amount = (decimal)(random.NextDouble() * 1000),
                OrderDate = BenchmarkNow().AddDays(-random.Next(30)),
                ProductName = $"Product {random.Next(1, 100)}"
            });
        }
    }

    private async Task ResetSchemaAsync()
    {
        if (Provider == "Sqlite")
        {
            var builder = new SqliteConnectionStringBuilder(_connectionString);
            if (!string.IsNullOrWhiteSpace(builder.DataSource))
            {
                SqliteConnection.ClearAllPools();
                File.Delete(builder.DataSource);
                File.Delete(builder.DataSource + "-wal");
                File.Delete(builder.DataSource + "-shm");
            }
        }

        await using var connection = await OpenBenchmarkConnectionAsync(Provider, _connectionString);
        foreach (var sql in SplitStatements(GetResetSql()))
            await connection.ExecuteAsync(sql);
    }

    private async Task EnsureProviderDatabaseAsync()
    {
        if (Provider != "MySql")
            return;

        var builder = new MySqlConnectionStringBuilder(_connectionString);
        var database = builder.Database;
        if (string.IsNullOrWhiteSpace(database))
            return;

        builder.Database = "";
        await using var connection = new MySqlConnection(builder.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"CREATE DATABASE IF NOT EXISTS {EscapeMySqlIdentifier(database)}";
        await command.ExecuteNonQueryAsync();
    }

    private static string EscapeMySqlIdentifier(string value)
        => $"`{value.Replace("`", "``", StringComparison.Ordinal)}`";

    private async Task SeedDatabaseAsync()
    {
        await using var connection = await OpenBenchmarkConnectionAsync(Provider, _connectionString);
        await using var transaction = await connection.BeginTransactionAsync();

        var userSql = InsertUserSql();
        var orderSql = $"""
            INSERT INTO {OrderTable()} ({Col("UserId")}, {Col("Amount")}, {Col("OrderDate")}, {Col("ProductName")})
            VALUES (@UserId, @Amount, @OrderDate, @ProductName)
            """;

        await connection.ExecuteAsync(userSql, _testUsers, transaction);
        await connection.ExecuteAsync(orderSql, _testOrders, transaction);
        await transaction.CommitAsync();
    }

    private void PrepareCommands()
    {
        _adoSimplePrepared = CreatePreparedCommand(QuerySimpleSql(prepared: true), out _adoSimpleTakeParam);

        _adoComplexPrepared = CreatePreparedCommand(QueryComplexSql(), out _adoComplexAgeParam, out _adoComplexCityParam);

        _adoJoinPrepared = CreateJoinPreparedCommand(QueryJoinSql(), out _adoJoinAmountParam);
    }

    private DbCommand CreatePreparedCommand(string sql, out DbParameter take)
    {
        var command = _adoConnection!.CreateCommand();
        command.CommandText = sql;
        AddParameter(command, "@IsActive", DbType.Boolean, ActiveValue());
        take = AddParameter(command, "@Take", DbType.Int32, 10);
        command.Prepare();
        return command;
    }

    private DbCommand CreatePreparedCommand(string sql, out DbParameter age, out DbParameter city)
    {
        var command = _adoConnection!.CreateCommand();
        command.CommandText = sql;
        AddParameter(command, "@IsActive", DbType.Boolean, ActiveValue());
        age = AddParameter(command, "@Age", DbType.Int32, 25);
        city = AddParameter(command, "@City", DbType.String, "New York", size: 128);
        command.Prepare();
        return command;
    }

    private DbCommand CreateJoinPreparedCommand(string sql, out DbParameter amount)
    {
        var command = _adoConnection!.CreateCommand();
        command.CommandText = sql;
        amount = AddParameter(command, "@Amount", DbType.Decimal, 100m);
        command.Prepare();
        return command;
    }

    [Benchmark]
    public async Task Insert_Single_EfCore()
    {
        _efContext!.Users.Add(NewUser("EF"));
        await _efContext.SaveChangesAsync();
        _efContext.ChangeTracker.Clear();
    }

    [Benchmark]
    public Task Insert_Single_nORM()
        => _normContext!.InsertAsync(NewUser("nORM"));

    [Benchmark]
    public Task Insert_Single_Dapper()
        => _adoConnection!.ExecuteAsync(InsertUserSql(), NewUser("Dapper"));

    [Benchmark]
    public async Task Insert_Single_RawAdo()
    {
        await using var command = _adoConnection!.CreateCommand();
        command.CommandText = InsertUserSql();
        AddUserParameters(command, NewUser("ADO"));
        await command.ExecuteNonQueryAsync();
    }

    [Benchmark]
    public Task<List<BenchmarkUser>> Query_Simple_EfCore()
        => EfQueryableExtensions.ToListAsync(EfQueryableExtensions.AsNoTracking(_efContext!.Users).Where(u => u.IsActive).Take(10));

    [Benchmark]
    public Task<List<BenchmarkUser>> Query_Simple_nORM()
        => NormAsyncExtensions.ToListAsync(_normContext!.Query<BenchmarkUser>()
            .Where(u => u.IsActive == true)
            
            .Take(10));

    [Benchmark]
    public async Task<List<BenchmarkUser>> Query_Simple_Dapper()
        => (await _adoConnection!.QueryAsync<BenchmarkUser>(QuerySimpleSql(prepared: false),
            new { IsActive = ActiveValue() })).ToList();

    [Benchmark(Description = "Query Simple Raw ADO (Convenience)")]
    public Task<List<BenchmarkUser>> Query_Simple_RawAdo_Convenience()
        => ReadUsersConvenienceAsync(QuerySimpleSql(prepared: false));

    [Benchmark(Description = "Query Simple Raw ADO (Optimized)")]
    public Task<List<BenchmarkUser>> Query_Simple_RawAdo_Optimized()
        => ReadUsersOptimizedAsync(QuerySimpleSql(prepared: false));

    [Benchmark(Description = "Query Simple Raw ADO (Typed NoBox)")]
    public Task<List<BenchmarkUser>> Query_Simple_RawAdo_TypedNoBox()
        => ReadUsersTypedNoBoxAsync(QuerySimpleSql(prepared: false));

    [Benchmark(Description = "Query Simple EF Core (Compiled)")]
    public async Task<List<BenchmarkUser>> Query_Simple_EfCore_Compiled()
    {
        var list = new List<BenchmarkUser>();
        await foreach (var user in s_efSimpleCompiled(_efContext!, 10))
            list.Add(user);
        return list;
    }

    [Benchmark(Description = "Query Simple nORM (Compiled)")]
    public Task<List<BenchmarkUser>> Query_Simple_nORM_Compiled()
        => s_normSimpleCompiled(_normContext!, 10);

    [Benchmark(Description = "Query Simple Raw ADO (Prepared Optimized)")]
    public Task<List<BenchmarkUser>> Query_Simple_RawAdo_PreparedOptimized()
    {
        _adoSimpleTakeParam!.Value = 10;
        return ReadUsersOptimizedAsync(_adoSimplePrepared!);
    }

    [Benchmark(Description = "Query Simple Raw ADO (Prepared Typed NoBox)")]
    public Task<List<BenchmarkUser>> Query_Simple_RawAdo_PreparedTypedNoBox()
    {
        _adoSimpleTakeParam!.Value = 10;
        return ReadUsersTypedNoBoxAsync(_adoSimplePrepared!);
    }

    [Benchmark]
    public Task<List<BenchmarkUser>> Query_Complex_EfCore()
        => EfQueryableExtensions.ToListAsync(EfQueryableExtensions.AsNoTracking(_efContext!.Users)
            .Where(u => u.IsActive && u.Age > 25 && u.City == "New York")
            .OrderBy(u => u.Name)
            .Skip(5)
            .Take(20));

    [Benchmark]
    public Task<List<BenchmarkUser>> Query_Complex_nORM()
        => NormAsyncExtensions.ToListAsync(_normContext!.Query<BenchmarkUser>()
            .Where(u => u.IsActive == true && u.Age > 25 && u.City == "New York")
            
            .OrderBy(u => u.Name)
            .Skip(5)
            .Take(20));

    [Benchmark]
    public async Task<List<BenchmarkUser>> Query_Complex_Dapper()
        => (await _adoConnection!.QueryAsync<BenchmarkUser>(QueryComplexSql(),
            new { IsActive = ActiveValue(), Age = 25, City = "New York" })).ToList();

    [Benchmark(Description = "Query Complex Raw ADO (Convenience)")]
    public Task<List<BenchmarkUser>> Query_Complex_RawAdo_Convenience()
        => ReadUsersConvenienceAsync(QueryComplexSql(), ("@Age", DbType.Int32, 25), ("@City", DbType.String, "New York"));

    [Benchmark(Description = "Query Complex Raw ADO (Optimized)")]
    public Task<List<BenchmarkUser>> Query_Complex_RawAdo_Optimized()
        => ReadUsersOptimizedAsync(QueryComplexSql(), ("@Age", DbType.Int32, 25), ("@City", DbType.String, "New York"));

    [Benchmark(Description = "Query Complex Raw ADO (Typed NoBox)")]
    public Task<List<BenchmarkUser>> Query_Complex_RawAdo_TypedNoBox()
        => ReadUsersTypedNoBoxAsync(QueryComplexSql(), ("@Age", DbType.Int32, 25), ("@City", DbType.String, "New York"));

    [Benchmark(Description = "Query Complex EF Core (Compiled)")]
    public async Task<List<BenchmarkUser>> Query_Complex_EfCore_Compiled()
    {
        var list = new List<BenchmarkUser>();
        await foreach (var user in s_efComplexCompiled(_efContext!, 25, "New York"))
            list.Add(user);
        return list;
    }

    [Benchmark(Description = "Query Complex nORM (Compiled)")]
    public Task<List<BenchmarkUser>> Query_Complex_nORM_Compiled()
        => s_normComplexCompiled(_normContext!, (25, "New York"));

    [Benchmark(Description = "Query Complex Raw ADO (Prepared Optimized)")]
    public Task<List<BenchmarkUser>> Query_Complex_RawAdo_PreparedOptimized()
    {
        _adoComplexAgeParam!.Value = 25;
        _adoComplexCityParam!.Value = "New York";
        return ReadUsersOptimizedAsync(_adoComplexPrepared!);
    }

    [Benchmark(Description = "Query Complex Raw ADO (Prepared Typed NoBox)")]
    public Task<List<BenchmarkUser>> Query_Complex_RawAdo_PreparedTypedNoBox()
    {
        _adoComplexAgeParam!.Value = 25;
        _adoComplexCityParam!.Value = "New York";
        return ReadUsersTypedNoBoxAsync(_adoComplexPrepared!);
    }

    [Benchmark]
    public Task<List<BenchmarkJoinRow>> Query_Join_EfCore()
        => EfQueryableExtensions.ToListAsync(EfQueryableExtensions.AsNoTracking(_efContext!.Users)
            .Join(_efContext.Orders, u => u.Id, o => o.UserId,
                (u, o) => new { u.Name, o.Amount, o.ProductName })
            .Where(x => x.Amount > 100)
            .Select(x => new BenchmarkJoinRow(x.Name, x.Amount, x.ProductName))
            .Take(50));

    [Benchmark]
    public Task<List<BenchmarkJoinRow>> Query_Join_nORM()
        => NormAsyncExtensions.ToListAsync(_normContext!.Query<BenchmarkUser>()
            .Join(_normContext!.Query<BenchmarkOrder>(), u => u.Id, o => o.UserId,
                (u, o) => new BenchmarkJoinRow(u.Name, o.Amount, o.ProductName))
            
            .Where(x => x.Amount > 100)
            .Take(50));

    [Benchmark]
    public async Task<List<BenchmarkJoinRow>> Query_Join_Dapper()
        => (await _adoConnection!.QueryAsync<BenchmarkJoinRow>(QueryJoinSql(), new { Amount = 100 })).ToList();

    [Benchmark(Description = "Query Join Raw ADO (Convenience)")]
    public Task<List<BenchmarkJoinRow>> Query_Join_RawAdo_Convenience()
        => ReadJoinRowsConvenienceAsync(QueryJoinSql(), ("@Amount", DbType.Decimal, 100m));

    [Benchmark(Description = "Query Join Raw ADO (Optimized)")]
    public Task<List<BenchmarkJoinRow>> Query_Join_RawAdo_Optimized()
        => ReadJoinRowsAsync(QueryJoinSql(), ("@Amount", DbType.Decimal, 100m));

    [Benchmark(Description = "Query Join Raw ADO (Prepared Optimized)")]
    public Task<List<BenchmarkJoinRow>> Query_Join_RawAdo_PreparedOptimized()
    {
        _adoJoinAmountParam!.Value = 100m;
        return ReadJoinRowsAsync(_adoJoinPrepared!);
    }

    [Benchmark]
    public Task<List<BenchmarkJoinRow>> Query_Join_nORM_Compiled()
        => s_normJoinCompiled(_normContext!, 100);

    [Benchmark]
    public Task<int> Count_EfCore()
        => EfQueryableExtensions.CountAsync(_efContext!.Users, u => u.IsActive);

    [Benchmark]
    public Task<int> Count_nORM()
        => NormAsyncExtensions.CountAsync(_normContext!.Query<BenchmarkUser>().Where(u => u.IsActive));

    [Benchmark]
    public Task<int> Count_Dapper()
        => _adoConnection!.QuerySingleAsync<int>(
            $"SELECT COUNT(*) FROM {UserTable()} WHERE {Col("IsActive")} = @IsActive",
            new { IsActive = ActiveValue() });

    [Benchmark(Description = "Count Raw ADO (Optimized)")]
    public async Task<int> Count_RawAdo_Optimized()
    {
        await using var command = _adoConnection!.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {UserTable()} WHERE {Col("IsActive")} = @IsActive";
        AddParameter(command, "@IsActive", DbType.Boolean, ActiveValue());
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    [Benchmark(Description = "Query Scale 1K - nORM")]
    public Task<List<BenchmarkUser>> Query_Scale1k_nORM()
        => NormAsyncExtensions.ToListAsync(_normContext!.Query<BenchmarkUser>()
            
            .OrderBy(u => u.Id)
            .Take(1_000));

    [Benchmark(Description = "Query Scale 1K - Dapper")]
    public async Task<List<BenchmarkUser>> Query_Scale1k_Dapper()
        => (await _adoConnection!.QueryAsync<BenchmarkUser>(QueryScaleSql(1_000))).ToList();

    [Benchmark(Description = "Query Scale 1K - Raw ADO Typed NoBox")]
    public Task<List<BenchmarkUser>> Query_Scale1k_RawAdo_TypedNoBox()
        => ReadUsersTypedNoBoxWithoutActiveAsync(QueryScaleSql(1_000));

    [Benchmark(Description = "Query Scale 10K - nORM")]
    public Task<List<BenchmarkUser>> Query_Scale10k_nORM()
        => NormAsyncExtensions.ToListAsync(_normContext!.Query<BenchmarkUser>()
            
            .OrderBy(u => u.Id)
            .Take(10_000));

    [Benchmark(Description = "Query Scale 10K - Dapper")]
    public async Task<List<BenchmarkUser>> Query_Scale10k_Dapper()
        => (await _adoConnection!.QueryAsync<BenchmarkUser>(QueryScaleSql(10_000))).ToList();

    [Benchmark(Description = "Query Scale 10K - Raw ADO Typed NoBox")]
    public Task<List<BenchmarkUser>> Query_Scale10k_RawAdo_TypedNoBox()
        => ReadUsersTypedNoBoxWithoutActiveAsync(QueryScaleSql(10_000));

    [Benchmark(Description = "Parallel Throughput - nORM")]
    public async Task<int> Query_ParallelThroughput_nORM()
    {
        var tasks = Enumerable.Range(0, ParallelThroughputWorkers).Select(async _ =>
        {
            await using var connection = await OpenBenchmarkConnectionAsync(Provider, _connectionString);
            using var context = new nORM.Core.DbContext(connection, CreateNormProvider(Provider), new nORM.Configuration.DbContextOptions
            {
                BulkBatchSize = 50,
            DefaultTrackingBehavior = nORM.Core.QueryTrackingBehavior.NoTracking,
                TimeoutConfiguration = { BaseTimeout = TimeSpan.FromSeconds(30) }
            });
            var rows = await NormAsyncExtensions.ToListAsync(context.Query<BenchmarkUser>()
                
                .Where(u => u.IsActive == true)
                .OrderBy(u => u.Id)
                .Take(100));
            return rows.Count;
        });

        return (await Task.WhenAll(tasks)).Sum();
    }

    [Benchmark(Description = "Parallel Throughput - Dapper")]
    public async Task<int> Query_ParallelThroughput_Dapper()
    {
        var sql = QuerySimpleOrderedSql(100);
        var tasks = Enumerable.Range(0, ParallelThroughputWorkers).Select(async _ =>
        {
            await using var connection = await OpenBenchmarkConnectionAsync(Provider, _connectionString);
            var rows = await connection.QueryAsync<BenchmarkUser>(sql, new { IsActive = ActiveValue() });
            return rows.Count();
        });

        return (await Task.WhenAll(tasks)).Sum();
    }

    [Benchmark(Description = "Parallel Throughput - Raw ADO Typed NoBox")]
    public async Task<int> Query_ParallelThroughput_RawAdo_TypedNoBox()
    {
        var sql = QuerySimpleOrderedSql(100);
        var tasks = Enumerable.Range(0, ParallelThroughputWorkers).Select(async _ =>
        {
            await using var connection = await OpenBenchmarkConnectionAsync(Provider, _connectionString);
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            AddParameter(command, "@IsActive", DbType.Boolean, ActiveValue());
            var rows = await ReadUsersTypedNoBoxAsync(command);
            return rows.Count;
        });

        return (await Task.WhenAll(tasks)).Sum();
    }

    [Benchmark(Description = "BulkInsert Naive - EF per row")]
    public async Task BulkInsert_Naive_EfCore()
    {
        for (var i = 1; i <= 100; i++)
        {
            _efContext!.Users.Add(NewBulkUser("Naive EF", i));
            await _efContext.SaveChangesAsync();
        }
        _efContext!.ChangeTracker.Clear();
    }

    [Benchmark(Description = "BulkInsert Naive - nORM per row")]
    public async Task BulkInsert_Naive_nORM()
    {
        for (var i = 1; i <= 100; i++)
            await _normContext!.InsertAsync(NewBulkUser("Naive nORM", i));
    }

    [Benchmark(Description = "BulkInsert Naive - Dapper per row")]
    public async Task BulkInsert_Naive_Dapper()
    {
        for (var i = 1; i <= 100; i++)
            await _adoConnection!.ExecuteAsync(InsertUserSql(), NewBulkUser("Naive Dapper", i));
    }

    [Benchmark(Description = "BulkInsert Batched - EF SaveChanges in Tx")]
    public async Task BulkInsert_Batched_EfCore()
    {
        var users = Enumerable.Range(1, 100).Select(i => NewBulkUser("Batch EF", i)).ToList();
        await using var tx = await _efContext!.Database.BeginTransactionAsync();
        _efContext.Users.AddRange(users);
        await _efContext.SaveChangesAsync();
        await tx.CommitAsync();
        _efContext.ChangeTracker.Clear();
    }

    [Benchmark(Description = "BulkInsert Batched - nORM Tx + per row")]
    public async Task BulkInsert_Batched_nORM()
    {
        await using var tx = await _normContext!.Database.BeginTransactionAsync();
        for (var i = 1; i <= 100; i++)
            await _normContext.InsertAsync(NewBulkUser("Batch nORM", i));
        await tx.CommitAsync();
    }

    [Benchmark(Description = "BulkInsert Batched - Dapper prepared")]
    public async Task BulkInsert_Batched_Dapper()
    {
        await using var tx = await _adoConnection!.BeginTransactionAsync();
        await using var command = _adoConnection.CreateCommand();
        command.CommandText = InsertUserSql();
        command.Transaction = tx;

        var pName = AddParameter(command, "@Name", DbType.String, "", 256);
        var pEmail = AddParameter(command, "@Email", DbType.String, "", 256);
        var pCreated = AddParameter(command, "@CreatedAt", DbType.DateTime2, BenchmarkNow());
        var pActive = AddParameter(command, "@IsActive", DbType.Boolean, true);
        var pAge = AddParameter(command, "@Age", DbType.Int32, 30);
        var pCity = AddParameter(command, "@City", DbType.String, "", 128);
        var pDepartment = AddParameter(command, "@Department", DbType.String, "", 128);
        var pSalary = AddParameter(command, "@Salary", DbType.Double, 60_000d);
        command.Prepare();

        for (var i = 1; i <= 100; i++)
        {
            pName.Value = $"Batch Dapper {i}";
            pEmail.Value = $"batch{i}@dapper.com";
            pCreated.Value = BenchmarkNow();
            pActive.Value = true;
            pAge.Value = 30;
            pCity.Value = "BulkCity";
            pDepartment.Value = "BulkDept";
            pSalary.Value = 60_000d;
            await command.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();
    }

    [Benchmark(Description = "BulkInsert Batched - nORM Prepared")]
    public async Task BulkInsert_Batched_nORM_Prepared()
    {
        await using var tx = await _normContext!.Database.BeginTransactionAsync();
        await using var prepared = await _normContext.PrepareInsertAsync<BenchmarkUser>(hydrateGeneratedKeys: false);

        for (var i = 1; i <= 100; i++)
            await prepared.ExecuteAsync(NewBulkUser("Prepared nORM", i));

        await tx.CommitAsync();
    }

    [Benchmark(Description = "BulkInsert Idiomatic - EF AddRange")]
    public async Task BulkInsert_Idiomatic_EfCore()
    {
        _efContext!.Users.AddRange(Enumerable.Range(1, 100).Select(i => NewBulkUser("Bulk EF", i)));
        await _efContext.SaveChangesAsync();
        _efContext.ChangeTracker.Clear();
    }

    [Benchmark(Description = "BulkInsert Idiomatic - nORM BulkInsert")]
    public Task BulkInsert_Idiomatic_nORM()
        => _normContext!.BulkInsertAsync(Enumerable.Range(1, 100).Select(i => NewBulkUser("Bulk nORM", i)).ToList());

    [Benchmark(Description = "BulkInsert Idiomatic - Dapper list in Tx")]
    public async Task BulkInsert_Idiomatic_Dapper()
    {
        var users = Enumerable.Range(1, 100).Select(i => NewBulkUser("Bulk Dapper", i)).ToList();
        await using var tx = await _adoConnection!.BeginTransactionAsync();
        await _adoConnection.ExecuteAsync(InsertUserSql(), users, tx);
        await tx.CommitAsync();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _efContext?.Dispose();
        _normContext?.Dispose();
        _normConnection?.Dispose();
        _adoConnection?.Dispose();

        if (Provider == "Sqlite")
        {
            SqliteConnection.ClearAllPools();
            var builder = new SqliteConnectionStringBuilder(_connectionString);
            File.Delete(builder.DataSource);
            File.Delete(builder.DataSource + "-wal");
            File.Delete(builder.DataSource + "-shm");
        }
    }

    private string QuerySimpleSql(bool prepared)
        => Provider == "SqlServer"
            ? $"SELECT TOP({(prepared ? "@Take" : "10")}) * FROM {UserTable()} WHERE {Col("IsActive")} = @IsActive"
            : $"SELECT * FROM {UserTable()} WHERE {Col("IsActive")} = @IsActive LIMIT {(prepared ? "@Take" : "10")}";

    private string QueryComplexSql()
        => Provider == "SqlServer"
            ? $"""
              SELECT * FROM {UserTable()}
              WHERE {Col("IsActive")} = @IsActive AND {Col("Age")} > @Age AND {Col("City")} = @City
              ORDER BY {Col("Name")}
              OFFSET 5 ROWS FETCH NEXT 20 ROWS ONLY
              """
            : $"""
              SELECT * FROM {UserTable()}
              WHERE {Col("IsActive")} = @IsActive AND {Col("Age")} > @Age AND {Col("City")} = @City
              ORDER BY {Col("Name")}
              LIMIT 20 OFFSET 5
              """;

    private string QueryJoinSql()
        => Provider == "SqlServer"
            ? $"""
              SELECT TOP(50) u.{Col("Name")}, o.{Col("Amount")}, o.{Col("ProductName")}
              FROM {UserTable()} u
              INNER JOIN {OrderTable()} o ON u.{Col("Id")} = o.{Col("UserId")}
              WHERE o.{Col("Amount")} > @Amount
              """
            : $"""
              SELECT u.{Col("Name")}, o.{Col("Amount")}, o.{Col("ProductName")}
              FROM {UserTable()} u
              INNER JOIN {OrderTable()} o ON u.{Col("Id")} = o.{Col("UserId")}
              WHERE o.{Col("Amount")} > @Amount
              LIMIT 50
              """;

    private string QueryScaleSql(int take)
        => Provider == "SqlServer"
            ? $"SELECT TOP({take}) * FROM {UserTable()} ORDER BY {Col("Id")}"
            : $"SELECT * FROM {UserTable()} ORDER BY {Col("Id")} LIMIT {take}";

    private string QuerySimpleOrderedSql(int take)
        => Provider == "SqlServer"
            ? $"SELECT TOP({take}) * FROM {UserTable()} WHERE {Col("IsActive")} = @IsActive ORDER BY {Col("Id")}"
            : $"SELECT * FROM {UserTable()} WHERE {Col("IsActive")} = @IsActive ORDER BY {Col("Id")} LIMIT {take}";

    private string InsertUserSql() => $"""
        INSERT INTO {UserTable()} ({Col("Name")}, {Col("Email")}, {Col("CreatedAt")}, {Col("IsActive")}, {Col("Age")}, {Col("City")}, {Col("Department")}, {Col("Salary")})
        VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)
        """;

    private string UserTable() => QuoteIdentifier("BenchmarkUser");

    private string OrderTable() => QuoteIdentifier("BenchmarkOrder");

    private string Col(string name) => QuoteIdentifier(name);

    private string QuoteIdentifier(string name)
        => Provider switch
        {
            "Postgres" => $"\"{name}\"",
            "MySql" => $"`{name}`",
            _ => name
        };

    private async Task<List<BenchmarkUser>> ReadUsersConvenienceAsync(string sql, params (string Name, DbType Type, object Value)[] parameters)
    {
        await using var command = _adoConnection!.CreateCommand();
        command.CommandText = sql;
        AddParameter(command, "@IsActive", DbType.Boolean, ActiveValue());
        foreach (var (name, type, value) in parameters)
            AddParameter(command, name, type, value);
        return await ReadUsersConvenienceAsync(command);
    }

    private async Task<List<BenchmarkUser>> ReadUsersConvenienceAsync(DbCommand command)
    {
        EnsureActiveParameter(command);
        var users = new List<BenchmarkUser>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            users.Add(ReadUserConvenience(reader));
        return users;
    }

    private async Task<List<BenchmarkUser>> ReadUsersOptimizedAsync(string sql, params (string Name, DbType Type, object Value)[] parameters)
    {
        await using var command = _adoConnection!.CreateCommand();
        command.CommandText = sql;
        AddParameter(command, "@IsActive", DbType.Boolean, ActiveValue());
        foreach (var (name, type, value) in parameters)
            AddParameter(command, name, type, value);
        return await ReadUsersOptimizedAsync(command);
    }

    private async Task<List<BenchmarkUser>> ReadUsersOptimizedAsync(DbCommand command)
    {
        EnsureActiveParameter(command);
        var users = new List<BenchmarkUser>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            users.Add(ReadUserOptimized(reader));
        return users;
    }

    private async Task<List<BenchmarkUser>> ReadUsersTypedNoBoxAsync(string sql, params (string Name, DbType Type, object Value)[] parameters)
    {
        await using var command = _adoConnection!.CreateCommand();
        command.CommandText = sql;
        AddParameter(command, "@IsActive", DbType.Boolean, ActiveValue());
        foreach (var (name, type, value) in parameters)
            AddParameter(command, name, type, value);
        return await ReadUsersTypedNoBoxAsync(command);
    }

    private async Task<List<BenchmarkUser>> ReadUsersTypedNoBoxAsync(DbCommand command)
    {
        EnsureActiveParameter(command);
        var users = new List<BenchmarkUser>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            users.Add(ReadUserTypedNoBox(reader));
        return users;
    }

    private async Task<List<BenchmarkUser>> ReadUsersTypedNoBoxWithoutActiveAsync(string sql)
    {
        await using var command = _adoConnection!.CreateCommand();
        command.CommandText = sql;
        var users = new List<BenchmarkUser>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            users.Add(ReadUserTypedNoBox(reader));
        return users;
    }

    private async Task<List<BenchmarkJoinRow>> ReadJoinRowsAsync(string sql, params (string Name, DbType Type, object Value)[] parameters)
    {
        await using var command = _adoConnection!.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, type, value) in parameters)
            AddParameter(command, name, type, value);

        return await ReadJoinRowsAsync(command);
    }

    private async Task<List<BenchmarkJoinRow>> ReadJoinRowsAsync(DbCommand command)
    {
        var rows = new List<BenchmarkJoinRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new BenchmarkJoinRow(
                reader.GetString(0),
                reader.GetDecimal(1),
                reader.GetString(2)));
        }

        return rows;
    }

    private async Task<List<BenchmarkJoinRow>> ReadJoinRowsConvenienceAsync(string sql, params (string Name, DbType Type, object Value)[] parameters)
    {
        await using var command = _adoConnection!.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, type, value) in parameters)
            AddParameter(command, name, type, value);

        var rows = new List<BenchmarkJoinRow>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new BenchmarkJoinRow(
                Convert.ToString(reader["Name"], CultureInfo.InvariantCulture)!,
                Convert.ToDecimal(reader["Amount"], CultureInfo.InvariantCulture),
                Convert.ToString(reader["ProductName"], CultureInfo.InvariantCulture)!));
        }

        return rows;
    }

    private void EnsureActiveParameter(DbCommand command)
    {
        if (!command.Parameters.Contains("@IsActive"))
            AddParameter(command, "@IsActive", DbType.Boolean, ActiveValue());
        else
            command.Parameters["@IsActive"]!.Value = ActiveValue();
    }

    private static BenchmarkUser ReadUserConvenience(DbDataReader reader) => new()
    {
        Id = Convert.ToInt32(reader["Id"]),
        Name = Convert.ToString(reader["Name"])!,
        Email = Convert.ToString(reader["Email"])!,
        CreatedAt = Convert.ToDateTime(reader["CreatedAt"]),
        IsActive = Convert.ToBoolean(reader["IsActive"]),
        Age = Convert.ToInt32(reader["Age"]),
        City = Convert.ToString(reader["City"])!,
        Department = Convert.ToString(reader["Department"])!,
        Salary = Convert.ToDouble(reader["Salary"])
    };

    private static BenchmarkUser ReadUserOptimized(DbDataReader reader) => new()
    {
        Id = reader.GetInt32(0),
        Name = reader.GetString(1),
        Email = reader.GetString(2),
        CreatedAt = ReadDateTime(reader, 3),
        IsActive = ReadBoolean(reader, 4),
        Age = reader.GetInt32(5),
        City = reader.GetString(6),
        Department = reader.GetString(7),
        Salary = reader.GetDouble(8)
    };

    private static BenchmarkUser ReadUserTypedNoBox(DbDataReader reader) => new()
    {
        Id = reader.GetInt32(0),
        Name = reader.GetString(1),
        Email = reader.GetString(2),
        CreatedAt = reader.GetDateTime(3),
        IsActive = reader.GetBoolean(4),
        Age = reader.GetInt32(5),
        City = reader.GetString(6),
        Department = reader.GetString(7),
        Salary = reader.GetDouble(8)
    };

    private static DateTime ReadDateTime(DbDataReader reader, int ordinal)
    {
        var value = reader.GetValue(ordinal);
        return value is DateTime dateTime
            ? dateTime
            : Convert.ToDateTime(value, CultureInfo.InvariantCulture);
    }

    private static bool ReadBoolean(DbDataReader reader, int ordinal)
    {
        var value = reader.GetValue(ordinal);
        return value is bool boolean
            ? boolean
            : Convert.ToBoolean(value, CultureInfo.InvariantCulture);
    }

    private void AddUserParameters(DbCommand command, BenchmarkUser user)
    {
        AddParameter(command, "@Name", DbType.String, user.Name, 256);
        AddParameter(command, "@Email", DbType.String, user.Email, 256);
        AddParameter(command, "@CreatedAt", DbType.DateTime2, user.CreatedAt);
        AddParameter(command, "@IsActive", DbType.Boolean, user.IsActive);
        AddParameter(command, "@Age", DbType.Int32, user.Age);
        AddParameter(command, "@City", DbType.String, user.City, 128);
        AddParameter(command, "@Department", DbType.String, user.Department, 128);
        AddParameter(command, "@Salary", DbType.Double, user.Salary);
    }

    private static DbParameter AddParameter(DbCommand command, string name, DbType type, object value, int? size = null)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value;
        if (size.HasValue)
            parameter.Size = size.Value;
        if (command is SqlClient.SqlCommand && type == DbType.Decimal && parameter is SqlClient.SqlParameter sqlParameter)
        {
            sqlParameter.Precision = 18;
            sqlParameter.Scale = 4;
        }
        else if (command is SqlClient.SqlCommand && parameter.Size == 0)
            parameter.Size = 1;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private static BenchmarkUser NewUser(string framework) => new()
    {
        Name = $"Test User {framework}",
        Email = $"test-{framework.ToLowerInvariant()}@example.com",
        CreatedAt = BenchmarkNow(),
        IsActive = true,
        Age = 25,
        City = "TestCity",
        Department = "TestDept",
        Salary = 50_000
    };

    private static BenchmarkUser NewBulkUser(string prefix, int i) => new()
    {
        Name = $"{prefix} {i}",
        Email = $"{prefix.Replace(" ", "-", StringComparison.Ordinal).ToLowerInvariant()}{i}@example.com",
        CreatedAt = BenchmarkNow(),
        IsActive = true,
        Age = 30,
        City = "BulkCity",
        Department = "BulkDept",
        Salary = 60_000
    };

    private object ActiveValue() => Provider == "Sqlite" ? 1 : true;

    private static DateTime BenchmarkNow()
        => DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    private string GetResetSql()
        => Provider switch
        {
            "Sqlite" => """
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA foreign_keys = ON;
                DROP TABLE IF EXISTS BenchmarkOrder;
                DROP TABLE IF EXISTS BenchmarkUser;
                CREATE TABLE BenchmarkUser (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL,
                    Email TEXT NOT NULL,
                    CreatedAt TEXT NOT NULL,
                    IsActive INTEGER NOT NULL,
                    Age INTEGER NOT NULL,
                    City TEXT NOT NULL,
                    Department TEXT NOT NULL,
                    Salary REAL NOT NULL
                );
                CREATE INDEX IX_BenchmarkUser_IsActive ON BenchmarkUser (IsActive);
                CREATE INDEX IX_BenchmarkUser_Age ON BenchmarkUser (Age);
                CREATE INDEX IX_BenchmarkUser_City ON BenchmarkUser (City);
                CREATE TABLE BenchmarkOrder (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    UserId INTEGER NOT NULL,
                    Amount NUMERIC NOT NULL,
                    OrderDate TEXT NOT NULL,
                    ProductName TEXT NOT NULL
                );
                CREATE INDEX IX_BenchmarkOrder_UserId ON BenchmarkOrder (UserId);
                """,
            "SqlServer" => """
                IF OBJECT_ID('BenchmarkOrder', 'U') IS NOT NULL DROP TABLE BenchmarkOrder;
                IF OBJECT_ID('BenchmarkUser', 'U') IS NOT NULL DROP TABLE BenchmarkUser;
                CREATE TABLE BenchmarkUser (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    Name NVARCHAR(256) NOT NULL,
                    Email NVARCHAR(256) NOT NULL,
                    CreatedAt DATETIME2 NOT NULL,
                    IsActive BIT NOT NULL,
                    Age INT NOT NULL,
                    City NVARCHAR(128) NOT NULL,
                    Department NVARCHAR(128) NOT NULL,
                    Salary FLOAT NOT NULL
                );
                CREATE INDEX IX_BenchmarkUser_IsActive ON BenchmarkUser (IsActive);
                CREATE INDEX IX_BenchmarkUser_Age ON BenchmarkUser (Age);
                CREATE INDEX IX_BenchmarkUser_City ON BenchmarkUser (City);
                CREATE TABLE BenchmarkOrder (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    UserId INT NOT NULL,
                    Amount DECIMAL(18,2) NOT NULL,
                    OrderDate DATETIME2 NOT NULL,
                    ProductName NVARCHAR(256) NOT NULL
                );
                CREATE INDEX IX_BenchmarkOrder_UserId ON BenchmarkOrder (UserId);
                """,
            "Postgres" => """
                DROP TABLE IF EXISTS "BenchmarkOrder";
                DROP TABLE IF EXISTS "BenchmarkUser";
                CREATE TABLE "BenchmarkUser" (
                    "Id" INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    "Name" TEXT NOT NULL,
                    "Email" TEXT NOT NULL,
                    "CreatedAt" TIMESTAMP NOT NULL,
                    "IsActive" BOOLEAN NOT NULL,
                    "Age" INTEGER NOT NULL,
                    "City" TEXT NOT NULL,
                    "Department" TEXT NOT NULL,
                    "Salary" DOUBLE PRECISION NOT NULL
                );
                CREATE INDEX "IX_BenchmarkUser_IsActive" ON "BenchmarkUser" ("IsActive");
                CREATE INDEX "IX_BenchmarkUser_Age" ON "BenchmarkUser" ("Age");
                CREATE INDEX "IX_BenchmarkUser_City" ON "BenchmarkUser" ("City");
                CREATE TABLE "BenchmarkOrder" (
                    "Id" INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    "UserId" INTEGER NOT NULL,
                    "Amount" NUMERIC NOT NULL,
                    "OrderDate" TIMESTAMP NOT NULL,
                    "ProductName" TEXT NOT NULL
                );
                CREATE INDEX "IX_BenchmarkOrder_UserId" ON "BenchmarkOrder" ("UserId");
                """,
            "MySql" => """
                DROP TABLE IF EXISTS `BenchmarkOrder`;
                DROP TABLE IF EXISTS `BenchmarkUser`;
                CREATE TABLE `BenchmarkUser` (
                    `Id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `Name` VARCHAR(256) NOT NULL,
                    `Email` VARCHAR(256) NOT NULL,
                    `CreatedAt` DATETIME(6) NOT NULL,
                    `IsActive` TINYINT(1) NOT NULL,
                    `Age` INT NOT NULL,
                    `City` VARCHAR(128) NOT NULL,
                    `Department` VARCHAR(128) NOT NULL,
                    `Salary` DOUBLE NOT NULL
                );
                CREATE INDEX `IX_BenchmarkUser_IsActive` ON `BenchmarkUser` (`IsActive`);
                CREATE INDEX `IX_BenchmarkUser_Age` ON `BenchmarkUser` (`Age`);
                CREATE INDEX `IX_BenchmarkUser_City` ON `BenchmarkUser` (`City`);
                CREATE TABLE `BenchmarkOrder` (
                    `Id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `UserId` INT NOT NULL,
                    `Amount` DECIMAL(18,2) NOT NULL,
                    `OrderDate` DATETIME(6) NOT NULL,
                    `ProductName` VARCHAR(256) NOT NULL
                );
                CREATE INDEX `IX_BenchmarkOrder_UserId` ON `BenchmarkOrder` (`UserId`);
                """,
            _ => throw new InvalidOperationException($"Unsupported benchmark provider '{Provider}'.")
        };

    private static IEnumerable<string> SplitStatements(string sql)
        => sql.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private static DbConnection CreateConnection(string provider, string connectionString)
        => provider switch
        {
            "Sqlite" => new SqliteConnection(connectionString),
            "SqlServer" => new SqlClient.SqlConnection(connectionString),
            "Postgres" => new NpgsqlConnection(connectionString),
            "MySql" => new MySqlConnection(connectionString),
            _ => throw new InvalidOperationException($"Unsupported benchmark provider '{provider}'.")
        };

    private static async Task<DbConnection> OpenBenchmarkConnectionAsync(string provider, string connectionString)
    {
        var connection = CreateConnection(provider, connectionString);
        await connection.OpenAsync();
        await ApplyBenchmarkConnectionSettingsAsync(provider, connection);
        return connection;
    }

    private static async Task ApplyBenchmarkConnectionSettingsAsync(string provider, DbConnection connection)
    {
        if (provider != "Sqlite")
            return;

        foreach (var sql in SplitStatements("""
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 5000;
            """))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
    }

    private static DatabaseProvider CreateNormProvider(string provider)
        => provider switch
        {
            "Sqlite" => new SqliteProvider(),
            "SqlServer" => new SqlServerProvider(),
            "Postgres" => new PostgresProvider(),
            "MySql" => new MySqlProvider(),
            _ => throw new InvalidOperationException($"Unsupported benchmark provider '{provider}'.")
        };

    private static string GetConnectionString(string provider)
        => provider switch
        {
            "Sqlite" => "Data Source=provider_matrix_benchmark.db",
            "SqlServer" => GetLiveConnectionString("NORM_TEST_SQLSERVER"),
            "Postgres" => GetLiveConnectionString("NORM_TEST_POSTGRES"),
            "MySql" => GetLiveConnectionString("NORM_TEST_MYSQL"),
            _ => throw new InvalidOperationException($"Unsupported benchmark provider '{provider}'.")
        };

    private static string GetLiveConnectionString(string canonicalName)
    {
        var value = Environment.GetEnvironmentVariable(canonicalName);
        var alias = Environment.GetEnvironmentVariable(canonicalName + "_CS");
        if (!string.IsNullOrWhiteSpace(value) && !IsEnableFlag(value))
            return value;
        if (!string.IsNullOrWhiteSpace(alias))
            return alias;
        throw new InvalidOperationException(
            $"Provider matrix benchmark requires {canonicalName} or {canonicalName}_CS for this provider.");
    }

    private static bool IsEnableFlag(string value)
        => value.Equals("1", StringComparison.OrdinalIgnoreCase) ||
           value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
           value.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
           value.Equals("enabled", StringComparison.OrdinalIgnoreCase);

    private static string GetRandomCity(Random random)
    {
        var cities = new[] { "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Madrid" };
        return cities[random.Next(cities.Length)];
    }

    private static string GetRandomDepartment(Random random)
    {
        var departments = new[] { "Sales", "Engineering", "HR", "Marketing" };
        return departments[random.Next(departments.Length)];
    }

    private sealed class BenchmarkNpgsqlParameterFactory : IDbParameterFactory
    {
        public DbParameter CreateParameter(string name, object? value)
            => new NpgsqlParameter(name, value ?? DBNull.Value);
    }

    private sealed class BenchmarkMySqlParameterFactory : IDbParameterFactory
    {
        public DbParameter CreateParameter(string name, object? value)
            => new MySqlParameter(name, value ?? DBNull.Value);
    }
}
