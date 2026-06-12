using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using MigrationRunners = nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class MigrationRunnerWithContextDisposeTests
{
    private static SqliteConnection OpenSqlite()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static Assembly EmptyAsm()
        => System.Reflection.Emit.AssemblyBuilder.DefineDynamicAssembly(
            new System.Reflection.AssemblyName("EmptyMig_" + Guid.NewGuid().ToString("N")),
            System.Reflection.Emit.AssemblyBuilderAccess.Run);

    [Fact]
    public async Task Postgres_WithContext_DisposeAsync_DisposesContext()
    {
        await using var cn = OpenSqlite();
        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        var runner = new MigrationRunners.PostgresMigrationRunner(cn, EmptyAsm(), opts);
        await runner.DisposeAsync();
        // Second call should be idempotent (_disposed = true guards it)
        await runner.DisposeAsync();
    }

    [Fact]
    public void Postgres_WithContext_Dispose_DisposesContext()
    {
        using var cn = OpenSqlite();
        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        var runner = new MigrationRunners.PostgresMigrationRunner(cn, EmptyAsm(), opts);
        runner.Dispose(); // _context != null path
        runner.Dispose(); // idempotent (no-op on second call)
    }

    [Fact]
    public async Task SqlServer_WithContext_DisposeAsync_DisposesContext()
    {
        await using var cn = OpenSqlite();
        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        var runner = new MigrationRunners.SqlServerMigrationRunner(cn, EmptyAsm(), opts);
        await runner.DisposeAsync();
        await runner.DisposeAsync(); // idempotent
    }

    [Fact]
    public void SqlServer_WithContext_Dispose_DisposesContext()
    {
        using var cn = OpenSqlite();
        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        var runner = new MigrationRunners.SqlServerMigrationRunner(cn, EmptyAsm(), opts);
        runner.Dispose();
        runner.Dispose(); // idempotent
    }

    [Fact]
    public async Task Postgres_WithContext_GetPendingMigrations_UsesInterceptorPath()
    {
        // Covers ExecuteReaderAsync with _context != null (interceptor routing)
        await using var cn = OpenSqlite();
        // Create Postgres history table in SQLite format
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE \"__NormMigrationsHistory\" (\"Version\" INTEGER PRIMARY KEY, \"Name\" TEXT NOT NULL, \"AppliedOn\" TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        await using var runner = new MigrationRunners.PostgresMigrationRunner(cn, EmptyAsm(), opts);

        // Invoke GetPendingMigrationsInternalAsync via reflection to bypass advisory lock
        var m = typeof(MigrationRunners.PostgresMigrationRunner)
            .GetMethod("GetPendingMigrationsInternalAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var pending = await (Task<List<MigrationRunners.Migration>>)m.Invoke(runner, new object[] { CancellationToken.None })!;
        Assert.Empty(pending);
    }

    [Fact]
    public async Task SqlServer_WithContext_GetPendingInternal_UsesInterceptorPath()
    {
        // Covers ExecuteReaderAsync with _context != null (interceptor routing)
        await using var cn = OpenSqlite();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE [__NormMigrationsHistory] ([Version] INTEGER PRIMARY KEY, [Name] TEXT NOT NULL, [AppliedOn] TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        var opts = new DbContextOptions { CommandInterceptors = { new NoOpInterceptor() } };
        await using var runner = new MigrationRunners.SqlServerMigrationRunner(cn, EmptyAsm(), opts);

        var m = typeof(MigrationRunners.SqlServerMigrationRunner)
            .GetMethod("GetPendingMigrationsInternalAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var pending = await (Task<List<MigrationRunners.Migration>>)m.Invoke(runner, new object[] { CancellationToken.None })!;
        Assert.Empty(pending);
    }
}

// Covers: ExecuteSync<TResult> (IQueryProvider.Execute sync), ConvertScalarResult<short>,
//         <byte>, <float>, <DateTime>, fallback ChangeType path
