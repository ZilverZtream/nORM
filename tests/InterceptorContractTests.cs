using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class InterceptorContractTests
{
    [Fact]
    public async Task CommandInterceptors_RunInRegistrationOrder_AndMutationsCascade()
    {
        var events = new List<string>();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new MutatingScalarInterceptor("first", events, "SELECT 2"));
        options.CommandInterceptors.Add(new ObservingScalarInterceptor("second", events));

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        await using var command = cn.CreateCommand();
        command.CommandText = "SELECT 1";

        var result = await command.ExecuteScalarWithInterceptionAsync(ctx, CancellationToken.None);

        Assert.Equal(2L, result);
        Assert.Equal(new[]
        {
            "first:before",
            "second:before:SELECT 2",
            "first:after:2",
            "second:after:2"
        }, events);
    }

    [Fact]
    public async Task SuppressedCommand_SkipsDatabaseAndNotifiesAllExecutedHooks()
    {
        var events = new List<string>();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new SuppressingNonQueryInterceptor("first", events, 42));
        options.CommandInterceptors.Add(new ObservingNonQueryInterceptor("second", events));

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        await using var command = cn.CreateCommand();
        command.CommandText = "THIS IS NOT SQL";

        var result = await command.ExecuteNonQueryWithInterceptionAsync(ctx, CancellationToken.None);

        Assert.Equal(42, result);
        Assert.Equal(new[]
        {
            "first:before",
            "first:after:42",
            "second:after:42"
        }, events);
    }

    [Fact]
    public async Task FailedCommand_NotifiesFailuresInRegistrationOrder_AndRethrowsOriginalException()
    {
        var events = new List<string>();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new FailureObserverInterceptor("first", events));
        options.CommandInterceptors.Add(new FailureObserverInterceptor("second", events));

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        await using var command = cn.CreateCommand();
        command.CommandText = "SELECT * FROM MissingTable";

        await Assert.ThrowsAsync<SqliteException>(() =>
            command.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.Default, CancellationToken.None));

        Assert.Equal(new[] { "first:before", "second:before", "first:failed", "second:failed" }, events);
    }

    private sealed class MutatingScalarInterceptor : IDbCommandInterceptor
    {
        private readonly string _name;
        private readonly List<string> _events;
        private readonly string _replacementSql;

        public MutatingScalarInterceptor(string name, List<string> events, string replacementSql)
        {
            _name = name;
            _events = events;
            _replacementSql = replacementSql;
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<int>.Continue());

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":before");
            command.CommandText = _replacementSql;
            return Task.FromResult(InterceptionResult<object?>.Continue());
        }

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":after:" + result);
            return Task.CompletedTask;
        }

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class ObservingScalarInterceptor : IDbCommandInterceptor
    {
        private readonly string _name;
        private readonly List<string> _events;

        public ObservingScalarInterceptor(string name, List<string> events)
        {
            _name = name;
            _events = events;
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<int>.Continue());

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":before:" + command.CommandText);
            return Task.FromResult(InterceptionResult<object?>.Continue());
        }

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":after:" + result);
            return Task.CompletedTask;
        }

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class SuppressingNonQueryInterceptor : IDbCommandInterceptor
    {
        private readonly string _name;
        private readonly List<string> _events;
        private readonly int _result;

        public SuppressingNonQueryInterceptor(string name, List<string> events, int result)
        {
            _name = name;
            _events = events;
            _result = result;
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":before");
            return Task.FromResult(InterceptionResult<int>.SuppressWithResult(_result));
        }

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":after:" + result);
            return Task.CompletedTask;
        }

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<object?>.Continue());

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class ObservingNonQueryInterceptor : IDbCommandInterceptor
    {
        private readonly string _name;
        private readonly List<string> _events;

        public ObservingNonQueryInterceptor(string name, List<string> events)
        {
            _name = name;
            _events = events;
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":before");
            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":after:" + result);
            return Task.CompletedTask;
        }

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<object?>.Continue());

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class FailureObserverInterceptor : IDbCommandInterceptor
    {
        private readonly string _name;
        private readonly List<string> _events;

        public FailureObserverInterceptor(string name, List<string> events)
        {
            _name = name;
            _events = events;
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<int>.Continue());

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<object?>.Continue());

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":before");
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
        {
            _events.Add(_name + ":failed");
            return Task.CompletedTask;
        }
    }
}
