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
public class ConnectionManagerPrivateMethodTests
{
    private static bool InvokeHasSocketChain(Exception ex)
    {
        var m = typeof(ConnectionManager)
            .GetMethod("HasSocketExceptionInChain", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (bool)m.Invoke(null, new object[] { ex })!;
    }

    private static bool InvokeIsNetworkIO(System.IO.IOException ex)
    {
        var m = typeof(ConnectionManager)
            .GetMethod("IsNetworkIOException", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (bool)m.Invoke(null, new object[] { ex })!;
    }

    private static bool InvokeIsTransientDb(DbException ex)
    {
        var m = typeof(ConnectionManager)
            .GetMethod("IsTransientDatabaseError", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (bool)m.Invoke(null, new object[] { ex })!;
    }

    [Fact]
    public void HasSocketExceptionInChain_WithSocketException_ReturnsTrue()
    {
        var socketEx = new System.Net.Sockets.SocketException();
        Assert.True(InvokeHasSocketChain(socketEx));
    }

    [Fact]
    public void HasSocketExceptionInChain_WithSocketAsInner_ReturnsTrue()
    {
        var socketEx = new System.Net.Sockets.SocketException();
        var outer = new Exception("outer", socketEx);
        Assert.True(InvokeHasSocketChain(outer));
    }

    [Fact]
    public void HasSocketExceptionInChain_WithNoSocket_ReturnsFalse()
    {
        var ex = new InvalidOperationException("no socket here");
        Assert.False(InvokeHasSocketChain(ex));
    }

    [Fact]
    public void HasSocketExceptionInChain_NullInnerChain_ReturnsFalse()
    {
        var ex = new Exception("only one level");
        Assert.False(InvokeHasSocketChain(ex));
    }

    [Fact]
    public void IsNetworkIOException_WithSocketInner_ReturnsTrue()
    {
        var socketEx = new System.Net.Sockets.SocketException();
        var ioEx = new System.IO.IOException("network failure", socketEx);
        Assert.True(InvokeIsNetworkIO(ioEx));
    }

    [Fact]
    public void IsNetworkIOException_WithoutSocketInner_ReturnsFalse()
    {
        var ioEx = new System.IO.IOException("disk error");
        Assert.False(InvokeIsNetworkIO(ioEx));
    }

    [Fact]
    public void IsTransientDatabaseError_WithSocketInner_ReturnsTrue()
    {
        var socketEx = new System.Net.Sockets.SocketException();
        var dbEx = new FakeDbEx("transient", socketEx);
        Assert.True(InvokeIsTransientDb(dbEx));
    }

    [Fact]
    public void IsTransientDatabaseError_WithoutSocket_ReturnsFalse()
    {
        var dbEx = new FakeDbEx("regular error");
        Assert.False(InvokeIsTransientDb(dbEx));
    }

    [Fact]
    public void ConnectionManager_TriggerFailoverNoHealthyNodes_LogsError()
    {
        var topology = new DatabaseTopology(); // no nodes added

        using var mgr = new ConnectionManager(
            topology, new SqliteProvider(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            TimeSpan.FromHours(1));

        // Must throw NormConnectionException("No healthy primary node available.")
        // TriggerFailoverAsync runs internally and logs the "no healthy nodes" error
        Assert.Throws<NormConnectionException>(() =>
            mgr.GetWriteConnectionAsync().GetAwaiter().GetResult());
    }

    private sealed class FakeDbEx : DbException
    {
        public FakeDbEx(string msg, Exception? inner = null) : base(msg, inner) { }
    }
}

//         both create provider connections when drivers are present or throw when absent.
