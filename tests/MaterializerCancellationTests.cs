using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.SourceGeneration;
using Xunit;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Tests
{
    public class MaterializerCancellationTests
    {
        private class Uncompiled
        {
            public int Id { get; set; }
        }

        [Fact]
        public async Task Compiled_materializer_honors_cancellation()
        {
            Assert.True(CompiledMaterializerStore.TryGet(typeof(Materialized), out var mat));

            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT 1 AS Id";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await mat(reader, cts.Token));
        }

        [Fact]
        public async Task Runtime_materializer_honors_cancellation()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var ctx = new DbContext(cn, new SqliteProvider());
            var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
            var translator = Activator.CreateInstance(translatorType, ctx)!;
            var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
            var mapping = getMapping.Invoke(ctx, new object[] { typeof(Uncompiled) });
            var create = translatorType.GetMethod("CreateMaterializer")!;
            var materializer = (Func<DbDataReader, CancellationToken, Task<object>>)create.Invoke(translator, new object?[] { mapping!, typeof(Uncompiled), null })!;

            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT 1 AS Id";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await materializer(reader, cts.Token));
        }
    }
}
