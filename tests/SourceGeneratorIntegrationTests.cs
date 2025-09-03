using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.SourceGeneration;
using Xunit;

namespace nORM.Tests
{
    public class SourceGeneratorIntegrationTests
    {
        [Fact]
        public void QueryTranslator_uses_precompiled_materializer_when_available()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            using var ctx = new DbContext(cn, new SqliteProvider());
            var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
            var translator = Activator.CreateInstance(translatorType, ctx)!;
            var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
            var mapping = getMapping.Invoke(ctx, new object[] { typeof(Materialized) });
            var create = translatorType.GetMethod("CreateMaterializer")!;
            var materializer = (Func<DbDataReader, object>)create.Invoke(translator, new object?[] { mapping!, typeof(Materialized), null })!;
            Assert.True(CompiledMaterializerStore.TryGet(typeof(Materialized), out var precompiled));
            Assert.Same(precompiled, materializer);
        }
    }
}
