using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    public class QueryTranslatorTests
    {
        private static (string Sql, Dictionary<string, object> Params, Type ElementType) Translate<T, TResult>(Func<INormQueryable<T>, IQueryable<TResult>> build) where T : class, new()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            using var ctx = new DbContext(cn, new SqliteProvider());
            var query = build(ctx.Query<T>());
            var expr = query.Expression;
            var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
            var translator = Activator.CreateInstance(translatorType, ctx)!;
            var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
            var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
            var parameters = (IReadOnlyDictionary<string, object>)plan.GetType().GetProperty("Parameters")!.GetValue(plan)!;
            var elementType = (Type)plan.GetType().GetProperty("ElementType")!.GetValue(plan)!;
            return (sql, new Dictionary<string, object>(parameters), elementType);
        }

        private class Product
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }

        private class ProductDto
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }

        [Fact]
        public void Select_into_anonymous_type()
        {
            var (sql, parameters, elementType) = Translate<Product, object>(q => q.Select(p => new { p.Id, p.Name }));
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\"", sql);
            Assert.Empty(parameters);
            Assert.StartsWith("<>", elementType.Name);
        }

        [Fact]
        public void Select_into_named_dto_class()
        {
            var (sql, parameters, elementType) = Translate<Product, ProductDto>(q => q.Select(p => new ProductDto { Id = p.Id, Name = p.Name }));
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\"", sql);
            Assert.Empty(parameters);
            Assert.Equal(typeof(ProductDto), elementType);
        }

        [Fact]
        public void OrderBy_followed_by_ThenBy()
        {
            var (sql, parameters, _) = Translate<Product, Product>(q => q.OrderBy(p => p.Name).ThenBy(p => p.Id));
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\" T0 ORDER BY T0.\"Name\" ASC, T0.\"Id\" ASC", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void OrderByDescending_followed_by_ThenByDescending()
        {
            var (sql, parameters, _) = Translate<Product, Product>(q => q.OrderByDescending(p => p.Name).ThenByDescending(p => p.Id));
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\" T0 ORDER BY T0.\"Name\" DESC, T0.\"Id\" DESC", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Take_applies_limit()
        {
            var (sql, parameters, _) = Translate<Product, Product>(q => q.Take(5));
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\" LIMIT 5", sql);
            Assert.Empty(parameters);
        }
    }
}
