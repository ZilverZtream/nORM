using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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

        private class Order
        {
            public int Id { get; set; }
            public int ProductId { get; set; }
        }

        private static (string Sql, Dictionary<string, object> Params, Type ElementType) TranslateExpression<T>(Func<DbContext, IQueryable<T>, Expression> build) where T : class, new()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            using var ctx = new DbContext(cn, new SqliteProvider());
            var query = ctx.Query<T>();
            var expr = build(ctx, query);
            var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
            var translator = Activator.CreateInstance(translatorType, ctx)!;
            var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
            var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
            var parameters = (IReadOnlyDictionary<string, object>)plan.GetType().GetProperty("Parameters")!.GetValue(plan)!;
            var elementType = (Type)plan.GetType().GetProperty("ElementType")!.GetValue(plan)!;
            return (sql, new Dictionary<string, object>(parameters), elementType);
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

        [Fact]
        public void Skip_applies_offset()
        {
            var (sql, parameters, _) = Translate<Product, Product>(q => q.Skip(5));
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\" OFFSET 5", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Skip_then_Take_for_paging()
        {
            var (sql, parameters, _) = Translate<Product, Product>(q => q.Skip(10).Take(5));
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\" LIMIT 5 OFFSET 10", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Count_without_predicate()
        {
            var (sql, parameters, elementType) = TranslateExpression<Product>((ctx, q) =>
            {
                var method = typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.Count) && m.GetParameters().Length == 1).MakeGenericMethod(typeof(Product));
                return Expression.Call(null, method, q.Expression);
            });
            Assert.Equal("SELECT COUNT(*)", sql);
            Assert.Empty(parameters);
            Assert.Equal(typeof(int), elementType);
        }

        [Fact]
        public void Count_with_predicate()
        {
            var (sql, parameters, elementType) = TranslateExpression<Product>((ctx, q) =>
            {
                var method = typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.Count) && m.GetParameters().Length == 2).MakeGenericMethod(typeof(Product));
                Expression<Func<Product, bool>> pred = p => p.Id > 5;
                return Expression.Call(null, method, q.Expression, pred);
            });
            Assert.Equal("SELECT COUNT(*)", sql);
            Assert.Empty(parameters);
            Assert.Equal(typeof(int), elementType);
        }

        [Fact]
        public void Any_with_predicate()
        {
            var (sql, parameters, _) = TranslateExpression<Product>((ctx, q) =>
            {
                var method = typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.Any) && m.GetParameters().Length == 2).MakeGenericMethod(typeof(Product));
                Expression<Func<Product, bool>> pred = p => p.Id > 5;
                return Expression.Call(null, method, q.Expression, pred);
            });
            Assert.Equal("SELECT 1 WHERE EXISTS(SELECT 1 FROM \"Product\" LIMIT 1)", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void First_with_predicate()
        {
            var (sql, parameters, _) = TranslateExpression<Product>((ctx, q) =>
            {
                var method = typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.First) && m.GetParameters().Length == 2).MakeGenericMethod(typeof(Product));
                Expression<Func<Product, bool>> pred = p => p.Id > 5;
                return Expression.Call(null, method, q.Expression, pred);
            });
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\" LIMIT 1", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void FirstOrDefault_on_empty_query()
        {
            var (sql, parameters, _) = TranslateExpression<Product>((ctx, q) =>
            {
                var method = typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.FirstOrDefault) && m.GetParameters().Length == 1).MakeGenericMethod(typeof(Product));
                return Expression.Call(null, method, q.Expression);
            });
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\" LIMIT 1", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Single_with_predicate()
        {
            var (sql, parameters, _) = TranslateExpression<Product>((ctx, q) =>
            {
                var method = typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.Single) && m.GetParameters().Length == 2).MakeGenericMethod(typeof(Product));
                Expression<Func<Product, bool>> pred = p => p.Id == 1;
                return Expression.Call(null, method, q.Expression, pred);
            });
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\" LIMIT 1", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void SingleOrDefault_on_empty_query()
        {
            var (sql, parameters, _) = TranslateExpression<Product>((ctx, q) =>
            {
                var method = typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.SingleOrDefault) && m.GetParameters().Length == 1).MakeGenericMethod(typeof(Product));
                return Expression.Call(null, method, q.Expression);
            });
            Assert.Equal("SELECT \"Id\", \"Name\" FROM \"Product\" LIMIT 1", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Join_between_two_tables()
        {
            var (sql, parameters, _) = TranslateExpression<Product>((ctx, q) =>
                q.Join(ctx.Query<Order>(), p => p.Id, o => o.ProductId, (p, o) => new { p.Id, o.ProductId }).Expression);
            Assert.Equal("SELECT T0.\"Id\", T0.\"Name\", T1.\"Id\", T1.\"ProductId\" FROM \"Product\" T0 INNER JOIN \"Order\" T1 ON T0.\"Id\" = T1.\"ProductId\"", sql);
            Assert.Empty(parameters);
        }
    }
}
