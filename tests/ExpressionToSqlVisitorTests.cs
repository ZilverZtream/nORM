using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    public class ExpressionToSqlVisitorTests
    {
        private static (string Sql, Dictionary<string, object> Params) Translate<T>(Expression<Func<T, bool>> expr) where T : class, new()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            using var ctx = new DbContext(cn, new SqliteProvider());
            var getMapping = typeof(DbContext).GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var mapping = getMapping.Invoke(ctx, new object[] { typeof(T) });
            var visitorType = typeof(DbContext).Assembly.GetType("nORM.Query.ExpressionToSqlVisitor", true)!;
            var visitor = Activator.CreateInstance(visitorType, ctx, mapping, ctx.Provider, expr.Parameters[0], "T0", null)!;
            var sql = (string)visitorType.GetMethod("Translate")!.Invoke(visitor, new object[] { expr.Body })!;
            var parameters = (Dictionary<string, object>)visitorType.GetMethod("GetParameters")!.Invoke(visitor, null)!;
            return (sql, parameters);
        }

        private class Product
        {
            public int Id { get; set; }
            public string? Name { get; set; }
            public decimal Price { get; set; }
            public bool IsAvailable { get; set; }
        }

        [Fact]
        public void Where_with_simple_equality()
        {
            var (sql, parameters) = Translate<Product>(p => p.Id == 5);
            Assert.Equal("(T0.\"Id\" = @p0)", sql);
            Assert.Single(parameters);
            Assert.Equal(5, parameters["@p0"]);
        }

        [Fact]
        public void Where_with_combined_logical_operators()
        {
            var (sql, parameters) = Translate<Product>(p => p.Id == 5 && (p.Name == "Foo" || p.Name == "Bar"));
            Assert.Equal("((T0.\"Id\" = @p0) AND ((T0.\"Name\" = @p1) OR (T0.\"Name\" = @p2)))", sql);
            Assert.Equal(3, parameters.Count);
            Assert.Equal(5, parameters["@p0"]);
            Assert.Equal("Foo", parameters["@p1"]);
            Assert.Equal("Bar", parameters["@p2"]);
        }

        [Fact]
        public void Where_with_captured_variable_from_closure()
        {
            var threshold = 10;
            var (sql, parameters) = Translate<Product>(p => p.Id == threshold);
            Assert.Equal("(T0.\"Id\" = @p0)", sql);
            Assert.Single(parameters);
            Assert.Equal(threshold, parameters["@p0"]);
        }

        [Fact]
        public void Where_with_string_contains()
        {
            var (sql, parameters) = Translate<Product>(p => p.Name!.Contains("Foo"));
            Assert.Equal("T0.\"Name\" LIKE @p0 ESCAPE '\\'", sql);
            Assert.Single(parameters);
            Assert.Equal("%Foo%", parameters["@p0"]);
        }

        [Fact]
        public void Where_with_startswith_and_endswith()
        {
            var (sql, parameters) = Translate<Product>(p => p.Name!.StartsWith("Foo") && p.Name!.EndsWith("Bar"));
            Assert.Equal("(T0.\"Name\" LIKE @p0 ESCAPE '\\' AND T0.\"Name\" LIKE @p1 ESCAPE '\\')", sql);
            Assert.Equal(2, parameters.Count);
            Assert.Equal("Foo%", parameters["@p0"]);
            Assert.Equal("%Bar", parameters["@p1"]);
        }

        [Fact]
        public void Where_with_greater_and_less_than()
        {
            var (sql, parameters) = Translate<Product>(p => p.Price > 50 && p.Price <= 100);
            Assert.Equal("((T0.\"Price\" > @p0) AND (T0.\"Price\" <= @p1))", sql);
            Assert.Equal(2, parameters.Count);
            Assert.Equal(50m, parameters["@p0"]);
            Assert.Equal(100m, parameters["@p1"]);
        }

        [Fact]
        public void Where_with_not_equal()
        {
            var (sql, parameters) = Translate<Product>(p => p.Name != "Test");
            Assert.Equal("(T0.\"Name\" <> @p0)", sql);
            Assert.Single(parameters);
            Assert.Equal("Test", parameters["@p0"]);
        }

        [Fact]
        public void Where_with_null_check()
        {
            var (sql, parameters) = Translate<Product>(p => p.Name == null);
            Assert.Equal("(T0.\"Name\" = @p0)", sql);
            Assert.Single(parameters);
            Assert.IsType<DBNull>(parameters["@p0"]);
        }

        [Fact]
        public void Where_on_boolean_property_true()
        {
            var (sql, parameters) = Translate<Product>(p => p.IsAvailable);
            Assert.Equal("T0.\"IsAvailable\"", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Where_on_boolean_property_false()
        {
            var (sql, parameters) = Translate<Product>(p => !p.IsAvailable);
            Assert.Equal("(NOT(T0.\"IsAvailable\"))", sql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Where_with_method_call_resolving_to_constant()
        {
            var testName = "TEST";
            var ex = Assert.Throws<System.Reflection.TargetInvocationException>(() => Translate<Product>(p => p.Name == testName.ToLower()));
            Assert.IsType<NotSupportedException>(ex.InnerException);
        }

        [Fact]
        public void Where_with_list_contains_translates_to_in()
        {
            var ids = new List<int> { 1, 2, 3 };
            var (sql, parameters) = Translate<Product>(p => ids.Contains(p.Id));
            Assert.Equal("T0.\"Id\" IN (@p0, @p1, @p2)", sql);
            Assert.Equal(3, parameters.Count);
            Assert.Equal(1, parameters["@p0"]);
            Assert.Equal(2, parameters["@p1"]);
            Assert.Equal(3, parameters["@p2"]);
        }
    }
}

