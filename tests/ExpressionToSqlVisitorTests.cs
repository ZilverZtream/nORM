using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Xunit;
using nORM.Query;
using nORM.Core;

namespace nORM.Tests
{
    public class ExpressionToSqlVisitorTests : TestBase
    {
        private class Product
        {
            public int Id { get; set; }
            public string? Name { get; set; }
            public decimal Price { get; set; }
            public bool IsAvailable { get; set; }
            public DateTime CreatedAt { get; set; }
        }

        public class NumericTypesEntity
        {
            public int IntValue { get; set; }
            public long LongValue { get; set; }
            public decimal DecimalValue { get; set; }
            public double DoubleValue { get; set; }
            public float FloatValue { get; set; }
        }

        private class JsonEntity
        {
            public string? ProfileData { get; set; }
        }

        public static IEnumerable<object[]> SimpleEqualityProviders()
        {
            foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
                yield return new object[] { provider };
        }

        public static IEnumerable<object[]> WhereGreaterThanNumericCases()
        {
            foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
            {
                yield return new object[] { provider, (Expression<Func<NumericTypesEntity, bool>>)(e => e.IntValue > 5), "IntValue", 5 };
                yield return new object[] { provider, (Expression<Func<NumericTypesEntity, bool>>)(e => e.LongValue > 5L), "LongValue", 5L };
                yield return new object[] { provider, (Expression<Func<NumericTypesEntity, bool>>)(e => e.DecimalValue > 5m), "DecimalValue", 5m };
                yield return new object[] { provider, (Expression<Func<NumericTypesEntity, bool>>)(e => e.DoubleValue > 5.0), "DoubleValue", 5.0 };
                yield return new object[] { provider, (Expression<Func<NumericTypesEntity, bool>>)(e => e.FloatValue > 5f), "FloatValue", 5f };
            }
        }

        private static class CustomFunctions
        {
            [SqlFunction("SOUNDEX({0})")]
            public static string Soundex(string s) => throw new NotSupportedException();
        }

        [Theory]
        [MemberData(nameof(SimpleEqualityProviders))]
        public void Where_with_simple_equality(ProviderKind providerKind)
        {
            var setup = CreateProvider(providerKind);
            using var connection = setup.Connection;
            var provider = setup.Provider;

            var (sql, parameters) = Translate<Product>(p => p.Id == 5, connection, provider);
            var expected = $"(T0.{provider.Escape("Id")} = @p0)";
            Assert.Equal(expected, sql);
            Assert.Single(parameters);
            Assert.Equal(5, parameters["@p0"]);
        }

        [Theory]
        [MemberData(nameof(WhereGreaterThanNumericCases))]
        public void Where_with_greater_than_numeric_types(ProviderKind providerKind, Expression<Func<NumericTypesEntity, bool>> predicate, string column, object expectedParam)
        {
            var setup = CreateProvider(providerKind);
            using var connection = setup.Connection;
            var provider = setup.Provider;

            var (sql, parameters) = Translate(predicate, connection, provider);
            var expectedSql = $"(T0.{provider.Escape(column)} > @p0)";
            Assert.Equal(expectedSql, sql);
            Assert.Single(parameters);
            Assert.Equal(expectedParam, parameters["@p0"]);
        }

        [Theory]
        [MemberData(nameof(SimpleEqualityProviders))]
        public void Where_with_string_method_translation(ProviderKind providerKind)
        {
            var setup = CreateProvider(providerKind);
            using var connection = setup.Connection;
            var provider = setup.Provider;

            var (sql, parameters) = Translate<Product>(p => p.Name!.ToUpper() == "ABC", connection, provider);
            var expected = $"(UPPER(T0.{provider.Escape("Name")}) = @p0)";
            Assert.Equal(expected, sql);
            Assert.Single(parameters);
            Assert.Equal("ABC", parameters["@p0"]);
        }

        [Theory]
        [MemberData(nameof(SimpleEqualityProviders))]
        public void Where_with_custom_sql_function(ProviderKind providerKind)
        {
            var setup = CreateProvider(providerKind);
            using var connection = setup.Connection;
            var provider = setup.Provider;

            var (sql, parameters) = Translate<Product>(p => CustomFunctions.Soundex(p.Name!) == "ABC", connection, provider);
            var expected = $"(SOUNDEX(T0.{provider.Escape("Name")}) = @p0)";
            Assert.Equal(expected, sql);
            Assert.Single(parameters);
            Assert.Equal("ABC", parameters["@p0"]);
        }

        [Theory]
        [MemberData(nameof(SimpleEqualityProviders))]
        public void Where_with_json_value_access(ProviderKind providerKind)
        {
            var setup = CreateProvider(providerKind);
            using var connection = setup.Connection;
            var provider = setup.Provider;

            var (sql, parameters) = Translate<JsonEntity>(e => Json.Value<string>(e.ProfileData!, "$.address.city") == "New York", connection, provider);
            var columnSql = $"T0.{provider.Escape("ProfileData")}";
            var expected = $"({provider.TranslateJsonPathAccess(columnSql, "$.address.city")} = @p0)";
            Assert.Equal(expected, sql);
            Assert.Single(parameters);
            Assert.Equal("New York", parameters["@p0"]);
        }
    }
}
