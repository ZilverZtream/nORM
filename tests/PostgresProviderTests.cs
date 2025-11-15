using System;
using System.Reflection;
using Xunit;
using nORM.Providers;
using nORM.Internal;

namespace nORM.Tests
{
    /// <summary>
    /// Tests for PostgresProvider, focusing on JSONPath translation and other PostgreSQL-specific features.
    /// </summary>
    public class PostgresProviderTests
    {
        private readonly PostgresProvider _provider;

        public PostgresProviderTests()
        {
            _provider = new PostgresProvider(new SqliteParameterFactory());
        }

        /// <summary>
        /// Helper method to call the private ParseJsonPath method using reflection.
        /// </summary>
        private static string[] InvokeParseJsonPath(string jsonPath)
        {
            var method = typeof(PostgresProvider).GetMethod("ParseJsonPath",
                BindingFlags.NonPublic | BindingFlags.Static);

            if (method == null)
                throw new InvalidOperationException("ParseJsonPath method not found");

            var result = method.Invoke(null, new object[] { jsonPath });

            if (result is System.Collections.Generic.List<string> list)
                return list.ToArray();

            throw new InvalidOperationException("Unexpected return type from ParseJsonPath");
        }

        [Fact]
        public void ParseJsonPath_SimpleProperty_ReturnsSingleSegment()
        {
            // $.field → ['field']
            var segments = InvokeParseJsonPath("$.field");

            Assert.Single(segments);
            Assert.Equal("field", segments[0]);
        }

        [Fact]
        public void ParseJsonPath_NestedProperties_ReturnsMultipleSegments()
        {
            // $.user.address.city → ['user', 'address', 'city']
            var segments = InvokeParseJsonPath("$.user.address.city");

            Assert.Equal(3, segments.Length);
            Assert.Equal("user", segments[0]);
            Assert.Equal("address", segments[1]);
            Assert.Equal("city", segments[2]);
        }

        [Fact]
        public void ParseJsonPath_SimpleArrayAccessor_ReturnsIndexSegment()
        {
            // $.items[0] → ['items', '0']
            var segments = InvokeParseJsonPath("$.items[0]");

            Assert.Equal(2, segments.Length);
            Assert.Equal("items", segments[0]);
            Assert.Equal("0", segments[1]);
        }

        [Fact]
        public void ParseJsonPath_NestedArrayAccessors_ReturnsAllSegments()
        {
            // $.items[0].tags[1] → ['items', '0', 'tags', '1']
            var segments = InvokeParseJsonPath("$.items[0].tags[1]");

            Assert.Equal(4, segments.Length);
            Assert.Equal("items", segments[0]);
            Assert.Equal("0", segments[1]);
            Assert.Equal("tags", segments[2]);
            Assert.Equal("1", segments[3]);
        }

        [Fact]
        public void ParseJsonPath_ComplexPath_ReturnsAllSegments()
        {
            // $.items[0].tags[1].name → ['items', '0', 'tags', '1', 'name']
            var segments = InvokeParseJsonPath("$.items[0].tags[1].name");

            Assert.Equal(5, segments.Length);
            Assert.Equal("items", segments[0]);
            Assert.Equal("0", segments[1]);
            Assert.Equal("tags", segments[2]);
            Assert.Equal("1", segments[3]);
            Assert.Equal("name", segments[4]);
        }

        [Fact]
        public void ParseJsonPath_PropertyWithUnderscore_ParsesCorrectly()
        {
            // $.user_profile.first_name → ['user_profile', 'first_name']
            var segments = InvokeParseJsonPath("$.user_profile.first_name");

            Assert.Equal(2, segments.Length);
            Assert.Equal("user_profile", segments[0]);
            Assert.Equal("first_name", segments[1]);
        }

        [Fact]
        public void ParseJsonPath_PropertyWithNumbers_ParsesCorrectly()
        {
            // $.field123.item456 → ['field123', 'item456']
            var segments = InvokeParseJsonPath("$.field123.item456");

            Assert.Equal(2, segments.Length);
            Assert.Equal("field123", segments[0]);
            Assert.Equal("item456", segments[1]);
        }

        [Fact]
        public void ParseJsonPath_MultipleDigitArrayIndex_ParsesCorrectly()
        {
            // $.items[123] → ['items', '123']
            var segments = InvokeParseJsonPath("$.items[123]");

            Assert.Equal(2, segments.Length);
            Assert.Equal("items", segments[0]);
            Assert.Equal("123", segments[1]);
        }

        [Fact]
        public void ParseJsonPath_WithoutLeadingDollar_ParsesCorrectly()
        {
            // user.address → ['user', 'address']
            var segments = InvokeParseJsonPath("user.address");

            Assert.Equal(2, segments.Length);
            Assert.Equal("user", segments[0]);
            Assert.Equal("address", segments[1]);
        }

        [Fact]
        public void ParseJsonPath_EmptyString_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() => InvokeParseJsonPath(""));
        }

        [Fact]
        public void ParseJsonPath_NullOrWhitespace_ThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(() => InvokeParseJsonPath("   "));
        }

        [Fact]
        public void ParseJsonPath_UnclosedBracket_ThrowsArgumentException()
        {
            // $.items[0 (missing closing bracket)
            Assert.Throws<ArgumentException>(() => InvokeParseJsonPath("$.items[0"));
        }

        [Fact]
        public void ParseJsonPath_EmptyArrayIndex_ThrowsArgumentException()
        {
            // $.items[] (empty index)
            Assert.Throws<ArgumentException>(() => InvokeParseJsonPath("$.items[]"));
        }

        [Fact]
        public void ParseJsonPath_NonNumericArrayIndex_ThrowsArgumentException()
        {
            // $.items[abc] (non-numeric index)
            Assert.Throws<ArgumentException>(() => InvokeParseJsonPath("$.items[abc]"));
        }

        [Fact]
        public void ParseJsonPath_InvalidCharacter_ThrowsArgumentException()
        {
            // $.user@address (@ is invalid)
            Assert.Throws<ArgumentException>(() => InvokeParseJsonPath("$.user@address"));
        }

        [Fact]
        public void ParseJsonPath_OnlyDollarSign_ThrowsArgumentException()
        {
            // $ (no path)
            Assert.Throws<ArgumentException>(() => InvokeParseJsonPath("$"));
        }

        [Fact]
        public void ParseJsonPath_OnlyDollarAndDot_ThrowsArgumentException()
        {
            // $. (no property)
            Assert.Throws<ArgumentException>(() => InvokeParseJsonPath("$."));
        }

        [Fact]
        public void TranslateJsonPathAccess_SimpleProperty_GeneratesCorrectSql()
        {
            var result = _provider.TranslateJsonPathAccess("\"data\"", "$.field");

            Assert.Equal("jsonb_extract_path_text(\"data\", 'field')", result);
        }

        [Fact]
        public void TranslateJsonPathAccess_NestedProperties_GeneratesCorrectSql()
        {
            var result = _provider.TranslateJsonPathAccess("\"profile\"", "$.user.address.city");

            Assert.Equal("jsonb_extract_path_text(\"profile\", 'user', 'address', 'city')", result);
        }

        [Fact]
        public void TranslateJsonPathAccess_WithArrayAccessor_GeneratesCorrectSql()
        {
            var result = _provider.TranslateJsonPathAccess("\"data\"", "$.items[0]");

            Assert.Equal("jsonb_extract_path_text(\"data\", 'items', '0')", result);
        }

        [Fact]
        public void TranslateJsonPathAccess_ComplexPath_GeneratesCorrectSql()
        {
            var result = _provider.TranslateJsonPathAccess("\"data\"", "$.items[0].tags[1].name");

            Assert.Equal("jsonb_extract_path_text(\"data\", 'items', '0', 'tags', '1', 'name')", result);
        }

        [Fact]
        public void TranslateJsonPathAccess_OldStylePath_StillWorks()
        {
            // Ensure backward compatibility with the old simple dot-notation paths
            var result = _provider.TranslateJsonPathAccess("\"data\"", "$.address.city");

            Assert.Equal("jsonb_extract_path_text(\"data\", 'address', 'city')", result);
        }
    }
}
