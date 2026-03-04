using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    public class SqlInjectionPreventionTests
    {
        [Fact]
        public void IsSafeRawSql_ReturnsFalse_ForDropStatement()
        {
            var unsafeSql = "SELECT * FROM Users; DROP TABLE Users;";
            Assert.False(NormValidator.IsSafeRawSql(unsafeSql));
        }

        [Fact]
        public void IsSafeRawSql_ReturnsTrue_ForSimpleSelect()
        {
            var safeSql = "SELECT 1";
            Assert.True(NormValidator.IsSafeRawSql(safeSql));
        }

        // SEC-1: DML statements must be rejected

        [Theory]
        [InlineData("DELETE FROM Users WHERE Id = 1")]
        [InlineData("UPDATE Users SET Name='x' WHERE Id=1")]
        [InlineData("INSERT INTO Users(Name) VALUES('x')")]
        [InlineData("MERGE Users USING src ON ...")]
        public void IsSafeRawSql_ReturnsFalse_ForDmlStatements(string dmlSql)
        {
            Assert.False(NormValidator.IsSafeRawSql(dmlSql));
        }

        [Fact]
        public void IsSafeRawSql_ReturnsFalse_ForUpdateStatement()
        {
            Assert.False(NormValidator.IsSafeRawSql("UPDATE Users SET Col=1"));
        }

        [Fact]
        public void IsSafeRawSql_ReturnsFalse_ForInsertStatement()
        {
            Assert.False(NormValidator.IsSafeRawSql("INSERT INTO Users VALUES(1,'x')"));
        }

        [Fact]
        public void IsSafeRawSql_ReturnsFalse_ForDeleteStatement()
        {
            Assert.False(NormValidator.IsSafeRawSql("DELETE FROM Users"));
        }

        private class Dummy
        {
            public int Id { get; set; }
        }

        [Fact]
        public async Task QueryUnchangedAsync_Throws_ForUnsafeSql()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            var provider = new SqliteProvider();

            using var ctx = new DbContext(cn, provider);

            var unsafeSql = "SELECT * FROM Users; DROP TABLE Users;";
            var ex = await Assert.ThrowsAsync<NormException>(() => ctx.QueryUnchangedAsync<Dummy>(unsafeSql));
            Assert.IsType<NormUsageException>(ex.InnerException);
        }

        [Fact]
        public async Task QueryUnchangedAsync_AllowsSafeSql()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            var provider = new SqliteProvider();

            using var ctx = new DbContext(cn, provider);

            var safeSql = "SELECT 1 AS Id";
            var results = await ctx.QueryUnchangedAsync<Dummy>(safeSql);
            Assert.Single(results);
            Assert.Equal(1, results[0].Id);
        }
    }
}
