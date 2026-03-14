using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

namespace nORM.Tests;

public class SqlComparisonDiagnostic
{
    private readonly ITestOutputHelper _output;

    public SqlComparisonDiagnostic(ITestOutputHelper output)
    {
        _output = output;
    }

    public class BenchUser
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public string Department { get; set; } = string.Empty;
        public double Salary { get; set; }
        public bool IsActive { get; set; }
        public DateTime CreatedAt { get; set; }
        public int Age { get; set; }
        public string City { get; set; } = string.Empty;
    }

    private static void SeedDatabase(SqliteConnection cn)
    {
        using var createCmd = cn.CreateCommand();
        createCmd.CommandText = @"
            CREATE TABLE BenchUser (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL, Email TEXT NOT NULL,
                Department TEXT NOT NULL, Salary REAL NOT NULL,
                IsActive INTEGER NOT NULL, CreatedAt TEXT NOT NULL,
                Age INTEGER NOT NULL, City TEXT NOT NULL);";
        createCmd.ExecuteNonQuery();

        var random = new Random(42);
        var cities = new[] { "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Madrid" };
        using var tx = cn.BeginTransaction();
        using var insertCmd = cn.CreateCommand();
        insertCmd.Transaction = tx;
        for (int i = 1; i <= 1000; i++)
        {
            insertCmd.CommandText = $"INSERT INTO BenchUser VALUES({i},'User {i}','u{i}@e.com','Eng',{random.Next(40000, 100000)},{(random.Next(10) > 2 ? 1 : 0)},'2024-01-01',{random.Next(18, 80)},'{cities[random.Next(cities.Length)]}')";
            insertCmd.ExecuteNonQuery();
        }
        tx.Commit();
    }

    /// <summary>Clean test: nORM compiled vs ADO, no flag contamination.</summary>
    [Fact]
    public async Task Overhead_breakdown()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        SeedDatabase(cn);

        using var ctx = new DbContext(cn, new SqliteProvider());
        const int iterations = 50000;

        // Compile query
        var compiled = Norm.CompileQuery<DbContext, int, BenchUser>(
            (c, take) => c.Query<BenchUser>().Where(u => u.IsActive == true).Take(take));
        // Warmup
        for (int i = 0; i < 500; i++) await compiled(ctx, 10);

        // 1. nORM compiled FIRST (no contamination)
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
            await compiled(ctx, 10);
        sw.Stop();
        Log($"nORM compiled: {sw.Elapsed.TotalMicroseconds / iterations:F2} us");

        // 2. ADO prepared (same SQL, same connection)
        // Get the SQL from the plan
        Expression<Func<DbContext, int, IQueryable<BenchUser>>> simpleExpr =
            (c, take) => c.Query<BenchUser>().Where(u => u.IsActive == true).Take(take);
        var ctxParam = simpleExpr.Parameters[0];
        var body = new nORM.Internal.ParameterReplacer(ctxParam, Expression.Constant(ctx)).Visit(simpleExpr.Body)!;
        body = new QueryCallEval().Visit(body)!;
        var provider = ctx.GetQueryProvider();
        var plan = provider.GetPlan(body, out _, out _);
        Log($"SQL: {plan.Sql}");

        var adoCmd = cn.CreateCommand();
        adoCmd.CommandText = plan.Sql;
        foreach (var p in plan.Parameters)
        {
            var param = adoCmd.CreateParameter();
            param.ParameterName = p.Key;
            param.Value = p.Value;
            adoCmd.Parameters.Add(param);
        }
        adoCmd.Prepare();
        var mat = plan.SyncMaterializer!;
        // Warmup
        for (int i = 0; i < 500; i++)
        {
            adoCmd.Parameters[0].Value = 10;
            using var r = adoCmd.ExecuteReader();
            while (r.Read()) {}
        }

        // 2a. ADO + nORM materializer (no flags)
        sw.Restart();
        for (int i = 0; i < iterations; i++)
        {
            adoCmd.Parameters[0].Value = 10;
            using var reader = adoCmd.ExecuteReader();
            var list = new System.Collections.Generic.List<BenchUser>(10);
            while (reader.Read())
                list.Add((BenchUser)mat(reader));
        }
        sw.Stop();
        Log($"ADO + nORM materializer (no flags): {sw.Elapsed.TotalMicroseconds / iterations:F2} us");

        // 2b. ADO + manual materialization (no flags)
        sw.Restart();
        for (int i = 0; i < iterations; i++)
        {
            adoCmd.Parameters[0].Value = 10;
            using var reader = adoCmd.ExecuteReader();
            var list = new System.Collections.Generic.List<BenchUser>(10);
            while (reader.Read())
            {
                list.Add(new BenchUser
                {
                    Id = reader.GetInt32(0), Name = reader.GetString(1), Email = reader.GetString(2),
                    Department = reader.GetString(3), Salary = reader.GetDouble(4),
                    IsActive = reader.GetInt32(5) == 1, CreatedAt = reader.GetDateTime(6),
                    Age = reader.GetInt32(7), City = reader.GetString(8),
                });
            }
        }
        sw.Stop();
        Log($"ADO + manual materialization (no flags): {sw.Elapsed.TotalMicroseconds / iterations:F2} us");

        // 2c. Dapper-style: different SQL (SELECT *)
        var dapperCmd = cn.CreateCommand();
        dapperCmd.CommandText = "SELECT * FROM BenchUser WHERE IsActive = 1 LIMIT @Take";
        var dapperParam = dapperCmd.CreateParameter();
        dapperParam.ParameterName = "@Take";
        dapperCmd.Parameters.Add(dapperParam);
        dapperCmd.Prepare();
        for (int i = 0; i < 200; i++) { dapperParam.Value = 10; using var r = dapperCmd.ExecuteReader(); while (r.Read()) {} }

        sw.Restart();
        for (int i = 0; i < iterations; i++)
        {
            dapperParam.Value = 10;
            using var reader = dapperCmd.ExecuteReader();
            var list = new System.Collections.Generic.List<BenchUser>(10);
            while (reader.Read())
            {
                list.Add(new BenchUser
                {
                    Id = reader.GetInt32(0), Name = reader.GetString(1), Email = reader.GetString(2),
                    Department = reader.GetString(3), Salary = reader.GetDouble(4),
                    IsActive = reader.GetInt32(5) == 1, CreatedAt = reader.GetDateTime(6),
                    Age = reader.GetInt32(7), City = reader.GetString(8),
                });
            }
        }
        sw.Stop();
        Log($"Dapper-style SQL + manual (no flags): {sw.Elapsed.TotalMicroseconds / iterations:F2} us");

        // 3. nORM compiled AGAIN (after ADO tests)
        sw.Restart();
        for (int i = 0; i < iterations; i++)
            await compiled(ctx, 10);
        sw.Stop();
        Log($"nORM compiled (2nd run): {sw.Elapsed.TotalMicroseconds / iterations:F2} us");

        // Row count
        {
            adoCmd.Parameters[0].Value = 10;
            using var reader = adoCmd.ExecuteReader();
            int count = 0;
            while (reader.Read()) count++;
            Log($"Rows returned: {count}");
        }
    }

    private static readonly string _logFile = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "norm_diag.txt");
    private void Log(string msg)
    {
        _output.WriteLine(msg);
        System.IO.File.AppendAllText(_logFile, msg + "\n");
    }

    private sealed class QueryCallEval : ExpressionVisitor
    {
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(NormQueryable) && node.Method.Name == "Query")
            {
                var result = nORM.Internal.ExpressionCompiler.Evaluate(node);
                return Expression.Constant(result, node.Type);
            }
            return base.VisitMethodCall(node);
        }
    }
}
