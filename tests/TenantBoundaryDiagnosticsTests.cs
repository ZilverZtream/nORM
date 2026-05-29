using System;
using System.ComponentModel.DataAnnotations;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class TenantBoundaryDiagnosticsTests
{
    private sealed class DiagTenantRow
    {
        [Key]
        public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class DiagGlobalRow
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public void GetTenantBoundaryDiagnostics_reports_redacted_predicate_shape()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, tenantId: 101);

        var diagnostics = ctx.GetTenantBoundaryDiagnostics<DiagTenantRow>("query");

        Assert.True(diagnostics.IsTenantScoped);
        Assert.Equal(typeof(DiagTenantRow), diagnostics.EntityType);
        Assert.Equal(nameof(DiagTenantRow.TenantId), diagnostics.TenantColumnName);
        Assert.Equal(typeof(int).FullName, diagnostics.TenantIdType);
        Assert.Equal("@__tenant", diagnostics.ParameterName);
        Assert.Contains("TenantId", diagnostics.PredicateSql, StringComparison.Ordinal);
        Assert.Contains("@__tenant", diagnostics.PredicateSql, StringComparison.Ordinal);
        Assert.DoesNotContain("101", diagnostics.PredicateSql, StringComparison.Ordinal);
    }

    [Fact]
    public void GetTenantBoundaryDiagnostics_reports_unscoped_when_no_tenant_provider()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, tenantId: null);

        var diagnostics = ctx.GetTenantBoundaryDiagnostics<DiagTenantRow>();

        Assert.False(diagnostics.IsTenantScoped);
        Assert.Null(diagnostics.TenantColumnName);
        Assert.Null(diagnostics.PredicateSql);
    }

    [Fact]
    public void GetTenantBoundaryDiagnostics_fails_closed_when_tenant_column_missing()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, tenantId: 101);

        var ex = Assert.Throws<NormConfigurationException>(() =>
            ctx.GetTenantBoundaryDiagnostics<DiagGlobalRow>("query"));
        Assert.Contains("does not map tenant column", ex.Message, StringComparison.Ordinal);
    }

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static DbContext CreateContext(SqliteConnection cn, int? tenantId)
    {
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<DiagTenantRow>();
                mb.Entity<DiagGlobalRow>();
            }
        };
        if (tenantId != null)
        {
            options.TenantColumnName = nameof(DiagTenantRow.TenantId);
            options.TenantProvider = new FixedTenantProvider(tenantId.Value);
        }

        return new DbContext(cn, new SqliteProvider(), options);
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }
}
