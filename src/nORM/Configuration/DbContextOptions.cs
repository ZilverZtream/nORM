using System;
using nORM.Enterprise;

#nullable enable

namespace nORM.Configuration
{
    public class DbContextOptions
    {
        public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public int BulkBatchSize { get; set; } = 1000;
        public IDbContextLogger? Logger { get; set; }
        public RetryPolicy? RetryPolicy { get; set; }
        public ITenantProvider? TenantProvider { get; set; }
        public string TenantColumnName { get; set; } = "TenantId";
        public Action<ModelBuilder>? OnModelCreating { get; set; }
        public bool UseBatchedBulkOps { get; set; } = false;
    }
}