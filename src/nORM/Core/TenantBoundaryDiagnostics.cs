using System;

namespace nORM.Core
{
    /// <summary>
    /// Describes the tenant boundary nORM applies to generated paths for one mapped entity.
    /// </summary>
    public sealed class TenantBoundaryDiagnostics
    {
        /// <summary>
        /// Initializes tenant boundary diagnostics.
        /// </summary>
        /// <param name="entityType">Mapped CLR entity type.</param>
        /// <param name="tableName">Mapped database table name.</param>
        /// <param name="isTenantScoped">Whether tenant mode applies to the entity.</param>
        /// <param name="tenantColumnName">Mapped tenant column name, when tenant-scoped.</param>
        /// <param name="tenantIdType">Runtime tenant identifier type, when available.</param>
        /// <param name="parameterName">Generated parameter name used for diagnostics.</param>
        /// <param name="predicateSql">Redacted provider SQL predicate shape.</param>
        public TenantBoundaryDiagnostics(
            Type entityType,
            string tableName,
            bool isTenantScoped,
            string? tenantColumnName,
            string? tenantIdType,
            string? parameterName,
            string? predicateSql)
        {
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            IsTenantScoped = isTenantScoped;
            TenantColumnName = tenantColumnName;
            TenantIdType = tenantIdType;
            ParameterName = parameterName;
            PredicateSql = predicateSql;
        }

        /// <summary>Mapped CLR entity type.</summary>
        public Type EntityType { get; }

        /// <summary>Mapped database table name.</summary>
        public string TableName { get; }

        /// <summary>Whether tenant mode applies to the entity.</summary>
        public bool IsTenantScoped { get; }

        /// <summary>Mapped tenant column name, when tenant-scoped.</summary>
        public string? TenantColumnName { get; }

        /// <summary>Runtime tenant identifier type, when available.</summary>
        public string? TenantIdType { get; }

        /// <summary>Generated parameter name used for diagnostics.</summary>
        public string? ParameterName { get; }

        /// <summary>Redacted provider SQL predicate shape.</summary>
        public string? PredicateSql { get; }
    }
}
