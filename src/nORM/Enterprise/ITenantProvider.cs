#nullable enable

namespace nORM.Enterprise
{
    /// <summary>
    /// Provides access to the current tenant identifier in multi-tenant applications.
    /// </summary>
    public interface ITenantProvider
    {
        /// <summary>
        /// Retrieves the identifier of the tenant associated with the current execution context.
        /// </summary>
        /// <returns>A value representing the current tenant.</returns>
        object GetCurrentTenantId();
    }
}