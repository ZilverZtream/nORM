#nullable enable

namespace nORM.Enterprise
{
    public interface ITenantProvider
    {
        object GetCurrentTenantId();
    }
}