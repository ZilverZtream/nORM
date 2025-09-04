using System.Data.Common;

#nullable enable

namespace nORM.Providers
{
    public interface IDbParameterFactory
    {
        DbParameter CreateParameter(string name, object? value);
    }
}
