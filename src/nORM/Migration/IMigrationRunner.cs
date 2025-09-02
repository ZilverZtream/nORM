using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace nORM.Migration
{
    public interface IMigrationRunner
    {
        Task<bool> HasPendingMigrationsAsync(CancellationToken ct = default);
        Task ApplyMigrationsAsync(CancellationToken ct = default);
        Task<string[]> GetPendingMigrationsAsync(CancellationToken ct = default);
    }
}