using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace nORM.Migration
{
    /// <summary>
    /// Defines a service capable of applying and querying database migrations.
    /// </summary>
    public interface IMigrationRunner
    {
        /// <summary>
        /// Determines whether there are migrations in the configured assembly that have not
        /// yet been applied to the target database.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns><c>true</c> if pending migrations exist; otherwise, <c>false</c>.</returns>
        Task<bool> HasPendingMigrationsAsync(CancellationToken ct = default);

        /// <summary>
        /// Applies all pending migrations to the database.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        Task ApplyMigrationsAsync(CancellationToken ct = default);

        /// <summary>
        /// Returns the identifiers of migrations that have not yet been applied.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>An array of strings representing pending migration identifiers.</returns>
        Task<string[]> GetPendingMigrationsAsync(CancellationToken ct = default);
    }
}