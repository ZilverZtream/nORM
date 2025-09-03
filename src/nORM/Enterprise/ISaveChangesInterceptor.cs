using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;

#nullable enable

namespace nORM.Enterprise
{
    /// <summary>
    /// Provides hooks that run before and after <see cref="DbContext.SaveChangesAsync"/>.
    /// Implementations may inspect or modify tracked entities or react to persistence results.
    /// </summary>
    public interface ISaveChangesInterceptor
    {
        /// <summary>
        /// Called before any changes are persisted to the database.
        /// </summary>
        Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken cancellationToken);

        /// <summary>
        /// Called after changes have been persisted to the database.
        /// </summary>
        Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken cancellationToken);
    }
}
