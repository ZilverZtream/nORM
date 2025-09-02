using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;

#nullable enable

namespace nORM.Enterprise
{
    /// <summary>
    /// Defines hooks that are invoked before and after execution of <see cref="DbCommand"/> instances.
    /// Implementations may inspect or modify the command, short-circuit execution, or react to failures.
    /// </summary>
    public interface IDbCommandInterceptor
    {
        /// <summary>
        /// Called before a command that does not return rows is executed.
        /// Returning a suppressed result prevents command execution and returns the provided value instead.
        /// </summary>
        Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken);

        /// <summary>
        /// Called after a command that does not return rows has executed.
        /// </summary>
        Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken);

        /// <summary>
        /// Called before a command that returns a scalar value is executed.
        /// Returning a suppressed result prevents command execution and returns the provided value instead.
        /// </summary>
        Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken);

        /// <summary>
        /// Called after a command that returns a scalar value has executed.
        /// </summary>
        Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken);

        /// <summary>
        /// Called before a command that returns a reader is executed.
        /// Returning a suppressed reader prevents command execution and returns the provided reader instead.
        /// </summary>
        Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken);

        /// <summary>
        /// Called after a command that returns a reader has executed.
        /// </summary>
        Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken);

        /// <summary>
        /// Called when execution of a command results in an exception.
        /// </summary>
        Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken);
    }
}
