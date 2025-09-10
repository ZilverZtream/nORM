using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Enterprise;

#nullable enable

namespace nORM.Internal
{
    internal static class CommandInterceptorExtensions
    {
        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteNonQueryAsync"/> while invoking any registered
        /// command interceptors before and after execution.
        /// </summary>
        /// <param name="command">The database command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The number of rows affected.</returns>
        public static async Task<int> ExecuteNonQueryWithInterceptionAsync(this DbCommand command, DbContext ctx, CancellationToken ct)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.NonQueryExecutingAsync(command, ctx, ct).ConfigureAwait(false);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.NonQueryExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, ct).ConfigureAwait(false);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.NonQueryExecutedAsync(command, ctx, result, sw.Elapsed, ct).ConfigureAwait(false);
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct).ConfigureAwait(false);
                }
                throw;
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteNonQuery"/> while invoking registered command interceptors
        /// before and after execution.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <returns>The number of rows affected.</returns>
        public static int ExecuteNonQueryWithInterception(this DbCommand command, DbContext ctx)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return command.ExecuteNonQuery();
            }

            foreach (var interceptor in interceptors)
            {
                var interception = interceptor.NonQueryExecutingAsync(command, ctx, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        i.NonQueryExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = command.ExecuteNonQuery();
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.NonQueryExecutedAsync(command, ctx, result, sw.Elapsed, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.CommandFailedAsync(command, ctx, ex, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                throw;
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteScalarAsync"/> while invoking registered
        /// command interceptors surrounding the call.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The scalar result returned by the command.</returns>
        public static async Task<object?> ExecuteScalarWithInterceptionAsync(this DbCommand command, DbContext ctx, CancellationToken ct)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.ScalarExecutingAsync(command, ctx, ct).ConfigureAwait(false);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.ScalarExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, ct).ConfigureAwait(false);
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.ScalarExecutedAsync(command, ctx, result, sw.Elapsed, ct).ConfigureAwait(false);
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct).ConfigureAwait(false);
                }
                throw;
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteScalar"/> while invoking registered command interceptors
        /// before and after execution.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <returns>The scalar result returned by the command.</returns>
        public static object? ExecuteScalarWithInterception(this DbCommand command, DbContext ctx)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return command.ExecuteScalar();
            }

            foreach (var interceptor in interceptors)
            {
                var interception = interceptor.ScalarExecutingAsync(command, ctx, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        i.ScalarExecutedAsync(command, ctx, interception.Result, TimeSpan.Zero, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                    return interception.Result;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var result = command.ExecuteScalar();
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.ScalarExecutedAsync(command, ctx, result, sw.Elapsed, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                return result;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.CommandFailedAsync(command, ctx, ex, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                throw;
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>
        /// while invoking registered command interceptors around the execution.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <param name="ctx">The current <see cref="DbContext"/>.</param>
        /// <param name="behavior">Reader behavior flags.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The resulting <see cref="DbDataReader"/>.</returns>
        public static async Task<DbDataReader> ExecuteReaderWithInterceptionAsync(this DbCommand command, DbContext ctx, CommandBehavior behavior, CancellationToken ct)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return await command.ExecuteReaderAsync(behavior, ct).ConfigureAwait(false);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = await interceptor.ReaderExecutingAsync(command, ctx, ct).ConfigureAwait(false);
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        await i.ReaderExecutedAsync(command, ctx, interception.Result!, TimeSpan.Zero, ct).ConfigureAwait(false);
                    return interception.Result!;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var reader = await command.ExecuteReaderAsync(behavior, ct).ConfigureAwait(false);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.ReaderExecutedAsync(command, ctx, reader, sw.Elapsed, ct).ConfigureAwait(false);
                }
                return reader;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    await interceptor.CommandFailedAsync(command, ctx, ex, ct).ConfigureAwait(false);
                }
                throw;
            }
        }

        /// <summary>
        /// Executes <see cref="DbCommand.ExecuteReader(CommandBehavior)"/> while wrapping the
        /// call with the currently registered command interceptors. This allows interceptors
        /// to observe, modify or suppress the command execution and to be notified about
        /// success or failure.
        /// </summary>
        /// <param name="command">The database command to execute.</param>
        /// <param name="ctx">The <see cref="DbContext"/> associated with the command.</param>
        /// <param name="behavior">Behavior flags that influence reader execution.</param>
        /// <returns>The <see cref="DbDataReader"/> returned by the command execution.</returns>
        /// <remarks>
        /// The method synchronously waits for any asynchronous interceptor callbacks and
        /// therefore should only be used in fully synchronous flows.
        /// </remarks>
        public static DbDataReader ExecuteReaderWithInterception(this DbCommand command, DbContext ctx, CommandBehavior behavior)
        {
            var interceptors = ctx.Options.CommandInterceptors;
            if (interceptors.Count == 0)
            {
                return command.ExecuteReader(behavior);
            }

            foreach (var interceptor in interceptors)
            {
                var interception = interceptor.ReaderExecutingAsync(command, ctx, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                if (interception.IsSuppressed)
                {
                    foreach (var i in interceptors)
                        i.ReaderExecutedAsync(command, ctx, interception.Result!, TimeSpan.Zero, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                    return interception.Result!;
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                var reader = command.ExecuteReader(behavior);
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.ReaderExecutedAsync(command, ctx, reader, sw.Elapsed, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                return reader;
            }
            catch (Exception ex)
            {
                sw.Stop();
                foreach (var interceptor in interceptors)
                {
                    interceptor.CommandFailedAsync(command, ctx, ex, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                throw;
            }
        }
    }
}
