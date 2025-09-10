using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Versioning
{
    /// <summary>
    /// Handles creation of temporal history tables and triggers for entities configured for versioning.
    /// </summary>
    internal static class TemporalManager
    {
        /// <summary>
        /// Ensures that temporal infrastructure exists for all mapped entities.
        /// </summary>
        /// <param name="context">The <see cref="DbContext"/> containing the mappings.</param>
        /// <returns>A task representing the initialization process.</returns>
        public static async Task InitializeAsync(DbContext context)
        {
            var provider = context.Provider;
            await CreateTagsTableIfNotExistsAsync(context).ConfigureAwait(false);

            foreach (var mapping in context.GetAllMappings())
            {
                if (await HistoryTableExistsAsync(context, mapping).ConfigureAwait(false))
                    continue;

                var createHistoryTableSql = provider.GenerateCreateHistoryTableSql(mapping);
                await ExecuteDdlAsync(context, createHistoryTableSql).ConfigureAwait(false);

                var createTriggersSql = provider.GenerateTemporalTriggersSql(mapping);
                await ExecuteDdlAsync(context, createTriggersSql).ConfigureAwait(false);
            }
        }

        private static async Task CreateTagsTableIfNotExistsAsync(DbContext context)
        {
            var table = context.Provider.Escape("__NormTemporalTags");
            var sql = $"CREATE TABLE IF NOT EXISTS {table} (TagName TEXT PRIMARY KEY, Timestamp TEXT NOT NULL);";
            await ExecuteDdlAsync(context, sql).ConfigureAwait(false);
        }

        private static async Task<bool> HistoryTableExistsAsync(DbContext context, TableMapping mapping)
        {
            var historyTable = context.Provider.Escape(mapping.TableName + "_History");
            try
            {
                await using var cmd = (await context.EnsureConnectionAsync().ConfigureAwait(false)).CreateCommand();
                cmd.CommandText = $"SELECT 1 FROM {historyTable} LIMIT 1";
                await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static async Task ExecuteDdlAsync(DbContext context, string sql)
        {
            var handler = new NormExceptionHandler(NullLogger.Instance);

            var batches = sql.Split(new[] { "\r\nGO\r\n", "\nGO\n", "\r\nGO\n", "\nGO\r\n", "\rGO\r", "\nGO\r" }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var batch in batches)
            {
                var trimmed = batch.Trim();
                if (trimmed.Length == 0) continue;

                if (!IsValidDdl(trimmed))
                    throw new NormQueryException($"Invalid DDL: {trimmed}");

                await handler.ExecuteWithExceptionHandling(async () =>
                {
                    await using var cmd = (await context.EnsureConnectionAsync().ConfigureAwait(false)).CreateCommand();
                    cmd.CommandText = trimmed;
                    await cmd.ExecuteNonQueryWithInterceptionAsync(context, default).ConfigureAwait(false);
                    return 0;
                }, "ExecuteDdlAsync", new Dictionary<string, object> { ["Sql"] = trimmed }).ConfigureAwait(false);
            }
        }

        private static bool IsValidDdl(string ddl)
        {
            var lower = ddl.ToLowerInvariant();
            return lower.StartsWith("create") || lower.StartsWith("alter") || lower.StartsWith("drop");
        }
    }
}
