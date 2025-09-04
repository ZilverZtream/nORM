using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Versioning
{
    internal static class TemporalManager
    {
        public static async Task InitializeAsync(DbContext context)
        {
            var provider = context.Provider;
            await CreateTagsTableIfNotExistsAsync(context);

            foreach (var mapping in context.GetAllMappings())
            {
                if (await HistoryTableExistsAsync(context, mapping))
                    continue;

                var createHistoryTableSql = provider.GenerateCreateHistoryTableSql(mapping);
                await ExecuteDdlAsync(context, createHistoryTableSql);

                var createTriggersSql = provider.GenerateTemporalTriggersSql(mapping);
                await ExecuteDdlAsync(context, createTriggersSql);
            }
        }

        private static async Task CreateTagsTableIfNotExistsAsync(DbContext context)
        {
            var table = context.Provider.Escape("__NormTemporalTags");
            var sql = $"CREATE TABLE IF NOT EXISTS {table} (TagName TEXT PRIMARY KEY, Timestamp TEXT NOT NULL);";
            await ExecuteDdlAsync(context, sql);
        }

        private static async Task<bool> HistoryTableExistsAsync(DbContext context, TableMapping mapping)
        {
            var historyTable = context.Provider.Escape(mapping.TableName + "_History");
            try
            {
                await using var cmd = (await context.EnsureConnectionAsync()).CreateCommand();
                cmd.CommandText = $"SELECT 1 FROM {historyTable} LIMIT 1";
                await cmd.ExecuteScalarAsync();
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

            await handler.ExecuteWithExceptionHandling(async () =>
            {
                await using var cmd = (await context.EnsureConnectionAsync()).CreateCommand();
                cmd.CommandText = sql;
                await cmd.ExecuteNonQueryWithInterceptionAsync(context, default);
                return 0;
            }, "ExecuteDdlAsync", new Dictionary<string, object> { ["Sql"] = sql }).ConfigureAwait(false);
        }
    }
}
