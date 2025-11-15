using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Lightweight provider implementation targeting SQLite databases.
    /// Provides SQL generation and initialization routines specific to SQLite's feature set.
    /// </summary>
    public sealed class SqliteProvider : DatabaseProvider
    {
        /// <summary>
        /// Maximum length of a single SQL statement supported by SQLite.
        /// </summary>
        public override int MaxSqlLength => 1_000_000;

        /// <summary>
        /// Maximum number of parameters allowed in a single SQLite command.
        /// </summary>
        public override int MaxParameters => 999;

        /// <summary>
        /// Escapes an identifier by wrapping it in double quotes, per SQLite requirements.
        /// </summary>
        /// <param name="id">Identifier to escape.</param>
        /// <returns>The escaped identifier.</returns>
        public override string Escape(string id) => $"\"{id}\"";

        /// <summary>
        /// Character used to prefix parameter names in SQLite commands.
        /// </summary>
        public override char ParameterPrefixChar => '@';

        /// <summary>
        /// SQLite does not support stored procedures; commands are always text.
        /// </summary>
        public override CommandType StoredProcedureCommandType => CommandType.Text;

        /// <summary>
        /// Builds a minimal <c>SELECT</c> statement directly into a character buffer.
        /// </summary>
        /// <param name="buffer">Buffer receiving the SQL.</param>
        /// <param name="table">Table name to query.</param>
        /// <param name="columns">Comma-separated column list.</param>
        /// <param name="length">Receives the length of the generated SQL.</param>
        public override void BuildSimpleSelect(Span<char> buffer, ReadOnlySpan<char> table,
            ReadOnlySpan<char> columns, out int length)
        {
            "SELECT ".CopyTo(buffer);
            var pos = 7;
            columns.CopyTo(buffer.Slice(pos));
            pos += columns.Length;
            " FROM ".CopyTo(buffer.Slice(pos));
            pos += 6;
            table.CopyTo(buffer.Slice(pos));
            length = pos + table.Length;
        }

        /// <summary>
        /// Applies performance-related <c>PRAGMA</c> settings to the supplied SQLite connection asynchronously.
        /// </summary>
        /// <param name="connection">The connection to configure.</param>
        /// <param name="ct">Cancellation token.</param>
        public override async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
        {
            ValidateConnection(connection);
            await using var pragmaCmd = connection.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
            await pragmaCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Applies performance-related <c>PRAGMA</c> settings to the SQLite connection.
        /// </summary>
        /// <param name="connection">The connection to configure.</param>
        public override void InitializeConnection(DbConnection connection)
        {
            ValidateConnection(connection);
            using var pragmaCmd = connection.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
            pragmaCmd.ExecuteNonQuery();
        }
        
        /// <summary>
        /// Appends SQLite <c>LIMIT</c> and <c>OFFSET</c> clauses to the SQL builder.
        /// </summary>
        /// <param name="sb">Builder receiving the clauses.</param>
        /// <param name="limit">Maximum number of rows to return.</param>
        /// <param name="offset">Number of rows to skip.</param>
        /// <param name="limitParameterName">Name of the parameter supplying the limit.</param>
        /// <param name="offsetParameterName">Name of the parameter supplying the offset.</param>
        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            if (limitParameterName != null)
                sb.Append(" LIMIT ").Append(limitParameterName);

            if (offsetParameterName != null)
                sb.Append(" OFFSET ").Append(offsetParameterName);
        }
        
        /// <summary>
        /// Returns SQL that retrieves the last rowid generated by an <c>INSERT</c>.
        /// </summary>
        /// <param name="m">The mapping for which the identity is retrieved.</param>
        /// <returns>SQL fragment to append to the insert command.</returns>
        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT last_insert_rowid();";
        
        /// <summary>
        /// Creates a SQLite parameter with the given name and value.
        /// </summary>
        /// <param name="name">Parameter name including prefix.</param>
        /// <param name="value">Parameter value; <c>null</c> becomes <see cref="DBNull.Value"/>.</param>
        /// <returns>A configured <see cref="SqliteParameter"/>.</returns>
        public override DbParameter CreateParameter(string name, object? value)
        {
            return new SqliteParameter(name, value ?? DBNull.Value);
        }

        /// <summary>
        /// Attempts to translate a .NET method into its SQLite SQL equivalent.
        /// </summary>
        /// <param name="name">Name of the method being translated.</param>
        /// <param name="declaringType">Type that declares the method.</param>
        /// <param name="args">SQL fragments representing the arguments.</param>
        /// <returns>The translated SQL or <c>null</c> if unsupported.</returns>
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            if (declaringType == typeof(string))
            {
                return name switch
                {
                    nameof(string.ToUpper) => $"UPPER({args[0]})",
                    nameof(string.ToLower) => $"LOWER({args[0]})",
                    nameof(string.Length) when args.Length == 1 => $"LENGTH({args[0]})",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTime))
            {
                return name switch
                {
                    nameof(DateTime.Year) => $"CAST(strftime('%Y', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Month) => $"CAST(strftime('%m', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Day) => $"CAST(strftime('%d', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Hour) => $"CAST(strftime('%H', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Minute) => $"CAST(strftime('%M', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Second) => $"CAST(strftime('%S', {args[0]}) AS INTEGER)",
                    _ => null
                };
            }

            if (declaringType == typeof(Math))
            {
                return name switch
                {
                    nameof(Math.Abs) => $"ABS({args[0]})",
                    nameof(Math.Ceiling) => $"CEIL({args[0]})",
                    nameof(Math.Floor) => $"FLOOR({args[0]})",
                    nameof(Math.Round) when args.Length > 1 => $"ROUND({args[0]}, {args[1]})",
                    nameof(Math.Round) => $"ROUND({args[0]})",
                    _ => null
                };
            }

            return null;
        }

        /// <summary>
        /// Translates JSON value access using SQLite's <c>json_extract</c> function.
        /// </summary>
        /// <param name="columnName">JSON column to access.</param>
        /// <param name="jsonPath">JSON path expression.</param>
        /// <returns>SQL fragment retrieving the requested JSON value.</returns>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
            => $"json_extract({columnName}, '{jsonPath}')";

        /// <summary>
        /// Generates SQL to create a simple temporal history table for SQLite.
        /// </summary>
        /// <param name="mapping">The entity mapping being tracked.</param>
        /// <returns>DDL statement that creates the history table.</returns>
        public override string GenerateCreateHistoryTableSql(TableMapping mapping)
        {
            var columns = string.Join(", ", mapping.Columns.Select(c => $"{Escape(c.PropName)} TEXT"));
            return @$"CREATE TABLE IF NOT EXISTS {Escape(mapping.TableName + "_History")} (
                __VersionId INTEGER PRIMARY KEY AUTOINCREMENT,
                __ValidFrom TEXT NOT NULL,
                __ValidTo TEXT NOT NULL,
                __Operation TEXT NOT NULL,
                {columns}
            );";
        }

        /// <summary>
        /// Generates trigger definitions that maintain the temporal history table.
        /// </summary>
        /// <param name="mapping">The mapping describing the source table.</param>
        /// <returns>DDL statements creating the triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columnList = string.Join(", ", mapping.Columns.Select(c => Escape(c.PropName)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.PropName)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.PropName)));
            var keyCondition = mapping.KeyColumns.Length > 0
                ? string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.PropName)} = OLD.{Escape(c.PropName)}"))
                : "1=1";

            return @$"
CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_ai")} AFTER INSERT ON {table}
BEGIN
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES (datetime('now'), '9999-12-31', 'I', {newColumns});
END;

CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_au")} AFTER UPDATE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = datetime('now') WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES (datetime('now'), '9999-12-31', 'U', {newColumns});
END;

CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_ad")} AFTER DELETE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = datetime('now') WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES (datetime('now'), datetime('now'), 'D', {oldColumns});
END;";
        }

        /// <summary>
        /// Verifies that the provided connection is a <see cref="SqliteConnection"/>, as required
        /// by this provider.
        /// </summary>
        /// <param name="connection">The connection to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown if the connection is not compatible.</exception>
        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection is not SqliteConnection)
                throw new InvalidOperationException("A SqliteConnection is required for SqliteProvider.");
        }

        /// <summary>
        /// Determines if the SQLite provider can be used in the current environment by
        /// verifying that the required <c>Microsoft.Data.Sqlite</c> assembly is available
        /// and that the SQLite engine meets the minimum version requirement.
        /// </summary>
        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Microsoft.Data.Sqlite.SqliteConnection, Microsoft.Data.Sqlite");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString = "Data Source=:memory:";
            try
            {
                await cn.OpenAsync();
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "select sqlite_version()";
                var result = await cmd.ExecuteScalarAsync();
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var version = new Version(versionStr);
                return version >= new Version(3, 9);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Creates a savepoint within a SQLite transaction allowing partial rollbacks.
        /// </summary>
        /// <param name="transaction">The active SQLite transaction.</param>
        /// <param name="name">Name of the savepoint to create.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction is SqliteTransaction sqliteTransaction)
            {
                sqliteTransaction.Save(name);
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction));
        }

        /// <summary>
        /// Rolls back a SQLite transaction to the specified savepoint.
        /// </summary>
        /// <param name="transaction">The active SQLite transaction.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction is SqliteTransaction sqliteTransaction)
            {
                sqliteTransaction.Rollback(name);
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction));
        }

        // PERFORMANCE FIX (TASK 16): SQLite-optimized bulk insert using prepared statements
        /// <summary>
        /// Inserts a collection of entities using SQLite-optimized prepared statements in a single transaction.
        /// PERFORMANCE FIX (TASK 16): Uses prepared statement reuse and transaction batching for optimal SQLite performance.
        /// This is significantly faster than calling base class methods which use multiple transactions.
        /// </summary>
        /// <typeparam name="T">Type of entity being inserted.</typeparam>
        /// <param name="ctx">Current <see cref="DbContext"/>.</param>
        /// <param name="m">Mapping for the destination table.</param>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows inserted.</returns>
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities as ICollection<T> ?? entities.ToList();
            if (!entityList.Any()) return 0;

            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();
            if (cols.Length == 0) return 0; // Nothing to insert.

            var totalInserted = 0;

            // 1. Start a single transaction for the entire operation.
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                // 2. Create ONE command and ONE set of parameters that will be reused.
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = BuildInsert(m); // Uses the cached single-row INSERT statement.

                // Create parameter objects ONCE and add them to the command.
                var parameters = new DbParameter[cols.Length];
                for (int i = 0; i < cols.Length; i++)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = $"{ParamPrefix}{cols[i].PropName}";
                    cmd.Parameters.Add(p);
                    parameters[i] = p;
                }

                // 3. Prepare the command ONCE before the loop.
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

                // 4. Loop through each entity and execute the prepared command.
                foreach (var entity in entityList)
                {
                    // 5. Simply update the values of the existing parameters. No new objects created.
                    for (int i = 0; i < cols.Length; i++)
                    {
                        parameters[i].Value = cols[i].Getter(entity) ?? DBNull.Value;
                    }

                    totalInserted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }

                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
            }

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw.Elapsed);
            return totalInserted;
        }
        
        // PERFORMANCE FIX (TASK 16): SQLite-optimized bulk update using temp tables
        /// <summary>
        /// Updates multiple entities using SQLite-optimized temp table approach for efficient bulk updates.
        /// PERFORMANCE FIX (TASK 16): Uses temp tables and UPDATE FROM pattern for optimal SQLite performance.
        /// This is significantly faster than the base class batched operations.
        /// </summary>
        /// <typeparam name="T">Type of entity being updated.</typeparam>
        /// <param name="ctx">Active <see cref="DbContext"/>.</param>
        /// <param name="m">Mapping metadata for the entity's table.</param>
        /// <param name="entities">Entities containing new values.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of rows updated.</returns>
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            // PERFORMANCE FIX (TASK 16): Use temp table approach for SQLite bulk updates
            var tempTableName = $"\"BulkUpdate_{Guid.NewGuid():N}\"";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToList();
            var keyCols = m.KeyColumns.ToList();

            var totalUpdated = 0;

            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                // Create temp table with same schema
                var colDefs = string.Join(", ", m.Columns.Select(c => $"{Escape(c.PropName)} {GetSqliteType(c.Prop.PropertyType)}"));
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = $"CREATE TEMP TABLE {tempTableName} ({colDefs})";
                    await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                // Insert entities into temp table using prepared statement
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    var insertCols = m.Columns.ToArray();
                    var paramPlaceholders = string.Join(", ", insertCols.Select(c => ParamPrefix + c.PropName));
                    var colNames = string.Join(", ", insertCols.Select(c => Escape(c.PropName)));
                    cmd.CommandText = $"INSERT INTO {tempTableName} ({colNames}) VALUES ({paramPlaceholders})";

                    var parameters = new DbParameter[insertCols.Length];
                    for (int i = 0; i < insertCols.Length; i++)
                    {
                        var p = cmd.CreateParameter();
                        p.ParameterName = ParamPrefix + insertCols[i].PropName;
                        cmd.Parameters.Add(p);
                        parameters[i] = p;
                    }

                    await cmd.PrepareAsync(ct).ConfigureAwait(false);

                    foreach (var entity in entityList)
                    {
                        for (int i = 0; i < insertCols.Length; i++)
                        {
                            parameters[i].Value = insertCols[i].Getter(entity) ?? DBNull.Value;
                        }
                        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                    }
                }

                // Perform bulk update using temp table join
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    var setClause = string.Join(", ", nonKeyCols.Select(c => $"{c.EscCol} = (SELECT {Escape(c.PropName)} FROM {tempTableName} WHERE {string.Join(" AND ", keyCols.Select(k => $"{tempTableName}.{Escape(k.PropName)} = {m.EscTable}.{k.EscCol}"))})"));
                    var whereClause = $"EXISTS (SELECT 1 FROM {tempTableName} WHERE {string.Join(" AND ", keyCols.Select(k => $"{tempTableName}.{Escape(k.PropName)} = {m.EscTable}.{k.EscCol}"))})";

                    cmd.CommandText = $"UPDATE {m.EscTable} SET {setClause} WHERE {whereClause}";
                    totalUpdated = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }

                // Clean up temp table
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = $"DROP TABLE {tempTableName}";
                    await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
            }

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }

        private static string GetSqliteType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte) || t == typeof(bool)) return "INTEGER";
            if (t == typeof(decimal) || t == typeof(double) || t == typeof(float)) return "REAL";
            if (t == typeof(byte[])) return "BLOB";
            return "TEXT";
        }
        
        // PERFORMANCE FIX (TASK 16): SQLite-optimized bulk delete using WHERE IN clauses
        /// <summary>
        /// Deletes entities in bulk using SQLite-optimized WHERE IN clauses for single-key tables
        /// or prepared statements for composite keys.
        /// PERFORMANCE FIX (TASK 16): Uses batched WHERE IN clauses for optimal SQLite performance.
        /// This is significantly faster than the base class operations.
        /// </summary>
        /// <typeparam name="T">Type of entity to delete.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> managing the connection.</param>
        /// <param name="m">Mapping that provides key column information.</param>
        /// <param name="entities">Entities whose keys determine the rows to remove.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows deleted.</returns>
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;
            
            if (!m.KeyColumns.Any())
                throw new NormConfigurationException($"Cannot delete from '{m.EscTable}': no key columns defined.");
            
            var totalDeleted = 0;
            // Respect provider parameter limits when batching deletes
            var batchSize = ctx.Options.BulkBatchSize;
            if (MaxParameters != int.MaxValue)
                batchSize = Math.Min(batchSize, MaxParameters);
            if (batchSize <= 0) batchSize = 1;
            
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                if (m.KeyColumns.Length == 1)
                {
                    var keyCol = m.KeyColumns[0];
                    
                    for (int i = 0; i < entityList.Count; i += batchSize)
                    {
                        var batch = entityList.Skip(i).Take(batchSize).ToList();
                        await using var cmd = ctx.Connection.CreateCommand();
                        cmd.Transaction = transaction;
                        cmd.CommandTimeout = 30;
                        
                        var paramNames = new List<string>();
                        var paramIndex = 0;
                        
                        foreach (var entity in batch)
                        {
                            var paramName = $"{ParamPrefix}p{paramIndex++}";
                            paramNames.Add(paramName);
                            cmd.AddParam(paramName, keyCol.Getter(entity));
                        }
                        
                        cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {keyCol.EscCol} IN ({string.Join(",", paramNames)})";
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
                }
                else
                {
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = BuildDelete(m);
                    cmd.CommandTimeout = 30;
                    await cmd.PrepareAsync(ct).ConfigureAwait(false);
                    
                    foreach (var entity in entityList)
                    {
                        cmd.Parameters.Clear();
                        
                        foreach (var col in m.KeyColumns)
                        {
                            cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                        }
                        
                        if (m.TimestampColumn != null)
                        {
                            cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                        }
                        
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
                }
                
                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
            }
            
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }
    }
}