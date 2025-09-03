using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Providers
{
    public sealed class SqliteProvider : DatabaseProvider
    {
        public override int MaxSqlLength => 1_000_000;
        public override int MaxParameters => 999;
        public override string Escape(string id) => $"\"{id}\"";
        
        public override void ApplyPaging(StringBuilder sb, int? limit, int? offset, string? limitParam, string? offsetParam)
        {
            if (limitParam != null)
                sb.Append(" LIMIT ").Append(limitParam);
            else if (limit.HasValue)
                sb.Append($" LIMIT {limit}");

            if (offsetParam != null)
                sb.Append(" OFFSET ").Append(offsetParam);
            else if (offset.HasValue)
                sb.Append($" OFFSET {offset}");
        }
        
        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT last_insert_rowid();";
        
        public override DbParameter CreateParameter(string name, object? value)
        {
            return new SqliteParameter(name, value ?? DBNull.Value);
        }

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

        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection is not SqliteConnection)
                throw new InvalidOperationException("A SqliteConnection is required for SqliteProvider.");
        }

        public override Task<bool> IsAvailableAsync()
        {
            return Task.FromResult(Type.GetType("Microsoft.Data.Sqlite.SqliteConnection, Microsoft.Data.Sqlite") != null);
        }

        // High-performance SQLite bulk insert using optimized single-row prepared statements
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities as List<T> ?? entities.ToList();
            if (!entityList.Any()) return 0;
            
            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();
            if (!cols.Any()) return 0;
            
            var recordsAffected = 0;
            
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct);
            try
            {
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandTimeout = 30;
                
                // Build single-row INSERT SQL
                var colNames = string.Join(", ", cols.Select(c => c.EscCol));
                var paramNames = string.Join(", ", cols.Select(c => ParamPrefix + c.PropName));
                cmd.CommandText = $"INSERT INTO {m.EscTable} ({colNames}) VALUES ({paramNames})";
                
                // Add parameters once
                foreach (var col in cols)
                {
                    var param = cmd.CreateParameter();
                    param.ParameterName = ParamPrefix + col.PropName;
                    cmd.Parameters.Add(param);
                }
                
                // Prepare the command once
                await cmd.PrepareAsync(ct);
                
                // Insert all data
                foreach (var entity in entityList)
                {
                    foreach (var col in cols)
                    {
                        cmd.Parameters[ParamPrefix + col.PropName].Value = col.Getter(entity) ?? DBNull.Value;
                    }
                    recordsAffected += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                }
                
                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
            }
            
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, recordsAffected, sw.Elapsed);
            return recordsAffected;
        }
        
        // Optimized bulk update for SQLite
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;
            
            var totalUpdated = 0;
            
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct);
            try
            {
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = BuildUpdate(m);
                cmd.CommandTimeout = 30;
                
                await cmd.PrepareAsync(ct);
                
                foreach (var entity in entityList)
                {
                    cmd.Parameters.Clear();
                    
                    foreach (var col in m.Columns.Where(c => !c.IsKey && !c.IsTimestamp))
                    {
                        cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                    }
                    
                    foreach (var col in m.KeyColumns)
                    {
                        cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                    }
                    
                    if (m.TimestampColumn != null)
                    {
                        cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                    }
                    
                    totalUpdated += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                }
                
                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
            }
            
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }
        
        // Optimized bulk delete for SQLite using IN clauses where possible
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;
            
            if (!m.KeyColumns.Any())
                throw new System.Exception($"Cannot delete from '{m.EscTable}': no key columns defined.");
            
            var totalDeleted = 0;
            var batchSize = 100; // Reasonable batch size for IN clauses
            
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct);
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
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                    }
                }
                else
                {
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = BuildDelete(m);
                    cmd.CommandTimeout = 30;
                    await cmd.PrepareAsync(ct);
                    
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
                        
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                    }
                }
                
                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
            }
            
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }
    }
}