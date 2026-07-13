using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    // Write-support infrastructure shared by the single-entity, batched, and bulk write paths:
    // parameter binding helpers, concurrency-token generation, and command scoping. Kept apart
    // from DbContext.WriteOperations.cs (the CRUD/batch orchestration) so each partial stays
    // focused on one responsibility.
    public partial class DbContext
    {
        internal static Type? GetParameterKnownType(Column col, object? providerValue)
            => providerValue?.GetType() ?? (col.Converter == null ? col.Prop.PropertyType : null);

        // TODO: Consider replacing the tuple array with parallel name[] and value[] arrays
        // to reduce per-element overhead. ValueTuple<string, object> boxes the object on every
        // iteration. Two flat arrays (string[] names, object[] values) would avoid the tuple
        // allocation and improve cache locality for the SetParametersFast hot path.
        private IReadOnlyDictionary<string, object> AddParametersFast(DbCommand cmd, object[] parameters)
        {
            var span = new (string name, object value)[parameters.Length];
            var hasProviderParameters = false;
            for (int i = 0; i < parameters.Length; i++)
            {
                var name = $"{_p.ParamPrefix}p{i}";
                var value = parameters[i] ?? DBNull.Value;
                span[i] = (name, value);
                hasProviderParameters |= value is DbParameter;
            }

            if (hasProviderParameters)
            {
                cmd.Parameters.Clear();
                var providerParameterDict = new Dictionary<string, object>(parameters.Length);

                foreach (var (name, value) in span)
                {
                    if (value is DbParameter providerParameter)
                    {
                        providerParameter.ParameterName = name;
                        cmd.Parameters.Add(providerParameter);
                        providerParameterDict[name] = providerParameter.Value ?? DBNull.Value;
                    }
                    else
                    {
                        var parameter = cmd.CreateParameter();
                        parameter.ParameterName = name;
                        nORM.Query.ParameterAssign.AssignValue(parameter, value);
                        cmd.Parameters.Add(parameter);
                        providerParameterDict[name] = value;
                    }
                }

                return providerParameterDict;
            }

            cmd.SetParametersFast(span);

            // Gate B fix: Always populate the parameter dictionary so that ValidateRawSql
            // receives accurate parameter metadata regardless of logging state.
            // The dictionary is required for validation (not just logging) - decoupling the
            // two concerns ensures parameterized queries are never incorrectly flagged by
            // the validator when debug logging is disabled.
            if (parameters.Length == 0)
                return EmptyDictionary<string, object>.Instance;

            var dict = new Dictionary<string, object>(parameters.Length);
            foreach (var (name, value) in span) dict[name] = value;
            return dict;
        }

        internal static class EmptyDictionary<TKey, TValue> where TKey : notnull
        {
            public static readonly IReadOnlyDictionary<TKey, TValue> Instance = new Dictionary<TKey, TValue>(0);
        }

        private readonly struct CommandScope : IAsyncDisposable
        {
            private readonly DbConnection _connection;
            private readonly DbTransaction? _transaction;
            public CommandScope(DbConnection connection, DbTransaction? transaction)
            {
                _connection = connection;
                _transaction = transaction;
            }

            /// <summary>
            /// Creates a <see cref="DbCommand"/> tied to the scoped connection and transaction.
            /// </summary>
            /// <returns>A configured command ready for parameter population and execution.</returns>
            public DbCommand CreateCommand()
            {
                var cmd = _connection.CreateCommand();
                if (_transaction != null)
                    cmd.Transaction = _transaction;
                return cmd;
            }
            /// <summary>
            /// Disposes the command scope. For pooled connections no additional
            /// cleanup is required so a completed <see cref="ValueTask"/> is returned.
            /// </summary>
            /// <returns>A completed task representing the asynchronous dispose operation.</returns>
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
