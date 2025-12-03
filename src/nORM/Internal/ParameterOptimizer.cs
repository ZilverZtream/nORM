using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;

#nullable enable

namespace nORM.Internal
{
    /// <summary>
    /// PERFORMANCE OPTIMIZATION: Expanded type map from 7 to 25+ types.
    /// Reduces fallback to DbType.Object which can cause inefficient query plans.
    /// </summary>
    internal static class ParameterOptimizer
    {
        private static readonly ConcurrentDictionary<Type, DbType> _typeMap = new()
        {
            // Original 7 types
            [typeof(int)] = DbType.Int32,
            [typeof(long)] = DbType.Int64,
            [typeof(string)] = DbType.String,
            [typeof(DateTime)] = DbType.DateTime2,
            [typeof(bool)] = DbType.Boolean,
            [typeof(decimal)] = DbType.Decimal,
            [typeof(Guid)] = DbType.Guid,

            // Additional integer types
            [typeof(short)] = DbType.Int16,
            [typeof(byte)] = DbType.Byte,
            [typeof(sbyte)] = DbType.SByte,
            [typeof(ushort)] = DbType.UInt16,
            [typeof(uint)] = DbType.UInt32,
            [typeof(ulong)] = DbType.UInt64,

            // Additional floating point types
            [typeof(float)] = DbType.Single,
            [typeof(double)] = DbType.Double,

            // Date/time types
            [typeof(DateTimeOffset)] = DbType.DateTimeOffset,
            [typeof(TimeSpan)] = DbType.Time,

            // Binary data
            [typeof(byte[])] = DbType.Binary,

            // Nullable versions of common types
            [typeof(int?)] = DbType.Int32,
            [typeof(long?)] = DbType.Int64,
            [typeof(DateTime?)] = DbType.DateTime2,
            [typeof(bool?)] = DbType.Boolean,
            [typeof(decimal?)] = DbType.Decimal,
            [typeof(Guid?)] = DbType.Guid,
            [typeof(short?)] = DbType.Int16,
            [typeof(byte?)] = DbType.Byte,
            [typeof(float?)] = DbType.Single,
            [typeof(double?)] = DbType.Double,
            [typeof(DateTimeOffset?)] = DbType.DateTimeOffset,
            [typeof(TimeSpan?)] = DbType.Time
        };

        /// <summary>
        /// Adds a parameter to the command, attempting to infer the optimal <see cref="DbType"/> and
        /// size based on the supplied value. When a <paramref name="knownType"/> is provided and the
        /// value is <c>null</c>, the mapping is still applied to avoid provider ambiguity.
        /// </summary>
        /// <param name="cmd">The command to which the parameter is added.</param>
        /// <param name="name">The parameter name including prefix (e.g. <c>@Id</c>).</param>
        /// <param name="value">The value to bind to the parameter.</param>
        /// <param name="knownType">Optional type hint used when <paramref name="value"/> is <c>null</c>.</param>
        public static void AddOptimizedParam(this DbCommand cmd, string name, object? value, Type? knownType = null)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = name;

            if (value == null)
            {
                param.Value = DBNull.Value;
                param.DbType = knownType != null && _typeMap.TryGetValue(knownType, out var dbType)
                    ? dbType
                    : DbType.Object;
            }
            else
            {
                param.Value = value;
                var valueType = value.GetType();

                if (_typeMap.TryGetValue(valueType, out var mappedType))
                {
                    param.DbType = mappedType;

                    if (mappedType == DbType.String && value is string str)
                    {
                        param.Size = str.Length <= 4000 ? str.Length : -1;
                    }
                }
            }

            cmd.Parameters.Add(param);
        }

        /// <summary>
        /// Adds a parameter without additional type metadata by delegating to
        /// <see cref="AddOptimizedParam(DbCommand,string,object?,Type?)"/>.
        /// </summary>
        /// <param name="cmd">The command to which the parameter is added.</param>
        /// <param name="name">The parameter name including prefix.</param>
        /// <param name="value">The value to bind to the parameter.</param>
        public static void AddParam(this DbCommand cmd, string name, object? value)
            => AddOptimizedParam(cmd, name, value, null);
    }
}
