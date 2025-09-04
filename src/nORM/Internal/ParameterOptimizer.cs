using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;

#nullable enable

namespace nORM.Internal
{
    internal static class ParameterOptimizer
    {
        private static readonly ConcurrentDictionary<Type, DbType> _typeMap = new()
        {
            [typeof(int)] = DbType.Int32,
            [typeof(long)] = DbType.Int64,
            [typeof(string)] = DbType.String,
            [typeof(DateTime)] = DbType.DateTime2,
            [typeof(bool)] = DbType.Boolean,
            [typeof(decimal)] = DbType.Decimal,
            [typeof(Guid)] = DbType.Guid
        };

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

        public static void AddParam(this DbCommand cmd, string name, object? value)
            => AddOptimizedParam(cmd, name, value, null);
    }
}
