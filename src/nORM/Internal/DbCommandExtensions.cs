using System;
using System.Data.Common;
using nORM.Query;

namespace nORM.Internal
{
    internal static class DbCommandExtensions
    {
        public static void AddParamsStackAlloc(this DbCommand cmd, ReadOnlySpan<(string name, object value)> parameters)
        {
            Span<DbParameter> paramSpan = stackalloc DbParameter[parameters.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = parameters[i].name;
                ParameterAssign.AssignValue(param, parameters[i].value);
                cmd.Parameters.Add(param);
            }
        }
    }
}
