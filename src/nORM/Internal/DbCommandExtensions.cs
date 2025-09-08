using System;
using System.Data.Common;
using nORM.Query;

namespace nORM.Internal
{
    internal static class DbCommandExtensions
    {
        public static void AddParamsStackAlloc(this DbCommand cmd, ReadOnlySpan<(string name, object value)> parameters)
        {
            for (int i = 0; i < parameters.Length; i++)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = parameters[i].name;
                ParameterAssign.AssignValue(param, parameters[i].value);
                cmd.Parameters.Add(param);
            }
        }

        public static void SetParametersFast(this DbCommand cmd, ReadOnlySpan<(string name, object value)> parameters)
        {
            var cmdParams = cmd.Parameters;

            for (int i = 0; i < parameters.Length; i++)
            {
                DbParameter param;
                if (i < cmdParams.Count)
                {
                    param = cmdParams[i];
                    param.ParameterName = parameters[i].name;
                }
                else
                {
                    param = cmd.CreateParameter();
                    param.ParameterName = parameters[i].name;
                    cmdParams.Add(param);
                }

                ParameterAssign.AssignValue(param, parameters[i].value);
            }

            while (cmdParams.Count > parameters.Length)
                cmdParams.RemoveAt(cmdParams.Count - 1);
        }
    }
}
