using System;
using System.Data.Common;
using System.Runtime.CompilerServices;
using nORM.Query;

namespace nORM.Internal
{
    internal static class DbCommandExtensions
    {
        // PERFORMANCE OPTIMIZATION 5: Aggressive inlining for parameter setup hot paths
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static void AddParamsStackAlloc(this DbCommand cmd, ReadOnlySpan<(string name, object value)> parameters)
        {
            // PERFORMANCE OPTIMIZATION 6: Reuse parameter objects when possible
            var cmdParams = cmd.Parameters;
            int paramCount = parameters.Length;

            for (int i = 0; i < paramCount; i++)
            {
                DbParameter param;

                // Reuse existing parameter if available
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

            // Remove excess parameters if we have fewer than before
            while (cmdParams.Count > paramCount)
            {
                cmdParams.RemoveAt(cmdParams.Count - 1);
            }
        }

        // PERFORMANCE OPTIMIZATION 7: Aggressive inlining + avoid RemoveAt calls in hot path
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static void SetParametersFast(this DbCommand cmd, ReadOnlySpan<(string name, object value)> parameters)
        {
            var cmdParams = cmd.Parameters;
            int paramCount = parameters.Length;

            // PERFORMANCE OPTIMIZATION 8: Minimize virtual calls by caching count
            int existingCount = cmdParams.Count;

            for (int i = 0; i < paramCount; i++)
            {
                DbParameter param;
                if (i < existingCount)
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

            // PERFORMANCE OPTIMIZATION 9: Batch removal from the end (more efficient)
            while (cmdParams.Count > paramCount)
            {
                cmdParams.RemoveAt(cmdParams.Count - 1);
            }
        }
    }
}
