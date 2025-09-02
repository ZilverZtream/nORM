using System;
using System.Data;
using System.Data.Common;

#nullable enable

namespace nORM.Internal
{
    internal static class Extensions
    {
        public static void AddParam(this DbCommand cmd, string n, object? v)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = n;
            p.Value = v ?? DBNull.Value;
            cmd.Parameters.Add(p);
        }

    }
}