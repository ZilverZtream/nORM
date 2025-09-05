using System.Collections.Generic;
using System.Data.Common;
using nORM.Providers;
using nORM.Internal;

#nullable enable

namespace nORM.Core
{
    internal static class ParameterHelper
    {
        public static Dictionary<string, object> AddParameters(DatabaseProvider provider, DbCommand cmd, object[] parameters)
        {
            var paramDict = new Dictionary<string, object>();
            for (int i = 0; i < parameters.Length; i++)
            {
                var pName = $"{provider.ParamPrefix}p{i}";
                cmd.AddParam(pName, parameters[i]);
                paramDict[pName] = parameters[i];
            }

            return paramDict;
        }
    }
}
