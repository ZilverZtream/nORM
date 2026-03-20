using System;
using System.Collections.Generic;
using System.Data.Common;
using nORM.Providers;
using nORM.Internal;

#nullable enable

namespace nORM.Core
{
    internal static class ParameterHelper
    {
        /// <summary>
        /// Adds the specified parameter values to the command using provider-specific naming.
        /// </summary>
        /// <param name="provider">Database provider supplying the parameter prefix.</param>
        /// <param name="cmd">Command to which parameters will be added.</param>
        /// <param name="parameters">Parameter values in positional order.</param>
        /// <returns>A dictionary mapping generated parameter names to their values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when any argument is <c>null</c>.</exception>
        public static Dictionary<string, object> AddParameters(DatabaseProvider provider, DbCommand cmd, object[] parameters)
        {
            ArgumentNullException.ThrowIfNull(provider);
            ArgumentNullException.ThrowIfNull(cmd);
            ArgumentNullException.ThrowIfNull(parameters);

            var paramDict = new Dictionary<string, object>(parameters.Length);
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
