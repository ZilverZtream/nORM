using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        private static void EnsureWritableMapping(TableMapping map, string operation)
        {
            if (!map.IsReadOnly)
                return;

            throw new NormUnsupportedFeatureException(
                $"{operation} for '{map.Type.Name}' is not supported because the entity is configured as read-only/query-only. " +
                "Use Query<T>() or raw SQL query APIs for read access, and map a keyed writable table for generated writes.");
        }
    }
}
