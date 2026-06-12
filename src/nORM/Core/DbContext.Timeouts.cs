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
        /// <summary>
        /// Converts a <see cref="TimeSpan"/> to an integer number of seconds, clamping at
        /// <see cref="int.MaxValue"/> to prevent overflow when very large or maximum timeouts
        /// are provided. Negative results are raised to a minimum of 1.
        /// </summary>
        private static int ToSecondsClamped(TimeSpan t)
        {
            // Check for overflow before casting
            if (t.TotalSeconds > int.MaxValue)
                return int.MaxValue;

            return Math.Max(1, (int)Math.Ceiling(t.TotalSeconds));
        }
    }
}
