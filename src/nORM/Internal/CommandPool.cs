using System;
using System.Data;
using System.Data.Common;

namespace nORM.Internal
{
    internal sealed class CommandPool
    {
        // Removed thread-static caching. Commands are cheap to create and callers typically dispose them.
        public static DbCommand Get(DbConnection connection, string sql)
        {
            if (connection is null) throw new ArgumentNullException(nameof(connection));
            if (sql is null) throw new ArgumentNullException(nameof(sql));

            var cmd = connection.CreateCommand();
            cmd.Parameters.Clear();
            cmd.CommandText = sql;
            cmd.CommandType = CommandType.Text;
            return cmd;
        }
    }
}
