using System;
using System.Data;
using System.Data.Common;

namespace nORM.Internal
{
    internal sealed class CommandPool
    {
        [ThreadStatic]
        private static DbCommand? _cachedCommand;

        public static DbCommand Get(DbConnection connection, string sql)
        {
            var cmd = _cachedCommand;
            if (cmd == null || cmd.Connection != connection)
            {
                cmd?.Dispose();
                cmd = connection.CreateCommand();
                _cachedCommand = cmd;
            }

            cmd.Parameters.Clear();
            cmd.CommandText = sql;
            cmd.CommandType = CommandType.Text;
            return cmd;
        }
    }
}
