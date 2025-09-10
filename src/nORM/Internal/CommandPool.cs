using System;
using System.Data;
using System.Data.Common;

namespace nORM.Internal
{
    internal sealed class CommandPool
    {
        // Removed thread-static caching. Commands are cheap to create and callers typically dispose them.

        /// <summary>
        /// Creates a new <see cref="DbCommand"/> for the specified SQL statement using the provided
        /// connection. The command is returned with a clean parameter collection and configured for
        /// text execution.
        /// </summary>
        /// <param name="connection">The open database connection from which the command is created.</param>
        /// <param name="sql">The SQL text that the command should execute.</param>
        /// <returns>A freshly instantiated <see cref="DbCommand"/> ready for parameterization and execution.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> or
        /// <paramref name="sql"/> is <c>null</c>.</exception>
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
