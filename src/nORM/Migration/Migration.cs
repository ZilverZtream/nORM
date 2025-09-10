using System.Data.Common;

#nullable enable

namespace nORM.Migration
{
    public abstract class Migration
    {
        protected Migration(long version, string name)
        {
            Version = version;
            Name = name;
        }
        
        public long Version { get; }
        public string Name { get; }
        
        /// <summary>
        /// Applies the migration by bringing the database schema to the new state.
        /// </summary>
        /// <param name="connection">An open connection to the target database.</param>
        /// <param name="transaction">The transaction within which the migration should run.</param>
        public abstract void Up(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Reverts the migration, returning the database schema to the previous state.
        /// </summary>
        /// <param name="connection">An open connection to the target database.</param>
        /// <param name="transaction">The transaction within which the rollback should run.</param>
        public abstract void Down(DbConnection connection, DbTransaction transaction);
    }
}