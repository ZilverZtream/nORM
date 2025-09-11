using System.Data.Common;

#nullable enable

namespace nORM.Migration
{
    /// <summary>
    /// Represents a discrete database schema change that can be applied or rolled back.
    /// Migrations are executed in version order to evolve the database alongside the
    /// application's model.
    /// </summary>
    public abstract class Migration
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Migration"/> class.
        /// </summary>
        /// <param name="version">Monotonically increasing version number of the migration.</param>
        /// <param name="name">Human-readable name describing the migration.</param>
        protected Migration(long version, string name)
        {
            Version = version;
            Name = name;
        }

        /// <summary>
        /// Gets the unique version number that orders migrations.
        /// </summary>
        public long Version { get; }

        /// <summary>
        /// Gets the descriptive name of the migration.
        /// </summary>
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