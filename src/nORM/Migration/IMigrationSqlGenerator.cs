using System.Collections.Generic;

namespace nORM.Migration
{
    /// <summary>
    /// Defines a service capable of translating schema differences into database-specific SQL statements.
    /// </summary>
    public interface IMigrationSqlGenerator
    {
        /// <summary>
        /// Generates the SQL statements required to migrate the database from the old schema to the new schema.
        /// </summary>
        /// <param name="diff">The schema differences between the current database and the target model.</param>
        /// <returns>A collection of SQL statements for applying and reverting the schema changes.</returns>
        MigrationSqlStatements GenerateSql(SchemaDiff diff);
    }

    /// <summary>
    /// Represents the SQL statements required to apply a migration and to roll it back.
    /// </summary>
    /// <param name="Up">The statements that upgrade the schema to the new version.</param>
    /// <param name="Down">The statements that revert the schema to the previous version.</param>
    public record MigrationSqlStatements(IReadOnlyList<string> Up, IReadOnlyList<string> Down);
}
