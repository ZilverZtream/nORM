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
    /// MG-2: Statements are now split into pre-transaction, transactional, and post-transaction
    /// segments. This allows providers like SQLite to execute connection-level statements (such as
    /// <c>PRAGMA foreign_keys=OFF/ON</c>) outside the migration transaction where required.
    /// </summary>
    /// <param name="Up">The transactional statements that upgrade the schema (excludes pre/post segments).</param>
    /// <param name="Down">The transactional statements that revert the schema (excludes pre/post segments).</param>
    /// <param name="PreTransactionUp">Statements to execute BEFORE beginning the Up transaction (e.g. PRAGMA foreign_keys=OFF). Null or empty when unused.</param>
    /// <param name="PostTransactionUp">Statements to execute AFTER committing the Up transaction (e.g. PRAGMA foreign_keys=ON). Null or empty when unused.</param>
    /// <param name="PreTransactionDown">Statements to execute BEFORE beginning the Down transaction. Null or empty when unused.</param>
    /// <param name="PostTransactionDown">Statements to execute AFTER committing the Down transaction. Null or empty when unused.</param>
    public record MigrationSqlStatements(
        IReadOnlyList<string> Up,
        IReadOnlyList<string> Down,
        IReadOnlyList<string>? PreTransactionUp = null,
        IReadOnlyList<string>? PostTransactionUp = null,
        IReadOnlyList<string>? PreTransactionDown = null,
        IReadOnlyList<string>? PostTransactionDown = null);
}
