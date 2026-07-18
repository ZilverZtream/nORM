using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Migration;
using nORM.Providers;

namespace nORM.Core
{
    /// <summary>
    /// Provides access to database-specific functionality for a <see cref="DbContext"/>,
    /// such as transaction management.
    /// </summary>
    public class DatabaseFacade
    {
        private readonly DbContext _context;

        internal DatabaseFacade(DbContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        /// <summary>
        /// Ensures the tables for the context's mapped entities exist, creating any that are
        /// missing — matching Entity Framework Core's <c>EnsureCreated</c>. This is the quick
        /// "make the schema" path for prototyping and tests; production schema management belongs
        /// in migrations (<c>dotnet norm migrations</c>). The target schema is derived from the
        /// fluent model (<c>DbContextOptions.OnModelCreating</c>), so only entities
        /// configured there are created. Returns <c>true</c> when at least one table was created,
        /// <c>false</c> when every mapped table already existed.
        /// </summary>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns><c>true</c> if any table was created; otherwise <c>false</c>.</returns>
        [RequiresDynamicCode("EnsureCreated builds a schema snapshot by reflecting over entity mappings; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("EnsureCreated reflects over entity types and their mapping metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public async Task<bool> EnsureCreatedAsync(CancellationToken ct = default)
        {
            await _context.EnsureConnectionAsync(ct).ConfigureAwait(false);

            var target = SchemaSnapshotBuilder.Build(_context);
            if (target.Tables.Count == 0)
                return false;

            // Idempotency: only create tables that are not already present. The probe is portable
            // (a zero-row SELECT succeeds when the table exists and throws when it does not), so no
            // provider-specific existence SQL is needed; a create that then fails for a real reason
            // still surfaces honestly.
            var missing = new List<TableSchema>();
            foreach (var table in target.Tables)
                if (!await TableExistsAsync(table.Name, ct).ConfigureAwait(false))
                    missing.Add(table);
            if (missing.Count == 0)
                return false;

            // Inline FK constraints require the referenced table to already exist on strict
            // providers, so create a principal before its dependents. (SQLite defers FK checks past
            // CREATE, so it is order-agnostic — but a single order is correct everywhere.)
            var ordered = OrderByForeignKeyDependencies(missing);

            var diff = new SchemaDiff();
            diff.AddedTables.AddRange(ordered);

            var statements = CreateMigrationSqlGenerator().GenerateSql(diff);
            foreach (var sql in EnumerateUpStatements(statements))
                await ExecuteNonQueryAsync(sql, ct).ConfigureAwait(false);

            return true;
        }

        private async Task<bool> TableExistsAsync(string tableName, CancellationToken ct)
        {
            try
            {
                using var cmd = _context.CreateCommand();
                cmd.CommandText = $"SELECT 1 FROM {_context.RawProvider.Escape(tableName)} WHERE 1 = 0";
                await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return false;
            }
        }

        private async Task ExecuteNonQueryAsync(string sql, CancellationToken ct)
        {
            using var cmd = _context.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        [RequiresDynamicCode("Migration SQL generators emit provider DDL and are not NativeAOT-compatible.")]
        [RequiresUnreferencedCode("Migration SQL generators reflect over schema metadata; trimming may remove the required members.")]
        private IMigrationSqlGenerator CreateMigrationSqlGenerator() => _context.RawProvider switch
        {
            SqliteProvider => new SqliteMigrationSqlGenerator(),
            SqlServerProvider => new SqlServerMigrationSqlGenerator(),
            PostgresProvider => new PostgresMigrationSqlGenerator(),
            MySqlProvider => new MySqlMigrationSqlGenerator(),
            _ => throw new NormUnsupportedFeatureException(
                $"EnsureCreatedAsync does not support the provider '{_context.RawProvider.GetType().Name}'. " +
                "Use migrations for custom providers.")
        };

        private static IEnumerable<string> EnumerateUpStatements(MigrationSqlStatements statements)
        {
            if (statements.PreTransactionUp != null)
                foreach (var sql in statements.PreTransactionUp)
                    yield return sql;
            foreach (var sql in statements.Up)
                yield return sql;
            if (statements.PostTransactionUp != null)
                foreach (var sql in statements.PostTransactionUp)
                    yield return sql;
        }

        /// <summary>
        /// Post-order DFS over the FK graph so a referenced (principal) table is emitted before the
        /// tables that reference it. Self-references and dependency cycles are broken (emitted in
        /// encounter order) rather than looping.
        /// </summary>
        private static List<TableSchema> OrderByForeignKeyDependencies(List<TableSchema> tables)
        {
            var byName = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
            foreach (var t in tables)
                byName[t.Name] = t;

            var result = new List<TableSchema>(tables.Count);
            var done = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var onStack = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            void Visit(TableSchema table)
            {
                if (done.Contains(table.Name) || !onStack.Add(table.Name))
                    return;
                foreach (var fk in table.ForeignKeys)
                {
                    if (!string.Equals(fk.PrincipalTable, table.Name, StringComparison.OrdinalIgnoreCase)
                        && byName.TryGetValue(fk.PrincipalTable, out var principal))
                        Visit(principal);
                }
                onStack.Remove(table.Name);
                if (done.Add(table.Name))
                    result.Add(table);
            }

            foreach (var t in tables)
                Visit(t);
            return result;
        }

        /// <summary>
        /// Gets the active <see cref="DbTransaction"/> for the context, if one exists.
        /// </summary>
        public DbTransaction? CurrentTransaction
        {
            get
            {
                _context.ThrowIfStrictProviderMobilityEscapeHatch(nameof(CurrentTransaction));
                return _context.CurrentTransaction;
            }
        }

        /// <summary>
        /// Gets the active nORM-managed transaction wrapper for the context, if one exists.
        /// This property does not expose the underlying provider transaction handle and is
        /// safe to use with strict provider mobility.
        /// </summary>
        public DbContextTransaction? CurrentContextTransaction
            => _context.CurrentContextTransaction;

        /// <summary>
        /// Begins a new database transaction for the current context.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A <see cref="DbContextTransaction"/> representing the started transaction.</returns>
        /// <exception cref="InvalidOperationException">Thrown when a transaction is already active.</exception>
        public async Task<DbContextTransaction> BeginTransactionAsync(CancellationToken ct = default)
        {
            if (_context.CurrentTransaction != null)
                throw new NormUsageException("A transaction is already active.");
            await _context.EnsureConnectionAsync(ct).ConfigureAwait(false);
            var transaction = await _context.RawConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
            var contextTransaction = new DbContextTransaction(transaction, _context);
            _context.SetCurrentTransaction(transaction, contextTransaction);
            return contextTransaction;
        }
    }
}
