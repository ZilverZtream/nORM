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

        /// <summary>Synchronous <see cref="EnsureCreatedAsync"/> (EF Core's <c>EnsureCreated</c>).</summary>
        /// <returns><c>true</c> if any table was created; otherwise <c>false</c>.</returns>
        [RequiresDynamicCode("EnsureCreated builds a schema snapshot by reflecting over entity mappings; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("EnsureCreated reflects over entity types and their mapping metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public bool EnsureCreated()
        {
            _context.EnsureConnection();

            var target = SchemaSnapshotBuilder.Build(_context);
            if (target.Tables.Count == 0)
                return false;

            var missing = new List<TableSchema>();
            foreach (var table in target.Tables)
                if (!TableExists(table.Name))
                    missing.Add(table);
            if (missing.Count == 0)
                return false;

            var ordered = OrderByForeignKeyDependencies(missing);
            var diff = new SchemaDiff();
            diff.AddedTables.AddRange(ordered);

            var statements = CreateMigrationSqlGenerator().GenerateSql(diff);
            foreach (var sql in EnumerateUpStatements(statements))
                ExecuteNonQuery(sql);

            return true;
        }

        /// <summary>
        /// Drops the tables for the context's mapped entities if they exist — the inverse of
        /// <see cref="EnsureCreatedAsync"/> and the model-scoped equivalent of EF Core's <c>EnsureDeleted</c>.
        /// Like <see cref="EnsureCreatedAsync"/> it operates only on the entities configured in the fluent
        /// model, dropping dependents before principals so foreign keys do not block the drop. Returns
        /// <c>true</c> when at least one table was dropped, <c>false</c> when none existed.
        /// </summary>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns><c>true</c> if any table was dropped; otherwise <c>false</c>.</returns>
        [RequiresDynamicCode("EnsureDeleted builds a schema snapshot by reflecting over entity mappings; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("EnsureDeleted reflects over entity types and their mapping metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public async Task<bool> EnsureDeletedAsync(CancellationToken ct = default)
        {
            await _context.EnsureConnectionAsync(ct).ConfigureAwait(false);

            var target = SchemaSnapshotBuilder.Build(_context);
            if (target.Tables.Count == 0)
                return false;

            var existing = new List<TableSchema>();
            foreach (var table in target.Tables)
                if (await TableExistsAsync(table.Name, ct).ConfigureAwait(false))
                    existing.Add(table);
            if (existing.Count == 0)
                return false;

            // Drop dependents before principals (reverse of the create order) so an inline FK constraint
            // does not block dropping a referenced table on strict providers.
            var ordered = OrderByForeignKeyDependencies(existing);
            ordered.Reverse();

            var diff = new SchemaDiff();
            diff.DroppedTables.AddRange(ordered);

            var statements = CreateMigrationSqlGenerator().GenerateSql(diff);
            foreach (var sql in EnumerateUpStatements(statements))
                await ExecuteNonQueryAsync(sql, ct).ConfigureAwait(false);

            return true;
        }

        /// <summary>Synchronous <see cref="EnsureDeletedAsync"/> (EF Core's <c>EnsureDeleted</c>).</summary>
        /// <returns><c>true</c> if any table was dropped; otherwise <c>false</c>.</returns>
        [RequiresDynamicCode("EnsureDeleted builds a schema snapshot by reflecting over entity mappings; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("EnsureDeleted reflects over entity types and their mapping metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public bool EnsureDeleted()
        {
            _context.EnsureConnection();

            var target = SchemaSnapshotBuilder.Build(_context);
            if (target.Tables.Count == 0)
                return false;

            var existing = new List<TableSchema>();
            foreach (var table in target.Tables)
                if (TableExists(table.Name))
                    existing.Add(table);
            if (existing.Count == 0)
                return false;

            var ordered = OrderByForeignKeyDependencies(existing);
            ordered.Reverse();

            var diff = new SchemaDiff();
            diff.DroppedTables.AddRange(ordered);

            var statements = CreateMigrationSqlGenerator().GenerateSql(diff);
            foreach (var sql in EnumerateUpStatements(statements))
                ExecuteNonQuery(sql);

            return true;
        }

        /// <summary>
        /// Returns the DDL that would create the tables for the context's mapped entities, as a single
        /// script — matching Entity Framework Core's <c>Database.GenerateCreateScript</c>. This GENERATES the
        /// SQL only: it opens no connection and executes nothing, so it is the tool for inspecting, diffing,
        /// or exporting the schema the fluent model (<c>DbContextOptions.OnModelCreating</c>) implies. Tables
        /// are ordered so a principal precedes its dependents (inline foreign keys). Returns an empty string
        /// when the model configures no tables.
        /// </summary>
        /// <returns>The create-table DDL for every mapped entity, one statement per line.</returns>
        [RequiresDynamicCode("GenerateCreateScript builds a schema snapshot by reflecting over entity mappings; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("GenerateCreateScript reflects over entity types and their mapping metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public string GenerateCreateScript()
        {
            var target = SchemaSnapshotBuilder.Build(_context);
            if (target.Tables.Count == 0)
                return string.Empty;

            var ordered = OrderByForeignKeyDependencies(new List<TableSchema>(target.Tables));
            var diff = new SchemaDiff();
            diff.AddedTables.AddRange(ordered);

            var statements = CreateMigrationSqlGenerator().GenerateSql(diff);
            var sb = new System.Text.StringBuilder();
            foreach (var sql in EnumerateUpStatements(statements))
            {
                var trimmed = sql.TrimEnd();
                if (trimmed.Length == 0)
                    continue;
                sb.Append(trimmed);
                if (!trimmed.EndsWith(";", StringComparison.Ordinal))
                    sb.Append(';');
                sb.Append('\n');
            }
            return sb.ToString();
        }

        /// <summary>
        /// Tests whether the database is reachable by opening the connection and running a trivial probe —
        /// matching EF Core's <c>Database.CanConnectAsync</c>. Returns <c>false</c> (rather than throwing)
        /// when the connection cannot be opened or the probe fails.
        /// </summary>
        /// <param name="ct">Token used to cancel the operation.</param>
        public async Task<bool> CanConnectAsync(CancellationToken ct = default)
        {
            try
            {
                await _context.EnsureConnectionAsync(ct).ConfigureAwait(false);
                using var cmd = _context.CreateCommand();
                cmd.CommandText = "SELECT 1";
                await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return false;
            }
        }

        /// <summary>Synchronous <see cref="CanConnectAsync"/>.</summary>
        public bool CanConnect()
        {
            try
            {
                _context.EnsureConnection();
                using var cmd = _context.CreateCommand();
                cmd.CommandText = "SELECT 1";
                cmd.ExecuteScalar();
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return false;
            }
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

        private bool TableExists(string tableName)
        {
            try
            {
                using var cmd = _context.CreateCommand();
                cmd.CommandText = $"SELECT 1 FROM {_context.RawProvider.Escape(tableName)} WHERE 1 = 0";
                cmd.ExecuteScalar();
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

        private void ExecuteNonQuery(string sql)
        {
            using var cmd = _context.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
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
        /// Executes a raw non-query SQL command (INSERT/UPDATE/DELETE/DDL) against the database and
        /// returns the number of rows affected — matching EF Core's <c>Database.ExecuteSqlRawAsync</c>.
        /// Pass values as <paramref name="parameters"/> (referenced positionally in the SQL) rather than
        /// string-concatenating them, to avoid SQL injection. Participates in the active transaction.
        /// </summary>
        /// <param name="sql">The non-query SQL to execute.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <param name="parameters">Parameter values referenced by the SQL.</param>
        public Task<int> ExecuteSqlRawAsync(string sql, CancellationToken ct = default, params object[] parameters)
            => _context.ExecuteSqlRawCoreAsync(sql, parameters ?? Array.Empty<object>(), ct);

        /// <summary>Synchronous <see cref="ExecuteSqlRawAsync(string, CancellationToken, object[])"/>.</summary>
        public int ExecuteSqlRaw(string sql, params object[] parameters)
            => _context.ExecuteSqlRawCore(sql, parameters ?? Array.Empty<object>());

        /// <summary>
        /// Executes interpolated non-query SQL, turning each interpolation hole into a database parameter
        /// before execution — matching EF Core's <c>Database.ExecuteSqlInterpolatedAsync</c>.
        /// </summary>
        /// <param name="sql">The interpolated non-query SQL; interpolation holes become parameters.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        public Task<int> ExecuteSqlInterpolatedAsync(FormattableString sql, CancellationToken ct = default)
            => _context.ExecuteSqlInterpolatedCoreAsync(sql, ct);

        /// <summary>Synchronous <see cref="ExecuteSqlInterpolatedAsync"/>.</summary>
        public int ExecuteSqlInterpolated(FormattableString sql)
            => _context.ExecuteSqlInterpolatedCore(sql);

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

        /// <summary>
        /// Synchronous <see cref="BeginTransactionAsync"/>, matching Entity Framework Core's
        /// <c>BeginTransaction</c>. Begins a new database transaction for the current context and returns a
        /// nORM-managed wrapper — commit or roll it back through the returned <see cref="DbContextTransaction"/>.
        /// </summary>
        /// <returns>A <see cref="DbContextTransaction"/> representing the started transaction.</returns>
        /// <exception cref="NormUsageException">Thrown when a transaction is already active.</exception>
        public DbContextTransaction BeginTransaction()
        {
            if (_context.CurrentTransaction != null)
                throw new NormUsageException("A transaction is already active.");
            _context.EnsureConnection();
            var transaction = _context.RawConnection.BeginTransaction();
            var contextTransaction = new DbContextTransaction(transaction, _context);
            _context.SetCurrentTransaction(transaction, contextTransaction);
            return contextTransaction;
        }

        /// <summary>
        /// Returns the underlying provider <see cref="DbConnection"/> the context uses, matching Entity
        /// Framework Core's <c>GetDbConnection</c> — the escape hatch for interop with Dapper or raw ADO.NET
        /// on the same connection (and any active transaction, via <see cref="CurrentTransaction"/>). Because
        /// it exposes a provider-specific handle it is disallowed under strict provider mobility.
        /// </summary>
        /// <returns>The context's <see cref="DbConnection"/> (not opened by this call).</returns>
        public DbConnection GetDbConnection()
        {
            _context.ThrowIfStrictProviderMobilityEscapeHatch(nameof(GetDbConnection));
            return _context.RawConnection;
        }
    }
}
