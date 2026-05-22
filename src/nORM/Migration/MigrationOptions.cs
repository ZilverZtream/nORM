using System;
using System.Text.RegularExpressions;

namespace nORM.Migration
{
    /// <summary>
    /// Configures provider migration runners.
    /// </summary>
    public sealed class MigrationOptions
    {
        private static readonly Regex SafeIdentifierPattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

        /// <summary>
        /// Initializes migration runner options.
        /// </summary>
        /// <param name="historyTableName">Name of the migration history table. Defaults to <c>__NormMigrationsHistory</c>.</param>
        /// <param name="lockName">Name of the migration advisory lock for providers that use named locks. Defaults to <c>__NormMigrationsLock</c>.</param>
        /// <param name="lockTimeout">Maximum time to wait for advisory-lock acquisition. Defaults to 30 seconds.</param>
        /// <param name="postgresAdvisoryLockKey">PostgreSQL advisory-lock key. Defaults to nORM's stable key.</param>
        public MigrationOptions(
            string? historyTableName = null,
            string? lockName = null,
            TimeSpan? lockTimeout = null,
            long? postgresAdvisoryLockKey = null)
        {
            HistoryTableName = ValidateIdentifier(historyTableName ?? "__NormMigrationsHistory", nameof(historyTableName));
            LockName = ValidateLockName(lockName ?? "__NormMigrationsLock", nameof(lockName));
            LockTimeout = ValidateLockTimeout(lockTimeout ?? TimeSpan.FromSeconds(30), nameof(lockTimeout));
            PostgresAdvisoryLockKey = postgresAdvisoryLockKey ?? unchecked((long)0x62C3B8F921A4D507L);
        }

        /// <summary>
        /// Name of the migration history table.
        /// </summary>
        public string HistoryTableName { get; }

        /// <summary>
        /// Name of the advisory lock used by SQL Server and MySQL migration runners.
        /// </summary>
        public string LockName { get; }

        /// <summary>
        /// Maximum time to wait for migration advisory-lock acquisition.
        /// </summary>
        public TimeSpan LockTimeout { get; }

        /// <summary>
        /// Stable PostgreSQL advisory-lock key used by the PostgreSQL migration runner.
        /// </summary>
        public long PostgresAdvisoryLockKey { get; }

        internal int LockTimeoutMilliseconds => checked((int)Math.Ceiling(LockTimeout.TotalMilliseconds));

        internal int LockTimeoutSeconds => checked((int)Math.Ceiling(LockTimeout.TotalSeconds));

        private static string ValidateIdentifier(string value, string parameterName)
        {
            if (string.IsNullOrWhiteSpace(value))
                throw new ArgumentException("Migration history table name cannot be empty.", parameterName);

            if (!SafeIdentifierPattern.IsMatch(value))
                throw new ArgumentException("Migration history table name must contain only letters, digits, and underscores, and cannot start with a digit.", parameterName);

            return value;
        }

        private static string ValidateLockName(string value, string parameterName)
        {
            if (string.IsNullOrWhiteSpace(value))
                throw new ArgumentException("Migration advisory lock name cannot be empty.", parameterName);

            foreach (var ch in value)
            {
                if (!(char.IsLetterOrDigit(ch) || ch is '_' or '-' or ':' or '.'))
                    throw new ArgumentException("Migration advisory lock name may contain only letters, digits, underscores, hyphens, colons, and periods.", parameterName);
            }

            return value;
        }

        private static TimeSpan ValidateLockTimeout(TimeSpan value, string parameterName)
        {
            if (value <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(parameterName, value, "Migration advisory lock timeout must be greater than zero.");

            if (value.TotalMilliseconds > int.MaxValue)
                throw new ArgumentOutOfRangeException(parameterName, value, "Migration advisory lock timeout is too large.");

            return value;
        }
    }
}
