using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace nORM.Core
{
    /// <summary>
    /// Represents one provider-neutral temporal history row for an entity.
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity.</typeparam>
    public sealed class TemporalHistoryEntry<T> where T : class
    {
        /// <summary>
        /// Initializes a new temporal history entry.
        /// </summary>
        /// <param name="entity">The historical entity snapshot.</param>
        /// <param name="validFrom">Timestamp when this version became valid.</param>
        /// <param name="validTo">Timestamp when this version stopped being valid.</param>
        /// <param name="operation">Provider-neutral operation marker: I, U, or D.</param>
        public TemporalHistoryEntry(T entity, DateTime validFrom, DateTime validTo, string operation)
        {
            Entity = entity ?? throw new ArgumentNullException(nameof(entity));
            ValidFrom = validFrom;
            ValidTo = validTo;
            Operation = operation ?? throw new ArgumentNullException(nameof(operation));
        }

        /// <summary>Historical entity snapshot stored in the history table.</summary>
        public T Entity { get; }

        /// <summary>Timestamp when this version became valid.</summary>
        public DateTime ValidFrom { get; }

        /// <summary>Timestamp when this version stopped being valid.</summary>
        public DateTime ValidTo { get; }

        /// <summary>Provider-neutral operation marker: I, U, or D.</summary>
        public string Operation { get; }
    }

    /// <summary>
    /// Represents one changed mapped property between two temporal history versions.
    /// </summary>
    public sealed class TemporalPropertyChange
    {
        /// <summary>
        /// Initializes a temporal property change.
        /// </summary>
        /// <param name="propertyName">Mapped CLR property name.</param>
        /// <param name="previousValue">Value in the previous temporal version.</param>
        /// <param name="currentValue">Value in the current temporal version.</param>
        public TemporalPropertyChange(string propertyName, object? previousValue, object? currentValue)
        {
            PropertyName = propertyName ?? throw new ArgumentNullException(nameof(propertyName));
            PreviousValue = previousValue;
            CurrentValue = currentValue;
        }

        /// <summary>Mapped CLR property name.</summary>
        public string PropertyName { get; }

        /// <summary>Value in the previous temporal version.</summary>
        public object? PreviousValue { get; }

        /// <summary>Value in the current temporal version.</summary>
        public object? CurrentValue { get; }
    }

    /// <summary>
    /// Represents changed mapped properties between two consecutive temporal history versions.
    /// </summary>
    /// <typeparam name="T">Type of the mapped entity.</typeparam>
    public sealed class TemporalDiffEntry<T> where T : class
    {
        /// <summary>
        /// Initializes a temporal diff entry.
        /// </summary>
        /// <param name="previous">Previous temporal history version.</param>
        /// <param name="current">Current temporal history version.</param>
        /// <param name="changes">Changed mapped properties.</param>
        public TemporalDiffEntry(
            TemporalHistoryEntry<T> previous,
            TemporalHistoryEntry<T> current,
            IReadOnlyList<TemporalPropertyChange> changes)
        {
            Previous = previous ?? throw new ArgumentNullException(nameof(previous));
            Current = current ?? throw new ArgumentNullException(nameof(current));
            Changes = changes ?? throw new ArgumentNullException(nameof(changes));
        }

        /// <summary>Previous temporal history version.</summary>
        public TemporalHistoryEntry<T> Previous { get; }

        /// <summary>Current temporal history version.</summary>
        public TemporalHistoryEntry<T> Current { get; }

        /// <summary>Changed mapped properties.</summary>
        public IReadOnlyList<TemporalPropertyChange> Changes { get; }
    }

    /// <summary>
    /// Provides helper methods for querying temporal tables at specific points in time.
    /// </summary>
    [RequiresDynamicCode("nORM temporal extensions emit reflection-built LINQ expressions; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [RequiresUnreferencedCode("nORM temporal extensions reflect over LINQ method metadata; trimming may remove the required members.")]
    public static class TemporalExtensions
    {
        /// <summary>
        /// Creates a query that returns entity states as of the specified timestamp.
        /// </summary>
        /// <typeparam name="T">Type of the entity.</typeparam>
        /// <param name="source">Queryable source representing a temporal table.</param>
        /// <param name="timestamp">Point in time to query.</param>
        /// <returns>A queryable that yields entity values at the given timestamp.</returns>
        public static IQueryable<T> AsOf<T>(this IQueryable<T> source, DateTime timestamp)
        {
            ArgumentNullException.ThrowIfNull(source);
            var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(T));
            var call = Expression.Call(null, method, source.Expression, Expression.Constant(timestamp));
            return source.Provider.CreateQuery<T>(call);
        }

        /// <summary>
        /// Creates a query that uses a temporal history table tag to filter results.
        /// </summary>
        /// <typeparam name="T">Type of the entity.</typeparam>
        /// <param name="source">Queryable source representing a temporal table.</param>
        /// <param name="tagName">Name of the history tag to apply.</param>
        /// <returns>A queryable filtered by the specified temporal tag.</returns>
        public static IQueryable<T> AsOf<T>(this IQueryable<T> source, string tagName)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentException.ThrowIfNullOrWhiteSpace(tagName);
            var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(T));
            var call = Expression.Call(null, method, source.Expression, Expression.Constant(tagName));
            return source.Provider.CreateQuery<T>(call);
        }
    }
}
