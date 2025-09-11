using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Compact aggregate extensions for nORM using numeric generics
    /// </summary>
    public static class AdvancedLinqExtensions
    {
        #region Sum

        /// <summary>
        /// Asynchronously computes the sum of the sequence of values obtained by
        /// applying the projection to each element of an <see cref="INormQueryable{TSource}"/>.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The nORM queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the computed sum.</returns>
        public static Task<TResult> SumAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Sum", ct);

        /// <summary>
        /// Asynchronously computes the sum of the sequence of nullable values
        /// obtained by applying the projection to each element of an <see cref="INormQueryable{TSource}"/>.
        /// Null values are ignored.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The nORM queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the computed sum or <c>null</c> if the sequence is empty.</returns>
        public static Task<TResult?> SumAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Sum", ct);

        /// <summary>
        /// Asynchronously computes the sum for an arbitrary <see cref="IQueryable{TSource}"/> backed by nORM.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the computed sum.</returns>
        public static Task<TResult> SumAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Sum", ct);

        /// <summary>
        /// Asynchronously computes the sum of nullable values for an arbitrary <see cref="IQueryable{TSource}"/> backed by nORM.
        /// Null values are ignored.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the computed sum or <c>null</c> if the sequence is empty.</returns>
        public static Task<TResult?> SumAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Sum", ct);

        #endregion

        #region Average

        /// <summary>
        /// Asynchronously computes the average of the sequence of values obtained by applying the projection to each element of an <see cref="INormQueryable{TSource}"/>.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The nORM queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the computed average.</returns>
        public static Task<TResult> AverageAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Average", ct);

        /// <summary>
        /// Asynchronously computes the average of the sequence of nullable values
        /// obtained by applying the projection to each element of an <see cref="INormQueryable{TSource}"/>.
        /// Null values are ignored.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The nORM queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the computed average or <c>null</c> if the sequence is empty.</returns>
        public static Task<TResult?> AverageAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Average", ct);

        /// <summary>
        /// Asynchronously computes the average for an arbitrary <see cref="IQueryable{TSource}"/> backed by nORM.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the computed average.</returns>
        public static Task<TResult> AverageAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Average", ct);

        /// <summary>
        /// Asynchronously computes the average of nullable values for an <see cref="IQueryable{TSource}"/> backed by nORM.
        /// Null values are ignored.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the computed average or <c>null</c> if the sequence is empty.</returns>
        public static Task<TResult?> AverageAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Average", ct);

        #endregion

        #region Min/Max

        /// <summary>
        /// Asynchronously computes the minimum value of the sequence obtained by applying the projection to each element of an <see cref="INormQueryable{TSource}"/>.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The nORM queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the minimum value.</returns>
        public static Task<TResult> MinAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Min", ct);

        /// <summary>
        /// Asynchronously computes the minimum of the sequence of nullable values obtained by applying the projection to each element of an <see cref="INormQueryable{TSource}"/>.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The nORM queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the minimum value or <c>null</c> if the sequence is empty.</returns>
        public static Task<TResult?> MinAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Min", ct);

        /// <summary>
        /// Asynchronously computes the minimum value for an arbitrary <see cref="IQueryable{TSource}"/> backed by nORM.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the minimum value.</returns>
        public static Task<TResult> MinAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Min", ct);

        /// <summary>
        /// Asynchronously computes the minimum of nullable values for an <see cref="IQueryable{TSource}"/> backed by nORM.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the minimum value or <c>null</c> if the sequence is empty.</returns>
        public static Task<TResult?> MinAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Min", ct);

        /// <summary>
        /// Asynchronously computes the maximum value of the sequence obtained by applying the projection to each element of an <see cref="INormQueryable{TSource}"/>.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The nORM queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the maximum value.</returns>
        public static Task<TResult> MaxAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Max", ct);

        /// <summary>
        /// Asynchronously computes the maximum of the sequence of nullable values obtained by applying the projection to each element of an <see cref="INormQueryable{TSource}"/>.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The nORM queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the maximum value or <c>null</c> if the sequence is empty.</returns>
        public static Task<TResult?> MaxAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Max", ct);

        /// <summary>
        /// Asynchronously computes the maximum value for an arbitrary <see cref="IQueryable{TSource}"/> backed by nORM.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the maximum value.</returns>
        public static Task<TResult> MaxAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Max", ct);

        /// <summary>
        /// Asynchronously computes the maximum of nullable values for an <see cref="IQueryable{TSource}"/> backed by nORM.
        /// </summary>
        /// <typeparam name="TSource">The element type of the queryable source.</typeparam>
        /// <typeparam name="TResult">The numeric type of the projection.</typeparam>
        /// <param name="source">The queryable source.</param>
        /// <param name="selector">A projection to apply to each element.</param>
        /// <param name="ct">A cancellation token for the operation.</param>
        /// <returns>A task containing the maximum value or <c>null</c> if the sequence is empty.</returns>
        public static Task<TResult?> MaxAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Max", ct);

        #endregion

        #region Helpers

        private static Task<TResult> ExecuteAggregateAsync<TSource, TResult>(INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, string aggregateFunction, CancellationToken ct)
            where TSource : class, new()
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, aggregateFunction);
                return normProvider.ExecuteAsync<TResult>(aggregateExpression, ct);
            }

            throw new InvalidOperationException($"{aggregateFunction}Async can only be used with nORM queries. Make sure you started with context.Query<T>().");
        }

        private static Task<TResult> ExecuteQueryableAggregateAsync<TSource, TResult>(IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, string aggregateFunction, CancellationToken ct)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, aggregateFunction);
                return normProvider.ExecuteAsync<TResult>(aggregateExpression, ct);
            }

            throw new InvalidOperationException(
                $"{aggregateFunction}Async extension can only be used with nORM queries. Make sure you started with context.Query<T>(). " +
                $"For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.{aggregateFunction}Async().");
        }

        /// <summary>
        /// Builds an expression tree that represents invoking the requested
        /// aggregate function (<c>Sum</c>, <c>Average</c>, <c>Min</c>, <c>Max</c>)
        /// over the given source with the provided selector.
        /// </summary>
        /// <param name="sourceExpression">The expression representing the source sequence.</param>
        /// <param name="selector">Lambda selecting the value to aggregate.</param>
        /// <param name="function">Name of the aggregate function to call.</param>
        /// <returns>An <see cref="Expression"/> that can be executed by the query provider.</returns>
        private static Expression CreateAggregateExpression(Expression sourceExpression, LambdaExpression selector, string function)
        {
            var sourceType = sourceExpression.Type.GetGenericArguments()[0];

            MethodInfo genericMethod = function switch
            {
                "Sum" => GetAggregateMethod(SumMethods.Value, selector.ReturnType).MakeGenericMethod(sourceType),
                "Average" => GetAggregateMethod(AverageMethods.Value, selector.ReturnType).MakeGenericMethod(sourceType),
                "Min" => MinMethod.Value.MakeGenericMethod(sourceType, selector.ReturnType),
                "Max" => MaxMethod.Value.MakeGenericMethod(sourceType, selector.ReturnType),
                _ => throw new InvalidOperationException($"Unsupported aggregate function '{function}'.")
            };

            return Expression.Call(null, genericMethod, sourceExpression, selector);
        }

        /// <summary>
        /// Resolves the appropriate generic <see cref="Queryable"/> method that
        /// matches the selector's return type for the specified aggregate operation.
        /// </summary>
        /// <param name="cache">A lookup of selector types to aggregate methods.</param>
        /// <param name="selectorType">The return type of the selector expression.</param>
        /// <returns>The matching <see cref="MethodInfo"/> for the aggregate call.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no suitable method is found.</exception>
        private static MethodInfo GetAggregateMethod(IReadOnlyDictionary<Type, MethodInfo> cache, Type selectorType)
        {
            if (!cache.TryGetValue(selectorType, out var method))
            {
                throw new InvalidOperationException($"No suitable Queryable overload found for selector type {selectorType}.");
            }
            return method;
        }

        private static readonly Lazy<MethodInfo[]> QueryableMethods = new(() =>
            typeof(Queryable).GetMethods()
                .Where(m => m.GetParameters().Length == 2 &&
                            m.GetParameters()[1].ParameterType.IsGenericType &&
                            m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>))
                .ToArray());

        private static readonly Lazy<IReadOnlyDictionary<Type, MethodInfo>> SumMethods = new(() =>
            QueryableMethods.Value
                .Where(m => m.Name == nameof(Queryable.Sum))
                .ToDictionary(
                    m => m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments()[1],
                    m => m));

        private static readonly Lazy<IReadOnlyDictionary<Type, MethodInfo>> AverageMethods = new(() =>
            QueryableMethods.Value
                .Where(m => m.Name == nameof(Queryable.Average))
                .ToDictionary(
                    m => m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments()[1],
                    m => m));

        private static readonly Lazy<MethodInfo> MinMethod = new(() =>
            QueryableMethods.Value.Single(m => m.Name == nameof(Queryable.Min)));

        private static readonly Lazy<MethodInfo> MaxMethod = new(() =>
            QueryableMethods.Value.Single(m => m.Name == nameof(Queryable.Max)));

        #endregion
    }
}

