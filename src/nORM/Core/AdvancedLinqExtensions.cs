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

        public static Task<TResult> SumAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Sum", ct);

        public static Task<TResult?> SumAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Sum", ct);

        public static Task<TResult> SumAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Sum", ct);

        public static Task<TResult?> SumAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Sum", ct);

        #endregion

        #region Average

        public static Task<TResult> AverageAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Average", ct);

        public static Task<TResult?> AverageAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Average", ct);

        public static Task<TResult> AverageAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Average", ct);

        public static Task<TResult?> AverageAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Average", ct);

        #endregion

        #region Min/Max

        public static Task<TResult> MinAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Min", ct);

        public static Task<TResult?> MinAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Min", ct);

        public static Task<TResult> MinAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Min", ct);

        public static Task<TResult?> MinAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Min", ct);

        public static Task<TResult> MaxAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Max", ct);

        public static Task<TResult?> MaxAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            where TResult : struct, INumber<TResult>
            => ExecuteAggregateAsync(source, selector, "Max", ct);

        public static Task<TResult> MaxAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
            where TResult : struct, INumber<TResult>
            => ExecuteQueryableAggregateAsync(source, selector, "Max", ct);

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

        private static Expression CreateAggregateExpression(Expression sourceExpression, LambdaExpression selector, string function)
        {
            var sourceType = sourceExpression.Type.GetGenericArguments()[0];

            MethodInfo genericMethod = function switch
            {
                "Sum" => GetAggregateMethod(SumMethods, selector.ReturnType).MakeGenericMethod(sourceType),
                "Average" => GetAggregateMethod(AverageMethods, selector.ReturnType).MakeGenericMethod(sourceType),
                "Min" => MinMethod.MakeGenericMethod(sourceType, selector.ReturnType),
                "Max" => MaxMethod.MakeGenericMethod(sourceType, selector.ReturnType),
                _ => throw new InvalidOperationException($"Unsupported aggregate function '{function}'.")
            };

            return Expression.Call(null, genericMethod, sourceExpression, selector);
        }

        private static MethodInfo GetAggregateMethod(IReadOnlyDictionary<Type, MethodInfo> cache, Type selectorType)
        {
            if (!cache.TryGetValue(selectorType, out var method))
            {
                throw new InvalidOperationException($"No suitable Queryable overload found for selector type {selectorType}.");
            }
            return method;
        }

        private static readonly IReadOnlyDictionary<Type, MethodInfo> SumMethods;
        private static readonly IReadOnlyDictionary<Type, MethodInfo> AverageMethods;
        private static readonly MethodInfo MinMethod;
        private static readonly MethodInfo MaxMethod;

        static AdvancedLinqExtensions()
        {
            var methods = typeof(Queryable).GetMethods()
                .Where(m => m.GetParameters().Length == 2 &&
                            m.GetParameters()[1].ParameterType.IsGenericType &&
                            m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>))
                .ToArray();

            SumMethods = methods
                .Where(m => m.Name == nameof(Queryable.Sum))
                .ToDictionary(
                    m => m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments()[1],
                    m => m);

            AverageMethods = methods
                .Where(m => m.Name == nameof(Queryable.Average))
                .ToDictionary(
                    m => m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments()[1],
                    m => m);

            MinMethod = methods.Single(m => m.Name == nameof(Queryable.Min));
            MaxMethod = methods.Single(m => m.Name == nameof(Queryable.Max));
        }

        #endregion
    }
}

