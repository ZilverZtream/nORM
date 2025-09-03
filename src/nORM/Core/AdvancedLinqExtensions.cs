using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Advanced LINQ aggregate operations for nORM with enterprise-grade performance
    /// These provide the missing functionality to achieve EF Core feature parity
    /// </summary>
    public static class AdvancedLinqExtensions
    {
        #region Aggregate Functions with Predicate Support
        
        /// <summary>
        /// Computes the sum of a sequence of numeric values with optimal SQL translation
        /// </summary>
        public static Task<decimal> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, decimal>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, decimal>(source, selector, "Sum", ct);

        /// <summary>
        /// Computes the sum of a sequence of nullable decimal values
        /// </summary>
        public static Task<decimal?> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, decimal?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, decimal?>(source, selector, "Sum", ct);

        /// <summary>
        /// Computes the sum of a sequence of double values
        /// </summary>
        public static Task<double> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, double>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, double>(source, selector, "Sum", ct);

        /// <summary>
        /// Computes the sum of a sequence of nullable double values
        /// </summary>
        public static Task<double?> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, double?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, double?>(source, selector, "Sum", ct);

        /// <summary>
        /// Computes the sum of a sequence of float values
        /// </summary>
        public static Task<float> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, float>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, float>(source, selector, "Sum", ct);

        /// <summary>
        /// Computes the sum of a sequence of nullable float values
        /// </summary>
        public static Task<float?> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, float?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, float?>(source, selector, "Sum", ct);

        /// <summary>
        /// Computes the sum of a sequence of int values
        /// </summary>
        public static Task<int> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, int>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, int>(source, selector, "Sum", ct);

        /// <summary>
        /// Computes the sum of a sequence of nullable int values
        /// </summary>
        public static Task<int?> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, int?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, int?>(source, selector, "Sum", ct);

        /// <summary>
        /// Computes the sum of a sequence of long values
        /// </summary>
        public static Task<long> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, long>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, long>(source, selector, "Sum", ct);

        /// <summary>
        /// Computes the sum of a sequence of nullable long values
        /// </summary>
        public static Task<long?> SumAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, long?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, long?>(source, selector, "Sum", ct);

        #endregion

        #region IQueryable overloads for LINQ chained operations - only for nORM queries
        
        /// <summary>
        /// Computes the sum of a sequence of decimal values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<decimal> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<decimal>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }

        /// <summary>
        /// Computes the sum of a sequence of nullable decimal values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<decimal?> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<decimal?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }

        /// <summary>
        /// Computes the sum of a sequence of double values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<double> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<double>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }

        /// <summary>
        /// Computes the sum of a sequence of nullable double values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<double?> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<double?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }

        /// <summary>
        /// Computes the sum of a sequence of float values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<float> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<float>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }

        /// <summary>
        /// Computes the sum of a sequence of nullable float values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<float?> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<float?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }

        /// <summary>
        /// Computes the sum of a sequence of int values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<int> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<int>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }

        /// <summary>
        /// Computes the sum of a sequence of nullable int values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<int?> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<int?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }

        /// <summary>
        /// Computes the sum of a sequence of long values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<long> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<long>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }

        /// <summary>
        /// Computes the sum of a sequence of nullable long values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<long?> SumAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Sum");
                return normProvider.ExecuteAsync<long?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "SumAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.SumAsync().");
        }
        
        /// <summary>
        /// Computes the average of a sequence of decimal values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<decimal> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Average");
                return normProvider.ExecuteAsync<decimal>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }

        /// <summary>
        /// Computes the average of a sequence of nullable decimal values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<decimal?> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Average");
                return normProvider.ExecuteAsync<decimal?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }

        /// <summary>
        /// Computes the average of a sequence of double values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<double> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Average");
                return normProvider.ExecuteAsync<double>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }

        /// <summary>
        /// Computes the average of a sequence of nullable double values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<double?> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Average");
                return normProvider.ExecuteAsync<double?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }

        /// <summary>
        /// Computes the average of a sequence of float values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<float> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Average");
                return normProvider.ExecuteAsync<float>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }

        /// <summary>
        /// Computes the average of a sequence of nullable float values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<float?> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Average");
                return normProvider.ExecuteAsync<float?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }

        /// <summary>
        /// Computes the average of a sequence of int values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<double> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var converted = Expression.Lambda<Func<TSource, double>>(Expression.Convert(selector.Body, typeof(double)), selector.Parameters);
                var aggregateExpression = CreateAggregateExpression(source.Expression, converted, "Average");
                return normProvider.ExecuteAsync<double>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }

        /// <summary>
        /// Computes the average of a sequence of nullable int values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<double?> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var converted = Expression.Lambda<Func<TSource, double?>>(Expression.Convert(selector.Body, typeof(double?)), selector.Parameters);
                var aggregateExpression = CreateAggregateExpression(source.Expression, converted, "Average");
                return normProvider.ExecuteAsync<double?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }

        /// <summary>
        /// Computes the average of a sequence of long values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<double> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var converted = Expression.Lambda<Func<TSource, double>>(Expression.Convert(selector.Body, typeof(double)), selector.Parameters);
                var aggregateExpression = CreateAggregateExpression(source.Expression, converted, "Average");
                return normProvider.ExecuteAsync<double>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }

        /// <summary>
        /// Computes the average of a sequence of nullable long values (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<double?> AverageAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long?>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var converted = Expression.Lambda<Func<TSource, double?>>(Expression.Convert(selector.Body, typeof(double?)), selector.Parameters);
                var aggregateExpression = CreateAggregateExpression(source.Expression, converted, "Average");
                return normProvider.ExecuteAsync<double?>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "AverageAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AverageAsync().");
        }
        
        /// <summary>
        /// Returns the minimum value in a sequence (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<TResult> MinAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Min");
                return normProvider.ExecuteAsync<TResult>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "MinAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.MinAsync().");
        }
        
        /// <summary>
        /// Returns the maximum value in a sequence (for LINQ chained nORM queries only)
        /// </summary>
        public static Task<TResult> MaxAsync<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, "Max");
                return normProvider.ExecuteAsync<TResult>(aggregateExpression, ct);
            }
            throw new InvalidOperationException(
                "MaxAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.MaxAsync().");
        }

        #endregion

        #region Average Functions

        /// <summary>
        /// Computes the average of a sequence of decimal values
        /// </summary>
        public static Task<decimal> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, decimal>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, decimal>(source, selector, "Average", ct);

        /// <summary>
        /// Computes the average of a sequence of nullable decimal values
        /// </summary>
        public static Task<decimal?> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, decimal?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, decimal?>(source, selector, "Average", ct);

        /// <summary>
        /// Computes the average of a sequence of double values
        /// </summary>
        public static Task<double> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, double>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, double>(source, selector, "Average", ct);

        /// <summary>
        /// Computes the average of a sequence of nullable double values
        /// </summary>
        public static Task<double?> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, double?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, double?>(source, selector, "Average", ct);

        /// <summary>
        /// Computes the average of a sequence of float values
        /// </summary>
        public static Task<float> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, float>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, float>(source, selector, "Average", ct);

        /// <summary>
        /// Computes the average of a sequence of nullable float values
        /// </summary>
        public static Task<float?> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, float?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, float?>(source, selector, "Average", ct);

        /// <summary>
        /// Computes the average of a sequence of int values
        /// </summary>
        public static Task<double> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, int>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, double>(source, Expression.Lambda<Func<TSource, double>>(
                Expression.Convert(selector.Body, typeof(double)), selector.Parameters), "Average", ct);

        /// <summary>
        /// Computes the average of a sequence of nullable int values
        /// </summary>
        public static Task<double?> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, int?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, double?>(source, Expression.Lambda<Func<TSource, double?>>(
                Expression.Convert(selector.Body, typeof(double?)), selector.Parameters), "Average", ct);

        /// <summary>
        /// Computes the average of a sequence of long values
        /// </summary>
        public static Task<double> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, long>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, double>(source, Expression.Lambda<Func<TSource, double>>(
                Expression.Convert(selector.Body, typeof(double)), selector.Parameters), "Average", ct);

        /// <summary>
        /// Computes the average of a sequence of nullable long values
        /// </summary>
        public static Task<double?> AverageAsync<TSource>(this INormQueryable<TSource> source, Expression<Func<TSource, long?>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, double?>(source, Expression.Lambda<Func<TSource, double?>>(
                Expression.Convert(selector.Body, typeof(double?)), selector.Parameters), "Average", ct);

        #endregion

        #region Min/Max Functions

        /// <summary>
        /// Returns the minimum value in a sequence
        /// </summary>
        public static Task<TResult> MinAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, TResult>(source, selector, "Min", ct);

        /// <summary>
        /// Returns the maximum value in a sequence
        /// </summary>
        public static Task<TResult> MaxAsync<TSource, TResult>(this INormQueryable<TSource> source, Expression<Func<TSource, TResult>> selector, CancellationToken ct = default)
            where TSource : class, new()
            => ExecuteAggregateAsync<TSource, TResult>(source, selector, "Max", ct);

        #endregion

        #region Helper Methods

        /// <summary>
        /// Core aggregate execution method with optimized SQL generation
        /// </summary>
        private static Task<TResult> ExecuteAggregateAsync<TSource, TResult>(
            INormQueryable<TSource> source, 
            Expression<Func<TSource, TResult>> selector, 
            string aggregateFunction,
            CancellationToken ct)
            where TSource : class, new()
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                // Create a simplified expression that the QueryTranslator can handle
                var aggregateExpression = CreateAggregateExpression(source.Expression, selector, aggregateFunction);
                
                return normProvider.ExecuteAsync<TResult>(aggregateExpression, ct);
            }
            
            throw new InvalidOperationException($"{aggregateFunction}Async can only be used with nORM queries. Make sure you started with context.Query<T>().");
        }

        /// <summary>
        /// Creates optimized aggregate expressions for SQL translation
        /// </summary>
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

        private static readonly IReadOnlyDictionary<Type, MethodInfo> SumMethods = BuildSelectorMethods(nameof(Queryable.Sum));
        private static readonly IReadOnlyDictionary<Type, MethodInfo> AverageMethods = BuildSelectorMethods(nameof(Queryable.Average));
        private static readonly MethodInfo MinMethod = BuildMinMaxMethod(nameof(Queryable.Min));
        private static readonly MethodInfo MaxMethod = BuildMinMaxMethod(nameof(Queryable.Max));

        private static IReadOnlyDictionary<Type, MethodInfo> BuildSelectorMethods(string name)
            => typeof(Queryable).GetMethods()
                .Where(m => m.Name == name &&
                            m.GetParameters().Length == 2 &&
                            m.GetParameters()[1].ParameterType.IsGenericType &&
                            m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>))
                .ToDictionary(
                    m => m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments()[1],
                    m => m);

        private static MethodInfo BuildMinMaxMethod(string name)
            => typeof(Queryable).GetMethods()
                .Single(m => m.Name == name &&
                             m.GetParameters().Length == 2 &&
                             m.GetParameters()[1].ParameterType.IsGenericType &&
                             m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>));

        #endregion
    }
}
