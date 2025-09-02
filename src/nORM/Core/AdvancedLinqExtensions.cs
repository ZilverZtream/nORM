using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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
            // Use standard LINQ Queryable methods that the query translator can understand
            var methodInfo = function switch
            {
                "Sum" => typeof(Queryable).GetMethods()
                    .First(m => m.Name == "Sum" && m.GetParameters().Length == 2 && 
                               m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>)),
                "Average" => typeof(Queryable).GetMethods()
                    .First(m => m.Name == "Average" && m.GetParameters().Length == 2 && 
                               m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>)),
                "Min" => typeof(Queryable).GetMethods()
                    .First(m => m.Name == "Min" && m.GetParameters().Length == 2 && 
                               m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>)),
                "Max" => typeof(Queryable).GetMethods()
                    .First(m => m.Name == "Max" && m.GetParameters().Length == 2 && 
                               m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>)),
                _ => throw new ArgumentException($"Unknown aggregate function: {function}")
            };

            // Get the source element type
            var sourceType = sourceExpression.Type.GetGenericArguments()[0];
            
            // Make the generic method specific to our types
            var genericMethod = methodInfo.MakeGenericMethod(sourceType, selector.ReturnType);
            
            return Expression.Call(
                null,
                genericMethod,
                sourceExpression,
                selector);
        }

        #endregion
    }
}
