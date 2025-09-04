using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace nORM.Core
{
    public static class TemporalExtensions
    {
        public static IQueryable<T> AsOf<T>(this IQueryable<T> source, DateTime timestamp)
        {
            var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(T));
            var call = Expression.Call(null, method, source.Expression, Expression.Constant(timestamp));
            return source.Provider.CreateQuery<T>(call);
        }

        public static IQueryable<T> AsOf<T>(this IQueryable<T> source, string tagName)
        {
            var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(T));
            var call = Expression.Call(null, method, source.Expression, Expression.Constant(tagName));
            return source.Provider.CreateQuery<T>(call);
        }
    }
}
