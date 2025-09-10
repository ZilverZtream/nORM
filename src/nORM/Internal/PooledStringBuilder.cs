using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Internal;

/// <summary>
/// Provides shared <see cref="StringBuilder"/> pooling helpers for SQL generation.
/// </summary>
internal static class PooledStringBuilder
{
    private static readonly ObjectPool<StringBuilder> _pool =
        new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

    /// <summary>
    /// Retrieves a <see cref="StringBuilder"/> instance from the shared pool. Callers must return
    /// the instance via <see cref="Return"/> when finished to avoid unnecessary allocations.
    /// </summary>
    /// <returns>A <see cref="StringBuilder"/> instance ready for use.</returns>
    public static StringBuilder Rent() => _pool.Get();

    /// <summary>
    /// Returns a previously rented <see cref="StringBuilder"/> to the pool after clearing its
    /// content so it can be reused without carrying state between operations.
    /// </summary>
    /// <param name="sb">The <see cref="StringBuilder"/> instance to return.</param>
    public static void Return(StringBuilder sb)
    {
        sb.Clear();
        _pool.Return(sb);
    }

    /// <summary>
    /// Concatenates the specified string values using a pooled <see cref="StringBuilder"/> and
    /// returns the resulting string. The builder is automatically returned to the pool even if
    /// an exception occurs during iteration.
    /// </summary>
    /// <param name="values">The sequence of string values to join.</param>
    /// <param name="separator">The separator inserted between values. Defaults to a comma and space.</param>
    /// <returns>The concatenated string.</returns>
    public static string Join(IEnumerable<string> values, string separator = ", ")
    {
        var sb = Rent();
        try
        {
            using var e = values.GetEnumerator();
            if (e.MoveNext())
            {
                sb.Append(e.Current);
                while (e.MoveNext())
                {
                    sb.Append(separator);
                    sb.Append(e.Current);
                }
            }
            return sb.ToString();
        }
        finally
        {
            Return(sb);
        }
    }

    public static string JoinOrderBy(List<(string col, bool asc)> orderBy)
    {
        var sb = Rent();
        try
        {
            for (int i = 0; i < orderBy.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                var (col, asc) = orderBy[i];
                sb.Append(col).Append(asc ? " ASC" : " DESC");
            }
            return sb.ToString();
        }
        finally
        {
            Return(sb);
        }
    }
}
