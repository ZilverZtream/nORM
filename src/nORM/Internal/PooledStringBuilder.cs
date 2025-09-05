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

    public static StringBuilder Rent() => _pool.Get();

    public static void Return(StringBuilder sb)
    {
        sb.Clear();
        _pool.Return(sb);
    }

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
