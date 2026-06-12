using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.ObjectPool;
using System.Text;
using nORM.Core;
using nORM.Configuration;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using nORM.SourceGeneration;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        /// <summary>
        /// Retrieves the timestamp associated with a named temporal tag from the special
        /// <c>__NormTemporalTags</c> table. Tags are used to reference specific points in time
        /// for temporal queries.
        /// </summary>
        /// <param name="tagName">The name of the temporal tag.</param>
        /// <param name="ct">Optional cancellation token.</param>
        /// <returns>The timestamp stored for the specified tag.</returns>
        /// <exception cref="NormQueryException">Thrown if the tag does not exist.</exception>
        private async Task<DateTime> GetTimestampForTagAsync(string tagName, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tagName))
                throw new ArgumentException("Tag name must not be null or empty.", nameof(tagName));
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            var pName = _provider.ParamPrefix + "p0";
            // Use provider-escaped identifier SQL to avoid hardcoded unescaped names.
            cmd.CommandText = _provider.GetTagLookupSql(pName);
            cmd.AddParam(pName, tagName);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (result == null || result == DBNull.Value)
                throw new NormQueryException($"Tag '{tagName}' not found.");
            return Convert.ToDateTime(result);
        }
    }
}
