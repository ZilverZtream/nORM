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
        private string BuildSelectWithWindowFunctions(LambdaExpression projection, List<WindowFunctionInfo> windowFuncs, string overClause)
        {
            if (projection.Body is not NewExpression ne)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Window function projection must be an anonymous object initializer."));
            var paramMap = windowFuncs.ToDictionary(w => w.ResultParameter, w => w);
            var sb = PooledStringBuilder.Rent();
            try
            {
                for (int i = 0; i < ne.Arguments.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    var arg = ne.Arguments[i];
                    var alias = ne.Members?[i]?.Name ?? $"Item{i + 1}";
                    if (arg is MemberExpression me)
                    {
                        if (!_mapping.TryGetColumnForMemberAccess(me, out var col))
                            throw new NormQueryException(
                                $"Property path '{me}' on type '{_mapping.Type.Name}' is not mapped to a database column.");
                        sb.Append(col.EscCol).Append(" AS ").Append(_provider.Escape(alias));
                    }
                    else if (arg is ParameterExpression p && paramMap.TryGetValue(p, out var wf))
                    {
                        var wfSql = BuildWindowFunctionSql(wf, overClause);
                        sb.Append(wfSql).Append(" AS ").Append(_provider.Escape(alias));
                    }
                    else
                    {
                        var param = projection.Parameters[0];
                        if (!_correlatedParams.TryGetValue(param, out var info))
                        {
                            info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? EscapeAlias("T" + _joinCounter));
                            _correlatedParams[param] = info;
                        }
                        var vctx = new VisitorContext(_ctx, _mapping, _provider, param, info.Alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                        var visitor = FastExpressionVisitorPool.Get(in vctx);
                        var sql = visitor.Translate(arg);
                        // AddLiteralParameter - see HandleAggregateExpression / OrderByTranslator.
                        foreach (var kvp in visitor.GetParameters())
                            AddLiteralParameter(kvp.Key, kvp.Value);
                        FastExpressionVisitorPool.Return(visitor);
                        sb.Append(sql).Append(" AS ").Append(_provider.Escape(alias));
                    }
                }
                return sb.ToString();
            }
            finally
            {
                PooledStringBuilder.Return(sb);
            }
        }
        private string BuildWindowFunctionSql(WindowFunctionInfo wf, string overClause)
        {
            if (wf.ValueSelector != null)
            {
                var param = wf.ValueSelector.Parameters[0];
                if (!_correlatedParams.TryGetValue(param, out var info))
                {
                    info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? EscapeAlias("T" + _joinCounter));
                    _correlatedParams[param] = info;
                }
                var vctx = new VisitorContext(_ctx, _mapping, _provider, param, info.Alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                var visitor = FastExpressionVisitorPool.Get(in vctx);
                var valueSql = visitor.Translate(wf.ValueSelector.Body);
                // AddLiteralParameter - window-function value selectors with COALESCE / literal
                // fallbacks must merge constants the same way as the rest of the family.
                foreach (var kvp in visitor.GetParameters())
                    AddLiteralParameter(kvp.Key, kvp.Value);
                FastExpressionVisitorPool.Return(visitor);
                string defaultSql = string.Empty;
                if (wf.DefaultValueSelector != null)
                {
                    var dParam = wf.DefaultValueSelector.Parameters[0];
                    if (!_correlatedParams.TryGetValue(dParam, out info))
                    {
                        info = (_mapping, _correlatedParams.Values.FirstOrDefault().Alias ?? EscapeAlias("T" + _joinCounter));
                        _correlatedParams[dParam] = info;
                    }
                    var vctx2 = new VisitorContext(_ctx, _mapping, _provider, dParam, info.Alias, _correlatedParams, _compiledParams, _paramMap, _recursionDepth, _params.Count);
                    var visitor2 = FastExpressionVisitorPool.Get(in vctx2);
                    var defSql = visitor2.Translate(wf.DefaultValueSelector.Body);
                    // AddLiteralParameter - see the value-selector branch above.
                    foreach (var kv in visitor2.GetParameters())
                        AddLiteralParameter(kv.Key, kv.Value);
                    FastExpressionVisitorPool.Return(visitor2);
                    defaultSql = $", {defSql}";
                }
                return $"{wf.FunctionName}({valueSql}, {wf.Offset.ToString(System.Globalization.CultureInfo.InvariantCulture)}{defaultSql}) OVER ({overClause})";
            }
            return _provider.GetIntCastSql($"{wf.FunctionName}() OVER ({overClause})", asLong: false);
        }
    }
}
