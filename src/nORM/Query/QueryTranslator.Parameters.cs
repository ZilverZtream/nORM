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
        internal static bool TryGetConstantValue(Expression e, out object? value)
        {
            switch (e)
            {
                case ConstantExpression ce:
                    value = ce.Value;
                    return true;
                case MemberExpression me:
                    // Static member access (DateTime.UtcNow, MyClass.StaticField). me.Expression
                    // is null in this case - read directly from the type.
                    if (me.Expression == null)
                    {
                        value = me.Member switch
                        {
                            FieldInfo sfi => sfi.GetValue(null),
                            PropertyInfo spi => spi.GetValue(null),
                            _ => null
                        };
                        return true;
                    }
                    if (TryGetConstantValue(me.Expression, out var obj))
                    {
                        value = me.Member switch
                        {
                            FieldInfo fi => fi.GetValue(obj),
                            PropertyInfo pi => pi.GetValue(obj),
                            _ => null
                        };
                        return true;
                    }
                    break;
                // Constant array element access `arr[i]` (a captured array indexed by a constant).
                // Safe to evaluate — plain element read, no arbitrary code execution (unlike a
                // MethodCall). Fold to the element so `col == arr[2]` binds the value as a parameter
                // instead of reaching the operator emitter (ArrayIndex has no SQL operator).
                case BinaryExpression be when be.NodeType == ExpressionType.ArrayIndex:
                    if (TryGetConstantValue(be.Left, out var arrObj) && arrObj is Array arr
                        && TryGetConstantValue(be.Right, out var idxObj) && idxObj is int idx
                        && idx >= 0 && idx < arr.Length)
                    {
                        value = arr.GetValue(idx);
                        return true;
                    }
                    break;
                // MethodCallExpression handling was intentionally removed to prevent RCE.
                // Method calls are translated to SQL (e.g., string.Contains) or throw NotSupportedException.
                // Executing arbitrary user code via Invoke() would be a critical security vulnerability.
            }
            value = null;
            return false;
        }
        private void MergeSubPlanParameters(QueryPlan subPlan)
        {
            var compiledSubPlanParameters = subPlan.CompiledParameters.Count == 0
                ? null
                : new HashSet<string>(subPlan.CompiledParameters, StringComparer.Ordinal);

            foreach (var parameter in subPlan.Parameters)
            {
                _params[parameter.Key] = parameter.Value;
                if (compiledSubPlanParameters?.Contains(parameter.Key) == true && !_compiledParams.Contains(parameter.Key))
                    _compiledParams.Add(parameter.Key);

                AdvanceParameterIndexPast(parameter.Key);
            }
        }

        private void AdvanceParameterIndexPast(string parameterName)
        {
            var generatedPrefix = _ctx.RawProvider.ParamPrefix + "p";
            if (!parameterName.StartsWith(generatedPrefix, StringComparison.Ordinal))
                return;

            var indexText = parameterName.Substring(generatedPrefix.Length);
            // Strip a __qv / __qm<Member> query-parameter source marker before parsing.
            var markerStart = indexText.IndexOf("__", StringComparison.Ordinal);
            if (markerStart >= 0)
                indexText = indexText.Substring(0, markerStart);
            if (!int.TryParse(indexText, out var index))
                return;

            var nextIndex = index + 1;
            if (_parameterManager.Index < nextIndex)
                _parameterManager.Index = nextIndex;
        }

        private void AddParameter(string name, object? value)
        {
            _params[name] = value ?? DBNull.Value;
            if (!_compiledParams.Contains(name))
            {
                _compiledParams.Add(name);
            }
        }

        /// <summary>
        /// Stores a parameter value without flagging it as compiled. Use when copying inline
        /// constants from a sub-visitor - the sub-visitor's closure-capture path already
        /// registers compiled entries in the shared list, so blindly re-flagging literals
        /// causes BindPlanParameters to skip them at execution time.
        /// </summary>
        private void AddLiteralParameter(string name, object? value)
        {
            _params[name] = value ?? DBNull.Value;
        }

        private static bool TryGetIntValue(Expression expr, out int value)
        {
            value = 0;
            if (expr is ConstantExpression c && c.Value is int i)
            {
                value = i;
                return true;
            }
            return false;
        }

        private bool TryBindPagingParameter(Expression expression, out string parameterName)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                expression = convert.Operand;

            if (expression is ParameterExpression parameter)
            {
                if (!_paramMap.TryGetValue(parameter, out parameterName!))
                {
                    // __qv / __qm<Member> markers name the query-parameter source so the
                    // compiled-query pipeline pairs these slots by name — paging slots
                    // register at clause time while projection slots register at Build
                    // time, so document-order positional pairing would cross-bind them.
                    parameterName = _ctx.RawProvider.ParamPrefix + "p" + _parameterManager.GetNextIndex() + "__qv";
                    AddParameter(parameterName, DBNull.Value);
                    _paramMap[parameter] = parameterName;
                }
                return true;
            }

            if (expression is MemberExpression member && HasUncorrelatedParameterRoot(member))
            {
                var marker = member.Expression is ParameterExpression
                    ? "__qm" + member.Member.Name
                    : string.Empty;
                parameterName = _ctx.RawProvider.ParamPrefix + "p" + _parameterManager.GetNextIndex() + marker;
                AddParameter(parameterName, DBNull.Value);
                return true;
            }

            parameterName = string.Empty;
            return false;
        }

        private bool HasUncorrelatedParameterRoot(MemberExpression member)
        {
            Expression? current = member.Expression;
            while (current is MemberExpression nested)
                current = nested.Expression;

            return current is ParameterExpression parameter && !_correlatedParams.ContainsKey(parameter);
        }
    }
}
