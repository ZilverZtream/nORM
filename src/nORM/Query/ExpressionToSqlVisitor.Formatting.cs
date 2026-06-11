using System;
using System.Collections.Generic;
using System.Collections.Frozen;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;
using nORM.Internal;

#nullable enable
namespace nORM.Query
{
    internal sealed partial class ExpressionToSqlVisitor
    {
        private readonly struct FormatSegment
        {
            public readonly bool IsLiteral;
            public readonly string? Literal;
            public readonly int ArgIndex;
            public readonly string? FormatSpec;
            // Alignment width: positive = right-align (pad-left), negative =
            // left-align (pad-right), zero = no alignment.
            public readonly int Alignment;
            public FormatSegment(string literal)
            {
                IsLiteral = true; Literal = literal; ArgIndex = -1; FormatSpec = null; Alignment = 0;
            }
            public FormatSegment(int argIndex, string? formatSpec, int alignment)
            {
                IsLiteral = false; Literal = null; ArgIndex = argIndex; FormatSpec = formatSpec; Alignment = alignment;
            }
        }

        /// <summary>
        /// Splits a <c>string.Format</c> template into literal / placeholder segments.
        /// Placeholders may include an optional alignment width after a comma
        /// (<c>{0,5}</c>, <c>{0,-5}</c>) and an optional format spec after a colon
        /// (<c>{0:F2}</c>, <c>{0,10:F2}</c>, <c>{0:yyyy-MM-dd}</c>); the spec and
        /// alignment are preserved on the segment so the emit can route them
        /// through the provider's FormatFixedDecimalSql /
        /// FormatDateUsingDotNetPattern hooks and PadLeft / PadRight wrap.
        /// Escaped braces (<c>{{</c>, <c>}}</c>) are recognized and emitted
        /// as literal single braces.
        /// </summary>
        private static List<FormatSegment>? TryParseSimpleFormatSegments(string template)
        {
            var segments = new List<FormatSegment>();
            var literal = new System.Text.StringBuilder();
            int i = 0;
            while (i < template.Length)
            {
                char c = template[i];
                if (c == '{')
                {
                    if (i + 1 < template.Length && template[i + 1] == '{') { literal.Append('{'); i += 2; continue; }
                    if (literal.Length > 0) { segments.Add(new FormatSegment(literal.ToString())); literal.Clear(); }
                    int end = template.IndexOf('}', i + 1);
                    if (end < 0) return null;
                    var inner = template.Substring(i + 1, end - i - 1);
                    if (inner.Length == 0) return null;
                    string indexPart = inner;
                    string? alignmentPart = null;
                    string? spec = null;
                    int colon = inner.IndexOf(':');
                    if (colon >= 0)
                    {
                        spec = inner.Substring(colon + 1);
                        if (spec.Length == 0) return null;
                        indexPart = inner.Substring(0, colon);
                    }
                    int comma = indexPart.IndexOf(',');
                    if (comma >= 0)
                    {
                        alignmentPart = indexPart.Substring(comma + 1);
                        indexPart = indexPart.Substring(0, comma);
                        if (alignmentPart.Length == 0) return null;
                    }
                    if (!int.TryParse(indexPart, out var argIdx) || argIdx < 0) return null;
                    int alignment = 0;
                    if (alignmentPart != null && !int.TryParse(alignmentPart, out alignment))
                        return null;
                    segments.Add(new FormatSegment(argIdx, spec, alignment));
                    i = end + 1;
                }
                else if (c == '}')
                {
                    if (i + 1 < template.Length && template[i + 1] == '}') { literal.Append('}'); i += 2; continue; }
                    return null; // unmatched closing brace
                }
                else
                {
                    literal.Append(c);
                    i++;
                }
            }
            if (literal.Length > 0) segments.Add(new FormatSegment(literal.ToString()));
            return segments;
        }

        /// <summary>
        /// Applies a .NET format spec to an already-translated SQL argument
        /// fragment by routing through the appropriate provider hook. Returns
        /// null when the spec cannot be portably represented (caller falls
        /// back to client-eval). Currently supports:
        ///  * F&lt;N&gt;  -- fixed decimal with N fractional digits (any numeric arg)
        ///  * Custom date pattern (any arg whose CLR type is DateTime /
        ///    DateTimeOffset / DateOnly / TimeOnly).
        /// </summary>
        private string? ApplyFormatSpecToArg(string argSql, Type argType, string spec)
        {
            var underlying = Nullable.GetUnderlyingType(argType) ?? argType;

            // Numeric F<N>: fixed-decimal text. Provider hook produces e.g.
            // printf('%.2f', x) on SQLite, FORMAT(x, 'F2') on SqlServer, etc.
            if (spec.Length >= 2 && (spec[0] == 'F' || spec[0] == 'f')
                && int.TryParse(spec.AsSpan(1), out var digits) && digits >= 0 && digits <= 28)
            {
                return _provider.FormatFixedDecimalSql(argSql, digits);
            }

            // Date / time custom patterns. Route through FormatDateUsingDotNetPattern;
            // null means the provider can't translate this pattern -> caller
            // defers to client-eval.
            if (underlying == typeof(DateTime)
                || underlying == typeof(DateTimeOffset)
                || underlying == typeof(DateOnly)
                || underlying == typeof(TimeOnly))
            {
                return _provider.FormatDateUsingDotNetPattern(argSql, spec);
            }

            return null;
        }

        /// <summary>
        /// C# emits `charExpr OP charExpr` as `intExpr OP intExpr` via implicit `char→int`
        /// promotions. When at least one side is `Convert(charExpr, int)`, recover the char
        /// comparison so the SQL emit doesn't compare a string SUBSTR result against an int
        /// parameter (which never matches at the database). Returns null when no rewrite
        /// is needed.
        /// </summary>
        private static BinaryExpression? TryRewriteLiftedCharComparison(BinaryExpression node)
        {
            bool leftIsLifted = node.Left is UnaryExpression { NodeType: ExpressionType.Convert } lu
                                && lu.Operand.Type == typeof(char);
            bool rightIsLifted = node.Right is UnaryExpression { NodeType: ExpressionType.Convert } ru
                                 && ru.Operand.Type == typeof(char);
            if (!leftIsLifted && !rightIsLifted) return null;

            Expression Demote(Expression side)
            {
                if (side is UnaryExpression { NodeType: ExpressionType.Convert } u && u.Operand.Type == typeof(char))
                    return u.Operand;
                if (side is ConstantExpression { Value: not null } intLit && intLit.Type == typeof(int))
                    return Expression.Constant((char)(int)intLit.Value, typeof(char));
                if (side is ConstantExpression { Value: null }) return Expression.Constant(null, typeof(char?));
                return side;
            }

            var newLeft = Demote(node.Left);
            var newRight = Demote(node.Right);
            if (ReferenceEquals(newLeft, node.Left) && ReferenceEquals(newRight, node.Right)) return null;
            return Expression.MakeBinary(node.NodeType, newLeft, newRight);
        }

        /// <summary>
        /// Recognises <c>collection.IndexOf(x) [op] 0/-1</c> shapes and rewrites them
        /// to the equivalent <c>collection.Contains(x)</c> (optionally negated) call.
        /// Lets the dedicated Contains handler (around line 1318) emit a SQL IN clause
        /// instead of throwing "Method 'IndexOf' cannot be translated".
        /// Supported operator/threshold combinations:
        ///   * <c>IndexOf(x) &gt;= 0</c>, <c>&gt; -1</c>, <c>!= -1</c> -> <c>Contains(x)</c>
        ///   * <c>IndexOf(x) &lt; 0</c>, <c>== -1</c>, <c>&lt;= -1</c> -> <c>!Contains(x)</c>
        ///   * Reversed operand order is also handled.
        /// </summary>
        private static Expression? TryRewriteIndexOfToContains(BinaryExpression node)
        {
            // Identify which side is the IndexOf call and which is the int constant.
            (MethodCallExpression call, int threshold, bool callOnLeft)? probe = null;
            if (node.Left is MethodCallExpression leftCall
                && TryGetIntConstant(node.Right, out var rightInt))
            {
                probe = (leftCall, rightInt, true);
            }
            else if (node.Right is MethodCallExpression rightCall
                     && TryGetIntConstant(node.Left, out var leftInt))
            {
                probe = (rightCall, leftInt, false);
            }
            if (probe is not { } p) return null;

            var call = p.call;
            if (call.Method.Name != "IndexOf"
                || call.Object == null
                || call.Arguments.Count != 1
                || call.Method.GetParameters().Length != 1
                || !IsTranslatableContainsReceiver(call.Object.Type)
                || call.Object.Type == typeof(string))
            {
                return null;
            }

            // Map (op, threshold) to "is Contains" vs "is !Contains", normalising for
            // reversed operand order. The truthy table covers IndexOf>=0, >-1, !=-1
            // (and the reversed 0<=IndexOf etc.); falsy covers IndexOf<0, ==-1, <=-1.
            // Anything else (e.g. >5, <-2) isn't a Contains rewrite -- bail out.
            var op = p.callOnLeft ? node.NodeType : Mirror(node.NodeType);
            bool? isContainsMatch = (op, p.threshold) switch
            {
                (ExpressionType.GreaterThanOrEqual, 0) => true,
                (ExpressionType.GreaterThan, -1) => true,
                (ExpressionType.NotEqual, -1) => true,
                (ExpressionType.LessThan, 0) => false,
                (ExpressionType.LessThanOrEqual, -1) => false,
                (ExpressionType.Equal, -1) => false,
                _ => null
            };
            if (isContainsMatch is not { } truthy) return null;

            // Build collection.Contains(arg). The actual Contains method is resolved
            // on the receiver's runtime type so List<int>.Contains, HashSet<int>.Contains,
            // etc. all bind correctly. Falls back to IEnumerable<T>.Contains via the
            // declared interfaces if the type doesn't expose a public Contains.
            var receiverType = call.Object.Type;
            var elementType = call.Arguments[0].Type;
            var containsMethod = receiverType
                .GetMethod("Contains", new[] { elementType });
            if (containsMethod == null) return null;

            Expression rewritten = Expression.Call(call.Object, containsMethod, call.Arguments[0]);
            if (!truthy) rewritten = Expression.Not(rewritten);
            return rewritten;
        }

        private static bool TryGetIntConstant(Expression e, out int value)
        {
            if (e is ConstantExpression { Value: int i })
            {
                value = i;
                return true;
            }
            if (e is UnaryExpression { NodeType: ExpressionType.Convert } u
                && u.Operand is ConstantExpression { Value: int j })
            {
                value = j;
                return true;
            }
            value = 0;
            return false;
        }

        private static ExpressionType Mirror(ExpressionType op) => op switch
        {
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            _ => op
        };
    }
}
