using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace nORM.Query
{
    internal static class ExpressionFingerprint
    {
        public static int Compute(Expression expression)
        {
            var visitor = new FingerprintVisitor();
            visitor.Visit(expression);
            return visitor.Hash;
        }

        private sealed class FingerprintVisitor : ExpressionVisitor
        {
            private readonly HashCode _hash = new();
            private readonly Dictionary<ParameterExpression, int> _parameters = new();

            public int Hash => _hash.ToHashCode();

            public override Expression? Visit(Expression? node)
            {
                if (node == null)
                    return null;

                _hash.Add(node.NodeType);
                _hash.Add(node.Type.FullName);

                return base.Visit(node);
            }

            protected override Expression VisitConstant(ConstantExpression node)
            {
                // Ignore constant value; base.VisitConstant does nothing
                return node;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                _hash.Add(node.Member.Module.ModuleVersionId);
                _hash.Add(node.Member.MetadataToken);
                return base.VisitMember(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                _hash.Add(node.Method.Module.ModuleVersionId);
                _hash.Add(node.Method.MetadataToken);
                return base.VisitMethodCall(node);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (!_parameters.TryGetValue(node, out var id))
                {
                    id = _parameters.Count;
                    _parameters[node] = id;
                }
                _hash.Add(id);
                _hash.Add(node.Type.FullName);
                return base.VisitParameter(node);
            }

            protected override Expression VisitLambda<T>(Expression<T> node)
            {
                foreach (var parameter in node.Parameters)
                {
                    if (!_parameters.ContainsKey(parameter))
                    {
                        int id = _parameters.Count;
                        _parameters[parameter] = id;
                    }
                }
                return base.VisitLambda(node);
            }
        }
    }
}
