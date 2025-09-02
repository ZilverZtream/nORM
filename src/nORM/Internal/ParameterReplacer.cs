using System.Linq.Expressions;

#nullable enable

namespace nORM.Internal
{
    internal sealed class ParameterReplacer : ExpressionVisitor
    {
        private readonly ParameterExpression _parameter;
        private readonly Expression _replacement;

        public ParameterReplacer(ParameterExpression parameter, Expression replacement)
        {
            _parameter = parameter;
            _replacement = replacement;
        }

        protected override Expression VisitParameter(ParameterExpression node)
            => node == _parameter ? _replacement : base.VisitParameter(node);
    }
}

