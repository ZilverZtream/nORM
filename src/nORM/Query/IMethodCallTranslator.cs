namespace nORM.Query;

using System.Linq.Expressions;

internal interface IMethodCallTranslator
{
    Expression Translate(QueryTranslator translator, MethodCallExpression node);
}
