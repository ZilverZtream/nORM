using System.Linq.Expressions;

namespace nORM.Query
{
    internal sealed record WindowFunctionInfo(
        string FunctionName,
        LambdaExpression? ValueSelector,
        int Offset,
        LambdaExpression? DefaultValueSelector,
        string Alias,
        ParameterExpression ResultParameter,
        LambdaExpression ResultSelector
    );
}
