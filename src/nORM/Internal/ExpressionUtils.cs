using System;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Internal
{
    internal static class ExpressionUtils
    {
        private const int MaxNodeCount = 10000;
        private const int MaxDepth = 100;

        internal sealed class Complexity
        {
            public int NodeCount { get; init; }
            public int Depth { get; init; }
        }

        public static Complexity AnalyzeExpressionComplexity(Expression expression)
        {
            var visitor = new ComplexityVisitor();
            visitor.Visit(expression);
            return new Complexity { NodeCount = visitor.NodeCount, Depth = visitor.MaxDepth };
        }

        public static void ValidateExpression(Expression expression)
        {
            var complexity = AnalyzeExpressionComplexity(expression);
            if (complexity.NodeCount > MaxNodeCount)
                throw new InvalidOperationException($"Expression too complex: {complexity.NodeCount} nodes");
            if (complexity.Depth > MaxDepth)
                throw new InvalidOperationException($"Expression too deep: {complexity.Depth} levels");
        }

        public static TimeSpan GetCompilationTimeout(Expression expression)
        {
            var complexity = AnalyzeExpressionComplexity(expression);
            var multiplier = Math.Max(1, Math.Max(complexity.NodeCount / 1000, complexity.Depth / 10));
            var timeout = TimeSpan.FromSeconds(30 * multiplier);
            var maxTimeout = TimeSpan.FromMinutes(5);
            return timeout > maxTimeout ? maxTimeout : timeout;
        }

        public static TDelegate CompileWithFallback<TDelegate>(Expression<TDelegate> expression, CancellationToken token)
        {
            if (!RuntimeFeature.IsDynamicCodeSupported || !RuntimeFeature.IsDynamicCodeCompiled)
                return expression.Compile(preferInterpretation: true);

            var task = Task.Run(() => expression.Compile(), token);
            try
            {
                task.Wait(token);
                return task.Result;
            }
            catch (OperationCanceledException)
            {
                return expression.Compile(preferInterpretation: true);
            }
            catch (PlatformNotSupportedException)
            {
                return expression.Compile(preferInterpretation: true);
            }
        }

        public static Delegate CompileWithFallback(LambdaExpression expression, CancellationToken token)
        {
            if (!RuntimeFeature.IsDynamicCodeSupported || !RuntimeFeature.IsDynamicCodeCompiled)
                return expression.Compile(preferInterpretation: true);

            var task = Task.Run(expression.Compile, token);
            try
            {
                task.Wait(token);
                return task.Result;
            }
            catch (OperationCanceledException)
            {
                return expression.Compile(preferInterpretation: true);
            }
            catch (PlatformNotSupportedException)
            {
                return expression.Compile(preferInterpretation: true);
            }
        }

        private sealed class ComplexityVisitor : ExpressionVisitor
        {
            public int NodeCount { get; private set; }
            public int MaxDepth { get; private set; }
            private int _currentDepth;

            public override Expression? Visit(Expression? node)
            {
                if (node == null)
                    return null;
                NodeCount++;
                _currentDepth++;
                if (_currentDepth > MaxDepth)
                    MaxDepth = _currentDepth;
                var result = base.Visit(node);
                _currentDepth--;
                return result;
            }
        }
    }
}
