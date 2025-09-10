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

        /// <summary>
        /// Validates that an expression tree is within the supported complexity limits. The method
        /// throws an <see cref="InvalidOperationException"/> when the number of nodes or the depth
        /// of the expression exceeds predefined thresholds, protecting the system from pathologically
        /// large or recursive expressions.
        /// </summary>
        /// <param name="expression">The expression tree to analyze.</param>
        /// <exception cref="InvalidOperationException">Thrown when the expression is too complex or too deep.</exception>
        public static void ValidateExpression(Expression expression)
        {
            var complexity = AnalyzeExpressionComplexity(expression);
            if (complexity.NodeCount > MaxNodeCount)
                throw new InvalidOperationException($"Expression too complex: {complexity.NodeCount} nodes");
            if (complexity.Depth > MaxDepth)
                throw new InvalidOperationException($"Expression too deep: {complexity.Depth} levels");
        }

        /// <summary>
        /// Calculates an appropriate timeout to use when compiling an expression tree. The timeout is
        /// scaled based on the expression's complexity to avoid excessive waits for large trees while
        /// still allowing simple expressions to compile quickly.
        /// </summary>
        /// <param name="expression">The expression for which a compilation timeout is required.</param>
        /// <returns>A <see cref="TimeSpan"/> representing the maximum allowed compilation time.</returns>
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

        /// <summary>
        /// Compiles the provided <see cref="LambdaExpression"/> into a delegate and falls back to
        /// interpreter-based execution if the compilation is cancelled or not supported on the
        /// current platform.
        /// </summary>
        /// <param name="expression">The lambda expression to compile.</param>
        /// <param name="token">Token used to cancel the compilation operation.</param>
        /// <returns>A <see cref="Delegate"/> representing the compiled expression or an interpreted version.</returns>
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

            /// <summary>
            /// Visits a node within the expression tree and tracks overall complexity metrics
            /// such as total node count and maximum traversal depth.
            /// </summary>
            /// <param name="node">The current expression node.</param>
            /// <returns>The visited expression node.</returns>
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
