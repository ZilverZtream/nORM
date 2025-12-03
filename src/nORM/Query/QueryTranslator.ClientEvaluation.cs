using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;
using nORM.Providers;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        /// <summary>
        /// Analyzes a projection expression to determine if it contains untranslatable operations
        /// that require client-side evaluation.
        /// </summary>
        private sealed class TranslatabilityAnalyzer : ExpressionVisitor
        {
            private readonly DatabaseProvider _provider;
            private bool _hasUntranslatableExpression;
            private readonly HashSet<string> _sqlTranslatableMethods = new()
            {
                // String methods
                nameof(string.ToUpper),
                nameof(string.ToLower),
                nameof(string.Contains),
                nameof(string.StartsWith),
                nameof(string.EndsWith),
                nameof(string.Substring),
                nameof(string.Trim),
                nameof(string.TrimStart),
                nameof(string.TrimEnd),

                // Math methods
                nameof(Math.Abs),
                nameof(Math.Ceiling),
                nameof(Math.Floor),
                nameof(Math.Round),

                // DateTime properties
                nameof(DateTime.Year),
                nameof(DateTime.Month),
                nameof(DateTime.Day),
                nameof(DateTime.Hour),
                nameof(DateTime.Minute),
                nameof(DateTime.Second),

                // LINQ aggregate methods (when used in proper context)
                nameof(Enumerable.Count),
                nameof(Enumerable.Sum),
                nameof(Enumerable.Average),
                nameof(Enumerable.Min),
                nameof(Enumerable.Max)
            };

            public TranslatabilityAnalyzer(DatabaseProvider provider)
            {
                _provider = provider;
            }

            public bool HasUntranslatableExpression => _hasUntranslatableExpression;

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                // Check if this is a method that can be translated to SQL
                var declaringType = node.Method.DeclaringType;

                // Check if provider can translate this function
                if (declaringType != null)
                {
                    var translated = _provider.TranslateFunction(node.Method.Name, declaringType);
                    if (translated == null && !_sqlTranslatableMethods.Contains(node.Method.Name))
                    {
                        // This method cannot be translated to SQL
                        _hasUntranslatableExpression = true;
                    }
                }
                else
                {
                    // Unknown declaring type - assume untranslatable
                    _hasUntranslatableExpression = true;
                }

                return base.VisitMethodCall(node);
            }

            protected override Expression VisitInvocation(InvocationExpression node)
            {
                // Lambda invocations cannot be translated to SQL
                _hasUntranslatableExpression = true;
                return base.VisitInvocation(node);
            }

            protected override Expression VisitNew(NewExpression node)
            {
                // Anonymous types and simple constructors are OK
                if (node.Type.Name.StartsWith("<>"))
                {
                    // Anonymous type - this is fine
                    return base.VisitNew(node);
                }

                // Check if this is a simple value type constructor
                if (node.Type.IsValueType && node.Arguments.Count <= 1)
                {
                    return base.VisitNew(node);
                }

                // Other constructors may require client evaluation
                // but we'll allow them if they only use translatable sub-expressions
                return base.VisitNew(node);
            }
        }

        /// <summary>
        /// Extracts all member accesses from an expression that reference the parameter.
        /// These are the columns we need to fetch from the database.
        /// </summary>
        private sealed class MemberAccessExtractor : ExpressionVisitor
        {
            private readonly ParameterExpression _parameter;
            private readonly HashSet<MemberInfo> _accessedMembers = new();

            public MemberAccessExtractor(ParameterExpression parameter)
            {
                _parameter = parameter;
            }

            public IReadOnlySet<MemberInfo> AccessedMembers => _accessedMembers;

            protected override Expression VisitMember(MemberExpression node)
            {
                // Check if this member access is directly on our parameter
                if (node.Expression == _parameter)
                {
                    _accessedMembers.Add(node.Member);
                }

                return base.VisitMember(node);
            }
        }

        /// <summary>
        /// Attempts to split a projection into a server-side (SQL) projection and a client-side
        /// projection when the original contains untranslatable expressions.
        /// </summary>
        /// <param name="originalProjection">The original projection lambda.</param>
        /// <param name="serverProjection">Output: Lambda that selects only required columns from DB.</param>
        /// <param name="clientProjection">Output: Delegate that applies remaining logic client-side.</param>
        /// <returns>True if the projection was successfully split; false if it can be fully translated to SQL.</returns>
        private bool TrySplitProjection(
            LambdaExpression originalProjection,
            out LambdaExpression? serverProjection,
            out Func<object, object>? clientProjection)
        {
            serverProjection = null;
            clientProjection = null;

            // Analyze if the projection contains untranslatable expressions
            var analyzer = new TranslatabilityAnalyzer(_provider);
            analyzer.Visit(originalProjection.Body);

            if (!analyzer.HasUntranslatableExpression)
            {
                // Everything can be translated to SQL
                return false;
            }

            // Extract all member accesses we need from the database
            var extractor = new MemberAccessExtractor(originalProjection.Parameters[0]);
            extractor.Visit(originalProjection.Body);

            if (extractor.AccessedMembers.Count == 0)
            {
                // No database columns needed - this is a computed expression
                // We still need to fetch something, so we'll fetch all columns
                return false;
            }

            try
            {
                // Build server-side projection: select only the columns we need
                var parameter = originalProjection.Parameters[0];

                if (extractor.AccessedMembers.Count == 0)
                {
                    return false;
                }

                // Create a tuple or simple structure to hold the intermediate values
                // We'll use the actual entity type as intermediate, just fetching specific columns
                // This is simpler than dynamic type creation

                // For now, we'll create a member init expression that only initializes the needed members
                var memberInit = Expression.MemberInit(
                    Expression.New(parameter.Type),
                    extractor.AccessedMembers.Select(m =>
                        Expression.Bind(m, Expression.MakeMemberAccess(parameter, m)))
                );

                serverProjection = Expression.Lambda(memberInit, parameter);

                // Build client-side projection: take the intermediate object and apply original logic
                var intermediateParam = Expression.Parameter(typeof(object), "intermediate");
                var castIntermediate = Expression.Convert(intermediateParam, parameter.Type);

                // Replace the parameter in the original body with the cast intermediate
                var replacer = new ParameterReplacer(parameter, castIntermediate);
                var clientBody = replacer.Visit(originalProjection.Body);

                // Convert result to object
                var clientBodyAsObject = Expression.Convert(clientBody, typeof(object));
                var clientLambda = Expression.Lambda<Func<object, object>>(clientBodyAsObject, intermediateParam);

                // Compile the client-side projection
                clientProjection = ExpressionUtils.CompileWithFallback(clientLambda, default);

                return true;
            }
            catch (Exception ex)
            {
                // If we can't split the projection, fall back to letting the SQL translator
                // try (and likely fail with a better error message)
                System.Diagnostics.Debug.WriteLine($"Failed to split projection for client-side evaluation: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Replaces a parameter with a different expression throughout an expression tree.
        /// </summary>
        private sealed class ParameterReplacer : ExpressionVisitor
        {
            private readonly ParameterExpression _oldParameter;
            private readonly Expression _newExpression;

            public ParameterReplacer(ParameterExpression oldParameter, Expression newExpression)
            {
                _oldParameter = oldParameter;
                _newExpression = newExpression;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                return node == _oldParameter ? _newExpression : base.VisitParameter(node);
            }
        }
    }
}
