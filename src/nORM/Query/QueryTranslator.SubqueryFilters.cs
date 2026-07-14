using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        // ── Global/tenant filters inside correlated subqueries ─────────────────
        //
        // NormQueryProvider.ApplyGlobalFilters rewrites the TOP-LEVEL expression
        // only: subquery roots living inside predicate or projection LAMBDAS are
        // never reached, so an explicit `ctx.Query<Child>()` correlated subquery
        // ran UNFILTERED — with a tenant filter configured that is a silent
        // cross-tenant leak through Any/All/Contains and every scalar aggregate.
        // The sub-translation sites call this helper on their composed source to
        // wrap the subquery ROOT (the Query call or a captured IQueryable) with
        // the same combined global-filter predicate and tenant equality the
        // provider applies to top-level roots.
        //
        // The injected filter is NOT part of the user's expression tree, so any
        // closure-dependent value inside it (a two-parameter ctx filter reading
        // live state) cannot use compiled slots — the extractor would never see
        // it. Such values are inlined as constants and the caller must mark the
        // plan fold-no-cache (the established `_unused`-name channel); constant
        // filter bodies (the typical `x => x.TenantId == "T1"`) stay cacheable,
        // and the tenant-provider constant is safe because the plan-cache key
        // already includes the tenant hash.

        internal static Expression ApplySubqueryRootFilters(DbContext ctx, Expression source, out bool foldedFilterValues)
        {
            foldedFilterValues = false;
            if (ctx.Options.GlobalFilters.Count == 0 && ctx.Options.TenantProvider == null)
                return source;
            return RewriteSubqueryRootWithFilters(ctx, source, ref foldedFilterValues);
        }

        private static Expression RewriteSubqueryRootWithFilters(DbContext ctx, Expression node, ref bool folded)
        {
            if (node is MethodCallExpression mce)
            {
                if (mce.Method.DeclaringType == typeof(NormQueryable) && mce.Method.Name == nameof(NormQueryable.Query))
                    return WrapSubqueryRoot(ctx, mce, ref folded);
                if (mce.Arguments.Count == 0)
                    return node;
                var rewritten = RewriteSubqueryRootWithFilters(ctx, mce.Arguments[0], ref folded);
                if (ReferenceEquals(rewritten, mce.Arguments[0]))
                    return node;
                var args = mce.Arguments.ToArray();
                args[0] = rewritten;
                return mce.Update(mce.Object, args);
            }
            if (node is ConstantExpression or MemberExpression && typeof(IQueryable).IsAssignableFrom(node.Type))
                return WrapSubqueryRoot(ctx, node, ref folded);
            return node;
        }

        private static Expression WrapSubqueryRoot(DbContext ctx, Expression root, ref bool folded)
        {
            var elementType = GetQueryableElementType(root.Type);
            if (elementType == null)
                return root;

            var result = root;

            List<Expression>? bodies = null;
            ParameterExpression? param = null;
            foreach (var kvp in ctx.Options.GlobalFilters)
            {
                if (!kvp.Key.IsAssignableFrom(elementType)) continue;
                foreach (var filter in kvp.Value)
                {
                    param ??= Expression.Parameter(elementType, "sgf");
                    Expression body = filter.Body;
                    if (filter.Parameters.Count == 2)
                        body = new ParameterReplacer(filter.Parameters[0], Expression.Constant(ctx)).Visit(body)!;
                    body = new ParameterReplacer(filter.Parameters[^1], param).Visit(body)!;
                    var inliner = new StandaloneCapturedValueInliner();
                    body = inliner.Visit(body)!;
                    folded |= inliner.Inlined;
                    (bodies ??= new List<Expression>()).Add(body);
                }
            }
            if (bodies != null)
            {
                var combined = bodies[0];
                for (var i = 1; i < bodies.Count; i++)
                    combined = Expression.AndAlso(combined, bodies[i]);
                result = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType },
                    result, Expression.Quote(Expression.Lambda(combined, param!)));
            }

            if (ctx.Options.TenantProvider != null)
            {
                var map = ctx.GetMapping(elementType);
                var tenantCol = ctx.RequireTenantColumn(map, "query");
                var tenantParam = Expression.Parameter(elementType, "stf");
                var body = Expression.Equal(
                    Expression.Property(tenantParam, tenantCol.Prop.Name),
                    Expression.Constant(ctx.GetRequiredTenantId(map, "query"), tenantCol.Prop.PropertyType));
                result = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType },
                    result, Expression.Quote(Expression.Lambda(body, tenantParam)));
            }

            return result;
        }

        private static Type? GetQueryableElementType(Type type)
        {
            if (type.IsGenericType && typeof(IQueryable).IsAssignableFrom(type))
                return type.GetGenericArguments()[0];
            return type.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>))
                ?.GetGenericArguments()[0];
        }

        /// <summary>
        /// Inlines closure-resolvable member accesses as constants, reporting whether
        /// anything was inlined. Injected filter fragments are outside the user's
        /// expression tree, so their values can never bind through compiled slots.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Closure lifting evaluates expression trees at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Closure lifting reflects over closure members; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class StandaloneCapturedValueInliner : ExpressionVisitor
        {
            public bool Inlined { get; private set; }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (TryGetConstantValue(node, out var value))
                {
                    Inlined = true;
                    return Expression.Constant(value, node.Type);
                }
                return base.VisitMember(node);
            }
        }
    }
}
