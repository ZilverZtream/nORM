using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using System.Collections.Frozen;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
#nullable enable
namespace nORM.Query
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
    internal sealed partial class ExpressionToSqlVisitor : ExpressionVisitor, nORM.Internal.IResettable, IDisposable
    {
        private DbContext _ctx = null!;
        private TableMapping _mapping = null!;
        private DatabaseProvider _provider = null!;
        private readonly Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _parameterMappings = new();
        private ParameterExpression _parameter = null!;
        private string _tableAlias = string.Empty;
        private OptimizedSqlBuilder _sql = null!;
        private readonly Dictionary<string, object> _params = new();
        // Value converters keyed by the compiled-parameter name of a closure value compared against a
        // value-converter column. Shared (like _compiledParams) with the owning translator so the
        // plan can apply them to the extractor-supplied value at execution time — a fingerprint-cached
        // plan is reused across differing captured values and cannot bake the conversion.
        private readonly Dictionary<string, nORM.Mapping.IValueConverter> _ownedParamConverters = new();
        private Dictionary<string, nORM.Mapping.IValueConverter> _paramConverters = null!;
        // Parameter sink (can be redirected to a shared dictionary)
        private Dictionary<string, object> _paramSink = null!;
        private int _paramIndex = 0;
        internal int ParamIndex => _paramIndex;
        private readonly List<string> _ownedCompiledParams = new();
        private readonly Dictionary<ParameterExpression, string> _ownedParamMap = new();
        private List<string> _compiledParams = null!;
        private Dictionary<ParameterExpression, string> _paramMap = null!;
        private bool _suppressNullCheck = false;
        // Outer translator's recursion depth so BuildExists/BuildIn pass depth+1 to sub-translators.
        private int _recursionDepth = 0;
        private const int ConstParamMapLimit = 1024;
        /// <summary>Maximum allowed length for a JSON path in <c>Json.Value()</c>.</summary>
        private const int MaxJsonPathLength = 500;
        /// <summary>SQL false literal used when a local collection is empty.</summary>
        private const string SqlFalseLiteral = "(1=0)";
        /// <summary>
        /// Maximum length (in characters or bytes) for inline string/binary parameters.
        /// Based on SQL Server's non-MAX varchar/varbinary threshold.
        /// </summary>
        private const int MaxInlineParameterLength = 8000;
        private readonly Dictionary<ConstKey, string> _constParamMap = new();
        private readonly Dictionary<(ParameterExpression Param, string Member), string> _memberParamMap = new();
        private static readonly Expression s_emptyExpression = Expression.Empty();
        private readonly Dictionary<ParameterExpression, string> _groupingKeys = new();
        // String methods (Contains, StartsWith, EndsWith) are handled via _fastMethodHandlers
        // for better performance, avoiding a redundant _translators dictionary lookup.
        private static readonly Dictionary<MethodInfo, Action<ExpressionToSqlVisitor, MethodCallExpression>> _fastMethodHandlers =
            BuildFastMethodHandlers();

        private static Dictionary<MethodInfo, Action<ExpressionToSqlVisitor, MethodCallExpression>> BuildFastMethodHandlers()
        {
            var dict = new Dictionary<MethodInfo, Action<ExpressionToSqlVisitor, MethodCallExpression>>
            {
                { typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string) })!, HandleStringContains },
                { typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string) })!, HandleStringStartsWith },
                { typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) })!, HandleStringEndsWith }
            };
            // 2-arg StringComparison overloads — common in user predicates. We honour
            // ordinal-ignore-case / invariant-ignore-case / current-ignore-case by wrapping
            // both sides of the LIKE / equality compare in LOWER(); culture-specific
            // collation isn't reachable from SQL but case-folding to lower is the
            // de-facto cross-provider approximation.
            var containsCi = typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string), typeof(StringComparison) });
            if (containsCi != null) dict.Add(containsCi, HandleStringContainsWithComparison);
            var startsCi = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string), typeof(StringComparison) });
            if (startsCi != null) dict.Add(startsCi, HandleStringStartsWithComparison);
            var endsCi = typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string), typeof(StringComparison) });
            if (endsCi != null) dict.Add(endsCi, HandleStringEndsWithComparison);
            var equalsInstanceCi = typeof(string).GetMethod(nameof(string.Equals), new[] { typeof(string), typeof(StringComparison) });
            if (equalsInstanceCi != null) dict.Add(equalsInstanceCi, HandleStringEqualsInstanceWithComparison);
            var equalsStaticCi = typeof(string).GetMethod(nameof(string.Equals), new[] { typeof(string), typeof(string), typeof(StringComparison) });
            if (equalsStaticCi != null) dict.Add(equalsStaticCi, HandleStringEqualsStaticWithComparison);
            // char-needle overloads: identical semantics to a one-character string, so they
            // share the string handlers — EmitLikePredicate parameterizes the needle and the
            // char value binds like any pattern. Without these registrations the shape fell
            // through to the provider TranslateFunction tail, which only SQLite implements
            // for Contains/StartsWith/EndsWith: the same query worked there and threw on
            // every server provider.
            var containsChar = typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(char) });
            if (containsChar != null) dict.Add(containsChar, HandleStringContains);
            var startsChar = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(char) });
            if (startsChar != null) dict.Add(startsChar, HandleStringStartsWith);
            var endsChar = typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(char) });
            if (endsChar != null) dict.Add(endsChar, HandleStringEndsWith);
            var containsCharCi = typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(char), typeof(StringComparison) });
            if (containsCharCi != null) dict.Add(containsCharCi, HandleStringContainsWithComparison);
            return dict;
        }

        private static bool IsIgnoreCase(Expression comparisonArg)
        {
            if (TryGetConstantValue(comparisonArg, out var v) && v is StringComparison sc)
            {
                return sc is StringComparison.OrdinalIgnoreCase
                          or StringComparison.InvariantCultureIgnoreCase
                          or StringComparison.CurrentCultureIgnoreCase;
            }
            return false;
        }
        private static bool TryResolveStringCompareMode(Expression comparisonArg, out bool ignoreCase, out bool forceCaseSensitive)
        {
            ignoreCase = false;
            forceCaseSensitive = false;
            if (!TryGetConstantValue(comparisonArg, out var v))
                return false;

            if (v is bool boolIgnoreCase)
            {
                ignoreCase = boolIgnoreCase;
                forceCaseSensitive = !boolIgnoreCase;
                return true;
            }

            if (v is StringComparison sc)
            {
                ignoreCase = sc is StringComparison.OrdinalIgnoreCase
                    or StringComparison.InvariantCultureIgnoreCase
                    or StringComparison.CurrentCultureIgnoreCase;
                forceCaseSensitive = !ignoreCase;
                return true;
            }

            return false;
        }
        internal ExpressionToSqlVisitor() { }
        public ExpressionToSqlVisitor(DbContext ctx, TableMapping mapping, DatabaseProvider provider,
                                      ParameterExpression parameter, string tableAlias,
                                      Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? correlated = null,
                                      List<string>? compiledParams = null,
                                      Dictionary<ParameterExpression, string>? paramMap = null,
                                      Dictionary<string, nORM.Mapping.IValueConverter>? paramConverters = null)
        {
            var context = new VisitorContext(ctx ?? throw new ArgumentNullException(nameof(ctx)), mapping ?? throw new ArgumentNullException(nameof(mapping)), provider ?? throw new ArgumentNullException(nameof(provider)), parameter ?? throw new ArgumentNullException(nameof(parameter)), tableAlias ?? throw new ArgumentNullException(nameof(tableAlias)), correlated, compiledParams, paramConverters, paramMap);
            Initialize(in context);
        }
        /// <summary>
        /// Initializes the visitor with all context required to translate a LINQ expression into
        /// SQL. This method can be called multiple times to reuse the same instance for different
        /// translations.
        /// </summary>
        /// <param name="context">A structure containing the current <see cref="DbContext"/>,
        /// table mapping, provider and parameter information.</param>
        public void Initialize(in VisitorContext context)
        {
            _paramSink = _params;
            _ctx = context.Context;
            _mapping = context.Mapping;
            _provider = context.Provider;
            _parameter = context.Parameter;
            _tableAlias = context.TableAlias;
            _parameterMappings.Clear();
            if (context.Correlated != null)
            {
                foreach (var kvp in context.Correlated)
                    _parameterMappings[kvp.Key] = kvp.Value;
            }
            _parameterMappings[context.Parameter] = (context.Mapping, context.TableAlias);
            _compiledParams = context.CompiledParams ?? _ownedCompiledParams;
            if (context.CompiledParams == null)
                _ownedCompiledParams.Clear();
            _paramConverters = context.ParamConverters ?? _ownedParamConverters;
            if (context.ParamConverters == null)
                _ownedParamConverters.Clear();
            _paramMap = context.ParamMap ?? _ownedParamMap;
            if (context.ParamMap == null)
                _ownedParamMap.Clear();
            _constParamMap.Clear();
            // Start numbering from the caller-supplied offset so that a visitor sharing
            // _compiledParams/_paramMap with a previous visitor does not reuse parameter names
            // (e.g. both inner Where and global-filter Where getting @p0).
            _paramIndex = context.ParamIndexStart;
            _suppressNullCheck = false;
            _recursionDepth = context.RecursionDepth;
            _memberParamMap.Clear();
            _groupingKeys.Clear();
        }
        /// <summary>
        /// Resets the internal state so that the visitor can be returned to an object pool and
        /// reused for subsequent translations.
        /// </summary>
        public void Reset()
        {
            _sql = null!;
            _params.Clear();
            _paramSink = null!;
            _paramIndex = 0;
            _parameterMappings.Clear();
            _ownedCompiledParams.Clear();
            _ownedParamMap.Clear();
            _compiledParams = null!;
            _paramMap = null!;
            _ctx = null!;
            _mapping = null!;
            _provider = null!;
            _parameter = null!;
            _tableAlias = string.Empty;
            _suppressNullCheck = false;
            _recursionDepth = 0;
            _constParamMap.Clear();
            _memberParamMap.Clear();
            _groupingKeys.Clear();
            _ownedParamConverters.Clear();
            _paramConverters = null!;
        }
        /// <summary>
        /// Releases resources by resetting the visitor's state. The instance can be reused after
        /// calling this method.
        /// </summary>
        public void Dispose()
        {
            Reset();
        }
        /// <summary>
        /// Translates the supplied LINQ expression tree into an SQL string using the configured
        /// provider and mapping information.
        /// </summary>
        /// <param name="expression">The expression to translate.</param>
        /// <returns>The SQL text corresponding to the expression.</returns>
        public string Translate(Expression expression)
        {
            using var builder = new OptimizedSqlBuilder();
            _sql = builder;
            if (!TryEmitMappedBooleanPredicate(expression, expectedValue: true))
                Visit(expression);
            return builder.ToSqlString();
        }
        /// <summary>
        /// Directs the visitor to use the provided dictionary for parameter
        /// collection, allowing multiple visitors to share a common parameter
        /// store.
        /// </summary>
        /// <param name="shared">The dictionary to populate with parameters, or
        /// <c>null</c> to revert to the visitor's internal dictionary.</param>
        public void UseSharedParameterDictionary(Dictionary<string, object> shared)
        {
            _paramSink = shared ?? _params;
        }

        /// <summary>
        /// Registers a SQL expression for the <c>Key</c> property of a grouping parameter,
        /// so that subsequent <c>g.Key</c> accesses emit the correct SQL column reference.
        /// </summary>
        public void RegisterGroupingKey(ParameterExpression parameter, string keySql)
        {
            _groupingKeys[parameter] = keySql;
        }

        /// <summary>
        /// Translates a <c>GroupBy</c> method call into the corresponding SQL <c>GROUP BY</c>
        /// clause, registering grouping key bindings for downstream aggregate expressions.
        /// </summary>
        private void HandleGroupByMethod(MethodCallExpression node)
        {
            var keySelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "GroupBy key selector must be a lambda expression"));

            var keySql = GetSql(keySelector.Body);

            // Register grouping scope for downstream accesses to g.Key
            if (node.Arguments.Count > 2 && StripQuotes(node.Arguments[2]) is LambdaExpression resultSelector)
            {
                if (resultSelector.Parameters.Count > 1)
                {
                    RegisterGroupingKey(resultSelector.Parameters[1], keySql);
                    Visit(resultSelector.Body);
                }
                else
                {
                    Visit(resultSelector.Body);
                }
            }
            else
            {
                _sql.Append(keySql);
            }

            _sql.AppendGroupBy(keySql);
        }



        /// <summary>
        /// Quickly resets the visitor to a clean state so that it can be reused
        /// without allocating a new instance.
        /// </summary>
        /// <remarks>
        /// This method clears accumulated SQL, parameters, and internal caches
        /// while preserving preallocated buffers when possible.
        /// </remarks>
        public void FastReset()
        {
            if (_sql != null) _sql.Clear();
            _params.Clear();
            if (_paramSink != null && !ReferenceEquals(_paramSink, _params))
                _paramSink.Clear();
            _paramIndex = 0;
            _suppressNullCheck = false;
            _constParamMap.Clear();
            _memberParamMap.Clear();
            _ownedCompiledParams.Clear();
            _ownedParamMap.Clear();
            _ownedParamConverters.Clear();
        }
    }
}
