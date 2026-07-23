using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Query
{
    internal readonly struct ExpressionFingerprint : IEquatable<ExpressionFingerprint>
    {
        private readonly ulong _low;
        private readonly ulong _high;

        private static readonly ObjectPool<FingerprintVisitor> _visitorPool =
            new DefaultObjectPool<FingerprintVisitor>(new FingerprintVisitorPooledObjectPolicy(), Environment.ProcessorCount * 2);

        private ExpressionFingerprint(ulong low, ulong high)
        {
            _low = low;
            _high = high;
        }

        public static ExpressionFingerprint Compute(Expression expression)
            => ComputeCore(expression, includeClosureCollectionValues: false);

        public static ExpressionFingerprint ComputeForPlanCache(Expression expression)
            => ComputeCore(expression, includeClosureCollectionValues: true);

        /// <summary>
        /// Computes the plan-cache fingerprint AND collects the closure-captured parameter values in a SINGLE
        /// tree walk. The fingerprint bytes are identical to <see cref="ComputeForPlanCache"/> (value collection
        /// only appends to a list, never to the hash), and the collected values are identical in count and order
        /// to a separate <c>ParameterValueExtractor</c> walk — so the caller can skip that second traversal on
        /// the hot path. <paramref name="collectedValues"/> is owned by the caller (the pooled visitor is reset).
        /// </summary>
        public static ExpressionFingerprint ComputeForPlanCacheWithValues(Expression expression, out List<object?>? collectedValues)
        {
            var visitor = _visitorPool.Get();
            try
            {
                visitor.IncludeClosureCollectionValues = true;
                visitor.EnableValueCollection();
                visitor.Visit(expression);
                Span<byte> hash = stackalloc byte[16];
                visitor.GetCurrentHash(hash);
                var low = BinaryPrimitives.ReadUInt64LittleEndian(hash[..8]);
                var high = BinaryPrimitives.ReadUInt64LittleEndian(hash[8..]);
                collectedValues = visitor.CollectedValues; // captured before Return() resets the visitor
                return new ExpressionFingerprint(low, high);
            }
            finally
            {
                _visitorPool.Return(visitor);
            }
        }

        private static ExpressionFingerprint ComputeCore(Expression expression, bool includeClosureCollectionValues)
        {
            var visitor = _visitorPool.Get();
            try
            {
                visitor.IncludeClosureCollectionValues = includeClosureCollectionValues;
                visitor.Visit(expression);
                Span<byte> hash = stackalloc byte[16];
                visitor.GetCurrentHash(hash);
                var low = BinaryPrimitives.ReadUInt64LittleEndian(hash[..8]);
                var high = BinaryPrimitives.ReadUInt64LittleEndian(hash[8..]);
                return new ExpressionFingerprint(low, high);
            }
            finally
            {
                _visitorPool.Return(visitor);
            }
        }

        public ExpressionFingerprint Extend(int value)
        {
            Span<byte> buffer = stackalloc byte[20];
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[..8], _low);
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[8..16], _high);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[16..], value);
            var hash = XxHash128.Hash(buffer);
            var low = BinaryPrimitives.ReadUInt64LittleEndian(hash.AsSpan(0, 8));
            var high = BinaryPrimitives.ReadUInt64LittleEndian(hash.AsSpan(8, 8));
            return new ExpressionFingerprint(low, high);
        }

        /// <summary>
        /// Batch-extend with 5 int values in a single hash operation.
        /// Eliminates 4 intermediate XxHash128.Hash calls compared to 5 sequential Extend() calls.
        /// </summary>
        public ExpressionFingerprint Extend(int v1, int v2, int v3, int v4, int v5)
        {
            Span<byte> buffer = stackalloc byte[36]; // 16 (fingerprint) + 5×4 (ints)
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[..8], _low);
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[8..16], _high);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[16..], v1);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[20..], v2);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[24..], v3);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[28..], v4);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[32..], v5);
            var hash = XxHash128.Hash(buffer);
            var low = BinaryPrimitives.ReadUInt64LittleEndian(hash.AsSpan(0, 8));
            var high = BinaryPrimitives.ReadUInt64LittleEndian(hash.AsSpan(8, 8));
            return new ExpressionFingerprint(low, high);
        }

        public bool Equals(ExpressionFingerprint other) => _low == other._low && _high == other._high;
        public override bool Equals(object? obj) => obj is ExpressionFingerprint fp && Equals(fp);
        public override int GetHashCode() => HashCode.Combine(_low, _high);
        // Without this override, string concatenation (used in _simpleSqlCache keys) calls
        // the default struct ToString() which returns the type name for every instance, collapsing
        // all distinct fingerprints into the same key and causing wrong SQL to be reused.
        public override string ToString() => $"{_low:x16}{_high:x16}";

        private sealed class FingerprintVisitor : ExpressionVisitor
        {
            private readonly XxHash128 _hasher = new();
            private readonly Dictionary<ParameterExpression, int> _parameters = new();

            public bool IncludeClosureCollectionValues { get; set; }

            // Optional single-pass closure-value collection: when non-null, VisitMember records the same
            // closure-captured values (in the same document order) that the separate ParameterValueExtractor
            // walk would, so the plan-cache path can fingerprint AND collect parameter values in ONE tree walk.
            // The HASHING is untouched (identical bytes → identical fingerprint → zero collision risk); only
            // an extra list append is added. _suppressCollectionDepth replicates the extractor's early-return:
            // once a closure member is collected, nested member accesses under it are hashed but NOT collected.
            private bool _collectValues;
            private List<object?>? _collectedValues;   // lazily allocated on first collected value
            private int _suppressCollectionDepth;

            public void GetCurrentHash(Span<byte> destination) => _hasher.GetCurrentHash(destination);

            public void EnableValueCollection() => _collectValues = true;
            public List<object?>? CollectedValues => _collectedValues;

            public void Reset()
            {
                _hasher.Reset();
                _parameters.Clear();
                IncludeClosureCollectionValues = false;
                _collectValues = false;
                _collectedValues = null;
                _suppressCollectionDepth = 0;
            }

            public override Expression? Visit(Expression? node)
            {
                if (node == null)
                    return null;

                // Fingerprinting walks the tree recursively before translation ever runs,
                // so a deeply nested tree would overflow the stack here first. Fail with a
                // catchable query exception instead of a process-killing StackOverflowException.
                if (!System.Runtime.CompilerServices.RuntimeHelpers.TryEnsureSufficientExecutionStack())
                    throw new nORM.Core.NormQueryException(string.Format(nORM.Core.ErrorMessages.QueryTranslationFailed,
                        "Expression tree is nested too deeply to translate. Break the query into smaller operations."));

                AppendInt((int)node.NodeType);
                // Use TypeHandle (pointer-based identity) instead of FullName (UTF-8 encoding).
                // TypeHandle.Value is unique per type within a process and costs 8 bytes vs 40-200 bytes
                // for FullName encoding. For a join query with 25 nodes, this saves ~2.5KB of UTF-8 work.
                AppendLong(node.Type.TypeHandle.Value.ToInt64());

                return base.Visit(node);
            }

            protected override Expression VisitConstant(ConstantExpression node)
            {
                // Note: node.Type is already committed by Visit() via AppendLong(node.Type.TypeHandle).
                // Do NOT append it again here — a duplicate type-handle contribution makes the byte
                // stream inconsistent with other node kinds (MemberExpression, ParameterExpression, etc.)
                // which only receive the type handle once from Visit().

                // Include whether the constant is null as a bit in the fingerprint.
                // `x.NullableStr != null` and `x.NullableStr != "Alice"` generate completely different
                // SQL shapes (IS NOT NULL vs. col IS NULL OR col <> @p), so they must not share a plan.
                // 1 = null constant, 0 = non-null constant.
                AppendInt(node.Value is null ? 1 : 0);

                // Include stable full-value bytes instead of GetHashCode() (32-bit, collision-prone).
                // GetHashCode() on strings/longs/doubles loses bit precision and causes wrong-plan reuse.
                if (node.Value is not null && node.Value is not IQueryable)
                    AppendStableValue(node.Value);

                return base.VisitConstant(node);
            }

            // The only reflection/dynamic-code call below (QueryTranslator.TryGetConstantValue) is
            // short-circuit-guarded by _collectValues, which is enabled solely by
            // ComputeForPlanCacheWithValues - the runtime query-plan value-collection path, itself
            // RequiresUnreferencedCode/RequiresDynamicCode via NormQueryProvider. The AOT-relevant
            // fingerprint-only path (Compute/ComputeForPlanCache) never enables collection, so the
            // call is unreachable there. VisitMember overrides a non-annotated base, so annotating it
            // RequiresUnreferencedCode would itself be an IL2046 mismatch; suppress instead.
            [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026",
                Justification = "Guarded by _collectValues, enabled only on the RequiresUnreferencedCode value-collection path.")]
            [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050",
                Justification = "Guarded by _collectValues, enabled only on the RequiresDynamicCode value-collection path.")]
            protected override Expression VisitMember(MemberExpression node)
            {
                // Stable per-process member id (trim/NativeAOT-safe) + declaring type handle.
                // GetStableMemberId replaces MemberInfo.MetadataToken, which is unavailable under NativeAOT.
                AppendInt(GetStableMemberId(node.Member));
                if (node.Member.DeclaringType != null)
                    AppendLong(node.Member.DeclaringType.TypeHandle.Value.ToInt64());

                // For closure captures, include nullness because null/non-null values can
                // produce different SQL shapes. Captured collections are expanded into fixed
                // IN-clause parameters, so their contents also participate in the fingerprint.
                if (TryGetClosureValue(node, out var capturedValue))
                {
                    AppendInt(capturedValue is null ? 1 : 0);
                    if (IncludeClosureCollectionValues &&
                        capturedValue is System.Collections.IEnumerable enumerable &&
                        capturedValue is not string &&
                        capturedValue is not byte[] &&
                        capturedValue is not IQueryable)
                    {
                        AppendEnumerableStableValue(enumerable);
                    }
                }

                // Single-pass parameter-value collection (opt-in). Mirrors ParameterValueExtractor.VisitMember
                // EXACTLY: collect on QueryTranslator.TryGetConstantValue success, then suppress collection
                // (not hashing) for the descent so nested closure members under a collected one are not
                // double-counted (the extractor early-returns; we keep descending to hash but stop collecting).
                var collectedHere = false;
                if (_collectValues && _suppressCollectionDepth == 0
                    && QueryTranslator.TryGetConstantValue(node, out var collectValue))
                {
                    (_collectedValues ??= new List<object?>()).Add(collectValue ?? DBNull.Value);
                    collectedHere = true;
                }

                if (collectedHere)
                    _suppressCollectionDepth++;
                var result = base.VisitMember(node);
                if (collectedHere)
                    _suppressCollectionDepth--;
                return result;
            }

            private static bool TryGetClosureValue(MemberExpression node, out object? value)
            {
                if (!HasConstantRoot(node))
                {
                    value = null;
                    return false;
                }

                try
                {
                    return ExpressionValueExtractor.TryGetConstantValue(node, out value);
                }
                catch (Exception ex) when (ex is MemberAccessException
                                                or TargetInvocationException
                                                or InvalidOperationException
                                                or NotSupportedException
                                                or UnauthorizedAccessException)
                {
                    value = null;
                    return false;
                }
            }

            private static bool HasConstantRoot(MemberExpression node)
            {
                Expression? current = node.Expression;
                while (current is MemberExpression member)
                    current = member.Expression;
                return current is ConstantExpression;
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                // Stable per-process method id (trim/NativeAOT-safe) + declaring type handle.
                // GetStableMemberId replaces MethodInfo.MetadataToken, which is unavailable under NativeAOT.
                // Distinct MethodInfos (including overloads and constructed generics) get distinct ids.
                AppendInt(GetStableMemberId(node.Method));
                if (node.Method.DeclaringType != null)
                    AppendLong(node.Method.DeclaringType.TypeHandle.Value.ToInt64());
                AppendInt(node.Arguments.Count);
                return base.VisitMethodCall(node);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (!_parameters.TryGetValue(node, out var id))
                {
                    id = _parameters.Count;
                    _parameters[node] = id;
                }
                AppendInt(id);
                // Use TypeHandle instead of FullName
                AppendLong(node.Type.TypeHandle.Value.ToInt64());
                return base.VisitParameter(node);
            }

            protected override Expression VisitLambda<T>(Expression<T> node)
            {
                AppendInt(node.Parameters.Count);
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

            // Trim/NativeAOT-safe replacement for MemberInfo.MetadataToken, which throws
            // "no metadata token available for the given member" for trimmed members under NativeAOT.
            // Each distinct member gets a stable per-process id via reference identity (MemberInfo
            // equality identifies the underlying member, including overloads and declaring type), so it
            // is strictly more unique than a module-local token. The plan cache is in-memory per process,
            // so per-process id stability is sufficient; the map is bounded by the small set of distinct
            // members that appear in query expressions.
            private static readonly System.Collections.Concurrent.ConcurrentDictionary<MemberInfo, int> _memberIds = new();
            private static int _memberIdCounter;
            private static int GetStableMemberId(MemberInfo member)
                => _memberIds.GetOrAdd(member, static _ => System.Threading.Interlocked.Increment(ref _memberIdCounter));

            private void AppendInt(int value)
            {
                Span<byte> data = stackalloc byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(data, value);
                _hasher.Append(data);
            }

            private void AppendLong(long value)
            {
                Span<byte> data = stackalloc byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(data, value);
                _hasher.Append(data);
            }
            private void AppendEnumerableStableValue(System.Collections.IEnumerable values)
            {
                AppendString("IEnumerable");
                var count = 0;
                foreach (var item in values)
                {
                    count++;
                    AppendInt(item is null ? 1 : 0);
                    if (item != null)
                    {
                        AppendString(item.GetType().FullName ?? string.Empty);
                        AppendStableValue(item);
                    }
                }
                AppendInt(count);
            }

            // Stable value hashing — emits full bytes for each type so two constants
            // that differ only in high bits (e.g., longs with same low 32 bits) get distinct fingerprints.
            private void AppendStableValue(object value)
            {
                switch (value)
                {
                    case string s:
                        AppendString(s);
                        break;
                    case int v:    AppendInt(v); break;
                    case uint v:   AppendLong(v); break; // zero-extend to 64 bits; (int)v would sign-flip values > int.MaxValue
                    case short v:  AppendInt(v); break;
                    case ushort v: AppendInt(v); break;
                    case byte v:   AppendInt(v); break;
                    case sbyte v:  AppendInt(v); break;
                    case char v:   AppendInt(v); break;
                    case bool v:   AppendInt(v ? 1 : 0); break;
                    case long v:   AppendLong(v); break;
                    case ulong v:  AppendLong((long)v); break;
                    case float v:  AppendInt(BitConverter.SingleToInt32Bits(v)); break;
                    case double v: AppendLong(BitConverter.DoubleToInt64Bits(v)); break;
                    case decimal v:
                        var bits = decimal.GetBits(v);
                        AppendInt(bits[0]); AppendInt(bits[1]); AppendInt(bits[2]); AppendInt(bits[3]);
                        break;
                    case Guid v:   AppendGuid(v); break;
                    case DateTime v:
                        AppendLong(v.Ticks);
                        AppendInt((int)v.Kind);
                        break;
                    case DateTimeOffset v:
                        AppendLong(v.Ticks);
                        AppendLong(v.Offset.Ticks);
                        break;
                    case DateOnly v:
                        // G1: Use DayNumber (stable int) instead of ToString() (culture-dependent).
                        // DateOnly(2024, 7, 15).ToString() returns "7/15/2024" in en-US but different
                        // formats in other cultures, causing spurious cache misses across cultures.
                        AppendInt(v.DayNumber);
                        break;
                    case TimeOnly v:
                        // G1: Use Ticks (stable long) instead of ToString() (culture-dependent).
                        AppendLong(v.Ticks);
                        break;
                    case byte[] v:
                        AppendInt(v.Length);
                        _hasher.Append(v);
                        break;
                    default:
                        if (value.GetType().IsEnum)
                        {
                            // Convert.ToInt32 overflows for long/ulong-backed enums outside Int32 range.
                            // Use the underlying type to choose the correct serialization path.
                            var underlying = Enum.GetUnderlyingType(value.GetType());
                            var raw = Convert.ChangeType(value, underlying);
                            if (raw is long lv)         AppendLong(lv);
                            else if (raw is ulong ulv)  AppendLong((long)ulv);
                            else if (raw is int iv)     AppendInt(iv);
                            else if (raw is uint uiv)   AppendLong(uiv);
                            else if (raw is short sv)   AppendInt(sv);
                            else if (raw is ushort usv) AppendInt(usv);
                            else if (raw is byte bv)    AppendInt(bv);
                            else if (raw is sbyte sbv)  AppendInt(sbv);
                            else                        AppendString(raw.ToString() ?? string.Empty);
                        }
                        else
                        {
                            // Use type name + ToString() for a stable, collision-resistant fingerprint.
                            AppendString(value.GetType().FullName ?? string.Empty);
                            AppendString(value.ToString() ?? string.Empty);
                        }
                        break;
                }
            }

            private void AppendGuid(Guid value)
            {
                Span<byte> data = stackalloc byte[16];
                value.TryWriteBytes(data);
                _hasher.Append(data);
            }

            // Optimized string hashing with reduced allocations.
            // Format: [4-byte UTF-8 byte count LE] [UTF-8 bytes]
            // The prefix is the UTF-8 byte count (not the UTF-16 char count) so that the
            // stream is self-consistent: the prefix matches the number of bytes that follow.
            private void AppendString(string value)
            {
                if (value.Length == 0)
                {
                    // Write a 4-byte zero length prefix and no body.
                    Span<byte> zeroLen = stackalloc byte[4];
                    BinaryPrimitives.WriteInt32LittleEndian(zeroLen, 0);
                    _hasher.Append(zeroLen);
                    return;
                }

                // UTF-8 worst case: 4 bytes per UTF-16 code unit. Surrogate pairs encode one
                // supplementary codepoint as 4 UTF-8 bytes (= 2 bytes per UTF-16 char), but a
                // lone BMP char can be up to 3 bytes. Therefore value.Length * 4 bytes is always
                // sufficient for any valid UTF-16 input. Using * 3 was insufficient for strings
                // composed entirely of supplementary-plane characters.
                // The chunked slow path was also unsafe: slicing at a char boundary can split
                // a surrogate pair, producing a lone surrogate that Encoding.UTF8 rejects.
                const int MaxStackBufferSize = 512;

                // Fast path: encode the full string in one shot if it fits on the stack.
                if (value.Length * 4 <= MaxStackBufferSize)
                {
                    Span<byte> buffer = stackalloc byte[MaxStackBufferSize];
                    int written = System.Text.Encoding.UTF8.GetBytes(value, buffer);
                    // Write the actual UTF-8 byte count as prefix (not the char count).
                    Span<byte> lengthPrefix = stackalloc byte[4];
                    BinaryPrimitives.WriteInt32LittleEndian(lengthPrefix, written);
                    _hasher.Append(lengthPrefix);
                    _hasher.Append(buffer[..written]);
                    return;
                }

                // Slow path: heap-allocate for strings longer than ~128 chars
                // (rare — type names and SQL constant values are almost always short).
                var bytes = System.Text.Encoding.UTF8.GetBytes(value);
                Span<byte> slowLen = stackalloc byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(slowLen, bytes.Length);
                _hasher.Append(slowLen);
                _hasher.Append(bytes);
            }
        }

        private sealed class FingerprintVisitorPooledObjectPolicy : PooledObjectPolicy<FingerprintVisitor>
        {
            public override FingerprintVisitor Create() => new();

            public override bool Return(FingerprintVisitor obj)
            {
                obj.Reset();
                return true;
            }
        }
    }
}
