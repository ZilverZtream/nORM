using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq.Expressions;
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
        {
            var visitor = _visitorPool.Get();
            try
            {
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

        public bool Equals(ExpressionFingerprint other) => _low == other._low && _high == other._high;
        public override bool Equals(object? obj) => obj is ExpressionFingerprint fp && Equals(fp);
        public override int GetHashCode() => HashCode.Combine(_low, _high);

        private sealed class FingerprintVisitor : ExpressionVisitor
        {
            private readonly XxHash128 _hasher = new();
            private readonly Dictionary<ParameterExpression, int> _parameters = new();

            public void GetCurrentHash(Span<byte> destination) => _hasher.GetCurrentHash(destination);

            public void Reset()
            {
                _hasher.Reset();
                _parameters.Clear();
            }

            public override Expression? Visit(Expression? node)
            {
                if (node == null)
                    return null;

                AppendInt((int)node.NodeType);
                AppendString(node.Type.FullName ?? string.Empty);

                return base.Visit(node);
            }

            protected override Expression VisitConstant(ConstantExpression node)
            {
                // Ignore constant value; base.VisitConstant does nothing
                AppendString(node.Type.FullName ?? string.Empty);
                return base.VisitConstant(node);
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                AppendGuid(node.Member.Module.ModuleVersionId);
                AppendInt(node.Member.MetadataToken);
                return base.VisitMember(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                AppendGuid(node.Method.Module.ModuleVersionId);
                AppendInt(node.Method.MetadataToken);
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
                AppendString(node.Type.FullName ?? string.Empty);
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

            private void AppendInt(int value)
            {
                Span<byte> data = stackalloc byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(data, value);
                _hasher.Append(data);
            }

            private void AppendGuid(Guid value)
            {
                Span<byte> data = stackalloc byte[16];
                value.TryWriteBytes(data);
                _hasher.Append(data);
            }

            // PERFORMANCE OPTIMIZATION 10: Optimized string hashing with reduced allocations
            private void AppendString(string value)
            {
                Span<byte> length = stackalloc byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(length, value.Length);
                _hasher.Append(length);

                if (value.Length == 0) return;

                // PERFORMANCE OPTIMIZATION 11: Use larger stack buffer to reduce encoding calls
                // Most type names fit in 512 bytes, avoiding heap allocation
                const int MaxStackBufferSize = 512;
                int maxByteCount = value.Length * 3; // UTF-8 worst case is 3 bytes per char for BMP

                if (maxByteCount <= MaxStackBufferSize)
                {
                    // Fast path: fits on stack
                    Span<byte> buffer = stackalloc byte[MaxStackBufferSize];
                    int written = System.Text.Encoding.UTF8.GetBytes(value, buffer);
                    _hasher.Append(buffer[..written]);
                }
                else
                {
                    // Slow path: encode in chunks
                    Span<byte> buffer = stackalloc byte[MaxStackBufferSize];
                    int offset = 0;
                    while (offset < value.Length)
                    {
                        int chunkSize = Math.Min(MaxStackBufferSize / 3, value.Length - offset);
                        int written = System.Text.Encoding.UTF8.GetBytes(value.AsSpan(offset, chunkSize), buffer);
                        _hasher.Append(buffer[..written]);
                        offset += chunkSize;
                    }
                }
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
