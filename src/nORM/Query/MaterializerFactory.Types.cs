using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class MaterializerFactory
    {
        private sealed class ValidationDbDataReader : DbDataReader
        {
            private readonly int _fieldCount;

            public ValidationDbDataReader(int fieldCount)
            {
                _fieldCount = fieldCount;
            }

            /// <summary>
            /// Gets the number of fields that the validation reader exposes.
            /// </summary>
            /// <remarks>
            /// The value is supplied when the <see cref="ValidationDbDataReader"/> is created
            /// and represents the expected number of columns for validation.
            /// </remarks>
            /// <value>The total number of fields defined for validation.</value>
            public override int FieldCount => _fieldCount;
            /// <summary>
            /// Indicates that the value at the specified ordinal is always
            /// <c>DBNull</c>. This allows materializer validation to proceed
            /// without requiring actual data.
            /// </summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>Always returns <c>true</c>.</returns>
            public override bool IsDBNull(int ordinal) => true;
            /// <summary>
            /// Always reports the field as <c>DBNull</c> for validation purposes.
            /// </summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <param name="cancellationToken">Token used to cancel the operation.</param>
            /// <returns>A completed task returning <c>true</c>.</returns>
            public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => Task.FromResult(true);
            public override object GetValue(int ordinal) => DBNull.Value;

            public override int GetValues(object[] values)
            {
                Array.Fill(values, DBNull.Value);
                return Math.Min(values.Length, _fieldCount);
            }

            public override string GetName(int ordinal) => $"Field_{ordinal}";
            public override int GetOrdinal(string name)
            {
                if (name.StartsWith("Field_", StringComparison.Ordinal) &&
                    int.TryParse(name.AsSpan(6), out var ordinal))
                    return ordinal;
                var available = string.Join(", ", Enumerable.Range(0, _fieldCount).Select(i => $"Field_{i}"));
                throw new IndexOutOfRangeException(
                    $"Column name '{name}' was not found in the validation reader. " +
                    $"Available columns ({_fieldCount}): {available}.");
            }
            public override string GetDataTypeName(int ordinal) => nameof(Object);
            public override Type GetFieldType(int ordinal) => typeof(object);
            public override bool HasRows => false;
            /// <summary>
            /// Always reports that the reader remains open.
            /// </summary>
            /// <remarks>
            /// The validation reader operates purely in-memory and therefore never
            /// transitions to a closed state.
            /// </remarks>
            public override bool IsClosed => false;

            /// <summary>
            /// Always returns <c>0</c> because no records are ever affected by the
            /// validation reader.
            /// </summary>
            public override int RecordsAffected => 0;

            public override object this[int ordinal] => DBNull.Value;
            public override object this[string name] => DBNull.Value;

            /// <summary>
            /// Returns an enumerator that iterates over an empty result set.
            /// </summary>
            /// <returns>An <see cref="IEnumerator"/> that contains no elements.</returns>
            public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
            public override bool Read() => false;
            /// <summary>
            /// Always returns <c>false</c> because this reader has no rows.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the read operation.</param>
            /// <returns>A completed task returning <c>false</c>.</returns>
            public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(false);
            public override bool NextResult() => false;
            /// <summary>
            /// Always returns <c>false</c> because there are no additional result sets.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the operation.</param>
            /// <returns>A completed task returning <c>false</c>.</returns>
            public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);
            /// <summary>
            /// Gets the nesting depth of the current row within the result set.
            /// </summary>
            /// <remarks>The validation reader has no hierarchy and therefore always returns <c>0</c>.</remarks>
            /// <value>Always <c>0</c>.</value>
            public override int Depth => 0;
            public override int VisibleFieldCount => _fieldCount;
            /// <summary>Returns the default Boolean value for validation.</summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>Always <c>false</c>.</returns>
            public override bool GetBoolean(int ordinal) => default;

            /// <summary>Returns the default byte value for validation.</summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>Always <c>0</c>.</returns>
            public override byte GetByte(int ordinal) => default;
            public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
            /// <summary>Returns the default character value for validation.</summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>The null character.</returns>
            public override char GetChar(int ordinal) => default;
            public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
            public override Guid GetGuid(int ordinal) => default;
            public override short GetInt16(int ordinal) => default;
            public override int GetInt32(int ordinal) => default;
            public override long GetInt64(int ordinal) => default;
            /// <summary>Returns the default <see cref="DateTime"/> value.</summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns><see cref="DateTime.MinValue"/>.</returns>
            public override DateTime GetDateTime(int ordinal) => default;

            /// <summary>Returns the default <see cref="decimal"/> value.</summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>Always <c>0m</c>.</returns>
            public override decimal GetDecimal(int ordinal) => default;

            /// <summary>Returns the default <see cref="double"/> value.</summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>Always <c>0d</c>.</returns>
            public override double GetDouble(int ordinal) => default;

            /// <summary>Returns the default <see cref="float"/> value.</summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>Always <c>0f</c>.</returns>
            public override float GetFloat(int ordinal) => default;

            /// <summary>
            /// Returns an empty string to satisfy string retrieval during
            /// validation.
            /// </summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>An empty string.</returns>
            public override string GetString(int ordinal) => string.Empty;
            public override T GetFieldValue<T>(int ordinal) => default!;
            public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken) => Task.FromResult(default(T)!);
            /// <summary>
            /// Schema information is not available for the validation reader and
            /// attempting to access it will throw.
            /// </summary>
            /// <returns>Never returns; always throws <see cref="NotSupportedException"/>.</returns>
            public override System.Data.DataTable GetSchemaTable() => throw new NotSupportedException();
        }


        private sealed class OptimizedOrdinalShimReader : DbDataReader
        {
            private readonly DbDataReader _inner;
            private readonly OrdinalMapping _mapping;

            public OptimizedOrdinalShimReader(DbDataReader inner, OrdinalMapping mapping)
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
                _mapping = mapping;
            }

            private int MapOrdinal(int ordinal)
            {
                if ((uint)ordinal >= (uint)_mapping.Ordinals.Length) return UnmappedOrdinal;
                return _mapping.Ordinals[ordinal];
            }

            /// <summary>
            /// Retrieves the value at the specified ordinal, applying the
            /// precomputed ordinal mapping. Unmapped ordinals return
            /// <see cref="DBNull.Value"/>.
            /// </summary>
            /// <param name="ordinal">The requested column ordinal.</param>
            /// <returns>The value from the underlying reader or <see cref="DBNull.Value"/>.</returns>
            public override object GetValue(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetValue(mapped) : DBNull.Value;
            }

            /// <summary>
            /// Determines whether the value at the specified ordinal is
            /// <c>DBNull</c>, respecting the ordinal mapping.
            /// </summary>
            /// <param name="ordinal">The ordinal to evaluate.</param>
            /// <returns><c>true</c> if the column is unmapped or contains <c>DBNull</c>.</returns>
            public override bool IsDBNull(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.IsDBNull(mapped) : true;
            }

            /// <summary>
            /// Gets the name of the column at the specified ordinal, or a
            /// synthetic name if the ordinal is unmapped.
            /// </summary>
            /// <param name="ordinal">The column ordinal.</param>
            /// <returns>The column name or a generated name for unmapped ordinals.</returns>
            public override string GetName(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetName(mapped) : $"Unmapped_{ordinal}";
            }

            public override int FieldCount => Math.Max(_inner.FieldCount, _mapping.Ordinals.Length);
            /// <summary>
            /// Retrieves the ordinal of the column with the given name directly
            /// from the underlying reader.
            /// </summary>
            /// <param name="name">The column name.</param>
            /// <returns>The ordinal of the named column.</returns>
            public override int GetOrdinal(string name) => _inner.GetOrdinal(name);

            /// <summary>
            /// Returns the data type of the column at the specified ordinal,
            /// falling back to <see cref="object"/> for unmapped ordinals.
            /// </summary>
            /// <param name="ordinal">The column ordinal.</param>
            /// <returns>The column's <see cref="Type"/>.</returns>
            public override Type GetFieldType(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetFieldType(mapped) : typeof(object);
            }

            // Delegate most properties and methods to inner reader
            public override int Depth => _inner.Depth;
            public override bool HasRows => _inner.HasRows;
            public override bool IsClosed => _inner.IsClosed;
            public override int RecordsAffected => _inner.RecordsAffected;
            /// <summary>
            /// Advances the reader to the next record, delegating to the inner
            /// reader.
            /// </summary>
            /// <returns><c>true</c> if the next record was read.</returns>
            public override bool Read() => _inner.Read();
            /// <summary>
            /// Asynchronously reads the next row, delegating to the inner reader while applying ordinal mapping.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the read operation.</param>
            /// <returns>A task that resolves to <c>true</c> if a row was read.</returns>
            public override Task<bool> ReadAsync(CancellationToken cancellationToken) => _inner.ReadAsync(cancellationToken);
            /// <summary>
            /// Advances the reader to the next result set, delegating to the
            /// inner reader.
            /// </summary>
            /// <returns><c>true</c> if another result set is available.</returns>
            public override bool NextResult() => _inner.NextResult();
            /// <summary>
            /// Advances to the next result set asynchronously.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the operation.</param>
            /// <returns>A task that resolves to <c>true</c> if another result set is available.</returns>
            public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => _inner.NextResultAsync(cancellationToken);
            /// <summary>
            /// Populates the provided array with column values from the current
            /// row using the ordinal mapping.
            /// </summary>
            /// <param name="values">Destination array for the values.</param>
            /// <returns>The number of values copied.</returns>
            public override int GetValues(object[] values)
            {
                var count = Math.Min(values.Length, _mapping.Ordinals.Length);
                for (int i = 0; i < count; i++)
                {
                    values[i] = GetValue(i);
                }
                return count;
            }
            public override object this[int ordinal] => GetValue(ordinal);
            public override object this[string name] => _inner[name];
            /// <summary>
            /// Gets the data type name of the column at the specified ordinal via
            /// the underlying reader.
            /// </summary>
            /// <param name="ordinal">The column ordinal.</param>
            /// <returns>The database-specific type name.</returns>
            public override string GetDataTypeName(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetDataTypeName(mapped) : nameof(Object);
            }
            /// <summary>
            /// Returns an enumerator that iterates through the rows of the data
            /// reader.
            /// </summary>
            /// <returns>An <see cref="IEnumerator"/> over the reader.</returns>
            public override IEnumerator GetEnumerator() => ((IEnumerable)_inner).GetEnumerator();

            // Typed getters with ordinal mapping

            /// <summary>
            /// Retrieves a Boolean value from the underlying reader using the mapped ordinal.
            /// </summary>
            /// <param name="ordinal">The zero-based column ordinal to read.</param>
            /// <returns>The Boolean value if the ordinal is mapped; otherwise the default value.</returns>
            public override bool GetBoolean(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetBoolean(mapped) : default;
            }

            /// <summary>
            /// Retrieves a byte from the underlying reader using the mapped ordinal.
            /// </summary>
            /// <param name="ordinal">The zero-based column ordinal to read.</param>
            /// <returns>The byte value if available; otherwise the default value.</returns>
            public override byte GetByte(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetByte(mapped) : default;
            }

            /// <summary>
            /// Reads a sequence of bytes from the column at the specified ordinal.
            /// </summary>
            /// <param name="ordinal">The column ordinal.</param>
            /// <param name="dataOffset">The index within the field from which to begin the read operation.</param>
            /// <param name="buffer">The buffer into which the data will be copied.</param>
            /// <param name="bufferOffset">The index within the buffer at which to start copying.</param>
            /// <param name="length">The maximum number of bytes to read.</param>
            /// <returns>The actual number of bytes read.</returns>
            public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetBytes(mapped, dataOffset, buffer, bufferOffset, length) : 0;
            }

            /// <summary>
            /// Retrieves a single character value from the mapped column.
            /// </summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>The character value if the ordinal is mapped; otherwise the default character.</returns>
            public override char GetChar(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetChar(mapped) : default;
            }

            /// <summary>
            /// Reads a sequence of characters from the column at the specified ordinal.
            /// </summary>
            /// <param name="ordinal">The column ordinal.</param>
            /// <param name="dataOffset">The index within the field from which to begin the read operation.</param>
            /// <param name="buffer">The destination buffer.</param>
            /// <param name="bufferOffset">The index within the buffer at which to start copying.</param>
            /// <param name="length">The maximum number of characters to read.</param>
            /// <returns>The actual number of characters read.</returns>
            public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetChars(mapped, dataOffset, buffer, bufferOffset, length) : 0;
            }

            /// <summary>
            /// Retrieves a <see cref="Guid"/> value using the mapped ordinal.
            /// </summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <returns>The <see cref="Guid"/> value if mapped; otherwise the default value.</returns>
            public override Guid GetGuid(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetGuid(mapped) : default;
            }

            /// <summary>
            /// Retrieves a 16-bit integer using the mapped ordinal.
            /// </summary>
            /// <param name="ordinal">The column ordinal to read.</param>
            /// <returns>The <see cref="short"/> value if mapped; otherwise the default value.</returns>
            public override short GetInt16(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetInt16(mapped) : default;
            }

            /// <summary>
            /// Retrieves a 32-bit integer using the mapped ordinal.
            /// </summary>
            /// <param name="ordinal">The column ordinal to read.</param>
            /// <returns>The <see cref="int"/> value if mapped; otherwise the default value.</returns>
            public override int GetInt32(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetInt32(mapped) : default;
            }

            /// <summary>
            /// Retrieves a 64-bit integer using the mapped ordinal.
            /// </summary>
            /// <param name="ordinal">The column ordinal to read.</param>
            /// <returns>The <see cref="long"/> value if mapped; otherwise the default value.</returns>
            public override long GetInt64(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetInt64(mapped) : default;
            }

            /// <summary>
            /// Retrieves a <see cref="DateTime"/> value using the mapped ordinal.
            /// </summary>
            /// <param name="ordinal">The column ordinal to read.</param>
            /// <returns>The <see cref="DateTime"/> value if mapped; otherwise the default value.</returns>
            public override DateTime GetDateTime(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetDateTime(mapped) : default;
            }

            /// <summary>
            /// Retrieves a string value from the mapped column.
            /// </summary>
            /// <param name="ordinal">The column ordinal.</param>
            /// <returns>The string value if mapped; otherwise an empty string.</returns>
            public override string GetString(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetString(mapped) : string.Empty;
            }

            /// <summary>
            /// Retrieves a <see cref="decimal"/> value from the mapped column.
            /// DATA INTEGRITY FIX: Now throws FormatException on invalid data instead of silently returning 0.
            /// </summary>
            /// <param name="ordinal">The column ordinal.</param>
            /// <returns>The decimal value if mapped and convertible.</returns>
            /// <exception cref="FormatException">Thrown when the data cannot be converted to decimal.</exception>
            public override decimal GetDecimal(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                if (mapped < 0) return default;

                // DATA INTEGRITY FIX: Let exceptions propagate instead of silently returning 0
                // This prevents catastrophic silent data corruption in financial applications
                if (_inner.GetFieldType(mapped) == typeof(string) && !_inner.IsDBNull(mapped))
                {
                    var stringValue = _inner.GetString(mapped);
                    // Use InvariantCulture to ensure consistent parsing regardless of the
                    // current thread's culture (e.g., '.' is always the decimal separator).
                    return decimal.Parse(stringValue, CultureInfo.InvariantCulture);
                }
                return _inner.GetDecimal(mapped); // Throws FormatException on invalid data
            }

            /// <summary>
            /// Retrieves a double-precision floating-point value using the mapped ordinal.
            /// </summary>
            /// <param name="ordinal">The column ordinal.</param>
            /// <returns>The <see cref="double"/> value if mapped; otherwise the default value.</returns>
            public override double GetDouble(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetDouble(mapped) : default;
            }

            /// <summary>
            /// Retrieves a single-precision floating-point value using the mapped ordinal.
            /// </summary>
            /// <param name="ordinal">The column ordinal.</param>
            /// <returns>The <see cref="float"/> value if mapped; otherwise the default value.</returns>
            public override float GetFloat(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetFloat(mapped) : default;
            }

            public override int VisibleFieldCount => Math.Max(_inner.VisibleFieldCount, _mapping.Ordinals.Length);
        }

        // MaterializerCacheKey is a readonly struct to guarantee immutability: once constructed,
        // its fields cannot be modified. This is critical for correctness as a dictionary/cache key
        // because mutable keys would break hash-based lookups if mutated after insertion. The
        // readonly constraint is enforced by the compiler -- all fields are readonly and the struct
        // itself is declared readonly, preventing any post-construction mutation.
        // Uses actual Type references instead of hash codes to prevent collision between
        // different types that happen to produce the same hash code.
        // ProjectionHash is 64-bit to prevent hash collisions between distinct projection
        // expression trees that happen to share a 32-bit hash.
        private readonly struct MaterializerCacheKey : IEquatable<MaterializerCacheKey>
        {
            public readonly Type MappingType;     // was int MappingTypeHash
            public readonly Type TargetType;      // was int TargetTypeHash
            public readonly long ProjectionHash;  // was int â€” now 64-bit to reduce collision risk
            public readonly string TableName;
            public readonly int StartOffset;
            public readonly int ConverterFingerprint;
            public readonly int ShadowFingerprint;

            public MaterializerCacheKey(Type mappingType, Type targetType, long projectionHash, string tableName, int startOffset, int converterFingerprint = 0, int shadowFingerprint = 0)
            {
                MappingType = mappingType;
                TargetType = targetType;
                ProjectionHash = projectionHash;
                TableName = tableName ?? string.Empty;
                StartOffset = startOffset;
                ConverterFingerprint = converterFingerprint;
                ShadowFingerprint = shadowFingerprint;
            }

            /// <summary>
            /// Determines whether the specified <see cref="MaterializerCacheKey"/> is equal to the current instance.
            /// Uses reference equality for Type fields to avoid hash collision between distinct types.
            /// </summary>
            /// <param name="other">The cache key to compare with the current key.</param>
            /// <returns><c>true</c> if the keys represent the same configuration; otherwise, <c>false</c>.</returns>
            public bool Equals(MaterializerCacheKey other) =>
                MappingType == other.MappingType &&   // reference equality â€” no collision
                TargetType == other.TargetType &&
                ProjectionHash == other.ProjectionHash &&
                StartOffset == other.StartOffset &&
                ConverterFingerprint == other.ConverterFingerprint &&
                ShadowFingerprint == other.ShadowFingerprint &&
                string.Equals(TableName, other.TableName, StringComparison.Ordinal);

            /// <summary>
            /// Determines whether the specified object is equal to the current <see cref="MaterializerCacheKey"/>.
            /// </summary>
            /// <param name="obj">The object to compare with the current key.</param>
            /// <returns><c>true</c> if <paramref name="obj"/> is a <see cref="MaterializerCacheKey"/> and represents the same configuration; otherwise, <c>false</c>.</returns>
            public override bool Equals(object? obj) => obj is MaterializerCacheKey other && Equals(other);

            /// <summary>
            /// Generates a hash code for the current key instance.
            /// </summary>
            /// <returns>A hash code that can be used in hashing algorithms and data structures.</returns>
            public override int GetHashCode() => HashCode.Combine(MappingType, TargetType, ProjectionHash, TableName, StartOffset, ConverterFingerprint, ShadowFingerprint);
        }

        private readonly struct SchemaCacheKey : IEquatable<SchemaCacheKey>
        {
            public readonly string[] FieldNames;
            public readonly Type[] FieldTypes;
            public readonly string TableName;
            private readonly int _hashCode;

            public SchemaCacheKey(string[] fieldNames, Type[] fieldTypes, string tableName)
            {
                FieldNames = fieldNames;
                FieldTypes = fieldTypes;
                TableName = tableName;

                // Pre-compute hash code
                var hash = new HashCode();
                hash.Add(TableName);
                foreach (var name in fieldNames)
                    hash.Add(name);
                foreach (var type in fieldTypes)
                    hash.Add(type);
                _hashCode = hash.ToHashCode();
            }

            public bool Equals(SchemaCacheKey other)
            {
                if (!string.Equals(TableName, other.TableName, StringComparison.Ordinal))
                    return false;

                if (FieldNames.Length != other.FieldNames.Length || FieldTypes.Length != other.FieldTypes.Length)
                    return false;

                for (int i = 0; i < FieldNames.Length; i++)
                {
                    if (!string.Equals(FieldNames[i], other.FieldNames[i], StringComparison.Ordinal))
                        return false;
                }

                for (int i = 0; i < FieldTypes.Length; i++)
                {
                    if (FieldTypes[i] != other.FieldTypes[i])
                        return false;
                }

                return true;
            }

            public override bool Equals(object? obj) => obj is SchemaCacheKey other && Equals(other);
            public override int GetHashCode() => _hashCode;
        }

        private readonly struct OrdinalMapping
        {
            public readonly int[] Ordinals;
            public readonly bool IsValid;

            public OrdinalMapping(int[] ordinals, bool isValid)
            {
                Ordinals = ordinals;
                IsValid = isValid;
            }
        }
    }
}
