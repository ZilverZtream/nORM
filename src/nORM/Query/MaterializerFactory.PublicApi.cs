using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Mapping;
using nORM.SourceGeneration;
using System.Globalization;
using nORM.Core;

namespace nORM.Query
{
    internal sealed partial class MaterializerFactory
    {
        /// <summary>
        /// Creates a strongly-typed synchronous materializer delegate, avoiding boxing for value types.
        /// </summary>
        /// <typeparam name="T">The type to materialize.</typeparam>
        /// <param name="mapping">Mapping describing the source table schema.</param>
        /// <param name="projection">Optional projection expression selecting specific members.</param>
        /// <param name="startOffset">Zero-based column offset to start reading from the data reader.</param>
        /// <returns>A delegate that synchronously materializes objects from a data reader without boxing.</returns>
        public Func<DbDataReader, T> CreateSyncMaterializer<T>(
            TableMapping mapping,
            LambdaExpression? projection = null,
            int startOffset = 0)
        {
            ArgumentNullException.ThrowIfNull(mapping);

            var targetType = typeof(T);
            var cacheKey = new MaterializerCacheKey(
                mapping.Type,
                targetType,
                projection != null ? ComputeProjectionHash(projection) : 0L,
                mapping.TableName,
                startOffset,
                mapping.ConverterFingerprint,
                mapping.ShadowFingerprint);

            // Get the object-returning materializer from cache
            var objectMaterializer = _syncCache.GetOrAdd(cacheKey, _ =>
            {
                // For simple entity materialization without projection, prefer fast materializers
                // Skip fast materializer when converters are configured
                if (projection == null && startOffset == 0 && mapping.ConverterFingerprint == 0 && _fastMaterializers.TryGetValue(targetType, out var fast))
                {
                    return fast;
                }

                // Fall back to existing reflection-based approach
                var sync = CreateMaterializerInternal(mapping, targetType, projection, false, startOffset);
                ValidateMaterializer(sync, mapping, targetType);
                return sync;
            });

            // Wrap to return strongly-typed result (JIT will optimize this cast away for reference types)
            return reader => (T)objectMaterializer(reader);
        }

        /// <summary>
        /// Creates a synchronous materializer delegate that converts a <see cref="DbDataReader"/> row into the target type.
        /// Sync delegates are cached to avoid Task allocation overhead.
        /// </summary>
        /// <param name="mapping">Mapping describing the source table schema.</param>
        /// <param name="targetType">CLR type to materialize.</param>
        /// <param name="projection">Optional projection expression selecting specific members.</param>
        /// <param name="startOffset">Zero-based column offset to start reading from the data reader.</param>
        /// <param name="projectionSubqueryConverters">Optional per-member converters for projection members
        /// sourced from a correlated subquery over a converter column; folded into the cache key.</param>
        /// <param name="groupKeyConverter">Optional converter for the IGrouping.Key column when the projection
        /// surfaces the group key over a value-converter column; folded into the cache key.</param>
        /// <returns>A delegate that synchronously materializes objects from a data reader.</returns>
        public Func<DbDataReader, object> CreateSyncMaterializer(
            TableMapping mapping,
            Type targetType,
            LambdaExpression? projection = null,
            int startOffset = 0,
            IReadOnlyDictionary<string, nORM.Mapping.IValueConverter>? projectionSubqueryConverters = null,
            nORM.Mapping.IValueConverter? groupKeyConverter = null)
        {
            ArgumentNullException.ThrowIfNull(mapping);
            ArgumentNullException.ThrowIfNull(targetType);

            var cacheKey = new MaterializerCacheKey(
                mapping.Type,
                targetType,
                (projection != null ? ComputeProjectionHash(projection) : 0L)
                    ^ QueryTranslator.ProjectionSubqueryConverterFingerprint(projectionSubqueryConverters)
                    ^ (groupKeyConverter != null ? (long)groupKeyConverter.GetType().GetHashCode() : 0L),
                mapping.TableName,
                startOffset,
                mapping.ConverterFingerprint,
                mapping.ShadowFingerprint);

            return _syncCache.GetOrAdd(cacheKey, _ =>
            {
                // For simple entity materialization without projection, prefer fast materializers
                // Skip fast materializer when converters are configured
                if (projection == null && startOffset == 0 && mapping.ConverterFingerprint == 0 && _fastMaterializers.TryGetValue(targetType, out var fast))
                {
                    return fast;
                }

                // Fall back to existing reflection-based approach
                var sync = CreateMaterializerInternal(mapping, targetType, projection, false, startOffset, projectionSubqueryConverters, groupKeyConverter);
                ValidateMaterializer(sync, mapping, targetType);
                return sync;
            });
        }

        /// <summary>
        /// Creates a strongly-typed async materializer delegate, avoiding boxing for value types.
        /// </summary>
        /// <typeparam name="T">The type to materialize.</typeparam>
        /// <param name="mapping">Mapping describing the source table schema.</param>
        /// <param name="projection">Optional projection expression selecting specific members.</param>
        /// <returns>A delegate that asynchronously materializes objects from a data reader without boxing.</returns>
        public Func<DbDataReader, CancellationToken, Task<T>> CreateMaterializer<T>(
            TableMapping mapping,
            LambdaExpression? projection = null)
        {
            ArgumentNullException.ThrowIfNull(mapping);

            var targetType = typeof(T);

            // CHECK FOR COMPILED MATERIALIZER FIRST - use mapping.TableName to discriminate
            // between the same CLR type registered under different model mappings.
            // X1 fix: skip compiled materializer if the runtime mapping has fluent-only column
            // renames that the source generator couldn't see at compile time. The generator uses
            // [Column] attributes only; fluent HasColumnName overrides are invisible to it.
            // X1/VC fix: skip compiled materializer when ValueConverters are registered - the
            // source-generated delegate reads raw provider values without applying converters.
            // SG1 fix: skip compiled materializer when owned scalar navigations (OwnsOne) are
            // present - the generator cannot reconstruct the nested property assignment.
            if (projection == null && CompiledMaterializerStore.TryGet(targetType, mapping.TableName, out var compiled)
                && !HasFluentColumnRenames(targetType, mapping)
                && mapping.ConverterFingerprint == 0
                && !HasOwnedNavigationColumns(mapping))
            {
                // Wrap the compiled materializer to return strongly-typed result
                return async (reader, ct) => (T)(await compiled(reader, ct).ConfigureAwait(false));
            }

            // Get the strongly-typed sync materializer
            var syncMaterializer = CreateSyncMaterializer<T>(mapping, projection);

            return (reader, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult(syncMaterializer(reader));
            };
        }

        /// <summary>
        /// Creates an async materializer delegate that converts a <see cref="DbDataReader"/> row into the target type.
        /// Wraps the sync materializer efficiently without per-row Task allocation.
        /// </summary>
        /// <param name="mapping">Mapping describing the source table schema.</param>
        /// <param name="targetType">CLR type to materialize.</param>
        /// <param name="projection">Optional projection expression selecting specific members.</param>
        /// <param name="startOffset">Zero-based column offset to start reading from the data reader.</param>
        /// <returns>A delegate that asynchronously materializes objects from a data reader.</returns>
        public Func<DbDataReader, CancellationToken, Task<object>> CreateMaterializer(
            TableMapping mapping,
            Type targetType,
            LambdaExpression? projection = null,
            int startOffset = 0)
        {
            ArgumentNullException.ThrowIfNull(mapping);
            ArgumentNullException.ThrowIfNull(targetType);

            // CHECK FOR COMPILED MATERIALIZER FIRST - use mapping.TableName to discriminate
            // between the same CLR type registered under different model mappings.
            // X1 fix: skip compiled materializer when fluent-only column renames are present.
            // X1/VC fix: skip when ValueConverters are registered.
            // SG1 fix: skip when owned scalar navigations are present.
            if (projection == null && startOffset == 0 && CompiledMaterializerStore.TryGet(targetType, mapping.TableName, out var compiled)
                && !HasFluentColumnRenames(targetType, mapping)
                && mapping.ConverterFingerprint == 0
                && !HasOwnedNavigationColumns(mapping))
            {
                return compiled;
            }

            var cacheKey = new MaterializerCacheKey(
                mapping.Type,
                targetType,
                projection != null ? ComputeProjectionHash(projection) : 0L,
                mapping.TableName,
                startOffset,
                mapping.ConverterFingerprint,
                mapping.ShadowFingerprint);

            return _asyncCache.GetOrAdd(cacheKey, _ =>
            {
                // Get the sync materializer and wrap it once.
                // This wrapper is cached, so we don't allocate a Task per row.
                var syncMaterializer = CreateSyncMaterializer(mapping, targetType, projection, startOffset);

                return (reader, ct) =>
                {
                    ct.ThrowIfCancellationRequested();
                    return Task.FromResult(syncMaterializer(reader));
                };
            });
        }

        // Optimized schema-aware materializer for JOIN scenarios
        /// <summary>
        /// Creates a materializer that is aware of the actual reader schema, enabling efficient JOIN projections.
        /// </summary>
        /// <param name="mapping">Mapping describing the table layout.</param>
        /// <param name="targetType">Type of object to materialize.</param>
        /// <param name="projection">Optional projection expression.</param>
        /// <param name="startOffset">Zero-based column offset to start reading from the data reader.</param>
        /// <param name="projectionSubqueryConverters">Optional per-member converters for projection members
        /// sourced from a correlated subquery over a converter column; folded into the cache key.</param>
        /// <param name="groupKeyConverter">Optional converter for the IGrouping.Key column when the projection
        /// surfaces the group key over a value-converter column; folded into the cache key.</param>
        /// <returns>A delegate that materializes objects taking the reader schema into account.</returns>
        public Func<DbDataReader, CancellationToken, Task<object>> CreateSchemaAwareMaterializer(
            TableMapping mapping,
            Type targetType,
            LambdaExpression? projection = null,
            int startOffset = 0,
            IReadOnlyDictionary<string, nORM.Mapping.IValueConverter>? projectionSubqueryConverters = null,
            nORM.Mapping.IValueConverter? groupKeyConverter = null)
        {
            ArgumentNullException.ThrowIfNull(mapping);
            ArgumentNullException.ThrowIfNull(targetType);

            // For simple cases without JOINs, use regular materializer.
            // X1 fix: skip compiled materializer when fluent-only column renames are present.
            // X1/VC fix: skip when ValueConverters are registered.
            // SG1 fix: skip when owned scalar navigations are present.
            if (projection == null && startOffset == 0 && CompiledMaterializerStore.TryGet(targetType, mapping.TableName, out var compiled)
                && !HasFluentColumnRenames(targetType, mapping)
                && mapping.ConverterFingerprint == 0
                && !HasOwnedNavigationColumns(mapping))
            {
                return compiled;
            }

            var cacheKey = new MaterializerCacheKey(
                mapping.Type,
                targetType,
                (projection != null ? ComputeProjectionHash(projection) : 0L)
                    ^ QueryTranslator.ProjectionSubqueryConverterFingerprint(projectionSubqueryConverters)
                    ^ (groupKeyConverter != null ? (long)groupKeyConverter.GetType().GetHashCode() : 0L),
                mapping.TableName,
                startOffset,
                mapping.ConverterFingerprint,
                mapping.ShadowFingerprint);

            return _asyncCache.GetOrAdd(cacheKey, _ =>
            {
                var baseMaterializer = CreateMaterializerInternal(mapping, targetType, projection, false, startOffset, projectionSubqueryConverters, groupKeyConverter);

                // For most cases, use base materializer with minimal overhead
                if (projection == null)
                {
                    return (reader, ct) =>
                    {
                        ct.ThrowIfCancellationRequested();
                        return Task.FromResult(baseMaterializer(reader));
                    };
                }

                // Only use schema-aware logic for complex projections
                OrdinalMapping? cachedMapping = null;
                int mappingComputedFlag = 0; // 0=not computed, 1=computed; Interlocked for thread safety

                return (DbDataReader reader, CancellationToken ct) =>
                {
                    ct.ThrowIfCancellationRequested();

                    // Always try base materializer first - it's fastest
                    try
                    {
                        return Task.FromResult(baseMaterializer(reader));
                    }
                    catch (FormatException)
                    {
                        // Only on format exception, compute schema mapping once (thread-safe)
                        if (Interlocked.CompareExchange(ref mappingComputedFlag, 1, 0) == 0)
                        {
                            cachedMapping = CreateOrdinalMapping(reader, mapping);
                        }

                        if (cachedMapping?.IsValid == true)
                        {
                            using var shimReader = new OptimizedOrdinalShimReader(reader, cachedMapping.Value);
                            return Task.FromResult(baseMaterializer(shimReader));
                        }

                        // SCHEMA VALIDATION FIX: Provide detailed error about missing/incompatible columns
                        // This fails faster than waiting for DbException with better diagnostics
                        if (cachedMapping != null && !cachedMapping.Value.IsValid)
                        {
                            var missingColumns = new List<string>();
                            for (int i = 0; i < cachedMapping.Value.Ordinals.Length; i++)
                            {
                                if (cachedMapping.Value.Ordinals[i] == UnmappedOrdinal)
                                {
                                    missingColumns.Add(mapping.Columns[i].PropName);
                                }
                            }

                            var availableColumns = new string[reader.FieldCount];
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                availableColumns[i] = reader.GetName(i);
                            }

                            throw new InvalidOperationException(
                                $"Schema mismatch: Unable to map {missingColumns.Count} column(s) for entity type '{mapping.TableName}'. " +
                                $"Missing or incompatible columns: {string.Join(", ", missingColumns)}. " +
                                $"Available columns in result set: {string.Join(", ", availableColumns)}");
                        }

                        // If mapping failed, rethrow original exception
                        throw;
                    }
                };
            });
        }
    }
}
