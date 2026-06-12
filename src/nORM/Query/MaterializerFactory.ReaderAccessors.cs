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
        private static Func<DbDataReader, object> CreateReaderGetter(Type type, int index, int startOffset)
        {
            var readerParam = Expression.Parameter(typeof(DbDataReader), "reader");
            var valueExpr = GetOptimizedReaderCall(readerParam, type, index + startOffset);
            var body = Expression.Convert(valueExpr, typeof(object));
            return Expression.Lambda<Func<DbDataReader, object>>(body, readerParam).Compile();
        }

        private static Func<DbDataReader, object> CreateOptimizedMaterializer(Column[] columns, Type targetType, int startOffset)
        {
            var readerParam = Expression.Parameter(typeof(DbDataReader), "reader");
            var entityVar = Expression.Variable(targetType, "entity");
            var expressions = new List<Expression>
            {
                Expression.Assign(entityVar, Expression.New(targetType))
            };

            // Use the class-level cached NullabilityInfoContext to detect non-nullable reference types (NRT).
            // This lets us skip IsDBNull for non-nullable strings etc., saving ~47ns per call.
            var nullabilityCtx = _nullabilityInfoContext;

            for (int i = 0; i < columns.Length; i++)
            {
                var column = columns[i];
                var propType = column.Prop.PropertyType;
                var getValue = GetOptimizedReaderCall(readerParam, propType, i + startOffset);
                var setProperty = Expression.Call(entityVar, column.Prop.GetSetMethod()!, getValue);

                bool skipIsDbNull;
                if (propType.IsValueType)
                {
                    // Non-nullable value type: skip IsDBNull
                    skipIsDbNull = Nullable.GetUnderlyingType(propType) == null;
                }
                else if (nullabilityCtx != null)
                {
                    // Reference type: skip IsDBNull if NRT metadata says non-nullable
                    try
                    {
                        NullabilityInfo nullabilityInfo;
                        lock (_nullabilityInfoContextLock)
                        {
                            nullabilityInfo = nullabilityCtx.Create(column.Prop);
                        }
                        skipIsDbNull = nullabilityInfo.WriteState == NullabilityState.NotNull;
                    }
                    catch (InvalidOperationException)
                    {
                        // NullabilityInfoContext.Create can throw for dynamic/emitted properties.
                        // Fallback: conservatively keep IsDBNull check for this column.
                        skipIsDbNull = false;
                    }
                    catch (ArgumentException)
                    {
                        // NullabilityInfoContext.Create can throw ArgumentException for unsupported property metadata.
                        // Fallback: conservatively keep IsDBNull check for this column.
                        skipIsDbNull = false;
                    }
                }
                else
                {
                    skipIsDbNull = false;
                }

                if (skipIsDbNull)
                {
                    // Skip IsDBNull check for non-nullable types.
                    // The DB schema should have NOT NULL constraint matching the C# type.
                    // If the DB unexpectedly returns NULL, the typed accessor will throw -
                    // which is correct for a schema violation.
                    expressions.Add(setProperty);
                }
                else
                {
                    var isNullCheck = Expression.Call(readerParam, Methods.IsDbNull, Expression.Constant(i + startOffset));
                    expressions.Add(Expression.IfThen(Expression.Not(isNullCheck), setProperty));
                }
            }

            expressions.Add(Expression.Convert(entityVar, typeof(object)));
            var block = Expression.Block(new[] { entityVar }, expressions);
            return Expression.Lambda<Func<DbDataReader, object>>(block, readerParam).Compile();
        }

        private static void ValidateMaterializer(Func<DbDataReader, object> materializer, TableMapping mapping, Type targetType)
        {
            using var reader = new ValidationDbDataReader(mapping.Columns.Length);
            try
            {
                materializer(reader);
            }
            catch (Exception ex) when (
                ex is InvalidOperationException or InvalidCastException or FormatException or IndexOutOfRangeException)
            {
                // MAP-4: Expected during validation - the validation reader purposely sends DBNull to
                // every column to test the structural correctness of the materializer. A NULL-for-non-nullable
                // exception here means the materializer code path was reached and will throw appropriately
                // during real execution. FormatException/IndexOutOfRangeException can occur when the
                // validation reader's synthetic column names don't match GetOrdinal expectations.
            }
            catch (Exception ex)
            {
                var columnInfo = string.Join(", ", mapping.Columns.Select((c, i) => $"[{i}]={c.Name}({c.Prop.PropertyType.Name})"));
                throw new InvalidOperationException(
                    $"Materializer validation failed for type {targetType.Name} " +
                    $"with {mapping.Columns.Length} column(s): {columnInfo}. Error: {ex.Message}", ex);
            }
        }

        private static readonly MethodInfo _convertToEnumOpenMethod =
            typeof(MaterializerFactory).GetMethod(nameof(ConvertToEnum), BindingFlags.NonPublic | BindingFlags.Static)!;

        private static readonly MethodInfo _convertToDateOnlyMethod =
            typeof(MaterializerFactory).GetMethod(nameof(ConvertToDateOnly), BindingFlags.Public | BindingFlags.Static)!;

        private static readonly MethodInfo _convertToTimeOnlyMethod =
            typeof(MaterializerFactory).GetMethod(nameof(ConvertToTimeOnly), BindingFlags.Public | BindingFlags.Static)!;

        private static readonly MethodInfo _convertToDateTimeOffsetMethod =
            typeof(MaterializerFactory).GetMethod(nameof(ConvertToDateTimeOffset), BindingFlags.Public | BindingFlags.Static)!;

        private static readonly MethodInfo _convertToTimeSpanMethod =
            typeof(MaterializerFactory).GetMethod(nameof(ConvertToTimeSpan), BindingFlags.Public | BindingFlags.Static)!;

        private static readonly MethodInfo _convertToCharMethod =
            typeof(MaterializerFactory).GetMethod(nameof(ConvertToChar), BindingFlags.Public | BindingFlags.Static)!;

        private static Expression GetOptimizedReaderCall(ParameterExpression reader, Type propertyType, int index)
        {
            var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
            Expression call;

            if (underlyingType.IsEnum)
            {
                // MM-2: Enum types - use ConvertToEnum<TEnum> helper to safely convert from Int64/Int32/etc.
                // Expression.Convert(object, EnumType) throws InvalidCastException at runtime for boxed integers.
                var enumConvertMethod = _convertToEnumOpenMethod.MakeGenericMethod(underlyingType);
                var getRawValue = Expression.Call(reader, Methods.GetValue, Expression.Constant(index));
                call = Expression.Call(enumConvertMethod, getRawValue);
            }
            else if (underlyingType == typeof(DateOnly))
            {
                // MM1 fix: DateOnly - Convert.ChangeType and Expression.Convert don't support DateOnly
                var getRawValue = Expression.Call(reader, Methods.GetValue, Expression.Constant(index));
                call = Expression.Call(_convertToDateOnlyMethod, getRawValue);
            }
            else if (underlyingType == typeof(TimeOnly))
            {
                // MM1 fix: TimeOnly - Convert.ChangeType and Expression.Convert don't support TimeOnly
                var getRawValue = Expression.Call(reader, Methods.GetValue, Expression.Constant(index));
                call = Expression.Call(_convertToTimeOnlyMethod, getRawValue);
            }
            else if (underlyingType == typeof(DateTimeOffset))
            {
                // SQLite stores DateTimeOffset as TEXT; cast(object?DateTimeOffset) cannot parse it.
                var getRawValue = Expression.Call(reader, Methods.GetValue, Expression.Constant(index));
                call = Expression.Call(_convertToDateTimeOffsetMethod, getRawValue);
            }
            else if (underlyingType == typeof(TimeSpan))
            {
                var getRawValue = Expression.Call(reader, Methods.GetValue, Expression.Constant(index));
                call = Expression.Call(_convertToTimeSpanMethod, getRawValue);
            }
            else if (underlyingType == typeof(char))
            {
                // SQLite stores char as single-character TEXT - direct (string ? char) cast
                // throws InvalidCastException at runtime. Route through ConvertToChar which
                // pulls the first code unit of the string (or accepts a boxed char / numeric).
                var getRawValue = Expression.Call(reader, Methods.GetValue, Expression.Constant(index));
                call = Expression.Call(_convertToCharMethod, getRawValue);
            }
            else if (underlyingType == typeof(byte) || underlyingType == typeof(sbyte)
                || underlyingType == typeof(short) || underlyingType == typeof(ushort)
                || underlyingType == typeof(uint) || underlyingType == typeof(ulong))
            {
                // Small / wide integer columns: SQLite returns these as boxed Int64. Direct
                // Expression.Convert(boxed-long ? byte) throws InvalidCastException at runtime.
                // Route through the matching System.Convert.To*(object, IFormatProvider)
                // overload which performs the numeric narrowing safely.
                var getRawValue = Expression.Call(reader, Methods.GetValue, Expression.Constant(index));
                var converter = underlyingType.Name switch
                {
                    nameof(Byte)   => typeof(Convert).GetMethod(nameof(Convert.ToByte),   new[] { typeof(object), typeof(IFormatProvider) })!,
                    nameof(SByte)  => typeof(Convert).GetMethod(nameof(Convert.ToSByte),  new[] { typeof(object), typeof(IFormatProvider) })!,
                    nameof(Int16)  => typeof(Convert).GetMethod(nameof(Convert.ToInt16),  new[] { typeof(object), typeof(IFormatProvider) })!,
                    nameof(UInt16) => typeof(Convert).GetMethod(nameof(Convert.ToUInt16), new[] { typeof(object), typeof(IFormatProvider) })!,
                    nameof(UInt32) => typeof(Convert).GetMethod(nameof(Convert.ToUInt32), new[] { typeof(object), typeof(IFormatProvider) })!,
                    nameof(UInt64) => typeof(Convert).GetMethod(nameof(Convert.ToUInt64), new[] { typeof(object), typeof(IFormatProvider) })!,
                    _ => throw new System.InvalidOperationException($"Unexpected narrow-integer type '{underlyingType.Name}'.")
                };
                call = Expression.Call(converter, getRawValue, Expression.Constant(System.Globalization.CultureInfo.InvariantCulture, typeof(IFormatProvider)));
            }
            else
            {
                call = underlyingType.Name switch
                {
                    nameof(Int32) => Expression.Call(reader, Methods.GetInt32, Expression.Constant(index)),
                    nameof(String) => Expression.Call(reader, Methods.GetString, Expression.Constant(index)),
                    nameof(DateTime) => Expression.Call(reader, Methods.GetDateTime, Expression.Constant(index)),
                    nameof(Boolean) => Expression.Call(reader, Methods.GetBoolean, Expression.Constant(index)),
                    nameof(Decimal) => Expression.Call(reader, Methods.GetDecimal, Expression.Constant(index)),
                    nameof(Int64) => Expression.Call(reader, Methods.GetInt64, Expression.Constant(index)),
                    nameof(Double) => Expression.Call(reader, Methods.GetDouble, Expression.Constant(index)),
                    nameof(Single) => Expression.Call(reader, Methods.GetFloat, Expression.Constant(index)),
                    nameof(Guid) => Expression.Call(reader, Methods.GetGuid, Expression.Constant(index)),
                    _ => Expression.Convert(
                            Expression.Call(reader, Methods.GetValue, Expression.Constant(index)),
                            underlyingType)
                };
            }

            if (call.Type != propertyType)
                call = Expression.Convert(call, propertyType);

            return call;
        }

        private static Action<object, DbDataReader>[] GetOptimizedSetters(Type type, int startOffset)
        {
            return _setterCache.GetOrAdd((type, startOffset), t =>
            {
                var properties = _propertiesCache.GetOrAdd(t.Item1, tt => tt.GetProperties(BindingFlags.Instance | BindingFlags.Public));
                var setters = new Action<object, DbDataReader>[properties.Length];

                for (int i = 0; i < properties.Length; i++)
                {
                    var prop = properties[i];
                    setters[i] = CreateOptimizedSetter(prop, i + startOffset);
                }
                return setters;
            });
        }

        private static Action<object, DbDataReader> CreateOptimizedSetter(PropertyInfo property, int index)
        {
            var targetParam = Expression.Parameter(typeof(object), "target");
            var readerParam = Expression.Parameter(typeof(DbDataReader), "reader");

            var isDbNull = Expression.Call(readerParam, Methods.IsDbNull, Expression.Constant(index));
            var getValue = GetOptimizedReaderCall(readerParam, property.PropertyType, index);
            // Cast target to property.DeclaringType (not the concrete entity type) because the
            // property's setter MethodInfo is bound to its declaring type. For inherited properties,
            // DeclaringType is the base class, and calling the setter on a derived-type expression
            // without this cast would throw an ArgumentException from Expression.Call.
            var assign = Expression.Call(Expression.Convert(targetParam, property.DeclaringType!), property.GetSetMethod()!, getValue);
            var body = Expression.IfThen(Expression.Not(isDbNull), assign);

            return Expression.Lambda<Action<object, DbDataReader>>(body, targetParam, readerParam).Compile();
        }
    }
}
