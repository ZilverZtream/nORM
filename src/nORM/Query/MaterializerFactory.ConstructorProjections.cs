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
        private static Func<object?[], object> CreateConstructorDelegate(ConstructorInfo ctor)
        {
            // Use DynamicMethod to bypass visibility checks (restrictedSkipVisibility: true)
            // This allows materializing internal/anonymous types from external assemblies
            var method = new DynamicMethod(
                $"Ctor_{ctor.DeclaringType?.Name}_{Guid.NewGuid():N}",
                typeof(object),
                new[] { typeof(object?[]) },
                typeof(MaterializerFactory),
                true); // <--- Key fix: true skips visibility checks for internal types

            var il = method.GetILGenerator();
            var parameters = ctor.GetParameters();

            for (int i = 0; i < parameters.Length; i++)
            {
                // Load array argument
                il.Emit(OpCodes.Ldarg_0);
                // Load index
                il.Emit(OpCodes.Ldc_I4, i);
                // Load element at index
                il.Emit(OpCodes.Ldelem_Ref);

                // Cast/Unbox to parameter type
                var paramType = parameters[i].ParameterType;
                if (paramType.IsValueType)
                    il.Emit(OpCodes.Unbox_Any, paramType);
                else
                    il.Emit(OpCodes.Castclass, paramType);
            }

            // Call constructor
            il.Emit(OpCodes.Newobj, ctor);

            // Box if it's a value type (unlikely for anonymous types, but good safety)
            if (ctor.DeclaringType!.IsValueType)
                il.Emit(OpCodes.Box, ctor.DeclaringType);

            il.Emit(OpCodes.Ret);

            return (Func<object?[], object>)method.CreateDelegate(typeof(Func<object?[], object>));
        }

        private static bool IsAnonymousType(Type t)
            => t.Namespace == null && t.Name.Contains("AnonymousType", StringComparison.Ordinal);

        private static bool HasNestedAnonymousProjectionArg(NewExpression body)
        {
            foreach (var arg in body.Arguments)
            {
                if (!IsAnonymousType(arg.Type)) continue;
                // Explicit nested anonymous payload: `Stats = new { Total = ..., Count = ... }`.
                if (arg is NewExpression) return true;
                // Composite key projected whole: `Key = g.Key`.
                if (arg is MemberExpression me
                    && me.Member.Name == "Key"
                    && me.Expression is ParameterExpression mep
                    && mep.Type.IsGenericType
                    && mep.Type.GetGenericTypeDefinition() == typeof(System.Linq.IGrouping<,>))
                {
                    return true;
                }
                // Bare key parameter from 3-arg GroupBy result selector: `(k, g) => new { Key = k, ... }`.
                if (arg is ParameterExpression pe && IsAnonymousType(pe.Type))
                {
                    return true;
                }
            }
            return false;
        }

        private static Func<DbDataReader, object> CreateNestedAnonymousProjectionMaterializer(
            NewExpression body,
            ConstructorInfo ctor,
            int startOffset)
        {
            var reader = Expression.Parameter(typeof(DbDataReader), "reader");
            var parameters = ctor.GetParameters();
            var args = new Expression[parameters.Length];
            int cursor = startOffset;

            for (int i = 0; i < parameters.Length; i++)
            {
                var arg = body.Arguments[i];
                var paramType = parameters[i].ParameterType;

                // Explicit nested anonymous payload: the projection itself has a `new {...}`
                // sub-expression. The SQL side emitted one flat column per sub-arg with a
                // prefixed alias; read them sequentially and call the inner anon ctor.
                if (arg is NewExpression nestedNew && IsAnonymousType(arg.Type))
                {
                    var nestedCtor = nestedNew.Constructor ?? paramType.GetConstructors()[0];
                    var nestedParams = nestedCtor.GetParameters();
                    var nestedArgs = new Expression[nestedParams.Length];
                    for (int j = 0; j < nestedParams.Length; j++)
                    {
                        var subType = nestedParams[j].ParameterType;
                        var subRead = GetOptimizedReaderCall(reader, subType, cursor);
                        var subDefault = Expression.Default(subType);
                        var subIsNull = Expression.Call(reader, Methods.IsDbNull, Expression.Constant(cursor));
                        nestedArgs[j] = Expression.Condition(subIsNull, subDefault, subRead);
                        cursor++;
                    }
                    args[i] = Expression.New(nestedCtor, nestedArgs);
                    continue;
                }

                bool isNestedAnonRef = IsAnonymousType(arg.Type)
                    && (
                        (arg is MemberExpression me
                         && me.Member.Name == "Key"
                         && me.Expression is ParameterExpression mep
                         && mep.Type.IsGenericType
                         && mep.Type.GetGenericTypeDefinition() == typeof(System.Linq.IGrouping<,>))
                        || (arg is ParameterExpression pe && IsAnonymousType(pe.Type))
                    );

                if (isNestedAnonRef)
                {
                    var nestedCtor = paramType.GetConstructors()[0];
                    var nestedParams = nestedCtor.GetParameters();
                    var nestedArgs = new Expression[nestedParams.Length];
                    for (int j = 0; j < nestedParams.Length; j++)
                    {
                        var subType = nestedParams[j].ParameterType;
                        var subRead = GetOptimizedReaderCall(reader, subType, cursor);
                        var subDefault = Expression.Default(subType);
                        var subIsNull = Expression.Call(reader, Methods.IsDbNull, Expression.Constant(cursor));
                        nestedArgs[j] = Expression.Condition(subIsNull, subDefault, subRead);
                        cursor++;
                    }
                    args[i] = Expression.New(nestedCtor, nestedArgs);
                }
                else
                {
                    var readValue = GetOptimizedReaderCall(reader, paramType, cursor);
                    var defaultValue = Expression.Default(paramType);
                    var isDbNull = Expression.Call(reader, Methods.IsDbNull, Expression.Constant(cursor));
                    args[i] = Expression.Condition(isDbNull, defaultValue, readValue);
                    cursor++;
                }
            }

            var bodyExpr = Expression.Convert(Expression.New(ctor, args), typeof(object));
            return Expression.Lambda<Func<DbDataReader, object>>(bodyExpr, reader).Compile();
        }

        private static Func<DbDataReader, object> CreateProjectionConstructorMaterializer(
            ConstructorInfo ctor,
            ParameterInfo[] parameters,
            IReadOnlyList<Expression> arguments,
            IReadOnlyList<Column> columns,
            int startOffset,
            TableMapping mapping)
        {
            var reader = Expression.Parameter(typeof(DbDataReader), "reader");
            var args = new Expression[parameters.Length];

            // Non-collection args map 1:1 to the projection columns (in the same order the SELECT emitted
            // them); a shaped/bare navigation-collection arg has no column, so track the column position with
            // a separate cursor that only advances past real columns.
            var columnCursor = 0;
            for (var i = 0; i < parameters.Length; i++)
            {
                var paramType = parameters[i].ParameterType;

                // A navigation collection is loaded by the split-query pipeline, not read from the row.
                // Construct an empty MUTABLE list as the constructor argument; the stitch populates it in
                // place, because an anonymous type is immutable and its member can't be assigned afterwards.
                if (IsShapedOrBareNavigationCollection(arguments[i], mapping))
                {
                    args[i] = BuildEmptyProjectedCollection(paramType);
                    continue;
                }

                var col = columns[columnCursor];
                var offset = columnCursor + startOffset;
                columnCursor++;

                var readValue = col.Converter != null
                    ? BuildConverterReadExpression(reader, col.Converter!, paramType, offset)
                    : GetOptimizedReaderCall(reader, paramType, offset);
                if (CanSkipDbNullCheck(parameters[i], arguments[i], col))
                {
                    args[i] = readValue;
                }
                else
                {
                    var defaultValue = Expression.Default(paramType);
                    var isDbNull = Expression.Call(reader, Methods.IsDbNull, Expression.Constant(offset));
                    args[i] = Expression.Condition(isDbNull, defaultValue, readValue);
                }
            }

            var body = Expression.Convert(Expression.New(ctor, args), typeof(object));
            return Expression.Lambda<Func<DbDataReader, object>>(body, reader).Compile();
        }

        /// <summary>
        /// Builds an empty, mutable <see cref="List{T}"/> assignable to a projected collection member so a
        /// split-query stitch can populate it in place. The member type comes from the shaped collection's
        /// terminator: <c>ToList</c>/<c>AsEnumerable</c> yield a <c>List&lt;T&gt;</c>-assignable type. A
        /// <c>ToArray</c> member (<c>T[]</c>) is fixed-size and can't be populated after construction, so it
        /// is rejected with guidance rather than silently returning an empty array.
        /// </summary>
        private static Expression BuildEmptyProjectedCollection(Type memberType)
        {
            var elementType = memberType.IsArray
                ? memberType.GetElementType()
                : (memberType.IsGenericType ? memberType.GetGenericArguments()[0] : null);
            if (elementType == null)
                throw new nORM.Core.NormQueryException(
                    $"Cannot project a navigation collection into '{memberType}'.");
            var listType = typeof(List<>).MakeGenericType(elementType);
            if (!memberType.IsAssignableFrom(listType))
                throw new nORM.Core.NormUnsupportedFeatureException(
                    $"An anonymous-type projection can't populate a '{memberType.Name}' collection member. Use " +
                    ".ToList() for the projected collection, or project into a named type with a settable property.");
            return Expression.New(listType);
        }

        private static bool CanSkipDbNullCheck(ParameterInfo parameter, Expression argument, Column column)
        {
            // SQL expressions such as SUM/AVG over no matching rows can return NULL even
            // when the CLR projection member is non-nullable. Only skip the reader null
            // guard for direct mapped column projections whose mapping says NOT NULL.
            if (argument is not MemberExpression || column.IsNullable)
                return false;

            var parameterType = parameter.ParameterType;
            if (parameterType.IsValueType)
                return Nullable.GetUnderlyingType(parameterType) == null;

            var nullabilityCtx = _nullabilityInfoContext;
            if (nullabilityCtx == null)
                return false;

            try
            {
                NullabilityInfo nullabilityInfo;
                lock (_nullabilityInfoContextLock)
                {
                    nullabilityInfo = nullabilityCtx.Create(parameter);
                }
                return nullabilityInfo.ReadState == NullabilityState.NotNull;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }
    }
}
