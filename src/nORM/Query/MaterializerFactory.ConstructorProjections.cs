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
            int startOffset)
        {
            var reader = Expression.Parameter(typeof(DbDataReader), "reader");
            var args = new Expression[parameters.Length];

            for (var i = 0; i < parameters.Length; i++)
            {
                var paramType = parameters[i].ParameterType;
                var readValue = columns[i].Converter != null
                    ? BuildConverterReadExpression(reader, columns[i].Converter!, paramType, i + startOffset)
                    : GetOptimizedReaderCall(reader, paramType, i + startOffset);
                if (CanSkipDbNullCheck(parameters[i], arguments[i], columns[i]))
                {
                    args[i] = readValue;
                }
                else
                {
                    var defaultValue = Expression.Default(paramType);
                    var isDbNull = Expression.Call(reader, Methods.IsDbNull, Expression.Constant(i + startOffset));
                    args[i] = Expression.Condition(isDbNull, defaultValue, readValue);
                }
            }

            var body = Expression.Convert(Expression.New(ctor, args), typeof(object));
            return Expression.Lambda<Func<DbDataReader, object>>(body, reader).Compile();
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
