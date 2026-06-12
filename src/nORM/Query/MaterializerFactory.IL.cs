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
        private static Func<DbDataReader, object> CreateILMaterializer<T>(int startOffset = 0) where T : class
        {
            var type = typeof(T);
            var method = new DynamicMethod("Materialize", typeof(object), new[] { typeof(DbDataReader) }, typeof(MaterializerFactory), true);
            var il = method.GetILGenerator();
            var parameterlessCtor = type.GetConstructor(Type.EmptyTypes);

            if (parameterlessCtor != null)
            {
                var props = _propertiesCache.GetOrAdd(type, t => t.GetProperties(BindingFlags.Instance | BindingFlags.Public));
                il.DeclareLocal(type); // local 0: entity

                il.Emit(OpCodes.Newobj, parameterlessCtor);
                il.Emit(OpCodes.Stloc_0);

                for (int i = 0; i < props.Length; i++)
                {
                    var prop = props[i];
                    if (!prop.CanWrite) continue;

                    var skip = il.DefineLabel();
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, i + startOffset);
                    il.Emit(OpCodes.Callvirt, Methods.IsDbNull);
                    il.Emit(OpCodes.Brtrue_S, skip);

                    il.Emit(OpCodes.Ldloc_0);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, i + startOffset);

                    var readerMethod = Methods.GetReaderMethod(prop.PropertyType);
                    il.Emit(OpCodes.Callvirt, readerMethod);

                    var pType = prop.PropertyType;
                    var underlying = Nullable.GetUnderlyingType(pType);
                    if (underlying != null)
                    {
                        if (readerMethod == Methods.GetValue)
                        {
                            if (underlying.IsEnum)
                            {
                                // MM-2: Nullable<TEnum> - convert via enum helper to avoid InvalidCastException
                                il.Emit(OpCodes.Call, _convertToEnumOpenMethod.MakeGenericMethod(underlying));
                            }
                            else if (underlying == typeof(DateOnly))
                            {
                                // MM1 fix: Nullable<DateOnly> - Convert.ChangeType doesn't support DateOnly
                                il.Emit(OpCodes.Call, _convertToDateOnlyMethod);
                            }
                            else if (underlying == typeof(TimeOnly))
                            {
                                // MM1 fix: Nullable<TimeOnly> - Convert.ChangeType doesn't support TimeOnly
                                il.Emit(OpCodes.Call, _convertToTimeOnlyMethod);
                            }
                            else
                            {
                                il.Emit(OpCodes.Ldtoken, underlying);
                                il.Emit(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle))!);
                                il.Emit(OpCodes.Call, typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] { typeof(object), typeof(Type) })!);
                                il.Emit(OpCodes.Unbox_Any, underlying);
                            }
                        }
                        else if (readerMethod.ReturnType != underlying)
                        {
                            il.Emit(OpCodes.Unbox_Any, underlying);
                        }
                        var ctorNullable = pType.GetConstructor(new[] { underlying })!;
                        il.Emit(OpCodes.Newobj, ctorNullable);
                    }
                    else if (pType.IsEnum)
                    {
                        // MM-2: Enum types - call our helper which handles Int64?int?enum conversion
                        // that Convert.ChangeType cannot do directly (throws InvalidCastException).
                        il.Emit(OpCodes.Call, _convertToEnumOpenMethod.MakeGenericMethod(pType));
                    }
                    else if (pType == typeof(DateOnly))
                    {
                        // MM1 fix: DateOnly - Convert.ChangeType doesn't support DateOnly
                        if (readerMethod.ReturnType == typeof(object))
                        {
                            il.Emit(OpCodes.Call, _convertToDateOnlyMethod);
                        }
                    }
                    else if (pType == typeof(TimeOnly))
                    {
                        // MM1 fix: TimeOnly - Convert.ChangeType doesn't support TimeOnly
                        if (readerMethod.ReturnType == typeof(object))
                        {
                            il.Emit(OpCodes.Call, _convertToTimeOnlyMethod);
                        }
                    }
                    else if (pType.IsValueType)
                    {
                        if (readerMethod.ReturnType == typeof(object))
                        {
                            // Value came from GetValue() as a boxed object; convert then unbox.
                            il.Emit(OpCodes.Ldtoken, pType);
                            il.Emit(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle))!);
                            il.Emit(OpCodes.Call, typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] { typeof(object), typeof(Type) })!);
                            il.Emit(OpCodes.Unbox_Any, pType);
                        }
                        // If the typed getter (GetInt32, GetBoolean, etc.) already returned the
                        // exact value type, it is already on the stack - no unboxing needed.
                    }
                    else if (readerMethod.ReturnType != pType)
                    {
                        if (readerMethod == Methods.GetValue)
                        {
                            il.Emit(OpCodes.Ldtoken, pType);
                            il.Emit(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle))!);
                            il.Emit(OpCodes.Call, typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] { typeof(object), typeof(Type) })!);
                        }
                        il.Emit(OpCodes.Castclass, pType);
                    }

                    il.Emit(OpCodes.Callvirt, prop.GetSetMethod()!);
                    il.MarkLabel(skip);
                }

                il.Emit(OpCodes.Ldloc_0);
                il.Emit(OpCodes.Ret);
                return (Func<DbDataReader, object>)method.CreateDelegate(typeof(Func<DbDataReader, object>));
            }

            // Parameterized constructor path
            var ctors = type.GetConstructors();
            if (ctors.Length == 0)
                throw new InvalidOperationException($"No constructors found for type {type.FullName}");

            var ctor = ctors.OrderByDescending(c => c.GetParameters().Length).First();
            var parameters = ctor.GetParameters();

            var getOrdinal = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetOrdinal))!;
            var ordinals = new LocalBuilder[parameters.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                ordinals[i] = il.DeclareLocal(typeof(int));
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldstr, parameters[i].Name!);
                il.Emit(OpCodes.Callvirt, getOrdinal);
                il.Emit(OpCodes.Stloc, ordinals[i]);
            }

            var convertMethod = typeof(MaterializerFactory).GetMethod(nameof(ConvertDbValue), BindingFlags.NonPublic | BindingFlags.Static)!;

            for (int i = 0; i < parameters.Length; i++)
            {
                var param = parameters[i];
                var pType = param.ParameterType;
                var skip = il.DefineLabel();
                var end = il.DefineLabel();

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldloc, ordinals[i]);
                il.Emit(OpCodes.Callvirt, Methods.IsDbNull);
                il.Emit(OpCodes.Brtrue_S, skip);

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldloc, ordinals[i]);
                il.Emit(OpCodes.Callvirt, Methods.GetValue);
                il.Emit(OpCodes.Ldtoken, pType);
                il.Emit(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle))!);
                il.Emit(OpCodes.Call, convertMethod);

                if (pType.IsValueType)
                {
                    il.Emit(OpCodes.Unbox_Any, pType);
                }
                else
                {
                    il.Emit(OpCodes.Castclass, pType);
                }
                il.Emit(OpCodes.Br_S, end);

                il.MarkLabel(skip);
                if (pType.IsValueType)
                {
                    var loc = il.DeclareLocal(pType);
                    il.Emit(OpCodes.Ldloca_S, loc);
                    il.Emit(OpCodes.Initobj, pType);
                    il.Emit(OpCodes.Ldloc, loc);
                }
                else
                {
                    il.Emit(OpCodes.Ldnull);
                }
                il.MarkLabel(end);
            }

            il.Emit(OpCodes.Newobj, ctor);
            il.Emit(OpCodes.Ret);
            return (Func<DbDataReader, object>)method.CreateDelegate(typeof(Func<DbDataReader, object>));
        }
    }
}
