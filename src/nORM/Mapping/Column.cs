using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using System.Reflection.Emit;
using nORM.Configuration;
using nORM.Providers;
using nORM.Internal;

#nullable enable

namespace nORM.Mapping
{
    public sealed class Column
    {
        public readonly PropertyInfo Prop;
        public readonly string PropName, EscCol;
        public readonly bool IsKey, IsDbGenerated, IsTimestamp, IsShadow;
        public readonly string? ForeignKeyPrincipalTypeName;
        public readonly Func<object, object?> Getter;
        public readonly Action<object, object?> Setter;
        public readonly MethodInfo SetterMethod;

        public Column(PropertyInfo pi, DatabaseProvider p, IEntityTypeConfiguration? fluentConfig, string? prefix = null,
            Func<object, object?>? getterOverride = null, Action<object, object?>? setterOverride = null,
            MethodInfo? setterMethodOverride = null)
        {
            Prop = pi;
            PropName = (prefix != null ? prefix + "_" : "") + pi.Name;

            var fluentColName = fluentConfig?.ColumnNames.TryGetValue(pi, out var name) == true ? name : null;
            var colName = fluentColName ?? pi.GetCustomAttribute<ColumnAttribute>()?.Name ?? PropName;
            EscCol = p.Escape(colName);

            IsKey = (fluentConfig?.KeyProperties.Contains(pi) ?? false) || pi.GetCustomAttribute<KeyAttribute>() != null;
            IsTimestamp = pi.GetCustomAttribute<TimestampAttribute>() != null;
            IsDbGenerated = pi.GetCustomAttribute<DatabaseGeneratedAttribute>()?.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity;
            ForeignKeyPrincipalTypeName = pi.GetCustomAttribute<ForeignKeyAttribute>()?.Name;
            if (ForeignKeyPrincipalTypeName == null && !IsKey)
            {
                var propName = pi.Name;
                if (propName.EndsWith("Id", StringComparison.OrdinalIgnoreCase) && propName.Length > 2)
                {
                    var principalName = propName[..^2];
                    if (!string.Equals(principalName, "Id", StringComparison.OrdinalIgnoreCase))
                        ForeignKeyPrincipalTypeName = principalName;
                }
                else
                {
                    var underscore = propName.IndexOf('_');
                    if (underscore > 0)
                        ForeignKeyPrincipalTypeName = propName.Substring(0, underscore);
                }
            }

            Getter = getterOverride ?? CreateGetterDelegate(pi);
            Setter = setterOverride ?? CreateSetterDelegate(pi);
            SetterMethod = setterMethodOverride ?? pi.GetSetMethod()!;
            IsShadow = false;
        }

        public Column(string name, Type clrType, Type declaringType, DatabaseProvider p, string? columnName = null)
        {
            Prop = new ShadowPropertyInfo(name, clrType, declaringType);
            PropName = name;
            var colName = columnName ?? name;
            EscCol = p.Escape(colName);
            IsKey = false;
            IsTimestamp = false;
            IsDbGenerated = false;
            ForeignKeyPrincipalTypeName = null;
            var propName = name;
            if (propName.EndsWith("Id", StringComparison.OrdinalIgnoreCase) && propName.Length > 2)
            {
                var principalName = propName[..^2];
                if (!string.Equals(principalName, "Id", StringComparison.OrdinalIgnoreCase))
                    ForeignKeyPrincipalTypeName = principalName;
            }
            else
            {
                var underscore = propName.IndexOf('_');
                if (underscore > 0)
                    ForeignKeyPrincipalTypeName = propName.Substring(0, underscore);
            }
            Getter = e => ShadowPropertyStore.Get(e, name);
            Setter = (e, v) => ShadowPropertyStore.Set(e, name, v);
            SetterMethod = Methods.SetShadowValue;
            IsShadow = true;
        }

        public static Func<object, object?> CreateGetterDelegate(PropertyInfo property)
        {
            var dm = new DynamicMethod("get_" + property.Name, typeof(object), new[] { typeof(object) }, property.DeclaringType!.Module, true);
            var il = dm.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, property.DeclaringType!);
            il.Emit(OpCodes.Callvirt, property.GetGetMethod()!);
            if (property.PropertyType.IsValueType) il.Emit(OpCodes.Box, property.PropertyType);
            il.Emit(OpCodes.Ret);
            return (Func<object, object?>)dm.CreateDelegate(typeof(Func<object, object?>));
        }

        private static Action<object, object?> CreateSetterDelegate(PropertyInfo property)
        {
            var dm = new DynamicMethod("set_" + property.Name, null, new[] { typeof(object), typeof(object) }, property.DeclaringType!.Module, true);
            var il = dm.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, property.DeclaringType!);
            il.Emit(OpCodes.Ldarg_1);
            if (property.PropertyType.IsValueType)
                il.Emit(OpCodes.Unbox_Any, property.PropertyType);
            else
                il.Emit(OpCodes.Castclass, property.PropertyType);
            il.Emit(OpCodes.Callvirt, property.GetSetMethod()!);
            il.Emit(OpCodes.Ret);
            return (Action<object, object?>)dm.CreateDelegate(typeof(Action<object, object?>));
        }
    }
}