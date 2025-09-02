using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using System.Reflection.Emit;
using nORM.Configuration;
using nORM.Providers;

#nullable enable

namespace nORM.Mapping
{
    public sealed class Column
    {
        public readonly PropertyInfo Prop;
        public readonly string PropName, EscCol;
        public readonly bool IsKey, IsDbGenerated, IsTimestamp;
        public readonly string? ForeignKeyPrincipalTypeName;
        public readonly Func<object, object?> Getter;
        public readonly Action<object, object?> Setter;
        public readonly MethodInfo SetterMethod;

        public Column(PropertyInfo pi, DatabaseProvider p, IEntityTypeConfiguration? fluentConfig)
        {
            Prop = pi;
            PropName = pi.Name;

            var fluentColName = fluentConfig?.ColumnNames.TryGetValue(pi, out var name) == true ? name : null;
            EscCol = p.Escape(fluentColName ?? pi.GetCustomAttribute<ColumnAttribute>()?.Name ?? pi.Name);

            IsKey = fluentConfig?.KeyProperty == pi || pi.GetCustomAttribute<KeyAttribute>() != null;
            IsTimestamp = pi.GetCustomAttribute<TimestampAttribute>() != null;
            IsDbGenerated = pi.GetCustomAttribute<DatabaseGeneratedAttribute>()?.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity;
            ForeignKeyPrincipalTypeName = pi.GetCustomAttribute<ForeignKeyAttribute>()?.Name;

            Getter = CreateGetterDelegate(pi);
            Setter = CreateSetterDelegate(pi);
            SetterMethod = pi.GetSetMethod()!;
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