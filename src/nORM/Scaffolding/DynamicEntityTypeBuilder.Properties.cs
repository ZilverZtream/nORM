#nullable enable
using System;
using System.Reflection;
using System.Reflection.Emit;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTypeBuilder
    {
        private static void AppendProperty(TypeBuilder typeBuilder, DynamicEntityTypeGenerator.ColumnInfo column)
        {
            var propertyType = column.PropertyType;
            var fieldBuilder = typeBuilder.DefineField($"_{column.PropertyName}", propertyType, FieldAttributes.Private);
            var propertyBuilder = typeBuilder.DefineProperty(column.PropertyName, PropertyAttributes.HasDefault, propertyType, null);

            ApplyPropertyAttributes(propertyBuilder, column, propertyType);
            DefinePropertyAccessors(typeBuilder, propertyBuilder, fieldBuilder, column.PropertyName, propertyType);
        }

        private static void DefinePropertyAccessors(
            TypeBuilder typeBuilder,
            PropertyBuilder propertyBuilder,
            FieldBuilder fieldBuilder,
            string propertyName,
            Type propertyType)
        {
            var getMethod = typeBuilder.DefineMethod(
                $"get_{propertyName}",
                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                propertyType,
                Type.EmptyTypes);

            var getIl = getMethod.GetILGenerator();
            getIl.Emit(OpCodes.Ldarg_0);
            getIl.Emit(OpCodes.Ldfld, fieldBuilder);
            getIl.Emit(OpCodes.Ret);

            var setMethod = typeBuilder.DefineMethod(
                $"set_{propertyName}",
                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                null,
                new[] { propertyType });

            var setIl = setMethod.GetILGenerator();
            setIl.Emit(OpCodes.Ldarg_0);
            setIl.Emit(OpCodes.Ldarg_1);
            setIl.Emit(OpCodes.Stfld, fieldBuilder);
            setIl.Emit(OpCodes.Ret);

            propertyBuilder.SetGetMethod(getMethod);
            propertyBuilder.SetSetMethod(setMethod);
        }
    }
}
