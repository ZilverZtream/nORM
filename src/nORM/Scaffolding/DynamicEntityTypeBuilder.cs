#nullable enable
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal static class DynamicEntityTypeBuilder
    {
        /// <summary>Namespace prefix used for all dynamically generated entity types.</summary>
        private const string DynamicTypeNamespace = "nORM.Dynamic";

        /// <summary>Name of the shared dynamic assembly that hosts all generated entity types.</summary>
        private const string DynamicAssemblyName = "nORM.Dynamic.Entities";

        /// <summary>Name of the dynamic module within the shared assembly.</summary>
        private const string DynamicModuleName = "MainModule";

        // Shared static AssemblyBuilder and ModuleBuilder for all generated types,
        // preventing unloadable assembly accumulation when types are evicted from cache.
        private static readonly AssemblyBuilder SharedAssembly;
        private static readonly ModuleBuilder SharedModule;
        private static readonly object ModuleBuilderLock = new();
        private static long _typeCounter;

        static DynamicEntityTypeBuilder()
        {
            var assemblyName = new AssemblyName(DynamicAssemblyName);
            SharedAssembly = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
            SharedModule = SharedAssembly.DefineDynamicModule(DynamicModuleName);
        }

        /// <summary>
        /// Builds a dynamic CLR type from the given column descriptors using the shared <see cref="ModuleBuilder"/>.
        /// Each invocation generates a uniquely-named type to prevent conflicts when the same table
        /// is regenerated after a schema change.
        /// </summary>
        public static Type BuildType(
            string? schemaName,
            string tableName,
            IReadOnlyList<DynamicEntityTypeGenerator.ColumnInfo> columns,
            bool isReadOnlyEntity)
        {
            lock (ModuleBuilderLock)
            {
                var className = ScaffoldNameHelper.EscapeCSharpIdentifier(ScaffoldNameHelper.ToPascalCase(tableName));

                var typeId = Interlocked.Increment(ref _typeCounter);
                var uniqueTypeName = $"{DynamicTypeNamespace}.{className}_{typeId}";

                var typeBuilder = SharedModule.DefineType(
                    uniqueTypeName,
                    TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.Sealed,
                    typeof(object));

                typeBuilder.DefineDefaultConstructor(
                    MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName);

                var tableAttrCtor = typeof(TableAttribute).GetConstructor(new[] { typeof(string) })!;
                if (schemaName is null)
                {
                    typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(tableAttrCtor, new object[] { tableName }));
                }
                else
                {
                    var schemaProperty = typeof(TableAttribute).GetProperty(nameof(TableAttribute.Schema))!;
                    typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(
                        tableAttrCtor,
                        new object[] { tableName },
                        new[] { schemaProperty },
                        new object[] { schemaName }));
                }

                var orderedColumns = OrderDynamicColumns(columns);

                if (isReadOnlyEntity || !orderedColumns.Any(static column => column.IsKey))
                {
                    var readOnlyAttrCtor = typeof(ReadOnlyEntityAttribute).GetConstructor(Type.EmptyTypes)!;
                    typeBuilder.SetCustomAttribute(new CustomAttributeBuilder(readOnlyAttrCtor, Array.Empty<object>()));
                }

                foreach (var col in orderedColumns)
                    AppendProperty(typeBuilder, col);

                return typeBuilder.CreateType()!;
            }
        }

        private static IReadOnlyList<DynamicEntityTypeGenerator.ColumnInfo> OrderDynamicColumns(
            IReadOnlyList<DynamicEntityTypeGenerator.ColumnInfo> columns)
            => columns
                .OrderBy(static column => column.IsKey ? 0 : 1)
                .ThenBy(static column => column.IsKey ? column.KeyOrdinal : int.MaxValue)
                .ThenBy(static column => column.SourceOrdinal)
                .ToArray();

        private static void AppendProperty(TypeBuilder typeBuilder, DynamicEntityTypeGenerator.ColumnInfo column)
        {
            var propertyType = column.PropertyType;
            var fieldBuilder = typeBuilder.DefineField($"_{column.PropertyName}", propertyType, FieldAttributes.Private);
            var propertyBuilder = typeBuilder.DefineProperty(column.PropertyName, PropertyAttributes.HasDefault, propertyType, null);

            var columnAttrCtor = typeof(ColumnAttribute).GetConstructor(new[] { typeof(string) })!;
            var columnAttr = column.DecimalPrecision is { } decimalPrecision
                ? new CustomAttributeBuilder(
                    columnAttrCtor,
                    new object[] { column.ColumnName },
                    new[] { typeof(ColumnAttribute).GetProperty(nameof(ColumnAttribute.TypeName))! },
                    new object[]
                    {
                        FormatDecimalTypeName(decimalPrecision)
                    })
                : new CustomAttributeBuilder(columnAttrCtor, new object[] { column.ColumnName });
            propertyBuilder.SetCustomAttribute(columnAttr);

            if (column.IsKey)
            {
                var keyAttrCtor = typeof(KeyAttribute).GetConstructor(Type.EmptyTypes)!;
                var keyAttr = new CustomAttributeBuilder(keyAttrCtor, Array.Empty<object>());
                propertyBuilder.SetCustomAttribute(keyAttr);
            }

            if (column.IsRowVersion)
            {
                var timestampAttrCtor = typeof(TimestampAttribute).GetConstructor(Type.EmptyTypes)!;
                var timestampAttr = new CustomAttributeBuilder(timestampAttrCtor, Array.Empty<object>());
                propertyBuilder.SetCustomAttribute(timestampAttr);
            }

            if (column.IsAuto || column.IsComputed || column.IsRowVersion)
            {
                var dbGenAttrCtor = typeof(DatabaseGeneratedAttribute).GetConstructor(new[] { typeof(DatabaseGeneratedOption) })!;
                var option = column.IsAuto ? DatabaseGeneratedOption.Identity : DatabaseGeneratedOption.Computed;
                var dbGenAttr = new CustomAttributeBuilder(dbGenAttrCtor, new object[] { option });
                propertyBuilder.SetCustomAttribute(dbGenAttr);
            }

            if (column.MaxLength.HasValue)
            {
                var maxLenAttrCtor = typeof(MaxLengthAttribute).GetConstructor(new[] { typeof(int) })!;
                var maxLenAttr = new CustomAttributeBuilder(maxLenAttrCtor, new object[] { column.MaxLength.Value });
                propertyBuilder.SetCustomAttribute(maxLenAttr);
            }

            if (!propertyType.IsValueType && !column.AllowsNull)
            {
                var requiredAttrCtor = typeof(RequiredAttribute).GetConstructor(Type.EmptyTypes)!;
                var requiredAttr = new CustomAttributeBuilder(requiredAttrCtor, Array.Empty<object>());
                propertyBuilder.SetCustomAttribute(requiredAttr);
            }

            var getMethod = typeBuilder.DefineMethod(
                $"get_{column.PropertyName}",
                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                propertyType,
                Type.EmptyTypes);

            var getIl = getMethod.GetILGenerator();
            getIl.Emit(OpCodes.Ldarg_0);
            getIl.Emit(OpCodes.Ldfld, fieldBuilder);
            getIl.Emit(OpCodes.Ret);

            var setMethod = typeBuilder.DefineMethod(
                $"set_{column.PropertyName}",
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

        private static string FormatDecimalTypeName(DynamicEntityTypeGenerator.ScaffoldDecimalPrecision decimalPrecision)
        {
            var precision = decimalPrecision.Precision.ToString(CultureInfo.InvariantCulture);
            return decimalPrecision.Scale.HasValue
                ? $"decimal({precision},{decimalPrecision.Scale.Value.ToString(CultureInfo.InvariantCulture)})"
                : $"decimal({precision})";
        }
    }
}
