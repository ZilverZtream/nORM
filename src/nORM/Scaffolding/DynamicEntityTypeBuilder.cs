#nullable enable
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTypeBuilder
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
    }
}
