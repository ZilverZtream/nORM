using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace nORM.SourceGenerators
{
    /// <summary>
    /// Roslyn source generator that produces materializer registrations for
    /// <c>[GenerateMaterializer]</c>-annotated entity types and compiled query
    /// implementations for <c>[CompileTimeQuery]</c>-annotated partial methods.
    /// </summary>
    [Generator]
    public sealed class MaterializerQueryGenerator : ISourceGenerator
    {
        /// <summary>
        /// Minimum number of parameters required on a <c>[CompileTimeQuery]</c> method.
        /// The first parameter must be a <c>DbContext</c>.
        /// </summary>
        private const int MinQueryMethodParameters = 1;

#pragma warning disable RS2008 // Enable analyzer release tracking
        private static readonly DiagnosticDescriptor SG001 = new DiagnosticDescriptor(
            id: "nORMSG001",
            title: "Unsupported return type for [CompileTimeQuery]",
            messageFormat: "Method '{0}' has return type '{1}' which is not supported by [CompileTimeQuery]. Return type must be Task<List<T>>.",
            category: "nORM.SourceGeneration",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor SG002 = new DiagnosticDescriptor(
            id: "nORMSG002",
            title: "Unsupported containing type for [CompileTimeQuery]",
            messageFormat: "Method '{0}' must be in a non-nested, public, static partial class to use [CompileTimeQuery]",
            category: "nORM.SourceGeneration",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor SG003 = new DiagnosticDescriptor(
            id: "nORMSG003",
            title: "Type lacks parameterless constructor for [GenerateMaterializer]",
            messageFormat: "Type '{0}' does not have a public parameterless constructor. [GenerateMaterializer] requires parameterless construction.",
            category: "nORM.SourceGeneration",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor SG004 = new DiagnosticDescriptor(
            id: "nORMSG004",
            title: "Entity type for [CompileTimeQuery] lacks [GenerateMaterializer]",
            messageFormat: "Entity type '{0}' used in [CompileTimeQuery] does not have [GenerateMaterializer]. The query will use the runtime materializer, which may be slower. Add [GenerateMaterializer] to '{0}' for best performance.",
            category: "nORM.SourceGeneration",
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true);
#pragma warning restore RS2008

        private sealed class SyntaxReceiver : ISyntaxContextReceiver
        {
            public List<(ClassDeclarationSyntax Syntax, INamedTypeSymbol Symbol)> CandidateTypes { get; } = new();
            public List<(MethodDeclarationSyntax Syntax, IMethodSymbol Symbol)> CandidateMethods { get; } = new();

            public void OnVisitSyntaxNode(GeneratorSyntaxContext context)
            {
                // Only collect classes that have at least one attribute (potential [GenerateMaterializer] targets)
                if (context.Node is ClassDeclarationSyntax cds && cds.AttributeLists.Count > 0)
                {
                    if (context.SemanticModel.GetDeclaredSymbol(cds) is INamedTypeSymbol typeSymbol)
                    {
                        CandidateTypes.Add((cds, typeSymbol));
                    }
                }
                else if (context.Node is MethodDeclarationSyntax mds)
                {
                    if (mds.Body == null && mds.ExpressionBody == null)
                    {
                        if (context.SemanticModel.GetDeclaredSymbol(mds) is IMethodSymbol methodSymbol)
                        {
                            CandidateMethods.Add((mds, methodSymbol));
                        }
                    }
                }
            }
        }

        public void Initialize(GeneratorInitializationContext context)
        {
            context.RegisterForSyntaxNotifications(() => new SyntaxReceiver());
        }

        public void Execute(GeneratorExecutionContext context)
        {
            if (context.SyntaxContextReceiver is not SyntaxReceiver receiver)
                return;

            var matAttr = context.Compilation.GetTypeByMetadataName("nORM.SourceGeneration.GenerateMaterializerAttribute");
            var queryAttr = context.Compilation.GetTypeByMetadataName("nORM.SourceGeneration.CompileTimeQueryAttribute");

            foreach (var candidate in receiver.CandidateTypes)
            {
                if (matAttr != null && HasAttribute(candidate.Symbol, matAttr))
                {
                    GenerateMaterializer(candidate.Symbol, context);
                }
            }

            foreach (var method in receiver.CandidateMethods)
            {
                if (queryAttr != null && TryGetAttribute(method.Symbol, queryAttr, out var attrData))
                {
                    if (attrData?.ConstructorArguments.Length == 1 && attrData.ConstructorArguments[0].Value is string sql)
                    {
                        GenerateQuery(method.Symbol, sql, matAttr, context);
                    }
                }
            }
        }

        private static bool HasAttribute(ISymbol symbol, INamedTypeSymbol attr)
            => symbol.GetAttributes().Any(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, attr));

        private static bool TryGetAttribute(ISymbol symbol, INamedTypeSymbol attr, out AttributeData? data)
        {
            data = symbol.GetAttributes().FirstOrDefault(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, attr));
            return data != null;
        }

        /// <summary>
        /// Returns the database table name for an entity type, honouring
        /// <c>[System.ComponentModel.DataAnnotations.Schema.Table("name")]</c> when present.
        /// Falls back to the CLR type name, matching <c>CompiledMaterializerStore.GetTableName</c>.
        /// </summary>
        private static string GetTableNameForType(INamedTypeSymbol type)
        {
            foreach (var attr in type.GetAttributes())
            {
                var cls = attr.AttributeClass?.ToDisplayString();
                if (cls == "System.ComponentModel.DataAnnotations.Schema.TableAttribute"
                    && attr.ConstructorArguments.Length > 0
                    && attr.ConstructorArguments[0].Value is string tblName
                    && !string.IsNullOrEmpty(tblName))
                    return tblName;
            }
            return type.Name;
        }

        /// <summary>
        /// Returns the database column name for a property, honouring
        /// <c>[System.ComponentModel.DataAnnotations.Schema.Column("name")]</c> when present.
        /// Fluent <c>HasColumnName</c> is runtime-only and cannot be resolved at compile time;
        /// entities that rely solely on fluent renames must not use <c>[GenerateMaterializer]</c>.
        /// </summary>
        /// <summary>SG2: escapes a value for embedding in a C# regular string literal.</summary>
        private static string EscapeCSharpLiteral(string s)
            => s.Replace("\\", "\\\\").Replace("\"", "\\\"");

        private static string GetColumnName(IPropertySymbol prop)
        {
            foreach (var attr in prop.GetAttributes())
            {
                var cls = attr.AttributeClass?.ToDisplayString();
                if (cls == "System.ComponentModel.DataAnnotations.Schema.ColumnAttribute"
                    && attr.ConstructorArguments.Length > 0
                    && attr.ConstructorArguments[0].Value is string colName
                    && !string.IsNullOrEmpty(colName))
                    return colName;
            }
            return prop.Name;
        }

        private void GenerateMaterializer(INamedTypeSymbol type, GeneratorExecutionContext context)
        {
            var ns = type.ContainingNamespace.IsGlobalNamespace ? null : type.ContainingNamespace.ToDisplayString();
            var typeName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var simpleName = type.Name;
            var props = type.GetMembers().OfType<IPropertySymbol>()
                .Where(p => !p.IsStatic && p.GetMethod != null && p.SetMethod != null)
                .OrderBy(p => p.Name)
                .ToList();

            // Emit diagnostic when type has no public parameterless constructor.
            var hasParameterlessCtor = type.Constructors.Any(c =>
                c.Parameters.Length == 0 &&
                c.DeclaredAccessibility == Microsoft.CodeAnalysis.Accessibility.Public);
            if (!hasParameterlessCtor)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    SG003,
                    type.Locations.FirstOrDefault(),
                    type.Name));
                return;
            }

            // Collect (ordinalVarName, columnName) pairs first so we can emit
            // reader.GetOrdinal("ColumnName") lookups instead of hardcoded positional indices.
            // Use mapped column name from [Column] attribute when present, else prop.Name.
            // For owned types: use {ownerColumnName}_{ownedColumnName} (mirrors runtime convention).
            var ordinalEntries = new List<(string VarName, string ColName)>();
            foreach (var prop in props)
            {
                if (IsOwnedType(prop.Type))
                {
                    var ownedType = (INamedTypeSymbol)prop.Type;
                    var ownedProps = ownedType.GetMembers().OfType<IPropertySymbol>()
                        .Where(p => !p.IsStatic && p.GetMethod != null && p.SetMethod != null)
                        .OrderBy(p => p.Name);
                    var ownerColName = GetColumnName(prop);
                    foreach (var op in ownedProps)
                        ordinalEntries.Add(($"__ord_{prop.Name}_{op.Name}", $"{ownerColName}_{GetColumnName(op)}"));
                }
                else
                {
                    ordinalEntries.Add(($"__ord_{prop.Name}", GetColumnName(prop)));
                }
            }

            var sb = new StringBuilder();
            sb.AppendLine("using System;");
            sb.AppendLine("using System.Data.Common;");
            sb.AppendLine("using nORM.SourceGeneration;");
            if (ns != null) sb.AppendLine($"namespace {ns};");
            sb.AppendLine($"internal static class Materializer_{simpleName}");
            sb.AppendLine("{");
            sb.AppendLine("    [global::System.Runtime.CompilerServices.ModuleInitializer]");
            sb.AppendLine("    public static void Register()");
            sb.AppendLine("    {");
            // Pass compile-time-resolved table name so multi-model scenarios
            // where the same CLR type maps to different tables each get their own materializer.
            var resolvedTableName = GetTableNameForType(type);
            var escapedTableName = resolvedTableName.Replace("\\", "\\\\").Replace("\"", "\\\"");
            sb.AppendLine($"        CompiledMaterializerStore.Add<{typeName}>(\"{escapedTableName}\", reader =>");
            sb.AppendLine("        {");
            sb.AppendLine($"            var entity = new {typeName}();");

            // Emit ordinal resolution - name-based, correct for any column order.
            // SG2: escape column names so backslash/quote in [Column("...")] values
            // produce valid C# string literals instead of compile errors.
            foreach (var (varName, colName) in ordinalEntries)
                sb.AppendLine($"            int {varName} = reader.GetOrdinal(\"{EscapeCSharpLiteral(colName)}\");");

            // Emit property assignments using resolved ordinals
            var entryIndex = 0;
            foreach (var prop in props)
            {
                if (IsOwnedType(prop.Type))
                {
                    var ownedType = (INamedTypeSymbol)prop.Type;
                    var ownedProps = ownedType.GetMembers().OfType<IPropertySymbol>()
                        .Where(p => !p.IsStatic && p.GetMethod != null && p.SetMethod != null)
                        .OrderBy(p => p.Name)
                        .ToList();
                    foreach (var op in ownedProps)
                        sb.AppendLine(BuildOwnedAssignmentExpression(prop, op, ownedType, ordinalEntries[entryIndex++].VarName));
                }
                else
                {
                    sb.AppendLine(BuildAssignmentExpression(prop, ordinalEntries[entryIndex++].VarName));
                }
            }

            sb.AppendLine("            return entity;");
            sb.AppendLine("        });");
            sb.AppendLine("    }");
            sb.AppendLine("}");

            // Use fully-qualified type name as hint to avoid collisions when two classes
            // share the same simple name but live in different namespaces.
            var hintName = ns != null
                ? $"{ns.Replace(".", "_")}_{simpleName}_Materializer.g.cs"
                : $"{simpleName}_Materializer.g.cs";
            context.AddSource(hintName, SourceText.From(sb.ToString(), Encoding.UTF8));
        }

        private static string BuildAssignmentExpression(IPropertySymbol prop, string ordVar)
        {
            var read = GetReaderExpression(prop.Type, ordVar);
            var typeName = prop.Type.ToDisplayString();
            var isNullableValueType = prop.Type is INamedTypeSymbol nts
                && nts.IsGenericType
                && nts.ConstructedFrom.SpecialType == SpecialType.System_Nullable_T;
            var needsNullCheck = prop.Type.IsReferenceType
                || prop.NullableAnnotation == NullableAnnotation.Annotated
                || isNullableValueType
                || typeName.Contains("DateOnly") || typeName.Contains("TimeOnly");
            return needsNullCheck
                ? $"            if (!reader.IsDBNull({ordVar})) entity.{prop.Name} = {read};"
                : $"            entity.{prop.Name} = {read};";
        }

        private static bool IsOwnedType(ITypeSymbol type)
            => type.GetAttributes().Any(a => a.AttributeClass?.ToDisplayString() == "nORM.Mapping.OwnedAttribute");

        private static string BuildOwnedAssignmentExpression(IPropertySymbol owner, IPropertySymbol ownedProp, INamedTypeSymbol ownedType, string ordVar)
        {
            var read = GetReaderExpression(ownedProp.Type, ordVar);
            var ownedPropTypeName = ownedProp.Type.ToDisplayString();
            var isNullableValueType = ownedProp.Type is INamedTypeSymbol nvt
                && nvt.IsGenericType
                && nvt.ConstructedFrom.SpecialType == SpecialType.System_Nullable_T;
            var needsNullCheck = ownedProp.Type.IsReferenceType
                || ownedProp.NullableAnnotation == NullableAnnotation.Annotated
                || isNullableValueType
                || ownedPropTypeName.Contains("DateOnly") || ownedPropTypeName.Contains("TimeOnly");
            var ownedTypeName = ownedType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var ensureOwner = $"if (entity.{owner.Name} == null) entity.{owner.Name} = new {ownedTypeName}(); ";
            return needsNullCheck
                ? $"            if (!reader.IsDBNull({ordVar})) {{ {ensureOwner}entity.{owner.Name}.{ownedProp.Name} = {read}; }}"
                : $"            {ensureOwner}entity.{owner.Name}.{ownedProp.Name} = {read};";
        }

        private static string GetReaderExpression(ITypeSymbol type, string ordVar)
        {
            if (type is INamedTypeSymbol named && named.IsGenericType && named.ConstructedFrom.SpecialType == SpecialType.System_Nullable_T)
            {
                type = named.TypeArguments[0];
            }

            if (type.TypeKind == TypeKind.Enum && type is INamedTypeSymbol enumType)
            {
                var underlying = enumType.EnumUnderlyingType!;
                var underlyingExpr = GetReaderExpression(underlying, ordVar);
                var enumName = enumType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                return $"({enumName}){underlyingExpr}";
            }

            var typeName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            switch (type.SpecialType)
            {
                case SpecialType.System_Boolean: return $"reader.GetBoolean({ordVar})";
                case SpecialType.System_Byte: return $"reader.GetByte({ordVar})";
                case SpecialType.System_Int16: return $"reader.GetInt16({ordVar})";
                case SpecialType.System_Int32: return $"reader.GetInt32({ordVar})";
                case SpecialType.System_Int64: return $"reader.GetInt64({ordVar})";
                case SpecialType.System_Single: return $"reader.GetFloat({ordVar})";
                case SpecialType.System_Double: return $"reader.GetDouble({ordVar})";
                case SpecialType.System_Decimal: return $"reader.GetDecimal({ordVar})";
                case SpecialType.System_DateTime: return $"reader.GetDateTime({ordVar})";
                case SpecialType.System_String: return $"reader.GetString({ordVar})";
            }

            if (typeName == "global::System.Guid")
                return $"reader.GetGuid({ordVar})";
            if (typeName == "global::System.Byte[]")
                return $"reader.GetFieldValue<byte[]>({ordVar})";

            // DateOnly/TimeOnly require explicit conversion because providers may
            // return DateTime, TimeSpan, or string rather than native DateOnly/TimeOnly.
            // This matches the runtime materializer's ConvertToDateOnly/ConvertToTimeOnly helpers.
            if (typeName == "global::System.DateOnly")
                return $"nORM.Query.MaterializerFactory.ConvertToDateOnly(reader.GetValue({ordVar}))";
            if (typeName == "global::System.TimeOnly")
                return $"nORM.Query.MaterializerFactory.ConvertToTimeOnly(reader.GetValue({ordVar}))";

            return $"reader.GetFieldValue<{typeName}>({ordVar})";
        }

        private void GenerateQuery(IMethodSymbol method, string sql, INamedTypeSymbol? matAttr, GeneratorExecutionContext context)
        {
            var cls = method.ContainingType;
            var ns = cls.ContainingNamespace.IsGlobalNamespace ? null : cls.ContainingNamespace.ToDisplayString();
            var className = cls.Name;
            var methodName = method.Name;

            // Emit diagnostic for unsupported return types
            if (!TryGetEntityType(method.ReturnType, out var entity))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    SG001,
                    method.Locations.FirstOrDefault(),
                    method.Name,
                    method.ReturnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)));
                return;
            }

            // Emit diagnostic for unsupported containing type shapes
            if (cls.ContainingType != null || !cls.IsStatic || cls.DeclaredAccessibility != Microsoft.CodeAnalysis.Accessibility.Public)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    SG002,
                    method.Locations.FirstOrDefault(),
                    method.Name));
                return;
            }

            var entityTypeName = entity!.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

            // SG1/SG2: Emit warning when entity lacks [GenerateMaterializer].
            // The generated query will fall back to runtime materialization via
            // DbContext.GetCompiledQueryMaterializer, which correctly applies runtime
            // guards (fluent renames, converters, owned navigations) and is safe but slower.
            if (matAttr != null && !HasAttribute(entity!, matAttr))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    SG004,
                    method.Locations.FirstOrDefault(),
                    entity!.Name));
            }

            // Guard: the first parameter must be the DbContext; methods with zero parameters are invalid.
            if (method.Parameters.Length < MinQueryMethodParameters)
                return;

            var ctxParam = method.Parameters[0].Name;
            var ctSymbol = method.Parameters.FirstOrDefault(p => p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) == "global::System.Threading.CancellationToken");
            var ctParam = ctSymbol?.Name;
            var queryParams = method.Parameters.Skip(1).Where(p => !SymbolEqualityComparer.Default.Equals(p, ctSymbol)).ToList();

            var paramList = string.Join(", ", method.Parameters.Select(p => $"{p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)} {p.Name}"));

            var sb = new StringBuilder();
            sb.AppendLine("using System;");
            sb.AppendLine("using System.Collections.Generic;");
            sb.AppendLine("using System.Data;");
            sb.AppendLine("using System.Threading;");
            sb.AppendLine("using System.Threading.Tasks;");
            sb.AppendLine("using nORM.Core;");
            if (ns != null) sb.AppendLine($"namespace {ns};");
            sb.AppendLine($"public static partial class {className}");
            sb.AppendLine("{");
            sb.AppendLine($"    public static async partial System.Threading.Tasks.Task<System.Collections.Generic.List<{entityTypeName}>> {methodName}({paramList})");
            sb.AppendLine("    {");
            // Use CreateCompiledQueryCommandAsync instead of ctx.Connection.CreateCommand()
            // so that connection initialisation, transaction binding, and interceptors all fire.
            var ctArg = ctParam ?? "System.Threading.CancellationToken.None";
            sb.AppendLine($"        await using var cmd = await {ctxParam}.CreateCompiledQueryCommandAsync({ctArg}).ConfigureAwait(false);");
            var escapedSql = sql.Replace("\"", "\"\"");
            sb.AppendLine($"        cmd.CommandText = @\"{escapedSql}\";");
            // Route parameter binding through AddOptimizedParam (the same binder
            // used by runtime queries) so that DateOnly, TimeOnly, char, enum, and typed-null
            // coercions are applied uniformly.
            foreach (var p in queryParams)
            {
                sb.AppendLine($"        nORM.Internal.ParameterOptimizer.AddOptimizedParam(cmd, $\"{{{ctxParam}.Provider.ParamPrefix}}{p.Name}\", {GetParameterValueExpression(p)});");
            }
            // SG1/SG2: Use GetCompiledQueryMaterializer instead of CompiledMaterializerStore.Get.
            // This respects the same eligibility guards as MaterializerFactory (fluent renames,
            // converters, owned navigations) and falls back to the runtime materializer when any
            // unsafe condition is present, preventing wrong-value hydration and KeyNotFoundException.
            var queryTableName = GetTableNameForType(entity!);
            var escapedQueryTableName = queryTableName.Replace("\\", "\\\\").Replace("\"", "\\\"");
            sb.AppendLine($"        var materializer = {ctxParam}.GetCompiledQueryMaterializer<{entityTypeName}>(\"{escapedQueryTableName}\");");
            // Use ExecuteCompiledQueryListAsync instead of cmd.ExecuteReaderAsync so that
            // command interceptors (logging, tracing, auditing) are invoked.
            sb.AppendLine($"        return await {ctxParam}.ExecuteCompiledQueryListAsync(cmd, materializer, {ctArg}).ConfigureAwait(false);");
            sb.AppendLine("    }");
            sb.AppendLine("}");

            context.AddSource($"{className}_{methodName}_Query.g.cs", SourceText.From(sb.ToString(), Encoding.UTF8));
        }

        private static bool TryGetEntityType(ITypeSymbol returnType, out INamedTypeSymbol? entity)
        {
            entity = null;
            if (returnType is INamedTypeSymbol taskType &&
                taskType.IsGenericType &&
                taskType.Name == "Task" &&
                taskType.ContainingNamespace.ToDisplayString() == "System.Threading.Tasks")
            {
                if (taskType.TypeArguments[0] is INamedTypeSymbol listType &&
                    listType.IsGenericType &&
                    listType.Name == "List" &&
                    listType.ContainingNamespace.ToDisplayString() == "System.Collections.Generic")
                {
                    if (listType.TypeArguments[0] is INamedTypeSymbol ent)
                    {
                        entity = ent;
                    }
                }
            }

            return entity != null;
        }

        private static string GetParameterValueExpression(IParameterSymbol param)
        {
            var type = param.Type;
            if (type.IsReferenceType || param.NullableAnnotation == NullableAnnotation.Annotated)
            {
                return $"{param.Name} ?? (object)DBNull.Value";
            }

            if (type is INamedTypeSymbol named && named.IsGenericType && named.ConstructedFrom.SpecialType == SpecialType.System_Nullable_T)
            {
                return $"{param.Name}.HasValue ? {param.Name}.Value : (object)DBNull.Value";
            }

            return param.Name;
        }
    }
}
