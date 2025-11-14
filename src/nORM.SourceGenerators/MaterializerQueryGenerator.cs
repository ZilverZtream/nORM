using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace nORM.SourceGenerators
{
    [Generator]
    public sealed class MaterializerQueryGenerator : ISourceGenerator
    {
        private sealed class SyntaxReceiver : ISyntaxContextReceiver
        {
            public List<(ClassDeclarationSyntax Syntax, INamedTypeSymbol Symbol)> CandidateTypes { get; } = new();
            public List<(MethodDeclarationSyntax Syntax, IMethodSymbol Symbol)> CandidateMethods { get; } = new();

            public void OnVisitSyntaxNode(GeneratorSyntaxContext context)
            {
                if (context.Node is ClassDeclarationSyntax cds)
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
                        GenerateQuery(method.Symbol, sql, context);
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

        private void GenerateMaterializer(INamedTypeSymbol type, GeneratorExecutionContext context)
        {
            var ns = type.ContainingNamespace.IsGlobalNamespace ? null : type.ContainingNamespace.ToDisplayString();
            var typeName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var simpleName = type.Name;
            var props = type.GetMembers().OfType<IPropertySymbol>()
                .Where(p => !p.IsStatic && p.GetMethod != null && p.SetMethod != null)
                .OrderBy(p => p.Name)
                .ToList();

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
            sb.AppendLine($"        CompiledMaterializerStore.Add<{typeName}>(reader =>");
            sb.AppendLine("        {");
            sb.AppendLine($"            var entity = new {typeName}();");
            var colIndex = 0;
            foreach (var prop in props)
            {
                if (IsOwnedType(prop.Type))
                {
                    var ownedType = (INamedTypeSymbol)prop.Type;
                    var ownedProps = ownedType.GetMembers().OfType<IPropertySymbol>()
                        .Where(p => !p.IsStatic && p.GetMethod != null && p.SetMethod != null)
                        .OrderBy(p => p.Name);
                    foreach (var op in ownedProps)
                    {
                        sb.AppendLine(BuildOwnedAssignmentExpression(prop, op, ownedType, colIndex++));
                    }
                }
                else
                {
                    sb.AppendLine(BuildAssignmentExpression(prop, colIndex++));
                }
            }
            sb.AppendLine("            return entity;");
            sb.AppendLine("        });");
            sb.AppendLine("    }");
            sb.AppendLine("}");

            context.AddSource($"{simpleName}_Materializer.g.cs", SourceText.From(sb.ToString(), Encoding.UTF8));
        }

        private static string BuildAssignmentExpression(IPropertySymbol prop, int index)
        {
            var read = GetReaderExpression(prop.Type, index);
            var needsNullCheck = prop.Type.IsReferenceType || prop.NullableAnnotation == NullableAnnotation.Annotated;
            return needsNullCheck
                ? $"            if (!reader.IsDBNull({index})) entity.{prop.Name} = {read};"
                : $"            entity.{prop.Name} = {read};";
        }

        private static bool IsOwnedType(ITypeSymbol type)
            => type.GetAttributes().Any(a => a.AttributeClass?.ToDisplayString() == "nORM.Mapping.OwnedAttribute");

        private static string BuildOwnedAssignmentExpression(IPropertySymbol owner, IPropertySymbol ownedProp, INamedTypeSymbol ownedType, int index)
        {
            var read = GetReaderExpression(ownedProp.Type, index);
            var needsNullCheck = ownedProp.Type.IsReferenceType || ownedProp.NullableAnnotation == NullableAnnotation.Annotated;
            var ownedTypeName = ownedType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var ensureOwner = $"if (entity.{owner.Name} == null) entity.{owner.Name} = new {ownedTypeName}(); ";
            return needsNullCheck
                ? $"            if (!reader.IsDBNull({index})) {{ {ensureOwner}entity.{owner.Name}.{ownedProp.Name} = {read}; }}"
                : $"            {ensureOwner}entity.{owner.Name}.{ownedProp.Name} = {read};";
        }

        private static string GetReaderExpression(ITypeSymbol type, int index)
        {
            if (type is INamedTypeSymbol named && named.IsGenericType && named.ConstructedFrom.SpecialType == SpecialType.System_Nullable_T)
            {
                type = named.TypeArguments[0];
            }

            if (type.TypeKind == TypeKind.Enum && type is INamedTypeSymbol enumType)
            {
                var underlying = enumType.EnumUnderlyingType!;
                var underlyingExpr = GetReaderExpression(underlying, index);
                var enumName = enumType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                return $"({enumName}){underlyingExpr}";
            }

            var typeName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            switch (type.SpecialType)
            {
                case SpecialType.System_Boolean: return $"reader.GetBoolean({index})";
                case SpecialType.System_Byte: return $"reader.GetByte({index})";
                case SpecialType.System_Int16: return $"reader.GetInt16({index})";
                case SpecialType.System_Int32: return $"reader.GetInt32({index})";
                case SpecialType.System_Int64: return $"reader.GetInt64({index})";
                case SpecialType.System_Single: return $"reader.GetFloat({index})";
                case SpecialType.System_Double: return $"reader.GetDouble({index})";
                case SpecialType.System_Decimal: return $"reader.GetDecimal({index})";
                case SpecialType.System_DateTime: return $"reader.GetDateTime({index})";
                case SpecialType.System_String: return $"reader.GetString({index})";
            }

            if (typeName == "global::System.Guid")
                return $"reader.GetGuid({index})";
            if (typeName == "global::System.Byte[]")
                return $"reader.GetFieldValue<byte[]>({index})";

            return $"reader.GetFieldValue<{typeName}>({index})";
        }

        private void GenerateQuery(IMethodSymbol method, string sql, GeneratorExecutionContext context)
        {
            var cls = method.ContainingType;
            var ns = cls.ContainingNamespace.IsGlobalNamespace ? null : cls.ContainingNamespace.ToDisplayString();
            var className = cls.Name;
            var methodName = method.Name;

            if (!TryGetEntityType(method.ReturnType, out var entity))
                return;

            var entityTypeName = entity!.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

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
            sb.AppendLine("using nORM.SourceGeneration;");
            if (ns != null) sb.AppendLine($"namespace {ns};");
            sb.AppendLine($"public static partial class {className}");
            sb.AppendLine("{");
            sb.AppendLine($"    public static async partial System.Threading.Tasks.Task<System.Collections.Generic.List<{entityTypeName}>> {methodName}({paramList})");
            sb.AppendLine("    {");
            sb.AppendLine($"        await using var cmd = {ctxParam}.Connection.CreateCommand();");
            var escapedSql = sql.Replace("\"", "\"\"");
            sb.AppendLine($"        cmd.CommandText = @\"{escapedSql}\";");
            foreach (var p in queryParams)
            {
                sb.AppendLine($"        var p_{p.Name} = cmd.CreateParameter();");
                // DIALECT FIX (TASK 8): Use provider's parameter prefix instead of hard-coding "@"
                // Different databases use different prefixes: SQL Server (@), PostgreSQL ($), Oracle (:)
                sb.AppendLine($"        p_{p.Name}.ParameterName = $\"{{{ctxParam}.Provider.ParamPrefix}}{p.Name}\";");
                sb.AppendLine($"        p_{p.Name}.Value = {GetParameterValueExpression(p)};");
                sb.AppendLine($"        cmd.Parameters.Add(p_{p.Name});");
            }
            sb.AppendLine($"        var materializer = CompiledMaterializerStore.Get<{entityTypeName}>();");
            sb.AppendLine($"        var list = new System.Collections.Generic.List<{entityTypeName}>();");
            var ctArg = ctParam ?? "System.Threading.CancellationToken.None";
            sb.AppendLine($"        await using var reader = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess, {ctArg}).ConfigureAwait(false);");
            sb.AppendLine($"        while (await reader.ReadAsync({ctArg}).ConfigureAwait(false))");
            sb.AppendLine("        {");
            sb.AppendLine($"            list.Add(await materializer(reader, {ctArg}).ConfigureAwait(false));");
            sb.AppendLine("        }");
            sb.AppendLine("        return list;");
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

