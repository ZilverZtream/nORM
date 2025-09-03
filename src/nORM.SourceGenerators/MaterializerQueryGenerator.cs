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
                    if (attrData?.ConstructorArguments.Length == 1 && attrData.ConstructorArguments[0].Value is INamedTypeSymbol ent)
                    {
                        GenerateQuery(method.Symbol, ent, context);
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
            sb.AppendLine($"        CompiledMaterializerStore.Add(typeof({typeName}), reader =>");
            sb.AppendLine("        {");
            sb.AppendLine($"            var entity = new {typeName}();");
            for (int i = 0; i < props.Count; i++)
            {
                var prop = props[i];
                var propType = prop.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                var read = $"reader.GetFieldValue<{propType}>({i})";
                if (prop.Type.IsReferenceType || prop.NullableAnnotation == NullableAnnotation.Annotated)
                {
                    sb.AppendLine($"            if (!reader.IsDBNull({i})) entity.{prop.Name} = {read};");
                }
                else
                {
                    sb.AppendLine($"            entity.{prop.Name} = {read};");
                }
            }
            sb.AppendLine("            return entity;");
            sb.AppendLine("        });");
            sb.AppendLine("    }");
            sb.AppendLine("}");

            context.AddSource($"{simpleName}_Materializer.g.cs", SourceText.From(sb.ToString(), Encoding.UTF8));
        }

        private void GenerateQuery(IMethodSymbol method, INamedTypeSymbol entity, GeneratorExecutionContext context)
        {
            var cls = method.ContainingType;
            var ns = cls.ContainingNamespace.IsGlobalNamespace ? null : cls.ContainingNamespace.ToDisplayString();
            var className = cls.Name;
            var methodName = method.Name;
            var entityTypeName = entity.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

            var tableAttr = entity.GetAttributes().FirstOrDefault(a => a.AttributeClass?.ToDisplayString() == "System.ComponentModel.DataAnnotations.Schema.TableAttribute");
            var tableName = tableAttr != null && tableAttr.ConstructorArguments.Length > 0
                ? tableAttr.ConstructorArguments[0].Value?.ToString() ?? entity.Name
                : entity.Name;

            var props = entity.GetMembers().OfType<IPropertySymbol>()
                .Where(p => !p.IsStatic && p.GetMethod != null && p.SetMethod != null)
                .ToList();

            var cols = props.Select(p =>
            {
                var colAttr = p.GetAttributes().FirstOrDefault(a => a.AttributeClass?.ToDisplayString() == "System.ComponentModel.DataAnnotations.Schema.ColumnAttribute");
                return colAttr != null && colAttr.ConstructorArguments.Length > 0
                    ? colAttr.ConstructorArguments[0].Value?.ToString() ?? p.Name
                    : p.Name;
            }).ToList();

            var sql = $"SELECT {string.Join(", ", cols)} FROM {tableName}";

            var ctxParam = method.Parameters[0].Name;
            var ctParam = method.Parameters.Length > 1 ? method.Parameters[1].Name : "ct";

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
            sb.AppendLine($"    public static async partial System.Threading.Tasks.Task<System.Collections.Generic.List<{entityTypeName}>> {methodName}(nORM.Core.DbContext {ctxParam}, System.Threading.CancellationToken {ctParam})");
            sb.AppendLine("    {");
            sb.AppendLine($"        await using var cmd = {ctxParam}.Connection.CreateCommand();");
            sb.AppendLine($"        cmd.CommandText = \"{sql}\";");
            sb.AppendLine($"        var materializer = CompiledMaterializerStore.Get<{entityTypeName}>();");
            sb.AppendLine($"        var list = new System.Collections.Generic.List<{entityTypeName}>();");
            sb.AppendLine($"        await using var reader = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess, {ctParam});");
            sb.AppendLine($"        while (await reader.ReadAsync({ctParam}))");
            sb.AppendLine("        {");
            sb.AppendLine("            list.Add(materializer(reader));");
            sb.AppendLine("        }");
            sb.AppendLine("        return list;");
            sb.AppendLine("    }");
            sb.AppendLine("}");

            context.AddSource($"{className}_{methodName}_Query.g.cs", SourceText.From(sb.ToString(), Encoding.UTF8));
        }
    }
}

