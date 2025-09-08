using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Collections.Generic;
using System.Linq;
using System.Text;

[Generator]
public class MaterializerSourceGenerator : ISourceGenerator
{
    public void Initialize(GeneratorInitializationContext context)
    {
        context.RegisterForSyntaxNotifications(() => new MaterializerSyntaxReceiver());
    }

    public void Execute(GeneratorExecutionContext context)
    {
        if (context.SyntaxReceiver is not MaterializerSyntaxReceiver receiver)
            return;

        var compilation = context.Compilation;
        var materializerAttribute = compilation.GetTypeByMetadataName("nORM.SourceGeneration.GenerateMaterializerAttribute");
        
        if (materializerAttribute == null)
            return;

        // Generate materializers for each marked type
        foreach (var classDeclaration in receiver.CandidateClasses)
        {
            var semanticModel = compilation.GetSemanticModel(classDeclaration.SyntaxTree);
            var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration) as INamedTypeSymbol;
            
            if (classSymbol == null)
                continue;

            // Check if class has [GenerateMaterializer] attribute
            if (!classSymbol.GetAttributes().Any(attr => 
                SymbolEqualityComparer.Default.Equals(attr.AttributeClass, materializerAttribute)))
                continue;

            var source = GenerateMaterializerCode(classSymbol);
            var fileName = $"{classSymbol.Name}Materializer.g.cs";
            context.AddSource(fileName, source);
        }

        // Generate the registration code
        var registrationSource = GenerateRegistrationCode(receiver.CandidateClasses, compilation, materializerAttribute);
        context.AddSource("MaterializerRegistration.g.cs", registrationSource);
    }

    private string GenerateMaterializerCode(INamedTypeSymbol classSymbol)
    {
        var namespaceName = classSymbol.ContainingNamespace.ToDisplayString();
        var className = classSymbol.Name;
        var properties = GetWritableProperties(classSymbol);

        var sb = new StringBuilder();
        sb.AppendLine("using System;");
        sb.AppendLine("using System.Data.Common;");
        sb.AppendLine("using nORM.SourceGeneration;");
        sb.AppendLine();
        sb.AppendLine($"namespace {namespaceName}");
        sb.AppendLine("{");
        sb.AppendLine($"    public static class {className}Materializer");
        sb.AppendLine("    {");
        sb.AppendLine($"        public static {className} Materialize(DbDataReader reader)");
        sb.AppendLine("        {");
        sb.AppendLine($"            var entity = new {className}();");
        
        for (int i = 0; i < properties.Count; i++)
        {
            var prop = properties[i];
            var propType = prop.Type;
            var isNullable = propType.CanBeReferencedByName && propType.Name.StartsWith("Nullable");
            var underlyingType = isNullable ? ((INamedTypeSymbol)propType).TypeArguments[0] : propType;
            
            sb.AppendLine($"            if (!reader.IsDBNull({i}))");
            sb.AppendLine("            {");
            
            var readerMethod = GetReaderMethod(underlyingType);
            if (readerMethod != null)
            {
                sb.AppendLine($"                entity.{prop.Name} = reader.{readerMethod}({i});");
            }
            else
            {
                sb.AppendLine($"                var value = reader.GetValue({i});");
                sb.AppendLine($"                entity.{prop.Name} = ({propType.ToDisplayString()})Convert.ChangeType(value, typeof({underlyingType.ToDisplayString()}));");
            }
            
            sb.AppendLine("            }");
        }
        
        sb.AppendLine("            return entity;");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");

        return sb.ToString();
    }

    private string GenerateRegistrationCode(List<ClassDeclarationSyntax> classes, Compilation compilation, INamedTypeSymbol materializerAttribute)
    {
        var sb = new StringBuilder();
        sb.AppendLine("using nORM.SourceGeneration;");
        sb.AppendLine("using System.Runtime.CompilerServices;");
        sb.AppendLine();
        sb.AppendLine("namespace nORM.Generated");
        sb.AppendLine("{");
        sb.AppendLine("    public static class MaterializerInitializer");
        sb.AppendLine("    {");
        sb.AppendLine("        [ModuleInitializer]");
        sb.AppendLine("        public static void Initialize()");
        sb.AppendLine("        {");

        foreach (var classDeclaration in classes)
        {
            var semanticModel = compilation.GetSemanticModel(classDeclaration.SyntaxTree);
            var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration) as INamedTypeSymbol;
            
            if (classSymbol?.GetAttributes().Any(attr => 
                SymbolEqualityComparer.Default.Equals(attr.AttributeClass, materializerAttribute)) == true)
            {
                var fullName = classSymbol.ToDisplayString();
                var className = classSymbol.Name;
                sb.AppendLine($"            CompiledMaterializerStore.Add<{fullName}>({className}Materializer.Materialize);");
            }
        }

        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");

        return sb.ToString();
    }

    private List<IPropertySymbol> GetWritableProperties(INamedTypeSymbol classSymbol)
    {
        return classSymbol.GetMembers()
            .OfType<IPropertySymbol>()
            .Where(p => p.SetMethod != null && p.SetMethod.DeclaredAccessibility == Accessibility.Public)
            .OrderBy(p => p.Name) // Consistent ordering
            .ToList();
    }

    private string? GetReaderMethod(ITypeSymbol type)
    {
        return type.SpecialType switch
        {
            SpecialType.System_Int32 => "GetInt32",
            SpecialType.System_Int64 => "GetInt64",
            SpecialType.System_String => "GetString",
            SpecialType.System_Boolean => "GetBoolean",
            SpecialType.System_DateTime => "GetDateTime",
            SpecialType.System_Decimal => "GetDecimal",
            SpecialType.System_Double => "GetDouble",
            SpecialType.System_Single => "GetFloat",
            SpecialType.System_Int16 => "GetInt16",
            SpecialType.System_Byte => "GetByte",
            _ => null
        };
    }
}

public class MaterializerSyntaxReceiver : ISyntaxReceiver
{
    public List<ClassDeclarationSyntax> CandidateClasses { get; } = new();

    public void OnVisitSyntaxNode(SyntaxNode syntaxNode)
    {
        if (syntaxNode is ClassDeclarationSyntax classDeclaration && 
            classDeclaration.AttributeLists.Count > 0)
        {
            CandidateClasses.Add(classDeclaration);
        }
    }
}
