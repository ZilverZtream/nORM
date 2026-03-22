using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Collections.Generic;
using System.Linq;
using System.Text;


/// <summary>
/// Roslyn source generator that produces materializer implementations for annotated entity types.
/// </summary>
[Generator]
public class MaterializerSourceGenerator : ISourceGenerator
{
    /// <summary>
    /// Registers the syntax receiver used to discover candidate classes.
    /// </summary>
    /// <param name="context">Initialization context provided by the compiler.</param>
    public void Initialize(GeneratorInitializationContext context)
    {
        context.RegisterForSyntaxNotifications(() => new MaterializerSyntaxReceiver());
    }

    /// <summary>
    /// Generates materializer code for classes marked with <c>GenerateMaterializerAttribute</c>.
    /// </summary>
    /// <param name="context">Execution context provided by the compiler.</param>
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
            // SG2 fix: qualify hint name with namespace to avoid collisions for same-named classes
            var nsPrefix = classSymbol.ContainingNamespace.IsGlobalNamespace
                ? ""
                : classSymbol.ContainingNamespace.ToDisplayString().Replace(".", "_") + "_";
            var fileName = $"{nsPrefix}{classSymbol.Name}Materializer.g.cs";
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

        // SG1 fix: resolve column ordinals using mapped column name ([Column] attr if present,
        // else property name), so [Column("db_name")] renames are honoured.
        for (int i = 0; i < properties.Count; i++)
            sb.AppendLine($"            int __ord_{properties[i].Name} = reader.GetOrdinal(\"{GetMappedColumnName(properties[i])}\");");

        for (int i = 0; i < properties.Count; i++)
        {
            var prop = properties[i];
            var propType = prop.Type;
            var isNullable = propType.CanBeReferencedByName && propType.Name.StartsWith("Nullable");
            var underlyingType = isNullable ? ((INamedTypeSymbol)propType).TypeArguments[0] : propType;
            var ordVar = $"__ord_{prop.Name}";

            sb.AppendLine($"            if (!reader.IsDBNull({ordVar}))");
            sb.AppendLine("            {");

            var readerMethod = GetReaderMethod(underlyingType);
            var underlyingName = underlyingType.ToDisplayString();
            if (readerMethod != null)
            {
                sb.AppendLine($"                entity.{prop.Name} = reader.{readerMethod}({ordVar});");
            }
            else if (underlyingName == "System.DateOnly" || underlyingName == "global::System.DateOnly")
            {
                // SG1 fix: DateOnly requires explicit conversion — providers return DateTime/string
                sb.AppendLine($"                entity.{prop.Name} = nORM.Query.MaterializerFactory.ConvertToDateOnly(reader.GetValue({ordVar}));");
            }
            else if (underlyingName == "System.TimeOnly" || underlyingName == "global::System.TimeOnly")
            {
                // SG1 fix: TimeOnly requires explicit conversion — providers return TimeSpan/string
                sb.AppendLine($"                entity.{prop.Name} = nORM.Query.MaterializerFactory.ConvertToTimeOnly(reader.GetValue({ordVar}));");
            }
            else
            {
                sb.AppendLine($"                var value = reader.GetValue({ordVar});");
                sb.AppendLine($"                entity.{prop.Name} = ({propType.ToDisplayString()})Convert.ChangeType(value, typeof({underlyingName}));");
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
                // SG1 fix: pass compile-time-resolved table name so multi-model scenarios
                // where the same CLR type maps to different tables each get their own materializer.
                var tableName = GetTableNameForType(classSymbol);
                var escapedTableName = tableName.Replace("\\", "\\\\").Replace("\"", "\\\"");
                // SG1 namespace fix: fully qualify the materializer class name so the registration
                // compiles correctly for entities in non-global namespaces. The generated
                // {ClassName}Materializer class lives in the same namespace as the entity, but
                // MaterializerRegistration.g.cs is in nORM.Generated and has no using for the
                // entity's namespace — an unqualified {ClassName}Materializer reference fails.
                var materializerFullName = classSymbol.ContainingNamespace.IsGlobalNamespace
                    ? $"{className}Materializer"
                    : $"{classSymbol.ContainingNamespace.ToDisplayString()}.{className}Materializer";
                sb.AppendLine($"            CompiledMaterializerStore.Add<{fullName}>(\"{escapedTableName}\", {materializerFullName}.Materialize);");
            }
        }

        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");

        return sb.ToString();
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
    /// SG1: Returns the database column name for a property. Reads the <c>[Column("name")]</c>
    /// attribute when present; falls back to the property name.
    /// </summary>
    private static string GetMappedColumnName(IPropertySymbol prop)
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

/// <summary>
/// Syntax receiver that collects classes decorated with attributes.
/// </summary>
public class MaterializerSyntaxReceiver : ISyntaxReceiver
{
    /// <summary>
    /// Gets the list of candidate classes discovered during syntax traversal.
    /// </summary>
    public List<ClassDeclarationSyntax> CandidateClasses { get; } = new();

    /// <summary>
    /// Called for every syntax node in the compilation to identify candidates.
    /// </summary>
    /// <param name="syntaxNode">The node being visited.</param>
    public void OnVisitSyntaxNode(SyntaxNode syntaxNode)
    {
        if (syntaxNode is ClassDeclarationSyntax classDeclaration &&
            classDeclaration.AttributeLists.Count > 0)
        {
            CandidateClasses.Add(classDeclaration);
        }
    }
}
