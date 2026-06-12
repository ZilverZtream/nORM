#nullable enable

using System.Collections.Generic;
using System;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void ToPascalCase_UnderscoreSeparated_ReturnsPascalCase()
    {
        var result = InvokeToPascalCase("user_name");
        Assert.Equal("UserName", result);
    }

    [Fact]
    public void ToPascalCase_AllUpperWithUnderscore_ReturnsFirstLetterUpper()
    {
        var result = InvokeToPascalCase("CUSTOMER_ID");
        // Each segment: C+ustomer = "Customer", I+d = "Id"
        Assert.Equal("CustomerId", result);
    }
    [Fact]
    public void ToPascalCase_SpaceSeparated_ReturnsPascalCase()
    {
        var result = InvokeToPascalCase("first name");
        Assert.Equal("FirstName", result);
    }

    [Fact]
    public void ToPascalCase_SingleLower_CapitalizesFirst()
    {
        var result = InvokeToPascalCase("id");
        Assert.Equal("Id", result);
    }

    [Fact]
    public void ToPascalCase_AlreadyPascal_RoundsTrip()
    {
        var result = InvokeToPascalCase("User");
        Assert.Equal("User", result);
    }

    [Fact]
    public void ToPascalCase_WhitespaceOnly_ReturnsInput()
    {
        var result = InvokeToPascalCase("   ");
        Assert.Equal("   ", result);
    }

    [Fact]
    public void ToPascalCase_MultipleUnderscores_RemovesAll()
    {
        var result = InvokeToPascalCase("order_line_item");
        Assert.Equal("OrderLineItem", result);
    }

    [Fact]
    public void ToPascalCase_SingleChar_CapitalizesIt()
    {
        var result = InvokeToPascalCase("x");
        Assert.Equal("X", result);
    }

    [Fact]
    public void ToPascalCase_CamelCase_PreservesInnerWordCapital()
    {
        var result = InvokeToPascalCase("blogPost");
        Assert.Equal("BlogPost", result);
    }

    [Fact]
    public void ToPascalCase_InvalidSeparators_BecomeWordBoundaries()
    {
        var result = InvokeToPascalCase("bad-table.name");
        Assert.Equal("BadTableName", result);
    }


    [Fact]
    public void EscapeCSharpIdentifier_Keyword_class_PrependsAt()
    {
        Assert.Equal("@class", InvokeEscapeCSharpIdentifier("class"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_string_PrependsAt()
    {
        Assert.Equal("@string", InvokeEscapeCSharpIdentifier("string"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_int_PrependsAt()
    {
        Assert.Equal("@int", InvokeEscapeCSharpIdentifier("int"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_bool_PrependsAt()
    {
        Assert.Equal("@bool", InvokeEscapeCSharpIdentifier("bool"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_return_PrependsAt()
    {
        Assert.Equal("@return", InvokeEscapeCSharpIdentifier("return"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_ValidIdentifier_ReturnsUnchanged()
    {
        Assert.Equal("MyField", InvokeEscapeCSharpIdentifier("MyField"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_StartsWithDigit_PrefixesUnderscore()
    {
        Assert.Equal("_123abc", InvokeEscapeCSharpIdentifier("123abc"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_ContainsHyphen_ReplacesWithUnderscore()
    {
        // Hyphens are invalid in C# identifiers
        var result = InvokeEscapeCSharpIdentifier("my-field");
        Assert.Equal("my_field", result);
    }

    [Fact]
    public void EscapeCSharpIdentifier_EmptyString_ReturnsFallbackIdentifier()
    {
        Assert.Equal("_", InvokeEscapeCSharpIdentifier(""));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Underscore_IsValid()
    {
        // Underscore alone is a valid C# identifier
        Assert.Equal("_", InvokeEscapeCSharpIdentifier("_"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_Keyword_var_PrependsAt()
    {
        Assert.Equal("@var", InvokeEscapeCSharpIdentifier("var"));
    }

    [Fact]
    public void EscapeCSharpIdentifier_AlreadyEscapedIdentifier_ReturnsUnchanged()
    {
        Assert.Equal("@class", InvokeEscapeCSharpIdentifier("@class"));
        Assert.Equal("@record", InvokeEscapeCSharpIdentifier("@record"));
        Assert.Equal("@class", InvokeDynamicEscapeCSharpIdentifier("@class"));
        Assert.Equal("@record", InvokeDynamicEscapeCSharpIdentifier("@record"));
    }

    [Fact]
    public void BuildColumnPropertyNameMap_Reserves_Enclosing_Entity_Name()
    {
        var method = GetMethod(
            "BuildColumnPropertyNameMap",
            new[]
            {
                typeof(IReadOnlyDictionary<string, IReadOnlyList<string>>),
                typeof(IReadOnlyDictionary<string, string>),
                typeof(bool)
            });
        var orderedColumns = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["User"] = new[] { "User", "Id" }
        };
        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["User"] = "User"
        };

        var result = (IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>)method.Invoke(
            null,
            new object[] { orderedColumns, entityByTable, false })!;

        Assert.Equal("User2", result["User"]["User"]);
        Assert.Equal("Id", result["User"]["Id"]);
    }

    [Fact]
    public void EscapeCSharpIdentifier_InvalidVerbatimIdentifier_IsSanitized()
    {
        Assert.Equal("_bad_name", InvokeEscapeCSharpIdentifier("@bad-name"));
        Assert.Equal("_bad_name", InvokeDynamicEscapeCSharpIdentifier("@bad-name"));
    }

    [Fact]
    public void GetUnqualifiedName_SchemaQualified_ReturnsTablePart()
    {
        Assert.Equal("table", InvokeGetUnqualifiedName("schema.table"));
    }

    [Fact]
    public void GetUnqualifiedName_NoSchema_ReturnsWhole()
    {
        Assert.Equal("mytable", InvokeGetUnqualifiedName("mytable"));
    }

    [Fact]
    public void GetUnqualifiedName_TwoLevelSchema_ReturnsLast()
    {
        Assert.Equal("leaf", InvokeGetUnqualifiedName("db.schema.leaf"));
    }


    [Fact]
    public void GetSchemaNameOrNull_SchemaQualified_ReturnsSchema()
    {
        Assert.Equal("schema", InvokeGetSchemaNameOrNull("schema.table"));
    }

    [Fact]
    public void GetSchemaNameOrNull_NoSchema_ReturnsNull()
    {
        Assert.Null(InvokeGetSchemaNameOrNull("table"));
    }

    [Fact]
    public void GetSchemaNameOrNull_LeadingDot_ReturnsNull()
    {
        Assert.Null(InvokeGetSchemaNameOrNull(".table"));
    }
}
