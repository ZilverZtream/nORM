#nullable enable

using nORM.Configuration;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Theory]
    [InlineData(null, "NO ACTION", true, ReferentialAction.NoAction, "ReferentialAction.NoAction")]
    [InlineData("", "NO ACTION", true, ReferentialAction.NoAction, "ReferentialAction.NoAction")]
    [InlineData("set_null", "SET NULL", true, ReferentialAction.SetNull, "ReferentialAction.SetNull")]
    [InlineData("SET DEFAULT", "SET DEFAULT", true, ReferentialAction.SetDefault, "ReferentialAction.SetDefault")]
    [InlineData("NO ACTION DEFERRABLE", "NO ACTION DEFERRABLE", false, ReferentialAction.NoAction, "ReferentialAction.NoAction")]
    [InlineData("NO ACTION MATCH FULL", "NO ACTION MATCH FULL", false, ReferentialAction.NoAction, "ReferentialAction.NoAction")]
    public void ScaffoldReferentialAction_centralizes_fk_action_semantics(
        string? raw,
        string expectedNormalized,
        bool expectedParsed,
        ReferentialAction expectedAction,
        string expectedLiteral)
    {
        Assert.Equal(expectedNormalized, ScaffoldReferentialAction.Normalize(raw));
        Assert.Equal(expectedParsed, ScaffoldReferentialAction.IsScaffoldable(raw));
        Assert.Equal(expectedParsed, ScaffoldReferentialAction.TryParse(raw, out var action));
        Assert.Equal(expectedAction, action);
        Assert.Equal(expectedLiteral, ScaffoldReferentialAction.FormatModelBuilderLiteral(raw));
    }
}
