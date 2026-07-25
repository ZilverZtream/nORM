using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Unit tests for MaterializerFactory.IsGroupElementCollection — the slice-1 shape predicate for GroupBy
/// element materialization (see docs/v1-sharpening/group-element-materialization-plan.md). Slice 1 matches
/// g.Select(x => scalar).ToList() and g.ToList(); ordered/filtered/paged forms and non-collection group
/// methods must NOT match (they stay fail-loud until slice 2).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GroupElementCollectionPredicateTests
{
    private sealed class Order { public int CustomerId { get; set; } public int Amount { get; set; } }

    [Fact]
    public void Matches_group_select_scalar_toList_and_returns_projection()
    {
        Expression<Func<IGrouping<int, Order>, List<int>>> e = g => g.Select(x => x.Amount).ToList();
        Assert.True(MaterializerFactory.IsGroupElementCollection(e.Body, out var proj));
        Assert.NotNull(proj);
    }

    [Fact]
    public void Matches_group_toList_whole_element_with_null_projection()
    {
        Expression<Func<IGrouping<int, Order>, List<Order>>> e = g => g.ToList();
        Assert.True(MaterializerFactory.IsGroupElementCollection(e.Body, out var proj));
        Assert.Null(proj);
    }

    [Fact]
    public void Matches_group_select_toArray()
    {
        Expression<Func<IGrouping<int, Order>, int[]>> e = g => g.Select(x => x.Amount).ToArray();
        Assert.True(MaterializerFactory.IsGroupElementCollection(e.Body, out _));
    }

    [Fact]
    public void Does_not_match_ordered_form_reserved_for_slice2()
    {
        Expression<Func<IGrouping<int, Order>, List<int>>> e = g => g.OrderBy(x => x.Amount).Select(x => x.Amount).ToList();
        Assert.False(MaterializerFactory.IsGroupElementCollection(e.Body, out _));
    }

    [Fact]
    public void Does_not_match_filtered_form_reserved_for_slice2()
    {
        Expression<Func<IGrouping<int, Order>, List<int>>> e = g => g.Where(x => x.Amount > 0).Select(x => x.Amount).ToList();
        Assert.False(MaterializerFactory.IsGroupElementCollection(e.Body, out _));
    }

    [Fact]
    public void Does_not_match_scalar_aggregate()
    {
        Expression<Func<IGrouping<int, Order>, int>> e = g => g.Count();
        Assert.False(MaterializerFactory.IsGroupElementCollection(e.Body, out _));
    }
}
