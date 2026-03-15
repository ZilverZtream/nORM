using System;
using System.Collections.Generic;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

//<summary>
//Verifies that CountParameterMarkers uses a mini SQL lexer so that
//'@' inside string literals and comments is NOT counted as a parameter marker,
//preventing false negatives in the injection detection check.
//
//Before the fix: 'user@example.com' caused paramMarkerCount > 0, bypassing
//the Pattern 4 injection check entirely.
//After the fix: '@' inside literals/comments is not counted; real @param markers are.
//</summary>
public class SqlInjectionMarkerInLiteralTests
{
 // ─── '@' inside email literal must NOT suppress injection check ─────

    [Fact]
    public void EmailInLiteral_NoRealParams_InjectionCheckFires()
    {
 // core case: '@' is inside 'user@example.com' — a string literal.
 // Before fix: paramMarkerCount was 1 (false positive), bypassing Pattern 4.
 // After fix: paramMarkerCount is 0; ContainsSuspiciousLiteralPattern fires
 // (email-like: contains '@' and '.').
        var sql = "SELECT * FROM Users WHERE Email = 'user@example.com'";
        Assert.Throws<NormUsageException>(() =>
            NormValidator.ValidateRawSql(sql, parameters: null));
    }

 // ─── Real @param marker outside literals must still be counted ─────

    [Fact]
    public void RealParamMarker_IsCounted_NoInjectionException()
    {
 // A real parameter marker (@name) must be counted, so Pattern 4
 // (quotes without parameterization) does NOT fire.
        var sql = "SELECT * FROM Users WHERE Name = @name";
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql(sql, parameters: null));
        Assert.Null(ex);
    }

 // ─── Literal '@' + real @param both present — marker counted ───────

    [Fact]
    public void LiteralAt_PlusRealParam_MarkerCounted_NoException()
    {
 // When a string literal contains '@' (email) AND a real @param marker exists,
 // only the real marker counts. Pattern 4 still passes (marker found).
        var sql = "SELECT * FROM Users WHERE Email = 'user@example.com' AND Id = @id";
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql(sql, parameters: null));
        Assert.Null(ex);
    }

 // ─── '@' inside line comment must NOT be counted ───────────────────

    [Fact]
    public void LineComment_MarkerNotCounted_InjectionCheckFires()
    {
 // '@admin' appears inside a line comment (-- ...). It must not be counted.
 // The WHERE clause has an email literal → suspicious → injection check fires.
        var sql = "SELECT * FROM Users -- @admin\nWHERE Email = 'user@example.com'";
        Assert.Throws<NormUsageException>(() =>
            NormValidator.ValidateRawSql(sql, parameters: null));
    }

 // ─── '@' inside block comment must NOT be counted ──────────────────

    [Fact]
    public void BlockComment_MarkerNotCounted_InjectionCheckFires()
    {
 // '@name' appears inside a block comment (/* ... */). It must not be counted.
 // The WHERE clause has an email literal → suspicious → injection check fires.
        var sql = "SELECT /* @name */ * FROM Users WHERE Email = 'user@example.com'";
        Assert.Throws<NormUsageException>(() =>
            NormValidator.ValidateRawSql(sql, parameters: null));
    }
}
