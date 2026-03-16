using System;
using System.Data;
using Microsoft.Data.Sqlite;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// P1 — Parameter metadata contamination (Gate 3.8→4.0)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that AssignValue resets ALL provider-visible metadata (Size, Precision, Scale, DbType)
/// at the top of the non-null dispatch path, so that no stale metadata from a previous type
/// assignment can contaminate subsequent reuses of the same DbParameter object.
///
/// P1 root cause: Size was only reset in the null branch. A string→binary transition
/// would leave the previous string's Size on the parameter, potentially truncating
/// or mis-binding the binary value.
///
/// Size reset value is -1 (not 0) because Microsoft.Data.Sqlite uses Size to truncate
/// text-bound values (DateTime, DateOnly, Guid, decimal, binary): Size=0 produces an empty
/// string/blob; Size=-1 means "no limit" and binds the full value.
/// </summary>
public class ParameterMetadataContaminationTests
{
    // ── string → binary ───────────────────────────────────────────────────────

    [Fact]
    public void AssignValue_StringThenBinary_SizeIsReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, "hello world"); // Size = 11
        Assert.Equal(DbType.String, p.DbType);
        Assert.Equal(11, p.Size);

        ParameterAssign.AssignValue(p, new byte[] { 0xDE, 0xAD, 0xBE });
        Assert.Equal(DbType.Binary, p.DbType);
        Assert.Equal(-1, p.Size); // must NOT be 11 (stale string size); -1 = no limit
    }

    [Fact]
    public void AssignValue_LongStringThenBinary_SizeIsReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, new string('x', 8000)); // realistic NVARCHAR(MAX) scenario
        Assert.Equal(8000, p.Size);

        ParameterAssign.AssignValue(p, new byte[] { 1, 2, 3, 4, 5 });
        Assert.Equal(DbType.Binary, p.DbType);
        Assert.Equal(-1, p.Size); // stale 8000 removed; -1 = no limit
    }

    // ── string → int ──────────────────────────────────────────────────────────

    [Fact]
    public void AssignValue_StringThenInt_SizeIsReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, "abc");
        Assert.Equal(3, p.Size);

        ParameterAssign.AssignValue(p, 42);
        Assert.Equal(DbType.Int32, p.DbType);
        Assert.Equal(42, p.Value);
        Assert.Equal(-1, p.Size); // stale 3 removed
    }

    [Fact]
    public void AssignValue_StringThenLong_SizeIsReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, "hello");
        Assert.Equal(5, p.Size);

        ParameterAssign.AssignValue(p, 9_999_999_999L);
        Assert.Equal(DbType.Int64, p.DbType);
        Assert.Equal(-1, p.Size); // stale 5 removed
    }

    [Fact]
    public void AssignValue_StringThenBool_SizeIsReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, "truthy");
        Assert.Equal(6, p.Size);

        ParameterAssign.AssignValue(p, true);
        Assert.Equal(DbType.Boolean, p.DbType);
        Assert.Equal(-1, p.Size); // stale 6 removed
    }

    [Fact]
    public void AssignValue_StringThenDouble_SizeIsReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, "3.14");
        Assert.Equal(4, p.Size);

        ParameterAssign.AssignValue(p, 3.14159265);
        Assert.Equal(DbType.Double, p.DbType);
        Assert.Equal(-1, p.Size); // stale 4 removed
    }

    [Fact]
    public void AssignValue_StringThenDateTime_SizeIsReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, "2024-01-15");
        Assert.Equal(10, p.Size);

        ParameterAssign.AssignValue(p, new DateTime(2024, 6, 15));
        Assert.Equal(DbType.DateTime2, p.DbType);
        // Size=-1 (not 0!) — Size=0 would cause SQLite to bind DateTime as empty string.
        Assert.Equal(-1, p.Size);
    }

    // ── decimal → guid: stale Precision/Scale ─────────────────────────────────

    [Fact]
    public void AssignValue_DecimalThenGuid_PrecisionAndScaleReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, 3.14159265358979m);
        // Manually set non-zero precision/scale to simulate a driver that previously set them.
        p.Precision = 18;
        p.Scale = 8;

        ParameterAssign.AssignValue(p, Guid.NewGuid());
        Assert.Equal(DbType.Guid, p.DbType);
        Assert.Equal(0, p.Precision); // must NOT be 18 (stale decimal precision)
        Assert.Equal(0, p.Scale);     // must NOT be 8 (stale decimal scale)
    }

    [Fact]
    public void AssignValue_DecimalThenInt_PrecisionScaleAndSizeAllReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, 99.99m);
        p.Precision = 5;
        p.Scale = 2;

        ParameterAssign.AssignValue(p, 7);
        Assert.Equal(DbType.Int32, p.DbType);
        Assert.Equal(7, p.Value);
        Assert.Equal(0, p.Precision);
        Assert.Equal(0, p.Scale);
        Assert.Equal(-1, p.Size); // stale size removed
    }

    // ── mixed-type reuse cycle matrix ─────────────────────────────────────────

    [Fact]
    public void AssignValue_StringBinaryIntDecimalGuidCycle_MetadataAlwaysCorrect()
    {
        var p = new SqliteParameter();
        var guid = Guid.NewGuid();

        for (int i = 0; i < 10; i++)
        {
            // 1. String — sets Size
            ParameterAssign.AssignValue(p, new string('s', 50 + i));
            Assert.Equal(DbType.String, p.DbType);
            Assert.Equal(50 + i, p.Size);

            // 2. Binary — Size must be reset (stale string Size removed); -1 = no limit
            ParameterAssign.AssignValue(p, new byte[] { (byte)i, 0xFF });
            Assert.Equal(DbType.Binary, p.DbType);
            Assert.Equal(-1, p.Size);

            // 3. Int — Size stays -1
            ParameterAssign.AssignValue(p, i);
            Assert.Equal(DbType.Int32, p.DbType);
            Assert.Equal(-1, p.Size);

            // 4. Decimal with explicit precision/scale via manual set (simulating driver state)
            ParameterAssign.AssignValue(p, 1.23m);
            p.Precision = 20;
            p.Scale = 10;

            // 5. Guid — Precision and Scale must be reset to 0
            ParameterAssign.AssignValue(p, guid);
            Assert.Equal(DbType.Guid, p.DbType);
            Assert.Equal(0, p.Precision);
            Assert.Equal(0, p.Scale);
            Assert.Equal(-1, p.Size);
        }
    }

    [Fact]
    public void AssignValue_StringThenAllNumericTypes_SizeAlwaysReset()
    {
        var p = new SqliteParameter();
        object[] numericValues = { (short)1, (byte)2, 3.0f, 4u, 5UL, (sbyte)6, (ushort)7 };
        var expectedTypes = new[]
        {
            DbType.Int16, DbType.Byte, DbType.Double, DbType.UInt32,
            DbType.UInt64, DbType.SByte, DbType.UInt16
        };

        for (int i = 0; i < numericValues.Length; i++)
        {
            ParameterAssign.AssignValue(p, "a string value"); // set Size = 14
            Assert.Equal(14, p.Size);

            ParameterAssign.AssignValue(p, numericValues[i]);
            Assert.Equal(expectedTypes[i], p.DbType);
            Assert.Equal(-1, p.Size); // stale string Size removed; -1 = no limit
        }
    }

    // ── null clears all metadata ───────────────────────────────────────────────

    [Fact]
    public void AssignValue_StringThenNull_AllMetadataReset()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, "test string");
        Assert.Equal(DbType.String, p.DbType);
        Assert.Equal(11, p.Size);

        ParameterAssign.AssignValue(p, null);
        Assert.Equal(DBNull.Value, p.Value);
        Assert.Equal(DbType.Object, p.DbType);
        Assert.Equal(0, p.Size);   // null path resets to 0 (null binds as NULL regardless of Size)
        Assert.Equal(0, p.Precision);
        Assert.Equal(0, p.Scale);
    }

    // ── string Size is correctly set (not zeroed out) ─────────────────────────

    [Fact]
    public void AssignValue_StringAfterBinary_SizeSetCorrectly()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, new byte[] { 1, 2, 3 });
        Assert.Equal(-1, p.Size); // binary: -1 = no limit

        ParameterAssign.AssignValue(p, "hello");
        Assert.Equal(DbType.String, p.DbType);
        Assert.Equal(5, p.Size); // string Size must be str.Length, not -1
    }

    [Fact]
    public void AssignValue_LongerStringAfterShorterString_SizeUpdated()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, "hi");
        Assert.Equal(2, p.Size);

        ParameterAssign.AssignValue(p, "hello world");
        Assert.Equal(DbType.String, p.DbType);
        Assert.Equal(11, p.Size); // must grow, not stay at 2
    }

    [Fact]
    public void AssignValue_ShorterStringAfterLongerString_SizeUpdated()
    {
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, "hello world");
        Assert.Equal(11, p.Size);

        ParameterAssign.AssignValue(p, "hi");
        Assert.Equal(DbType.String, p.DbType);
        Assert.Equal(2, p.Size); // must shrink, not stay at 11
    }
}
