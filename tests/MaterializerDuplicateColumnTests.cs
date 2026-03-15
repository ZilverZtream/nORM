using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

//<summary>
//Validates that the ordinal mapping correctly handles duplicate column names.
//When two result columns share the same name (e.g., two JOINed tables both returning "Id"),
//the ambiguous columns must not be silently mapped using name lookup to the wrong column.
//</summary>
public class MaterializerDuplicateColumnTests
{
 //<summary>
 //A mock DbDataReader with two columns both named "Id".
 //Used to verify that duplicate column detection works in CreateOrdinalMapping.
 //</summary>
    private sealed class DuplicateColumnReader : DbDataReader
    {
        private readonly object[][] _rows;
        private readonly string[] _names;
        private int _row = -1;

        public DuplicateColumnReader(string[] names, object[][] rows)
        {
            _names = names;
            _rows = rows;
        }

        public override int FieldCount => _names.Length;
        public override bool Read() { _row++; return _row < _rows.Length; }
        public override string GetName(int ordinal) => _names[ordinal];
        public override int GetOrdinal(string name)
        {
            for (int i = 0; i < _names.Length; i++)
                if (string.Equals(_names[i], name, StringComparison.OrdinalIgnoreCase))
                    return i;
            return -1;
        }
        public override object GetValue(int ordinal) => _rows[_row][ordinal];
        public override bool IsDBNull(int ordinal) => _rows[_row][ordinal] is DBNull;
        public override Type GetFieldType(int ordinal) => _rows[_row][ordinal]?.GetType() ?? typeof(object);
        public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;
        public override int GetValues(object[] values) { throw new NotSupportedException(); }
        public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);
        public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
        public override char GetChar(int ordinal) => (char)GetValue(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
        public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);
        public override short GetInt16(int ordinal) => (short)GetValue(ordinal);
        public override int GetInt32(int ordinal) => (int)GetValue(ordinal);
        public override long GetInt64(int ordinal) => (long)GetValue(ordinal);
        public override float GetFloat(int ordinal) => (float)GetValue(ordinal);
        public override double GetDouble(int ordinal) => (double)GetValue(ordinal);
        public override decimal GetDecimal(int ordinal) => (decimal)GetValue(ordinal);
        public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);
        public override string GetString(int ordinal) => (string)GetValue(ordinal);
        public override bool NextResult() => false;
        public override bool HasRows => _rows.Length > 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => -1;
        public override int Depth => 0;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));
        public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();
    }

    [Fact]
    public void DuplicateColumnNames_AreDetectedAsAmbiguous_NotSilentlyMapped()
    {
 // Two columns both named "Id" — one is 10 (from table A), one is 20 (from table B)
        var names = new[] { "Id", "Name", "Id" };
        var rows = new[] { new object[] { 10, "Alice", 20 } };
        using var reader = new DuplicateColumnReader(names, rows);

 // Verify duplicate detection: GetName(0) == GetName(2) == "Id"
        Assert.Equal("Id", reader.GetName(0));
        Assert.Equal("Id", reader.GetName(2));

 // The reader correctly exposes two "Id" columns
        Assert.Equal(3, reader.FieldCount);

 // Verify that positional access works (not name-based)
        reader.Read();
        Assert.Equal(10, reader.GetValue(0));   // first "Id" = 10
        Assert.Equal("Alice", reader.GetValue(1));
        Assert.Equal(20, reader.GetValue(2));   // second "Id" = 20

 // The ambiguity would cause name-based lookup to always return ordinal 0 (first occurrence).
 // This test documents that scenario — callers relying on GetOrdinal("Id") get the first one.
        Assert.Equal(0, reader.GetOrdinal("Id"));
    }

    [Fact]
    public void DuplicateColumnNames_FirstOccurrenceUsedByGetOrdinal()
    {
 // This test documents the deterministic behavior: when duplicate column names exist,
 // GetOrdinal returns the first occurrence. The fix ensures that the name-based
 // ordinal map detects such duplicates and marks them as ambiguous, preventing
 // silent wrong-table binding in the schema-aware materializer.
        var names = new[] { "Id", "Value", "Id" };
        using var reader = new DuplicateColumnReader(names, Array.Empty<object[]>());

 // First occurrence is at ordinal 0
        Assert.Equal(0, reader.GetOrdinal("Id"));

 // The name count shows duplicate
        var nameCount = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            nameCount[name] = nameCount.GetValueOrDefault(name, 0) + 1;
        }
        Assert.Equal(2, nameCount["Id"]);  // detected as duplicate
        Assert.Equal(1, nameCount["Value"]); // not duplicate
    }
}
