using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.SourceGeneration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SG1 fix: Source-generated [CompileTimeQuery] methods previously used
/// <c>CompiledMaterializerStore.Get&lt;T&gt;()</c> which derives the table name
/// from <c>[Table]</c> attribute or type name at runtime. This ignores the
/// compile-time-resolved table name, causing wrong materializer lookup in
/// multi-model scenarios where the same CLR type maps to different tables.
///
/// Fix: Both generators now emit <c>Add&lt;T&gt;("tableName", ...)</c> and
/// <c>Get&lt;T&gt;("tableName")</c> with the table name resolved at compile time.
/// New overloads <c>Add&lt;T&gt;(string, Func)</c> and <c>Get&lt;T&gt;(string)</c>
/// support explicit table name discriminators.
/// </summary>
public class SourceGenMultiModelMaterializerTests
{
    // ── Entity types ─────────────────────────────────────────────────────────

    // Same CLR type to register under different table names
    private class SharedEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("CustomTableA")]
    private class TableAEntity
    {
        public int Id { get; set; }
        public string Value { get; set; } = "";
    }

    [Table("CustomTableB")]
    private class TableBEntity
    {
        public int Id { get; set; }
        public string Value { get; set; } = "";
    }

    // Type without [Table] attribute -- uses CLR type name
    private class PlainEntity
    {
        public int Id { get; set; }
        public string Tag { get; set; } = "";
    }

    // ── Add<T>(tableName, mat) + Get<T>(tableName) ──────────────────────────

    [Fact]
    public void Add_WithExplicitTableName_Get_ReturnsCorrectMaterializer()
    {
        // Register the same CLR type under two different table names
        Func<DbDataReader, SharedEntity> matA = r => new SharedEntity { Id = 1, Name = "fromA" };
        Func<DbDataReader, SharedEntity> matB = r => new SharedEntity { Id = 2, Name = "fromB" };

        CompiledMaterializerStore.Add("SG1_TableA", matA);
        CompiledMaterializerStore.Add("SG1_TableB", matB);

        // Get with explicit table name returns the correct materializer
        var gotA = CompiledMaterializerStore.Get<SharedEntity>("SG1_TableA");
        var gotB = CompiledMaterializerStore.Get<SharedEntity>("SG1_TableB");

        Assert.NotNull(gotA);
        Assert.NotNull(gotB);
        // They must be different delegates (wrapping different underlying materializers)
        Assert.NotSame(gotA, gotB);
    }

    [Fact]
    public async Task Get_WithExplicitTableName_ExecutesCorrectMaterializer()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE SG1_Exec (Id INTEGER, Name TEXT); INSERT INTO SG1_Exec VALUES(42, 'hello')";
        setup.ExecuteNonQuery();

        Func<DbDataReader, SharedEntity> mat = r => new SharedEntity
        {
            Id = r.GetInt32(r.GetOrdinal("Id")),
            Name = r.GetString(r.GetOrdinal("Name"))
        };

        CompiledMaterializerStore.Add("SG1_Exec", mat);

        var materializer = CompiledMaterializerStore.Get<SharedEntity>("SG1_Exec");
        Assert.NotNull(materializer);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name FROM SG1_Exec";
        using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        var entity = await materializer(reader, CancellationToken.None);
        Assert.Equal(42, entity.Id);
        Assert.Equal("hello", entity.Name);
    }

    [Fact]
    public void Get_WithWrongTableName_Throws_KeyNotFoundException()
    {
        // Register under "SG1_Right"
        CompiledMaterializerStore.Add("SG1_Right", (Func<DbDataReader, PlainEntity>)(r =>
            new PlainEntity { Id = r.GetInt32(0), Tag = r.GetString(1) }));

        // Get with wrong table name must throw
        var ex = Assert.Throws<KeyNotFoundException>(() =>
            CompiledMaterializerStore.Get<PlainEntity>("SG1_Wrong"));

        Assert.Contains("PlainEntity", ex.Message);
        Assert.Contains("SG1_Wrong", ex.Message);
    }

    [Fact]
    public void TryGet_WithExplicitTableName_MatchesAddWithExplicitTableName()
    {
        Func<DbDataReader, SharedEntity> mat = r => new SharedEntity { Id = 99, Name = "explicit" };
        CompiledMaterializerStore.Add("SG1_Explicit", mat);

        // TryGet with matching table name should succeed
        var found = CompiledMaterializerStore.TryGet(typeof(SharedEntity), "SG1_Explicit", out var gotMat);
        Assert.True(found, "TryGet must find materializer registered with explicit table name");
        Assert.NotNull(gotMat);
    }

    [Fact]
    public void TryGet_WithNonMatchingTableName_ReturnsFalse()
    {
        CompiledMaterializerStore.Add("SG1_Registered", (Func<DbDataReader, SharedEntity>)(r =>
            new SharedEntity()));

        var found = CompiledMaterializerStore.TryGet(typeof(SharedEntity), "SG1_NotRegistered", out _);
        Assert.False(found, "TryGet with non-matching table name must return false");
    }

    // ── Backward compatibility: Add<T>(mat) still uses [Table] attr / type name ──

    [Fact]
    public void Add_WithoutTableName_StillUsesAttributeOrTypeName()
    {
        // TableAEntity has [Table("CustomTableA")]
        CompiledMaterializerStore.Add<TableAEntity>(r =>
            new TableAEntity { Id = r.GetInt32(0), Value = r.GetString(1) });

        // Get<T>() (no table name) should find it via the attribute
        var mat = CompiledMaterializerStore.Get<TableAEntity>();
        Assert.NotNull(mat);

        // Get<T>("CustomTableA") should also find it
        var matByName = CompiledMaterializerStore.Get<TableAEntity>("CustomTableA");
        Assert.NotNull(matByName);
    }

    [Fact]
    public void Get_NoArg_DelegatesToGetWithAttributeName()
    {
        // TableBEntity has [Table("CustomTableB")]
        CompiledMaterializerStore.Add<TableBEntity>(r =>
            new TableBEntity { Id = r.GetInt32(0), Value = r.GetString(1) });

        // Get<T>() should delegate to Get<T>("CustomTableB")
        var mat1 = CompiledMaterializerStore.Get<TableBEntity>();
        var mat2 = CompiledMaterializerStore.Get<TableBEntity>("CustomTableB");

        Assert.NotNull(mat1);
        Assert.NotNull(mat2);
    }

    // ── Multi-model: two different table names for same CLR type ─────────────

    [Fact]
    public async Task MultiModel_SameType_DifferentTables_IndependentMaterializers()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // Create two tables with different data
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE SG1_Model1 (Id INTEGER, Name TEXT);
                INSERT INTO SG1_Model1 VALUES(1, 'model1');
                CREATE TABLE SG1_Model2 (Id INTEGER, Name TEXT);
                INSERT INTO SG1_Model2 VALUES(2, 'model2');
            ";
            cmd.ExecuteNonQuery();
        }

        // Register distinct materializers for the same CLR type under different table names
        CompiledMaterializerStore.Add("SG1_Model1", (Func<DbDataReader, SharedEntity>)(r =>
            new SharedEntity { Id = r.GetInt32(r.GetOrdinal("Id")), Name = "mat1:" + r.GetString(r.GetOrdinal("Name")) }));
        CompiledMaterializerStore.Add("SG1_Model2", (Func<DbDataReader, SharedEntity>)(r =>
            new SharedEntity { Id = r.GetInt32(r.GetOrdinal("Id")), Name = "mat2:" + r.GetString(r.GetOrdinal("Name")) }));

        // Retrieve materializer for Model1
        var mat1 = CompiledMaterializerStore.Get<SharedEntity>("SG1_Model1");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT Id, Name FROM SG1_Model1";
            using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            var entity = await mat1(reader, CancellationToken.None);
            Assert.Equal(1, entity.Id);
            Assert.Equal("mat1:model1", entity.Name);
        }

        // Retrieve materializer for Model2
        var mat2 = CompiledMaterializerStore.Get<SharedEntity>("SG1_Model2");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT Id, Name FROM SG1_Model2";
            using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            var entity = await mat2(reader, CancellationToken.None);
            Assert.Equal(2, entity.Id);
            Assert.Equal("mat2:model2", entity.Name);
        }
    }

    // ── First-write-wins per (type, tableName) key with explicit table name ──

    [Fact]
    public void Add_ExplicitTableName_FirstWriteWins()
    {
        int firstCalls = 0;
        int secondCalls = 0;

        Func<DbDataReader, SharedEntity> first = _ => { firstCalls++; return new SharedEntity(); };
        Func<DbDataReader, SharedEntity> second = _ => { secondCalls++; return new SharedEntity(); };

        CompiledMaterializerStore.Add("SG1_FWW", first);
        CompiledMaterializerStore.Add("SG1_FWW", second); // must be ignored

        var mat = CompiledMaterializerStore.Get<SharedEntity>("SG1_FWW");

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE _fww (Id INTEGER, Name TEXT); INSERT INTO _fww VALUES(1,'x')";
        setup.ExecuteNonQuery();
        using var read = cn.CreateCommand();
        read.CommandText = "SELECT Id, Name FROM _fww";
        using var reader = read.ExecuteReader();
        reader.Read();

        var task = mat(reader, CancellationToken.None);
        Assert.True(task.IsCompleted);

        Assert.Equal(1, firstCalls);
        Assert.Equal(0, secondCalls);
    }

    // ── Error message includes table name ────────────────────────────────────

    [Fact]
    public void Get_WithTableName_ErrorMessageIncludesTableName()
    {
        var ex = Assert.Throws<KeyNotFoundException>(() =>
            CompiledMaterializerStore.Get<SharedEntity>("SG1_NonExistent_12345"));

        Assert.Contains("SG1_NonExistent_12345", ex.Message);
        Assert.Contains(nameof(SharedEntity), ex.Message);
    }
}
