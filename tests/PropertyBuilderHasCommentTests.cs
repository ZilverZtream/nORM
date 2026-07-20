using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// EF Core parity: <c>Property(x => x.Col).HasComment("...")</c> emits a column comment using each provider's
/// native mechanism. The SQLite path is verified end-to-end (fluent config -> snapshot -> the comment surviving
/// verbatim in <c>sqlite_master.sql</c>); the MySQL/PostgreSQL/SQL Server paths are verified through their
/// migration SQL generators, including that the comment text is escaped so it cannot break out of the literal.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PropertyBuilderHasCommentTests
{
    [Table("HcomWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Plain { get; set; }
    }

    [Fact]
    public void HasComment_survives_verbatim_in_sqlite_schema()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property<string>(w => w.Name).HasComment("the display name");
                // Plain has no comment as a contrast.
            }
        }, ownsConnection: false);

        ctx.Database.EnsureCreated();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT sql FROM sqlite_master WHERE name='HcomWidget'";
        var ddl = (string)cmd.ExecuteScalar()!;

        Assert.Contains("/* the display name */", ddl);   // comment retained verbatim in the stored schema
        // The comment belongs to the Name column, not the Plain column.
        Assert.Contains("\"Name\"", ddl);
        Assert.DoesNotContain("Plain\" INTEGER NULL /*", ddl);
    }

    [Fact]
    public void HasComment_neutralizes_the_block_comment_terminator_for_sqlite()
    {
        var t = CommentTable("Name", "danger */ ; DROP TABLE X --");
        var sql = string.Join("\n", new SqliteMigrationSqlGenerator().GenerateSql(new SchemaDiff { AddedTables = { t } }).Up);

        Assert.DoesNotContain("*/ ; DROP", sql);        // the terminating */ was broken up
        Assert.Contains("/* danger * / ; DROP TABLE X -- */", sql);
    }

    [Fact]
    public void HasComment_emits_comment_on_column_for_postgres()
    {
        var t = CommentTable("Name", "the display name");
        var sql = string.Join("\n", new PostgresMigrationSqlGenerator().GenerateSql(new SchemaDiff { AddedTables = { t } }).Up);

        Assert.Contains("COMMENT ON COLUMN", sql);
        Assert.Contains("IS 'the display name'", sql);
    }

    [Fact]
    public void HasComment_emits_inline_comment_clause_for_mysql()
    {
        var t = CommentTable("Name", "the display name");
        var sql = string.Join("\n", new MySqlMigrationSqlGenerator().GenerateSql(new SchemaDiff { AddedTables = { t } }).Up);

        Assert.Contains("COMMENT 'the display name'", sql);
    }

    [Fact]
    public void HasComment_emits_extended_property_for_sqlserver()
    {
        var t = CommentTable("Name", "the display name");
        var sql = string.Join("\n", new SqlServerMigrationSqlGenerator().GenerateSql(new SchemaDiff { AddedTables = { t } }).Up);

        Assert.Contains("sp_addextendedproperty", sql);
        Assert.Contains("@name=N'MS_Description'", sql);
        Assert.Contains("@value=N'the display name'", sql);
        Assert.Contains("@level2name=N'Name'", sql);
    }

    [Theory]
    [InlineData("O'Brien's note")]
    public void HasComment_escapes_quotes_in_string_literal_providers(string comment)
    {
        var t = CommentTable("Name", comment);
        var pg = string.Join("\n", new PostgresMigrationSqlGenerator().GenerateSql(new SchemaDiff { AddedTables = { t } }).Up);
        var my = string.Join("\n", new MySqlMigrationSqlGenerator().GenerateSql(new SchemaDiff { AddedTables = { t } }).Up);
        var ss = string.Join("\n", new SqlServerMigrationSqlGenerator().GenerateSql(new SchemaDiff { AddedTables = { t } }).Up);

        Assert.Contains("IS 'O''Brien''s note'", pg);
        Assert.Contains("COMMENT 'O''Brien''s note'", my);
        Assert.Contains("@value=N'O''Brien''s note'", ss);
    }

    private static TableSchema CommentTable(string commentedColumn, string comment)
    {
        var t = new TableSchema { Name = "HcomWidget" };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        t.Columns.Add(new ColumnSchema
        {
            Name = commentedColumn,
            ClrType = typeof(string).FullName!,
            IsNullable = true,
            Comment = comment,
        });
        return t;
    }
}
