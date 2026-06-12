#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public async Task ScaffoldAsync_WithPluralTableNames_SingularizesEntitiesAndKeepsPluralQueryProperties()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Blogs (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Categories (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE People (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Classes (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Statuses (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Canvases (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_plural_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "PluralCtx");

            AssertEntity(dir, "Blog");
            AssertEntity(dir, "Category");
            AssertEntity(dir, "Person");
            AssertEntity(dir, "Class");
            AssertEntity(dir, "Status");
            AssertEntity(dir, "Canvas");

            var contextCode = File.ReadAllText(Path.Combine(dir, "PluralCtx.cs"));
            Assert.Contains("public IQueryable<Blog> Blogs", contextCode);
            Assert.Contains("public IQueryable<Category> Categories", contextCode);
            Assert.Contains("public IQueryable<Person> People", contextCode);
            Assert.Contains("public IQueryable<Class> Classes", contextCode);
            Assert.Contains("public IQueryable<Status> Statuses", contextCode);
            Assert.Contains("public IQueryable<Canvas> Canvases", contextCode);
            Assert.DoesNotContain("Blogses", contextCode);
            Assert.DoesNotContain("Categorieses", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithPluralizerDisabled_PreservesEntityAndQueryNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Blogs (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE Categories (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_no_pluralizer_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "NoPluralizerCtx",
                new ScaffoldOptions { UsePluralizer = false });

            AssertEntity(dir, "Blogs");
            AssertEntity(dir, "Categories");

            var contextCode = File.ReadAllText(Path.Combine(dir, "NoPluralizerCtx.cs"));
            Assert.Contains("public IQueryable<Blogs> Blogs", contextCode);
            Assert.Contains("public IQueryable<Categories> Categories", contextCode);
            Assert.DoesNotContain("Blogses", contextCode);
            Assert.DoesNotContain("Categorieses", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithUseDatabaseNames_DoesNotDoublePluralizePreservedNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Blogs (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_db_plural_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "DatabasePluralCtx",
                new ScaffoldOptions { UseDatabaseNames = true });

            AssertEntity(dir, "Blogs");
            var contextCode = File.ReadAllText(Path.Combine(dir, "DatabasePluralCtx.cs"));
            Assert.Contains("public IQueryable<Blogs> Blogs", contextCode);
            Assert.DoesNotContain("Blogses", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    private static void AssertEntity(string dir, string entityName)
    {
        var entityCode = File.ReadAllText(Path.Combine(dir, entityName + ".cs"));
        Assert.Contains("public partial class " + entityName, entityCode);
    }
}
