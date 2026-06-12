#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public async Task ScaffoldAsync_EmptyDatabase_ThrowsOrWritesContextFile()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "TestCtx");
            Assert.True(File.Exists(Path.Combine(dir, "TestCtx.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithTable_ThrowsOrWritesFiles()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SanWidget2 (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "MyCtx2");
            Assert.True(File.Exists(Path.Combine(dir, "MyCtx2.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SanWidget2.cs")));
            var entityCode = File.ReadAllText(Path.Combine(dir, "SanWidget2.cs"));
            Assert.Contains("[Required]", entityCode);
            Assert.Contains("public string Name { get; set; } = default!;", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoWarnings_RemovesStaleWarningReportsWhenOverwriteAllowed()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CleanWidget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var warningMd = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
        var warningJson = Path.Combine(dir, "nORM.ScaffoldWarnings.json");
        await File.WriteAllTextAsync(warningMd, "stale");
        await File.WriteAllTextAsync(warningJson, """{"stale":true}""");
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CleanCtx");

            Assert.True(File.Exists(Path.Combine(dir, "CleanWidget.cs")));
            Assert.False(File.Exists(warningMd));
            Assert.False(File.Exists(warningJson));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoWarningsAndNoOverwrite_FailsOnStaleWarningReports()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CleanNoOverwriteWidget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var warningMd = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
        await File.WriteAllTextAsync(warningMd, "stale");
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "CleanNoOverwriteCtx",
                    new ScaffoldOptions { OverwriteFiles = false }));

            Assert.Contains("stale scaffold warning report", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.True(File.Exists(warningMd));
            Assert.False(File.Exists(Path.Combine(dir, "CleanNoOverwriteWidget.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }
}
