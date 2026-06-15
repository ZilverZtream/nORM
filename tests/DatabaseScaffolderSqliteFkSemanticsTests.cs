#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public async Task ScaffoldAsync_WithSqliteMatchFullForeignKey_ReportsRelationshipDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE MatchParent (
                Id INTEGER PRIMARY KEY
            );
            CREATE TABLE MatchChild (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NOT NULL,
                CONSTRAINT FK_MatchChild_Parent FOREIGN KEY (ParentId) REFERENCES MatchParent(Id) MATCH FULL DEFERRABLE INITIALLY DEFERRED
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_sqlite_match_fk_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SqliteMatchFkCtx");

            var childCode = await File.ReadAllTextAsync(Path.Combine(dir, "MatchChild.cs"));
            var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "SqliteMatchFkCtx.cs"));
            using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

            Assert.Contains("public long ParentId { get; set; }", childCode, StringComparison.Ordinal);
            Assert.DoesNotContain("public MatchParent", childCode, StringComparison.Ordinal);
            Assert.DoesNotContain(".HasForeignKey(", contextCode, StringComparison.Ordinal);
            var diagnostic = Assert.Single(providerOwned, item => item.GetProperty("kind").GetString() == "ReferentialAction");
            Assert.Equal("SCF106", diagnostic.GetProperty("code").GetString());
            Assert.Contains("MATCH FULL", diagnostic.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("DEFERRABLE", diagnostic.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);
            var metadata = diagnostic.GetProperty("metadata");
            Assert.Equal("FK_MatchChild_Parent", diagnostic.GetProperty("name").GetString());
            Assert.True(metadata.GetProperty("navigationSuppressed").GetBoolean());
            Assert.False(metadata.GetProperty("generatedNavigationSupported").GetBoolean());
            Assert.Contains("MATCH FULL", metadata.GetProperty("onUpdate").GetString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("DEFERRABLE", metadata.GetProperty("onUpdate").GetString(), StringComparison.OrdinalIgnoreCase);

            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }
}
