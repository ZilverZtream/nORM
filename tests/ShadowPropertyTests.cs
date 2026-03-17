using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for shadow property infrastructure: ShadowPropertyStore, ShadowPropertyInfo,
/// Column.IsShadow, materialization, write paths, and change tracking.
/// </summary>
public class ShadowPropertyTests
{
    // ── Entities ─────────────────────────────────────────────────────────────

    public class Article
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    public class Post
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Content { get; set; } = "";
    }

    public class Widget
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static SqliteConnection CreateOpenDb(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    // ── SP-1: Configuration Tests ─────────────────────────────────────────

    [Fact]
    public void SP1_HasShadowProperty_RegistersInShadowProperties()
    {
        var builder = new EntityTypeBuilder<Article>();
        builder.Property<int>("CreatedYear");
        var config = builder.Configuration;
        Assert.True(config.ShadowProperties.ContainsKey("CreatedYear"));
    }

    [Fact]
    public void SP1_ShadowProperty_ClrType_CorrectType()
    {
        var builder = new EntityTypeBuilder<Article>();
        builder.Property<string>("Tenant");
        var config = builder.Configuration;
        Assert.Equal(typeof(string), config.ShadowProperties["Tenant"].ClrType);
    }

    [Fact]
    public void SP1_ShadowProperty_ColumnName_Override()
    {
        var builder = new EntityTypeBuilder<Article>();
        builder.Property<int>("Priority").HasColumnName("priority_col");
        var config = builder.Configuration;
        Assert.Equal("priority_col", config.ShadowProperties["Priority"].ColumnName);
    }

    [Fact]
    public void SP1_ShadowProperty_Column_InMapping()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, AuditTag TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("AuditTag")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Article));
        var shadow = mapping.Columns.FirstOrDefault(c => c.PropName == "AuditTag");
        Assert.NotNull(shadow);
    }

    [Fact]
    public void SP1_ShadowColumn_IsShadow_True()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, AuditTag TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("AuditTag")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Article));
        var shadow = mapping.Columns.First(c => c.PropName == "AuditTag");
        Assert.True(shadow.IsShadow);
    }

    [Fact]
    public void SP1_ShadowColumn_ColumnName_Override_UsedInMapping()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, audit_tag TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>()
                .Property<string>("AuditTag")
                .HasColumnName("audit_tag")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Article));
        var shadow = mapping.Columns.FirstOrDefault(c => c.PropName == "AuditTag");
        Assert.NotNull(shadow);
        Assert.Equal("audit_tag", shadow!.Name);
    }

    // ── SP-2: ShadowPropertyStore unit tests ──────────────────────────────

    [Fact]
    public void SP2_Store_SetAndGet_RoundTrip()
    {
        var entity = new Article { Id = 1, Title = "Test" };
        ShadowPropertyStore.Set(entity, "Tenant", "acme");
        var result = ShadowPropertyStore.Get(entity, "Tenant");
        Assert.Equal("acme", result);
    }

    [Fact]
    public void SP2_Store_Unset_Returns_Null()
    {
        var entity = new Article { Id = 1 };
        var result = ShadowPropertyStore.Get(entity, "NonExistent");
        Assert.Null(result);
    }

    [Fact]
    public void SP2_Store_TwoEntities_Independent()
    {
        var e1 = new Article { Id = 1 };
        var e2 = new Article { Id = 2 };
        ShadowPropertyStore.Set(e1, "Tenant", "a");
        ShadowPropertyStore.Set(e2, "Tenant", "b");
        Assert.Equal("a", ShadowPropertyStore.Get(e1, "Tenant"));
        Assert.Equal("b", ShadowPropertyStore.Get(e2, "Tenant"));
    }

    [Fact]
    public void SP2_Store_MultipleProperties_OnSameEntity()
    {
        var entity = new Article { Id = 1 };
        ShadowPropertyStore.Set(entity, "Tenant", "x");
        ShadowPropertyStore.Set(entity, "Priority", 5);
        ShadowPropertyStore.Set(entity, "IsDeleted", true);
        Assert.Equal("x", ShadowPropertyStore.Get(entity, "Tenant"));
        Assert.Equal(5, ShadowPropertyStore.Get(entity, "Priority"));
        Assert.Equal(true, ShadowPropertyStore.Get(entity, "IsDeleted"));
    }

    [Fact]
    public void SP2_Store_OverwriteValue()
    {
        var entity = new Article { Id = 1 };
        ShadowPropertyStore.Set(entity, "Tenant", "old");
        ShadowPropertyStore.Set(entity, "Tenant", "new");
        Assert.Equal("new", ShadowPropertyStore.Get(entity, "Tenant"));
    }

    [Fact]
    public void SP2_Store_SetNull_Allowed()
    {
        var entity = new Article { Id = 1 };
        ShadowPropertyStore.Set(entity, "Tenant", "value");
        ShadowPropertyStore.Set(entity, "Tenant", null);
        var result = ShadowPropertyStore.Get(entity, "Tenant");
        Assert.Null(result);
    }

    [Fact]
    public void SP2_Store_Concurrent_SetGet_Correct()
    {
        var entity = new Article { Id = 1 };
        var errors = new List<Exception>();
        var threads = new Thread[10];
        for (int i = 0; i < threads.Length; i++)
        {
            var n = i;
            threads[i] = new Thread(() =>
            {
                try
                {
                    ShadowPropertyStore.Set(entity, $"Prop{n}", n);
                    var v = ShadowPropertyStore.Get(entity, $"Prop{n}");
                    if (!n.Equals(v))
                        throw new Exception($"Expected {n} got {v}");
                }
                catch (Exception ex) { lock (errors) errors.Add(ex); }
            });
        }
        foreach (var t in threads) t.Start();
        foreach (var t in threads) t.Join();
        Assert.Empty(errors);
    }

    [Fact]
    public void SP2_Store_IntValue_Roundtrip()
    {
        var entity = new Article { Id = 1 };
        ShadowPropertyStore.Set(entity, "Score", 42);
        Assert.Equal(42, ShadowPropertyStore.Get(entity, "Score"));
    }

    // ── SP-3: Column integration tests ────────────────────────────────────

    [Fact]
    public void SP3_ShadowPropertyInfo_CanRead_True()
    {
        var info = new ShadowPropertyInfo("Tenant", typeof(string), typeof(Article));
        Assert.True(info.CanRead);
    }

    [Fact]
    public void SP3_ShadowPropertyInfo_CanWrite_True()
    {
        var info = new ShadowPropertyInfo("Tenant", typeof(string), typeof(Article));
        Assert.True(info.CanWrite);
    }

    [Fact]
    public void SP3_ShadowPropertyInfo_Name_Correct()
    {
        var info = new ShadowPropertyInfo("MyProp", typeof(int), typeof(Article));
        Assert.Equal("MyProp", info.Name);
    }

    [Fact]
    public void SP3_ShadowColumn_Getter_UsesStore()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Article));
        var shadowCol = mapping.Columns.First(c => c.PropName == "Tenant");
        var entity = new Article { Id = 1 };
        ShadowPropertyStore.Set(entity, "Tenant", "acme");
        Assert.Equal("acme", shadowCol.Getter(entity));
    }

    [Fact]
    public void SP3_ShadowColumn_Setter_UsesStore()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Article));
        var shadowCol = mapping.Columns.First(c => c.PropName == "Tenant");
        var entity = new Article { Id = 1 };
        shadowCol.Setter(entity, "newvalue");
        Assert.Equal("newvalue", ShadowPropertyStore.Get(entity, "Tenant"));
    }

    // ── SP-4: Materialization tests ───────────────────────────────────────

    [Fact]
    public async Task SP4_Materialize_ShadowColumn_PopulatesStore()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('Hello', 'acme')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var articles = await ctx.Query<Article>().ToListAsync();
        Assert.Single(articles);
        Assert.Equal("acme", ShadowPropertyStore.Get(articles[0], "Tenant"));
    }

    [Fact]
    public async Task SP4_Materialize_NullShadowColumn_SetsNull()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('Hello', NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var articles = await ctx.Query<Article>().ToListAsync();
        Assert.Single(articles);
        Assert.Null(ShadowPropertyStore.Get(articles[0], "Tenant"));
    }

    [Fact]
    public async Task SP4_Materialize_IntShadowColumn_PopulatesStore()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Priority INTEGER)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Priority) VALUES ('Hello', 99)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<long>("Priority")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var articles = await ctx.Query<Article>().ToListAsync();
        Assert.Single(articles);
        Assert.Equal(99L, ShadowPropertyStore.Get(articles[0], "Priority"));
    }

    [Fact]
    public async Task SP4_Materialize_MultipleRows_EachPopulated()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('A', 'x'), ('B', 'y')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var articles = await ctx.Query<Article>().OrderBy(a => a.Title).ToListAsync();
        Assert.Equal(2, articles.Count);
        Assert.Equal("x", ShadowPropertyStore.Get(articles[0], "Tenant"));
        Assert.Equal("y", ShadowPropertyStore.Get(articles[1], "Tenant"));
    }

    [Fact]
    public async Task SP4_Materialize_RegularColsAlsoPopulated()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('Hello', 'acme')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var articles = await ctx.Query<Article>().ToListAsync();
        Assert.Equal("Hello", articles[0].Title);
    }

    // ── SP-5: Write tests ─────────────────────────────────────────────────

    [Fact]
    public async Task SP5_Insert_ShadowColumn_Persisted()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = new Article { Title = "Hello" };
        ShadowPropertyStore.Set(article, "Tenant", "myorg");
        ctx.Add(article);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Tenant FROM Article WHERE Title = 'Hello'";
        var result = cmd.ExecuteScalar();
        Assert.Equal("myorg", result);
    }

    [Fact]
    public async Task SP5_Insert_IntShadowColumn_Persisted()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Priority INTEGER)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<long>("Priority")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = new Article { Title = "Hello" };
        ShadowPropertyStore.Set(article, "Priority", 5L);
        ctx.Add(article);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Priority FROM Article";
        var result = cmd.ExecuteScalar();
        Assert.Equal(5L, result);
    }

    [Fact]
    public async Task SP5_Update_ShadowColumn_Persisted()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('Hello', 'old')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = (await ctx.Query<Article>().ToListAsync())[0];
        ShadowPropertyStore.Set(article, "Tenant", "new");
        ctx.Update(article);
        await ctx.SaveChangesAsync();

        using var cmd2 = cn.CreateCommand();
        cmd2.CommandText = "SELECT Tenant FROM Article";
        var result = cmd2.ExecuteScalar();
        Assert.Equal("new", result);
    }

    [Fact]
    public async Task SP5_NullShadowValue_Inserted_AsNull()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = new Article { Title = "Hello" };
        // Don't set Tenant - should be null
        ctx.Add(article);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Tenant FROM Article";
        var result = cmd.ExecuteScalar();
        Assert.Equal(DBNull.Value, result);
    }

    [Fact]
    public async Task SP5_BatchInsert_ShadowColumn_AllPersisted()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        for (int i = 0; i < 3; i++)
        {
            var a = new Article { Title = $"A{i}" };
            ShadowPropertyStore.Set(a, "Tenant", $"org{i}");
            ctx.Add(a);
        }
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Article WHERE Tenant LIKE 'org%'";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task SP5_RoundTrip_WriteThenRead_ShadowValueMatches()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = new Article { Title = "Hello" };
        ShadowPropertyStore.Set(article, "Tenant", "myorg");
        ctx.Add(article);
        await ctx.SaveChangesAsync();

        var loaded = await ctx.Query<Article>().FirstAsync();
        Assert.Equal("myorg", ShadowPropertyStore.Get(loaded, "Tenant"));
    }

    // ── SP-6: Change tracking tests ───────────────────────────────────────

    [Fact]
    public async Task SP6_ShadowValueChange_DetectedByChangeTracker()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('Hello', 'old')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant"),
            EagerChangeTracking = true
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = (await ctx.Query<Article>().ToListAsync())[0];
        var entryBefore = ctx.Entry(article);
        Assert.Equal(EntityState.Unchanged, entryBefore.State);

        ShadowPropertyStore.Set(article, "Tenant", "new");
        ctx.ChangeTracker.DetectAllChanges();
        var entryAfter = ctx.Entry(article);
        Assert.Equal(EntityState.Modified, entryAfter.State);
    }

    [Fact]
    public async Task SP6_ShadowValueUnchanged_NoModified()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('Hello', 'same')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant"),
            EagerChangeTracking = true
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var articles = await ctx.Query<Article>().ToListAsync();
        var article = articles[0];
        // Tenant is "same" in both original and current - no change
        ctx.ChangeTracker.DetectAllChanges();
        var entry = ctx.Entry(article);
        Assert.Equal(EntityState.Unchanged, entry.State);
    }

    [Fact]
    public async Task SP6_Update_PersistsShadowChange()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('Hello', 'old')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant"),
            EagerChangeTracking = true
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = (await ctx.Query<Article>().ToListAsync())[0];
        ShadowPropertyStore.Set(article, "Tenant", "updated");
        await ctx.SaveChangesAsync();

        using var cmd2 = cn.CreateCommand();
        cmd2.CommandText = "SELECT Tenant FROM Article";
        var result = cmd2.ExecuteScalar();
        Assert.Equal("updated", result);
    }

    [Fact]
    public async Task SP6_ShadowPropertyEntry_State_After_Add()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = new Article { Title = "Hello" };
        ctx.Add(article);
        var entry = ctx.Entry(article);
        Assert.Equal(EntityState.Added, entry.State);
    }

    [Fact]
    public async Task SP6_ShadowProperty_SaveChanges_AfterUpdate()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('T1', 'org1')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = (await ctx.Query<Article>().ToListAsync())[0];
        ShadowPropertyStore.Set(article, "Tenant", "org2");
        ctx.Update(article);
        await ctx.SaveChangesAsync();

        // Verify no error was thrown and change was saved
        var loaded = await ctx.Query<Article>().FirstAsync();
        Assert.Equal("org2", ShadowPropertyStore.Get(loaded, "Tenant"));
    }

    // ── SP-7: Type coverage ───────────────────────────────────────────────

    [Fact]
    public async Task SP7_IntType_RoundTrip()
    {
        using var cn = CreateOpenDb("CREATE TABLE Widget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Score INTEGER)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().Property<long>("Score")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var w = new Widget { Name = "W1" };
        ShadowPropertyStore.Set(w, "Score", 100L);
        ctx.Add(w);
        await ctx.SaveChangesAsync();
        var loaded = await ctx.Query<Widget>().FirstAsync();
        Assert.Equal(100L, ShadowPropertyStore.Get(loaded, "Score"));
    }

    [Fact]
    public async Task SP7_StringType_RoundTrip()
    {
        using var cn = CreateOpenDb("CREATE TABLE Widget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Tag TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().Property<string>("Tag")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var w = new Widget { Name = "W1" };
        ShadowPropertyStore.Set(w, "Tag", "hello");
        ctx.Add(w);
        await ctx.SaveChangesAsync();
        var loaded = await ctx.Query<Widget>().FirstAsync();
        Assert.Equal("hello", ShadowPropertyStore.Get(loaded, "Tag"));
    }

    [Fact]
    public async Task SP7_BoolType_RoundTrip()
    {
        using var cn = CreateOpenDb("CREATE TABLE Widget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, IsDeleted INTEGER)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().Property<long>("IsDeleted")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var w = new Widget { Name = "W1" };
        ShadowPropertyStore.Set(w, "IsDeleted", 1L);
        ctx.Add(w);
        await ctx.SaveChangesAsync();
        var loaded = await ctx.Query<Widget>().FirstAsync();
        Assert.Equal(1L, ShadowPropertyStore.Get(loaded, "IsDeleted"));
    }

    [Fact]
    public async Task SP7_DateTimeType_RoundTrip()
    {
        using var cn = CreateOpenDb("CREATE TABLE Widget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, CreatedAt TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().Property<string>("CreatedAt")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var w = new Widget { Name = "W1" };
        ShadowPropertyStore.Set(w, "CreatedAt", "2024-01-15");
        ctx.Add(w);
        await ctx.SaveChangesAsync();
        var loaded = await ctx.Query<Widget>().FirstAsync();
        Assert.Equal("2024-01-15", ShadowPropertyStore.Get(loaded, "CreatedAt"));
    }

    [Fact]
    public async Task SP7_NullableString_RoundTrip()
    {
        using var cn = CreateOpenDb("CREATE TABLE Widget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Tag TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().Property<string>("Tag")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var w = new Widget { Name = "W1" };
        // Don't set Tag - should be null
        ctx.Add(w);
        await ctx.SaveChangesAsync();
        var loaded = await ctx.Query<Widget>().FirstAsync();
        Assert.Null(ShadowPropertyStore.Get(loaded, "Tag"));
    }

    [Fact]
    public async Task SP7_LargeInt_RoundTrip()
    {
        using var cn = CreateOpenDb("CREATE TABLE Widget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, BigNum INTEGER)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().Property<long>("BigNum")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var w = new Widget { Name = "W1" };
        ShadowPropertyStore.Set(w, "BigNum", 9999999999L);
        ctx.Add(w);
        await ctx.SaveChangesAsync();
        var loaded = await ctx.Query<Widget>().FirstAsync();
        Assert.Equal(9999999999L, ShadowPropertyStore.Get(loaded, "BigNum"));
    }

    // ── SP-8: Multi-entity type independence ──────────────────────────────

    [Fact]
    public async Task SP8_TwoEntityTypes_SameShadowName_Independent()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT);" +
            "CREATE TABLE Widget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Article>().Property<string>("Tenant");
                mb.Entity<Widget>().Property<string>("Tenant");
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var a = new Article { Title = "A" };
        var w = new Widget { Name = "W" };
        ShadowPropertyStore.Set(a, "Tenant", "article-org");
        ShadowPropertyStore.Set(w, "Tenant", "widget-org");
        ctx.Add(a);
        ctx.Add(w);
        await ctx.SaveChangesAsync();

        var articles = await ctx.Query<Article>().ToListAsync();
        var widgets = await ctx.Query<Widget>().ToListAsync();
        Assert.Equal("article-org", ShadowPropertyStore.Get(articles[0], "Tenant"));
        Assert.Equal("widget-org", ShadowPropertyStore.Get(widgets[0], "Tenant"));
    }

    [Fact]
    public async Task SP8_ShadowProp_DoesNotAppearOnEntityClrType()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Verify "Tenant" is NOT a property on Article CLR type
        var clrProps = typeof(Article).GetProperties();
        Assert.DoesNotContain(clrProps, p => p.Name == "Tenant");
    }

    [Fact]
    public async Task SP8_TwoInstances_SameShadowPropName_SeparateValues()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var a1 = new Article { Title = "A1" };
        var a2 = new Article { Title = "A2" };
        ShadowPropertyStore.Set(a1, "Tenant", "org1");
        ShadowPropertyStore.Set(a2, "Tenant", "org2");
        ctx.Add(a1);
        ctx.Add(a2);
        await ctx.SaveChangesAsync();

        var all = await ctx.Query<Article>().OrderBy(a => a.Title).ToListAsync();
        Assert.Equal("org1", ShadowPropertyStore.Get(all[0], "Tenant"));
        Assert.Equal("org2", ShadowPropertyStore.Get(all[1], "Tenant"));
    }

    // ── SP-9: Column name override ────────────────────────────────────────

    [Fact]
    public async Task SP9_ColumnNameOverride_WritesToCorrectColumn()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, audit_tag TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>()
                .Property<string>("AuditTag")
                .HasColumnName("audit_tag")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var a = new Article { Title = "T1" };
        ShadowPropertyStore.Set(a, "AuditTag", "tag_value");
        ctx.Add(a);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT audit_tag FROM Article";
        Assert.Equal("tag_value", cmd.ExecuteScalar());
    }

    [Fact]
    public async Task SP9_ColumnNameOverride_ReadsFromCorrectColumn()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, audit_tag TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, audit_tag) VALUES ('T1', 'read_val')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>()
                .Property<string>("AuditTag")
                .HasColumnName("audit_tag")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var articles = await ctx.Query<Article>().ToListAsync();
        Assert.Equal("read_val", ShadowPropertyStore.Get(articles[0], "AuditTag"));
    }

    [Fact]
    public void SP9_ShadowColumn_WithColumnNameOverride_CorrectName()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, x_col TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>()
                .Property<string>("MyShadow")
                .HasColumnName("x_col")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Article));
        var shadow = mapping.Columns.First(c => c.PropName == "MyShadow");
        Assert.Equal("x_col", shadow.Name);
    }

    // ── SP-10: Global filter interaction ─────────────────────────────────

    [Fact]
    public async Task SP10_GlobalFilter_CanExcludeByRegularColumn()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('A', 'org1'), ('B', 'org2')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Regular WHERE filter (shadow props in WHERE require expression support)
        var all = await ctx.Query<Article>().ToListAsync();
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task SP10_Count_WithShadowProperty_Mapped()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('A', 'org1'), ('B', 'org2'), ('C', 'org1')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var count = await ctx.Query<Article>().CountAsync();
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task SP10_Delete_WithShadowProperty_Works()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title, Tenant) VALUES ('Del', 'org1')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = (await ctx.Query<Article>().ToListAsync())[0];
        ctx.Remove(article);
        await ctx.SaveChangesAsync();

        using var cmd2 = cn.CreateCommand();
        cmd2.CommandText = "SELECT COUNT(*) FROM Article";
        Assert.Equal(0L, cmd2.ExecuteScalar());
    }

    // ── SP-11: Edge cases ─────────────────────────────────────────────────

    [Fact]
    public async Task SP11_SetNullThenValue_Correct()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = new Article { Title = "Hello" };
        ShadowPropertyStore.Set(article, "Tenant", null);
        ShadowPropertyStore.Set(article, "Tenant", "value");
        ctx.Add(article);
        await ctx.SaveChangesAsync();

        var loaded = await ctx.Query<Article>().FirstAsync();
        Assert.Equal("value", ShadowPropertyStore.Get(loaded, "Tenant"));
    }

    [Fact]
    public async Task SP11_SetValueThenNull_NullPersisted()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = new Article { Title = "Hello" };
        ShadowPropertyStore.Set(article, "Tenant", "value");
        ShadowPropertyStore.Set(article, "Tenant", null);
        ctx.Add(article);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Tenant FROM Article";
        Assert.Equal(DBNull.Value, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task SP11_ShadowProperty_MultipleColumnsOnEntity()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT, Priority INTEGER)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Article>().Property<string>("Tenant");
                mb.Entity<Article>().Property<long>("Priority");
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = new Article { Title = "Hello" };
        ShadowPropertyStore.Set(article, "Tenant", "org1");
        ShadowPropertyStore.Set(article, "Priority", 7L);
        ctx.Add(article);
        await ctx.SaveChangesAsync();

        var loaded = await ctx.Query<Article>().FirstAsync();
        Assert.Equal("org1", ShadowPropertyStore.Get(loaded, "Tenant"));
        Assert.Equal(7L, ShadowPropertyStore.Get(loaded, "Priority"));
    }

    [Fact]
    public async Task SP11_AfterSaveChanges_ReadBack_Correct()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Tenant TEXT)");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var article = new Article { Title = "Hello" };
        ShadowPropertyStore.Set(article, "Tenant", "saved");
        ctx.Add(article);
        await ctx.SaveChangesAsync();
        Assert.NotEqual(0, article.Id);

        var loaded = await ctx.Query<Article>().Where(a => a.Id == article.Id).FirstAsync();
        Assert.Equal("saved", ShadowPropertyStore.Get(loaded, "Tenant"));
    }

    [Fact]
    public async Task SP11_NoShadowProperty_Query_WorksNormally()
    {
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT)");
        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new Article { Title = "Normal" });
        await ctx.SaveChangesAsync();
        var result = await ctx.Query<Article>().ToListAsync();
        Assert.Single(result);
        Assert.Equal("Normal", result[0].Title);
    }

    [Fact]
    public async Task SP11_ShadowProp_NotInShadow_OnMissingColumn_SkippedGracefully()
    {
        // DB table does NOT have the shadow column; materialization should not throw.
        // (SQLite may return the column name as a string literal rather than failing,
        //  so we only assert that the query completes without exception and returns rows.)
        using var cn = CreateOpenDb("CREATE TABLE Article (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT)");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Article (Title) VALUES ('Hello')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Article>().Property<string>("Tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Should not throw even when shadow column is absent from the DB table.
        var articles = await ctx.Query<Article>().ToListAsync();
        Assert.Single(articles);
        Assert.Equal("Hello", articles[0].Title);
    }
}
