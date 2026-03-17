using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Tests for OwnsMany - owned collection navigation stored in a separate child table.
/// </summary>
public class OwnedManyTests
{
    // ── Entity definitions ────────────────────────────────────────────────

    private class Post
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public List<Tag> Tags { get; set; } = new();
    }

    private class Tag
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class Order
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string CustomerName { get; set; } = string.Empty;
        public List<OrderLine> Lines { get; set; } = new();
    }

    private class OrderLine
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string ProductName { get; set; } = string.Empty;
        public int Quantity { get; set; }
        public decimal Price { get; set; }
    }

    private class Album
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public List<Track> Tracks { get; set; } = new();
    }

    private class Track
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int DurationSeconds { get; set; }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static SqliteConnection CreateOpenDb(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext CreatePostContext(SqliteConnection cn) => new DbContext(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Tag>(p => p.Tags, tableName: "Tag", foreignKey: "PostId")
    });

    private static DbContext CreateOrderContext(SqliteConnection cn) => new DbContext(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Order>().OwnsMany<OrderLine>(o => o.Lines, tableName: "OrderLine", foreignKey: "OrderId")
    });

    private static SqliteConnection CreatePostDb()
    {
        var cn = CreateOpenDb(@"
            CREATE TABLE Post (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
            CREATE TABLE Tag (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Name TEXT NOT NULL);
        ");
        return cn;
    }

    private static SqliteConnection CreateOrderDb()
    {
        var cn = CreateOpenDb(@"
            CREATE TABLE ""Order"" (Id INTEGER PRIMARY KEY AUTOINCREMENT, CustomerName TEXT NOT NULL);
            CREATE TABLE OrderLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, OrderId INTEGER NOT NULL, ProductName TEXT NOT NULL, Quantity INTEGER NOT NULL, Price REAL NOT NULL);
        ");
        return cn;
    }

    // ── OM-1: Configuration ───────────────────────────────────────────────

    [Fact]
    public void OM1_OwnsMany_RegistersOwnedCollectionNavigation()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var map = ctx.GetMapping(typeof(Post));
        Assert.Single(map.OwnedCollections);
        var oc = map.OwnedCollections[0];
        Assert.Equal("Tag", oc.TableName);
        Assert.Equal("PostId", oc.ForeignKeyColumn);
        Assert.Equal(typeof(Tag), oc.OwnedType);
    }

    [Fact]
    public void OM1_OwnedCollectionMapping_HasCorrectColumns()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var map = ctx.GetMapping(typeof(Post));
        var oc = map.OwnedCollections[0];
        // Columns should include Id and Name (Tag's own columns, not the FK PostId)
        Assert.True(oc.Columns.Length >= 1);
        Assert.Contains(oc.Columns, c => c.Name == "Name");
    }

    [Fact]
    public void OM1_OwnedCollectionMapping_EscTableIsEscaped()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var map = ctx.GetMapping(typeof(Post));
        var oc = map.OwnedCollections[0];
        Assert.Contains("Tag", oc.EscTable);
    }

    [Fact]
    public void OM1_OwnedCollectionMapping_FKColumnEscaped()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var map = ctx.GetMapping(typeof(Post));
        var oc = map.OwnedCollections[0];
        Assert.Contains("PostId", oc.EscForeignKeyColumn);
    }

    [Fact]
    public void OM1_MultipleOwnedCollections_RegisteredCorrectly()
    {
        using var cn = CreatePostDb();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Post>()
                .OwnsMany<Tag>(p => p.Tags, tableName: "Tag", foreignKey: "PostId")
        });
        var map = ctx.GetMapping(typeof(Post));
        Assert.Single(map.OwnedCollections);
    }

    [Fact]
    public void OM1_DefaultTableName_UsesOwnedTypeName()
    {
        using var cn = CreatePostDb();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            // No explicit tableName - should default to "Tag"
            OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Tag>(p => p.Tags, foreignKey: "PostId")
        });
        var map = ctx.GetMapping(typeof(Post));
        Assert.Equal("Tag", map.OwnedCollections[0].TableName);
    }

    [Fact]
    public void OM1_DefaultForeignKey_UsesOwnerTypeNamePlusId()
    {
        using var cn = CreatePostDb();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            // No explicit foreignKey - should default to "PostId"
            OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Tag>(p => p.Tags, tableName: "Tag")
        });
        var map = ctx.GetMapping(typeof(Post));
        Assert.Equal("PostId", map.OwnedCollections[0].ForeignKeyColumn);
    }

    // ── OM-2: Insert (Add owner + owned items) ────────────────────────────

    [Fact]
    public async Task OM2_Add_Owner_With_OwnedItems_InsertsAll()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "Hello", Tags = new List<Tag> { new Tag { Name = "csharp" }, new Tag { Name = "dotnet" } } };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag WHERE PostId = " + post.Id;
        Assert.Equal(2L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM2_Add_Owner_With_EmptyCollection_NoOwnedInserts()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "Empty", Tags = new List<Tag>() };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag";
        Assert.Equal(0L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM2_Add_Owner_With_NullCollection_NoException()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "Null", Tags = null! };
        ctx.Add(post);
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }

    [Fact]
    public async Task OM2_Add_Owner_OwnedItemsGetCorrectFK()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "FK Test", Tags = new List<Tag> { new Tag { Name = "x" } } };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT PostId FROM Tag WHERE Name = 'x'";
        var fk = (long)cmd.ExecuteScalar()!;
        Assert.Equal(post.Id, (int)fk);
    }

    [Fact]
    public async Task OM2_Add_MultipleOwners_EachGetOwnedItems()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var p1 = new Post { Title = "P1", Tags = new List<Tag> { new Tag { Name = "a" } } };
        var p2 = new Post { Title = "P2", Tags = new List<Tag> { new Tag { Name = "b" }, new Tag { Name = "c" } } };
        ctx.Add(p1);
        ctx.Add(p2);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag WHERE PostId = " + p1.Id;
        Assert.Equal(1L, cmd.ExecuteScalar());
        cmd.CommandText = "SELECT COUNT(*) FROM Tag WHERE PostId = " + p2.Id;
        Assert.Equal(2L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM2_Add_OwnedItemValues_ArePersisted()
    {
        using var cn = CreateOrderDb();
        using var ctx = CreateOrderContext(cn);
        var order = new Order
        {
            CustomerName = "Alice",
            Lines = new List<OrderLine>
            {
                new OrderLine { ProductName = "Widget", Quantity = 3, Price = 9.99m }
            }
        };
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT ProductName, Quantity, Price FROM OrderLine WHERE OrderId = " + order.Id;
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Widget", reader.GetString(0));
        Assert.Equal(3, reader.GetInt32(1));
        Assert.Equal(9.99m, (decimal)reader.GetDouble(2), 2);
    }

    // ── OM-3: Load (Query owner and get owned items populated) ────────────

    [Fact]
    public async Task OM3_Query_PopulatesOwnedCollection()
    {
        using var cn = CreatePostDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Post (Title) VALUES ('Hello'); INSERT INTO Tag (PostId, Name) VALUES (1, 'csharp'), (1, 'dotnet');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = CreatePostContext(cn);
        var posts = await ctx.Query<Post>().ToListAsync();
        Assert.Single(posts);
        Assert.Equal(2, posts[0].Tags.Count);
        Assert.Contains(posts[0].Tags, t => t.Name == "csharp");
        Assert.Contains(posts[0].Tags, t => t.Name == "dotnet");
    }

    [Fact]
    public async Task OM3_Query_OwnedCollection_AssignedToCorrectOwner()
    {
        using var cn = CreatePostDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO Post (Title) VALUES ('P1');
                INSERT INTO Post (Title) VALUES ('P2');
                INSERT INTO Tag (PostId, Name) VALUES (1, 'a'), (1, 'b');
                INSERT INTO Tag (PostId, Name) VALUES (2, 'c');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = CreatePostContext(cn);
        var posts = await ctx.Query<Post>().ToListAsync();
        Assert.Equal(2, posts.Count);
        var p1 = posts.First(p => p.Title == "P1");
        var p2 = posts.First(p => p.Title == "P2");
        Assert.Equal(2, p1.Tags.Count);
        Assert.Single(p2.Tags);
    }

    [Fact]
    public async Task OM3_Query_NoOwnedItems_ReturnsEmptyCollection()
    {
        using var cn = CreatePostDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Post (Title) VALUES ('Empty');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = CreatePostContext(cn);
        var posts = await ctx.Query<Post>().ToListAsync();
        Assert.Single(posts);
        Assert.NotNull(posts[0].Tags);
        Assert.Empty(posts[0].Tags);
    }

    [Fact]
    public async Task OM3_Query_OwnedItemValues_AreCorrect()
    {
        using var cn = CreateOrderDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO ""Order"" (CustomerName) VALUES ('Bob');
                INSERT INTO OrderLine (OrderId, ProductName, Quantity, Price) VALUES (1, 'Gadget', 5, 19.99);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = CreateOrderContext(cn);
        var orders = await ctx.Query<Order>().ToListAsync();
        Assert.Single(orders);
        Assert.Single(orders[0].Lines);
        Assert.Equal("Gadget", orders[0].Lines[0].ProductName);
        Assert.Equal(5, orders[0].Lines[0].Quantity);
    }

    [Fact]
    public async Task OM3_Query_MultipleOwners_EachGetsCorrectItems()
    {
        using var cn = CreatePostDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO Post (Title) VALUES ('X'), ('Y');
                INSERT INTO Tag (PostId, Name) VALUES (1, 'a'), (2, 'b'), (2, 'c');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = CreatePostContext(cn);
        var posts = (await ctx.Query<Post>().ToListAsync()).OrderBy(p => p.Title).ToList();
        Assert.Single(posts[0].Tags); // X has 1
        Assert.Equal(2, posts[1].Tags.Count); // Y has 2
    }

    [Fact]
    public async Task OM3_Query_WithWhere_StillLoadsOwnedItems()
    {
        using var cn = CreatePostDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO Post (Title) VALUES ('Alpha'), ('Beta');
                INSERT INTO Tag (PostId, Name) VALUES (1, 'a'), (2, 'b');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = CreatePostContext(cn);
        var posts = await ctx.Query<Post>().Where(p => p.Title == "Alpha").ToListAsync();
        Assert.Single(posts);
        Assert.Single(posts[0].Tags);
        Assert.Equal("a", posts[0].Tags[0].Name);
    }

    // ── OM-4: Update (Modify owner with changed owned items) ──────────────

    [Fact]
    public async Task OM4_Update_Owner_ReplacesOwnedItems()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "V1", Tags = new List<Tag> { new Tag { Name = "old" } } };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        post.Tags = new List<Tag> { new Tag { Name = "new1" }, new Tag { Name = "new2" } };
        ctx.Update(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag WHERE PostId = " + post.Id;
        Assert.Equal(2L, cmd.ExecuteScalar());
        cmd.CommandText = "SELECT COUNT(*) FROM Tag WHERE Name = 'old'";
        Assert.Equal(0L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM4_Update_Owner_WithEmptyCollection_DeletesOwnedItems()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "HasTags", Tags = new List<Tag> { new Tag { Name = "x" } } };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        post.Tags = new List<Tag>();
        ctx.Update(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag";
        Assert.Equal(0L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM4_Update_Owner_SameItems_ReinsertsThem()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "Same", Tags = new List<Tag> { new Tag { Name = "keep" } } };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        // Update post title but keep same tags
        post.Title = "Same2";
        ctx.Update(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag WHERE PostId = " + post.Id + " AND Name = 'keep'";
        Assert.Equal(1L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM4_Update_NewOwnedItem_NewValues_Persisted()
    {
        using var cn = CreateOrderDb();
        using var ctx = CreateOrderContext(cn);
        var order = new Order { CustomerName = "Charlie", Lines = new List<OrderLine>() };
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        order.Lines = new List<OrderLine> { new OrderLine { ProductName = "Foo", Quantity = 1, Price = 5m } };
        ctx.Update(order);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT ProductName FROM OrderLine WHERE OrderId = " + order.Id;
        Assert.Equal("Foo", cmd.ExecuteScalar());
    }

    // ── OM-5: Delete (Delete owner cascades to owned items) ───────────────

    [Fact]
    public async Task OM5_Delete_Owner_DeletesOwnedItems()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "ToDelete", Tags = new List<Tag> { new Tag { Name = "a" }, new Tag { Name = "b" } } };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        ctx.Remove(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag";
        Assert.Equal(0L, cmd.ExecuteScalar());
        cmd.CommandText = "SELECT COUNT(*) FROM Post";
        Assert.Equal(0L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM5_Delete_Owner_WithNoOwnedItems_Works()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "Empty", Tags = new List<Tag>() };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        ctx.Remove(post);
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Post";
        Assert.Equal(0L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM5_Delete_OneOfManyOwners_OnlyDeletesCorrectOwnedItems()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var p1 = new Post { Title = "P1", Tags = new List<Tag> { new Tag { Name = "a" } } };
        var p2 = new Post { Title = "P2", Tags = new List<Tag> { new Tag { Name = "b" }, new Tag { Name = "c" } } };
        ctx.Add(p1);
        ctx.Add(p2);
        await ctx.SaveChangesAsync();

        ctx.Remove(p1);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag";
        Assert.Equal(2L, cmd.ExecuteScalar()); // p2's tags remain
        cmd.CommandText = "SELECT COUNT(*) FROM Post";
        Assert.Equal(1L, cmd.ExecuteScalar());
    }

    // ── OM-6: Roundtrip (Save and reload) ─────────────────────────────────

    [Fact]
    public async Task OM6_Roundtrip_SaveAndReload_OwnedItems()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "RT", Tags = new List<Tag> { new Tag { Name = "x" }, new Tag { Name = "y" } } };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        using var ctx2 = CreatePostContext(cn);
        var loaded = await ctx2.Query<Post>().Where(p => p.Id == post.Id).FirstAsync();
        Assert.Equal(2, loaded.Tags.Count);
        Assert.Contains(loaded.Tags, t => t.Name == "x");
        Assert.Contains(loaded.Tags, t => t.Name == "y");
    }

    [Fact]
    public async Task OM6_Roundtrip_UpdateAndReload_NewOwnedItems()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "Update", Tags = new List<Tag> { new Tag { Name = "old" } } };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        post.Tags = new List<Tag> { new Tag { Name = "new" } };
        ctx.Update(post);
        await ctx.SaveChangesAsync();

        using var ctx2 = CreatePostContext(cn);
        var loaded = await ctx2.Query<Post>().Where(p => p.Id == post.Id).FirstAsync();
        Assert.Single(loaded.Tags);
        Assert.Equal("new", loaded.Tags[0].Name);
    }

    [Fact]
    public async Task OM6_Roundtrip_MultipleFieldTypes_Preserved()
    {
        using var cn = CreateOrderDb();
        using var ctx = CreateOrderContext(cn);
        var order = new Order
        {
            CustomerName = "Dan",
            Lines = new List<OrderLine>
            {
                new OrderLine { ProductName = "Item1", Quantity = 7, Price = 12.50m },
                new OrderLine { ProductName = "Item2", Quantity = 3, Price = 5.00m }
            }
        };
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        using var ctx2 = CreateOrderContext(cn);
        var loaded = await ctx2.Query<Order>().Where(o => o.Id == order.Id).FirstAsync();
        Assert.Equal(2, loaded.Lines.Count);
        var line = loaded.Lines.First(l => l.ProductName == "Item1");
        Assert.Equal(7, line.Quantity);
        Assert.Equal(12.50m, line.Price, 2);
    }

    // ── OM-7: Fluent configuration options ────────────────────────────────

    [Fact]
    public void OM7_OwnsMany_CustomTableName_Used()
    {
        using var cn = CreatePostDb();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Tag>(p => p.Tags, tableName: "PostTags", foreignKey: "PostId")
        });
        var map = ctx.GetMapping(typeof(Post));
        Assert.Equal("PostTags", map.OwnedCollections[0].TableName);
    }

    [Fact]
    public void OM7_OwnsMany_CustomForeignKey_Used()
    {
        using var cn = CreatePostDb();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Tag>(p => p.Tags, tableName: "Tag", foreignKey: "ArticleId")
        });
        var map = ctx.GetMapping(typeof(Post));
        Assert.Equal("ArticleId", map.OwnedCollections[0].ForeignKeyColumn);
    }

    [Fact]
    public void OM7_OwnsMany_WithBuildAction_ConfiguresOwnedType()
    {
        using var cn = CreatePostDb();
        // Build action should be accepted without error
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Tag>(p => p.Tags, tableName: "Tag", foreignKey: "PostId",
                buildAction: tb => tb.ToTable("Tag"))
        });
        var map = ctx.GetMapping(typeof(Post));
        Assert.Single(map.OwnedCollections);
    }

    [Fact]
    public void OM7_OwnsMany_NavigationGetter_ReturnsCollection()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var map = ctx.GetMapping(typeof(Post));
        var oc = map.OwnedCollections[0];
        var post = new Post { Tags = new List<Tag> { new Tag { Name = "x" } } };
        var result = oc.CollectionGetter(post);
        Assert.NotNull(result);
        Assert.IsAssignableFrom<IEnumerable<Tag>>(result);
    }

    [Fact]
    public void OM7_OwnsMany_NavigationSetter_AssignsCollection()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var map = ctx.GetMapping(typeof(Post));
        var oc = map.OwnedCollections[0];
        var post = new Post();
        var tags = new List<Tag> { new Tag { Name = "y" } };
        oc.CollectionSetter(post, tags);
        Assert.Same(tags, post.Tags);
    }

    // ── OM-8: No OwnsMany config - regular entity not affected ─────────────

    [Fact]
    public async Task OM8_NoOwnsMany_RegularEntity_Works()
    {
        using var cn = CreatePostDb();
        using var ctx = new DbContext(cn, new SqliteProvider()); // No OwnsMany config
        var post = new Post { Title = "Normal" };
        ctx.Add(post);
        await ctx.SaveChangesAsync();
        var loaded = await ctx.Query<Post>().ToListAsync();
        Assert.Single(loaded);
        Assert.Equal("Normal", loaded[0].Title);
    }

    [Fact]
    public async Task OM8_NoOwnsMany_OwnedCollectionProperty_IsNull()
    {
        using var cn = CreatePostDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Post (Title) VALUES ('Test');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        var posts = await ctx.Query<Post>().ToListAsync();
        // Without OwnsMany config, Tags is not populated from the DB (stays at field-initializer value)
        Assert.Single(posts);
        Assert.Empty(posts[0].Tags);
    }

    // ── OM-9: Edge cases ──────────────────────────────────────────────────

    [Fact]
    public async Task OM9_Add_ManyOwnedItems_AllPersisted()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var tags = Enumerable.Range(1, 20).Select(i => new Tag { Name = $"tag{i}" }).ToList();
        var post = new Post { Title = "Many", Tags = tags };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag WHERE PostId = " + post.Id;
        Assert.Equal(20L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM9_Query_SingleOwner_NoOtherOwnersItems()
    {
        using var cn = CreatePostDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO Post (Title) VALUES ('A'), ('B');
                INSERT INTO Tag (PostId, Name) VALUES (1, 'only-for-a'), (2, 'only-for-b');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = CreatePostContext(cn);
        var posts = await ctx.Query<Post>().Where(p => p.Title == "A").ToListAsync();
        Assert.Single(posts);
        Assert.Single(posts[0].Tags);
        Assert.Equal("only-for-a", posts[0].Tags[0].Name);
    }

    [Fact]
    public async Task OM9_Delete_Owner_OwnedItemsNotOrphanedInDB()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "Del", Tags = new List<Tag> { new Tag { Name = "z" } } };
        ctx.Add(post);
        await ctx.SaveChangesAsync();
        var postId = post.Id;

        ctx.Remove(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM Tag WHERE PostId = {postId}";
        Assert.Equal(0L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM9_Update_ZeroToSomeItems_Inserts()
    {
        using var cn = CreatePostDb();
        using var ctx = CreatePostContext(cn);
        var post = new Post { Title = "Start", Tags = new List<Tag>() };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        post.Tags = new List<Tag> { new Tag { Name = "added" } };
        ctx.Update(post);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Tag WHERE PostId = " + post.Id;
        Assert.Equal(1L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task OM9_Add_WithAlbumTracks_AllPersisted()
    {
        using var cn = CreateOpenDb(@"
            CREATE TABLE Album (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
            CREATE TABLE Track (Id INTEGER PRIMARY KEY AUTOINCREMENT, AlbumId INTEGER NOT NULL, Name TEXT NOT NULL, DurationSeconds INTEGER NOT NULL);
        ");
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Album>().OwnsMany<Track>(a => a.Tracks, tableName: "Track", foreignKey: "AlbumId")
        });
        var album = new Album
        {
            Title = "Greatest Hits",
            Tracks = new List<Track>
            {
                new Track { Name = "Track 1", DurationSeconds = 210 },
                new Track { Name = "Track 2", DurationSeconds = 185 }
            }
        };
        ctx.Add(album);
        await ctx.SaveChangesAsync();

        using var ctx2 = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Album>().OwnsMany<Track>(a => a.Tracks, tableName: "Track", foreignKey: "AlbumId")
        });
        var loaded = await ctx2.Query<Album>().Where(a => a.Id == album.Id).FirstAsync();
        Assert.Equal(2, loaded.Tracks.Count);
        Assert.Contains(loaded.Tracks, t => t.Name == "Track 1" && t.DurationSeconds == 210);
    }

    [Fact]
    public async Task OM9_CountAsync_NotAffectedByOwnedCollections()
    {
        using var cn = CreatePostDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO Post (Title) VALUES ('P1'), ('P2'), ('P3');
                INSERT INTO Tag (PostId, Name) VALUES (1, 'a'), (1, 'b'), (2, 'c');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = CreatePostContext(cn);
        var count = await ctx.Query<Post>().CountAsync();
        Assert.Equal(3, count);
    }
}
