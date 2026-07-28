using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// When a relationship's foreign key targets a NON-PK (alternate) principal column
/// (HasForeignKey(dep => dep.Fk, prin => prin.AltKey)), setting the relationship through the reference
/// navigation must persist the principal's ALTERNATE-key value into the FK, not its primary key. The
/// reference-nav fixup hardcoded the principal PK, so it silently wrote the wrong FK — pointing the
/// dependent at the wrong (or nonexistent) principal. The collection direction already resolves the alt key.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AlternateKeyReferenceNavWriteTests
{
    [Table("AkWarehouse")]
    public sealed class Warehouse
    {
        [Key] public int Id { get; set; }             // DB-generated PK
        public int LocationCode { get; set; }          // ALTERNATE key (FK target)
        public string Name { get; set; } = "";
        public List<Shipment> Shipments { get; set; } = new();
    }

    [Table("AkShipment")]
    public sealed class Shipment
    {
        [Key] public int Id { get; set; }
        public int WarehouseLocation { get; set; }     // FK -> Warehouse.LocationCode (NOT Id)
        public string Tracking { get; set; } = "";
        public Warehouse? Warehouse { get; set; }
    }

    private static DbContext Seed(SqliteConnection cn)
    {
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE AkWarehouse (Id INTEGER PRIMARY KEY AUTOINCREMENT, LocationCode INTEGER NOT NULL UNIQUE, Name TEXT NOT NULL);" +
                "CREATE TABLE AkShipment (Id INTEGER PRIMARY KEY AUTOINCREMENT, WarehouseLocation INTEGER NOT NULL, Tracking TEXT NOT NULL);" +
                "INSERT INTO AkWarehouse (Id, LocationCode, Name) VALUES (1, 500, 'Main');"; // Id != LocationCode
            c.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Warehouse>().HasKey(w => w.Id);
                mb.Entity<Shipment>().HasKey(s => s.Id);
                mb.Entity<Warehouse>()
                    .HasMany(w => w.Shipments)
                    .WithOne(s => s.Warehouse!)
                    .HasForeignKey(s => s.WarehouseLocation, w => w.LocationCode);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static long FkOf(SqliteConnection cn, string tracking)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT WarehouseLocation FROM AkShipment WHERE Tracking = '{tracking}'";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Reference_nav_to_alternate_key_principal_persists_alt_key_not_pk()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await using var ctx = Seed(cn);
        var warehouse = await ctx.Query<Warehouse>().FirstAsync(w => w.LocationCode == 500);
        var shipment = new Shipment { Tracking = "T1", Warehouse = warehouse };   // reference nav only
        ctx.Add(shipment);
        await ctx.SaveChangesAsync();
        Assert.Equal(500, FkOf(cn, "T1"));           // was 1 (the PK)
        Assert.Equal(500, shipment.WarehouseLocation);
    }

    [Fact]
    public async Task Reference_nav_to_new_alternate_key_principal_persists_alt_key_not_generated_pk()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await using var ctx = Seed(cn);
        var warehouse = new Warehouse { LocationCode = 777, Name = "New" };
        var shipment = new Shipment { Tracking = "T3", Warehouse = warehouse };
        ctx.Add(warehouse);
        ctx.Add(shipment);
        await ctx.SaveChangesAsync();
        Assert.Equal(777, FkOf(cn, "T3"));           // was 2 (the generated PK)
    }

    [Fact]
    public async Task Collection_add_to_alternate_key_principal_persists_alt_key_not_pk()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await using var ctx = Seed(cn);
        var warehouse = await ctx.Query<Warehouse>().FirstAsync(w => w.LocationCode == 500);
        var shipment = new Shipment { Tracking = "T2" };
        warehouse.Shipments.Add(shipment);
        ctx.Add(shipment);
        await ctx.SaveChangesAsync();
        Assert.Equal(500, FkOf(cn, "T2"));           // control: collection direction already correct
    }
}
