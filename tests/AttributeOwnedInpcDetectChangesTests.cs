using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// An entity that implements INotifyPropertyChanged is tracked by notifications alone and excluded from the
/// snapshot scan — but a write to an owned-reference SUB-property (order.Subtotal.Amount) raises no
/// PropertyChanged on the OWNER, so it must still be scanned. The scan signal keyed only on FLUENT
/// OwnsOne registration missed an owned reference declared via the [Owned] ATTRIBUTE (which flattens to
/// columns but never appears in fluent config), so the edit was silently dropped and the UPDATE never ran.
/// The fluent OwnsOne shape and the POCO-owner shape both persist correctly — the gap was attribute + INPC.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AttributeOwnedInpcDetectChangesTests
{
    [Owned]
    public class AttrMoney { public decimal Amount { get; set; } public string Currency { get; set; } = ""; }

    [Table("ZZAttrOrder")]
    public class AttrOrder : INotifyPropertyChanged
    {
        [Key] public int Id { get; set; }
        private string _note = "";
        public string Note { get => _note; set { if (_note != value) { _note = value; PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Note))); } } }
        public AttrMoney Subtotal { get; set; } = new();
        public event PropertyChangedEventHandler? PropertyChanged;
    }

    [Fact]
    public async Task Inpc_owner_attribute_owned_reference_subproperty_edit_is_persisted()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE ZZAttrOrder (Id INTEGER PRIMARY KEY, Note TEXT NOT NULL, Subtotal_Amount TEXT NOT NULL, Subtotal_Currency TEXT NOT NULL);" +
                "INSERT INTO ZZAttrOrder VALUES (1, 'n', '100', 'USD');";
            cmd.ExecuteNonQuery();
        }
        await using var ctx = new DbContext(cn, new SqliteProvider());   // [Owned] attribute only, no OnModelCreating

        var order = ctx.Query<AttrOrder>().First();
        order.Subtotal.Amount = 250m;   // owned sub-property edit, no scalar edit on the INPC owner
        await ctx.SaveChangesAsync();

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Subtotal_Amount FROM ZZAttrOrder WHERE Id = 1";
        Assert.Equal(250m, Convert.ToDecimal(check.ExecuteScalar(), CultureInfo.InvariantCulture));
    }
}
