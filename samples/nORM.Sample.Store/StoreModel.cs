using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace nORM.Sample.Store;

[Table("SampleTenant")]
public sealed class StoreTenant
{
    [Key] public int Id { get; set; }
    public int TenantId { get; set; }
    public string Name { get; set; } = "";
}

[Table("SampleCustomer")]
public sealed class StoreCustomer
{
    [Key] public int Id { get; set; }
    public int TenantId { get; set; }
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public bool IsActive { get; set; }
}

[Table("SampleProduct")]
public sealed class StoreProduct
{
    [Key] public int Id { get; set; }
    public int TenantId { get; set; }
    public string Sku { get; set; } = "";
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public bool IsActive { get; set; }
}

[Table("SampleOrder")]
public sealed class StoreOrder
{
    [Key] public int Id { get; set; }
    public int TenantId { get; set; }
    public int CustomerId { get; set; }
    public string Status { get; set; } = "";
    public decimal Total { get; set; }
    public ICollection<StoreOrderLine> Lines { get; set; } = new List<StoreOrderLine>();
}

[Table("SampleOrderLine")]
public sealed class StoreOrderLine
{
    [Key] public int Id { get; set; }
    public int TenantId { get; set; }
    public int OrderId { get; set; }
    public int ProductId { get; set; }
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

[Table("SampleStoreEvent")]
public sealed class StoreEvent
{
    [Key] public int Id { get; set; }
    public int TenantId { get; set; }
    public string EventType { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime CreatedUtc { get; set; }
}

public sealed class StoreProductDto
{
    public int Id { get; set; }
    public string Sku { get; set; } = "";
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
}

public sealed class StoreOrderTotalDto
{
    public string Status { get; set; } = "";
    public decimal Total { get; set; }
}
