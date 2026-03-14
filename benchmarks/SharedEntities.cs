using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace nORM.Benchmarks
{
    public class BenchmarkUser
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public string Department { get; set; } = string.Empty;
        public double Salary { get; set; }
        public bool IsActive { get; set; }
        public DateTime CreatedAt { get; set; }
        public int Age { get; set; }
        public string City { get; set; } = string.Empty;
        [NotMapped]
        public virtual ICollection<BenchmarkOrder> Orders { get; set; } = new List<BenchmarkOrder>();
    }

    public class BenchmarkOrder
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int UserId { get; set; }
        public decimal Amount { get; set; }
        public DateTime OrderDate { get; set; }
        public string ProductName { get; set; } = string.Empty;
        [NotMapped]
        [ForeignKey(nameof(UserId))]
        public virtual BenchmarkUser? User { get; set; }
    }
}
