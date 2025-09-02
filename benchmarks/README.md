# nORM Performance Benchmarks

This benchmarks project provides comprehensive performance testing for nORM against other popular .NET ORMs and data access patterns.

## 🎯 What Gets Benchmarked

### **Competitors**
- **nORM** (our implementation)
- **Entity Framework Core** (Microsoft's ORM)
- **Dapper** (lightweight micro-ORM)
- **Raw ADO.NET** (baseline performance)

### **Test Scenarios**

#### **1. Single Insert Operations**
- `Insert_Single_EfCore()` - EF Core single insert
- `Insert_Single_nORM()` - nORM single insert
- `Insert_Single_Dapper()` - Dapper single insert
- `Insert_Single_RawAdo()` - Raw ADO.NET insert

#### **2. Simple Queries**
- Basic SELECT with WHERE and LIMIT
- Tests LINQ-to-SQL translation efficiency
- Measures object materialization performance

#### **3. Complex Queries**
- Multiple WHERE conditions
- ORDER BY with SKIP/TAKE
- Tests query optimization and parameter handling

#### **4. Join Operations** ⭐
- Inner joins between Users and Orders
- Tests our new join implementation
- Compares multi-table query performance

#### **5. Aggregation Queries**
- COUNT operations with filtering
- Tests scalar query performance

#### **6. Bulk Operations**
- Bulk insert of 100 records
- Tests batch processing efficiency

## 🚀 Running the Benchmarks

### **Full Benchmark Suite**
```bash
cd benchmarks
dotnet run -c Release
```

This will run the complete BenchmarkDotNet suite with:
- Memory allocation tracking
- Statistical analysis
- Multiple iterations for accuracy
- Detailed performance reports

### **Quick Functionality Test**
```bash
cd benchmarks
dotnet run -c Release -- --quick
```

This runs a quick functional test to verify nORM works correctly without the full benchmark overhead.

## 📊 Expected Results

Based on the design goals, we expect to see:

### **Performance Ranking (Fastest to Slowest)**
1. **Raw ADO.NET** - Direct database calls, no overhead
2. **nORM** - Dapper-level speed with EF-like features  
3. **Dapper** - Lightweight, minimal overhead
4. **Entity Framework Core** - Full-featured but slower

### **Memory Allocation Ranking (Least to Most)**
1. **Raw ADO.NET** - Minimal object creation
2. **nORM** - IL-based materialization, minimal allocations
3. **Dapper** - Efficient but some boxing
4. **Entity Framework Core** - Change tracking overhead

## 🔧 Benchmark Configuration

### **Test Data**
- **1,000 Users** with realistic properties
- **2,000 Orders** with foreign key relationships
- **SQLite databases** for consistent testing environment

### **Entity Structure**
```csharp
public class BenchmarkUser
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public DateTime CreatedAt { get; set; }
    public bool IsActive { get; set; }
    public int Age { get; set; }
    public string City { get; set; }
}

public class BenchmarkOrder
{
    public int Id { get; set; }
    public int UserId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
    public string ProductName { get; set; }
}
```

### **BenchmarkDotNet Settings**
- **Memory Diagnoser** enabled for allocation tracking
- **Statistical validation** with multiple runs
- **Rank ordering** for easy comparison
- **Configurable iterations** for accuracy

## 📈 Understanding Results

### **Key Metrics**
- **Mean**: Average execution time (lower = better)
- **Allocated**: Memory allocated per operation (lower = better) 
- **Rank**: Relative performance ranking (1 = fastest)
- **Ratio**: Performance compared to baseline

### **Sample Output**
```
|                    Method |      Mean | Allocated | Rank |
|-------------------------- |----------:|----------:|-----:|
|     Query_Simple_RawAdo   |  12.34 μs |      84 B |    1 |
|     Query_Simple_nORM     |  13.67 μs |     112 B |    2 |
|     Query_Simple_Dapper   |  14.23 μs |     128 B |    3 |
|     Query_Simple_EfCore   |  24.89 μs |     456 B |    4 |
```

## 🎯 Benchmark Goals

### **Validation Goals**
- ✅ **Functionality**: Verify all nORM operations work correctly
- ✅ **Compatibility**: Ensure consistent results across ORMs
- ✅ **Reliability**: Stable performance across multiple runs

### **Performance Goals**
- 🎯 **Speed**: Match or exceed Dapper performance
- 🎯 **Memory**: Minimal allocations through IL generation
- 🎯 **Scalability**: Linear performance with data size

### **Feature Goals**
- 🎯 **Join Performance**: Competitive multi-table queries
- 🎯 **Bulk Operations**: Efficient batch processing
- 🎯 **Complex Queries**: Fast LINQ translation

## 🔍 Analyzing Results

### **Performance Analysis**
1. **Compare Mean times** - Lower is better
2. **Check Allocated memory** - Lower means less GC pressure
3. **Look at Rank** - Relative positioning matters
4. **Consider Ratio** - How much slower than fastest?

### **Red Flags**
- nORM significantly slower than Dapper
- High memory allocations compared to competitors
- Large variance in execution times
- Join performance much worse than simple queries

### **Success Indicators**
- nORM within 20% of Dapper performance
- Lower allocations than Entity Framework
- Consistent performance across operations
- Join operations competitive with EF Core

## 🚨 Troubleshooting

### **Common Issues**
```bash
# If benchmark fails to start:
dotnet restore
dotnet build -c Release

# If SQLite issues:
# Make sure Microsoft.Data.Sqlite is installed

# If permission errors:
# Run as Administrator or check file permissions

# If memory errors:
# Reduce test data size in OrmBenchmarks.cs
```

### **Debugging Steps**
1. Run with `--quick` flag first
2. Check console output for specific errors
3. Verify database files can be created
4. Test individual benchmark methods

## 📝 Extending Benchmarks

### **Adding New Scenarios**
```csharp
[Benchmark]
public async Task<List<BenchmarkUser>> MyNewScenario_nORM()
{
    // Your test implementation
    return await _nOrmContext!.Query<BenchmarkUser>()
        .Where(/* your conditions */)
        .ToListAsync();
}
```

### **Adding New Competitors**
1. Add setup method in `Setup()`
2. Implement benchmark methods for each scenario
3. Add cleanup in `Cleanup()`
4. Update documentation

## 🏆 Benchmark Results History

Results will be saved in `BenchmarkDotNet.Artifacts/results/` with detailed reports including:
- HTML reports with charts
- CSV data for analysis
- Markdown summaries
- JSON data for automation

---

**🎯 Goal**: Prove nORM delivers **Entity Framework productivity** with **Dapper performance**!
