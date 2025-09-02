```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4652)
Intel Core i7-10750H CPU 2.60GHz, 1 CPU, 12 logical and 6 physical cores
.NET SDK 9.0.103
  [Host]     : .NET 8.0.13 (8.0.1325.6609), X64 RyuJIT AVX2
  Job-BRAPQQ : .NET 8.0.13 (8.0.1325.6609), X64 RyuJIT AVX2

IterationCount=3  WarmupCount=1  

```
| Method           | Mean      | Error      | StdDev    | Gen0   | Allocated |
|----------------- |----------:|-----------:|----------:|-------:|----------:|
| Count_Operation  |  20.18 μs |  52.804 μs |  2.894 μs | 1.0986 |   7.03 KB |
| Query_Simple     |  56.28 μs |  86.595 μs |  4.747 μs | 1.2207 |   8.22 KB |
| Insert_Single    |  63.69 μs |   7.730 μs |  0.424 μs | 0.6104 |   4.24 KB |
| Query_Complex    | 115.10 μs | 444.876 μs | 24.385 μs | 1.7090 |  11.87 KB |
| Query_Join       | 119.13 μs | 693.908 μs | 38.035 μs | 2.6855 |  16.47 KB |
| BulkInsert_Small | 274.31 μs | 132.118 μs |  7.242 μs | 9.7656 |  65.12 KB |
