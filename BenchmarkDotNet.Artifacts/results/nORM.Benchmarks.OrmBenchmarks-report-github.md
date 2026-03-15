```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean        | Error     | StdDev    | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |------------:|----------:|----------:|-----:|---------:|--------:|----------:|
| &#39;BulkInsert Naive - Dapper per row&#39;         |          NA |        NA |        NA |    ? |       NA |      NA |        NA |
| Count_nORM                                  |    13.22 μs |  0.112 μs |  0.099 μs |    1 |   0.0916 |       - |    1560 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    14.62 μs |  0.031 μs |  0.026 μs |    2 |   0.2747 |       - |    4424 B |
| Count_RawAdo                                |    15.14 μs |  0.198 μs |  0.185 μs |    3 |   0.0458 |       - |     808 B |
| Count_Dapper                                |    15.37 μs |  0.084 μs |  0.079 μs |    3 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    15.87 μs |  0.093 μs |  0.082 μs |    4 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    16.71 μs |  0.194 μs |  0.181 μs |    5 |   0.3662 |       - |    6024 B |
| Query_Simple_nORM                           |    21.45 μs |  0.220 μs |  0.206 μs |    6 |   0.4272 |       - |    6816 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    21.94 μs |  0.218 μs |  0.204 μs |    7 |   0.5188 |       - |    8264 B |
| Query_Simple_RawAdo                         |    23.05 μs |  0.121 μs |  0.113 μs |    8 |   0.3967 |       - |    6248 B |
| Query_Simple_Dapper                         |    27.22 μs |  0.210 μs |  0.196 μs |    9 |   0.4272 |       - |    6920 B |
| Count_EfCore                                |    27.62 μs |  0.193 μs |  0.181 μs |   10 |   0.2441 |       - |    4744 B |
| Query_Simple_EfCore                         |    28.81 μs |  0.262 μs |  0.245 μs |   11 |   0.7324 |       - |   12832 B |
| Query_Join_nORM_Compiled                    |    39.75 μs |  0.127 μs |  0.106 μs |   12 |   0.9155 |       - |   14544 B |
| Query_Join_Dapper                           |    41.09 μs |  0.230 μs |  0.204 μs |   13 |   0.9155 |       - |   14776 B |
| Insert_Single_nORM                          |    42.92 μs |  0.829 μs |  0.955 μs |   14 |   0.0610 |       - |    1480 B |
| Insert_Single_RawAdo                        |    48.73 μs |  0.959 μs |  1.142 μs |   15 |   0.1221 |       - |    2240 B |
| Insert_Single_Dapper                        |    49.33 μs |  0.913 μs |  0.854 μs |   15 |   0.3052 |       - |    4816 B |
| Query_Join_nORM                             |    51.21 μs |  0.621 μs |  0.581 μs |   16 |   1.2207 |       - |   22936 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    56.60 μs |  0.333 μs |  0.278 μs |   17 |   0.9155 |       - |   14648 B |
| Insert_Single_EfCore                        |    59.39 μs |  0.807 μs |  0.755 μs |   18 |   0.9766 |       - |   15840 B |
| Query_Complex_nORM                          |    67.37 μs |  0.830 μs |  0.777 μs |   19 |   0.9766 |       - |   18256 B |
| Query_Complex_EfCore                        |    79.80 μs |  0.968 μs |  0.858 μs |   20 |   1.4648 |       - |   23656 B |
| Query_Join_EfCore                           |    84.29 μs |  1.639 μs |  1.533 μs |   21 |   3.4180 |       - |   57162 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    90.29 μs |  0.747 μs |  0.699 μs |   22 |   0.4883 |       - |    8488 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    90.41 μs |  0.676 μs |  0.599 μs |   22 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    90.49 μs |  0.682 μs |  0.604 μs |   22 |   0.6104 |       - |   10312 B |
| Query_Complex_RawAdo                        |   102.91 μs |  1.621 μs |  1.516 μs |   23 |   0.6104 |       - |   11016 B |
| Query_Complex_Dapper                        |   110.67 μs |  1.361 μs |  1.207 μs |   24 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   486.09 μs |  6.312 μs |  5.904 μs |   25 |   7.8125 |       - |  132009 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   490.97 μs |  7.027 μs |  6.573 μs |   25 |   8.7891 |       - |  149649 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   520.70 μs |  8.528 μs |  7.977 μs |   26 |   6.8359 |       - |  118456 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   686.12 μs | 10.248 μs | 11.802 μs |   27 |  25.3906 |  1.9531 |  419161 B |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |   933.83 μs |  8.959 μs |  8.381 μs |   28 |  19.5313 |  1.9531 |  319940 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 1,854.61 μs | 24.492 μs | 21.712 μs |   29 |  78.1250 | 31.2500 | 1331342 B |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; | 1,879.97 μs | 28.450 μs | 26.612 μs |   29 |  78.1250 | 31.2500 | 1332485 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,286.58 μs | 84.125 μs | 96.878 μs |   30 |   7.8125 |       - |  158407 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 7,689.58 μs | 80.221 μs | 75.038 μs |   31 | 250.0000 | 31.2500 | 4234574 B |

Benchmarks with issues:
  OrmBenchmarks.'BulkInsert Naive - Dapper per row': DefaultJob
