```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean        | Error      | StdDev     | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |------------:|-----------:|-----------:|-----:|---------:|--------:|----------:|
| Insert_Single_Dapper                        |          NA |         NA |         NA |    ? |       NA |      NA |        NA |
| Insert_Single_RawAdo                        |          NA |         NA |         NA |    ? |       NA |      NA |        NA |
| Query_Simple_EfCore                         |          NA |         NA |         NA |    ? |       NA |      NA |        NA |
| Count_nORM                                  |    13.19 μs |   0.156 μs |   0.139 μs |    1 |   0.0916 |       - |    1560 B |
| Count_RawAdo                                |    14.94 μs |   0.083 μs |   0.069 μs |    2 |   0.0458 |       - |     808 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    14.97 μs |   0.090 μs |   0.084 μs |    2 |   0.2747 |       - |    4424 B |
| Count_Dapper                                |    15.47 μs |   0.190 μs |   0.178 μs |    3 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    16.19 μs |   0.103 μs |   0.097 μs |    4 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    16.40 μs |   0.326 μs |   0.423 μs |    4 |   0.3662 |       - |    6024 B |
| Query_Simple_nORM                           |    20.63 μs |   0.271 μs |   0.253 μs |    5 |   0.4272 |       - |    6816 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    21.36 μs |   0.162 μs |   0.152 μs |    6 |   0.5188 |       - |    8264 B |
| Query_Simple_RawAdo                         |    23.26 μs |   0.243 μs |   0.227 μs |    7 |   0.3967 |       - |    6248 B |
| Query_Simple_Dapper                         |    26.91 μs |   0.204 μs |   0.191 μs |    8 |   0.4272 |       - |    6920 B |
| Count_EfCore                                |    27.19 μs |   0.272 μs |   0.254 μs |    8 |   0.2441 |       - |    4744 B |
| Query_Join_nORM_Compiled                    |    39.37 μs |   0.393 μs |   0.368 μs |    9 |   0.9155 |       - |   14544 B |
| Query_Join_Dapper                           |    41.75 μs |   0.410 μs |   0.384 μs |   10 |   0.9155 |       - |   14776 B |
| Insert_Single_nORM                          |    44.08 μs |   0.877 μs |   0.778 μs |   11 |   0.0610 |       - |    1480 B |
| Query_Join_nORM                             |    53.07 μs |   1.002 μs |   0.782 μs |   12 |   1.2207 |       - |   22632 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    57.71 μs |   0.679 μs |   0.754 μs |   13 |   0.8545 |       - |   14640 B |
| Insert_Single_EfCore                        |    60.11 μs |   1.068 μs |   0.999 μs |   14 |   0.9766 |       - |   15840 B |
| Query_Complex_nORM                          |    62.32 μs |   0.550 μs |   0.459 μs |   15 |   0.9766 |       - |   18136 B |
| Query_Complex_EfCore                        |    75.01 μs |   0.861 μs |   0.805 μs |   16 |   1.4648 |       - |   23464 B |
| Query_Join_EfCore                           |    85.59 μs |   0.852 μs |   0.755 μs |   17 |   3.4180 |       - |   56921 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    89.94 μs |   1.273 μs |   1.191 μs |   18 |   0.4883 |       - |    8504 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    91.38 μs |   0.848 μs |   0.793 μs |   18 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    92.21 μs |   1.375 μs |   1.219 μs |   18 |   0.6104 |       - |   10312 B |
| Query_Complex_RawAdo                        |   103.34 μs |   1.322 μs |   1.104 μs |   19 |   0.6104 |       - |   11016 B |
| Query_Complex_Dapper                        |   109.53 μs |   1.484 μs |   1.388 μs |   20 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   652.39 μs |  11.720 μs |  10.963 μs |   21 |   7.8125 |       - |  132009 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   665.50 μs |  10.758 μs |  10.063 μs |   21 |   8.7891 |       - |  149648 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   674.15 μs |  12.727 μs |  14.146 μs |   21 |   6.8359 |       - |  118457 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   816.21 μs |  15.706 μs |  18.696 μs |   22 |  25.3906 |  1.9531 |  419161 B |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    | 1,058.14 μs |  19.538 μs |  20.905 μs |   23 |  19.5313 |  1.9531 |  319940 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 2,031.58 μs |  40.199 μs |  64.914 μs |   24 |  78.1250 | 31.2500 | 1331342 B |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; | 2,105.57 μs |  41.812 μs |  89.106 μs |   25 |  78.1250 | 31.2500 | 1332485 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,340.20 μs |  63.600 μs |  59.491 μs |   26 |   7.8125 |       - |  158406 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 7,642.89 μs |  77.187 μs |  72.201 μs |   27 | 250.0000 | 31.2500 | 4234576 B |
| &#39;BulkInsert Naive - Dapper per row&#39;         | 9,269.42 μs | 183.317 μs | 211.109 μs |   28 |  31.2500 |       - |  493542 B |

Benchmarks with issues:
  OrmBenchmarks.Insert_Single_Dapper: DefaultJob
  OrmBenchmarks.Insert_Single_RawAdo: DefaultJob
  OrmBenchmarks.Query_Simple_EfCore: DefaultJob
