```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean        | Error      | StdDev     | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |------------:|-----------:|-----------:|-----:|---------:|--------:|----------:|
| &#39;BulkInsert Naive - Dapper per row&#39;         |          NA |         NA |         NA |    ? |       NA |      NA |        NA |
| Count_nORM                                  |    13.14 μs |   0.129 μs |   0.121 μs |    1 |   0.0916 |       - |    1560 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    14.74 μs |   0.070 μs |   0.066 μs |    2 |   0.2747 |       - |    4424 B |
| Count_RawAdo                                |    14.84 μs |   0.166 μs |   0.155 μs |    2 |   0.0458 |       - |     808 B |
| Count_Dapper                                |    15.53 μs |   0.146 μs |   0.129 μs |    3 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    15.73 μs |   0.175 μs |   0.163 μs |    3 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    15.80 μs |   0.174 μs |   0.162 μs |    3 |   0.3662 |       - |    6024 B |
| Query_Simple_nORM                           |    20.18 μs |   0.099 μs |   0.088 μs |    4 |   0.4272 |       - |    6816 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    20.95 μs |   0.176 μs |   0.164 μs |    5 |   0.5188 |       - |    8264 B |
| Query_Simple_RawAdo                         |    23.18 μs |   0.208 μs |   0.195 μs |    6 |   0.3967 |       - |    6248 B |
| Query_Simple_Dapper                         |    26.17 μs |   0.169 μs |   0.158 μs |    7 |   0.4272 |       - |    6920 B |
| Query_Simple_EfCore                         |    26.68 μs |   0.481 μs |   0.450 μs |    7 |   0.7324 |       - |   12832 B |
| Count_EfCore                                |    27.67 μs |   0.279 μs |   0.261 μs |    8 |   0.2441 |       - |    4744 B |
| Query_Join_nORM_Compiled                    |    38.70 μs |   0.180 μs |   0.160 μs |    9 |   0.9155 |       - |   14544 B |
| Insert_Single_nORM                          |    40.84 μs |   0.814 μs |   0.905 μs |   10 |   0.0610 |       - |    1480 B |
| Query_Join_Dapper                           |    41.03 μs |   0.298 μs |   0.249 μs |   10 |   0.9155 |       - |   14776 B |
| Insert_Single_Dapper                        |    50.34 μs |   1.002 μs |   1.338 μs |   11 |   0.2441 |       - |    4816 B |
| Query_Join_nORM                             |    50.64 μs |   0.506 μs |   0.449 μs |   11 |   1.3428 |       - |   22744 B |
| Insert_Single_RawAdo                        |    52.93 μs |   1.002 μs |   0.837 μs |   12 |   0.1221 |       - |    2240 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    56.72 μs |   0.281 μs |   0.263 μs |   13 |   0.9155 |       - |   14648 B |
| Query_Complex_nORM                          |    62.39 μs |   0.513 μs |   0.480 μs |   14 |   0.9766 |       - |   18136 B |
| Insert_Single_EfCore                        |    62.45 μs |   1.041 μs |   0.974 μs |   14 |   0.9766 |       - |   15840 B |
| Query_Complex_EfCore                        |    74.19 μs |   0.762 μs |   0.713 μs |   15 |   1.4648 |       - |   23616 B |
| Query_Join_EfCore                           |    82.75 μs |   1.631 μs |   1.813 μs |   16 |   3.4180 |       - |   57082 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    88.41 μs |   0.538 μs |   0.503 μs |   17 |   0.4883 |       - |    8504 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    89.20 μs |   0.715 μs |   0.669 μs |   17 |   0.6104 |       - |   10304 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    89.68 μs |   0.801 μs |   0.749 μs |   17 |   0.6104 |       - |   10312 B |
| Query_Complex_RawAdo                        |   103.69 μs |   1.513 μs |   1.415 μs |   18 |   0.6104 |       - |   11016 B |
| Query_Complex_Dapper                        |   109.31 μs |   0.738 μs |   0.691 μs |   19 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   470.01 μs |   7.424 μs |   6.199 μs |   20 |   7.8125 |       - |  132009 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   488.61 μs |   4.354 μs |   4.072 μs |   21 |   7.3242 |       - |  118456 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   504.59 μs |   8.934 μs |   8.357 μs |   22 |   7.8125 |       - |  149650 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   666.67 μs |  11.640 μs |  10.318 μs |   23 |  25.3906 |  1.9531 |  419161 B |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |   899.26 μs |   6.219 μs |   5.513 μs |   24 |  19.5313 |  1.9531 |  319939 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 1,850.84 μs |  36.870 μs |  34.489 μs |   25 |  78.1250 | 31.2500 | 1331341 B |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; | 1,867.56 μs |  36.017 μs |  36.987 μs |   25 |  78.1250 | 31.2500 | 1332485 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,279.62 μs |  84.426 μs | 154.378 μs |   26 |   7.8125 |       - |  158406 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 7,733.77 μs | 123.297 μs | 109.300 μs |   27 | 250.0000 | 31.2500 | 4234573 B |

Benchmarks with issues:
  OrmBenchmarks.'BulkInsert Naive - Dapper per row': DefaultJob
