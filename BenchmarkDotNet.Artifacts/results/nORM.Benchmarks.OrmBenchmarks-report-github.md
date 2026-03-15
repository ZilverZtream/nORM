```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean        | Error      | StdDev     | Median      | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |------------:|-----------:|-----------:|------------:|-----:|---------:|--------:|----------:|
| &#39;BulkInsert Naive - Dapper per row&#39;         |          NA |         NA |         NA |          NA |    ? |       NA |      NA |        NA |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; |          NA |         NA |         NA |          NA |    ? |       NA |      NA |        NA |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |          NA |         NA |         NA |          NA |    ? |       NA |      NA |        NA |
| Count_nORM                                  |    12.96 μs |   0.110 μs |   0.098 μs |    12.98 μs |    1 |   0.0916 |       - |    1560 B |
| Count_RawAdo                                |    14.79 μs |   0.108 μs |   0.101 μs |    14.80 μs |    2 |   0.0458 |       - |     808 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    15.14 μs |   0.261 μs |   0.244 μs |    15.07 μs |    3 |   0.2747 |       - |    4424 B |
| Count_Dapper                                |    15.21 μs |   0.105 μs |   0.098 μs |    15.22 μs |    3 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    15.44 μs |   0.122 μs |   0.114 μs |    15.41 μs |    3 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    15.57 μs |   0.182 μs |   0.171 μs |    15.60 μs |    3 |   0.3662 |       - |    6024 B |
| Query_Simple_nORM                           |    20.31 μs |   0.077 μs |   0.069 μs |    20.30 μs |    4 |   0.4272 |       - |    6816 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    20.83 μs |   0.290 μs |   0.271 μs |    20.89 μs |    5 |   0.5188 |       - |    8264 B |
| Query_Simple_RawAdo                         |    22.84 μs |   0.385 μs |   0.321 μs |    22.75 μs |    6 |   0.3967 |       - |    6248 B |
| Count_EfCore                                |    26.89 μs |   0.162 μs |   0.151 μs |    26.92 μs |    7 |   0.2441 |       - |    4744 B |
| Query_Simple_EfCore                         |    27.02 μs |   0.375 μs |   0.332 μs |    27.00 μs |    7 |   0.7324 |       - |   12976 B |
| Query_Simple_Dapper                         |    27.24 μs |   0.294 μs |   0.275 μs |    27.23 μs |    7 |   0.4272 |       - |    6920 B |
| Query_Join_nORM_Compiled                    |    38.60 μs |   0.310 μs |   0.290 μs |    38.56 μs |    8 |   0.9155 |       - |   14544 B |
| Query_Join_Dapper                           |    41.44 μs |   0.217 μs |   0.203 μs |    41.36 μs |    9 |   0.9155 |       - |   14776 B |
| Insert_Single_nORM                          |    43.24 μs |   0.854 μs |   0.984 μs |    43.23 μs |   10 |   0.0610 |       - |    1480 B |
| Insert_Single_Dapper                        |    50.16 μs |   0.536 μs |   0.448 μs |    50.28 μs |   11 |   0.3052 |       - |    4816 B |
| Query_Join_nORM                             |    52.57 μs |   0.557 μs |   0.521 μs |    52.57 μs |   12 |   1.3428 |       - |   22632 B |
| Insert_Single_RawAdo                        |    52.63 μs |   2.680 μs |   7.061 μs |    50.41 μs |   13 |   0.1221 |       - |    2240 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    57.27 μs |   0.451 μs |   0.422 μs |    57.40 μs |   14 |   0.9155 |       - |   14648 B |
| Insert_Single_EfCore                        |    60.22 μs |   0.943 μs |   0.788 μs |    59.93 μs |   15 |   0.9766 |       - |   15840 B |
| Query_Complex_nORM                          |    62.53 μs |   1.042 μs |   0.975 μs |    62.55 μs |   16 |   0.9766 |       - |   18248 B |
| Query_Complex_EfCore                        |    74.71 μs |   0.665 μs |   0.622 μs |    74.70 μs |   17 |   1.4648 |       - |   23352 B |
| Query_Join_EfCore                           |    81.70 μs |   1.563 μs |   1.462 μs |    81.61 μs |   18 |   3.4180 |       - |   57329 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    88.06 μs |   0.557 μs |   0.521 μs |    88.06 μs |   19 |   0.4883 |       - |    8504 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    88.93 μs |   0.398 μs |   0.372 μs |    88.99 μs |   19 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    90.39 μs |   0.686 μs |   0.641 μs |    90.33 μs |   20 |   0.6104 |       - |   10304 B |
| Query_Complex_RawAdo                        |   102.59 μs |   0.759 μs |   0.710 μs |   102.58 μs |   21 |   0.6104 |       - |   11016 B |
| Query_Complex_Dapper                        |   108.88 μs |   0.540 μs |   0.478 μs |   108.76 μs |   22 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   645.17 μs |  12.206 μs |  14.531 μs |   644.62 μs |   23 |   7.8125 |       - |  132009 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   655.29 μs |  12.421 μs |  13.291 μs |   655.42 μs |   23 |   8.7891 |       - |  149649 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   681.61 μs |  15.838 μs |  46.451 μs |   693.56 μs |   24 |   6.8359 |       - |  118457 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   824.97 μs |  16.357 μs |  25.466 μs |   823.93 μs |   25 |  25.3906 |  1.9531 |  419161 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 1,946.50 μs |  38.892 μs |  70.130 μs | 1,965.28 μs |   26 |  78.1250 | 31.2500 | 1331342 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,332.27 μs |  85.342 μs | 199.485 μs | 4,327.73 μs |   27 |   7.8125 |       - |  158405 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 7,318.53 μs | 140.317 μs | 131.252 μs | 7,335.10 μs |   28 | 250.0000 | 31.2500 | 4234576 B |

Benchmarks with issues:
  OrmBenchmarks.'BulkInsert Naive - Dapper per row': DefaultJob
  OrmBenchmarks.'BulkInsert Batched - EF SaveChanges in Tx': DefaultJob
  OrmBenchmarks.'BulkInsert Batched - nORM Tx + per row': DefaultJob
