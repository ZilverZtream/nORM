```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean        | Error     | StdDev     | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |------------:|----------:|-----------:|-----:|---------:|--------:|----------:|
| &#39;BulkInsert Naive - Dapper per row&#39;         |          NA |        NA |         NA |    ? |       NA |      NA |        NA |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; |          NA |        NA |         NA |    ? |       NA |      NA |        NA |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |          NA |        NA |         NA |    ? |       NA |      NA |        NA |
| Count_nORM                                  |    13.39 μs |  0.111 μs |   0.104 μs |    1 |   0.0916 |       - |    1672 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    14.87 μs |  0.172 μs |   0.161 μs |    2 |   0.2747 |       - |    4424 B |
| Count_RawAdo                                |    15.02 μs |  0.112 μs |   0.105 μs |    2 |   0.0458 |       - |     808 B |
| Count_Dapper                                |    15.50 μs |  0.095 μs |   0.089 μs |    3 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    15.75 μs |  0.107 μs |   0.100 μs |    4 |   0.3662 |       - |    6016 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    15.79 μs |  0.096 μs |   0.085 μs |    4 |   0.3662 |       - |    6024 B |
| Query_Simple_nORM                           |    20.63 μs |  0.234 μs |   0.219 μs |    5 |   0.4272 |       - |    6808 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    21.10 μs |  0.176 μs |   0.164 μs |    6 |   0.5188 |       - |    8264 B |
| Query_Simple_RawAdo                         |    23.24 μs |  0.248 μs |   0.232 μs |    7 |   0.3967 |       - |    6248 B |
| Count_EfCore                                |    27.31 μs |  0.174 μs |   0.154 μs |    8 |   0.2441 |       - |    4744 B |
| Query_Simple_EfCore                         |    27.35 μs |  0.522 μs |   0.513 μs |    8 |   0.7324 |       - |   12832 B |
| Query_Simple_Dapper                         |    27.59 μs |  0.288 μs |   0.255 μs |    8 |   0.4272 |       - |    6920 B |
| Query_Join_nORM_Compiled                    |    39.10 μs |  0.268 μs |   0.238 μs |    9 |   0.9155 |       - |   14544 B |
| Query_Join_Dapper                           |    42.00 μs |  0.375 μs |   0.351 μs |   10 |   0.9155 |       - |   14776 B |
| Insert_Single_nORM                          |    43.05 μs |  0.802 μs |   0.787 μs |   11 |   0.0610 |       - |    1480 B |
| Query_Join_nORM                             |    52.17 μs |  0.573 μs |   0.536 μs |   12 |   1.4648 |       - |   22984 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    57.53 μs |  0.333 μs |   0.295 μs |   13 |   0.9155 |       - |   14648 B |
| Insert_Single_EfCore                        |    59.30 μs |  0.807 μs |   0.755 μs |   14 |   0.9766 |       - |   15840 B |
| Query_Complex_nORM                          |    62.72 μs |  0.768 μs |   0.719 μs |   15 |   0.9766 |       - |   18256 B |
| Query_Complex_EfCore                        |    74.39 μs |  1.181 μs |   1.105 μs |   16 |   1.4648 |       - |   23712 B |
| Query_Join_EfCore                           |    84.85 μs |  0.913 μs |   0.854 μs |   17 |   3.4180 |       - |   56921 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    88.60 μs |  0.650 μs |   0.608 μs |   18 |   0.4883 |       - |    8504 B |
| Insert_Single_RawAdo                        |    89.39 μs |  1.770 μs |   2.807 μs |   18 |   0.1221 |       - |    2240 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    90.18 μs |  0.671 μs |   0.627 μs |   18 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    90.79 μs |  0.421 μs |   0.394 μs |   18 |   0.6104 |       - |   10312 B |
| Insert_Single_Dapper                        |    90.95 μs |  1.759 μs |   2.523 μs |   18 |   0.2441 |       - |    4816 B |
| Query_Complex_RawAdo                        |   103.15 μs |  0.504 μs |   0.472 μs |   19 |   0.6104 |       - |   11008 B |
| Query_Complex_Dapper                        |   108.69 μs |  0.714 μs |   0.667 μs |   20 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   648.27 μs | 12.421 μs |  12.756 μs |   21 |   7.8125 |       - |  132009 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   677.16 μs | 13.345 μs |  13.704 μs |   22 |   6.8359 |       - |  118457 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   680.78 μs | 11.964 μs |  11.191 μs |   22 |   8.7891 |       - |  149649 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   835.51 μs | 16.505 μs |  28.028 μs |   23 |  25.3906 |  1.9531 |  419161 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 2,057.98 μs | 40.973 μs | 104.289 μs |   24 |  78.1250 | 31.2500 | 1331342 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,181.59 μs | 42.417 μs |  39.677 μs |   25 |   7.8125 |       - |  158407 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 7,683.29 μs | 86.404 μs |  80.823 μs |   26 | 250.0000 | 31.2500 | 4234570 B |

Benchmarks with issues:
  OrmBenchmarks.'BulkInsert Naive - Dapper per row': DefaultJob
  OrmBenchmarks.'BulkInsert Batched - EF SaveChanges in Tx': DefaultJob
  OrmBenchmarks.'BulkInsert Batched - nORM Tx + per row': DefaultJob
