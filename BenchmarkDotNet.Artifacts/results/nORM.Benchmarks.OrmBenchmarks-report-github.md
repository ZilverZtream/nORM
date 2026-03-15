```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean        | Error      | StdDev     | Median      | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |------------:|-----------:|-----------:|------------:|-----:|---------:|--------:|----------:|
| Insert_Single_Dapper                        |          NA |         NA |         NA |          NA |    ? |       NA |      NA |        NA |
| &#39;BulkInsert Naive - Dapper per row&#39;         |          NA |         NA |         NA |          NA |    ? |       NA |      NA |        NA |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; |          NA |         NA |         NA |          NA |    ? |       NA |      NA |        NA |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |          NA |         NA |         NA |          NA |    ? |       NA |      NA |        NA |
| Count_nORM                                  |    13.43 μs |   0.057 μs |   0.054 μs |    13.44 μs |    1 |   0.0916 |       - |    1560 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    15.09 μs |   0.112 μs |   0.105 μs |    15.08 μs |    2 |   0.2747 |       - |    4424 B |
| Count_RawAdo                                |    15.29 μs |   0.290 μs |   0.271 μs |    15.31 μs |    2 |   0.0305 |       - |     808 B |
| Count_Dapper                                |    15.88 μs |   0.197 μs |   0.184 μs |    15.90 μs |    3 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    16.02 μs |   0.312 μs |   0.291 μs |    15.84 μs |    3 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    18.18 μs |   0.070 μs |   0.062 μs |    18.18 μs |    4 |   0.3662 |       - |    6024 B |
| Query_Simple_nORM                           |    21.03 μs |   0.128 μs |   0.113 μs |    21.03 μs |    5 |   0.4272 |       - |    6816 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    21.36 μs |   0.345 μs |   0.288 μs |    21.44 μs |    5 |   0.5188 |       - |    8264 B |
| Query_Simple_RawAdo                         |    23.13 μs |   0.185 μs |   0.164 μs |    23.07 μs |    6 |   0.3967 |       - |    6248 B |
| Query_Simple_Dapper                         |    27.11 μs |   0.211 μs |   0.198 μs |    27.15 μs |    7 |   0.4272 |       - |    6920 B |
| Query_Simple_EfCore                         |    27.58 μs |   0.251 μs |   0.223 μs |    27.61 μs |    8 |   0.7324 |       - |   12832 B |
| Count_EfCore                                |    28.27 μs |   0.194 μs |   0.162 μs |    28.32 μs |    9 |   0.2441 |       - |    4744 B |
| Query_Join_nORM_Compiled                    |    39.93 μs |   0.743 μs |   0.695 μs |    40.04 μs |   10 |   0.9155 |       - |   14544 B |
| Query_Join_Dapper                           |    41.86 μs |   0.698 μs |   0.653 μs |    41.80 μs |   11 |   0.9155 |       - |   14776 B |
| Insert_Single_nORM                          |    43.56 μs |   0.867 μs |   1.564 μs |    43.47 μs |   12 |   0.0610 |       - |    1480 B |
| Insert_Single_RawAdo                        |    52.31 μs |   1.321 μs |   3.571 μs |    51.06 μs |   13 |   0.1221 |       - |    2240 B |
| Query_Join_nORM                             |    54.39 μs |   0.359 μs |   0.318 μs |    54.45 μs |   14 |   1.2207 |       - |   22632 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    58.70 μs |   0.090 μs |   0.079 μs |    58.69 μs |   15 |   0.9155 |       - |   14648 B |
| Insert_Single_EfCore                        |    63.33 μs |   1.266 μs |   2.283 μs |    63.16 μs |   16 |   0.9766 |       - |   15840 B |
| Query_Complex_nORM                          |    63.54 μs |   0.609 μs |   0.569 μs |    63.45 μs |   16 |   0.9766 |       - |   18296 B |
| Query_Complex_EfCore                        |    74.63 μs |   0.702 μs |   0.657 μs |    74.81 μs |   17 |   1.4648 |       - |   23672 B |
| Query_Join_EfCore                           |    86.45 μs |   1.703 μs |   1.593 μs |    86.94 μs |   18 |   3.4180 |       - |   56922 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    91.39 μs |   0.673 μs |   0.525 μs |    91.44 μs |   19 |   0.4883 |       - |    8496 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    93.27 μs |   0.578 μs |   0.483 μs |    93.38 μs |   20 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    93.87 μs |   0.799 μs |   0.667 μs |    94.08 μs |   20 |   0.6104 |       - |   10312 B |
| Query_Complex_RawAdo                        |   106.41 μs |   1.406 μs |   1.315 μs |   106.25 μs |   21 |   0.6104 |       - |   11016 B |
| Query_Complex_Dapper                        |   109.49 μs |   0.486 μs |   0.431 μs |   109.45 μs |   22 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   480.16 μs |   5.334 μs |   4.454 μs |   480.33 μs |   23 |   7.8125 |       - |  132009 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   484.22 μs |   4.606 μs |   4.083 μs |   484.39 μs |   23 |   8.7891 |       - |  149649 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   506.89 μs |   7.233 μs |   6.040 μs |   507.78 μs |   24 |   6.8359 |       - |  118457 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   671.51 μs |   8.513 μs |   7.963 μs |   672.48 μs |   25 |  26.3672 |  1.9531 |  419160 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 1,886.52 μs |  22.508 μs |  21.054 μs | 1,888.29 μs |   26 |  78.1250 | 31.2500 | 1331341 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,635.86 μs |  92.512 μs | 216.243 μs | 4,570.41 μs |   27 |   7.8125 |       - |  158405 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 8,045.43 μs | 117.384 μs | 104.058 μs | 8,057.28 μs |   28 | 250.0000 | 31.2500 | 4234574 B |

Benchmarks with issues:
  OrmBenchmarks.Insert_Single_Dapper: DefaultJob
  OrmBenchmarks.'BulkInsert Naive - Dapper per row': DefaultJob
  OrmBenchmarks.'BulkInsert Batched - EF SaveChanges in Tx': DefaultJob
  OrmBenchmarks.'BulkInsert Batched - nORM Tx + per row': DefaultJob
