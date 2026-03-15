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
| Count_nORM                                  |    13.08 μs |   0.121 μs |   0.101 μs |    1 |   0.0916 |       - |    1560 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    14.69 μs |   0.065 μs |   0.061 μs |    2 |   0.2747 |       - |    4424 B |
| Count_RawAdo                                |    14.77 μs |   0.108 μs |   0.101 μs |    2 |   0.0458 |       - |     808 B |
| Count_Dapper                                |    15.36 μs |   0.171 μs |   0.160 μs |    3 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    15.90 μs |   0.160 μs |   0.150 μs |    4 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    16.87 μs |   0.082 μs |   0.072 μs |    5 |   0.3662 |       - |    6024 B |
| Query_Simple_nORM                           |    20.87 μs |   0.266 μs |   0.236 μs |    6 |   0.4272 |       - |    6816 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    21.04 μs |   0.129 μs |   0.121 μs |    6 |   0.5188 |       - |    8264 B |
| Query_Simple_RawAdo                         |    22.73 μs |   0.151 μs |   0.141 μs |    7 |   0.3967 |       - |    6248 B |
| Count_EfCore                                |    27.18 μs |   0.194 μs |   0.182 μs |    8 |   0.2441 |       - |    4744 B |
| Query_Simple_Dapper                         |    27.96 μs |   0.248 μs |   0.220 μs |    9 |   0.4272 |       - |    6920 B |
| Query_Join_nORM_Compiled                    |    38.80 μs |   0.186 μs |   0.174 μs |   10 |   0.9155 |       - |   14544 B |
| Query_Join_Dapper                           |    40.69 μs |   0.175 μs |   0.155 μs |   11 |   0.9155 |       - |   14776 B |
| Insert_Single_nORM                          |    42.08 μs |   0.795 μs |   0.744 μs |   12 |   0.0610 |       - |    1480 B |
| Query_Join_nORM                             |    51.60 μs |   0.496 μs |   0.464 μs |   13 |   1.4648 |       - |   22984 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    56.46 μs |   0.129 μs |   0.115 μs |   14 |   0.9155 |       - |   14648 B |
| Insert_Single_EfCore                        |    61.05 μs |   1.004 μs |   0.939 μs |   15 |   0.9766 |       - |   15840 B |
| Query_Complex_nORM                          |    62.56 μs |   0.743 μs |   0.659 μs |   16 |   0.9766 |       - |   18288 B |
| Query_Complex_EfCore                        |    76.51 μs |   0.415 μs |   0.388 μs |   17 |   1.4648 |       - |   23456 B |
| Query_Join_EfCore                           |    85.54 μs |   1.280 μs |   1.198 μs |   18 |   3.4180 |       - |   56921 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    89.15 μs |   0.857 μs |   0.802 μs |   19 |   0.4883 |       - |    8504 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    90.22 μs |   0.682 μs |   0.638 μs |   19 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    90.35 μs |   0.559 μs |   0.467 μs |   19 |   0.6104 |       - |   10304 B |
| Query_Complex_RawAdo                        |   102.10 μs |   1.003 μs |   0.939 μs |   20 |   0.6104 |       - |   11008 B |
| Query_Complex_Dapper                        |   109.72 μs |   0.910 μs |   0.852 μs |   21 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   479.30 μs |   6.615 μs |   6.188 μs |   22 |   7.8125 |       - |  132009 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   492.82 μs |   9.534 μs |  10.979 μs |   23 |   8.7891 |       - |  149649 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   514.48 μs |   9.696 μs |  10.777 μs |   24 |   7.3242 |       - |  118456 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   644.55 μs |   3.884 μs |   3.443 μs |   25 |  26.3672 |  1.9531 |  419161 B |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |   905.20 μs |   8.918 μs |   8.341 μs |   26 |  19.5313 |  1.9531 |  319940 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 1,790.37 μs |  34.154 μs |  35.074 μs |   27 |  78.1250 | 31.2500 | 1331342 B |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; | 1,881.37 μs |  36.836 μs |  34.457 μs |   28 |  78.1250 | 31.2500 | 1332486 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,228.92 μs |  80.030 μs | 135.897 μs |   29 |   7.8125 |       - |  158405 B |
| &#39;BulkInsert Naive - Dapper per row&#39;         | 4,964.14 μs |  53.746 μs |  47.644 μs |   30 |  31.2500 |       - |  493531 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 7,668.50 μs | 129.947 μs | 168.967 μs |   31 | 250.0000 | 31.2500 | 4234572 B |

Benchmarks with issues:
  OrmBenchmarks.Insert_Single_Dapper: DefaultJob
  OrmBenchmarks.Insert_Single_RawAdo: DefaultJob
  OrmBenchmarks.Query_Simple_EfCore: DefaultJob
