```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean        | Error      | StdDev     | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |------------:|-----------:|-----------:|-----:|---------:|--------:|----------:|
| Count_nORM                                  |    13.18 μs |   0.101 μs |   0.095 μs |    1 |   0.0916 |       - |    1560 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    14.59 μs |   0.065 μs |   0.061 μs |    2 |   0.2747 |       - |    4424 B |
| Count_RawAdo                                |    14.78 μs |   0.083 μs |   0.077 μs |    3 |   0.0458 |       - |     808 B |
| Count_Dapper                                |    15.42 μs |   0.117 μs |   0.109 μs |    4 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    15.51 μs |   0.081 μs |   0.076 μs |    4 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    15.57 μs |   0.150 μs |   0.125 μs |    4 |   0.3662 |       - |    6024 B |
| Query_Simple_nORM                           |    20.67 μs |   0.233 μs |   0.218 μs |    5 |   0.4272 |       - |    6816 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    21.30 μs |   0.316 μs |   0.280 μs |    6 |   0.5188 |       - |    8264 B |
| Query_Simple_RawAdo                         |    23.97 μs |   0.359 μs |   0.336 μs |    7 |   0.3967 |       - |    6248 B |
| Count_EfCore                                |    27.00 μs |   0.217 μs |   0.203 μs |    8 |   0.2441 |       - |    4744 B |
| Query_Simple_EfCore                         |    27.20 μs |   0.495 μs |   0.463 μs |    8 |   0.7324 |       - |   13136 B |
| Query_Simple_Dapper                         |    27.35 μs |   0.347 μs |   0.325 μs |    8 |   0.4272 |       - |    6920 B |
| Query_Join_nORM_Compiled                    |    38.99 μs |   0.229 μs |   0.203 μs |    9 |   0.9155 |       - |   14544 B |
| Query_Join_Dapper                           |    42.82 μs |   0.455 μs |   0.426 μs |   10 |   0.9155 |       - |   14776 B |
| Insert_Single_nORM                          |    45.22 μs |   0.560 μs |   0.523 μs |   11 |   0.0610 |       - |    1480 B |
| Insert_Single_RawAdo                        |    49.38 μs |   0.893 μs |   0.697 μs |   12 |   0.1221 |       - |    2240 B |
| Insert_Single_Dapper                        |    51.02 μs |   0.762 μs |   0.712 μs |   13 |   0.3052 |       - |    4816 B |
| Query_Join_nORM                             |    51.23 μs |   0.596 μs |   0.557 μs |   13 |   1.3428 |       - |   22632 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    56.79 μs |   0.415 μs |   0.388 μs |   14 |   0.9155 |       - |   14648 B |
| Query_Complex_nORM                          |    62.97 μs |   0.717 μs |   0.670 μs |   15 |   0.9766 |       - |   18128 B |
| Insert_Single_EfCore                        |    63.24 μs |   1.264 μs |   2.005 μs |   15 |   0.9766 |       - |   15840 B |
| Query_Complex_EfCore                        |    74.29 μs |   0.747 μs |   0.699 μs |   16 |   1.4648 |       - |   23648 B |
| Query_Join_EfCore                           |    83.18 μs |   0.959 μs |   0.897 μs |   17 |   3.4180 |       - |   57002 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    87.97 μs |   0.652 μs |   0.610 μs |   18 |   0.4883 |       - |    8504 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    90.23 μs |   0.871 μs |   0.815 μs |   19 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    90.52 μs |   0.241 μs |   0.188 μs |   19 |   0.6104 |       - |   10312 B |
| Query_Complex_RawAdo                        |   102.24 μs |   0.514 μs |   0.481 μs |   20 |   0.6104 |       - |   11016 B |
| Query_Complex_Dapper                        |   108.89 μs |   0.841 μs |   0.787 μs |   21 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   468.83 μs |   4.130 μs |   3.449 μs |   22 |   7.8125 |       - |  132008 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   486.16 μs |   9.238 μs |   8.641 μs |   23 |   8.7891 |       - |  149649 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   497.29 μs |   4.960 μs |   4.639 μs |   24 |   7.3242 |       - |  118456 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   656.25 μs |   7.660 μs |   7.166 μs |   25 |  26.3672 |  1.9531 |  419161 B |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |   905.15 μs |   8.974 μs |   8.814 μs |   26 |  19.5313 |  1.9531 |  319939 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 1,818.15 μs |  35.771 μs |  38.275 μs |   27 |  78.1250 | 31.2500 | 1331341 B |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; | 1,841.30 μs |  22.931 μs |  21.450 μs |   27 |  78.1250 | 31.2500 | 1332486 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,445.69 μs |  88.137 μs | 111.466 μs |   28 |   7.8125 |       - |  158405 B |
| &#39;BulkInsert Naive - Dapper per row&#39;         | 5,025.16 μs |  85.305 μs |  79.794 μs |   29 |  31.2500 |       - |  493525 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 7,540.90 μs | 130.945 μs | 116.079 μs |   30 | 250.0000 | 31.2500 | 4234574 B |
