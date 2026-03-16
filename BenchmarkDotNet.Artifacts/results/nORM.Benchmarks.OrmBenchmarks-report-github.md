```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200
  [Host]     : .NET 8.0.25 (8.0.2526.11203), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.25 (8.0.2526.11203), X64 RyuJIT AVX2


```
| Method                                      | Mean        | Error      | StdDev     | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |------------:|-----------:|-----------:|-----:|---------:|--------:|----------:|
| Count_nORM                                  |    13.95 μs |   0.257 μs |   0.214 μs |    1 |   0.0916 |       - |    1560 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    15.17 μs |   0.094 μs |   0.088 μs |    2 |   0.2747 |       - |    4424 B |
| Count_RawAdo                                |    15.40 μs |   0.153 μs |   0.143 μs |    3 |   0.0305 |       - |     808 B |
| Count_Dapper                                |    16.01 μs |   0.278 μs |   0.260 μs |    4 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    16.17 μs |   0.156 μs |   0.145 μs |    4 |   0.3662 |       - |    6016 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    16.25 μs |   0.140 μs |   0.124 μs |    4 |   0.3662 |       - |    6024 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    21.48 μs |   0.144 μs |   0.128 μs |    5 |   0.5188 |       - |    8264 B |
| Query_Simple_nORM                           |    21.53 μs |   0.399 μs |   0.333 μs |    5 |   0.4272 |       - |    6816 B |
| Query_Simple_RawAdo                         |    23.71 μs |   0.245 μs |   0.229 μs |    6 |   0.3967 |       - |    6248 B |
| Query_Simple_EfCore                         |    27.80 μs |   0.352 μs |   0.329 μs |    7 |   0.7324 |       - |   12832 B |
| Query_Simple_Dapper                         |    27.90 μs |   0.231 μs |   0.216 μs |    7 |   0.4272 |       - |    6920 B |
| Count_EfCore                                |    28.75 μs |   0.473 μs |   0.485 μs |    8 |   0.2441 |       - |    4744 B |
| Query_Join_nORM_Compiled                    |    40.28 μs |   0.324 μs |   0.288 μs |    9 |   0.9155 |       - |   14544 B |
| Query_Join_Dapper                           |    42.78 μs |   0.178 μs |   0.158 μs |   10 |   0.9155 |       - |   14776 B |
| Insert_Single_nORM                          |    48.29 μs |   0.977 μs |   2.608 μs |   11 |   0.0610 |       - |    1480 B |
| Query_Join_nORM                             |    54.18 μs |   0.441 μs |   0.391 μs |   12 |   1.3428 |       - |   22712 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    59.87 μs |   1.182 μs |   1.361 μs |   13 |   0.8545 |       - |   14648 B |
| Query_Complex_nORM                          |    65.05 μs |   1.254 μs |   1.288 μs |   14 |   0.9766 |       - |   18136 B |
| Insert_Single_EfCore                        |    67.15 μs |   1.320 μs |   1.235 μs |   15 |   0.9766 |       - |   15840 B |
| Query_Complex_EfCore                        |    77.16 μs |   1.278 μs |   1.133 μs |   16 |   1.4648 |       - |   23752 B |
| Query_Join_EfCore                           |    89.27 μs |   1.736 μs |   1.783 μs |   17 |   3.4180 |       - |   57034 B |
| Insert_Single_RawAdo                        |    90.27 μs |   1.777 μs |   2.869 μs |   17 |   0.1221 |       - |    2240 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    92.24 μs |   0.593 μs |   0.526 μs |   17 |   0.4883 |       - |    8504 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    93.55 μs |   0.472 μs |   0.419 μs |   18 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    94.21 μs |   0.489 μs |   0.457 μs |   18 |   0.6104 |       - |   10312 B |
| Insert_Single_Dapper                        |    95.21 μs |   1.892 μs |   3.460 μs |   18 |   0.2441 |       - |    4816 B |
| Query_Complex_RawAdo                        |   108.08 μs |   1.500 μs |   1.253 μs |   19 |   0.6104 |       - |   11008 B |
| Query_Complex_Dapper                        |   113.86 μs |   1.180 μs |   1.104 μs |   20 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   484.68 μs |   7.845 μs |   7.338 μs |   21 |   7.8125 |       - |  132009 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   490.67 μs |   5.248 μs |   4.652 μs |   21 |   8.7891 |       - |  149648 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   510.18 μs |  10.034 μs |   8.895 μs |   22 |   7.3242 |       - |  118456 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   670.01 μs |   9.635 μs |   9.012 μs |   23 |  26.3672 |  1.9531 |  419160 B |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |   952.10 μs |  11.888 μs |  10.538 μs |   24 |  19.5313 |  1.9531 |  319940 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 1,879.49 μs |  23.783 μs |  22.246 μs |   25 |  78.1250 | 31.2500 | 1331337 B |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; | 2,041.33 μs |  39.743 μs |  68.554 μs |   26 |  78.1250 | 31.2500 | 1332485 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,710.31 μs |  93.683 μs | 220.821 μs |   27 |   7.8125 |       - |  158400 B |
| &#39;BulkInsert Naive - Dapper per row&#39;         | 5,841.90 μs | 115.269 μs | 145.778 μs |   28 |  31.2500 |       - |  493525 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 8,139.48 μs | 162.613 μs | 211.443 μs |   29 | 250.0000 | 31.2500 | 4234555 B |
