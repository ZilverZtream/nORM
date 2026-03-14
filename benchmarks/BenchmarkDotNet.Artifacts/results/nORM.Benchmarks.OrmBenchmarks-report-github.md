```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean        | Error     | StdDev     | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |------------:|----------:|-----------:|-----:|---------:|--------:|----------:|
| Count_RawAdo                                |    15.35 μs |  0.110 μs |   0.103 μs |    1 |   0.0458 |       - |     808 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    15.37 μs |  0.071 μs |   0.063 μs |    1 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    15.76 μs |  0.109 μs |   0.102 μs |    2 |   0.3662 |       - |    6024 B |
| Count_Dapper                                |    15.89 μs |  0.136 μs |   0.127 μs |    2 |   0.0610 |       - |    1096 B |
| &#39;Query Simple nORM (Compiled)&#39;              |    16.23 μs |  0.075 μs |   0.070 μs |    3 |   0.2747 |       - |    4384 B |
| Count_nORM                                  |    16.29 μs |  0.150 μs |   0.133 μs |    3 |   0.0916 |       - |    1904 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    20.56 μs |  0.174 μs |   0.163 μs |    4 |   0.5188 |       - |    8264 B |
| Query_Simple_nORM                           |    21.74 μs |  0.148 μs |   0.131 μs |    5 |   0.4272 |       - |    6816 B |
| Query_Simple_RawAdo                         |    22.50 μs |  0.150 μs |   0.141 μs |    6 |   0.3967 |       - |    6248 B |
| Query_Simple_EfCore                         |    25.69 μs |  0.287 μs |   0.254 μs |    7 |   0.7324 |       - |   12832 B |
| Query_Simple_Dapper                         |    25.87 μs |  0.151 μs |   0.141 μs |    7 |   0.4272 |       - |    6912 B |
| Count_EfCore                                |    27.38 μs |  0.169 μs |   0.150 μs |    8 |   0.2441 |       - |    4744 B |
| Query_Join_nORM_Compiled                    |    39.75 μs |  0.323 μs |   0.302 μs |    9 |   0.9155 |       - |   14512 B |
| Insert_Single_nORM                          |    40.16 μs |  0.693 μs |   0.614 μs |    9 |   0.0610 |       - |    1480 B |
| Query_Join_Dapper                           |    42.12 μs |  0.842 μs |   1.124 μs |   10 |   0.9155 |       - |   14776 B |
| Insert_Single_Dapper                        |    50.01 μs |  0.837 μs |   0.930 μs |   11 |   0.3052 |       - |    4816 B |
| Query_Join_nORM                             |    50.42 μs |  0.242 μs |   0.202 μs |   11 |   1.3428 |       - |   22632 B |
| Insert_Single_RawAdo                        |    51.87 μs |  0.635 μs |   0.531 μs |   12 |   0.1221 |       - |    2240 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    56.39 μs |  0.251 μs |   0.235 μs |   13 |   0.9155 |       - |   14648 B |
| Insert_Single_EfCore                        |    63.06 μs |  1.432 μs |   4.221 μs |   14 |   0.9766 |       - |   15840 B |
| Query_Complex_nORM                          |    66.12 μs |  0.625 μs |   0.585 μs |   14 |   0.9766 |       - |   18416 B |
| Query_Complex_EfCore                        |    76.40 μs |  0.660 μs |   0.617 μs |   15 |   1.4648 |       - |   23664 B |
| Query_Join_EfCore                           |    83.74 μs |  1.629 μs |   1.600 μs |   16 |   3.4180 |       - |   56922 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    89.14 μs |  0.248 μs |   0.207 μs |   17 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    89.59 μs |  0.382 μs |   0.358 μs |   17 |   0.6104 |       - |   10312 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    91.00 μs |  0.272 μs |   0.255 μs |   18 |   0.4883 |       - |    8464 B |
| Query_Complex_RawAdo                        |   100.83 μs |  0.239 μs |   0.223 μs |   19 |   0.6104 |       - |   11016 B |
| Query_Complex_Dapper                        |   107.69 μs |  0.478 μs |   0.424 μs |   20 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   489.20 μs |  4.489 μs |   4.199 μs |   21 |   8.7891 |       - |  149649 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   491.02 μs |  8.869 μs |   7.863 μs |   21 |   7.8125 |       - |  132009 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   537.24 μs | 10.271 μs |  11.828 μs |   22 |   6.8359 |       - |  118457 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   670.50 μs |  5.322 μs |   4.978 μs |   23 |  26.3672 |  1.9531 |  419161 B |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |   934.74 μs |  6.970 μs |   6.520 μs |   24 |  19.5313 |  1.9531 |  319940 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 1,880.39 μs | 27.944 μs |  26.139 μs |   25 |  78.1250 | 31.2500 | 1331341 B |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; | 1,922.40 μs | 24.374 μs |  22.800 μs |   26 |  78.1250 | 31.2500 | 1332480 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 4,585.93 μs | 81.586 μs | 103.180 μs |   27 |   7.8125 |       - |  158406 B |
| &#39;BulkInsert Naive - Dapper per row&#39;         | 5,286.61 μs | 73.958 μs |  57.742 μs |   28 |  31.2500 |       - |  493526 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 7,860.30 μs | 91.935 μs |  81.498 μs |   29 | 250.0000 | 31.2500 | 4234573 B |
