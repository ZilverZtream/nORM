```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean         | Error       | StdDev      | Median       | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |-------------:|------------:|------------:|-------------:|-----:|---------:|--------:|----------:|
| &#39;Query Simple nORM (Compiled)&#39;              |     7.526 μs |   0.0443 μs |   0.0393 μs |     7.519 μs |    1 |   0.2823 |       - |    4472 B |
| &#39;Query Complex nORM (Compiled)&#39;             |    11.570 μs |   0.0364 μs |   0.0304 μs |    11.577 μs |    2 |   0.3052 |       - |    4976 B |
| Count_RawAdo                                |    15.563 μs |   0.1508 μs |   0.1336 μs |    15.535 μs |    3 |   0.0305 |       - |     808 B |
| Count_Dapper                                |    15.912 μs |   0.1226 μs |   0.1024 μs |    15.883 μs |    4 |   0.0610 |       - |    1096 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |    17.316 μs |   0.3439 μs |   0.8501 μs |    17.022 μs |    5 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |    18.448 μs |   0.7750 μs |   2.1603 μs |    17.798 μs |    6 |   0.3662 |       - |    6024 B |
| Query_Complex_nORM                          |    21.485 μs |   0.2584 μs |   0.2158 μs |    21.409 μs |    7 |   0.7324 |       - |   11499 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |    22.077 μs |   0.0923 μs |   0.0818 μs |    22.077 μs |    8 |   0.5188 |       - |    8264 B |
| Count_nORM                                  |    22.179 μs |   0.1852 μs |   0.1641 μs |    22.149 μs |    8 |   0.5493 |       - |    8770 B |
| Query_Simple_RawAdo                         |    24.824 μs |   0.0740 μs |   0.0656 μs |    24.810 μs |    9 |   0.3967 |       - |    6240 B |
| Query_Simple_EfCore                         |    28.834 μs |   0.3858 μs |   0.3609 μs |    28.909 μs |   10 |   0.7324 |       - |   12832 B |
| Count_EfCore                                |    28.901 μs |   0.5663 μs |   0.6295 μs |    28.809 μs |   10 |   0.2441 |       - |    4744 B |
| Query_Simple_Dapper                         |    29.091 μs |   0.5726 μs |   0.8212 μs |    28.818 μs |   10 |   0.4272 |       - |    6920 B |
| Query_Simple_nORM                           |    29.276 μs |   0.3482 μs |   0.3087 μs |    29.173 μs |   10 |   0.6714 |       - |   11465 B |
| Query_Join_Dapper                           |    45.682 μs |   0.9050 μs |   1.9089 μs |    45.172 μs |   11 |   0.9155 |       - |   14776 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |    60.403 μs |   0.8839 μs |   0.9457 μs |    60.026 μs |   12 |   0.9155 |       - |   14648 B |
| Query_Join_nORM                             |    66.014 μs |   1.0851 μs |   1.0150 μs |    65.648 μs |   13 |   1.7090 |       - |   29752 B |
| Insert_Single_nORM                          |    73.598 μs |   1.3431 μs |   1.6986 μs |    73.121 μs |   14 |        - |       - |    6676 B |
| Insert_Single_EfCore                        |    76.730 μs |   1.8531 μs |   5.3761 μs |    75.932 μs |   15 |   0.9766 |       - |   15840 B |
| Query_Complex_EfCore                        |    81.807 μs |   1.6295 μs |   3.9972 μs |    80.130 μs |   16 |   1.4648 |       - |   23352 B |
| Query_Join_EfCore                           |    89.806 μs |   1.1306 μs |   1.0022 μs |    89.819 μs |   17 |   3.4180 |       - |   57066 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |    94.092 μs |   0.3292 μs |   0.2919 μs |    94.062 μs |   18 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |    95.544 μs |   0.5261 μs |   0.4664 μs |    95.504 μs |   19 |   0.6104 |       - |   10312 B |
| Query_Join_nORM_Compiled                    |    97.503 μs |   4.1029 μs |  11.3005 μs |    92.546 μs |   19 |   2.0752 |  0.1221 |   33568 B |
| Insert_Single_Dapper                        |   102.113 μs |   2.0052 μs |   2.8110 μs |   101.684 μs |   20 |   0.2441 |       - |    4816 B |
| Insert_Single_RawAdo                        |   103.812 μs |   2.0648 μs |   4.6606 μs |   103.054 μs |   20 |   0.1221 |       - |    2240 B |
| Query_Complex_RawAdo                        |   108.673 μs |   0.8535 μs |   0.7984 μs |   108.815 μs |   21 |   0.6104 |       - |   11016 B |
| Query_Complex_Dapper                        |   114.197 μs |   0.9895 μs |   0.8263 μs |   114.080 μs |   22 |   0.8545 |       - |   13528 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |   544.103 μs |   7.9886 μs |   7.4726 μs |   544.020 μs |   23 |   6.8359 |       - |  118457 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |   608.091 μs |   2.9310 μs |   2.2884 μs |   608.528 μs |   24 |  12.6953 |       - |  200968 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |   616.747 μs |  12.0400 μs |  16.0731 μs |   613.050 μs |   24 |  15.6250 |       - |  258665 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |   752.975 μs |  11.9366 μs |  10.5815 μs |   752.944 μs |   25 |  25.3906 |  1.9531 |  419161 B |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    | 1,157.283 μs |  14.4831 μs |  12.8389 μs | 1,156.808 μs |   26 |  31.2500 |  3.9063 |  547415 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        | 1,973.908 μs |  33.8333 μs |  29.9924 μs | 1,966.144 μs |   27 |  78.1250 | 31.2500 | 1331341 B |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; | 2,044.102 μs |  31.6652 μs |  29.6197 μs | 2,041.454 μs |   28 |  78.1250 | 31.2500 | 1332486 B |
| &#39;BulkInsert Naive - Dapper per row&#39;         | 5,840.490 μs | 109.9842 μs |  91.8418 μs | 5,866.764 μs |   29 |  31.2500 |       - |  493525 B |
| &#39;BulkInsert Naive - nORM per row&#39;           | 6,215.017 μs | 123.8446 μs | 296.7238 μs | 6,128.362 μs |   30 |  31.2500 |       - |  678574 B |
| &#39;BulkInsert Naive - EF per row&#39;             | 8,872.043 μs | 174.8873 μs | 221.1764 μs | 8,858.944 μs |   31 | 250.0000 | 31.2500 | 4234573 B |
