```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4351)
12th Gen Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.200-preview.0.26103.119
  [Host]     : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.23 (8.0.2325.60607), X64 RyuJIT AVX2


```
| Method                                      | Mean         | Error        | StdDev       | Median       | Rank | Gen0     | Gen1    | Allocated |
|-------------------------------------------- |-------------:|-------------:|-------------:|-------------:|-----:|---------:|--------:|----------:|
| Count_nORM                                  |     13.53 μs |     0.076 μs |     0.068 μs |     13.55 μs |    1 |   0.0916 |       - |    1560 B |
| &#39;Query Simple nORM (Compiled)&#39;              |     14.72 μs |     0.066 μs |     0.058 μs |     14.72 μs |    2 |   0.2747 |       - |    4424 B |
| Count_RawAdo                                |     15.03 μs |     0.184 μs |     0.172 μs |     15.00 μs |    3 |   0.0458 |       - |     808 B |
| &#39;Query Simple Raw ADO (Prepared)&#39;           |     15.64 μs |     0.104 μs |     0.092 μs |     15.66 μs |    4 |   0.3662 |       - |    6024 B |
| &#39;Query Simple Dapper (Prepared)&#39;            |     15.69 μs |     0.159 μs |     0.149 μs |     15.64 μs |    4 |   0.3662 |       - |    6016 B |
| Count_Dapper                                |     15.72 μs |     0.115 μs |     0.107 μs |     15.70 μs |    4 |   0.0610 |       - |    1096 B |
| Query_Simple_nORM                           |     20.68 μs |     0.107 μs |     0.089 μs |     20.69 μs |    5 |   0.4272 |       - |    6816 B |
| &#39;Query Simple EF Core (Compiled)&#39;           |     20.88 μs |     0.181 μs |     0.169 μs |     20.89 μs |    5 |   0.5188 |       - |    8264 B |
| Query_Simple_RawAdo                         |     23.80 μs |     0.335 μs |     0.313 μs |     23.87 μs |    6 |   0.3967 |       - |    6248 B |
| Query_Simple_EfCore                         |     26.95 μs |     0.298 μs |     0.279 μs |     26.92 μs |    7 |   0.7324 |       - |   12832 B |
| Query_Simple_Dapper                         |     27.00 μs |     0.113 μs |     0.106 μs |     27.00 μs |    7 |   0.4272 |       - |    6912 B |
| Count_EfCore                                |     27.85 μs |     0.133 μs |     0.118 μs |     27.85 μs |    8 |   0.2441 |       - |    4744 B |
| Query_Join_nORM_Compiled                    |     39.18 μs |     0.393 μs |     0.367 μs |     39.25 μs |    9 |   0.9155 |       - |   14544 B |
| Query_Join_Dapper                           |     41.42 μs |     0.431 μs |     0.403 μs |     41.42 μs |   10 |   0.9155 |       - |   14776 B |
| Query_Join_nORM                             |     52.70 μs |     0.451 μs |     0.422 μs |     52.74 μs |   11 |   1.3428 |       - |   22632 B |
| Insert_Single_nORM                          |     55.69 μs |     5.406 μs |    14.523 μs |     49.13 μs |   11 |   0.0610 |       - |    1480 B |
| &#39;Query Complex EF Core (Compiled)&#39;          |     56.56 μs |     0.270 μs |     0.240 μs |     56.60 μs |   12 |   0.9155 |       - |   14648 B |
| Insert_Single_EfCore                        |     59.84 μs |     0.850 μs |     0.710 μs |     59.98 μs |   13 |   0.9766 |       - |   15840 B |
| Query_Complex_nORM                          |     62.70 μs |     0.506 μs |     0.473 μs |     62.51 μs |   14 |   0.9766 |       - |   18288 B |
| Query_Complex_EfCore                        |     74.59 μs |     0.817 μs |     0.764 μs |     74.55 μs |   15 |   1.4648 |       - |   23352 B |
| Query_Join_EfCore                           |     83.61 μs |     1.606 μs |     1.650 μs |     84.07 μs |   16 |   3.4180 |       - |   56922 B |
| &#39;Query Complex nORM (Compiled)&#39;             |     88.28 μs |     0.422 μs |     0.395 μs |     88.18 μs |   17 |   0.4883 |       - |    8504 B |
| &#39;Query Complex Dapper (Prepared)&#39;           |     90.12 μs |     0.501 μs |     0.469 μs |     90.14 μs |   18 |   0.6104 |       - |   10312 B |
| &#39;Query Complex Raw ADO (Prepared)&#39;          |     90.34 μs |     0.594 μs |     0.556 μs |     90.57 μs |   18 |   0.6104 |       - |   10312 B |
| Query_Complex_RawAdo                        |    102.43 μs |     0.331 μs |     0.294 μs |    102.51 μs |   19 |   0.6104 |       - |   11016 B |
| Query_Complex_Dapper                        |    110.35 μs |     0.292 μs |     0.259 μs |    110.33 μs |   20 |   0.8545 |       - |   13520 B |
| Insert_Single_Dapper                        |    115.87 μs |     8.919 μs |    24.716 μs |    106.29 μs |   21 |        - |       - |    4816 B |
| Insert_Single_RawAdo                        |    146.57 μs |     7.552 μs |    20.157 μs |    152.68 μs |   22 |   0.1221 |       - |    2240 B |
| &#39;BulkInsert Idiomatic - nORM BulkInsert&#39;    |    478.39 μs |     6.326 μs |     5.608 μs |    477.58 μs |   23 |   8.7891 |       - |  149649 B |
| &#39;BulkInsert Batched - Dapper prepared&#39;      |    509.08 μs |     9.910 μs |    13.565 μs |    512.04 μs |   24 |   6.8359 |       - |  118457 B |
| &#39;BulkInsert Batched - nORM Prepared&#39;        |    513.84 μs |    10.258 μs |    16.854 μs |    520.47 μs |   24 |   8.3008 |       - |  132008 B |
| &#39;BulkInsert Idiomatic - Dapper list in Tx&#39;  |    665.69 μs |     7.961 μs |     7.057 μs |    666.00 μs |   25 |  25.3906 |  1.9531 |  419161 B |
| &#39;BulkInsert Batched - nORM Tx + per row&#39;    |  1,340.95 μs |    26.382 μs |    48.901 μs |  1,365.60 μs |   26 |  19.5313 |       - |  319941 B |
| &#39;BulkInsert Idiomatic - EF AddRange&#39;        |  1,853.80 μs |    35.005 μs |    31.031 μs |  1,846.82 μs |   27 |  78.1250 | 31.2500 | 1331341 B |
| &#39;BulkInsert Batched - EF SaveChanges in Tx&#39; |  2,347.52 μs |    46.486 μs |   105.873 μs |  2,289.56 μs |   28 |  78.1250 | 31.2500 | 1332486 B |
| &#39;BulkInsert Naive - nORM per row&#39;           |  4,346.47 μs |    85.896 μs |   161.333 μs |  4,369.59 μs |   29 |   7.8125 |       - |  158404 B |
| &#39;BulkInsert Naive - EF per row&#39;             |  7,407.23 μs |    65.985 μs |    55.100 μs |  7,410.61 μs |   30 | 250.0000 |       - | 4234574 B |
| &#39;BulkInsert Naive - Dapper per row&#39;         | 13,454.82 μs | 2,039.116 μs | 5,547.564 μs | 15,819.31 μs |   31 |  31.2500 |       - |  493529 B |
