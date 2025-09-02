```

BenchmarkDotNet v0.13.12, Windows 11 (10.0.26100.4652)
Intel Core i7-10750H CPU 2.60GHz, 1 CPU, 12 logical and 6 physical cores
.NET SDK 9.0.103
  [Host]     : .NET 8.0.13 (8.0.1325.6609), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.13 (8.0.1325.6609), X64 RyuJIT AVX2


```
| Method               | Mean          | Error         | StdDev        | Rank | Gen0     | Gen1    | Allocated  |
|--------------------- |--------------:|--------------:|--------------:|-----:|---------:|--------:|-----------:|
| Count_nORM           |      26.72 μs |      0.511 μs |      0.588 μs |    1 |   1.4648 |       - |    9.13 KB |
| Query_Simple_nORM    |      64.04 μs |      1.183 μs |      1.107 μs |    2 |   1.5869 |       - |   10.34 KB |
| Query_Simple_RawAdo  |      73.92 μs |      1.006 μs |      0.840 μs |    3 |   0.7324 |       - |    4.86 KB |
| Query_Simple_Dapper  |      80.28 μs |      0.778 μs |      0.728 μs |    4 |   0.8545 |       - |    5.78 KB |
| Count_Dapper         |      91.56 μs |      0.669 μs |      0.593 μs |    5 |   0.1221 |       - |    1.39 KB |
| Query_Complex_nORM   |      96.00 μs |      1.895 μs |      1.772 μs |    6 |   2.6855 |       - |   17.64 KB |
| Query_Join_Dapper    |     112.04 μs |      1.630 μs |      1.525 μs |    7 |   2.0752 |       - |   13.13 KB |
| Query_Join_nORM      |     134.62 μs |      1.611 μs |      1.428 μs |    8 |   3.9063 |       - |   25.04 KB |
| Count_EfCore         |     160.82 μs |      2.960 μs |      2.769 μs |    9 |   8.7891 |  0.9766 |   56.46 KB |
| Query_Simple_EfCore  |     171.63 μs |      3.332 μs |      4.213 μs |   10 |  11.2305 |  1.4648 |   69.65 KB |
| Query_Complex_Dapper |     186.02 μs |      3.276 μs |      3.065 μs |   11 |   1.7090 |       - |   11.04 KB |
| Query_Join_EfCore    |     295.76 μs |      5.335 μs |      4.990 μs |   12 |  17.5781 |  1.9531 |  108.72 KB |
| Query_Complex_EfCore |     315.64 μs |      5.753 μs |      5.382 μs |   13 |  13.6719 |  1.9531 |   85.71 KB |
| Insert_Single_nORM   |     687.18 μs |     10.870 μs |      9.636 μs |   14 |   3.9063 |  2.9297 |    27.3 KB |
| BulkInsert_nORM      |     938.53 μs |     15.610 μs |     14.602 μs |   15 |  23.4375 | 21.4844 |  151.31 KB |
| Insert_Single_EfCore |   1,734.96 μs |     34.809 μs |    102.636 μs |   16 |   7.8125 |       - |   65.63 KB |
| BulkInsert_EfCore    |   5,121.10 μs |     86.887 μs |    135.273 μs |   17 | 187.5000 | 62.5000 | 1181.25 KB |
| Insert_Single_RawAdo |   5,568.16 μs |    109.427 μs |    149.784 μs |   18 |        - |       - |    2.09 KB |
| Insert_Single_Dapper |   5,633.55 μs |    111.867 μs |    114.879 μs |   18 |        - |       - |    3.93 KB |
| BulkInsert_Dapper    | 552,597.11 μs | 10,662.265 μs | 14,233.818 μs |   19 |        - |       - |  310.23 KB |
