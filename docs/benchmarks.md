# Benchmarks

The following micro-benchmarks compare nORM with other popular .NET data access libraries. Measurements were captured on .NET 8.0.

| Method               | Mean        | Memory    |
|---------------------|------------:|----------:|
| Query_Simple_nORM   |    62.54 us |  12.03 KB |
| Count_nORM          |    66.75 us |   7.32 KB |
| Query_Simple_RawAdo |    70.29 us |    6.1 KB |
| Query_Simple_Dapper |    77.38 us |   6.76 KB |
| Query_Simple_EfCore |    81.94 us |   8.89 KB |
| Count_Dapper        |    83.41 us |   1.14 KB |
| Insert_Single_nORM  |    87.20 us |  12.48 KB |
| Count_EfCore        |   104.95 us |    4.7 KB |
| Query_Join_Dapper   |   106.25 us |  14.43 KB |
| Query_Complex_nORM  |   116.82 us |   9.66 KB |
| Query_Join_nORM     |   131.11 us |  41.37 KB |
| Query_Complex_Dapper |   189.78 us |  13.21 KB |
| Query_Complex_EfCore |   191.30 us |  15.88 KB |
| Query_Join_EfCore   |   214.23 us |  55.67 KB |
| BulkInsert_nORM     | 1,497.19 us | 336.18 KB |
| Insert_Single_RawAdo | 5,014.80 us |   2.2 KB |
| Insert_Single_Dapper | 5,060.18 us |   4.96 KB |
| Insert_Single_EfCore | 5,408.51 us |  67.53 KB |
| BulkInsert_Dapper   | 5,905.85 us | 409.34 KB |
| BulkInsert_EfCore   | 8,896.02 us | 1300.15 KB |
