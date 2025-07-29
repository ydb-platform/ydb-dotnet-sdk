// * Legends *
Mean                 : Arithmetic mean of all
measurements                                                                                                                                                                           
Error                : Half of 99.9% confidence
interval                                                                                                                                                                             
StdDev               : Standard deviation of all
measurements                                                                                                                                                                        
Median               : Value separating the higher half of all measurements (50th
percentile)                                                                                                                                        
Completed Work Items : The number of work items that have been processed in ThreadPool (per single
operation)                                                                                                                        
Lock Contentions     : The number of times there was contention upon trying to take a Monitor's lock (per single
operation)                                                                                                          
Gen0                 : GC Generation 0 collects per 1000
operations                                                                                                                                                                  
Allocated            : Allocated memory per single operation (managed only, inclusive, 1KB =
1024B)                                                                                                                                  
1 ns                 : 1 Nanosecond (0.000000001 sec)

# YDB .NET SDK Session Pool V1 On Semaphore-Based

| Method                              |             Mean |           Error |          StdDev | Completed Work Items | Lock Contentions |      Gen0 |   Gen1 |  Allocated |
|-------------------------------------|-----------------:|----------------:|----------------:|---------------------:|-----------------:|----------:|-------:|-----------:|
| SingleThreaded_OpenClose            |         126.0 ns |         0.85 ns |         0.71 ns |               0.0000 |                - |    0.0257 |      - |      216 B |                                                             
| MultiThreaded_OpenClose             |      36,028.4 ns |       710.14 ns |     1,146.75 ns |              40.0003 |           0.5005 |    1.4038 |      - |    11582 B |
| HighContention_OpenClose            |     155,614.4 ns |     3,083.73 ns |     5,400.90 ns |             230.8015 |           5.5818 |    8.7891 | 0.4883 |    74780 B |
| SessionReuse_Pattern                |     203,350.5 ns |     2,675.74 ns |     2,371.97 ns |             220.0027 |           5.5349 |    5.8594 |      - |    50511 B |
| SessionReuse_HighIterations_Pattern | 145,221,373.7 ns | 1,843,163.83 ns | 1,724,096.59 ns |          200020.2500 |        1764.5000 | 5000.0000 |      - | 43209728 B |

# YDB .NET SDK Session Pool Benchmarks (Npgsql)

| Method                              |             Mean |          Error |         StdDev | Completed Work Items | Lock Contentions |     Gen0 |    Gen1 | Allocated |
|-------------------------------------|-----------------:|---------------:|---------------:|---------------------:|-----------------:|---------:|--------:|----------:|
| SingleThreaded_OpenClose            |         26.34 ns |       0.476 ns |       0.422 ns |                    - |                - |        - |       - |         - |                                       
| MultiThreaded_OpenClose             |     32,578.44 ns |     650.578 ns |   1,583.596 ns |              40.0028 |           0.4411 |   0.8545 |       - |    7251 B |
| HighContention_OpenClose            |    138,930.59 ns |   2,729.363 ns |   4,990.795 ns |             209.1182 |           3.5857 |   4.8828 |       - |   40566 B |
| SessionReuse_Pattern                |    159,954.05 ns |   2,820.324 ns |   4,044.825 ns |             220.0000 |           6.0381 |   0.7324 |       - |    7307 B |
| SessionReuse_HighContention_Pattern |  8,914,529.81 ns |  46,900.448 ns |  41,576.026 ns |           19756.6563 |         149.0469 | 625.0000 | 93.7500 | 5289794 B |
| SessionReuse_HighIterations_Pattern | 81,211,792.96 ns | 749,115.160 ns | 664,071.077 ns |          200020.0000 |         614.8571 |        - |       - |    7458 B |
