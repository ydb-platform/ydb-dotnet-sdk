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

# YDB .NET SDK Session Pool Benchmarks

| Method                              |             Mean |          Error |         StdDev | Completed Work Items | Lock Contentions |   Gen0 | Allocated |
|-------------------------------------|-----------------:|---------------:|---------------:|---------------------:|-----------------:|-------:|----------:|
| SingleThreaded_OpenClose            |         25.35 ns |       0.258 ns |       0.241 ns |                    - |                - |      - |         - |                                                                            
| MultiThreaded_OpenClose             |     30,650.91 ns |     601.279 ns |   1,281.374 ns |              40.0031 |           0.5160 | 0.8545 |    7249 B |
| HighContention_OpenClose            |    142,804.56 ns |   2,845.384 ns |   7,138.512 ns |             205.9829 |           4.6663 | 4.6387 |   38864 B |
| SessionReuse_Pattern                |    153,983.59 ns |   2,272.104 ns |   2,014.161 ns |             220.0000 |           5.6296 | 0.7324 |    7303 B |
| SessionReuse_HighIterations_Pattern | 82,351,234.51 ns | 694,703.573 ns | 649,826.153 ns |          200020.0000 |         540.0000 |      - |    7458 B |

# YDB .NET SDK Session Source Benchmarks (Npgsql)

| Method                   |          Mean |        Error |       StdDev |        Median | Completed Work Items | Lock Contentions |   Gen0 | Allocated |
|--------------------------|--------------:|-------------:|-------------:|--------------:|---------------------:|-----------------:|-------:|----------:|
| SingleThreaded_OpenClose |      25.82 ns |     0.141 ns |     0.125 ns |      25.78 ns |                    - |                - |      - |         - |                                                                              
| MultiThreaded_OpenClose  |  20,893.61 ns |   829.087 ns | 2,431.569 ns |  19,694.30 ns |              20.0033 |           0.0303 | 0.5188 |    4526 B |
| HighContention_OpenClose | 108,688.27 ns | 2,160.177 ns | 3,298.819 ns | 108,755.99 ns |             100.0017 |           3.8002 | 2.5635 |   21839 B |
| SessionReuse_Pattern     | 130,849.34 ns | 2,616.397 ns | 4,977.967 ns | 129,920.39 ns |              20.0000 |           5.4443 | 0.4883 |    4588 B |
 