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

BenchmarkDotNet v0.15.1, macOS Sequoia 15.5 (24F74) [Darwin 24.5.0]
Apple M2 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 9.0.201
[Host]     : .NET 8.0.2 (8.0.224.6711), Arm64 RyuJIT AdvSIMD
DefaultJob : .NET 8.0.2 (8.0.224.6711), Arm64 RyuJIT AdvSIMD


# YDB .NET SDK Session Pool V1 On Semaphore-Based

| Method                              |             Mean |           Error |          StdDev | Completed Work Items | Lock Contentions |      Gen0 |     Gen1 |  Allocated |
|-------------------------------------|-----------------:|----------------:|----------------:|---------------------:|-----------------:|----------:|---------:|-----------:|
| SingleThreaded_OpenClose            |         125.1 ns |         2.54 ns |         2.37 ns |                    - |                - |    0.0257 |        - |      216 B |
| MultiThreaded_OpenClose             |      30,893.8 ns |       610.63 ns |       895.06 ns |              40.0002 |           0.3741 |    1.4038 |        - |    11573 B |
| HighContention_OpenClose            |     159,192.9 ns |     3,168.43 ns |     6,752.19 ns |             230.7124 |           5.6448 |    8.7891 |   0.4883 |    74723 B |
| SessionReuse_Pattern                |     202,559.0 ns |     2,181.67 ns |     1,934.00 ns |             220.0027 |           5.4810 |    5.8594 |        - |    50510 B |
| SessionReuse_HighContention_Pattern |  10,016,874.4 ns |    58,462.60 ns |    54,685.95 ns |           19707.6719 |         165.5469 |  921.8750 | 140.6250 |  7729448 B |
| SessionReuse_HighIterations_Pattern | 152,765,644.4 ns | 1,570,755.95 ns | 1,469,286.09 ns |          200020.0000 |        1751.2500 | 5000.0000 |        - | 43207568 B |

# YDB .NET SDK Session Pool Benchmarks (Npgsql)

| Method                              |             Mean |          Error |         StdDev | Completed Work Items | Lock Contentions |     Gen0 |    Gen1 | Allocated |
|-------------------------------------|-----------------:|---------------:|---------------:|---------------------:|-----------------:|---------:|--------:|----------:|
| SingleThreaded_OpenClose            |         26.34 ns |       0.476 ns |       0.422 ns |                    - |                - |        - |       - |         - |                                       
| MultiThreaded_OpenClose             |     32,578.44 ns |     650.578 ns |   1,583.596 ns |              40.0028 |           0.4411 |   0.8545 |       - |    7251 B |
| HighContention_OpenClose            |    138,930.59 ns |   2,729.363 ns |   4,990.795 ns |             209.1182 |           3.5857 |   4.8828 |       - |   40566 B |
| SessionReuse_Pattern                |    159,954.05 ns |   2,820.324 ns |   4,044.825 ns |             220.0000 |           6.0381 |   0.7324 |       - |    7307 B |
| SessionReuse_HighContention_Pattern |  8,914,529.81 ns |  46,900.448 ns |  41,576.026 ns |           19756.6563 |         149.0469 | 625.0000 | 93.7500 | 5289794 B |
| SessionReuse_HighIterations_Pattern | 81,211,792.96 ns | 749,115.160 ns | 664,071.077 ns |          200020.0000 |         614.8571 |        - |       - |    7458 B |

# YDB .NET SDK Session Pool Benchmarks (FIFO lock-free)

| Method                              |             Mean |            Error |           StdDev | Completed Work Items | Lock Contentions |     Gen0 |     Gen1 | Allocated |
|-------------------------------------|-----------------:|-----------------:|-----------------:|---------------------:|-----------------:|---------:|---------:|----------:|
| SingleThreaded_OpenClose            |         60.71 ns |         0.441 ns |         0.368 ns |                    - |                - |   0.0038 |        - |      32 B |                                                           
| MultiThreaded_OpenClose             |     23,368.69 ns |       464.175 ns |     1,129.867 ns |              40.0049 |                - |   0.9460 |        - |    7887 B |
| HighContention_OpenClose            |     91,700.72 ns |     1,803.206 ns |     3,842.780 ns |             204.6633 |           0.0007 |   5.0049 |        - |   41951 B |
| SessionReuse_Pattern                |    117,545.11 ns |     2,226.365 ns |     4,014.595 ns |             220.0000 |           0.0001 |   1.5869 |        - |   13656 B |
| SessionReuse_HighContention_Pattern |  7,463,819.00 ns |   148,409.083 ns |   364,050.038 ns |           19044.6172 |           1.1719 | 765.6250 | 125.0000 | 6367528 B |
| SessionReuse_HighIterations_Pattern | 70,844,972.06 ns | 1,400,128.942 ns | 3,589,066.009 ns |          200020.0000 |                - | 750.0000 |        - | 6407440 B |