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

# YDB .NET SDK Session Pool Benchmarks

| Method                   |         Mean |       Error |      StdDev | Completed Work Items | Lock Contentions |   Gen0 | Allocated |
|--------------------------|-------------:|------------:|------------:|---------------------:|-----------------:|-------:|----------:|
| SingleThreaded_OpenClose |     130.2 ns |     0.91 ns |     0.71 ns |               0.0000 |                - | 0.0257 |     216 B |                                                                                                 
| MultiThreaded_OpenClose  |  41,667.8 ns | 1,065.07 ns | 3,140.37 ns |              20.0018 |           0.3466 | 1.0376 |    8851 B |
| HighContention_OpenClose | 130,331.1 ns | 2,569.39 ns | 6,106.44 ns |             100.0000 |           1.9094 | 5.1270 |   43421 B |
| SessionReuse_Pattern     | 204,351.2 ns | 4,038.25 ns | 7,485.16 ns |              20.0000 |           3.6716 | 5.6152 |   47762 B |

# YDB .NET SDK Session Source Benchmarks (Npgsql)

| Method                   |          Mean |        Error |       StdDev |        Median | Completed Work Items | Lock Contentions |   Gen0 | Allocated |
|--------------------------|--------------:|-------------:|-------------:|--------------:|---------------------:|-----------------:|-------:|----------:|
| SingleThreaded_OpenClose |      25.82 ns |     0.141 ns |     0.125 ns |      25.78 ns |                    - |                - |      - |         - |                                                                              
| MultiThreaded_OpenClose  |  20,893.61 ns |   829.087 ns | 2,431.569 ns |  19,694.30 ns |              20.0033 |           0.0303 | 0.5188 |    4526 B |
| HighContention_OpenClose | 108,688.27 ns | 2,160.177 ns | 3,298.819 ns | 108,755.99 ns |             100.0017 |           3.8002 | 2.5635 |   21839 B |
| SessionReuse_Pattern     | 130,849.34 ns | 2,616.397 ns | 4,977.967 ns | 129,920.39 ns |              20.0000 |           5.4443 | 0.4883 |    4588 B |
 