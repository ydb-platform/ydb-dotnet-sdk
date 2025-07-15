# YDB .NET SDK Session Pool Benchmarks

| Method                   |         Mean |       Error |      StdDev | Completed Work Items | Lock Contentions |   Gen0 | Allocated |
|--------------------------|-------------:|------------:|------------:|---------------------:|-----------------:|-------:|----------:|
| SingleThreaded_OpenClose |     130.2 ns |     0.91 ns |     0.71 ns |               0.0000 |                - | 0.0257 |     216 B |                                                                                                 
| MultiThreaded_OpenClose  |  41,667.8 ns | 1,065.07 ns | 3,140.37 ns |              20.0018 |           0.3466 | 1.0376 |    8851 B |
| HighContention_OpenClose | 130,331.1 ns | 2,569.39 ns | 6,106.44 ns |             100.0000 |           1.9094 | 5.1270 |   43421 B |
| SessionReuse_Pattern     | 204,351.2 ns | 4,038.25 ns | 7,485.16 ns |              20.0000 |           3.6716 | 5.6152 |   47762 B |