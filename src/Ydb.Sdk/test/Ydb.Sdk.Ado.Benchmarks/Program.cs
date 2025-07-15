using System.Reflection;
using BenchmarkDotNet.Running;

Console.WriteLine("YDB .NET SDK Session Pool Benchmarks");
Console.WriteLine("=====================================");

var summaries = new BenchmarkSwitcher(typeof(Program).GetTypeInfo().Assembly).Run(args);

foreach (var summary in summaries)
{
    Console.WriteLine($"Benchmark completed. Results saved to: {summary.ResultsDirectoryPath}");
}
