// See https://aka.ms/new-console-template for more information

using EF;
using Internal;

await Cli.Run(new SloTableContext(), args);