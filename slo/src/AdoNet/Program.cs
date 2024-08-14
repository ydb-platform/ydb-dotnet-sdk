// See https://aka.ms/new-console-template for more information

using AdoNet;
using Internal.Cli;

await Cli.Run(new SloContext(), args);