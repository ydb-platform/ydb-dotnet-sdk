// See https://aka.ms/new-console-template for more information

using AdoNet;
using Internal;
using Ydb.Sdk.Ado;

await Cli.Run((_, config) => new SloTableContext(new YdbDataSource(
    new YdbConnectionStringBuilder(config.ConnectionString) { LoggerFactory = ISloContext.Factory })), args);