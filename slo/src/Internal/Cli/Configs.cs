namespace Internal.Cli;

public record CreateConfig(
    string Endpoint,
    string Db,
    string TableName,
    int MinPartitionsCount,
    int MaxPartitionsCount,
    int PartitionSize,
    int InitialDataCount,
    int WriteTimeout) : Config(Endpoint, Db, TableName, WriteTimeout);

public record CleanUpConfig(string Endpoint, string Db, string TableName, int WriteTimeout)
    : Config(Endpoint, Db, TableName, WriteTimeout);

public record RunConfig(
    string Endpoint,
    string Db,
    string TableName,
    string PromPgw,
    int ReportPeriod,
    int ReadRps,
    int ReadTimeout,
    int WriteRps,
    int WriteTimeout,
    int Time,
    int ShutdownTime) : Config(Endpoint, Db, TableName, WriteTimeout);

public record Config(string Endpoint, string Db, string TableName, int WriteTimeout);