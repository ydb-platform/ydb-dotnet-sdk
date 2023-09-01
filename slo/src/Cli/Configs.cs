namespace slo.Cli;

internal record CreateConfig(string Endpoint, string Db, string TableName, int MinPartitionsCount,
    int MaxPartitionsCount, int PartitionSize, int InitialDataCount, int WriteTimeout);

internal record CleanUpConfig(string Endpoint, string Db, string TableName, int WriteTimeout);

internal record RunConfig(string Endpoint, string Db, string TableName, int InitialDataCount,
    string PromPgw,
    int ReportPeriod, int ReadRps, int ReadTimeout, int WriteRps, int WriteTimeout, int Time,
    int ShutdownTime);