namespace Internal;

public record CreateConfig(
    string Endpoint,
    string Db,
    string ResourcePathYdb,
    int MinPartitionsCount,
    int MaxPartitionsCount,
    int InitialDataCount,
    int WriteTimeout) : Config(Endpoint, Db, ResourcePathYdb, WriteTimeout);

public record RunConfig(
    string Endpoint,
    string Db,
    string ResourcePathYdb,
    string PromPgw,
    int ReportPeriod,
    int ReadRps,
    int ReadTimeout,
    int WriteRps,
    int WriteTimeout,
    int Time) : Config(Endpoint, Db, ResourcePathYdb, WriteTimeout);

public record Config(string Endpoint, string Db, string ResourcePathYdb, int WriteTimeout);