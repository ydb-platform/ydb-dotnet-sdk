namespace Internal;

public record CreateConfig(
    string ConnectionString,
    int InitialDataCount,
    int WriteTimeout) : Config(ConnectionString, WriteTimeout);

public record RunConfig(
    string ConnectionString,
    string PromPgw,
    int ReportPeriod,
    int ReadRps,
    int ReadTimeout,
    int WriteRps,
    int WriteTimeout,
    int Time) : Config(ConnectionString, WriteTimeout);

public record Config(string ConnectionString, int WriteTimeout);