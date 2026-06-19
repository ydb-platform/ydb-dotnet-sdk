namespace Internal;

public record SloConfig(
    string ConnectionString,
    string? OtlpEndpoint,
    int ReportPeriod,
    int ReadRps,
    int ReadTimeout,
    int WriteRps,
    int WriteTimeout,
    int Time,
    int InitialDataCount
);