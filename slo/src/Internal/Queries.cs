namespace Internal;

public static class Queries
{
    public static string GetCreateQuery(string tableName, int partitionSize, int minPartitionsCount,
        int maxPartitionsCount)
    {
        return $@"
CREATE TABLE `{tableName}` (
    `hash` UINT64,
    `id` UINT64,
    `payload_str` UTF8,
    `payload_double` DOUBLE,
    `payload_timestamp` TIMESTAMP,
    `payload_hash` UINT64,
    PRIMARY KEY (`hash`, `id`)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = {partitionSize},
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {minPartitionsCount},
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {maxPartitionsCount}
);
";
    }

    public static string GetDropQuery(string tableName)
    {
        return $"DROP TABLE `{tableName}`";
    }

    public static string GetLoadMaxIdQuery(string tableName)
    {
        return $"SELECT MAX(id) as max_id FROM `{tableName}`";
    }

    public static string GetReadQuery(string tableName)
    {
        return $@"
DECLARE $id AS Uint64;
SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
FROM `{tableName}`
WHERE id = $id AND hash = Digest::NumericHash($id)
";
    }

    public static string GetWriteQuery(string tableName)
    {
        return $@"
DECLARE $id AS Uint64;
DECLARE $payload_str AS Utf8;
DECLARE $payload_double AS Double;
DECLARE $payload_timestamp AS Timestamp;
INSERT INTO `{tableName}` (id, hash, payload_str, payload_double, payload_timestamp)
VALUES ($id, Digest::NumericHash($id), $payload_str, $payload_double, $payload_timestamp)
";
    }
}