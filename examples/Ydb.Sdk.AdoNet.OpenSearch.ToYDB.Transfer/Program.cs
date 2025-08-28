using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Yc;
using OpenSearch.Client;
using System.Text.Json.Serialization;
using NLog.Extensions.Logging;

if (args.Length != 4)
{
    Console.WriteLine(
        "Usage: Program.exe <YdbConnectionString> <OpenSearchConnectionString> <OpenSearchPassword> <YdbTableName>");

    return 1;
}

const int workerCount = 20;
const int batchSize = 1_000;

var loggerFactory = LoggerFactory.Create(builder => builder.AddNLog());
var logger = loggerFactory.CreateLogger<Program>();

var builder = new YdbConnectionStringBuilder(args[0])
{
    CredentialsProvider = new MetadataProvider(loggerFactory: loggerFactory),
    ServerCertificates = YcCerts.GetYcServerCertificates(),
    MaxSessionPool = workerCount
};

await using var ydbDataSource = new YdbDataSource(builder);
await using (var ydbCommand = ydbDataSource.CreateCommand())
{
    ydbCommand.CommandText = $"""
                                 CREATE TABLE IF NOT EXISTS `{args[3]}` (
                                     indexId Text NOT NULL,
                                     chunkId Text NOT NULL,
                                     fileId Text NOT NULL,
                                     folderId Text NOT NULL,
                                     chunkText Text FAMILY family_chunkText NOT NULL,
                                     chunkVector Bytes,
                                     createdAt Timestamp NOT NULL,
                                     createdBy Text NOT NULL,
                                     updatedAt Timestamp NOT NULL,
                                     updatedBy Text NOT NULL,
                                     PRIMARY KEY (indexId, chunkId, fileId, folderId),
                                     FAMILY family_chunkText (
                                         DATA = "ssd",
                                         COMPRESSION = "lz4"
                                     ),
                                 ) WITH (
                                     AUTO_PARTITIONING_BY_SIZE = ENABLED,
                                     AUTO_PARTITIONING_BY_LOAD = ENABLED,
                                     AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 50,
                                     AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
                                 )
                              """;
    await ydbCommand.ExecuteNonQueryAsync();
}

var openSearchUri = new Uri(args[1]);
var openSearchSettings = new ConnectionSettings(openSearchUri)
    .BasicAuthentication("admin", args[2])
    .ServerCertificateValidationCallback((_, _, _, _) => true)
    .MaximumRetries(10)
    .ConnectionLimit(100)
    .EnableDebugMode()
    .RequestTimeout(TimeSpan.FromSeconds(30));

var openSearchClient = new OpenSearchClient(openSearchSettings);
var indicesResponse = await openSearchClient.Indices.GetAsync("*");

logger.LogInformation("Found count of indices: {Count}", indicesResponse.Indices.Count);

var filteredIndices = indicesResponse.Indices.Where(index => !index.Key.Name.StartsWith('.')).Select(index => index.Key)
    .ToArray();

var startTime = DateTime.UtcNow;
logger.LogInformation("Started at: {StartTime:yyyy-MM-dd HH:mm:ss} UTC", startTime);
logger.LogInformation("Total indices to process: {Count}", filteredIndices.Length);

var iterator = filteredIndices.Length;
var workers = new List<Task>();

for (var i = 0; i < workerCount; i++)
{
    workers.Add(Task.Run(async () =>
    {
        while (true)
        {
            var index = Interlocked.Decrement(ref iterator);

            // ReSharper disable once AccessToDisposedClosure
            await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();

            if (index < 0)
                break;

            await WorkerJobSingleIndex(filteredIndices[index], ydbConnection);
        }
    }));
}

await Task.WhenAll(workers);

var endTime = DateTime.UtcNow;
var duration = endTime - startTime;
logger.LogInformation("Completed at: {EndTime:yyyy-MM-dd HH:mm:ss} UTC", endTime);
logger.LogInformation(@"Total duration: {Duration:hh\:mm\:ss}", duration);
logger.LogInformation("All workers completed at {EndTime}. Total duration: {Duration}", endTime, duration);
return 0;

async Task WorkerJobSingleIndex(IndexName indexName, YdbConnection ydbConnection)
{
    var countResponse = await openSearchClient.CountAsync<Document>(c => c.Index(indexName));
    var totalDocuments = countResponse.Count;

    var bulkUpsertImporter = ydbConnection.BeginBulkUpsertImport(args[3],
    [
        "indexId", "chunkId", "fileId", "folderId", "chunkText", "chunkVector", "createdAt", "createdBy", "updatedAt",
        "updatedBy"
    ]);
    logger.LogInformation("Index {IndexName}: Total documents {TotalCount}", indexName, totalDocuments);

    var scrollResponse = await openSearchClient.SearchAsync<Document>(s => s
        .Index(indexName)
        .Size(batchSize)
        .Scroll("5m"));

    var scrollId = scrollResponse.ScrollId;
    var totalProcessed = 0;

    try
    {
        while (scrollResponse.Documents.Count != 0)
        {
            foreach (var doc in scrollResponse.Documents)
            {
                for (var attempt = 0; attempt < 10; attempt++)
                {
                    try
                    {
                        await bulkUpsertImporter.AddRowAsync(
                            indexName.Name,
                            doc.ChunkMetadata.ChunkId,
                            doc.ChunkMetadata.FileId,
                            doc.ChunkMetadata.FolderId,
                            doc.ChunkText,
                            new YdbParameter
                            {
                                YdbDbType = YdbDbType.Bytes,
                                Value = ConvertVectorToBytes(doc.ChunkVector)
                            },
                            doc.RecordMetadata.CreatedAt,
                            doc.RecordMetadata.CreatedBy,
                            doc.RecordMetadata.UpdatedAt,
                            doc.RecordMetadata.UpdatedBy
                        );

                        totalProcessed++;
                        break;
                    }
                    catch (YdbException e) when (e.IsTransient)
                    {
                        await Task.Delay(attempt * 1000);
                        logger.LogInformation(e, "Transient error during add row, attempt {Attempt}", attempt);

                        if (attempt == 9)
                        {
                            throw;
                        }
                    }
                }
            }

            logger.LogInformation("Index {IndexName}: processed {TotalProcessed}/{TotalDocuments} documents", indexName,
                totalProcessed, totalDocuments);

            for (var attempt = 0; attempt < 10; attempt++)
            {
                scrollResponse = await openSearchClient.ScrollAsync<Document>("5m", scrollId);
                if (!scrollResponse.IsValid)
                {
                    logger.LogError(scrollResponse.OriginalException, "Failed to scroll");

                    if (attempt == 9)
                    {
                        throw new Exception("Failed to scroll", scrollResponse.OriginalException);
                    }

                    continue;
                }

                if (scrollResponse.Documents.Count == 0)
                    return;

                break;
            }

            if (scrollResponse.Documents.Count != 0)
                continue;

            logger.LogInformation(
                "Index {IndexName}: Scroll completed - no more documents, processed {TotalProcessed}/{TotalDocuments}",
                indexName, totalProcessed, totalDocuments);

            break;
        }

        for (var attempt = 0; attempt < 10; attempt++)
        {
            try
            {
                await bulkUpsertImporter.FlushAsync();
                break;
            }
            catch (YdbException e) when (e.IsTransientWhenIdempotent)
            {
                await Task.Delay(attempt * 1000);
                logger.LogInformation(e, "Transient error during flush, attempt {Attempt}", attempt);

                if (attempt == 9)
                {
                    throw;
                }
            }
        }
    }
    catch (Exception ex)
    {
        logger.LogError(ex,
            "Index {IndexName}: Error during processing, processed {TotalProcessed}/{TotalDocuments} documents",
            indexName, totalProcessed, totalDocuments);
        throw;
    }
    finally
    {
        if (!string.IsNullOrEmpty(scrollId))
        {
            try
            {
                await openSearchClient.ClearScrollAsync(c => c.ScrollId(scrollId));
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Index {IndexName}: Failed to clear scroll", indexName);
            }
        }
    }
}

byte[]? ConvertVectorToBytes(float[]? vector)
{
    if (vector == null)
    {
        return null;
    }

    const int floatSize = sizeof(float);
    var result = new byte[vector.Length * floatSize + 1];

    for (var i = 0; i < vector.Length; i++)
    {
        var bytes = BitConverter.GetBytes(vector[i]);
        Array.Copy(bytes, 0, result, i * floatSize, floatSize);
    }

    result[^1] = 0x01;
    return result;
}

internal class Document
{
    [JsonRequired]
    [JsonPropertyName("chunkMetadata")]
    public ChunkMetadata ChunkMetadata { get; set; } = null!;

    [JsonRequired]
    [JsonPropertyName("chunkText")]
    public string ChunkText { get; set; } = null!;

    [JsonPropertyName("chunkVector")] public float[]? ChunkVector { get; set; } = null;

    [JsonRequired]
    [JsonPropertyName("recordMetadata")]
    public RecordMetadata RecordMetadata { get; set; } = null!;
}

internal class ChunkMetadata
{
    [JsonRequired]
    [JsonPropertyName("chunkId")]
    public string ChunkId { get; set; } = null!;

    [JsonRequired]
    [JsonPropertyName("fileId")]
    public string FileId { get; set; } = null!;

    [JsonRequired]
    [JsonPropertyName("folderId")]
    public string FolderId { get; set; } = null!;
}

internal class RecordMetadata
{
    [JsonRequired]
    [JsonPropertyName("createdAt")]
    public DateTime CreatedAt { get; set; }

    [JsonRequired]
    [JsonPropertyName("createdBy")]
    public string CreatedBy { get; set; } = null!;

    [JsonRequired]
    [JsonPropertyName("updatedAt")]
    public DateTime UpdatedAt { get; set; }

    [JsonRequired]
    [JsonPropertyName("updatedBy")]
    public string UpdatedBy { get; set; } = null!;
}