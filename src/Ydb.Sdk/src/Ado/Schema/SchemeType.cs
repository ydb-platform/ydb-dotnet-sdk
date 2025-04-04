namespace Ydb.Sdk.Ado.Schema;

internal enum SchemeType
{
    TypeUnspecified = 0,
    Directory = 1,
    Table = 2,
    PersQueueGroup = 3,
    Database = 4,
    RtmrVolume = 5,
    BlockStoreVolume = 6,
    CoordinationNode = 7,
    ColumnStore = 12,
    ColumnTable = 13,
    Sequence = 15,
    Replication = 16,
    Topic = 17,
    ExternalTable = 18,
    ExternalDataSource = 19,
    View = 20
}
