using Ydb.Scheme;
using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Operations;
using Ydb.Sdk.Value;
using Ydb.Table;
using Ydb.Table.V1;

namespace Ydb.Sdk.Services.Table;

public enum FeatureFlagStatus
{
    Unspecified,
    Enabled,
    Disabled
}

public enum IndexType
{
    None,
    GlobalIndex,
    GlobalAsyncIndex
}

internal static class TableEnumConverter
{
    internal static FeatureFlagStatus FromProto(this FeatureFlag.Types.Status proto) =>
        proto switch
        {
            FeatureFlag.Types.Status.Unspecified => FeatureFlagStatus.Unspecified,
            FeatureFlag.Types.Status.Enabled => FeatureFlagStatus.Enabled,
            FeatureFlag.Types.Status.Disabled => FeatureFlagStatus.Disabled,
            _ => throw new ArgumentOutOfRangeException()
        };

    internal static FeatureFlag.Types.Status GetProto(this FeatureFlagStatus status) =>
        status switch
        {
            FeatureFlagStatus.Unspecified => FeatureFlag.Types.Status.Unspecified,
            FeatureFlagStatus.Enabled => FeatureFlag.Types.Status.Enabled,
            FeatureFlagStatus.Disabled => FeatureFlag.Types.Status.Disabled,
            _ => throw new ArgumentOutOfRangeException(nameof(status), status, null)
        };
}

public class PartitionStats
{
    public ulong RowsEstimate { get; }
    public ulong StoreSize { get; }

    internal PartitionStats(Ydb.Table.PartitionStats proto)
    {
        RowsEstimate = proto.RowsEstimate;
        StoreSize = proto.StoreSize;
    }
}

public class TableStats
{
    public IReadOnlyList<PartitionStats>? PartitionStats { get; }
    public ulong RowsEstimate { get; }
    public ulong StoreSize { get; }
    public ulong Partitions { get; }
    public DateTime CreationTime { get; }
    public DateTime? ModificationTime { get; }

    internal TableStats(Ydb.Table.TableStats? proto)
    {
        if (proto == null)
        {
            return;
        }

        PartitionStats = proto.PartitionStats.Select(partitionStats => new PartitionStats(partitionStats)).ToList();
        RowsEstimate = proto.RowsEstimate;
        StoreSize = proto.StoreSize;
        Partitions = proto.Partitions;
        CreationTime = proto.CreationTime.ToDateTime();
        ModificationTime = proto.ModificationTime?.ToDateTime();
    }
}

public class DateTypeColumnModeSettings
{
    public string ColumnName { get; }
    public uint ExpireAfterSeconds { get; }

    public DateTypeColumnModeSettings(uint expireAfterSeconds, string columnName)
    {
        ExpireAfterSeconds = expireAfterSeconds;
        ColumnName = columnName;
    }

    public DateTypeColumnModeSettings(Ydb.Table.DateTypeColumnModeSettings proto)
    {
        ColumnName = proto.ColumnName;
        ExpireAfterSeconds = proto.ExpireAfterSeconds;
    }

    public Ydb.Table.DateTypeColumnModeSettings GetProto() =>
        new()
        {
            ColumnName = ColumnName,
            ExpireAfterSeconds = ExpireAfterSeconds
        };
}

public class ValueSinceUnixEpochModeSettings
{
    public enum Unit
    {
        Unspecified = 0,
        Seconds = 1,
        Milliseconds = 2,
        Microseconds = 3,
        Nanoseconds = 4
    }

    public string ColumnName { get; }
    public Unit ColumnUnit { get; }
    public uint ExpireAfterSeconds { get; }

    public ValueSinceUnixEpochModeSettings(string columnName, Unit columnUnit, uint expireAfterSeconds)
    {
        ColumnName = columnName;
        ColumnUnit = columnUnit;
        ExpireAfterSeconds = expireAfterSeconds;
    }

    public ValueSinceUnixEpochModeSettings(Ydb.Table.ValueSinceUnixEpochModeSettings proto)
    {
        ColumnName = proto.ColumnName;
        ColumnUnit = proto.ColumnUnit switch
        {
            Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Unspecified => Unit.Unspecified,
            Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Seconds => Unit.Seconds,
            Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Milliseconds => Unit.Milliseconds,
            Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Microseconds => Unit.Microseconds,
            Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Nanoseconds => Unit.Nanoseconds,
            _ => throw new ArgumentOutOfRangeException()
        };
        ExpireAfterSeconds = proto.ExpireAfterSeconds;
    }

    public Ydb.Table.ValueSinceUnixEpochModeSettings GetProto() =>
        new()
        {
            ColumnName = ColumnName,
            ColumnUnit = GetProtoUnit(ColumnUnit),
            ExpireAfterSeconds = ExpireAfterSeconds
        };

    private static Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit GetProtoUnit(Unit unit) =>
        unit switch
        {
            Unit.Unspecified => Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Unspecified,
            Unit.Seconds => Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Seconds,
            Unit.Milliseconds => Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Milliseconds,
            Unit.Microseconds => Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Microseconds,
            Unit.Nanoseconds => Ydb.Table.ValueSinceUnixEpochModeSettings.Types.Unit.Nanoseconds,
            _ => throw new ArgumentOutOfRangeException(nameof(unit), unit, null)
        };
}

public class TtlSettingsMode
{
    public enum ModeType
    {
        None,
        DateTypeColumn,
        ValueSinceUnixEpoch
    }

    public ModeType Type { get; }
    public DateTypeColumnModeSettings? DateTypeColumnModeSettings { get; }
    public ValueSinceUnixEpochModeSettings? ValueSinceUnixEpochModeSettings { get; }

    public TtlSettingsMode(DateTypeColumnModeSettings settings)
    {
        Type = ModeType.DateTypeColumn;
        DateTypeColumnModeSettings = settings;
    }

    public TtlSettingsMode(ValueSinceUnixEpochModeSettings settings)
    {
        Type = ModeType.DateTypeColumn;
        ValueSinceUnixEpochModeSettings = settings;
    }
}

public class TtlSettings
{
    public TtlSettingsMode Mode { get; }
    public uint RunIntervalSeconds { get; }

    private TtlSettings(TtlSettingsMode mode, uint runIntervalSeconds)
    {
        Mode = mode;
        RunIntervalSeconds = runIntervalSeconds;
    }

    public static TtlSettings? FromProto(Ydb.Table.TtlSettings? proto)
    {
        if (proto is null)
            return null;
        return proto.ModeCase switch
        {
            Ydb.Table.TtlSettings.ModeOneofCase.DateTypeColumn => new TtlSettings(
                new TtlSettingsMode(new DateTypeColumnModeSettings(proto.DateTypeColumn)),
                proto.RunIntervalSeconds),
            Ydb.Table.TtlSettings.ModeOneofCase.ValueSinceUnixEpoch => new TtlSettings(
                new TtlSettingsMode(new ValueSinceUnixEpochModeSettings(proto.ValueSinceUnixEpoch)),
                proto.RunIntervalSeconds),
            Ydb.Table.TtlSettings.ModeOneofCase.None => null,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public Ydb.Table.TtlSettings GetProto()
    {
        var proto = new Ydb.Table.TtlSettings
        {
            RunIntervalSeconds = RunIntervalSeconds
        };
        switch (Mode.Type)
        {
            case TtlSettingsMode.ModeType.DateTypeColumn:
                proto.DateTypeColumn = Mode.DateTypeColumnModeSettings?.GetProto();
                break;
            case TtlSettingsMode.ModeType.ValueSinceUnixEpoch:
                proto.ValueSinceUnixEpoch = Mode.ValueSinceUnixEpochModeSettings?.GetProto();
                break;
            case TtlSettingsMode.ModeType.None:
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        return proto;
    }
}

public class TableIndexDescription
{
    public enum TableIndexDescriptionStatus
    {
        Unspecified,
        Ready,
        Building
    }

    public string Name { get; }
    public IReadOnlyList<string> IndexColumns { get; }
    public TableIndexDescriptionStatus Status { get; }
    public IReadOnlyList<string> DataColumns { get; }
    public ulong SizeBytes { get; }

    public IndexType IndexType { get; }

    internal TableIndexDescription(Ydb.Table.TableIndexDescription proto)
    {
        Name = proto.Name;
        IndexColumns = proto.IndexColumns;
        Status = proto.Status switch
        {
            Ydb.Table.TableIndexDescription.Types.Status.Unspecified => TableIndexDescriptionStatus.Unspecified,
            Ydb.Table.TableIndexDescription.Types.Status.Ready => TableIndexDescriptionStatus.Ready,
            Ydb.Table.TableIndexDescription.Types.Status.Building => TableIndexDescriptionStatus.Building,
            _ => throw new ArgumentOutOfRangeException()
        };
        DataColumns = proto.DataColumns;
        SizeBytes = proto.SizeBytes;

        IndexType = proto.TypeCase switch
        {
            Ydb.Table.TableIndexDescription.TypeOneofCase.GlobalIndex => IndexType.GlobalIndex,
            Ydb.Table.TableIndexDescription.TypeOneofCase.GlobalAsyncIndex => IndexType.GlobalAsyncIndex,
            Ydb.Table.TableIndexDescription.TypeOneofCase.None => IndexType.None,
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}

public class StoragePool
{
    public string? Media { get; }

    public StoragePool(string media)
    {
        Media = media;
    }

    public StoragePool(Ydb.Table.StoragePool? proto)
    {
        Media = proto?.Media;
    }

    public Ydb.Table.StoragePool GetProto() => new() { Media = Media };
}

public class StorageSettings
{
    public StoragePool TabletCommitLog0 { get; }
    public StoragePool TabletCommitLog1 { get; }
    public StoragePool External { get; }
    public FeatureFlagStatus StoreExternalBlobs { get; }

    public StorageSettings(StoragePool tabletCommitLog0, StoragePool tabletCommitLog1, StoragePool external,
        FeatureFlagStatus storeExternalBlobs)
    {
        TabletCommitLog0 = tabletCommitLog0;
        TabletCommitLog1 = tabletCommitLog1;
        External = external;
        StoreExternalBlobs = storeExternalBlobs;
    }

    public StorageSettings(Ydb.Table.StorageSettings proto)
    {
        TabletCommitLog0 = new StoragePool(proto.TabletCommitLog0);
        TabletCommitLog1 = new StoragePool(proto.TabletCommitLog1);
        External = new StoragePool(proto.External);
        StoreExternalBlobs = proto.StoreExternalBlobs.FromProto();
    }

    public Ydb.Table.StorageSettings GetProto() =>
        new()
        {
            TabletCommitLog0 = TabletCommitLog0.GetProto(),
            TabletCommitLog1 = TabletCommitLog1.GetProto(),
            External = External.GetProto(),
            StoreExternalBlobs = StoreExternalBlobs.GetProto()
        };
}

public class PartitioningSettings
{
    public List<string> PartitionBy { get; }
    public FeatureFlagStatus PartitioningBySize { get; }
    public ulong PartitionSizeMb { get; }
    public FeatureFlagStatus PartitioningByLoad { get; }
    public ulong MinPartitionsCount { get; }
    public ulong MaxPartitionsCount { get; }

    public PartitioningSettings(List<string> partitionBy, FeatureFlagStatus partitioningBySize, ulong partitionSizeMb,
        FeatureFlagStatus partitioningByLoad, ulong minPartitionsCount, ulong maxPartitionsCount)
    {
        PartitionBy = partitionBy;
        PartitioningBySize = partitioningBySize;
        PartitionSizeMb = partitionSizeMb;
        PartitioningByLoad = partitioningByLoad;
        MinPartitionsCount = minPartitionsCount;
        MaxPartitionsCount = maxPartitionsCount;
    }

    public PartitioningSettings(Ydb.Table.PartitioningSettings proto)
    {
        PartitionBy = proto.PartitionBy.ToList();
        PartitioningBySize = proto.PartitioningBySize.FromProto();
        PartitionSizeMb = proto.PartitionSizeMb;
        PartitioningByLoad = proto.PartitioningByLoad.FromProto();
        MinPartitionsCount = proto.MinPartitionsCount;
        MaxPartitionsCount = proto.MaxPartitionsCount;
    }

    public Ydb.Table.PartitioningSettings GetProto() => new()
    {
        PartitionBy = { PartitionBy },
        PartitioningBySize = PartitioningBySize.GetProto(),
        PartitionSizeMb = PartitionSizeMb,
        PartitioningByLoad = PartitioningByLoad.GetProto(),
        MinPartitionsCount = MinPartitionsCount,
        MaxPartitionsCount = MaxPartitionsCount
    };
}

public class ReadReplicasSettings
{
    public enum SettingsType
    {
        None,
        PerAzReadReplicasCount,
        AnyAzReadReplicasCount
    }

    public SettingsType Type { get; }
    public ulong Settings { get; }

    public ReadReplicasSettings(SettingsType type, ulong settings)
    {
        Type = type;
        Settings = settings;
    }

    public ReadReplicasSettings(Ydb.Table.ReadReplicasSettings? proto)
    {
        if (proto is null)
        {
            return;
        }

        switch (proto.SettingsCase)
        {
            case Ydb.Table.ReadReplicasSettings.SettingsOneofCase.None:
                Type = SettingsType.None;
                Settings = default;
                break;
            case Ydb.Table.ReadReplicasSettings.SettingsOneofCase.PerAzReadReplicasCount:
                Type = SettingsType.PerAzReadReplicasCount;
                Settings = proto.PerAzReadReplicasCount;
                break;
            case Ydb.Table.ReadReplicasSettings.SettingsOneofCase.AnyAzReadReplicasCount:
                Type = SettingsType.AnyAzReadReplicasCount;
                Settings = proto.AnyAzReadReplicasCount;
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public Ydb.Table.ReadReplicasSettings GetProto() => Type switch
    {
        SettingsType.None => new Ydb.Table.ReadReplicasSettings(),
        SettingsType.PerAzReadReplicasCount => new Ydb.Table.ReadReplicasSettings
            { PerAzReadReplicasCount = Settings },
        SettingsType.AnyAzReadReplicasCount => new Ydb.Table.ReadReplicasSettings
            { AnyAzReadReplicasCount = Settings },
        _ => throw new ArgumentOutOfRangeException()
    };
}

public class DescribeTableSettings : OperationSettings
{
    public bool IncludeShardKeyBounds { get; private set; }
    public bool IncludeTableStats { get; private set; }
    public bool IncludePartitionStats { get; private set; }

    public DescribeTableSettings WithShardKeyBounds(bool include = true)
    {
        IncludeShardKeyBounds = include;
        return this;
    }

    public DescribeTableSettings WithTableStats(bool include = true)
    {
        IncludeTableStats = include;
        return this;
    }

    public DescribeTableSettings WithPartitionStats(bool include = true)
    {
        IncludePartitionStats = include;
        return this;
    }
}

public class DescribeTableResponse : ResponseWithResultBase<DescribeTableResponse.ResultData>
{
    internal DescribeTableResponse(Status status, ResultData? result = null) : base(status, result)
    {
    }

    public class ResultData
    {
        public Entry Self { get; }
        public IReadOnlyList<ColumnMeta> Columns { get; }
        public IReadOnlyList<string> PrimaryKey { get; }
        public IReadOnlyList<YdbValue> ShardKeyBounds { get; }
        public IReadOnlyList<TableIndexDescription> Indexes { get; }
        public TableStats TableStats { get; }
        public TtlSettings? TtlSettings { get; }
        public StorageSettings StorageSettings { get; }
        public IReadOnlyList<ColumnFamily> ColumnFamilies { get; }
        public IReadOnlyDictionary<string, string> Attributes { get; }
        public PartitioningSettings PartitioningSettings { get; }
        public FeatureFlagStatus KeyBloomFilter { get; }
        public ReadReplicasSettings ReadReplicasSettings { get; }

        public ResultData(Entry self, IReadOnlyList<ColumnMeta> columns, IReadOnlyList<string> primaryKey,
            IReadOnlyList<YdbValue> shardKeyBounds, IReadOnlyList<TableIndexDescription> indexes,
            TableStats tableStats, TtlSettings? ttlSettings, StorageSettings storageSettings,
            IReadOnlyList<ColumnFamily> columnFamilies, IReadOnlyDictionary<string, string> attributes,
            PartitioningSettings partitioningSettings, FeatureFlagStatus keyBloomFilter,
            ReadReplicasSettings readReplicasSettings)
        {
            Self = self;
            Columns = columns;
            PrimaryKey = primaryKey;
            ShardKeyBounds = shardKeyBounds;
            Indexes = indexes;
            TableStats = tableStats;
            TtlSettings = ttlSettings;
            StorageSettings = storageSettings;
            ColumnFamilies = columnFamilies;
            Attributes = attributes;
            PartitioningSettings = partitioningSettings;
            KeyBloomFilter = keyBloomFilter;
            ReadReplicasSettings = readReplicasSettings;
        }

        internal static ResultData FromProto(DescribeTableResult resultProto) =>
            new(
                self: resultProto.Self,
                columns: resultProto.Columns.Select(proto => new ColumnMeta(proto)).ToList(),
                primaryKey: resultProto.PrimaryKey.ToList(),
                shardKeyBounds: resultProto.ShardKeyBounds.Select(proto => new YdbValue(proto.Type, proto.Value))
                    .ToList(),
                indexes: resultProto.Indexes.Select(proto => new TableIndexDescription(proto)).ToList(),
                tableStats: new TableStats(resultProto.TableStats),
                ttlSettings: TtlSettings.FromProto(resultProto.TtlSettings),
                storageSettings: new StorageSettings(resultProto.StorageSettings),
                columnFamilies: resultProto.ColumnFamilies.Select(proto => new ColumnFamily(proto)).ToList(),
                attributes: new Dictionary<string, string>(resultProto.Attributes),
                partitioningSettings: new PartitioningSettings(resultProto.PartitioningSettings),
                keyBloomFilter: resultProto.KeyBloomFilter.FromProto(),
                readReplicasSettings: new ReadReplicasSettings(resultProto.ReadReplicasSettings)
            );
    }
}

public partial class TableClient
{
    public async Task<DescribeTableResponse> DescribeTable(string tablePath, DescribeTableSettings? settings = null)
    {
        settings ??= new DescribeTableSettings();
        var request = new DescribeTableRequest
        {
            OperationParams = settings.MakeOperationParams(),
            Path = MakeTablePath(tablePath),
            IncludeShardKeyBounds = settings.IncludeShardKeyBounds,
            IncludeTableStats = settings.IncludeTableStats,
            IncludePartitionStats = settings.IncludePartitionStats
        };

        var response = await _driver.UnaryCall(
            method: TableService.DescribeTableMethod,
            request: request,
            settings: settings
        );

        var status = response.Operation.TryUnpack(out DescribeTableResult? resultProto);
        DescribeTableResponse.ResultData? result = null;

        if (status.IsSuccess && resultProto is not null)
        {
            result = DescribeTableResponse.ResultData.FromProto(resultProto);
        }

        return new DescribeTableResponse(status, result);
    }
}
