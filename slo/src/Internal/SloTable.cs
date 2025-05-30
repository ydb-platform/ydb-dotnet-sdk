using System.ComponentModel.DataAnnotations.Schema;

namespace Internal;

[Table("slo_test_table")]
public class SloTable
{
    public const string Name = "slo_test_table";

    public const string Options = $"""
                                   ALTER TABLE {Name} SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);
                                   ALTER TABLE {Name} SET (AUTO_PARTITIONING_BY_LOAD = ENABLED);
                                   ALTER TABLE {Name} SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5);
                                   ALTER TABLE {Name} SET (AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 10);
                                   """;

    public Guid Guid { get; set; }
    public int Id { get; set; }

    public string PayloadStr { get; set; }
    public double PayloadDouble { get; set; }
    public DateTime PayloadTimestamp { get; set; }
}