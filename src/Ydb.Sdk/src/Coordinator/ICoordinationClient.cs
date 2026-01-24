using Ydb.Sdk.Coordinator.Description;
using Ydb.Sdk.Coordinator.Settings;

namespace Ydb.Sdk.Coordinator;

public interface ICoordinationClient
{
    // перенести в крайнем на обычный класс
    /*
    static ICoordinationClient NewClient(GrpcTransport transport)
    {
        return CoordinationServiceImpl.NewClient(transport);
    }
    */
    
    /// <summary>
    /// Database path.
    /// Used for creating coordination node path.
    /// </summary>
    /// <returns>Path to database.</returns>
    string GetDatabase();
    
    // временно закомментировано 
    /*
    /// <summary>
    /// Creates a new coordination session.
    /// The coordination session establishes bidirectional grpc stream with a specific
    /// coordination node and uses this stream for exchanging messages with the coordination service.
    /// </summary>
    /// <param name="path">Full path to coordination node.</param>
    /// <param name="settings">Coordination session settings.</param>
    /// <returns>New instance of coordination session.</returns>
    //ICoordinationSession CreateSession(string path, CoordinationSessionSettings settings);
    */
    
    /// <summary>
    /// Creates a new coordination node.
    /// </summary>
    /// <param name="path">Full path to coordination node.</param>
    /// <param name="settings">Coordination node settings.</param>
    /// <returns>Task with status of operation.</returns> // исправить текст
    Task CreateNode(string path, CoordinationNodeSettings settings);
    
    /// <summary>
    /// Modifies settings of a coordination node.
    /// </summary>
    /// <param name="path">Full path to coordination node.</param>
    /// <param name="settings">Coordination node settings.</param>
    /// <returns>Task with status of operation.</returns> // исправить текст
    Task AlterNode(string path, CoordinationNodeSettings settings);
    
    /// <summary>
    /// Drops a coordination node.
    /// </summary>
    /// <param name="path">Full path to coordination node.</param>
    /// <param name="settings">Drop coordination node settings.</param>
    /// <returns>Task with status of operation.</returns> // исправить текст
    Task DropNode(string path, DropCoordinationNodeSettings settings);
    
    
    /// <summary>
    /// Describes a coordination node.
    /// </summary>
    /// <param name="path">Full path to coordination node.</param>
    /// <param name="settings">Describe coordination node settings.</param>
    /// <returns>Task with node configuration.</returns> // исправить текст
    Task<NodeConfig> DescribeNode(string path, DescribeCoordinationNodeSettings settings); 
    
    
    // --------------- default methods ------------------------------
    
    /*
    
    /// <summary>
    /// Creates a new coordination session with default settings.
    /// The coordination session establishes bidirectional grpc stream with a specific coordination node and uses this
    /// stream for exchanging messages with the coordination service.
    /// </summary>
    /// <param name="path">Full path to coordination node.</param>
    /// <returns>New instance of coordination session.</returns> 
    CoordinationSession CreateSession(string path)
        => CreateSession(path, CoordinationSessionSettings.NewBuilder().Build());
    
    /// <summary>
    /// Creates a new coordination node with default settings.
    /// </summary>
    /// <param name="path">Full path to coordination node.</param>
    /// <returns>Task with status of operation.</returns>// исправить текст
    Task CreateNode(string path)
        => CreateNode(path, CoordinationNodeSettings.NewBuilder().Build());
    
    /// <summary>
    /// Drops a coordination node with default settings.
    /// </summary>
    /// <param name="path">Full path to coordination node.</param>
    /// <returns>Task with status of operation.</returns> // исправить текст
    Task DropNode(string path)
        => DropNode(path, DropCoordinationNodeSettings.NewBuilder().Build());
    
    /// <summary>
    /// Describes a coordination node with default settings.
    /// </summary>
    /// <param name="path">Full path to coordination node.</param>
    /// <returns>Task with result of operation.</returns> // исправить текст
    Task<NodeConfig> DescribeNode(string path)
        => DescribeNode(path, DescribeCoordinationNodeSettings.NewBuilder().Build());
    
    */
    
}
