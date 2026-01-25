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

    string GetDatabase();

    // временно закомментировано 
    /*
    //ICoordinationSession CreateSession(string path, CoordinationSessionSettings settings);
    */

    Task CreateNode(string path, CoordinationNodeSettings settings);

    Task AlterNode(string path, CoordinationNodeSettings settings);

    Task DropNode(string path, DropCoordinationNodeSettings settings);


    Task<NodeConfig> DescribeNode(string path, DescribeCoordinationNodeSettings settings);


    // --------------- default methods ------------------------------
}
