namespace Ydb.Sdk.Auth;

/// <summary>
/// Interface for providing authentication credentials to YDB.
/// </summary>
/// <remarks>
/// ICredentialsProvider defines the contract for authentication credential providers.
/// Implementations should handle token retrieval and refresh as needed.
/// </remarks>
public interface ICredentialsProvider
{
    /// <summary>
    /// Gets the authentication information asynchronously.
    /// </summary>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the authentication token.</returns>
    ValueTask<string> GetAuthInfoAsync();
}

/// <summary>
/// Interface for authentication clients.
/// </summary>
public interface IAuthClient
{
    Task<TokenResponse> FetchToken();
}
