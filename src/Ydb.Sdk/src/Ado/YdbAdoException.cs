namespace Ydb.Sdk.Ado;

public class YdbAdoException : Exception
{
    internal YdbAdoException(string message) : base(message)
    {
    }
}
