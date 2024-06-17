namespace Ydb.Sdk.Ado;

public class YdbAdoException : Exception
{
    public YdbAdoException()
    {
    }

    public YdbAdoException(string message) : base(message)
    {
    }

    public YdbAdoException(Status status) : base(status.ToString())
    {
    }
}
