using System.Diagnostics;

namespace Ydb.Sdk;

public abstract class Result
{
    public static Result<TValue> Success<TValue>(TValue value) where TValue : class
    {
        return new Result<TValue>(Status.Success, value);
    }

    public static Result<TValue> Fail<TValue>(Status status) where TValue : class
    {
        Debug.Assert(status.IsNotSuccess);

        return new Result<TValue>(status, null);
    }
}

public class Result<T> where T : class
{
    private readonly T? _value;

    public Status Status { get; }
    public bool IsSuccess => Status.IsSuccess;

    public T Value
    {
        get
        {
            if (Status.IsNotSuccess)
            {
                throw new UnexpectedResultException("Can't get value", Status);
            }

            return _value ?? throw new InvalidOperationException(
                "Invalid state result: the success result has a null value!");
        }
    }

    internal Result(Status status, T? value)
    {
        Status = status;
        _value = value;
    }

    public void Deconstruct(out Status status, out T? res)
    {
        status = Status;
        res = _value;
    }
}

public class UnexpectedResultException : Exception
{
    public Status Status { get; }

    public UnexpectedResultException(string message, Status status) : base(message + ": " + status)
    {
        Status = status;
    }

    public UnexpectedResultException(Status status) : base(status.ToString())
    {
        Status = status;
    }
}
