namespace Ydb.Sdk.Retry.Classifier;

internal interface IRetryClassifier
{
    Failure? Classify(Exception ex);
}
