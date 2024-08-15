using System.CommandLine;
using System.CommandLine.Binding;

namespace Internal;

public class CreateConfigBinder(
    Argument<string> endpointArgument,
    Argument<string> dbArgument,
    Option<string> tableOption,
    Option<int> minPartitionsCountOption,
    Option<int> maxPartitionsCountOption,
    Option<int> partitionSizeOption,
    Option<int> initialDataCountOption,
    Option<int> writeTimeoutOption)
    : BinderBase<CreateConfig>
{
    protected override CreateConfig GetBoundValue(BindingContext bindingContext)
    {
        return new CreateConfig(
            bindingContext.ParseResult.GetValueForArgument(endpointArgument),
            bindingContext.ParseResult.GetValueForArgument(dbArgument),
            bindingContext.ParseResult.GetValueForOption(tableOption)!,
            bindingContext.ParseResult.GetValueForOption(minPartitionsCountOption),
            bindingContext.ParseResult.GetValueForOption(maxPartitionsCountOption),
            bindingContext.ParseResult.GetValueForOption(partitionSizeOption),
            bindingContext.ParseResult.GetValueForOption(initialDataCountOption),
            bindingContext.ParseResult.GetValueForOption(writeTimeoutOption)
        );
    }
}

internal class CleanUpConfigBinder(
    Argument<string> endpointArgument,
    Argument<string> dbArgument,
    Option<string> tableOption,
    Option<int> writeTimeoutOption)
    : BinderBase<CleanUpConfig>
{
    protected override CleanUpConfig GetBoundValue(BindingContext bindingContext)
    {
        return new CleanUpConfig(
            bindingContext.ParseResult.GetValueForArgument(endpointArgument),
            bindingContext.ParseResult.GetValueForArgument(dbArgument),
            bindingContext.ParseResult.GetValueForOption(tableOption)!,
            bindingContext.ParseResult.GetValueForOption(writeTimeoutOption)
        );
    }
}

internal class RunConfigBinder(
    Argument<string> endpointArgument,
    Argument<string> dbArgument,
    Option<string> tableOption,
    Option<string> promPgwOption,
    Option<int> reportPeriodOption,
    Option<int> readRpsOption,
    Option<int> readTimeoutOption,
    Option<int> writeRpsOption,
    Option<int> writeTimeoutOption,
    Option<int> timeOption,
    Option<int> shutdownTimeOption)
    : BinderBase<RunConfig>
{
    protected override RunConfig GetBoundValue(BindingContext bindingContext)
    {
        return new RunConfig(
            bindingContext.ParseResult.GetValueForArgument(endpointArgument),
            bindingContext.ParseResult.GetValueForArgument(dbArgument),
            bindingContext.ParseResult.GetValueForOption(tableOption)!,
            bindingContext.ParseResult.GetValueForOption(promPgwOption)!,
            bindingContext.ParseResult.GetValueForOption(reportPeriodOption),
            bindingContext.ParseResult.GetValueForOption(readRpsOption),
            bindingContext.ParseResult.GetValueForOption(readTimeoutOption),
            bindingContext.ParseResult.GetValueForOption(writeRpsOption),
            bindingContext.ParseResult.GetValueForOption(writeTimeoutOption),
            bindingContext.ParseResult.GetValueForOption(timeOption),
            bindingContext.ParseResult.GetValueForOption(shutdownTimeOption)
        );
    }
}