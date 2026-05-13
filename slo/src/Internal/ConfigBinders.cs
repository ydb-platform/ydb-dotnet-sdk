using System.CommandLine;
using System.CommandLine.Binding;

namespace Internal;

internal class RunConfigBinder(
    Argument<string> connectionString,
    Option<string?> otlpEndpointOption,
    Option<int> reportPeriodOption,
    Option<int> readRpsOption,
    Option<int> readTimeoutOption,
    Option<int> writeRpsOption,
    Option<int> writeTimeoutOption,
    Option<int> timeOption,
    Option<int> initialDataCountOption
) : BinderBase<SloConfig>
{
    protected override SloConfig GetBoundValue(BindingContext bindingContext) =>
        new(
            bindingContext.ParseResult.GetValueForArgument(connectionString),
            bindingContext.ParseResult.GetValueForOption(otlpEndpointOption),
            bindingContext.ParseResult.GetValueForOption(reportPeriodOption),
            bindingContext.ParseResult.GetValueForOption(readRpsOption),
            bindingContext.ParseResult.GetValueForOption(readTimeoutOption),
            bindingContext.ParseResult.GetValueForOption(writeRpsOption),
            bindingContext.ParseResult.GetValueForOption(writeTimeoutOption),
            bindingContext.ParseResult.GetValueForOption(timeOption),
            bindingContext.ParseResult.GetValueForOption(initialDataCountOption)
        );
}