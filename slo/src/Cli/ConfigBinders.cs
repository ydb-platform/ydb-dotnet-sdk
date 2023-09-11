using System.CommandLine;
using System.CommandLine.Binding;

namespace slo.Cli;

internal class CreateConfigBinder : BinderBase<CreateConfig>
{
    private readonly Argument<string> _dbArgument;
    private readonly Argument<string> _endpointArgument;
    private readonly Option<int> _initialDataCountOption;
    private readonly Option<int> _maxPartitionsCountOption;
    private readonly Option<int> _minPartitionsCountOption;
    private readonly Option<int> _partitionSizeOption;
    private readonly Option<string> _tableOption;
    private readonly Option<int> _writeTimeoutOption;

    public CreateConfigBinder(Argument<string> endpointArgument, Argument<string> dbArgument,
        Option<string> tableOption, Option<int> minPartitionsCountOption, Option<int> maxPartitionsCountOption,
        Option<int> partitionSizeOption, Option<int> initialDataCountOption, Option<int> writeTimeoutOption)
    {
        _endpointArgument = endpointArgument;
        _dbArgument = dbArgument;
        _tableOption = tableOption;
        _minPartitionsCountOption = minPartitionsCountOption;
        _maxPartitionsCountOption = maxPartitionsCountOption;
        _partitionSizeOption = partitionSizeOption;
        _initialDataCountOption = initialDataCountOption;
        _writeTimeoutOption = writeTimeoutOption;
    }

    protected override CreateConfig GetBoundValue(BindingContext bindingContext)
    {
        return new CreateConfig(
            bindingContext.ParseResult.GetValueForArgument(_endpointArgument),
            bindingContext.ParseResult.GetValueForArgument(_dbArgument),
            bindingContext.ParseResult.GetValueForOption(_tableOption)!,
            bindingContext.ParseResult.GetValueForOption(_minPartitionsCountOption),
            bindingContext.ParseResult.GetValueForOption(_maxPartitionsCountOption),
            bindingContext.ParseResult.GetValueForOption(_partitionSizeOption),
            bindingContext.ParseResult.GetValueForOption(_initialDataCountOption),
            bindingContext.ParseResult.GetValueForOption(_writeTimeoutOption)
        );
    }
}

internal class CleanUpConfigBinder : BinderBase<CleanUpConfig>
{
    private readonly Argument<string> _dbArgument;
    private readonly Argument<string> _endpointArgument;
    private readonly Option<string> _tableOption;
    private readonly Option<int> _writeTimeoutOption;

    public CleanUpConfigBinder(Argument<string> endpointArgument, Argument<string> dbArgument,
        Option<string> tableOption, Option<int> writeTimeoutOption)
    {
        _endpointArgument = endpointArgument;
        _dbArgument = dbArgument;
        _tableOption = tableOption;
        _writeTimeoutOption = writeTimeoutOption;
    }

    protected override CleanUpConfig GetBoundValue(BindingContext bindingContext)
    {
        return new CleanUpConfig(
            bindingContext.ParseResult.GetValueForArgument(_endpointArgument),
            bindingContext.ParseResult.GetValueForArgument(_dbArgument),
            bindingContext.ParseResult.GetValueForOption(_tableOption)!,
            bindingContext.ParseResult.GetValueForOption(_writeTimeoutOption)
        );
    }
}

internal class RunConfigBinder : BinderBase<RunConfig>
{
    private readonly Argument<string> _dbArgument;
    private readonly Argument<string> _endpointArgument;
    private readonly Option<int> _initialDataCountOption;
    private readonly Option<string> _promPgwOption;
    private readonly Option<int> _readRpsOption;
    private readonly Option<int> _readTimeoutOption;
    private readonly Option<int> _reportPeriodOption;
    private readonly Option<int> _shutdownTimeOption;
    private readonly Option<string> _tableOption;
    private readonly Option<int> _timeOption;
    private readonly Option<int> _writeRpsOption;
    private readonly Option<int> _writeTimeoutOption;

    public RunConfigBinder(Argument<string> endpointArgument, Argument<string> dbArgument,
        Option<string> tableOption, Option<int> initialDataCountOption, Option<string> promPgwOption,
        Option<int> reportPeriodOption, Option<int> readRpsOption, Option<int> readTimeoutOption,
        Option<int> writeRpsOption, Option<int> writeTimeoutOption, Option<int> timeOption,
        Option<int> shutdownTimeOption)
    {
        _endpointArgument = endpointArgument;
        _dbArgument = dbArgument;
        _tableOption = tableOption;
        _initialDataCountOption = initialDataCountOption;
        _promPgwOption = promPgwOption;
        _reportPeriodOption = reportPeriodOption;
        _readRpsOption = readRpsOption;
        _readTimeoutOption = readTimeoutOption;
        _writeRpsOption = writeRpsOption;
        _writeTimeoutOption = writeTimeoutOption;
        _timeOption = timeOption;
        _shutdownTimeOption = shutdownTimeOption;
    }

    protected override RunConfig GetBoundValue(BindingContext bindingContext)
    {
        return new RunConfig(
            bindingContext.ParseResult.GetValueForArgument(_endpointArgument),
            bindingContext.ParseResult.GetValueForArgument(_dbArgument),
            bindingContext.ParseResult.GetValueForOption(_tableOption)!,
            bindingContext.ParseResult.GetValueForOption(_initialDataCountOption),
            bindingContext.ParseResult.GetValueForOption(_promPgwOption)!,
            bindingContext.ParseResult.GetValueForOption(_reportPeriodOption),
            bindingContext.ParseResult.GetValueForOption(_readRpsOption),
            bindingContext.ParseResult.GetValueForOption(_readTimeoutOption),
            bindingContext.ParseResult.GetValueForOption(_writeRpsOption),
            bindingContext.ParseResult.GetValueForOption(_writeTimeoutOption),
            bindingContext.ParseResult.GetValueForOption(_timeOption),
            bindingContext.ParseResult.GetValueForOption(_shutdownTimeOption)
        );
    }
}