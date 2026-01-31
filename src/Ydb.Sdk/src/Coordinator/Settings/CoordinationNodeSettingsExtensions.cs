namespace Ydb.Sdk.Coordinator.Settings;

// намудрил?
public static class CoordinationNodeSettingsExtensions
{
    /// <summary>
    /// Устанавливает таймаут выполнения операции.
    /// </summary>
    public static CoordinationNodeSettings WithOperationTimeout(
        this CoordinationNodeSettings settings,
        TimeSpan timeout)
    {
        settings.OperationTimeout = timeout;
        return settings;
    }

    /// <summary>
    /// Устанавливает таймаут отмены операции.
    /// </summary>
    public static CoordinationNodeSettings WithCancelTimeout(
        this CoordinationNodeSettings settings,
        TimeSpan timeout)
    {
        settings.CancelTimeout = timeout;
        return settings;
    }

    /// <summary>
    /// Включает асинхронный режим выполнения операции.
    /// </summary>
    public static CoordinationNodeSettings AsAsync(this CoordinationNodeSettings settings)
    {
        settings.IsAsyncMode = true;
        return settings;
    }

    /// <summary>
    /// Выключает асинхронный режим выполнения операции (делает её синхронной).
    /// </summary>
    public static CoordinationNodeSettings AsSync(this CoordinationNodeSettings settings)
    {
        settings.IsAsyncMode = false;
        return settings;
    }

    /// <summary>
    /// Включает отправку информации о стоимости операции.
    /// </summary>
    public static CoordinationNodeSettings WithCostInfo(this CoordinationNodeSettings settings)
    {
        settings.ReportCostInfo = true;
        return settings;
    }

    /// <summary>
    /// Выключает отправку информации о стоимости операции.
    /// </summary>
    public static CoordinationNodeSettings WithoutCostInfo(this CoordinationNodeSettings settings)
    {
        settings.ReportCostInfo = false;
        return settings;
    }

    /*
    /// <summary>
    /// Устанавливает конфигурацию ноды.
    /// </summary>
    public static CoordinationNodeSettings WithConfig(
        this CoordinationNodeSettings settings,
        NodeConfig config)
    {
        settings.Config = config ?? throw new ArgumentNullException(nameof(config));
        return settings;
    }
    */
}
