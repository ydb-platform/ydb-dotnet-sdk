using Ydb.Sdk.Ado;

await using var conn = new YdbConnection("Host=localhost;Port=2136;Database=/local");
await conn.OpenAsync();

await using (var setup = new YdbCommand(
    """
    CREATE TABLE IF NOT EXISTS `KiwiTest` (
        `Id` Int32 NOT NULL,
        `FoundOn` Uint8 NOT NULL,
        PRIMARY KEY (`Id`)
    );
    """, conn))
{
    await setup.ExecuteNonQueryAsync();
}

await using (var setup = new YdbCommand(
    "UPSERT INTO `KiwiTest` (`Id`, `FoundOn`) VALUES (1, 1);", conn))
{
    await setup.ExecuteNonQueryAsync();
}

async Task Try(string label, string sql)
{
    try
    {
        await using var cmd = new YdbCommand(sql, conn);
        var rows = await cmd.ExecuteNonQueryAsync();
        Console.WriteLine($"OK {label}: rows={rows}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"FAIL {label}: {ex.Message.Split('\n')[0]}");
    }
}

await Try("UPDATE ON 0ut", """
    UPDATE `KiwiTest` ON
    SELECT `Id`, 0ut AS `FoundOn`
    FROM `KiwiTest`
    WHERE `Id` = 1
    """);

await Try("UPDATE ON Uint8()", """
    UPDATE `KiwiTest` ON
    SELECT `Id`, Uint8("0") AS `FoundOn`
    FROM `KiwiTest`
    WHERE `Id` = 1
    """);

await Try("simple SET 0ut", "UPDATE `KiwiTest` SET `FoundOn` = 0ut WHERE `Id` = 1");
