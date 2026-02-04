# YDB .NET Ecosystem

[![Telegram](https://img.shields.io/badge/Telegram-Русский_чат-2ba2d9.svg?logo=telegram)](https://t.me/ydb_ru)
[![Telegram](https://img.shields.io/badge/Telegram-English_chat-2ba2d9.svg?logo=telegram)](https://t.me/ydb_en)
[![WebSite](https://img.shields.io/badge/website-ydb.tech-blue.svg)](https://ydb.tech)

## Overview

This repository contains all official C# components for working with YDB:

- **Ydb.Sdk** - Core SDK includes an ADO.NET provider and a topic (Writer / Reader) client.
- **EntityFrameworkCore.Ydb** - Entity Framework Core integration.

## Packages

| Package   | NuGet                                                                                                                      | Readme                               | Documentation                                                                                                                           |
|-----------|----------------------------------------------------------------------------------------------------------------------------|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `Ydb.Sdk` | [![NuGet](https://img.shields.io/nuget/v/Ydb.Sdk)](https://www.nuget.org/packages/Ydb.Sdk)                                 | [README](./src/Ydb.Sdk/README.md)    | [ADO.NET](https://ydb.tech/docs/en/reference/languages-and-apis/ado-net), [YDB Topic](https://ydb.tech/docs/en/reference/ydb-sdk/topic) |
| `Ydb.Sdk` | [![NuGet](https://img.shields.io/nuget/v/EntityFrameworkCore.Ydb)](https://www.nuget.org/packages/EntityFrameworkCore.Ydb) | [README](./src/EFCore.Ydb/README.md) | [link](https://ydb.tech/docs/en/integrations/orm/entity-framework?version=main)                                                         |

## Versioning

We follow the **[SemVer 2.0.0](https://semver.org)**. In particular, we provide backward compatibility in the `MAJOR`
releases. New features without loss of backward compatibility appear on the `MINOR` release. In the minor version, the
patch number starts from `0`. Bug fixes and internal changes are released with the third digit (`PATCH`) in the version.

Major version zero (`0.y.z`) is considered prerelease and **do not guarantee any backward compatibility**.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License

This repository is licensed under the Apache 2.0 License.

## Examples

See **[examples folder](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples)**
