## v1.0.1

- Fix: Replaced `OpenTelemetry` SDK dependency (>= 1.15.0) with `OpenTelemetry.Api` (>= 1.15.3). The package now only
  relies on the public OpenTelemetry API surface (`TracerProviderBuilder` / `MeterProviderBuilder`), so it can be added
  to projects that still use older OpenTelemetry SDK / Hosting / Exporter packages (e.g. 1.10.x) without triggering
  `NU1605` downgrade errors.

## v1.0.0

- Feat: Initial release. Extension methods for registering `Ydb.Sdk` as an OpenTelemetry source.
    - `TracerProviderBuilder.AddYdb()` — subscribes to the `Ydb.Sdk` activity source (ActivitySource name: `Ydb.Sdk`).
    - `MeterProviderBuilder.AddYdb()` — subscribes to the `Ydb.Sdk` meter (Meter name: `Ydb.Sdk`).
