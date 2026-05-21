# Release Gates

This repository has two levels of verification:

- The ordinary test suite, which runs without external databases and is suitable for every change.
- The live provider gate, which also runs provider parity and provider-swap tests against any configured SQL Server, PostgreSQL, or MySQL databases.

## Local Live Provider Gate

Use `eng\live-provider-gate.cmd` from the repository root.

```cmd
eng\live-provider-gate.cmd quick
eng\live-provider-gate.cmd live
eng\live-provider-gate.cmd full
```

Modes:

- `build`: restore and build the solution.
- `quick`: build and run the provider-swap smoke gate.
- `live`: build and run live provider parity, provider behavior, migration, bulk, and provider-swap tests.
- `full`: restore, build, run the full test suite, then run the live gate.

The script uses the same environment variables as the tests:

```cmd
set NORM_TEST_SQLSERVER=Server=localhost\SQLEXPRESS;Database=normtest;Integrated Security=True;TrustServerCertificate=True;Encrypt=False
set NORM_TEST_POSTGRES=Host=127.0.0.1;Port=5432;Database=normtest;Username=postgres;Password=postgres
set NORM_TEST_MYSQL=Server=127.0.0.1;Port=3306;Database=normtest;User=root;Password=normtest;AllowPublicKeyRetrieval=True
```

`*_CS` aliases are also supported by the test infrastructure, for example `NORM_TEST_POSTGRES_CS`.

The gate sets `NORM_REQUIRE_LIVE_PARITY=any` when at least one live provider is configured and defaults `NORM_MIN_LIVE_PROVIDERS` to the number of configured providers. Set either variable before running the script when a stricter local policy is needed.

For a release candidate, run `full` with every supported live provider configured. For everyday regression work, run `quick` or `live` with the providers available on the machine.
