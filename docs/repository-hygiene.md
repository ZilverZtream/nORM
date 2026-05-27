# Repository Hygiene

v1 release work must keep generated artifacts, local scratch files, and encoding
noise out of normal pull requests.

## Generated Artifacts

Do not commit local build outputs, NuGet packages, BenchmarkDotNet artifacts, TRX
files, coverage reports, or temporary scratch projects. These paths are ignored:

- `bin/` and `obj/`
- `*.nupkg` and `*.snupkg`
- `BenchmarkDotNet.Artifacts/`
- `.tmp/`
- `tests/TestResults/`
- `*.trx` and `*.coverage`
- `coverage/` and `coverage-report/`

Test ownership and legacy coverage-suite cleanup are documented in
`docs/test-suite-ownership.md`.

## Scripts

PowerShell scripts are not globally ignored. Release and engineering scripts
under `eng/` and `.github/` must remain visible to git. Local scratch scripts
should use one of these ignored suffixes:

- `*.local.ps1`
- `*.scratch.ps1`

## Encoding

Source, workflow, and documentation files should be UTF-8 text. Avoid decorative
box-drawing separators and mojibake in new code. Existing large test-comment
blocks may be cleaned opportunistically when the file is already being changed,
but release workflows and docs must stay free of obvious mojibake.
