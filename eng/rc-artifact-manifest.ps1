param(
    [ValidateSet('quick', 'live', 'full', 'rc')]
    [string]$Mode = 'rc',
    [string]$Configuration = 'Release',
    [int]$StressIterations = 20,
    [bool]$BenchmarkSkipped = $false,
    [string]$OutputDirectory = (Join-Path (Split-Path -Parent $PSScriptRoot) 'artifacts/v1-rc')
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

function Get-RelativeFiles {
    param(
        [string[]]$Paths
    )

    $files = New-Object System.Collections.Generic.List[string]
    foreach ($path in $Paths) {
        $fullPath = Join-Path $root $path
        if (-not (Test-Path $fullPath)) {
            continue
        }

        Get-ChildItem -LiteralPath $fullPath -File -Recurse | ForEach-Object {
            $relative = $_.FullName.Substring($root.Length) -replace '^[\\/]+', ''
            $files.Add($relative.Replace('\', '/'))
        }
    }

    return $files.ToArray()
}

function Test-ProviderConfigured {
    param([string]$Name)

    return [bool]([Environment]::GetEnvironmentVariable("NORM_TEST_$Name") -or
        [Environment]::GetEnvironmentVariable("NORM_TEST_${Name}_CS"))
}

function New-ProviderFloorEvidence {
    param(
        [string]$Name,
        [bool]$Configured,
        [string]$MinimumVersion,
        [string]$FloorLabel,
        [string[]]$RequiredFloorFeatures
    )

    [pscustomobject]@{
        Name = $Name
        Configured = $Configured
        MinimumVersion = $MinimumVersion
        FloorLabel = $FloorLabel
        RequiredFloorFeatures = $RequiredFloorFeatures
        ActualServerVersion = $null
        ActualServerVersionSource = 'not captured by rc-artifact-manifest; use dotnet-norm portability certify target reports for live floor proof'
    }
}

function Assert-RcReleaseEvidenceComplete {
    param(
        [string]$Status,
        [string[]]$BenchmarkArtifacts,
        [string[]]$ReleaseEvidence
    )

    if ($Mode -ne 'rc' -or $BenchmarkSkipped) {
        return
    }

    if (-not [string]::IsNullOrWhiteSpace($Status)) {
        throw "Benchmark-enabled RC artifact manifests require a clean git working tree. Commit or stash changes, then rerun."
    }

    $requiredEvidence = @(
        'BenchmarkDotNet.Artifacts/v1-evidence/benchmark-evidence.json',
        'BenchmarkDotNet.Artifacts/v1-evidence/benchmark-evidence.md',
        'BenchmarkDotNet.Artifacts/v1-evidence/benchmark-thresholds.json',
        'BenchmarkDotNet.Artifacts/v1-evidence/benchmark-thresholds.md'
    )

    $missingEvidence = @($requiredEvidence | Where-Object { $ReleaseEvidence -notcontains $_ })
    $rawReports = @($BenchmarkArtifacts | Where-Object { $_ -like '*-report.csv' })

    if ($missingEvidence.Count -gt 0 -or $rawReports.Count -eq 0) {
        $missingText = if ($missingEvidence.Count -gt 0) { " Missing release evidence: $($missingEvidence -join ', ')." } else { '' }
        $rawReportText = if ($rawReports.Count -eq 0) { ' No raw BenchmarkDotNet CSV reports were found in the artifact bundle.' } else { '' }
        throw "Benchmark-enabled RC artifact manifests require benchmark evidence, threshold summaries, and raw BenchmarkDotNet CSV reports.$missingText$rawReportText"
    }
}

$commit = (& git -C $root rev-parse HEAD).Trim()
$statusOutput = & git -C $root status --short
$status = if ($null -eq $statusOutput) { '' } else { ($statusOutput -join [Environment]::NewLine).Trim() }
$sdkVersion = (& dotnet --version).Trim()
$os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
$processArch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString()
$generatedUtc = [DateTime]::UtcNow.ToString('O')

$providers = @(
    New-ProviderFloorEvidence `
        -Name 'SQL Server' `
        -Configured (Test-ProviderConfigured 'SQLSERVER') `
        -MinimumVersion '13.0' `
        -FloorLabel 'SQL Server 2016' `
        -RequiredFloorFeatures @(
            'JSON_VALUE JSON translation',
            'ROW_NUMBER/window translation',
            'IDENTITY plus OUTPUT generated-value retrieval',
            'sp_rename column rename',
            'SAVE TRANSACTION savepoints',
            'IF NOT EXISTS idempotent join-table insert',
            'nORM-managed temporal history/triggers',
            'provider-native temporal DDL and AS OF translation',
            'native bulk insert',
            'native tenant session context')
    New-ProviderFloorEvidence `
        -Name 'PostgreSQL' `
        -Configured (Test-ProviderConfigured 'POSTGRES') `
        -MinimumVersion '12.0' `
        -FloorLabel 'PostgreSQL 12' `
        -RequiredFloorFeatures @(
            'jsonb JSON translation',
            'ROW_NUMBER/window translation',
            'GENERATED AS IDENTITY plus RETURNING generated-value retrieval',
            'ALTER TABLE RENAME COLUMN',
            'SAVEPOINT savepoints',
            'ON CONFLICT DO NOTHING idempotent join-table insert',
            'nORM-managed temporal history/triggers',
            'native bulk insert',
            'native tenant session context')
    New-ProviderFloorEvidence `
        -Name 'MySQL' `
        -Configured (Test-ProviderConfigured 'MYSQL') `
        -MinimumVersion '8.0' `
        -FloorLabel 'MySQL 8.0' `
        -RequiredFloorFeatures @(
            'JSON_EXTRACT JSON translation',
            'ROW_NUMBER/window translation',
            'AUTO_INCREMENT plus LAST_INSERT_ID generated-value retrieval',
            'ALTER TABLE RENAME COLUMN',
            'SAVEPOINT savepoints',
            'INSERT IGNORE idempotent join-table insert',
            'nORM-managed temporal history/triggers',
            'native bulk insert')
    New-ProviderFloorEvidence `
        -Name 'SQLite' `
        -Configured $true `
        -MinimumVersion '3.25' `
        -FloorLabel 'SQLite 3.25' `
        -RequiredFloorFeatures @(
            'JSON1 json_extract JSON translation',
            'ROW_NUMBER/window translation',
            'rowid/AUTOINCREMENT generated-value retrieval',
            'ALTER TABLE RENAME COLUMN',
            'SAVEPOINT savepoints',
            'INSERT OR IGNORE idempotent join-table insert',
            'nORM-managed temporal history/triggers')
)

$testResults = Get-RelativeFiles @('tests/TestResults')
$packageRelativePaths = Get-RelativeFiles @(
    "src/bin/$Configuration",
    "src/dotnet-norm/bin/$Configuration"
) | Where-Object { $_ -match '\.s?nupkg$' }

# Compute SHA-256 hashes for each package file to support integrity verification.
$packages = New-Object System.Collections.Generic.List[object]
foreach ($rel in $packageRelativePaths) {
    $fullPath = Join-Path $root $rel
    if (Test-Path $fullPath) {
        $hash = (Get-FileHash -LiteralPath $fullPath -Algorithm SHA256).Hash.ToLowerInvariant()
        $packages.Add([pscustomobject]@{
            File = $rel.Replace('\', '/')
            Sha256 = $hash
        })
    }
    else {
        $packages.Add([pscustomobject]@{
            File = $rel.Replace('\', '/')
            Sha256 = $null
        })
    }
}

if ($BenchmarkSkipped) {
    $benchmarkArtifacts = @()
    $releaseEvidence = @()
}
else {
    $benchmarkArtifacts = Get-RelativeFiles @('BenchmarkDotNet.Artifacts', 'benchmarks/BenchmarkDotNet.Artifacts')
    $releaseEvidence = Get-RelativeFiles @('BenchmarkDotNet.Artifacts/v1-evidence')
}

Assert-RcReleaseEvidenceComplete -Status $status -BenchmarkArtifacts $benchmarkArtifacts -ReleaseEvidence $releaseEvidence

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

$manifest = [ordered]@{
    GeneratedUtc = $generatedUtc
    gitCommit = $commit
    Commit = $commit
    WorkingTreeClean = [string]::IsNullOrWhiteSpace($status)
    Mode = $Mode
    Configuration = $Configuration
    StressIterations = $StressIterations
    BenchmarkSkipped = $BenchmarkSkipped
    SdkVersion = $sdkVersion
    OS = $os
    ProcessArchitecture = $processArch
    Providers = [object]$providers
    TestResults = [object]$testResults
    Packages = [object]$packages.ToArray()
    BenchmarkArtifacts = [object]$benchmarkArtifacts
    ReleaseEvidence = [object]$releaseEvidence
}

$jsonPath = Join-Path $OutputDirectory 'rc-artifacts.json'
$mdPath = Join-Path $OutputDirectory 'rc-artifacts.md'
$manifest | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $jsonPath -Encoding UTF8

$lines = New-Object System.Collections.Generic.List[string]
$lines.Add('# RC Artifact Manifest')
$lines.Add('')
$lines.Add("- Generated UTC: $generatedUtc")
$lines.Add("- Git commit: $commit")
$lines.Add("- Mode: $Mode")
$lines.Add("- Configuration: $Configuration")
$lines.Add("- Stress iterations: $StressIterations")
$lines.Add("- Benchmark skipped: $BenchmarkSkipped")
if ($BenchmarkSkipped) {
    $lines.Add("- **Performance note**: This manifest was produced with `-SkipBenchmark`. No fresh BenchmarkDotNet evidence was captured. Performance claims documented in `docs/benchmark-governance.md` are not backed by this run. A separate benchmark run is required before making public performance claims.")
}
$lines.Add("- SDK: $sdkVersion")
$lines.Add("- OS: $os ($processArch)")
$lines.Add("- Working tree clean: $([string]::IsNullOrWhiteSpace($status))")
$lines.Add('')
$lines.Add('## Providers')
$lines.Add('')
$lines.Add('| Provider | Configured | Minimum Version | Floor Label | Required Floor Features | Actual Server Version |')
$lines.Add('| --- | --- | --- | --- | --- | --- |')
foreach ($provider in $providers) {
    $actualVersion = if ($provider.ActualServerVersion) { $provider.ActualServerVersion } else { 'not captured; see provider mobility target report' }
    $features = ($provider.RequiredFloorFeatures -join '<br>')
    $lines.Add("| $($provider.Name) | $($provider.Configured) | $($provider.MinimumVersion) | $($provider.FloorLabel) | $features | $actualVersion |")
}

# Packages — rendered separately with hash column
$lines.Add('')
$lines.Add('## Packages')
$lines.Add('')
if ($packages.Count -eq 0) {
    $lines.Add('- none')
}
else {
    $lines.Add('| File | SHA-256 |')
    $lines.Add('| --- | --- |')
    foreach ($pkg in $packages) {
        $hashDisplay = if ($pkg.Sha256) { $pkg.Sha256 } else { 'n/a' }
        $lines.Add(('| `{0}` | `{1}` |' -f $pkg.File, $hashDisplay))
    }
}

$sections = @(
    @{ Title = 'Test Results'; Values = $testResults },
    @{ Title = 'Benchmark Artifacts'; Values = $benchmarkArtifacts },
    @{ Title = 'Release Evidence'; Values = $releaseEvidence }
)

foreach ($section in $sections) {
    $lines.Add('')
    $lines.Add("## $($section.Title)")
    $lines.Add('')
    if ($section.Values.Count -eq 0) {
        $lines.Add('- none')
        continue
    }

    foreach ($value in $section.Values) {
        $lines.Add(('- `{0}`' -f $value))
    }
}

$lines | Set-Content -LiteralPath $mdPath -Encoding UTF8

# Copy package files into the artifact bundle so consumers can verify the exact
# files the gate ran against without needing the source repo's bin/Release output.
$pkgBundleDir = Join-Path $OutputDirectory 'packages'
New-Item -ItemType Directory -Force -Path $pkgBundleDir | Out-Null
Get-ChildItem -LiteralPath $pkgBundleDir -File -Filter '*.nupkg' -ErrorAction SilentlyContinue |
    Remove-Item -Force
Get-ChildItem -LiteralPath $pkgBundleDir -File -Filter '*.snupkg' -ErrorAction SilentlyContinue |
    Remove-Item -Force
foreach ($pkg in $packages) {
    $src = Join-Path $root $pkg.File.Replace('/', '\')
    if (Test-Path $src) {
        Copy-Item -LiteralPath $src -Destination $pkgBundleDir -Force
    }
}

Write-Host "RC artifact manifest written to $OutputDirectory"
