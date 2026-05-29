param(
    [string]$ResultsDirectory = (Join-Path (Split-Path -Parent $PSScriptRoot) 'BenchmarkDotNet.Artifacts/results'),
    [string]$OutputDirectory = (Join-Path (Split-Path -Parent $PSScriptRoot) 'BenchmarkDotNet.Artifacts/v1-evidence'),
    [string]$BenchmarkFilter = '*ProviderMatrixBenchmarks*',
    [string]$Mode = 'manual'
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

function Redact-ConnectionString {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ''
    }

    $parts = $Value -split ';'
    $redacted = foreach ($part in $parts) {
        if ([string]::IsNullOrWhiteSpace($part) -or -not $part.Contains('=')) {
            $part
            continue
        }

        $idx = $part.IndexOf('=')
        $key = $part.Substring(0, $idx)
        $val = $part.Substring($idx + 1)
        if ($key -match '(?i)^(password|pwd|user id|uid|username|user|access token|token|secret)$') {
            "$key=***"
        }
        else {
            "$key=$val"
        }
    }

    return ($redacted -join ';')
}

function Get-ProviderSetting {
    param([string]$Name)

    $raw = [Environment]::GetEnvironmentVariable($Name)
    if (-not $raw) {
        $raw = [Environment]::GetEnvironmentVariable("${Name}_CS")
    }

    [pscustomobject]@{
        Name = $Name
        Configured = [bool]$raw
        Redacted = if ($raw) { Redact-ConnectionString $raw } else { '' }
    }
}

function Convert-MeanToNanoseconds {
    param([string]$Mean)

    if ([string]::IsNullOrWhiteSpace($Mean)) {
        return [double]::PositiveInfinity
    }

    $normalized = $Mean.Trim() -replace ',', '.'
    if ($normalized -match '^\s*([0-9.]+)\s*(ns|us|µs|ms|s)\s*$') {
        $value = [double]::Parse($matches[1], [Globalization.CultureInfo]::InvariantCulture)
        switch ($matches[2]) {
            'ns' { return $value }
            { $_ -in @('us', 'µs') } { return $value * 1000 }
            'ms' { return $value * 1000 * 1000 }
            's'  { return $value * 1000 * 1000 * 1000 }
        }
    }

    return [double]::PositiveInfinity
}

function Get-DriverPackageVersions {
    $assetsPath = Join-Path $root 'benchmarks/obj/project.assets.json'
    if (-not (Test-Path $assetsPath)) {
        return @()
    }

    $json = Get-Content $assetsPath -Raw | ConvertFrom-Json
    $libraries = $json.libraries.PSObject.Properties
    $packageNames = @('Microsoft.Data.SqlClient', 'Microsoft.Data.Sqlite', 'Npgsql', 'MySqlConnector', 'Dapper', 'Microsoft.EntityFrameworkCore')
    foreach ($package in $packageNames) {
        $match = $libraries | Where-Object { $_.Name -like "$package/*" } | Select-Object -First 1
        if ($match) {
            [pscustomobject]@{
                Name = $package
                Version = ($match.Name -split '/', 2)[1]
            }
        }
    }
}

if (-not (Test-Path $ResultsDirectory)) {
    throw "Benchmark results directory not found: $ResultsDirectory"
}

$csvFiles = Get-ChildItem -LiteralPath $ResultsDirectory -File -Filter '*-report.csv' |
    Sort-Object LastWriteTimeUtc -Descending
if ($csvFiles.Count -eq 0) {
    throw "No BenchmarkDotNet CSV reports found in $ResultsDirectory"
}

if ($Mode -in @('rc', 'full')) {
    $fastReports = @($csvFiles | Where-Object { $_.Name -match 'FastNormBenchmarks' })
    if ($fastReports.Count -gt 0) {
        $names = ($fastReports | ForEach-Object Name) -join ', '
        throw "FastNormBenchmarks reports are smoke-test artifacts and cannot be used as public release evidence: $names"
    }
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

$commit = (& git -C $root rev-parse HEAD).Trim()
$sdkVersion = (& dotnet --version).Trim()
$os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
$processArch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString()
$providers = @(
    Get-ProviderSetting 'NORM_TEST_SQLSERVER'
    Get-ProviderSetting 'NORM_TEST_POSTGRES'
    Get-ProviderSetting 'NORM_TEST_MYSQL'
)
$drivers = @(Get-DriverPackageVersions)

$reports = New-Object System.Collections.Generic.List[string]
foreach ($csvFile in $csvFiles) {
    $relativePath = $csvFile.FullName.Substring($root.Length) -replace '^[\\/]+', ''
    $reports.Add($relativePath)
}

$summaries = New-Object System.Collections.Generic.List[object]
$providerReportFiles = New-Object System.Collections.Generic.List[object]
foreach ($file in $csvFiles) {
    $rows = Import-Csv $file.FullName -Delimiter ';'
    if (-not $rows -or -not ($rows[0].PSObject.Properties.Name -contains 'Method')) {
        $rows = Import-Csv $file.FullName
    }

    $providerGroups = $rows | Where-Object { $_.Provider -and $_.Method -and $_.Mean } | Group-Object Provider
    foreach ($group in $providerGroups) {
        $safeProviderName = $group.Name -replace '[^A-Za-z0-9_.-]', '_'
        $baseName = [IO.Path]::GetFileNameWithoutExtension($file.Name) -replace '[^A-Za-z0-9_.-]', '_'
        $providerCsvName = "$baseName.$safeProviderName.csv"
        $providerCsvPath = Join-Path $OutputDirectory $providerCsvName
        $group.Group | Export-Csv -LiteralPath $providerCsvPath -Delimiter ';' -NoTypeInformation -Encoding UTF8
        $providerReportFiles.Add([pscustomobject]@{
            Provider = $group.Name
            Report = $file.Name
            Path = $providerCsvName
        })

        $fastest = $group.Group | Sort-Object { Convert-MeanToNanoseconds $_.Mean } | Select-Object -First 1
        if ($fastest) {
            $summaries.Add([pscustomobject]@{
                Report = $file.Name
                Provider = $group.Name
                FastestMethod = $fastest.Method
                Mean = $fastest.Mean
                Allocated = $fastest.Allocated
            })
        }
    }
}

$generatedUtc = [DateTime]::UtcNow.ToString('O')
$evidence = New-Object System.Collections.Specialized.OrderedDictionary
$evidence.Add('GeneratedUtc', $generatedUtc)
$evidence.Add('Mode', $Mode)
$evidence.Add('BenchmarkFilter', $BenchmarkFilter)
$evidence.Add('Commit', $commit)
$evidence.Add('SdkVersion', $sdkVersion)
$evidence.Add('OS', $os)
$evidence.Add('ProcessArchitecture', $processArch)
$evidence.Add('Reports', [object]$reports.ToArray())
$evidence.Add('ProviderReports', [object]$providerReportFiles.ToArray())
$evidence.Add('Providers', [object]$providers)
$evidence.Add('DriverPackages', [object]$drivers)
$evidence.Add('FastestByProvider', [object]$summaries.ToArray())

$jsonPath = Join-Path $OutputDirectory 'benchmark-evidence.json'
$mdPath = Join-Path $OutputDirectory 'benchmark-evidence.md'
$evidence | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $jsonPath -Encoding UTF8

$lines = New-Object System.Collections.Generic.List[string]
$lines.Add('# Benchmark Evidence')
$lines.Add('')
$lines.Add("- Generated UTC: $generatedUtc")
$lines.Add("- Commit: $commit")
$lines.Add("- Mode: $Mode")
$lines.Add(('- Filter: `{0}`' -f $BenchmarkFilter))
$lines.Add("- SDK: $sdkVersion")
$lines.Add("- OS: $os ($processArch)")
$lines.Add('')
$lines.Add('## Provider Configuration')
$lines.Add('')
$lines.Add('| Provider env | Configured | Redacted summary |')
$lines.Add('| --- | --- | --- |')
foreach ($provider in $providers) {
    $summary = if ($provider.Configured) { $provider.Redacted } else { '' }
    $lines.Add(('| `{0}` | {1} | `{2}` |' -f $provider.Name, $provider.Configured, $summary))
}
$lines.Add('')
$lines.Add('## Driver Packages')
$lines.Add('')
$lines.Add('| Package | Version |')
$lines.Add('| --- | --- |')
foreach ($driver in $drivers) {
    $lines.Add("| $($driver.Name) | $($driver.Version) |")
}
$lines.Add('')
$lines.Add('## Raw Reports')
$lines.Add('')
foreach ($report in $reports) {
    $lines.Add(('- `{0}`' -f $report))
}
$lines.Add('')
$lines.Add('## Provider-Split Reports')
$lines.Add('')
$lines.Add('| Provider | Source report | Split CSV |')
$lines.Add('| --- | --- | --- |')
foreach ($providerReport in $providerReportFiles) {
    $lines.Add(('| {0} | `{1}` | `{2}` |' -f $providerReport.Provider, $providerReport.Report, $providerReport.Path))
}
$lines.Add('')
$lines.Add('## Fastest Method By Provider')
$lines.Add('')
$lines.Add('| Report | Provider | Fastest method | Mean | Allocated |')
$lines.Add('| --- | --- | --- | --- | --- |')
foreach ($summary in $summaries) {
    $lines.Add(('| {0} | {1} | `{2}` | {3} | {4} |' -f $summary.Report, $summary.Provider, $summary.FastestMethod, $summary.Mean, $summary.Allocated))
}

$lines | Set-Content -LiteralPath $mdPath -Encoding UTF8
Write-Host "Benchmark evidence written to $OutputDirectory"
