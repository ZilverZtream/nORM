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

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

$commit = (& git -C $root rev-parse HEAD).Trim()
$status = (& git -C $root status --short).Trim()
$sdkVersion = (& dotnet --version).Trim()
$generatedUtc = [DateTime]::UtcNow.ToString('O')

$providers = @(
    [pscustomobject]@{ Name = 'SQL Server'; Configured = (Test-ProviderConfigured 'SQLSERVER') }
    [pscustomobject]@{ Name = 'PostgreSQL'; Configured = (Test-ProviderConfigured 'POSTGRES') }
    [pscustomobject]@{ Name = 'MySQL'; Configured = (Test-ProviderConfigured 'MYSQL') }
    [pscustomobject]@{ Name = 'SQLite'; Configured = $true }
)

$testResults = Get-RelativeFiles @('tests/TestResults')
$packages = Get-RelativeFiles @(
    "src/bin/$Configuration",
    "src/dotnet-norm/bin/$Configuration"
) | Where-Object { $_ -match '\.s?nupkg$' }
if ($BenchmarkSkipped) {
    $benchmarkArtifacts = @()
    $releaseEvidence = @()
}
else {
    $benchmarkArtifacts = Get-RelativeFiles @('BenchmarkDotNet.Artifacts', 'benchmarks/BenchmarkDotNet.Artifacts')
    $releaseEvidence = Get-RelativeFiles @('BenchmarkDotNet.Artifacts/v1-evidence')
}

$manifest = [ordered]@{
    GeneratedUtc = $generatedUtc
    Commit = $commit
    WorkingTreeClean = [string]::IsNullOrWhiteSpace($status)
    Mode = $Mode
    Configuration = $Configuration
    StressIterations = $StressIterations
    BenchmarkSkipped = $BenchmarkSkipped
    SdkVersion = $sdkVersion
    Providers = [object]$providers
    TestResults = [object]$testResults
    Packages = [object]$packages
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
$lines.Add("- Commit: $commit")
$lines.Add("- Mode: $Mode")
$lines.Add("- Configuration: $Configuration")
$lines.Add("- Stress iterations: $StressIterations")
$lines.Add("- Benchmark skipped: $BenchmarkSkipped")
$lines.Add("- SDK: $sdkVersion")
$lines.Add("- Working tree clean: $([string]::IsNullOrWhiteSpace($status))")
$lines.Add('')
$lines.Add('## Providers')
$lines.Add('')
$lines.Add('| Provider | Configured |')
$lines.Add('| --- | --- |')
foreach ($provider in $providers) {
    $lines.Add("| $($provider.Name) | $($provider.Configured) |")
}

$sections = @(
    @{ Title = 'Test Results'; Values = $testResults },
    @{ Title = 'Packages'; Values = $packages },
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
Write-Host "RC artifact manifest written to $OutputDirectory"
