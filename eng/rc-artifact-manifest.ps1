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
$statusOutput = & git -C $root status --short
$status = if ($null -eq $statusOutput) { '' } else { ($statusOutput -join [Environment]::NewLine).Trim() }
$sdkVersion = (& dotnet --version).Trim()
$os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
$processArch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString()
$generatedUtc = [DateTime]::UtcNow.ToString('O')

$providers = @(
    [pscustomobject]@{ Name = 'SQL Server'; Configured = (Test-ProviderConfigured 'SQLSERVER') }
    [pscustomobject]@{ Name = 'PostgreSQL'; Configured = (Test-ProviderConfigured 'POSTGRES') }
    [pscustomobject]@{ Name = 'MySQL'; Configured = (Test-ProviderConfigured 'MYSQL') }
    [pscustomobject]@{ Name = 'SQLite'; Configured = $true }
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
$lines.Add("- SDK: $sdkVersion")
$lines.Add("- OS: $os ($processArch)")
$lines.Add("- Working tree clean: $([string]::IsNullOrWhiteSpace($status))")
$lines.Add('')
$lines.Add('## Providers')
$lines.Add('')
$lines.Add('| Provider | Configured |')
$lines.Add('| --- | --- |')
foreach ($provider in $providers) {
    $lines.Add("| $($provider.Name) | $($provider.Configured) |")
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
Write-Host "RC artifact manifest written to $OutputDirectory"
