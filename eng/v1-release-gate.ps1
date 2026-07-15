param(
    [ValidateSet('quick', 'live', 'full', 'rc')]
    [string]$Mode = 'full',
    [string]$Configuration = $(if ($env:NORM_GATE_CONFIGURATION) { $env:NORM_GATE_CONFIGURATION } else { 'Release' }),
    [int]$StressIterations = 20,
    [int]$MinLiveProviders = $(if ($env:NORM_MIN_LIVE_PROVIDERS) { [int]$env:NORM_MIN_LIVE_PROVIDERS } else { 0 }),
    [switch]$SkipBenchmark,
    [switch]$SkipProviderMatrixBenchmark,
    [switch]$FullBenchmarkMatrix,
    [string]$ProviderMatrixBenchmarkFilter = '',
    [int]$ProviderMatrixSliceTimeoutMinutes = $(if ($env:NORM_PROVIDER_MATRIX_SLICE_TIMEOUT_MINUTES) { [int]$env:NORM_PROVIDER_MATRIX_SLICE_TIMEOUT_MINUTES } else { 90 }),
    [int]$BenchmarkStepTimeoutMinutes = $(if ($env:NORM_BENCHMARK_STEP_TIMEOUT_MINUTES) { [int]$env:NORM_BENCHMARK_STEP_TIMEOUT_MINUTES } else { 45 }),
    [int]$TestStepTimeoutMinutes = $(if ($env:NORM_TEST_STEP_TIMEOUT_MINUTES) { [int]$env:NORM_TEST_STEP_TIMEOUT_MINUTES } else { 45 }),
    [int]$TestStepHangTimeoutMinutes = $(if ($env:NORM_TEST_STEP_HANG_TIMEOUT_MINUTES) { [int]$env:NORM_TEST_STEP_HANG_TIMEOUT_MINUTES } else { 5 }),
    # Resume a failed gate from the named step (exact step name as printed in the
    # "==> <name>" log lines), skipping every step before it. Phases that already
    # passed on the SAME commit are deterministic evidence and must not be re-run
    # to validate a fix in a later phase. Requires the working tree and build
    # outputs from the failed run to be intact (resume skips restore/build too
    # unless you resume at or before them).
    [string]$StartAt = ''
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# The benchmark thresholds only consume specific target/baseline methods, so the
# default provider-matrix filter is derived from eng/benchmark-thresholds.json and
# stays in sync with rule changes. The full 54-benchmark class takes roughly twice
# as long per provider and is opt-in via -FullBenchmarkMatrix; an explicit
# -ProviderMatrixBenchmarkFilter value always wins.
if (-not $ProviderMatrixBenchmarkFilter) {
    if ($FullBenchmarkMatrix) {
        $ProviderMatrixBenchmarkFilter = '*ProviderMatrixBenchmarks*'
    }
    else {
        $thresholdRules = (Get-Content (Join-Path $PSScriptRoot 'benchmark-thresholds.json') -Raw | ConvertFrom-Json).rules
        $thresholdMethods = @($thresholdRules | ForEach-Object { @($_.targetMethods) + @($_.baselineMethods) }) |
            Where-Object { $_ } | Sort-Object -Unique
        if ($thresholdMethods.Count -eq 0) {
            throw 'Deriving the provider-matrix benchmark filter found no threshold methods; fix eng/benchmark-thresholds.json or pass -FullBenchmarkMatrix.'
        }
        $ProviderMatrixBenchmarkFilter = (@($thresholdMethods | ForEach-Object { "*ProviderMatrixBenchmarks.$_" }) -join ' ')
        Write-Host "Provider-matrix benchmark filter derived from thresholds ($($thresholdMethods.Count) methods); pass -FullBenchmarkMatrix for the complete class."
    }
}

$solutionPath = Join-Path $root 'nORM.sln'
$testsProjectPath = Join-Path (Join-Path $root 'tests') 'nORM.Tests.csproj'
$benchmarkProjectDir = Join-Path $root 'benchmarks'
$benchmarkProjectPath = Join-Path (Join-Path $root 'benchmarks') 'nORM.Benchmarks.csproj'
$benchmarkResultsPath = Join-Path (Join-Path $benchmarkProjectDir 'BenchmarkDotNet.Artifacts') 'results'
$benchmarkEvidencePath = Join-Path (Join-Path $root 'BenchmarkDotNet.Artifacts') 'v1-evidence'
$runtimeProjectPath = Join-Path (Join-Path $root 'src') 'nORM.csproj'
$toolProjectPath = Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') 'dotnet-norm.csproj'
$testResultsPath = Join-Path (Join-Path (Join-Path $root 'tests') 'TestResults') 'v1-release-gate'
New-Item -ItemType Directory -Force -Path $testResultsPath | Out-Null

if ($ProviderMatrixSliceTimeoutMinutes -le 0) {
    throw "ProviderMatrixSliceTimeoutMinutes must be greater than zero."
}

if ($BenchmarkStepTimeoutMinutes -le 0) {
    throw "BenchmarkStepTimeoutMinutes must be greater than zero."
}

if ($TestStepTimeoutMinutes -le 0) {
    throw "TestStepTimeoutMinutes must be greater than zero."
}

if ($TestStepHangTimeoutMinutes -le 0) {
    throw "TestStepHangTimeoutMinutes must be greater than zero."
}

if ($TestStepHangTimeoutMinutes -gt $TestStepTimeoutMinutes) {
    throw "TestStepHangTimeoutMinutes must not exceed TestStepTimeoutMinutes."
}

function Stop-OrphanedTestHosts {
    # A timed-out or interrupted prior gate run can leave testhost.exe holding
    # tests/bin/Release/net8.0/nORM.dll, which then breaks the next build copy step. The gate
    # must self-recover so consecutive runs work without manual `Stop-Process testhost`.
    try {
        $procs = Get-Process -Name testhost, dotnet -ErrorAction SilentlyContinue |
            Where-Object {
                try {
                    ($_.Path -and $_.Path -like "*\testhost*") -or
                    ($_.MainModule -and $_.MainModule.FileName -like "*\testhost*")
                } catch { $false }
            }
        foreach ($p in $procs) {
            try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch { }
        }
    } catch { }
}

# When -StartAt is set, every step before the named one is skipped; the flag
# flips false at the first matching step and stays false for the rest of the run.
$script:resumeSkipping = -not [string]::IsNullOrWhiteSpace($StartAt)

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Command
    )

    if ($script:resumeSkipping) {
        if ($Name -eq $StartAt) {
            $script:resumeSkipping = $false
        }
        else {
            Write-Host "==> $Name (skipped - resuming at '$StartAt')"
            return
        }
    }

    Write-Host ""
    Write-Host "==> $Name"
    $global:LASTEXITCODE = $null
    & $Command
    if ($null -ne $LASTEXITCODE -and $LASTEXITCODE -ne 0) {
        throw "Step failed: $Name (exit code $LASTEXITCODE)"
    }
}

function Get-TrxLogFileName {
    param([string]$Name)
    return (($Name -replace '[^A-Za-z0-9_.-]', '_') + '.trx')
}

function Get-DuplicateBenchmarkProjects {
    $expectedProject = [System.IO.Path]::GetFullPath($benchmarkProjectPath)
    return @(Get-ChildItem -LiteralPath $root -Recurse -File -Filter 'nORM.Benchmarks.csproj' |
        Where-Object { [System.IO.Path]::GetFullPath($_.FullName) -ne $expectedProject })
}

function Copy-CurrentWorkspaceForBenchmarkIsolation {
    param([string]$Destination)

    New-Item -ItemType Directory -Force -Path $Destination | Out-Null

    $excludedDirectories = @(
        '.git',
        '.claude',
        '.dotnet-home',
        '.vs',
        'BenchmarkDotNet.Artifacts',
        'artifacts',
        'bin',
        'obj',
        'TestResults'
    )

    $robocopy = Get-Command robocopy -ErrorAction SilentlyContinue
    if ($robocopy) {
        $arguments = @(
            $root,
            $Destination,
            '/MIR',
            '/XD'
        ) + $excludedDirectories + @(
            '/XF',
            '*.nupkg',
            '*.snupkg',
            '/NFL',
            '/NDL',
            '/NP'
        )

        & $robocopy @arguments | Write-Host
        if ($LASTEXITCODE -gt 7) {
            throw "Failed to copy isolated benchmark workspace to $Destination (robocopy exit code $LASTEXITCODE)."
        }

        $global:LASTEXITCODE = 0
        return
    }

    Get-ChildItem -LiteralPath $root -Force |
        Where-Object { $excludedDirectories -notcontains $_.Name } |
        Copy-Item -Destination $Destination -Recurse -Force
}

function Invoke-TestStep {
    param(
        [string]$Name,
        [string]$Filter
    )

    Invoke-Step $Name {
        $arguments = @(
            'test',
            $testsProjectPath,
            '-c',
            $Configuration,
            '--no-build',
            '--no-restore',
            '--logger',
            'console;verbosity=minimal',
            '--logger',
            "trx;LogFileName=$(Get-TrxLogFileName $Name)",
            '--results-directory',
            $testResultsPath,
            '--blame-hang',
            '--blame-hang-timeout',
            "$($TestStepHangTimeoutMinutes)m"
        )

        # In quick mode, exclude live-provider tests when no explicit filter is given.
        # This ensures the quick gate never blocks on missing provider connection strings.
        if ($Filter) {
            $arguments += @('--filter', $Filter)
        }
        elseif ($Mode -eq 'quick') {
            $arguments += @('--filter', 'Category!=LiveProvider')
        }

        $logDirectory = Join-Path $testResultsPath 'logs'
        New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
        $safeName = $Name -replace '[^A-Za-z0-9_.-]', '_'
        $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
        $stdoutPath = Join-Path $logDirectory "$safeName-$timestamp.out.log"
        $stderrPath = Join-Path $logDirectory "$safeName-$timestamp.err.log"
        $exitCodePath = Join-Path $logDirectory "$safeName-$timestamp.exitcode"
        $argumentsPath = Join-Path $logDirectory "$safeName-$timestamp.args.json"
        $runnerPath = Join-Path $logDirectory "$safeName-$timestamp.runner.ps1"
        $arguments | ConvertTo-Json | Set-Content -LiteralPath $argumentsPath -Encoding UTF8

        @'
param(
    [string]$RepoRoot,
    [string]$ExitCodePath,
    [string]$ArgumentsPath
)

$exitCode = 1
try {
    Set-Location -LiteralPath $RepoRoot
    $dotnetArguments = @(Get-Content -LiteralPath $ArgumentsPath -Raw | ConvertFrom-Json)
    & dotnet @dotnetArguments
    $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { $LASTEXITCODE }
}
catch {
    Write-Error $_
    $exitCode = 1
}
finally {
    Set-Content -LiteralPath $ExitCodePath -Value $exitCode -Encoding ASCII
}

exit $exitCode
'@ | Set-Content -LiteralPath $runnerPath -Encoding UTF8

        Write-Host "Test step timeout: $TestStepTimeoutMinutes minute(s)"
        Write-Host "Test hang timeout: $TestStepHangTimeoutMinutes minute(s)"
        # Launch the same PowerShell engine that is running this script so the
        # runner works on Windows PowerShell and on pwsh under Linux/macOS CI,
        # where no executable named 'powershell' exists.
        $powerShellHost = [System.Diagnostics.Process]::GetCurrentProcess().MainModule.FileName
        $startProcessArgs = @{
            FilePath = $powerShellHost
            ArgumentList = @(
                '-NoProfile',
                '-ExecutionPolicy', 'Bypass',
                '-File', $runnerPath,
                '-RepoRoot', $root,
                '-ExitCodePath', $exitCodePath,
                '-ArgumentsPath', $argumentsPath
            )
            WorkingDirectory = $root
            RedirectStandardOutput = $stdoutPath
            RedirectStandardError = $stderrPath
            PassThru = $true
        }

        if ($IsWindows -or $env:OS -eq 'Windows_NT') {
            $startProcessArgs.WindowStyle = 'Hidden'
        }

        $process = Start-Process @startProcessArgs
        $timeoutMs = [int][TimeSpan]::FromMinutes($TestStepTimeoutMinutes).TotalMilliseconds
        if (-not $process.WaitForExit($timeoutMs)) {
            Stop-ProcessTreeById $process.Id
            $terminated = $process.WaitForExit(30000)
            $terminationNote = if ($terminated) { '' } else { ' Process tree did not terminate within 30 seconds.' }
            throw "Test step '$Name' exceeded $TestStepTimeoutMinutes minute(s).$terminationNote`nStdout log: $stdoutPath`nStderr log: $stderrPath`nRecent stdout:`n$(Get-LogTail $stdoutPath)`nRecent stderr:`n$(Get-LogTail $stderrPath)"
        }

        $process.WaitForExit()
        if (Test-Path -LiteralPath $stdoutPath) {
            Get-Content -LiteralPath $stdoutPath | ForEach-Object { Write-Host $_ }
        }
        if (Test-Path -LiteralPath $stderrPath) {
            Get-Content -LiteralPath $stderrPath | ForEach-Object { Write-Host $_ }
        }

        if (-not (Test-Path -LiteralPath $exitCodePath)) {
            throw "Test step '$Name' finished but did not report an exit code.`nStdout log: $stdoutPath`nStderr log: $stderrPath`nRecent stdout:`n$(Get-LogTail $stdoutPath)`nRecent stderr:`n$(Get-LogTail $stderrPath)"
        }

        $exitCodeText = (Get-Content -LiteralPath $exitCodePath -Raw).Trim()
        $exitCode = [int]$exitCodeText
        if ($exitCode -ne 0) {
            throw "Test step '$Name' failed with exit code $exitCode.`nStdout log: $stdoutPath`nStderr log: $stderrPath`nRecent stdout:`n$(Get-LogTail $stdoutPath)`nRecent stderr:`n$(Get-LogTail $stderrPath)"
        }

        $global:LASTEXITCODE = 0
    }
}

function Test-ProviderConfigured {
    param([string]$Name)
    return [bool]([Environment]::GetEnvironmentVariable("NORM_TEST_$Name") -or
        [Environment]::GetEnvironmentVariable("NORM_TEST_${Name}_CS"))
}

function Get-WorkingTreeStatus {
    $statusLines = @(& git -C $root status --porcelain)
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to read git working tree status."
    }

    return @($statusLines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Assert-CleanReleaseEvidenceWorkspace {
    param([string]$Context)

    $dirty = @(Get-WorkingTreeStatus)
    if ($dirty.Count -eq 0) {
        return
    }

    $preview = ($dirty | Select-Object -First 20) -join [Environment]::NewLine
    $overflow = if ($dirty.Count -gt 20) { "`n... $($dirty.Count - 20) more dirty path(s)" } else { '' }
    throw "$Context requires a clean git working tree before collecting release benchmark evidence. Commit or stash changes, then rerun. Dirty paths:`n$preview$overflow"
}

function Get-NormVersion {
    $propsPath = Join-Path $root 'Directory.Build.props'
    [xml]$props = Get-Content $propsPath
    return $props.Project.PropertyGroup.NormVersion
}

function Clear-PackageOutput {
    param(
        [string]$Directory,
        [string]$PackageId
    )

    if (-not (Test-Path $Directory)) {
        return
    }

    Get-ChildItem -LiteralPath $Directory -File -Filter "$PackageId.*.nupkg" | Remove-Item -Force
    Get-ChildItem -LiteralPath $Directory -File -Filter "$PackageId.*.snupkg" | Remove-Item -Force
}

function Get-LogTail {
    param(
        [string]$Path,
        [int]$Count = 40
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return '<log file not found>'
    }

    return (Get-Content -LiteralPath $Path -Tail $Count) -join [Environment]::NewLine
}

function Stop-ProcessTreeById {
    param([int]$ProcessId)

    if ($IsWindows -or $env:OS -eq 'Windows_NT') {
        & taskkill.exe /PID $ProcessId /T /F *> $null
        return
    }

    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Invoke-TimedBenchmarkProcess {
    param(
        [string]$WorkingDirectory,
        [string[]]$Arguments,
        [int]$TimeoutMinutes
    )

    $logDirectory = Join-Path $root 'BenchmarkDotNet.Artifacts/v1-release-gate-logs'
    New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $stdoutPath = Join-Path $logDirectory "benchmark-$timestamp.out.log"
    $stderrPath = Join-Path $logDirectory "benchmark-$timestamp.err.log"
    $exitCodePath = Join-Path $logDirectory "benchmark-$timestamp.exitcode"
    $argumentsPath = Join-Path $logDirectory "benchmark-$timestamp.args.json"
    $runnerPath = Join-Path $logDirectory "benchmark-$timestamp.runner.ps1"
    $dotnetArguments = @('run', '-c', $Configuration, '--') + $Arguments
    $dotnetArguments | ConvertTo-Json | Set-Content -LiteralPath $argumentsPath -Encoding UTF8

    @'
param(
    [string]$BenchmarkWorkingDirectory,
    [string]$ExitCodePath,
    [string]$ArgumentsPath
)

$exitCode = 1
try {
    Set-Location -LiteralPath $BenchmarkWorkingDirectory
    $DotnetArguments = @(Get-Content -LiteralPath $ArgumentsPath -Raw | ConvertFrom-Json)
    & dotnet @DotnetArguments
    $exitCode = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
}
catch {
    Write-Error $_
    $exitCode = 1
}
finally {
    Set-Content -LiteralPath $ExitCodePath -Value $exitCode -Encoding ASCII
}

exit $exitCode
'@ | Set-Content -LiteralPath $runnerPath -Encoding UTF8

    Write-Host "Benchmark command: dotnet $($dotnetArguments -join ' ')"
    Write-Host "Benchmark timeout: $TimeoutMinutes minute(s)"
    Write-Host "Benchmark stdout log: $stdoutPath"
    Write-Host "Benchmark stderr log: $stderrPath"

    $startProcessArgs = @{
        FilePath = 'powershell'
        ArgumentList = @(
            '-NoProfile',
            '-ExecutionPolicy', 'Bypass',
            '-File', $runnerPath,
            '-BenchmarkWorkingDirectory', $WorkingDirectory,
            '-ExitCodePath', $exitCodePath,
            '-ArgumentsPath', $argumentsPath
        )
        WorkingDirectory = $WorkingDirectory
        PassThru = $true
        RedirectStandardOutput = $stdoutPath
        RedirectStandardError = $stderrPath
    }
    if ($IsWindows -or $env:OS -eq 'Windows_NT') {
        $startProcessArgs.WindowStyle = 'Hidden'
    }

    $process = Start-Process @startProcessArgs
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $timeoutMs = [int][TimeSpan]::FromMinutes($TimeoutMinutes).TotalMilliseconds

    while (-not $process.WaitForExit([Math]::Min(30000, [Math]::Max(1000, $timeoutMs - [int]$stopwatch.ElapsedMilliseconds)))) {
        Write-Host "Benchmark still running after $([Math]::Round($stopwatch.Elapsed.TotalMinutes, 1)) minute(s)."
        if ($stopwatch.ElapsedMilliseconds -ge $timeoutMs) {
            Stop-ProcessTreeById $process.Id
            $terminated = $process.WaitForExit(30000)
            $terminationNote = if ($terminated) { '' } else { "`nThe benchmark process did not exit within 30 seconds after the kill request; check for leftover child processes." }
            throw "Benchmark command exceeded $TimeoutMinutes minute(s).$terminationNote`nStdout log: $stdoutPath`nStderr log: $stderrPath`nRecent stdout:`n$(Get-LogTail $stdoutPath)`nRecent stderr:`n$(Get-LogTail $stderrPath)"
        }
    }

    $process.WaitForExit()
    try { $process.Refresh() } catch { }

    $exitCode = $null
    if (Test-Path -LiteralPath $exitCodePath) {
        $rawExitCode = (Get-Content -LiteralPath $exitCodePath -First 1 -ErrorAction SilentlyContinue)
        $parsedExitCode = 0
        if ([int]::TryParse([string]$rawExitCode, [ref]$parsedExitCode)) {
            $exitCode = $parsedExitCode
        }
    }

    if ($null -eq $exitCode) {
        try {
            $processExitCode = $process.ExitCode
            if ($null -ne $processExitCode -and "$processExitCode" -ne '') {
                $exitCode = [int]$processExitCode
            }
        } catch { }
    }

    if ($null -eq $exitCode) {
        throw "Benchmark command finished but did not report an exit code.`nStdout log: $stdoutPath`nStderr log: $stderrPath`nRecent stdout:`n$(Get-LogTail $stdoutPath)`nRecent stderr:`n$(Get-LogTail $stderrPath)"
    }

    if ($exitCode -eq 0) {
        Remove-Item -LiteralPath $runnerPath, $exitCodePath, $argumentsPath -Force -ErrorAction SilentlyContinue
    }

    return $exitCode
}

function Invoke-BenchmarkStep {
    param(
        [string[]]$Arguments
    )

    $workingBenchmarkProjectDir = $benchmarkProjectDir
    $tempRoot = $null
    $benchmarkExitCode = 0

    $duplicateBenchmarkProjects = Get-DuplicateBenchmarkProjects
    if ($duplicateBenchmarkProjects.Count -gt 0) {
        $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('norm-bench-gate-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
        Write-Host "Duplicate benchmark projects detected under repo; running isolated benchmark workspace at $tempRoot"
        Copy-CurrentWorkspaceForBenchmarkIsolation $tempRoot

        $workingBenchmarkProjectDir = Join-Path $tempRoot 'benchmarks'
    }

    try {
        $benchmarkExitCode = Invoke-TimedBenchmarkProcess `
            -WorkingDirectory $workingBenchmarkProjectDir `
            -Arguments $Arguments `
            -TimeoutMinutes $BenchmarkStepTimeoutMinutes

        if ($tempRoot) {
            $tempArtifacts = Join-Path $workingBenchmarkProjectDir 'BenchmarkDotNet.Artifacts'
            $localArtifacts = Join-Path $benchmarkProjectDir 'BenchmarkDotNet.Artifacts'
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $localArtifacts
            if (Test-Path $tempArtifacts) {
                Copy-Item -LiteralPath $tempArtifacts -Destination $benchmarkProjectDir -Recurse -Force
            }
        }
    }
    finally {
        if ($tempRoot) {
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $tempRoot
        }
    }

    if ($benchmarkExitCode -ne 0) {
        throw "Benchmark command failed with exit code $benchmarkExitCode."
    }
}

function Assert-CurrentPackageOutput {
    param(
        [string]$Directory,
        [string]$PackageId,
        [string]$Version
    )

    $expected = @(
        "$PackageId.$Version.nupkg",
        "$PackageId.$Version.snupkg"
    ) | Sort-Object
    $actual = Get-ChildItem -LiteralPath $Directory -File -Filter "$PackageId.*.*nupkg" |
        Select-Object -ExpandProperty Name |
        Sort-Object

    if (@($actual).Count -ne @($expected).Count) {
        throw "Unexpected package artifacts for $PackageId in $Directory. Expected: $($expected -join ', '); actual: $($actual -join ', ')"
    }

    for ($i = 0; $i -lt $expected.Count; $i++) {
        if ($actual[$i] -ne $expected[$i]) {
            throw "Unexpected package artifact for $PackageId at index $i. Expected '$($expected[$i])', actual '$($actual[$i])'."
        }
    }
}

$liveProviders = @()
if (Test-ProviderConfigured 'SQLSERVER') { $liveProviders += 'SQL Server' }
if (Test-ProviderConfigured 'POSTGRES') { $liveProviders += 'PostgreSQL' }
if (Test-ProviderConfigured 'MYSQL') { $liveProviders += 'MySQL' }

if ($MinLiveProviders -eq 0 -and $Mode -eq 'rc') {
    $MinLiveProviders = 3
}
elseif ($MinLiveProviders -eq 0 -and $Mode -eq 'live') {
    $MinLiveProviders = 2
}

if ($liveProviders.Count -lt $MinLiveProviders) {
    throw "v1 gate requires $MinLiveProviders live provider(s), but only $($liveProviders.Count) are configured: $($liveProviders -join ', ')"
}

if (-not $env:NORM_REQUIRE_LIVE_PARITY -and $liveProviders.Count -gt 0) {
    $env:NORM_REQUIRE_LIVE_PARITY = 'any'
}
if (-not $env:NORM_MIN_LIVE_PROVIDERS) {
    $env:NORM_MIN_LIVE_PROVIDERS = "$($liveProviders.Count)"
}

$normVersion = Get-NormVersion

$liveSupplementalFilter = 'Category=ProviderParity'
$liveFilter = "Category=LiveProvider|$liveSupplementalFilter"
$navigationFilter = 'Category=NavigationStress'
$transactionFilter = 'Category=TransactionStress'
$compiledFilter = 'Category=CompiledQueryStress'
$parityFilter = 'Category=ProviderSourceGenParity'
$bulkFilter = 'Category=BulkProviderParity'
$migrationFilter = 'Category=MigrationParity'
$cacheMemoryFilter = 'Category=CacheMemory'
$adversarialFilter = 'Category=AdversarialConcurrency'

Write-Host "nORM v1 release gate"
Write-Host "  Mode:                $Mode"
Write-Host "  Configuration:       $Configuration"
Write-Host "  Stress iterations:   $StressIterations"
Write-Host "  Live providers:      $($liveProviders -join ', ')"
Write-Host "  Min live providers:  $MinLiveProviders"
Write-Host "  Benchmark:           $(if ($SkipBenchmark) { 'skipped' } else { 'enabled' })"
Write-Host "  Provider matrix:     $(if ($SkipBenchmark -or $SkipProviderMatrixBenchmark -or $Mode -ne 'rc') { 'skipped' } else { 'enabled' })"
Write-Host "  Matrix slice timeout: $(if ($SkipBenchmark -or $SkipProviderMatrixBenchmark -or $Mode -ne 'rc') { 'n/a' } else { "$ProviderMatrixSliceTimeoutMinutes min" })"
Write-Host "  Test step timeout:   $TestStepTimeoutMinutes min"
Write-Host "  Test hang timeout:   $TestStepHangTimeoutMinutes min"
Write-Host "  Package version:     $normVersion"

Invoke-Step 'clean orphaned test hosts' { Stop-OrphanedTestHosts }
if ($Mode -eq 'rc' -and -not $SkipBenchmark) {
    Invoke-Step 'clean release evidence working tree check' {
        Assert-CleanReleaseEvidenceWorkspace 'RC benchmark evidence'
    }
}
Invoke-Step 'clean package outputs before build' {
    Clear-PackageOutput (Join-Path (Join-Path $root 'src') "bin\$Configuration") 'Normad'
    Clear-PackageOutput (Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') "bin\$Configuration") 'dotnet-norm'
}
Invoke-Step 'restore' { dotnet restore $solutionPath }
Invoke-Step 'build' { dotnet build $solutionPath --no-restore -c $Configuration --nologo }
Invoke-Step 'encoding scan' {
    & (Join-Path $root 'eng/scripts/check-encoding.ps1')
}
Invoke-Step 'AOT publish warning scan' {
    # Run a real AOT publish on the runtime project and diff IL diagnostics against the
    # baseline at eng/aot-baseline.txt. The source generator project is isolated from PublishAot
    # propagation via TreatAsLocalProperty in nORM.SourceGenerators.csproj, so NETSDK1207 no
    # longer fires there. New IL diagnostics (file:line:code not in the baseline) fail the gate;
    # entries that have disappeared from the baseline are reported as progress, prompting the
    # operator to regenerate the baseline via eng/scripts/update-aot-baseline.ps1.
    $aotOutput = dotnet publish $runtimeProjectPath -c $Configuration -r linux-x64 --self-contained `
        -p:PublishAot=true --nologo 2>&1 | Out-String

    $diagnosticRegex = [regex]'(?m)^(?<file>.+?)\((?<line>\d+),(?<col>\d+)\):\s+(?:error|warning)\s+(?<code>IL\d{4}):'
    $repoRootPrefix = ($root + '\')
    $observed = $diagnosticRegex.Matches($aotOutput) | ForEach-Object {
        $relativeFile = ($_.Groups['file'].Value -replace [regex]::Escape($repoRootPrefix), '') -replace '\\', '/'
        '{0}:{1}:{2}' -f $relativeFile, $_.Groups['line'].Value, $_.Groups['code'].Value
    } | Sort-Object -Unique

    $baselinePath = Join-Path $root 'eng/aot-baseline.txt'
    if (-not (Test-Path -LiteralPath $baselinePath)) {
        throw "AOT baseline missing at $baselinePath. Regenerate via eng/scripts/update-aot-baseline.ps1."
    }
    $baseline = @(Get-Content -LiteralPath $baselinePath | Where-Object { $_ -and -not $_.StartsWith('#') })

    $newDiagnostics    = @($observed | Where-Object { $baseline -notcontains $_ })
    $cleanedDiagnostics = @($baseline | Where-Object { $observed -notcontains $_ })

    Write-Host ("AOT publish scan: observed={0} baseline={1} new={2} cleaned={3}" -f `
        @($observed).Count, @($baseline).Count, $newDiagnostics.Count, $cleanedDiagnostics.Count)

    if ($cleanedDiagnostics.Count -gt 0) {
        Write-Host "AOT diagnostics no longer present (regenerate baseline to lock in progress):"
        $cleanedDiagnostics | ForEach-Object { Write-Host "  - $_" }
    }

    if ($newDiagnostics.Count -gt 0) {
        Write-Host "New AOT diagnostics not in baseline:"
        $newDiagnostics | ForEach-Object { Write-Host "  + $_" }
        $expectAllowed = $env:NORM_AOT_NEW_DIAGNOSTICS_ALLOWED -eq '1'
        if (-not $expectAllowed) {
            throw "AOT publish produced $($newDiagnostics.Count) new IL diagnostic(s) not in eng/aot-baseline.txt. Annotate the call sites or, if intentional, regenerate the baseline."
        }
        Write-Host "NORM_AOT_NEW_DIAGNOSTICS_ALLOWED=1: not failing the gate (diagnostic mode)."
    }

    if ($newDiagnostics.Count -eq 0 -and $observed.Count -eq 0 -and $baseline.Count -eq 0) {
        Write-Host "AOT publish warning scan: no IL diagnostics (runtime is AOT-clean)."
    }

    # `dotnet publish` returns a non-zero exit code whenever the baseline contains IL errors
    # (NETSDK or analyzer errors). Once the diff against the baseline is clean, that exit
    # code is no longer a gate failure — the gate fails on new IL diagnostics, not on the
    # presence of the baseline. Reset $LASTEXITCODE so Invoke-Step does not re-throw.
    $global:LASTEXITCODE = 0
}
Invoke-TestStep 'public API snapshot' 'FullyQualifiedName~PublicApiSnapshotTests'
Invoke-TestStep 'package consumer smoke tests' 'Category=PackageConsumer'
Invoke-TestStep 'CLI smoke tests' 'FullyQualifiedName~CliIntegrationTests'

if ($Mode -in @('live', 'full', 'rc')) {
    Invoke-TestStep 'live provider gate' $liveFilter
}

if ($Mode -in @('full', 'rc')) {
    Invoke-TestStep 'full test suite' ''
}

if ($Mode -eq 'rc') {
    foreach ($i in 1..$StressIterations) {
        Invoke-TestStep "navigation stress $i/$StressIterations" $navigationFilter
    }
    foreach ($i in 1..$StressIterations) {
        Invoke-TestStep "transaction stress $i/$StressIterations" $transactionFilter
    }
    foreach ($i in 1..$StressIterations) {
        Invoke-TestStep "compiled query stress $i/$StressIterations" $compiledFilter
    }

    Invoke-TestStep 'provider/source-gen parity' $parityFilter
    Invoke-TestStep 'bulk/provider parity' $bulkFilter
    Invoke-TestStep 'migration provider gate' $migrationFilter
    Invoke-TestStep 'cache memory bounds gate' $cacheMemoryFilter
    Invoke-TestStep 'concurrency/adversarial gate' $adversarialFilter
    Invoke-TestStep 'live provider gate second pass' $liveFilter
    Invoke-TestStep 'full test suite second pass' ''
}

if (-not $SkipBenchmark -and $Mode -in @('full', 'rc')) {
    Invoke-Step 'clean benchmark outputs before run' {
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue `
            (Join-Path $root 'BenchmarkDotNet.Artifacts'),
            (Join-Path $benchmarkProjectDir 'BenchmarkDotNet.Artifacts')
    }

    Invoke-Step 'fast complex query benchmark' { Invoke-BenchmarkStep @('--fast', 'Query_Complex') }
}

if (-not $SkipBenchmark -and -not $SkipProviderMatrixBenchmark -and $Mode -eq 'rc') {
    if (-not (Test-ProviderConfigured 'SQLSERVER') -or -not (Test-ProviderConfigured 'POSTGRES') -or -not (Test-ProviderConfigured 'MYSQL')) {
        throw 'Provider matrix benchmark requires SQL Server, PostgreSQL, and MySQL live connection strings.'
    }

    Invoke-Step 'SQLite/SQL Server/PostgreSQL/MySQL provider matrix benchmark slices' {
        $sliceRoot = Join-Path $root 'BenchmarkDotNet.Artifacts/provider-slices'
        & (Join-Path $root 'eng/run-provider-benchmark-slice.ps1') `
            -Providers 'Sqlite,SqlServer,Postgres,MySql' `
            -Filters $ProviderMatrixBenchmarkFilter `
            -Configuration $Configuration `
            -OutputRoot $sliceRoot `
            -SliceTimeoutMinutes $ProviderMatrixSliceTimeoutMinutes

        $latestSlice = Get-ChildItem -LiteralPath $sliceRoot -Directory |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -First 1
        if (-not $latestSlice) {
            throw "Provider matrix benchmark slices produced no run directory under $sliceRoot."
        }

        $script:benchmarkResultsPath = Join-Path $latestSlice.FullName 'results'
    }
}

# A resume that starts at/after the evidence manifest skipped the slice step
# that normally points $benchmarkResultsPath at the fresh slice run, so reuse
# the latest existing slice results (the evidence the failed run produced).
if ($StartAt -in @('benchmark evidence manifest', 'benchmark threshold gate') -and $Mode -eq 'rc' -and
    $benchmarkResultsPath -eq (Join-Path (Join-Path $benchmarkProjectDir 'BenchmarkDotNet.Artifacts') 'results')) {
    $sliceRoot = Join-Path $root 'BenchmarkDotNet.Artifacts/provider-slices'
    $latestSlice = if (Test-Path $sliceRoot) {
        Get-ChildItem -LiteralPath $sliceRoot -Directory |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -First 1
    }
    if (-not $latestSlice) {
        throw "Resume at '$StartAt' needs provider slice results, but none exist under $sliceRoot."
    }
    $benchmarkResultsPath = Join-Path $latestSlice.FullName 'results'
    Write-Host "Resume: reusing provider slice results at $benchmarkResultsPath"
}

if (-not $SkipBenchmark -and $Mode -in @('full', 'rc')) {
    Invoke-Step 'benchmark evidence manifest' {
        $benchmarkEvidenceMode = if ($Mode -eq 'full') { 'smoke' } else { $Mode }
        $benchmarkEvidenceFilter = if ($Mode -eq 'full') { '--fast Query_Complex' } else { $ProviderMatrixBenchmarkFilter }
        & (Join-Path $root 'eng/benchmark-evidence.ps1') `
            -ResultsDirectory $benchmarkResultsPath `
            -OutputDirectory $benchmarkEvidencePath `
            -BenchmarkFilter $benchmarkEvidenceFilter `
            -Mode $benchmarkEvidenceMode
    }

    Invoke-Step 'benchmark threshold gate' {
        $thresholdArgs = @{
            ResultsDirectory = $benchmarkResultsPath
            OutputDirectory = $benchmarkEvidencePath
            ThresholdFile = (Join-Path $root 'eng/benchmark-thresholds.json')
        }
        if ($Mode -ne 'rc') {
            $thresholdArgs.AllowMissingRules = $true
        }

        & (Join-Path $root 'eng/check-benchmark-thresholds.ps1') @thresholdArgs
    }
}

Invoke-Step 'clean package outputs' {
    Clear-PackageOutput (Join-Path (Join-Path $root 'src') "bin\$Configuration") 'Normad'
    Clear-PackageOutput (Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') "bin\$Configuration") 'dotnet-norm'
}
Invoke-Step 'pack Normad' { dotnet pack $runtimeProjectPath -c $Configuration --no-build --include-symbols -p:SymbolPackageFormat=snupkg }
Invoke-Step 'pack dotnet-norm' { dotnet pack $toolProjectPath -c $Configuration --no-build --include-symbols -p:SymbolPackageFormat=snupkg }
Invoke-Step 'validate package outputs' {
    Assert-CurrentPackageOutput (Join-Path (Join-Path $root 'src') "bin\$Configuration") 'Normad' $normVersion
    Assert-CurrentPackageOutput (Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') "bin\$Configuration") 'dotnet-norm' $normVersion
}
Invoke-Step 'RC artifact manifest' {
    & (Join-Path $root 'eng/rc-artifact-manifest.ps1') `
        -Mode $Mode `
        -Configuration $Configuration `
        -StressIterations $StressIterations `
        -BenchmarkSkipped ([bool]$SkipBenchmark)
}

if ($script:resumeSkipping) {
    throw "-StartAt '$StartAt' did not match any step name; every step was skipped. Use the exact name from the '==> <name>' log lines."
}

Write-Host ""
Write-Host "nORM v1 release gate passed."
