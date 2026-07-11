param(
    [string]$Providers = 'Sqlite,SqlServer,Postgres,MySql',
    [string]$Filters = '*ProviderMatrixBenchmarks.Query_Join*',
    [string]$Configuration = 'Release',
    [string]$OutputRoot = (Join-Path (Split-Path -Parent $PSScriptRoot) 'BenchmarkDotNet.Artifacts/provider-slices'),
    [int]$SliceTimeoutMinutes = 90,
    [switch]$CheckThresholds,
    [switch]$NoIsolation,
    [switch]$KeepWorktree
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$benchmarksDir = Join-Path $root 'benchmarks'
$runStamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$runRoot = Join-Path $OutputRoot $runStamp
$mergedResults = Join-Path $runRoot 'results'
$providerList = @($Providers -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$filterList = @($Filters -split ';' | ForEach-Object { $_.Trim() } | Where-Object { $_ })

if ($SliceTimeoutMinutes -le 0) {
    throw "SliceTimeoutMinutes must be greater than zero."
}

function Get-SafeName {
    param([string]$Value)
    $safe = ($Value -replace '[^A-Za-z0-9_.-]', '_').Trim('_')
    # Multi-pattern filters produce very long names; Windows' MAX_PATH (260) then
    # breaks the report copy deep inside the slice directory. Truncate and append
    # a short stable hash so distinct filters still get distinct directories.
    if ($safe.Length -gt 80) {
        $sha = [System.Security.Cryptography.SHA256]::Create()
        try {
            $digest = $sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($safe))
        }
        finally {
            $sha.Dispose()
        }
        $hash = -join ($digest[0..3] | ForEach-Object { $_.ToString('x2') })
        $safe = $safe.Substring(0, 72) + '_' + $hash
    }
    return $safe
}

function Normalize-ProviderName {
    param([string]$Provider)

    switch ($Provider.ToLowerInvariant()) {
        'sqlite' { return 'Sqlite' }
        'sqlserver' { return 'SqlServer' }
        'sql-server' { return 'SqlServer' }
        'mssql' { return 'SqlServer' }
        'postgres' { return 'Postgres' }
        'postgresql' { return 'Postgres' }
        'mysql' { return 'MySql' }
        default { throw "Unsupported provider '$Provider'. Use Sqlite, SqlServer, Postgres, or MySql." }
    }
}

function Get-LogTail {
    param(
        [string]$Path,
        [int]$Lines = 80
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return ''
    }

    return (Get-Content -LiteralPath $Path -Tail $Lines -ErrorAction SilentlyContinue | Out-String)
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

function Get-SourceCommitForBenchmarkEvidence {
    $environmentCommit = [Environment]::GetEnvironmentVariable('NORM_BENCHMARK_COMMIT')
    if (-not [string]::IsNullOrWhiteSpace($environmentCommit)) {
        return $environmentCommit.Trim()
    }

    $gitCommit = @(& git -C $root rev-parse HEAD 2>$null)
    if ($LASTEXITCODE -eq 0) {
        $commitLine = $gitCommit |
            Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
            Select-Object -First 1
        if ($commitLine) {
            return ([string]$commitLine).Trim()
        }
    }

    return ''
}

function Invoke-BenchmarkSlice {
    param(
        [string]$Provider,
        [string]$Filter,
        [string]$LogPath
    )

    $errPath = [System.IO.Path]::ChangeExtension($LogPath, '.err.log')
    $exitCodePath = [System.IO.Path]::ChangeExtension($LogPath, '.exitcode')
    $argumentsPath = [System.IO.Path]::ChangeExtension($LogPath, '.args.json')
    $runnerPath = [System.IO.Path]::ChangeExtension($LogPath, '.runner.ps1')
    Remove-Item -LiteralPath $LogPath, $errPath, $exitCodePath, $argumentsPath, $runnerPath -Force -ErrorAction SilentlyContinue

    # A single filter entry may carry multiple space-separated BenchmarkDotNet glob
    # patterns (e.g. a threshold-derived method list); each becomes its own --filter
    # pattern argument so the union runs in one benchmark session per provider.
    $filterPatterns = @($Filter -split '\s+' | Where-Object { $_ })
    $arguments = @(
        'run',
        '-c', $Configuration,
        '--',
        '--provider-matrix',
        '--provider', $Provider,
        '--filter'
    ) + $filterPatterns
    $arguments | ConvertTo-Json | Set-Content -LiteralPath $argumentsPath -Encoding UTF8

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

    # Launch the same PowerShell engine that is running this script so the runner
    # works on Windows PowerShell and on pwsh under Linux/macOS CI.
    $powerShellHost = [System.Diagnostics.Process]::GetCurrentProcess().MainModule.FileName
    $process = Start-Process -FilePath $powerShellHost `
        -ArgumentList (@(
            '-NoProfile',
            '-ExecutionPolicy', 'Bypass',
            '-File', $runnerPath,
            '-BenchmarkWorkingDirectory', $benchmarksDir,
            '-ExitCodePath', $exitCodePath,
            '-ArgumentsPath', $argumentsPath
        )) `
        -WorkingDirectory $benchmarksDir `
        -RedirectStandardOutput $LogPath `
        -RedirectStandardError $errPath `
        -NoNewWindow `
        -PassThru

    $timeoutMs = [int][TimeSpan]::FromMinutes($SliceTimeoutMinutes).TotalMilliseconds
    if (-not $process.WaitForExit($timeoutMs)) {
        try {
            $process.Kill($true)
        }
        catch {
            try { taskkill /PID $process.Id /T /F | Out-Null } catch { }
        }

        $terminated = $process.WaitForExit(30000)
        if (-not $terminated) {
            try { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue } catch { }
        }

        $tail = Get-LogTail $LogPath
        $terminationNote = if ($terminated) { '' } else { "`nThe benchmark process did not exit within 30 seconds after the kill request; check for leftover child processes." }
        throw @"
Benchmark slice timed out after $SliceTimeoutMinutes minute(s) for provider '$Provider' filter '$Filter'.
Log: $LogPath
$terminationNote
Last log lines:
$tail
"@
    }

    if ((Test-Path -LiteralPath $errPath) -and (Get-Item -LiteralPath $errPath).Length -gt 0) {
        Add-Content -LiteralPath $LogPath -Value "`n// STDERR"
        Get-Content -LiteralPath $errPath | Add-Content -LiteralPath $LogPath
    }

    try { $process.Refresh() } catch { }

    $exitCode = $null
    if (Test-Path -LiteralPath $exitCodePath) {
        $rawExitCode = Get-Content -LiteralPath $exitCodePath -First 1 -ErrorAction SilentlyContinue
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
        throw @"
Benchmark slice finished but did not report an exit code for provider '$Provider' filter '$Filter'.
Log: $LogPath
Last log lines:
$(Get-LogTail $LogPath)
"@
    }

    if ($exitCode -eq 0) {
        Remove-Item -LiteralPath $runnerPath, $exitCodePath, $argumentsPath -Force -ErrorAction SilentlyContinue
    }

    return $exitCode
}

if (-not $NoIsolation) {
    $expectedProject = [System.IO.Path]::GetFullPath((Join-Path $benchmarksDir 'nORM.Benchmarks.csproj'))
    $benchmarkProjects = @(Get-ChildItem -LiteralPath $root -Recurse -File -Filter 'nORM.Benchmarks.csproj' |
        Where-Object { [System.IO.Path]::GetFullPath($_.FullName) -ne $expectedProject })

    if ($benchmarkProjects.Count -gt 0) {
        $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('norm-bench-slice-' + $runStamp)
        Write-Host "Duplicate benchmark projects detected under repo; running isolated benchmark workspace at $tempRoot"
        Copy-CurrentWorkspaceForBenchmarkIsolation $tempRoot

        $childScript = Join-Path $tempRoot 'eng/run-provider-benchmark-slice.ps1'
        $forwardArgs = @(
            '-NoProfile',
            '-ExecutionPolicy', 'Bypass',
            '-File', $childScript,
            '-Providers', ($providerList -join ','),
            '-Filters', ($filterList -join ';'),
            '-Configuration', $Configuration,
            '-OutputRoot', $OutputRoot,
            '-SliceTimeoutMinutes', $SliceTimeoutMinutes,
            '-NoIsolation'
        )
        if ($CheckThresholds) {
            $forwardArgs += '-CheckThresholds'
        }
        if ($KeepWorktree) {
            $forwardArgs += '-KeepWorktree'
        }

        $previousBenchmarkCommit = [Environment]::GetEnvironmentVariable('NORM_BENCHMARK_COMMIT')
        $benchmarkCommit = Get-SourceCommitForBenchmarkEvidence
        if (-not [string]::IsNullOrWhiteSpace($benchmarkCommit)) {
            [Environment]::SetEnvironmentVariable('NORM_BENCHMARK_COMMIT', $benchmarkCommit, 'Process')
        }

        try {
            powershell @forwardArgs
            $childExitCode = if ($LASTEXITCODE -ne $null) { $LASTEXITCODE } else { 0 }
        }
        finally {
            [Environment]::SetEnvironmentVariable('NORM_BENCHMARK_COMMIT', $previousBenchmarkCommit, 'Process')
            if (-not $KeepWorktree) {
                Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $tempRoot
            }
        }

        exit $childExitCode
    }
}

New-Item -ItemType Directory -Force -Path $mergedResults | Out-Null

foreach ($providerArg in $providerList) {
    $provider = Normalize-ProviderName $providerArg

    foreach ($filter in $filterList) {
        $safeFilter = Get-SafeName $filter
        $sliceDir = Join-Path (Join-Path $runRoot $provider) $safeFilter
        $sliceResults = Join-Path $sliceDir 'results'
        New-Item -ItemType Directory -Force -Path $sliceResults | Out-Null

        Push-Location $benchmarksDir
        try {
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue '.\BenchmarkDotNet.Artifacts'
            $logPath = Join-Path $sliceDir 'benchmark.log'
            Write-Host "Running provider slice: provider=$provider filter=$filter"
            $exitCode = Invoke-BenchmarkSlice -Provider $provider -Filter $filter -LogPath $logPath
            if ($exitCode -ne 0) {
                throw "Benchmark slice failed for provider '$provider' filter '$filter'. See $logPath."
            }

            $artifactResults = Join-Path $benchmarksDir 'BenchmarkDotNet.Artifacts/results'
            $reports = Get-ChildItem -LiteralPath $artifactResults -File -Filter '*-report.*' -ErrorAction SilentlyContinue
            if (-not $reports) {
                throw "Benchmark slice produced no report files for provider '$provider' filter '$filter'. See $logPath."
            }

            foreach ($report in $reports) {
                $sliceName = '{0}-{1}-{2}' -f $provider, $safeFilter, $report.Name
                Copy-Item -LiteralPath $report.FullName -Destination (Join-Path $sliceResults $report.Name) -Force
                Copy-Item -LiteralPath $report.FullName -Destination (Join-Path $mergedResults $sliceName) -Force
            }
        }
        finally {
            Pop-Location
        }
    }
}

& (Join-Path $root 'eng/benchmark-evidence.ps1') `
    -ResultsDirectory $mergedResults `
    -OutputDirectory (Join-Path $runRoot 'v1-evidence') `
    -BenchmarkFilter ($filterList -join '; ') `
    -Mode 'slice'

if ($CheckThresholds) {
    & (Join-Path $root 'eng/check-benchmark-thresholds.ps1') `
        -ResultsDirectory $mergedResults `
        -OutputDirectory (Join-Path $runRoot 'v1-evidence') `
        -ThresholdFile (Join-Path $root 'eng/benchmark-thresholds.json') `
        -AllowMissingRules
}

Write-Host "Provider benchmark slices written to $runRoot"
