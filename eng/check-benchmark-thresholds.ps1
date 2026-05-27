param(
    [string]$ResultsDirectory = (Join-Path (Split-Path -Parent $PSScriptRoot) 'BenchmarkDotNet.Artifacts/results'),
    [string]$ThresholdFile = (Join-Path $PSScriptRoot 'benchmark-thresholds.json'),
    [string]$OutputDirectory = (Join-Path (Split-Path -Parent $PSScriptRoot) 'BenchmarkDotNet.Artifacts/v1-evidence'),
    [switch]$AllowMissingRules
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

function Get-FullInputPath {
    param([string]$Path)

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }

    return [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $Path))
}

function Get-DisplayPath {
    param([string]$Path)

    $fullPath = Get-FullInputPath $Path
    $fullRoot = [System.IO.Path]::GetFullPath($root).TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
    if ($fullPath.Equals($fullRoot, [StringComparison]::OrdinalIgnoreCase)) {
        return '.'
    }

    $rootPrefix = $fullRoot + [System.IO.Path]::DirectorySeparatorChar
    if ($fullPath.StartsWith($rootPrefix, [StringComparison]::OrdinalIgnoreCase)) {
        return $fullPath.Substring($rootPrefix.Length)
    }

    return $fullPath
}

function Convert-MeanToNanoseconds {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return [double]::PositiveInfinity
    }

    $normalized = $Value.Trim() -replace ',', ''
    if ($normalized -notmatch '^([0-9]+(?:\.[0-9]+)?)\s*(\S+)?$') {
        return [double]::PositiveInfinity
    }

    $number = [double]::Parse($Matches[1], [System.Globalization.CultureInfo]::InvariantCulture)
    $unit = $Matches[2]
    switch ($unit) {
        's' { return $number * 1000000000.0 }
        'ms' { return $number * 1000000.0 }
        'us' { return $number * 1000.0 }
        ([char]0x00B5 + 's') { return $number * 1000.0 }
        ([char]0x03BC + 's') { return $number * 1000.0 }
        default { return $number }
    }
}

function Get-CanonicalMethodName {
    param([string]$Method)

    $name = $Method.Trim().Trim("'")
    switch ($name) {
        'Query Simple Raw ADO (Convenience)' { return 'Query_Simple_RawAdo_Convenience' }
        'Query Simple Raw ADO (Optimized)' { return 'Query_Simple_RawAdo_Optimized' }
        'Query Simple EF Core (Compiled)' { return 'Query_Simple_EfCore_Compiled' }
        'Query Simple nORM (Compiled)' { return 'Query_Simple_nORM_Compiled' }
        'Query Simple Raw ADO (Prepared Optimized)' { return 'Query_Simple_RawAdo_PreparedOptimized' }
        'Query Complex Raw ADO (Convenience)' { return 'Query_Complex_RawAdo_Convenience' }
        'Query Complex Raw ADO (Optimized)' { return 'Query_Complex_RawAdo_Optimized' }
        'Query Complex EF Core (Compiled)' { return 'Query_Complex_EfCore_Compiled' }
        'Query Complex nORM (Compiled)' { return 'Query_Complex_nORM_Compiled' }
        'Query Complex Raw ADO (Prepared Optimized)' { return 'Query_Complex_RawAdo_PreparedOptimized' }
        'Query Join Raw ADO (Convenience)' { return 'Query_Join_RawAdo_Convenience' }
        'Query Join Raw ADO (Optimized)' { return 'Query_Join_RawAdo_Optimized' }
        'Query Join Raw ADO (Prepared Optimized)' { return 'Query_Join_RawAdo_PreparedOptimized' }
        'Count Raw ADO (Optimized)' { return 'Count_RawAdo_Optimized' }
        'BulkInsert Naive - EF per row' { return 'BulkInsert_Naive_EfCore' }
        'BulkInsert Naive - nORM per row' { return 'BulkInsert_Naive_nORM' }
        'BulkInsert Naive - Dapper per row' { return 'BulkInsert_Naive_Dapper' }
        'BulkInsert Batched - EF SaveChanges in Tx' { return 'BulkInsert_Batched_EfCore' }
        'BulkInsert Batched - nORM Tx + per row' { return 'BulkInsert_Batched_nORM' }
        'BulkInsert Batched - Dapper prepared' { return 'BulkInsert_Batched_Dapper' }
        'BulkInsert Batched - nORM Prepared' { return 'BulkInsert_Batched_nORM_Prepared' }
        'BulkInsert Idiomatic - EF AddRange' { return 'BulkInsert_Idiomatic_EfCore' }
        'BulkInsert Idiomatic - nORM BulkInsert' { return 'BulkInsert_Idiomatic_nORM' }
        'BulkInsert Idiomatic - Dapper list in Tx' { return 'BulkInsert_Idiomatic_Dapper' }
        default { return $name }
    }
}

function Convert-AllocatedToBytes {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value) -or $Value.Trim() -eq '-') {
        return 0.0
    }

    $normalized = $Value.Trim() -replace ',', ''
    if ($normalized -notmatch '^([0-9]+(?:\.[0-9]+)?)\s*(B|KB|MB|GB)?$') {
        return [double]::PositiveInfinity
    }

    $number = [double]::Parse($Matches[1], [System.Globalization.CultureInfo]::InvariantCulture)
    switch ($Matches[2]) {
        'GB' { return $number * 1024.0 * 1024.0 * 1024.0 }
        'MB' { return $number * 1024.0 * 1024.0 }
        'KB' { return $number * 1024.0 }
        default { return $number }
    }
}

function Import-BenchmarkRows {
    param([string]$Directory)

    if (-not (Test-Path $Directory)) {
        throw "Benchmark results directory not found: $Directory"
    }

    $files = Get-ChildItem -LiteralPath $Directory -File -Filter '*-report.csv'
    if ($files.Count -eq 0) {
        throw "No BenchmarkDotNet CSV reports found in $Directory"
    }

    $rows = New-Object System.Collections.Generic.List[object]
    foreach ($file in $files) {
        $imported = Import-Csv $file.FullName -Delimiter ';'
        if (-not $imported -or -not ($imported[0].PSObject.Properties.Name -contains 'Method')) {
            $imported = Import-Csv $file.FullName
        }

        foreach ($row in $imported) {
            if (-not $row.Method -or -not $row.Mean) {
                continue
            }

            if (-not $row.Provider) {
                continue
            }

            $provider = $row.Provider
            $rows.Add([pscustomobject]@{
                Report = $file.Name
                Provider = $provider
                Method = $row.Method
                MethodKey = Get-CanonicalMethodName $row.Method
                Mean = $row.Mean
                MeanNs = Convert-MeanToNanoseconds $row.Mean
                Allocated = $row.Allocated
                AllocatedBytes = Convert-AllocatedToBytes $row.Allocated
            })
        }
    }

    return $rows
}

if (-not (Test-Path $ThresholdFile)) {
    throw "Benchmark threshold file not found: $ThresholdFile"
}

$thresholds = Get-Content -LiteralPath $ThresholdFile -Raw | ConvertFrom-Json
$rows = Import-BenchmarkRows $ResultsDirectory
$violations = New-Object System.Collections.Generic.List[string]
$results = New-Object System.Collections.Generic.List[object]

foreach ($rule in $thresholds.rules) {
    $providers = if ($rule.provider -eq '*') {
        @($rows | Select-Object -ExpandProperty Provider -Unique | Sort-Object)
    }
    else {
        @($rule.provider)
    }

    foreach ($provider in $providers) {
        $providerRows = @($rows | Where-Object { $_.Provider -eq $provider })
        $targetRows = @($providerRows | Where-Object { $rule.targetMethods -contains $_.MethodKey })
        $baselineRows = @($providerRows | Where-Object { $rule.baselineMethods -contains $_.MethodKey })

        if ($targetRows.Count -eq 0 -or $baselineRows.Count -eq 0) {
            if ($rule.required -and -not $AllowMissingRules) {
                $violations.Add("Missing benchmark rows for rule '$($rule.name)' provider '$provider'. Targets: $($rule.targetMethods -join ', '); baselines: $($rule.baselineMethods -join ', ').")
            }
            continue
        }

        $target = $targetRows | Sort-Object MeanNs | Select-Object -First 1
        $baseline = $baselineRows | Sort-Object MeanNs | Select-Object -First 1
        $meanRatio = if ($baseline.MeanNs -gt 0) { $target.MeanNs / $baseline.MeanNs } else { [double]::PositiveInfinity }
        $allocatedRatio = if ($baseline.AllocatedBytes -gt 0) {
            $target.AllocatedBytes / $baseline.AllocatedBytes
        }
        elseif ($target.AllocatedBytes -eq 0) {
            1.0
        }
        else {
            [double]::PositiveInfinity
        }

        $result = [pscustomobject]@{
            Rule = $rule.name
            Provider = $provider
            TargetMethod = $target.Method
            TargetMean = $target.Mean
            BaselineMethod = $baseline.Method
            BaselineMean = $baseline.Mean
            MeanRatio = [Math]::Round($meanRatio, 3)
            MaxMeanRatio = [double]$rule.maxMeanRatio
            TargetAllocated = $target.Allocated
            BaselineAllocated = $baseline.Allocated
            AllocatedRatio = [Math]::Round($allocatedRatio, 3)
            MaxAllocatedRatio = [double]$rule.maxAllocatedRatio
        }
        $results.Add($result)

        if ($meanRatio -gt [double]$rule.maxMeanRatio) {
            $violations.Add("Mean threshold failed for '$($rule.name)' provider '$provider': $($target.Method) $($target.Mean) vs $($baseline.Method) $($baseline.Mean), ratio $([Math]::Round($meanRatio, 3)) > $($rule.maxMeanRatio).")
        }

        if ($allocatedRatio -gt [double]$rule.maxAllocatedRatio) {
            $violations.Add("Allocation threshold failed for '$($rule.name)' provider '$provider': $($target.Method) $($target.Allocated) vs $($baseline.Method) $($baseline.Allocated), ratio $([Math]::Round($allocatedRatio, 3)) > $($rule.maxAllocatedRatio).")
        }
    }
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$jsonPath = Join-Path $OutputDirectory 'benchmark-thresholds.json'
$mdPath = Join-Path $OutputDirectory 'benchmark-thresholds.md'

$summary = [ordered]@{
    GeneratedUtc = [DateTime]::UtcNow.ToString('O')
    ThresholdFile = Get-DisplayPath $ThresholdFile
    ResultsDirectory = $ResultsDirectory
    Passed = $violations.Count -eq 0
    Results = [object]$results.ToArray()
    Violations = [object]$violations.ToArray()
}
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $jsonPath -Encoding UTF8

$lines = New-Object System.Collections.Generic.List[string]
$lines.Add('# Benchmark Thresholds')
$lines.Add('')
$lines.Add("- Passed: $($violations.Count -eq 0)")
$lines.Add(('- Threshold file: `{0}`' -f $summary.ThresholdFile))
$lines.Add('')
$lines.Add('| Rule | Provider | Target | Baseline | Mean ratio | Allocation ratio |')
$lines.Add('| --- | --- | --- | --- | --- | --- |')
foreach ($result in $results) {
    $lines.Add(('| {0} | {1} | `{2}` {3} | `{4}` {5} | {6}/{7} | {8}/{9} |' -f $result.Rule, $result.Provider, $result.TargetMethod, $result.TargetMean, $result.BaselineMethod, $result.BaselineMean, $result.MeanRatio, $result.MaxMeanRatio, $result.AllocatedRatio, $result.MaxAllocatedRatio))
}

if ($violations.Count -gt 0) {
    $lines.Add('')
    $lines.Add('## Violations')
    $lines.Add('')
    foreach ($violation in $violations) {
        $lines.Add("- $violation")
    }
}

$lines | Set-Content -LiteralPath $mdPath -Encoding UTF8

if ($violations.Count -gt 0) {
    throw "Benchmark threshold check failed with $($violations.Count) violation(s). See $mdPath."
}

Write-Host "Benchmark thresholds passed. Summary written to $OutputDirectory"
