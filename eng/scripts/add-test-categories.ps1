param(
    [switch]$DryRun
)

# Ensure every public xUnit test class in tests/*.cs carries a [Trait("Category", "...")]
# attribute. Classes without one get Fast by default; filename patterns promote others:
#   *Live*Tests.cs / Live*.cs               -> LiveProvider
#   *Stress*.cs / *Adversarial*.cs / *Fuzz* -> Stress
#   *FaultInjection*.cs / *Concurrent.*Tests -> Stress
#   *PackageConsumer*.cs / *PackageSmoke*.cs -> PackageConsumer
# Files contain multiple public classes; this script iterates every match and only inserts the
# trait when one isn't already present in the preceding attribute block.

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$testsDir = Join-Path $root 'tests'

$files = Get-ChildItem -Path $testsDir -Filter '*.cs' -File | Where-Object {
    $_.Name -notmatch '^(TestCategories|TestBase|SqliteParameterFactory|LiveProviderEnvironment|GlobalUsings)\.cs$'
}

$classPattern = [regex]'(?m)^(?<indent>[ \t]*)(?<modifier>public\s+(?:sealed\s+|abstract\s+|static\s+|partial\s+)*class)\s+(?<name>[A-Za-z_]\w*)'

$totalAdded = 0
$filesTouched = 0

foreach ($file in $files) {
    $content = [IO.File]::ReadAllText($file.FullName, [Text.Encoding]::UTF8)

    # Decide category by filename.
    $name = $file.Name
    $category = if ($name -match 'PackageConsumer|PackageSmoke') { 'PackageConsumer' }
                elseif ($name -match '^Live|LiveProvider|LiveCross') { 'LiveProvider' }
                elseif ($name -match 'Stress|Adversarial|FaultInjection|FaultInjected|Fuzz|Concurrent.*Tests') { 'Stress' }
                else { 'Fast' }

    # Map ranges of raw / verbatim string literals so we don't insert traits into embedded
    # source code (e.g., model class declarations inside `"""..."""` blocks consumed by
    # CliIntegrationTests when it synthesizes a child project).
    $stringRanges = New-Object 'System.Collections.Generic.List[int[]]'
    $rawLitRegex = [regex]'(?s)"""(?:""[^"]|[^"]|"[^"])*?"""'
    foreach ($rawMatch in $rawLitRegex.Matches($content)) {
        $stringRanges.Add(@($rawMatch.Index, $rawMatch.Index + $rawMatch.Length))
    }
    $verbLitRegex = [regex]'(?s)@"(?:[^"]|"")*?"'
    foreach ($verbMatch in $verbLitRegex.Matches($content)) {
        $stringRanges.Add(@($verbMatch.Index, $verbMatch.Index + $verbMatch.Length))
    }

    function InsideStringRange {
        param([int]$Position, [System.Collections.Generic.List[int[]]]$Ranges)
        foreach ($r in $Ranges) {
            if ($Position -ge $r[0] -and $Position -lt $r[1]) { return $true }
        }
        return $false
    }

    # Walk class declarations in reverse so insertions don't shift later offsets.
    $matches = @($classPattern.Matches($content))
    if ($matches.Count -eq 0) { continue }
    [array]::Reverse($matches)

    $modified = $false
    $added = 0
    foreach ($match in $matches) {
        $startIdx = $match.Index
        if (InsideStringRange -Position $startIdx -Ranges $stringRanges) { continue }
        $indent = $match.Groups['indent'].Value
        $modifier = $match.Groups['modifier'].Value
        $className = $match.Groups['name'].Value

        # Look back up to 600 chars for a preceding attribute block on the same indent and check
        # for an existing [Trait("Category", "...")] entry on THIS class.
        $lookback = [Math]::Max(0, $startIdx - 800)
        $context = $content.Substring($lookback, $startIdx - $lookback)
        # Take the contiguous run of attribute lines / blank lines immediately above the class.
        $contextLines = $context -split "`n"
        $attrBlock = New-Object 'System.Collections.Generic.List[string]'
        for ($i = $contextLines.Length - 1; $i -ge 0; $i--) {
            $line = $contextLines[$i].TrimEnd("`r")
            if ($line.Trim().StartsWith('[')) { $attrBlock.Insert(0, $line) }
            elseif ([string]::IsNullOrWhiteSpace($line)) { continue }
            else { break }
        }
        $attrText = ($attrBlock -join "`n")
        if ($attrText -match '\[(?:Xunit\.)?Trait\(\s*"Category"\s*,') { continue }

        $insertion = "$indent[Xunit.Trait(""Category"", ""$category"")]`r`n"
        $content = $content.Substring(0, $startIdx) + $insertion + $content.Substring($startIdx)
        $modified = $true
        $added++
    }

    if (-not $modified) { continue }
    $filesTouched++
    $totalAdded += $added

    if (-not $DryRun) {
        $bytes = [IO.File]::ReadAllBytes($file.FullName)
        $hadBom = $bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF
        $encoding = [Text.UTF8Encoding]::new($hadBom)
        [IO.File]::WriteAllText($file.FullName, $content, $encoding)
    }
    Write-Host ("[{0}] {1} (+{2} class(es))" -f $category, $file.Name, $added)
}

Write-Host ""
Write-Host ("Trait insertions: $totalAdded across $filesTouched file(s)")
if ($DryRun) { Write-Host "DRY RUN - no files written." }
